/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

//! A Sink for forwarding events directly to Remote service.
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fbinit::FacebookInit;

#[cfg(fbcode_build)]
mod fbcode {
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::SystemTime;

    use buck2_core::buck2_env;
    use buck2_data::InstantEvent;
    use buck2_data::Location;
    use buck2_data::StructuredError;
    use buck2_error::conversion::from_any_with_tag;
    use buck2_util::truncate::truncate;
    use fbinit::FacebookInit;
    use prost::Message;

    use crate::metadata;
    use crate::schedule_type::ScheduleType;
    use crate::sink::smart_truncate_event::smart_truncate_event;
    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;
    use crate::TraceId;

    // 1 MiB limit
    static SCRIBE_MESSAGE_SIZE_LIMIT: usize = 1024 * 1024;
    // 50k characters
    static TRUNCATED_SCRIBE_MESSAGE_SIZE: usize = 50000;

    /// RemoteEventSink is a ScribeSink backed by the Thrift-based client in the `buck2_scribe_client` crate.
    pub struct RemoteEventSink {
        category: String,
        client: scribe_client::ScribeClient,
        schedule_type: ScheduleType,
    }

    impl RemoteEventSink {
        /// Creates a new RemoteEventSink that forwards messages onto the Thrift-backed Scribe client.
        pub fn new(
            fb: FacebookInit,
            category: String,
            buffer_size: usize,
            retry_backoff: Duration,
            retry_attempts: usize,
            message_batch_size: Option<usize>,
        ) -> buck2_error::Result<RemoteEventSink> {
            let client = scribe_client::ScribeClient::new(
                fb,
                buffer_size,
                retry_backoff,
                retry_attempts,
                message_batch_size,
            )
            .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))?;

            // schedule_type can change for the same daemon, because on OD some builds are pre warmed for users
            // This would be problematic, because this is run just once on the daemon
            // But in this case we only check for 'diff' type, which shouldn't change
            let schedule_type = ScheduleType::new()?;
            Ok(RemoteEventSink {
                category,
                client,
                schedule_type,
            })
        }

        // Send this event now, bypassing internal message queue.
        pub async fn send_now(&self, event: BuckEvent) {
            self.send_messages_now(vec![event]).await;
        }

        // Send multiple events now, bypassing internal message queue.
        pub async fn send_messages_now(&self, events: Vec<BuckEvent>) {
            let messages = events
                .into_iter()
                .filter_map(|e| {
                    let message_key = e.trace_id().unwrap().hash();
                    Self::encode_message(e, false).map(|bytes| scribe_client::Message {
                        category: self.category.clone(),
                        message: bytes,
                        message_key: Some(message_key),
                    })
                })
                .collect();
            self.client.send_messages_now(messages).await;
        }

        // Send this event by placing it on the internal message queue.
        pub fn offer(&self, event: BuckEvent) {
            let message_key = event.trace_id().unwrap().hash();
            if let Some(bytes) = Self::encode_message(event, false) {
                self.client.offer(scribe_client::Message {
                    category: self.category.clone(),
                    message: bytes,
                    message_key: Some(message_key),
                });
            }
        }

        // Encodes message into something scribe understands.
        fn encode_message(mut event: BuckEvent, is_truncated: bool) -> Option<Vec<u8>> {
            smart_truncate_event(event.data_mut());
            let mut proto: Box<buck2_data::BuckEvent> = event.into();

            Self::prepare_event(&mut proto);

            // Add a header byte to indicate this is _not_ base64 encoding.
            let mut buf = Vec::with_capacity(proto.encoded_len() + 1);
            buf.push(b'!');
            let mut proto_bytes = proto.encode_to_vec();
            buf.append(&mut proto_bytes);

            if buf.len() > SCRIBE_MESSAGE_SIZE_LIMIT {
                // if this BuckEvent is already a truncated one but the buffer byte size exceeds the limit,
                // do not send Scribe another truncated version
                if is_truncated {
                    return None;
                }
                let json = serde_json::to_string(&proto).unwrap();

                Self::encode_message(
                    BuckEvent::new(
                        SystemTime::now(),
                        TraceId::new(),
                        None,
                        None,
                        buck2_data::buck_event::Data::Instant(InstantEvent {
                            data: Some(
                                StructuredError {
                                    location: Some(Location {
                                        file: file!().to_owned(),
                                        line: line!(),
                                        column: column!(),
                                    }),
                                    payload: format!("Soft Error: oversized_scribe: Message is oversized. Event data: {}. Original message size: {}", truncate(&json, TRUNCATED_SCRIBE_MESSAGE_SIZE),
                                    buf.len()),
                                    metadata: metadata::collect(),
                                    backtrace: Vec::new(),
                                    quiet: false,
                                    task: Some(true),
                                    soft_error_category: Some(buck2_data::SoftError {category: "oversized_scribe".to_owned(), is_quiet:false}),
                                    daemon_in_memory_state_is_corrupted: false,
                                    daemon_materializer_state_is_corrupted: false,
                                    action_cache_is_corrupted: false,
                                    deprecation: false,
                                }
                                .into(),
                            ),
                        }),
                    ),
                    true,
                )
            } else {
                Some(buf)
            }
        }

        fn prepare_event(event: &mut buck2_data::BuckEvent) {
            use buck2_data::buck_event::Data;

            match &mut event.data {
                Some(Data::SpanEnd(s)) => match &mut s.data {
                    Some(buck2_data::span_end_event::Data::ActionExecution(action)) => {
                        let mut is_cache_hit = false;

                        for command in action.commands.iter_mut() {
                            let Some(details) = command.details.as_mut() else {
                                continue;
                            };

                            {
                                let Some(ref command_kind) = details.command_kind else {
                                    continue;
                                };
                                let Some(ref command) = command_kind.command else {
                                    continue;
                                };
                                let buck2_data::command_execution_kind::Command::RemoteCommand(
                                    ref remote,
                                ) = command
                                else {
                                    continue;
                                };
                                if !remote.cache_hit {
                                    continue;
                                }
                            }

                            is_cache_hit = true;
                            details.metadata = None;
                        }

                        if is_cache_hit {
                            action.dep_file_key = None;
                            action.outputs.clear();
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }

    impl EventSink for RemoteEventSink {
        fn send(&self, event: Event) {
            match event {
                Event::Buck(event) => {
                    if should_send_event(event.data(), &self.schedule_type) {
                        self.offer(event);
                    }
                }
                Event::CommandResult(..) => {}
                Event::PartialResult(..) => {}
            }
        }
    }

    impl EventSinkWithStats for RemoteEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as _
        }

        fn stats(&self) -> EventSinkStats {
            let counters = self.client.export_counters();
            EventSinkStats {
                successes: counters.successes,
                failures_invalid_request: counters.failures_invalid_request,
                failures_unauthorized: counters.failures_unauthorized,
                failures_rate_limited: counters.failures_rate_limited,
                failures_pushed_back: counters.failures_pushed_back,
                failures_enqueue_failed: counters.failures_enqueue_failed,
                failures_internal_error: counters.failures_internal_error,
                failures_timed_out: counters.failures_timed_out,
                failures_unknown: counters.failures_unknown,
                buffered: counters.queue_depth,
                dropped: counters.dropped,
                bytes_written: counters.bytes_written,
            }
        }
    }

    fn should_send_event(d: &buck2_data::buck_event::Data, schedule_type: &ScheduleType) -> bool {
        use buck2_data::buck_event::Data;

        match d {
            Data::SpanStart(s) => {
                use buck2_data::span_start_event::Data;

                match &s.data {
                    Some(Data::Command(..)) => true,
                    None => false,
                    _ => false,
                }
            }
            Data::SpanEnd(s) => {
                use buck2_data::span_end_event::Data;
                use buck2_data::ActionExecutionKind;

                match &s.data {
                    Some(Data::Command(..)) => true,
                    Some(Data::ActionExecution(a)) => {
                        a.failed
                            || match ActionExecutionKind::from_i32(a.execution_kind) {
                                // Those kinds are not used in downstreams
                                Some(ActionExecutionKind::Simple) => false,
                                Some(ActionExecutionKind::Deferred) => false,
                                Some(ActionExecutionKind::NotSet) => false,
                                _ => true,
                            }
                    }
                    Some(Data::Analysis(..)) => !schedule_type.is_diff(),
                    Some(Data::Load(..)) => true,
                    Some(Data::CacheUpload(..)) => true,
                    Some(Data::DepFileUpload(..)) => true,
                    Some(Data::Materialization(..)) => true,
                    Some(Data::TestDiscovery(..)) => true,
                    Some(Data::TestEnd(..)) => true,
                    None => false,
                    _ => false,
                }
            }
            Data::Instant(i) => {
                use buck2_data::instant_event::Data;

                match i.data {
                    Some(Data::BuildGraphInfo(..)) => true,
                    Some(Data::RageResult(..)) => true,
                    Some(Data::ReSession(..)) => true,
                    Some(Data::StructuredError(..)) => true,
                    Some(Data::PersistEventLogSubprocess(..)) => true,
                    Some(Data::CleanStaleResult(..)) => true,
                    Some(Data::ConfigurationCreated(..)) => true,
                    None => false,
                    _ => false,
                }
            }
            Data::Record(r) => {
                use buck2_data::record_event::Data;

                match r.data {
                    Some(Data::InvocationRecord(..)) => true,
                    Some(Data::BuildGraphStats(..)) => true,
                    None => false,
                }
            }
        }
    }

    pub fn scribe_category() -> buck2_error::Result<String> {
        const DEFAULT_SCRIBE_CATEGORY: &str = "buck2_events";
        // Note that both daemon and client are emitting events, and that changing this variable has
        // no effect on the daemon until buckd is restarted but has effect on the client.
        Ok(
            buck2_env!("BUCK2_SCRIBE_CATEGORY", applicability = internal)?
                .unwrap_or(DEFAULT_SCRIBE_CATEGORY)
                .to_owned(),
        )
    }
}

#[cfg(not(fbcode_build))]
mod fbcode {
    use std::collections::HashMap;
    use std::env::VarError;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::thread::JoinHandle;

    use allocative::Allocative;
    use anyhow::Context;

    use async_stream::stream;

    use bazel_event_publisher_proto::build_event_stream;
    use bazel_event_publisher_proto::build_event_stream::build_event_id;
    use bazel_event_publisher_proto::build_event_stream::BuildEventId;
    use bazel_event_publisher_proto::google::devtools::build::v1;
    use buck2_data;
    use buck2_data::BuildCommandStart;
    use buck2_util::future::try_join_all;
    use dupe::Dupe;
    use futures::stream;
    use once_cell::sync::Lazy;

    use futures::Stream;
    use futures::StreamExt;
    use tonic::metadata;
    use tonic::metadata::MetadataKey;
    use tonic::metadata::MetadataValue;
    use tonic::service::interceptor::InterceptedService;
    use tonic::service::Interceptor;
    use tonic::transport::Channel;
    use tonic::transport::channel::ClientTlsConfig;
    use tonic::Request;

    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::UnboundedSender;

    use tokio_stream::wrappers::UnboundedReceiverStream;

    use bazel_event_publisher_proto::google::devtools::build::v1::OrderedBuildEvent;
    use bazel_event_publisher_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
    use bazel_event_publisher_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
    use bazel_event_publisher_proto::google::devtools::build::v1::StreamId;

    use prost;
    use prost::Message;
    use prost_types;

    use regex::Regex;

    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;

    pub struct RemoteEventSink {
        _handler: JoinHandle<()>,
        send: UnboundedSender<Vec<BuckEvent>>,
    }

    // TODO[AH] re-use definitions from REOSS crate.
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct HttpHeader {
        pub key: String,
        pub value: String,
    }

    impl FromStr for HttpHeader {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let mut iter = s.split(':');
            match (iter.next(), iter.next(), iter.next()) {
                (Some(key), Some(value), None) => Ok(Self {
                    key: key.trim().to_owned(),
                    value: value.trim().to_owned(),
                }),
                _ => Err(anyhow::anyhow!(
                    "Invalid header (expect exactly one `:`): `{}`",
                    s
                )),
            }
        }
    }

    /// Replace occurrences of $FOO in a string with the value of the env var $FOO.
    fn substitute_env_vars(s: &str) -> anyhow::Result<String> {
        substitute_env_vars_impl(s, |v| std::env::var(v))
    }

    fn substitute_env_vars_impl(
        s: &str,
        getter: impl Fn(&str) -> Result<String, VarError>,
    ) -> anyhow::Result<String> {
        static ENV_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("\\$[a-zA-Z_][a-zA-Z_0-9]*").unwrap());

        let mut out = String::with_capacity(s.len());
        let mut last_idx = 0;

        for mat in ENV_REGEX.find_iter(s) {
            out.push_str(&s[last_idx..mat.start()]);
            let var = &mat.as_str()[1..];
            let val = getter(var).with_context(|| format!("Error substituting `{}`", mat.as_str()))?;
            out.push_str(&val);
            last_idx = mat.end();
        }

        if last_idx < s.len() {
            out.push_str(&s[last_idx..s.len()]);
        }

        Ok(out)
    }

    #[derive(Clone, Dupe)]
    struct InjectHeadersInterceptor {
        headers: Arc<Vec<(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)>>,
    }

    impl InjectHeadersInterceptor {
        pub fn new(headers: &[HttpHeader]) -> anyhow::Result<Self> {
            let headers = headers
                .iter()
                .map(|h| {
                    // This means we can't have `$` in a header key or value, which isn't great. On the
                    // flip side, env vars are good for things like credentials, which those headers
                    // are likely to contain. In time, we should allow escaping.
                    let key = substitute_env_vars(&h.key)?;
                    let value = substitute_env_vars(&h.value)?;

                    let key = MetadataKey::<metadata::Ascii>::from_bytes(key.as_bytes())
                        .with_context(|| format!("Invalid key in header: `{}: {}`", key, value))?;

                    let value = MetadataValue::try_from(&value)
                        .with_context(|| format!("Invalid value in header: `{}: {}`", key, value))?;

                    anyhow::Ok((key, value))
                })
                .collect::<Result<_, _>>()
                .context("Error converting headers")?;

            Ok(Self {
                headers: Arc::new(headers),
            })
        }
    }

    impl Interceptor for InjectHeadersInterceptor {
        fn call(
            &mut self,
            mut request: tonic::Request<()>,
        ) -> Result<tonic::Request<()>, tonic::Status> {
            for (k, v) in self.headers.iter() {
                request.metadata_mut().insert(k.clone(), v.clone());
            }
            Ok(request)
        }
    }

    type GrpcService = InterceptedService<Channel, InjectHeadersInterceptor>;

    async fn connect_build_event_server() -> anyhow::Result<PublishBuildEventClient<GrpcService>> {
        let uri = std::env::var("BES_URI")?.parse()?;
        let mut channel = Channel::builder(uri);
        let tls_config = ClientTlsConfig::new();
        {
            let tls_setting = std::env::var("BES_TLS").unwrap_or("0".to_owned());
            match tls_setting.as_str() {
                "1" | "true" => {
                    channel = channel.tls_config(tls_config)?;
                },
                _ => {},
            }
        }
        // TODO: parse PEM
        let endpoint = channel
            .connect()
            .await
            .context("connecting to Bazel event stream gRPC server")?;
        let mut headers = vec![];
        for hdr in std::env::var("BES_HEADERS").unwrap_or("".to_owned()).split(",") {
            let hdr = hdr.trim();
            if !hdr.is_empty() {
                headers.push(HttpHeader::from_str(hdr)?);
            }
        };
        let interceptor = InjectHeadersInterceptor::new(&headers)?;
        let client = PublishBuildEventClient::with_interceptor(endpoint, interceptor);
        Ok(client)
    }

    fn buck_to_bazel_events<S: Stream<Item = BuckEvent>>(events: S) -> impl Stream<Item = v1::BuildEvent> {
        let mut target_actions: HashMap<(String, String), Vec<(BuildEventId, bool)>> = HashMap::new();
        stream! {
            for await event in events {
                //println!("EVENT {:?} {:?}", event.event.trace_id, event);
                match event.data() {
                    buck2_data::buck_event::Data::SpanStart(start) => {
                        //println!("START {:?}", start);
                        match start.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_start_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_start::Data::Build(BuildCommandStart {})) => {
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::Started(build_event_stream::build_event_id::BuildStartedId {})) }),
                                            children: vec![],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Started(build_event_stream::BuildStarted {
                                                uuid: event.event.trace_id.clone(),
                                                start_time_millis: 0,
                                                start_time: Some(event.timestamp().into()),
                                                build_tool_version: "BUCK2".to_owned(),
                                                options_description: "UNKNOWN".to_owned(),
                                                command: "build".to_owned(),
                                                working_directory: "UNKNOWN".to_owned(),
                                                workspace_directory: "UNKNOWN".to_owned(),
                                                server_pid: std::process::id() as i64,
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };
                                    },
                                    Some(_) => {},
                                }
                            },
                            Some(buck2_data::span_start_event::Data::Analysis(analysis)) => {
                                let label = match analysis.target.as_ref() {
                                    None => None,
                                    Some(buck2_data::analysis_start::Target::StandardTarget(label)) =>
                                        label.label.as_ref().map(|label| format!("{}:{}", label.package, label.name)),
                                    Some(buck2_data::analysis_start::Target::AnonTarget(_anon)) => None, // TODO
                                    Some(buck2_data::analysis_start::Target::DynamicLambda(_owner)) => None, // TODO
                                };
                                match label {
                                    None => {},
                                    Some(label) => {
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::TargetConfigured(build_event_id::TargetConfiguredId {
                                                label: label.clone(),
                                                aspect: "".to_owned(),
                                            })) }),
                                            children: vec![],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Configured(bazel_event_publisher_proto::build_event_stream::TargetConfigured {
                                                target_kind: "UNKNOWN".to_owned(),
                                                test_size: 0,
                                                tag: vec![],
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };

                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::Pattern(build_event_id::PatternExpandedId {
                                                pattern: vec![label.clone()],
                                            })) }),
                                            children: vec![
                                                build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::TargetConfigured(bazel_event_publisher_proto::build_event_stream::build_event_id::TargetConfiguredId {
                                                    label: label,
                                                    aspect: "".to_owned(),
                                                }))},
                                            ],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Expanded(bazel_event_publisher_proto::build_event_stream::PatternExpanded {
                                                test_suite_expansions: vec![],
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };
                                    },
                                }
                            },
                            Some(_) => {},
                        }
                    },
                    buck2_data::buck_event::Data::SpanEnd(end) => {
                        //println!("END   {:?}", end);
                        match end.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_end_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_end::Data::Build(_build)) => {
                                        // flush the target completed map.
                                        for ((label, config), actions) in target_actions.into_iter() {
                                            let success = actions.iter().all(|(_, success)| *success);
                                            let children: Vec<_> = actions.into_iter().map(|(id, _)| id).collect();
                                            let bes_event = build_event_stream::BuildEvent {
                                                id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::TargetCompleted(build_event_id::TargetCompletedId {
                                                    label: label,
                                                    configuration: Some(build_event_id::ConfigurationId { id: config }),
                                                    aspect: "".to_owned(),
                                                })) }),
                                                children: children,
                                                last_message: false,
                                                payload: Some(build_event_stream::build_event::Payload::Completed(build_event_stream::TargetComplete {
                                                    success: success,
                                                    target_kind: "".to_owned(),
                                                    test_size: 0,
                                                    output_group: vec![],
                                                    important_output: vec![],
                                                    directory_output: vec![],
                                                    tag: vec![],
                                                    test_timeout_seconds: 0,
                                                    test_timeout: None,
                                                    failure_detail: None,
                                                })),
                                            };
                                            let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                                type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                                value: bes_event.encode_to_vec(),
                                            });
                                            yield v1::BuildEvent {
                                                event_time: Some(event.timestamp().into()),
                                                event: Some(bazel_event),
                                            };
                                        }

                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::BuildFinished(build_event_stream::build_event_id::BuildFinishedId {})) }),
                                            children: vec![],
                                            last_message: true,
                                            payload: Some(build_event_stream::build_event::Payload::Finished(build_event_stream::BuildFinished {
                                                overall_success: command.is_success,
                                                exit_code: Some(
                                                    if command.is_success {
                                                        build_event_stream::build_finished::ExitCode {
                                                            name: "SUCCESS".to_owned(),
                                                            code: 0,
                                                        }
                                                    } else {
                                                        build_event_stream::build_finished::ExitCode {
                                                            name: "FAILURE".to_owned(),
                                                            code: 1,
                                                        }
                                                    }),
                                                finish_time_millis: 0,
                                                finish_time: Some(event.timestamp().into()),
                                                anomaly_report: None,
                                                // TODO: convert Buck2 ErrorReport
                                                failure_detail: None,
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };
                                        break;
                                    },
                                    Some(_) => {},
                                }
                            },
                            Some(buck2_data::span_end_event::Data::ActionExecution(action)) => {
                                let configuration = match &action.key {
                                    None => None,
                                    Some(key) => match &key.owner {
                                        None => None,
                                        Some(owner) => match owner {
                                           buck2_data::action_key::Owner::TargetLabel(target) => target.configuration.clone(),
                                           buck2_data::action_key::Owner::TestTargetLabel(test) => test.configuration.clone(),
                                           buck2_data::action_key::Owner::LocalResourceSetup(resource) => resource.configuration.clone(),
                                           buck2_data::action_key::Owner::AnonTarget(_anon) => None, // TODO: execution configuration?
                                           buck2_data::action_key::Owner::BxlKey(_bxl) => None,
                                        },
                                    },
                                }.map(|configuration| build_event_id::ConfigurationId { id: configuration.full_name.clone() });
                                let label = match &action.key {
                                    None => None,
                                    Some(key) => match &key.owner {
                                        None => None,
                                        Some(owner) => match owner {
                                           buck2_data::action_key::Owner::TargetLabel(target) => target.label.clone(),
                                           buck2_data::action_key::Owner::TestTargetLabel(test) => test.label.clone(),
                                           buck2_data::action_key::Owner::LocalResourceSetup(resource) => resource.label.clone(),
                                           buck2_data::action_key::Owner::AnonTarget(anon) => anon.name.clone(),
                                           buck2_data::action_key::Owner::BxlKey(_bxl) => None, // TODO: handle bxl
                                        },
                                    },
                                }.map(|label| format!("{}:{}", label.package, label.name));
                                let action_id = BuildEventId {id: Some(build_event_id::Id::ActionCompleted(build_event_id::ActionCompletedId {
                                    configuration: configuration.clone(),
                                    label: label.clone().unwrap_or("UNKOWN".to_owned()),
                                    primary_output: "UNKNOWN".to_owned(),
                                }))};
                                let mnemonic = action.name.as_ref().map(|name| name.category.clone()).unwrap_or("UNKNOWN".to_owned());
                                let success = !action.failed;
                                let last_command_details = action.commands.last().and_then(|command| command.details.as_ref());
                                let command_line: Vec<String> = match last_command_details.and_then(|command| command.command_kind.as_ref()).and_then(|kind| kind.command.as_ref()) {
                                    None => vec![],
                                    Some(buck2_data::command_execution_kind::Command::LocalCommand(command)) => command.argv.clone(),
                                    Some(_) => vec![], // TODO: handle remote, worker, and other commands
                                };
                                let exit_code = last_command_details.and_then(|details| details.signed_exit_code).unwrap_or(0);
                                let stdout = last_command_details.map(|details| details.stdout.clone());
                                let stderr = last_command_details.map(|details| details.stderr.clone());
                                let stdout_file = stdout.map(|stdout| bazel_event_publisher_proto::build_event_stream::File {
                                    path_prefix: vec![],
                                    name: "stdout".to_owned(),
                                    digest: "".to_owned(),
                                    length: stdout.len() as i64,
                                    file: Some(bazel_event_publisher_proto::build_event_stream::file::File::Contents(stdout.into())),
                                });
                                let stderr_file = stderr.clone().map(|stderr| bazel_event_publisher_proto::build_event_stream::File {
                                    path_prefix: vec![],
                                    name: "stderr".to_owned(),
                                    digest: "".to_owned(),
                                    length: stderr.len() as i64,
                                    file: Some(bazel_event_publisher_proto::build_event_stream::file::File::Contents(stderr.into())),
                                });
                                let start_time = last_command_details.and_then(|details| details.metadata.as_ref().and_then(|metadata| metadata.start_time.clone()));
                                //let wall_time = last_command_details.and_then(|details| details.metadata.as_ref().and_then(|metadata| metadata.wall_time.clone()));
                                //let end_time = ...; // TODO: add start_time and wall_time
                                match (label.as_ref(), configuration.as_ref()) {
                                    (Some(label), Some(configuration)) => {
                                        target_actions
                                            .entry((label.clone(), configuration.id.clone()))
                                            .or_default()
                                            .push((action_id.clone(), success));
                                    },
                                    _ => {},
                                }
                                let failure_detail = if success { None } else {
                                    Some(bazel_event_publisher_proto::failure_details::FailureDetail {
                                        message: stderr.unwrap_or("UNKNOWN".to_owned()),
                                        category: None, // TODO
                                    })
                                };
                                let bes_event = build_event_stream::BuildEvent {
                                    id: Some(action_id),
                                    children: vec![],
                                    last_message: false,
                                    payload: Some(build_event_stream::build_event::Payload::Action(build_event_stream::ActionExecuted {
                                        success: success,
                                        r#type: mnemonic,
                                        exit_code: exit_code,
                                        stdout: stdout_file,
                                        stderr: stderr_file,
                                        label: "".to_owned(),
                                        configuration: None,
                                        primary_output: None,
                                        command_line: command_line,
                                        action_metadata_logs: vec![],
                                        failure_detail: failure_detail,
                                        start_time: start_time, // TODO: should we deduct queue time?
                                        end_time: None,
                                        strategy_details: vec![],
                                    })),
                                };
                                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                    value: bes_event.encode_to_vec(),
                                });
                                yield v1::BuildEvent {
                                    event_time: Some(event.timestamp().into()),
                                    event: Some(bazel_event),
                                };
                            },
                            Some(_) => {},
                        }
                    },
                    buck2_data::buck_event::Data::Instant(_instant) => {
                        //println!("INST  {:?}", instant);
                    },
                    buck2_data::buck_event::Data::Record(_record) => {
                        //println!("REC   {:?}", record);
                    },
                }
            }
        }
    }

    fn stream_build_tool_events<S: Stream<Item = v1::BuildEvent>>(trace_id: String, events: S) -> impl Stream<Item = PublishBuildToolEventStreamRequest> {
        stream::iter(1..)
            .zip(events)
            .map(move |(sequence_number, event)| {
                PublishBuildToolEventStreamRequest {
                    check_preceding_lifecycle_events_present: false,
                    notification_keywords: vec![],
                    ordered_build_event: Some(OrderedBuildEvent {
                        stream_id: Some(StreamId {
                            build_id: trace_id.clone(),
                            invocation_id: trace_id.clone(),
                            component: 0,
                        }),
                        sequence_number,
                        event: Some(event),
                    }),
                    project_id: "12341234".to_owned(), // TODO: needed
                }
            })
    }

    async fn event_sink_loop(recv: UnboundedReceiver<Vec<BuckEvent>>) -> anyhow::Result<()> {
        let mut handlers: HashMap<String, (UnboundedSender<BuckEvent>, tokio::task::JoinHandle<anyhow::Result<()>>)> = HashMap::new();
        let client = connect_build_event_server().await?;
        let mut recv = UnboundedReceiverStream::new(recv)
            .flat_map(|v|stream::iter(v));
        let result_uri = std::env::var("BES_RESULT").ok();
        while let Some(event) = recv.next().await {
            //let dbg_trace_id = event.event.trace_id.clone();
            //println!("event_sink_loop event {:?}", &dbg_trace_id);
            if let Some((send, _)) = handlers.get(&event.event.trace_id) {
                //println!("event_sink_loop redirect {:?}", &dbg_trace_id);
                send.send(event).unwrap_or_else(|e| println!("build event send failed {:?}", e));
            } else {
                //println!("event_sink_loop new handler {:?}", event.event.trace_id);
                let (send, recv) = mpsc::unbounded_channel::<BuckEvent>();
                let mut client = client.clone();
                let result_uri = result_uri.clone();
                //let dbg_trace_id = dbg_trace_id.clone();
                let trace_id = event.event.trace_id.clone();
                let handler = tokio::spawn(async move {
                    let recv = UnboundedReceiverStream::new(recv);
                    let request = Request::new(stream_build_tool_events(trace_id.clone(), buck_to_bazel_events(recv)));
                    if let Some(result_uri) = result_uri.as_ref() {
                        println!("BES results: {}{}", &result_uri, &trace_id);
                    }
                    //println!("BES request {:?}", &dbg_trace_id);
                    let response = client.publish_build_tool_event_stream(request).await?;
                    //println!("BES response {:?}", &dbg_trace_id);
                    let mut inbound = response.into_inner();
                    while let Some(_ack) = inbound.message().await? {
                        // TODO: Handle ACKs properly and add retry.
                        //println!("ACK  {:?}", ack);
                    }
                    if let Some(result_uri) = result_uri.as_ref() {
                        println!("BES results: {}{}", &result_uri, &trace_id);
                    }
                    Ok(())
                });
                handlers.insert(event.event.trace_id.to_owned(), (send, handler));
            }
        }
        //println!("event_sink_loop recv CLOSED");
        // TODO: handle closure and retry.
        // close send handles and await all handlers.
        let handlers: Vec<tokio::task::JoinHandle<anyhow::Result<()>>> = handlers.into_values().map(|(_, handler)|handler).collect();
        // TODO: handle retry.
        try_join_all(handlers).await?.into_iter().collect::<anyhow::Result<Vec<()>>>()?;
        Ok(())
    }

    impl RemoteEventSink {
        pub fn new() -> anyhow::Result<Self> {
            let (send, recv) = mpsc::unbounded_channel::<Vec<BuckEvent>>();
            let handler = std::thread::Builder::new()
                .name("buck-event-producer".to_owned())
                .spawn({
                    move || {
                        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
                        runtime.block_on(event_sink_loop(recv)).unwrap();
                    }
                }).context("spawning buck-event-producer thread")?;
            Ok(RemoteEventSink {
                _handler: handler,
                send,
            })
        }
        pub async fn send_now(&self, event: BuckEvent) {
            self.send_messages_now(vec![event]).await;
        }
        pub async fn send_messages_now(&self, events: Vec<BuckEvent>) {
            // TODO: does this make sense for BES? If so, implement send now variant.
            if let Err(err) = self.send.send(events) {
                // TODO: proper error handling
                dbg!(err);
            }
        }
        pub fn offer(&self, event: BuckEvent) {
            if let Err(err) = self.send.send(vec![event]) {
                // TODO: proper error handling
                dbg!(err);
            }
        }
    }

    impl EventSink for RemoteEventSink {
        fn send(&self, event: Event) {
            match event {
                Event::Buck(event) => {
                    self.offer(event);
                }
                Event::CommandResult(..) => {},
                Event::PartialResult(..) => {},
            }
        }
    }

    impl EventSinkWithStats for RemoteEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as _
        }

        fn stats(&self) -> EventSinkStats {
            EventSinkStats {
                successes: 0,
                failures_invalid_request: 0,
                failures_unauthorized: 0,
                failures_rate_limited: 0,
                failures_pushed_back: 0,
                failures_enqueue_failed: 0,
                failures_internal_error: 0,
                failures_timed_out: 0,
                failures_unknown: 0,
                buffered: 0,
                dropped: 0,
                bytes_written: 0,
            }
        }
    }
}

pub use fbcode::*;

fn new_remote_event_sink_if_fbcode(
    fb: FacebookInit,
    buffer_size: usize,
    retry_backoff: Duration,
    retry_attempts: usize,
    message_batch_size: Option<usize>,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    #[cfg(fbcode_build)]
    {
        Ok(Some(RemoteEventSink::new(
            fb,
            scribe_category()?,
            buffer_size,
            retry_backoff,
            retry_attempts,
            message_batch_size,
        )?))
    }
    #[cfg(not(fbcode_build))]
    {
        let _ = (
            fb,
            buffer_size,
            retry_backoff,
            retry_attempts,
            message_batch_size,
        );
        match std::env::var("BES_URI") {
          Ok(_) => Ok(Some(RemoteEventSink::new()?)),
          _ => Ok(None),
        }
    }
}

pub fn new_remote_event_sink_if_enabled(
    fb: FacebookInit,
    buffer_size: usize,
    retry_backoff: Duration,
    retry_attempts: usize,
    message_batch_size: Option<usize>,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    if is_enabled() {
        new_remote_event_sink_if_fbcode(
            fb,
            buffer_size,
            retry_backoff,
            retry_attempts,
            message_batch_size,
        )
    } else {
        Ok(None)
    }
}

/// Whether or not remote event logging is enabled for this process. It must be explicitly disabled via `disable()`.
static REMOTE_EVENT_SINK_ENABLED: AtomicBool = AtomicBool::new(true);

/// Returns whether this process should actually write to remote sink, even if it is fully supported by the platform and
/// binary.
pub fn is_enabled() -> bool {
    REMOTE_EVENT_SINK_ENABLED.load(Ordering::Relaxed)
}

/// Disables remote event logging for this process. Remote event logging must be disabled explicitly on startup, otherwise it is
/// on by default.
pub fn disable() {
    REMOTE_EVENT_SINK_ENABLED.store(false, Ordering::Relaxed);
}
