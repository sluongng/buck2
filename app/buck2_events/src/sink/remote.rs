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

use buck2_data;
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

    use super::prepare_event;
    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;
    use crate::TraceId;
    use crate::metadata;
    use crate::schedule_type::ScheduleType;
    use crate::sink::smart_truncate_event::smart_truncate_event;

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

            prepare_event(&mut proto);

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
                use buck2_data::ActionExecutionKind;
                use buck2_data::span_end_event::Data;

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
    use std::env::VarError;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::time::Duration;

    use allocative::Allocative;
    use bazel_event_publisher_proto::google::devtools::build::v1;
    use bazel_event_publisher_proto::google::devtools::build::v1::OrderedBuildEvent;
    use bazel_event_publisher_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
    use bazel_event_publisher_proto::google::devtools::build::v1::PublishLifecycleEventRequest;
    use bazel_event_publisher_proto::google::devtools::build::v1::StreamId;
    use bazel_event_publisher_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
    use bazel_event_publisher_proto::google::devtools::build::v1::publish_lifecycle_event_request::ServiceLevel;
    use bazel_event_publisher_proto::google::devtools::build::v1::stream_id::BuildComponent;
    use buck2_error::BuckErrorContext;
    use buck2_error::conversion::from_any_with_tag;
    use dupe::Dupe;
    use futures::StreamExt;
    use futures::stream;
    use once_cell::sync::Lazy;
    use prost;
    use prost::Message;
    use prost_types;
    use regex::Regex;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::Request;
    use tonic::metadata;
    use tonic::metadata::MetadataKey;
    use tonic::metadata::MetadataValue;
    use tonic::service::Interceptor;
    use tonic::service::interceptor::InterceptedService;
    use tonic::transport::Channel;
    use tonic::transport::channel::ClientTlsConfig;

    use super::prepare_event;
    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;
    use crate::sink::smart_truncate_event::smart_truncate_event;

    // TODO[AH] re-use definitions from REOSS crate.
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct HttpHeader {
        pub key: String,
        pub value: String,
    }

    impl FromStr for HttpHeader {
        type Err = buck2_error::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let mut iter = s.split(':');
            match (iter.next(), iter.next(), iter.next()) {
                (Some(key), Some(value), None) => Ok(Self {
                    key: key.trim().to_owned(),
                    value: value.trim().to_owned(),
                }),
                _ => Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Tier0,
                    "Invalid header (expect exactly one `:`): `{}`",
                    s
                )),
            }
        }
    }

    /// Replace occurrences of $FOO in a string with the value of the env var $FOO.
    fn substitute_env_vars(s: &str) -> buck2_error::Result<String> {
        substitute_env_vars_impl(s, |v| std::env::var(v))
    }

    fn substitute_env_vars_impl(
        s: &str,
        getter: impl Fn(&str) -> Result<String, VarError>,
    ) -> buck2_error::Result<String> {
        static ENV_REGEX: Lazy<Regex> =
            Lazy::new(|| Regex::new("\\$[a-zA-Z_][a-zA-Z_0-9]*").unwrap());

        let mut out = String::with_capacity(s.len());
        let mut last_idx = 0;

        for mat in ENV_REGEX.find_iter(s) {
            out.push_str(&s[last_idx..mat.start()]);
            let var = &mat.as_str()[1..];
            let val = getter(var)
                .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))
                .with_buck_error_context(|| format!("Error substituting: `{}`", mat.as_str()))?;
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
        pub fn new(headers: &[HttpHeader]) -> buck2_error::Result<Self> {
            let headers = headers
                .iter()
                .map(|h| {
                    // This means we can't have `$` in a header key or value, which isn't great. On the
                    // flip side, env vars are good for things like credentials, which those headers
                    // are likely to contain. In time, we should allow escaping.
                    let key = substitute_env_vars(&h.key)?;
                    let value = substitute_env_vars(&h.value)?;

                    let key = MetadataKey::<metadata::Ascii>::from_bytes(key.as_bytes())
                        .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))
                        .with_buck_error_context(|| {
                            format!("Invalid key in header: `{}: {}`", key, value)
                        })?;

                    let value = MetadataValue::try_from(&value)
                        .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))
                        .with_buck_error_context(|| {
                            format!("Invalid value in header: `{}: {}`", key, value)
                        })?;

                    buck2_error::Ok((key, value))
                })
                .collect::<Result<Vec<_>, _>>()
                .with_buck_error_context(|| "Error converting headers")?;

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

    fn connect_build_event_server(
        bes_uri: String,
    ) -> buck2_error::Result<PublishBuildEventClient<GrpcService>> {
        let mut channel = Channel::builder(bes_uri.parse()?)
            .connect_timeout(Duration::from_secs(600))
            .keep_alive_while_idle(true);
        let tls_config = match std::env::var("BES_TLS") {
            Ok(tls_setting) => match tls_setting.as_str() {
                "1" | "true" => Some(ClientTlsConfig::new()),
                _ => None,
            },
            Err(_) => None,
        };
        if let Some(tls_config) = tls_config {
            channel = channel.tls_config(tls_config)?;
        }
        // TODO: parse PEM
        let mut headers = vec![];
        for hdr in std::env::var("BES_HEADERS")
            .unwrap_or("".to_owned())
            .split(",")
        {
            let hdr = hdr.trim();
            if !hdr.is_empty() {
                headers.push(HttpHeader::from_str(hdr)?);
            }
        }
        let interceptor = InjectHeadersInterceptor::new(&headers)?;
        let client = PublishBuildEventClient::with_interceptor(channel.connect_lazy(), interceptor);
        Ok(client)
    }

    static BES_PRODUCER: OnceLock<Arc<BesProducer>> = OnceLock::new();

    struct BesProducer {
        // For streaming events asynchronously via PublishBuildToolEventStream
        sender: UnboundedSender<Vec<BuckEvent>>,
        // For sending events directly with PublishLifecycleEvent
        client: PublishBuildEventClient<GrpcService>,
    }

    impl BesProducer {
        async fn event_sink_loop(
            &'static self,
            receiver: UnboundedReceiver<Vec<BuckEvent>>,
        ) -> buck2_error::Result<()> {
            let result_uri = std::env::var("BES_RESULT").ok();
            let result_uri_clone = result_uri.clone();

            let buck2_event_stream = UnboundedReceiverStream::new(receiver)
                .flat_map(|v| stream::iter(v))
                .map(|mut buck_event| {
                    smart_truncate_event(buck_event.data_mut());
                    let mut buck_event_proto: Box<buck2_data::BuckEvent> = buck_event.into();
                    prepare_event(&mut buck_event_proto);
                    buck_event_proto
                })
                .zip(stream::iter(1..))
                .map(move |(buck_event_proto, sequence_number)| {
                    if sequence_number == 1 {
                        if let Some(result_uri) = result_uri_clone.as_ref() {
                            println!("BES results: {}{}", &result_uri, buck_event_proto.trace_id);
                        }
                    }
                    let req = PublishBuildToolEventStreamRequest {
                        check_preceding_lifecycle_events_present: false,
                        notification_keywords: vec![],
                        ordered_build_event: Some(OrderedBuildEvent {
                            stream_id: Some(StreamId {
                                build_id: buck_event_proto.trace_id.clone(),
                                invocation_id: buck_event_proto.trace_id.clone(),
                                component: BuildComponent::UnknownComponent.into(),
                            }),
                            sequence_number,
                            event: Some(v1::BuildEvent {
                                event_time: buck_event_proto.timestamp.clone(),
                                event: Some(v1::build_event::Event::BuckEvent(prost_types::Any {
                                    type_url: "type.googleapis.com/buck.data.BuckEvent".to_owned(),
                                    value: buck_event_proto.encode_to_vec(),
                                })),
                            }),
                        }),
                        project_id: "buck2".to_owned(),
                    };

                    req
                });

            println!("About to stream");
            let mut resp_stream = match self
                .client
                .clone()
                .publish_build_tool_event_stream(Request::new(buck2_event_stream))
                .await
            {
                Ok(resp) => {
                    println!("stream ok");
                    resp.into_inner()
                }
                Err(e) => {
                    println!("Error streaming event: {:?}", e);
                    return Err(from_any_with_tag(e, buck2_error::ErrorTag::Tier0));
                }
            };
            let mut trace_id: Option<String> = None;
            println!("starting ack loop");
            loop {
                match resp_stream.message().await {
                    Ok(Some(ack)) => {
                        // ack is not used
                        if let (None, Some(stream_id)) = (&trace_id, &ack.stream_id) {
                            trace_id = Some(stream_id.invocation_id.clone());
                        }
                        println!("ack: {}", ack.sequence_number);
                    }
                    Ok(None) => {
                        println!("No ack message");
                        break;
                    }
                    Err(e) => {
                        // TODO: implement retry here
                        println!("Error getting ack: {:?}", e);
                        return Err(from_any_with_tag(e, buck2_error::ErrorTag::Tier0));
                    }
                }
            }
            println!("Finish waiting ack");

            if let (Some(result_uri), Some(trace_id)) = (result_uri, trace_id) {
                println!("BES results: {}{}", result_uri, trace_id);
            }

            Ok(())
        }
    }

    #[derive(Clone)]
    pub struct RemoteEventSink {
        producer: &'static BesProducer,
        handler: Arc<tokio::task::JoinHandle<()>>,
    }

    impl RemoteEventSink {
        pub fn new(bes_uri: String) -> buck2_error::Result<Self> {
            let (sender, receiver) = mpsc::unbounded_channel::<Vec<BuckEvent>>();
            let producer = &**BES_PRODUCER.get_or_try_init(|| -> buck2_error::Result<_> {
                let client: PublishBuildEventClient<GrpcService> =
                    connect_build_event_server(bes_uri)?;
                let producer = BesProducer { sender, client };

                Ok(Arc::new(producer))
            })?;

            let handler = tokio::spawn(async move {
                producer
                    .event_sink_loop(receiver)
                    .await
                    .expect("Event sink loop failed");
            });

            Ok(RemoteEventSink {
                producer,
                handler: Arc::new(handler),
            })
        }

        pub async fn send_now(&self, event: BuckEvent) {
            let mut client = self.producer.client.clone();
            let _ = client
                .publish_lifecycle_event(Request::new(PublishLifecycleEventRequest {
                    service_level: ServiceLevel::Interactive.into(),
                    project_id: "buck2".to_owned(),
                    stream_timeout: None,
                    notification_keywords: vec![],
                    check_preceding_lifecycle_events_present: false,
                    build_event: Some(OrderedBuildEvent {
                        stream_id: Some(StreamId {
                            build_id: event.event().trace_id.clone(),
                            invocation_id: event.event().trace_id.clone(),
                            component: BuildComponent::UnknownComponent.into(),
                        }),
                        sequence_number: 0,
                        event: Some(v1::BuildEvent {
                            event_time: event.event().timestamp.clone(),
                            event: Some(v1::build_event::Event::BuckEvent(prost_types::Any {
                                type_url: "type.googleapis.com/buck.data.BuckEvent".to_owned(),
                                value: event.event().encode_to_vec(),
                            })),
                        }),
                    }),
                }))
                .await;
        }
        pub async fn send_messages_now(&self, events: Vec<BuckEvent>) {
            for event in events {
                self.send_now(event).await;
            }
        }
        pub fn offer(&self, event: BuckEvent) {
            if let Err(e) = self.producer.sender.send(vec![event]) {
                println!("Error sending event to channel: {:?}", e);
            }
        }
    }

    impl Drop for RemoteEventSink {
        fn drop(&mut self) {
            if Arc::strong_count(&self.handler) == 1 {
                // Take ownership of the task and block until completion
                let task = Arc::into_inner(self.handler.clone()).unwrap();
                let _ = futures::executor::block_on(task);
            }
        }
    }

    impl EventSink for RemoteEventSink {
        fn send(&self, event: Event) {
            match event {
                Event::Buck(event) => {
                    self.offer(event);
                }
                Event::CommandResult(..) => {}
                Event::PartialResult(..) => {}
            }
        }
    }

    impl EventSinkWithStats for RemoteEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as Arc<dyn EventSink>
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
                        let buck2_data::command_execution_kind::Command::RemoteCommand(ref remote) =
                            command
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
        if let Ok(bes_uri) = std::env::var("BES_URI") {
            Ok(Some(RemoteEventSink::new(bes_uri)?))
        } else {
            Ok(None)
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
