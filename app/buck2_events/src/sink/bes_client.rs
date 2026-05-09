/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::HashMap;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use bes_grpc_proto::google::devtools::build::v1::BuildEvent;
use bes_grpc_proto::google::devtools::build::v1::OrderedBuildEvent;
use bes_grpc_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
use bes_grpc_proto::google::devtools::build::v1::PublishBuildToolEventStreamResponse;
use bes_grpc_proto::google::devtools::build::v1::StreamId;
use bes_grpc_proto::google::devtools::build::v1::build_event;
use bes_grpc_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
use bes_grpc_proto::google::devtools::build::v1::stream_id;
use buck2_data::buck_event;
use buck2_data::record_event;
use buck2_data::span_end_event;
use buck2_error::ErrorTag;
use fbinit::FacebookInit;
use prost::Message as _;
use prost_types::Any;
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataValue;

use crate::sink::bazel_converter::BazelEventConverter;
use crate::sink::bazel_converter::encode_bep_event;

const BUCK2_EVENT_TYPE_URL: &str = "type.googleapis.com/buck.data.BuckEvent";
const DEFAULT_BATCH_SIZE: usize = 1;
const CLOSE_ACK_TIMEOUT_MULTIPLIER: u32 = 30;
const MIN_CLOSE_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND_END_CLOSE_GRACE: Duration = Duration::from_millis(500);
const COMMAND_END_CLOSE_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub struct BesConfig {
    pub buffer_size: usize,
    pub retry_backoff: Duration,
    pub retry_attempts: usize,
    pub message_batch_size: Option<usize>,
    pub grpc_timeout: Duration,
    pub bes_backend: Option<String>,
    pub bes_headers: Vec<(String, String)>,
    pub event_format: BesEventFormat,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BesEventFormat {
    #[default]
    Buck,
    Bazel,
}

impl Default for BesConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10_000,
            retry_backoff: Duration::from_millis(500),
            retry_attempts: 5,
            message_batch_size: None,
            grpc_timeout: Duration::from_secs(2),
            bes_backend: None,
            bes_headers: Vec::new(),
            event_format: BesEventFormat::Buck,
        }
    }
}

impl BesConfig {
    pub(crate) fn bes_enabled(&self) -> bool {
        self.bes_backend
            .as_deref()
            .is_some_and(|backend| !backend.trim().is_empty())
    }
}

impl FromStr for BesEventFormat {
    type Err = buck2_error::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "buck" => Ok(Self::Buck),
            "bazel" => Ok(Self::Bazel),
            value => Err(buck2_error::buck2_error!(
                ErrorTag::Input,
                "Invalid `bes.event_format` value `{}` (expected `buck` or `bazel`)",
                value
            )),
        }
    }
}

pub struct Message {
    pub category: String,
    pub message: Vec<u8>,
    pub message_key: Option<i64>,
}

struct SendNowRequest {
    messages: Vec<Message>,
    done: oneshot::Sender<buck2_error::Result<()>>,
}

#[derive(Clone, Debug, Default)]
pub struct Counters {
    pub successes: u64,
    pub failures_invalid_request: u64,
    pub failures_unauthorized: u64,
    pub failures_rate_limited: u64,
    pub failures_pushed_back: u64,
    pub failures_enqueue_failed: u64,
    pub failures_internal_error: u64,
    pub failures_timed_out: u64,
    pub failures_unknown: u64,
    pub queue_depth: u64,
    pub dropped: u64,
    pub bytes_written: u64,
}

#[derive(Default)]
struct CounterState {
    successes: AtomicU64,
    failures_invalid_request: AtomicU64,
    failures_unauthorized: AtomicU64,
    failures_rate_limited: AtomicU64,
    failures_pushed_back: AtomicU64,
    failures_enqueue_failed: AtomicU64,
    failures_internal_error: AtomicU64,
    failures_timed_out: AtomicU64,
    failures_unknown: AtomicU64,
    queue_depth: AtomicU64,
    dropped: AtomicU64,
    bytes_written: AtomicU64,
}

impl CounterState {
    fn inc_success(&self, bytes: u64) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    fn inc_failures_invalid_request(&self) {
        self.failures_invalid_request
            .fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_unauthorized(&self) {
        self.failures_unauthorized.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_rate_limited(&self) {
        self.failures_rate_limited.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_pushed_back(&self) {
        self.failures_pushed_back.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_enqueue_failed(&self) {
        self.failures_enqueue_failed.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_internal_error(&self) {
        self.failures_internal_error.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_timed_out(&self) {
        self.failures_timed_out.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_unknown(&self) {
        self.failures_unknown.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_queue_depth(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_queue_depth(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn inc_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Counters {
        Counters {
            successes: self.successes.load(Ordering::Relaxed),
            failures_invalid_request: self.failures_invalid_request.load(Ordering::Relaxed),
            failures_unauthorized: self.failures_unauthorized.load(Ordering::Relaxed),
            failures_rate_limited: self.failures_rate_limited.load(Ordering::Relaxed),
            failures_pushed_back: self.failures_pushed_back.load(Ordering::Relaxed),
            failures_enqueue_failed: self.failures_enqueue_failed.load(Ordering::Relaxed),
            failures_internal_error: self.failures_internal_error.load(Ordering::Relaxed),
            failures_timed_out: self.failures_timed_out.load(Ordering::Relaxed),
            failures_unknown: self.failures_unknown.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
struct ConnectionConfig {
    endpoint: String,
    headers: Vec<(String, String)>,
}

pub struct BesClient {
    tx: crossbeam_channel::Sender<Message>,
    send_now_tx: crossbeam_channel::Sender<SendNowRequest>,
    counters: Arc<CounterState>,
}

impl BesClient {
    pub fn new(_fb: FacebookInit, config: BesConfig) -> buck2_error::Result<Self> {
        let connection = ConnectionConfig {
            endpoint: bes_backend(config.bes_backend.as_deref())?,
            headers: config.bes_headers.clone(),
        };
        let queue_capacity = config.buffer_size.max(1);
        let (tx, rx) = crossbeam_channel::bounded(queue_capacity);
        let (send_now_tx, send_now_rx) = crossbeam_channel::unbounded();
        let counters = Arc::new(CounterState::default());

        let thread_counters = counters.clone();
        let thread_config = config.clone();
        let thread_connection = connection.clone();

        thread::Builder::new()
            .name("buck2-bes-sink".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        thread_counters.inc_failures_internal_error();
                        return;
                    }
                };

                let mut worker =
                    WorkerState::new(thread_config, thread_connection, thread_counters);
                let batch_size = worker.batch_size();
                let mut send_now_open = true;
                loop {
                    if send_now_open {
                        loop {
                            match send_now_rx.try_recv() {
                                Ok(request) => {
                                    process_send_now_request(&runtime, &mut worker, request);
                                }
                                Err(crossbeam_channel::TryRecvError::Empty) => break,
                                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                                    send_now_open = false;
                                    break;
                                }
                            }
                        }
                    }

                    if send_now_open {
                        crossbeam_channel::select! {
                            recv(send_now_rx) -> request => match request {
                                Ok(request) => {
                                    process_send_now_request(&runtime, &mut worker, request);
                                }
                                Err(_) => {
                                    send_now_open = false;
                                }
                            },
                            recv(rx) -> message => match message {
                                Ok(message) => {
                                    process_queued_message(&runtime, &mut worker, message);
                                    for _ in 1..batch_size {
                                        let mut handled_send_now = false;
                                        loop {
                                            match send_now_rx.try_recv() {
                                                Ok(request) => {
                                                    handled_send_now = true;
                                                    process_send_now_request(
                                                        &runtime,
                                                        &mut worker,
                                                        request,
                                                    );
                                                }
                                                Err(crossbeam_channel::TryRecvError::Empty) => break,
                                                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                                                    send_now_open = false;
                                                    break;
                                                }
                                            }
                                        }
                                        if handled_send_now {
                                            break;
                                        }

                                        let Ok(message) = rx.try_recv() else {
                                            break;
                                        };
                                        process_queued_message(&runtime, &mut worker, message);
                                    }
                                    runtime.block_on(worker.close_due_streams());
                                }
                                Err(_) => break,
                            },
                            default(COMMAND_END_CLOSE_POLL_INTERVAL) => {
                                runtime.block_on(worker.close_due_streams());
                            }
                        }
                    } else {
                        match rx.recv_timeout(COMMAND_END_CLOSE_POLL_INTERVAL) {
                            Ok(message) => {
                                process_queued_message(&runtime, &mut worker, message);
                                for _ in 1..batch_size {
                                    let Ok(message) = rx.try_recv() else {
                                        break;
                                    };
                                    process_queued_message(&runtime, &mut worker, message);
                                }
                                runtime.block_on(worker.close_due_streams());
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                                runtime.block_on(worker.close_due_streams());
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                }
                runtime.block_on(worker.close_all_streams());
            })
            .map_err(|e| {
                buck2_error::buck2_error!(ErrorTag::Tier0, "Failed to start BES worker thread: {e}")
            })?;

        Ok(Self {
            tx,
            send_now_tx,
            counters,
        })
    }

    pub fn offer(&self, message: Message) {
        match self.tx.try_send(message) {
            Ok(()) => {
                self.counters.inc_queue_depth();
            }
            Err(_) => {
                self.counters.inc_failures_enqueue_failed();
                self.counters.inc_dropped();
            }
        }
    }

    // Send through a dedicated priority lane on the background worker and wait
    // for completion. This keeps emergency delivery semantics while reusing the
    // worker's existing per-invocation stream state.
    pub async fn send_messages_now(&self, messages: Vec<Message>) -> buck2_error::Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let (done_tx, done_rx) = oneshot::channel();
        self.send_now_tx
            .send(SendNowRequest {
                messages,
                done: done_tx,
            })
            .map_err(|_| {
                buck2_error::buck2_error!(
                    ErrorTag::Tier0,
                    "Failed to enqueue BES priority send request"
                )
            })?;

        done_rx.await.map_err(|_| {
            buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "BES worker dropped priority send response channel"
            )
        })?
    }

    pub fn export_counters(&self) -> Counters {
        self.counters.snapshot()
    }
}

fn process_queued_message(
    runtime: &tokio::runtime::Runtime,
    worker: &mut WorkerState,
    message: Message,
) {
    worker.counters.dec_queue_depth();
    let _ = runtime.block_on(worker.send_message_with_retry(&message, false));
}

fn process_send_now_request(
    runtime: &tokio::runtime::Runtime,
    worker: &mut WorkerState,
    request: SendNowRequest,
) {
    // Desired behavior (mirroring Bazel's BES uploader semantics):
    //
    // 1) `send_messages_now()` is an emergency path. Returning `Ok(())` means all events from this
    //    request were ACKed by BES.
    // 2) Stream retries must preserve delivery intent for already-enqueued events.
    //
    // Bazel keeps an "unacked queue" and, after reconnect, replays unacked events with their
    // original sequence numbers. This implementation mirrors that strategy per invocation stream:
    // - each stream keeps pending unacked requests in order;
    // - reconnect replays those requests before accepting new progress;
    // - sequence numbers remain stable across reconnects.
    //
    // Because sequence numbers are stable, `send_now` can safely wait for the max sequence per
    // invocation from this request.
    let mut ack_targets: HashMap<String, i64> = HashMap::new();
    let mut result = Ok(());
    for message in &request.messages {
        match runtime.block_on(worker.send_message_with_retry(message, true)) {
            Ok(Some((invocation_id, sequence_number))) => {
                ack_targets
                    .entry(invocation_id)
                    .and_modify(|seq| *seq = (*seq).max(sequence_number))
                    .or_insert(sequence_number);
            }
            Ok(None) => {}
            Err(e) => {
                result = Err(e);
                break;
            }
        }
    }
    if result.is_ok() && !ack_targets.is_empty() {
        if let Err(status) = runtime.block_on(worker.wait_for_acks(&ack_targets)) {
            worker.record_status_failure(&status);
            result = Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "Failed waiting for BES acknowledgements: {} ({})",
                status.message(),
                status.code()
            ));
        }
    }
    let _ = request.done.send(result);
}

struct WorkerState {
    config: BesConfig,
    connection: ConnectionConfig,
    counters: Arc<CounterState>,
    streams: HashMap<String, StreamState>,
}

impl WorkerState {
    fn new(config: BesConfig, connection: ConnectionConfig, counters: Arc<CounterState>) -> Self {
        Self {
            config,
            connection,
            counters,
            streams: HashMap::new(),
        }
    }

    fn batch_size(&self) -> usize {
        self.config
            .message_batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE)
            .max(1)
    }

    async fn send_message_with_retry(
        &mut self,
        message: &Message,
        fail_fast: bool,
    ) -> buck2_error::Result<Option<(String, i64)>> {
        // Route by each event's own invocation ID. Buck2 can process commands
        // concurrently, so a shared "active invocation" can misroute events
        // across streams and cause one command's end event to close another.
        //
        // We intentionally keep standalone non-record events in this path.
        // `send_messages_now()` callers expect to bypass queueing and still
        // deliver emergency events even without an active command context.
        let parsed = match ParsedMessage::from_message(message) {
            Ok(parsed) => parsed,
            Err(_) => {
                self.counters.inc_failures_invalid_request();
                return if fail_fast {
                    Err(buck2_error::buck2_error!(
                        ErrorTag::Tier0,
                        "Invalid Buck event payload"
                    ))
                } else {
                    Ok(None)
                };
            }
        };

        self.ensure_stream_exists(&parsed);

        let close_after = Instant::now() + COMMAND_END_CLOSE_GRACE;
        let close_immediately;
        let sequence_number;
        {
            let stream = self
                .streams
                .get_mut(&parsed.invocation_id)
                .expect("stream was inserted");
            sequence_number = stream.enqueue_event(&parsed, self.config.event_format);
            close_immediately = parsed.is_invocation_record;

            if parsed.is_command_end {
                stream.saw_command_end = true;
                stream.pending_close = Some(PendingClose {
                    close_after,
                    event_time: parsed.event_time.clone(),
                });
            } else if stream.saw_command_end {
                // Keep extending the quiet-period deadline while tail events arrive.
                stream.pending_close = Some(PendingClose {
                    close_after,
                    event_time: parsed.event_time.clone(),
                });
            }
        }

        let retries = self.config.retry_attempts;
        let mut last_error: Option<Status> = None;

        for attempt in 0..=retries {
            match self.flush_stream(&parsed.invocation_id).await {
                Ok(()) => {
                    if close_immediately {
                        match self
                            .close_stream(&parsed.invocation_id, parsed.event_time.clone())
                            .await
                        {
                            Ok(()) => {
                                self.counters.inc_success(parsed.payload_size as u64);
                                return Ok(None);
                            }
                            Err(status) => {
                                self.record_status_failure(&status);
                                self.discard_stream_transport(&parsed.invocation_id);
                                last_error = Some(status);
                            }
                        }
                    } else {
                        self.counters.inc_success(parsed.payload_size as u64);
                        return Ok(sequence_number.map(|sequence_number| {
                            (parsed.invocation_id.clone(), sequence_number)
                        }));
                    }
                }
                Err(status) => {
                    self.record_status_failure(&status);
                    self.discard_stream_transport(&parsed.invocation_id);
                    last_error = Some(status);
                }
            }
            if attempt < retries {
                tokio::time::sleep(backoff_for(&self.config.retry_backoff, attempt)).await;
            }
        }

        if fail_fast {
            let reason = match last_error {
                Some(status) => {
                    format!(
                        "Failed to send BES event after retries: {} ({})",
                        status.message(),
                        status.code()
                    )
                }
                None => "Failed to send BES event after retries".to_owned(),
            };
            return Err(buck2_error::buck2_error!(ErrorTag::Tier0, "{reason}"));
        }

        Ok(None)
    }

    fn ensure_stream_exists(&mut self, parsed: &ParsedMessage) {
        if self.streams.contains_key(&parsed.invocation_id) {
            return;
        }
        let stream = StreamState::new(parsed);
        self.streams.insert(parsed.invocation_id.clone(), stream);
    }

    async fn flush_stream(&mut self, invocation_id: &str) -> Result<(), Status> {
        self.ensure_stream_transport(invocation_id).await?;
        let Some(stream) = self.streams.get_mut(invocation_id) else {
            return Err(Status::unavailable(format!(
                "BES stream state missing for invocation {}",
                invocation_id
            )));
        };
        stream.flush_pending().await
    }

    async fn ensure_stream_transport(&mut self, invocation_id: &str) -> Result<(), Status> {
        let needs_reopen = match self.streams.get(invocation_id) {
            Some(stream) => stream.transport_needs_reopen(),
            None => {
                return Err(Status::unavailable(format!(
                    "BES stream state missing for invocation {}",
                    invocation_id
                )));
            }
        };

        if !needs_reopen {
            return Ok(());
        }

        self.discard_stream_transport(invocation_id);
        let last_acked_sequence_number = {
            let stream = self
                .streams
                .get(invocation_id)
                .expect("stream still exists before reopen");
            stream.last_acked_sequence_number.clone()
        };
        let (sender, ack_task) = self
            .open_stream_transport(last_acked_sequence_number)
            .await?;
        let stream = self
            .streams
            .get_mut(invocation_id)
            .expect("stream still exists after reopen");
        stream.attach_transport(sender, ack_task);
        Ok(())
    }

    async fn open_stream_transport(
        &self,
        last_acked_sequence_number: Arc<AtomicI64>,
    ) -> Result<
        (
            mpsc::Sender<PublishBuildToolEventStreamRequest>,
            tokio::task::JoinHandle<Result<(), Status>>,
        ),
        Status,
    > {
        let endpoint = tonic::transport::Endpoint::from_shared(self.connection.endpoint.clone())
            .map_err(|e| Status::internal(e.to_string()))?
            // Keep stream RPCs open for the duration of the build; use this value
            // only to bound connection establishment.
            .connect_timeout(self.config.grpc_timeout);
        let channel = endpoint.connect().await.map_err(map_transport_error)?;
        let mut client = PublishBuildEventClient::new(channel);
        let (tx, rx) = mpsc::channel(self.config.buffer_size.max(1));
        let outbound = ReceiverStream::new(rx);
        let mut request = tonic::Request::new(outbound);
        for (header_key, header_value) in &self.connection.headers {
            let metadata_key = MetadataKey::from_bytes(header_key.as_bytes())
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let metadata_value = MetadataValue::try_from(header_value.as_str())
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            request.metadata_mut().insert(metadata_key, metadata_value);
        }

        let ack_sequence_for_task = last_acked_sequence_number;
        // Don't block stream creation on response headers; start sending events
        // immediately and handle response/ACK processing in the background task.
        let ack_task = tokio::spawn(async move {
            let response: tonic::Response<
                tonic::codec::Streaming<PublishBuildToolEventStreamResponse>,
            > = client.publish_build_tool_event_stream(request).await?;
            let mut inbound = response.into_inner();
            loop {
                match inbound.message().await {
                    Ok(Some(response)) => {
                        update_max_sequence_number(
                            &ack_sequence_for_task,
                            response.sequence_number,
                        );
                    }
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        });

        Ok((tx, ack_task))
    }

    async fn close_due_streams(&mut self) {
        let now = Instant::now();
        let due = self
            .streams
            .iter()
            .filter_map(|(invocation_id, stream)| {
                stream.pending_close.as_ref().and_then(|pending_close| {
                    (pending_close.close_after <= now)
                        .then(|| (invocation_id.clone(), pending_close.event_time.clone()))
                })
            })
            .collect::<Vec<_>>();

        for (invocation_id, event_time) in due {
            if let Err(status) = self.close_stream(&invocation_id, event_time).await {
                self.record_status_failure(&status);
            }
        }
    }

    async fn close_stream(
        &mut self,
        invocation_id: &str,
        event_time: Option<Timestamp>,
    ) -> Result<(), Status> {
        if !self.streams.contains_key(invocation_id) {
            return Ok(());
        }

        {
            let stream = self
                .streams
                .get_mut(invocation_id)
                .expect("stream exists before close");
            if !stream.stream_finished_enqueued {
                let finish_event = BuildEvent {
                    event_time: event_time.or_else(|| Some(SystemTime::now().into())),
                    event: Some(build_event::Event::ComponentStreamFinished(
                        build_event::BuildComponentStreamFinished {
                            r#type:
                                build_event::build_component_stream_finished::FinishType::Finished
                                    as i32,
                        },
                    )),
                };
                stream.enqueue_raw_event(finish_event);
                stream.stream_finished_enqueued = true;
            }
        }

        self.flush_stream(invocation_id).await?;

        {
            let stream = self
                .streams
                .get_mut(invocation_id)
                .expect("stream exists after successful close flush");
            stream.pending_close = None;
        }

        let Some(mut stream) = self.streams.remove(invocation_id) else {
            return Ok(());
        };
        drop(stream.sender.take());

        let close_timeout = close_ack_timeout(self.config.grpc_timeout);
        let Some(ack_task) = stream.ack_task.take() else {
            return Err(Status::unavailable(
                "BES stream was closed before finish acknowledgement",
            ));
        };
        match tokio::time::timeout(close_timeout, ack_task).await {
            Ok(joined) => match joined {
                Ok(Ok(())) => Ok(()),
                Ok(Err(status)) => Err(status),
                Err(e) => Err(Status::internal(e.to_string())),
            },
            Err(_) => Err(Status::deadline_exceeded(format!(
                "Timed out waiting for BES stream acknowledgements after {:?}",
                close_timeout
            ))),
        }
    }

    fn discard_stream_transport(&mut self, invocation_id: &str) {
        let Some(stream) = self.streams.get_mut(invocation_id) else {
            return;
        };
        stream.discard_transport();
    }

    async fn close_all_streams(&mut self) {
        let invocation_ids = self.streams.keys().cloned().collect::<Vec<_>>();
        for invocation_id in invocation_ids {
            if let Err(status) = self.close_stream(&invocation_id, None).await {
                self.record_status_failure(&status);
            }
        }
    }

    async fn wait_for_acks(&self, ack_targets: &HashMap<String, i64>) -> Result<(), Status> {
        let deadline = Instant::now() + close_ack_timeout(self.config.grpc_timeout);
        loop {
            let mut all_acked = true;
            for (invocation_id, target_sequence_number) in ack_targets {
                let Some(stream) = self.streams.get(invocation_id) else {
                    continue;
                };
                if stream.last_acked_sequence_number() < *target_sequence_number {
                    all_acked = false;
                    if !stream.can_receive_more_acks() {
                        return Err(Status::unavailable(format!(
                            "BES stream closed before sequence {} was acknowledged for invocation {}",
                            target_sequence_number, invocation_id
                        )));
                    }
                }
            }

            if all_acked {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(format!(
                    "Timed out waiting for BES acknowledgements after {:?}",
                    close_ack_timeout(self.config.grpc_timeout)
                )));
            }

            tokio::time::sleep(COMMAND_END_CLOSE_POLL_INTERVAL).await;
        }
    }

    fn record_status_failure(&self, status: &Status) {
        match status.code() {
            tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => {
                self.counters.inc_failures_invalid_request();
            }
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                self.counters.inc_failures_unauthorized();
            }
            tonic::Code::ResourceExhausted => {
                self.counters.inc_failures_rate_limited();
            }
            tonic::Code::Unavailable => {
                self.counters.inc_failures_pushed_back();
            }
            tonic::Code::DeadlineExceeded => {
                self.counters.inc_failures_timed_out();
            }
            tonic::Code::Unknown
            | tonic::Code::Internal
            | tonic::Code::DataLoss
            | tonic::Code::Aborted
            | tonic::Code::OutOfRange
            | tonic::Code::Unimplemented => {
                self.counters.inc_failures_internal_error();
            }
            _ => {
                self.counters.inc_failures_unknown();
            }
        }
    }
}

struct StreamState {
    stream_id: StreamId,
    next_sequence_number: i64,
    last_acked_sequence_number: Arc<AtomicI64>,
    sender: Option<mpsc::Sender<PublishBuildToolEventStreamRequest>>,
    ack_task: Option<tokio::task::JoinHandle<Result<(), Status>>>,
    project_id: String,
    pending_unacked: VecDeque<PublishBuildToolEventStreamRequest>,
    bazel_converter: BazelEventConverter,
    last_sent_sequence_number: i64,
    saw_command_end: bool,
    pending_close: Option<PendingClose>,
    stream_finished_enqueued: bool,
}

struct PendingClose {
    close_after: Instant,
    event_time: Option<Timestamp>,
}

impl StreamState {
    fn new(parsed: &ParsedMessage) -> Self {
        Self {
            stream_id: StreamId {
                build_id: parsed.build_id.clone(),
                invocation_id: parsed.invocation_id.clone(),
                component: stream_id::BuildComponent::Tool as i32,
            },
            next_sequence_number: 1,
            last_acked_sequence_number: Arc::new(AtomicI64::new(0)),
            sender: None,
            ack_task: None,
            project_id: parsed.project_id.clone(),
            pending_unacked: VecDeque::new(),
            bazel_converter: BazelEventConverter::default(),
            last_sent_sequence_number: 0,
            saw_command_end: false,
            pending_close: None,
            stream_finished_enqueued: false,
        }
    }

    fn enqueue_event(
        &mut self,
        parsed: &ParsedMessage,
        event_format: BesEventFormat,
    ) -> Option<i64> {
        match event_format {
            BesEventFormat::Buck => Some(self.enqueue_raw_event(BuildEvent {
                event_time: parsed.event_time.clone(),
                event: Some(build_event::Event::ExperimentalBuildToolEvent(Any {
                    type_url: BUCK2_EVENT_TYPE_URL.to_owned(),
                    value: parsed.payload.clone(),
                })),
            })),
            BesEventFormat::Bazel => {
                let events = self
                    .bazel_converter
                    .convert(self.next_sequence_number, &parsed.buck_event);
                let mut last_sequence_number = None;
                for event in events {
                    last_sequence_number = Some(self.enqueue_raw_event(BuildEvent {
                        event_time: parsed.event_time.clone(),
                        event: Some(build_event::Event::BazelEvent(encode_bep_event(&event))),
                    }));
                }
                last_sequence_number
            }
        }
    }

    fn enqueue_raw_event(&mut self, event: BuildEvent) -> i64 {
        let seq = self.next_sequence_number;
        self.next_sequence_number += 1;

        let mut request = PublishBuildToolEventStreamRequest {
            ordered_build_event: Some(OrderedBuildEvent {
                stream_id: Some(self.stream_id.clone()),
                sequence_number: seq,
                event: Some(event),
            }),
            notification_keywords: Vec::new(),
            project_id: self.project_id.clone(),
            check_preceding_lifecycle_events_present: false,
        };
        if seq == 1 {
            request
                .notification_keywords
                .push("source=buck2".to_owned());
        }
        self.pending_unacked.push_back(request);
        seq
    }

    fn transport_needs_reopen(&self) -> bool {
        match (&self.sender, &self.ack_task) {
            (Some(_), Some(task)) => task.is_finished(),
            _ => true,
        }
    }

    fn attach_transport(
        &mut self,
        sender: mpsc::Sender<PublishBuildToolEventStreamRequest>,
        ack_task: tokio::task::JoinHandle<Result<(), Status>>,
    ) {
        self.sender = Some(sender);
        self.ack_task = Some(ack_task);
        self.prune_acked_requests();
        self.last_sent_sequence_number = self.last_acked_sequence_number();
    }

    fn discard_transport(&mut self) {
        drop(self.sender.take());
        if let Some(ack_task) = self.ack_task.take() {
            if !ack_task.is_finished() {
                ack_task.abort();
            }
        }
    }

    async fn flush_pending(&mut self) -> Result<(), Status> {
        self.prune_acked_requests();
        let sender = self
            .sender
            .clone()
            .ok_or_else(|| Status::unavailable("BES stream was not open"))?;

        let pending = self
            .pending_unacked
            .iter()
            .filter(|request| request_sequence_number(request) > self.last_sent_sequence_number)
            .cloned()
            .collect::<Vec<_>>();
        for request in pending {
            let sequence_number = request_sequence_number(&request);
            sender
                .send(request)
                .await
                .map_err(|_| Status::unavailable("BES stream was closed"))?;
            self.last_sent_sequence_number = sequence_number;
        }
        Ok(())
    }

    fn prune_acked_requests(&mut self) {
        let acked = self.last_acked_sequence_number();
        while self
            .pending_unacked
            .front()
            .is_some_and(|request| request_sequence_number(request) <= acked)
        {
            self.pending_unacked.pop_front();
        }
        if self.last_sent_sequence_number < acked {
            self.last_sent_sequence_number = acked;
        }
    }

    fn can_receive_more_acks(&self) -> bool {
        self.ack_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    fn last_acked_sequence_number(&self) -> i64 {
        self.last_acked_sequence_number.load(Ordering::Relaxed)
    }
}

fn request_sequence_number(request: &PublishBuildToolEventStreamRequest) -> i64 {
    request
        .ordered_build_event
        .as_ref()
        .map_or(0, |ordered| ordered.sequence_number)
}

struct ParsedMessage {
    build_id: String,
    invocation_id: String,
    project_id: String,
    event_time: Option<Timestamp>,
    buck_event: buck2_data::BuckEvent,
    payload: Vec<u8>,
    payload_size: usize,
    is_command_end: bool,
    is_invocation_record: bool,
}

impl ParsedMessage {
    // Normalize invocation IDs at parse time so every downstream path uses the
    // same stable key. This avoids random remapping and keeps stream routing
    // deterministic when trace IDs are malformed.
    //
    // For missing IDs, we intentionally generate a random UUID so emergency
    // standalone events don't collapse into a shared synthetic stream key.
    fn from_message(message: &Message) -> Result<Self, ()> {
        let event = buck2_data::BuckEvent::decode(message.message.as_slice()).map_err(|_| ())?;
        let event_id = if !event.trace_id.is_empty() {
            normalize_invocation_id(&event.trace_id)
        } else if let Some(message_key) = message.message_key {
            normalize_invocation_id(&message_key.to_string())
        } else {
            uuid::Uuid::new_v4().to_string()
        };

        let event_time = event.timestamp.clone();
        let is_command_end = is_command_end(&event);
        let is_invocation_record = is_invocation_record(&event);

        Ok(Self {
            build_id: event_id.clone(),
            invocation_id: event_id,
            project_id: message.category.clone(),
            event_time,
            buck_event: event,
            payload_size: message.message.len(),
            payload: message.message.clone(),
            is_command_end,
            is_invocation_record,
        })
    }
}

fn update_max_sequence_number(slot: &AtomicI64, value: i64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn backoff_for(initial_backoff: &Duration, attempt: usize) -> Duration {
    let capped = attempt.min(8);
    let multiplier = 1u32 << capped;
    initial_backoff
        .checked_mul(multiplier)
        .unwrap_or(*initial_backoff)
}

fn close_ack_timeout(grpc_timeout: Duration) -> Duration {
    let multiplied = grpc_timeout
        .checked_mul(CLOSE_ACK_TIMEOUT_MULTIPLIER)
        .unwrap_or(grpc_timeout);
    if multiplied < MIN_CLOSE_ACK_TIMEOUT {
        MIN_CLOSE_ACK_TIMEOUT
    } else {
        multiplied
    }
}

// The BES stream ID expects UUID-shaped IDs. We preserve valid UUIDs
// and map malformed IDs deterministically so the same malformed input always
// routes to the same stream.
fn normalize_invocation_id(invocation_id: &str) -> String {
    if let Ok(invocation_id) = uuid::Uuid::parse_str(invocation_id) {
        invocation_id.to_string()
    } else {
        deterministic_uuid_from(invocation_id).to_string()
    }
}

fn deterministic_uuid_from(input: &str) -> uuid::Uuid {
    // FNV-1a 128-bit. Good enough for stable buck2-internal ID normalization
    // without pulling in an additional hash dependency.
    const FNV_OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;
    const FNV_PRIME: u128 = 0x0000000001000000000000000000013B;

    let mut hash = FNV_OFFSET_BASIS;
    for b in input.bytes() {
        hash ^= u128::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    let mut bytes = hash.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

fn is_command_end(event: &buck2_data::BuckEvent) -> bool {
    match &event.data {
        Some(buck_event::Data::SpanEnd(span_end)) => {
            matches!(span_end.data, Some(span_end_event::Data::Command(_)))
        }
        _ => false,
    }
}

fn is_invocation_record(event: &buck2_data::BuckEvent) -> bool {
    match &event.data {
        Some(buck_event::Data::Record(record)) => {
            matches!(record.data, Some(record_event::Data::InvocationRecord(_)))
        }
        _ => false,
    }
}

fn map_transport_error(err: tonic::transport::Error) -> Status {
    Status::unavailable(err.to_string())
}

fn bes_backend(configured_endpoint: Option<&str>) -> buck2_error::Result<String> {
    let endpoint = configured_endpoint
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            buck2_error::buck2_error!(
                ErrorTag::Input,
                "BES backend is not configured (set `[bes] backend`)"
            )
        })?;

    let (scheme, target) = endpoint.split_once("://").ok_or_else(|| {
        buck2_error::buck2_error!(
            ErrorTag::Input,
            "Invalid BES backend `{}` (expected `grpc://HOST[:PORT]` or `grpcs://HOST[:PORT]`)",
            endpoint
        )
    })?;

    let target = target.trim();
    if target.is_empty() {
        return Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Invalid BES backend `{}` (missing target host)",
            endpoint
        ));
    }

    if scheme.eq_ignore_ascii_case("grpc") {
        Ok(format!("http://{target}"))
    } else if scheme.eq_ignore_ascii_case("grpcs") {
        Ok(format!("https://{target}"))
    } else {
        Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Invalid BES backend `{}` (expected scheme `grpc://` or `grpcs://`)",
            endpoint
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use buck2_wrapper_common::invocation_id::TraceId;

    use super::*;

    fn command_start_data() -> buck2_data::buck_event::Data {
        buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
            data: Some(buck2_data::CommandStart::default().into()),
        })
    }

    fn invocation_record_data() -> buck2_data::buck_event::Data {
        buck2_data::buck_event::Data::Record(buck2_data::RecordEvent {
            data: Some(record_event::Data::InvocationRecord(Box::new(
                buck2_data::InvocationRecord::default(),
            ))),
        })
    }

    fn make_message(
        trace_id: Option<&str>,
        message_key: Option<i64>,
        data: buck2_data::buck_event::Data,
    ) -> Message {
        let event = buck2_data::BuckEvent {
            timestamp: Some(SystemTime::now().into()),
            trace_id: trace_id.unwrap_or_default().to_owned(),
            span_id: 0,
            parent_id: 0,
            data: Some(data),
        };
        Message {
            category: "test".to_owned(),
            message: event.encode_to_vec(),
            message_key,
        }
    }

    #[test]
    fn normalize_invocation_id_is_deterministic_for_invalid_input() {
        let normalized_1 = normalize_invocation_id("not-a-uuid");
        let normalized_2 = normalize_invocation_id("not-a-uuid");
        assert_eq!(normalized_1, normalized_2);
        assert!(uuid::Uuid::parse_str(&normalized_1).is_ok());
    }

    #[test]
    fn parsed_message_preserves_per_event_trace_id() {
        let trace_a = TraceId::new().to_string();
        let trace_b = TraceId::new().to_string();

        let message_a = make_message(Some(&trace_a), Some(1), command_start_data());
        let message_b = make_message(Some(&trace_b), Some(2), command_start_data());

        let parsed_a = ParsedMessage::from_message(&message_a).expect("valid message");
        let parsed_b = ParsedMessage::from_message(&message_b).expect("valid message");

        assert_eq!(parsed_a.build_id, trace_a);
        assert_eq!(parsed_a.invocation_id, trace_a);
        assert_eq!(parsed_b.build_id, trace_b);
        assert_eq!(parsed_b.invocation_id, trace_b);
        assert_ne!(parsed_a.invocation_id, parsed_b.invocation_id);
    }

    #[test]
    fn parsed_message_uses_message_key_when_trace_id_is_missing() {
        let message = make_message(None, Some(42), command_start_data());
        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        assert_eq!(parsed.build_id, normalize_invocation_id("42"));
        assert_eq!(parsed.invocation_id, normalize_invocation_id("42"));
    }

    #[test]
    fn parsed_message_uses_random_uuid_only_when_ids_are_missing() {
        let message_a = make_message(None, None, command_start_data());
        let message_b = make_message(None, None, command_start_data());
        let parsed_a = ParsedMessage::from_message(&message_a).expect("valid message");
        let parsed_b = ParsedMessage::from_message(&message_b).expect("valid message");

        assert!(uuid::Uuid::parse_str(&parsed_a.build_id).is_ok());
        assert!(uuid::Uuid::parse_str(&parsed_a.invocation_id).is_ok());
        assert_eq!(parsed_a.build_id, parsed_a.invocation_id);

        assert!(uuid::Uuid::parse_str(&parsed_b.build_id).is_ok());
        assert!(uuid::Uuid::parse_str(&parsed_b.invocation_id).is_ok());
        assert_eq!(parsed_b.build_id, parsed_b.invocation_id);

        // Random fallback should avoid forcing all such events into one stream.
        assert_ne!(parsed_a.build_id, parsed_b.build_id);
    }

    #[test]
    fn bes_backend_accepts_grpc_and_grpcs_endpoints() {
        assert_eq!(
            bes_backend(Some("grpc://localhost:8980")).unwrap(),
            "http://localhost:8980"
        );
        assert_eq!(
            bes_backend(Some("grpcs://example.com:443")).unwrap(),
            "https://example.com:443"
        );
    }

    #[test]
    fn bes_backend_rejects_non_grpc_endpoints() {
        for endpoint in [
            "http://localhost:8980",
            "https://localhost:8980",
            "localhost:8980",
        ] {
            assert!(
                bes_backend(Some(endpoint)).is_err(),
                "expected `{}` to be rejected",
                endpoint
            );
        }
    }

    #[test]
    fn event_format_defaults_to_buck() {
        assert_eq!(BesConfig::default().event_format, BesEventFormat::Buck);
    }

    #[test]
    fn event_format_parses_supported_values() {
        assert_eq!(
            "buck".parse::<BesEventFormat>().unwrap(),
            BesEventFormat::Buck
        );
        assert_eq!(
            "bazel".parse::<BesEventFormat>().unwrap(),
            BesEventFormat::Bazel
        );
        assert_eq!(
            " bazel ".parse::<BesEventFormat>().unwrap(),
            BesEventFormat::Bazel
        );
    }

    #[test]
    fn event_format_rejects_unknown_values() {
        let err = "Bazel".parse::<BesEventFormat>().unwrap_err();
        assert!(err.to_string().contains("expected `buck` or `bazel`"));
    }

    #[test]
    fn bazel_enqueue_returns_highest_emitted_sequence_number() {
        let message = make_message(
            Some(&TraceId::new().to_string()),
            Some(1),
            command_start_data(),
        );
        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        let mut stream = StreamState::new(&parsed);

        let last_sequence = stream.enqueue_event(&parsed, BesEventFormat::Bazel);

        assert_eq!(last_sequence, Some(2));
        assert_eq!(stream.pending_unacked.len(), 2);
        assert_eq!(request_sequence_number(&stream.pending_unacked[0]), 1);
        assert_eq!(request_sequence_number(&stream.pending_unacked[1]), 2);
        let event = stream.pending_unacked[0]
            .ordered_build_event
            .as_ref()
            .and_then(|ordered| ordered.event.as_ref())
            .and_then(|event| event.event.as_ref());
        assert!(matches!(event, Some(build_event::Event::BazelEvent(_))));
    }

    #[tokio::test]
    async fn standalone_non_record_events_are_not_silently_dropped() {
        let message = make_message(
            Some(&TraceId::new().to_string()),
            Some(1),
            command_start_data(),
        );
        let config = BesConfig {
            retry_attempts: 0,
            grpc_timeout: Duration::from_millis(20),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            headers: Vec::new(),
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config, connection, counters);

        // Before this change, this returned Ok(()) because standalone non-record
        // events were dropped when there was no active command context.
        assert!(
            worker
                .send_message_with_retry(&message, true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn immediate_close_failures_still_retry() {
        let trace_id = TraceId::new().to_string();
        let message = make_message(Some(&trace_id), Some(1), invocation_record_data());
        let config = BesConfig {
            retry_attempts: 1,
            retry_backoff: Duration::ZERO,
            grpc_timeout: Duration::from_millis(20),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            headers: Vec::new(),
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config, connection, counters.clone());

        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        worker.ensure_stream_exists(&parsed);
        {
            let (sender, receiver) = mpsc::channel(1);
            drop(receiver);
            let ack_task =
                tokio::spawn(async { std::future::pending::<Result<(), Status>>().await });
            let stream = worker
                .streams
                .get_mut(&parsed.invocation_id)
                .expect("stream inserted");
            stream.attach_transport(sender, ack_task);
            // Make the outer flush a no-op so failure originates from the immediate-close path.
            stream.last_sent_sequence_number = 1;
        }

        assert!(
            worker
                .send_message_with_retry(&message, true)
                .await
                .is_err()
        );
        assert_eq!(counters.snapshot().failures_pushed_back, 2);
    }

    #[tokio::test]
    async fn close_stream_keeps_pending_close_when_close_flush_fails() {
        let trace_id = TraceId::new().to_string();
        let message = make_message(Some(&trace_id), Some(1), command_start_data());
        let config = BesConfig {
            grpc_timeout: Duration::from_millis(20),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            headers: Vec::new(),
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config, connection, counters);

        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        worker.ensure_stream_exists(&parsed);
        let stream = worker
            .streams
            .get_mut(&parsed.invocation_id)
            .expect("stream inserted");
        stream.pending_close = Some(PendingClose {
            close_after: Instant::now(),
            event_time: None,
        });

        assert!(
            worker
                .close_stream(&parsed.invocation_id, None)
                .await
                .is_err()
        );
        assert!(
            worker
                .streams
                .get(&parsed.invocation_id)
                .and_then(|stream| stream.pending_close.as_ref())
                .is_some()
        );
    }
}
