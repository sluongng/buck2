/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Build Event Service sink implementation for OSS builds.

#[cfg(not(fbcode_build))]
mod oss_impl {
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bes_proto::google::devtools::build::v1::BuildEvent;
    use bes_proto::google::devtools::build::v1::OrderedBuildEvent;
    use bes_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
    use bes_proto::google::devtools::build::v1::StreamId;
    use bes_proto::google::devtools::build::v1::build_event;
    use bes_proto::google::devtools::build::v1::publish_build_event_client;
    use bes_proto::google::devtools::build::v1::stream_id;
    use buck2_error::BuckErrorContext;
    use crossbeam_channel;
    use futures::stream;
    use prost::Message;
    use tokio::runtime::Builder;
    use tonic::Request;
    use tonic::transport::Channel;

    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;
    use crate::sink::smart_truncate_event::smart_truncate_event;

    // 1 MiB limit similar to Scribe - configurable via BES configuration
    const BES_MESSAGE_SIZE_LIMIT: usize = 1024 * 1024;

    use std::str::FromStr;

    use tonic::metadata::MetadataMap;
    use tonic::metadata::MetadataValue;

    /// HTTP header configuration
    #[derive(Clone, Debug, Default)]
    pub struct HttpHeader {
        pub key: String,
        pub value: String,
    }

    impl FromStr for HttpHeader {
        type Err = buck2_error::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let mut iter = s.splitn(2, ':');
            match (iter.next(), iter.next()) {
                (Some(key), Some(value)) => Ok(Self {
                    key: key.trim().to_owned(),
                    value: value.trim().to_owned(),
                }),
                _ => Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "Invalid header (expect name and value separated by `:`): `{}`",
                    s
                )),
            }
        }
    }

    /// Configuration for Build Event Service
    #[derive(Clone, Debug)]
    pub struct BesConfig {
        pub endpoint: String,
        pub project_id: String,
        pub build_id: String,
        pub invocation_id: String,
        pub buffer_size: usize,
        pub retry_attempts: usize,
        pub tls: bool,
        pub http_headers: Vec<HttpHeader>,
        pub connection_timeout_ms: Option<u64>,
        pub request_timeout_ms: Option<u64>,
    }

    impl BesConfig {
        /// Create a new BesConfig with defaults, allowing http_headers to be set later
        pub fn new(
            endpoint: String,
            project_id: String,
            build_id: String,
            invocation_id: String,
        ) -> Self {
            Self {
                endpoint,
                project_id,
                build_id,
                invocation_id,
                buffer_size: 100,
                retry_attempts: 3,
                tls: true,
                http_headers: Vec::new(),
                connection_timeout_ms: None,
                request_timeout_ms: None,
            }
        }

        /// Add HTTP headers with environment variable substitution
        pub fn with_headers(mut self, headers: Vec<HttpHeader>) -> buck2_error::Result<Self> {
            self.http_headers = Self::substitute_env_vars(&headers)?;
            Ok(self)
        }

        /// Set buffer size
        pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
            self.buffer_size = buffer_size;
            self
        }

        /// Set retry attempts
        pub fn with_retry_attempts(mut self, retry_attempts: usize) -> Self {
            self.retry_attempts = retry_attempts;
            self
        }

        /// Set connection timeout
        pub fn with_connection_timeout_ms(mut self, timeout_ms: u64) -> Self {
            self.connection_timeout_ms = Some(timeout_ms);
            self
        }

        /// Set request timeout
        pub fn with_request_timeout_ms(mut self, timeout_ms: u64) -> Self {
            self.request_timeout_ms = Some(timeout_ms);
            self
        }

        /// Substitute environment variables in HTTP headers with security validation
        fn substitute_env_vars(headers: &[HttpHeader]) -> buck2_error::Result<Vec<HttpHeader>> {
            let mut result = Vec::new();
            for header in headers {
                // Simple environment variable substitution for ${VAR} patterns
                let expanded_key = Self::expand_env_vars(&header.key)?;
                let expanded_value = Self::expand_env_vars(&header.value)?;

                result.push(HttpHeader {
                    key: expanded_key,
                    value: expanded_value,
                });
            }
            Ok(result)
        }

        /// Simple environment variable expansion for ${VAR} patterns
        fn expand_env_vars(input: &str) -> buck2_error::Result<String> {
            let mut result = input.to_string();

            // Simple regex-like replacement for ${VAR} patterns
            while let Some(start) = result.find("${") {
                if let Some(end) = result[start..].find('}') {
                    let end = start + end;
                    let var_name = &result[start + 2..end];

                    let env_value = std::env::var(var_name).unwrap_or_else(|_| {
                        tracing::warn!(
                            "Environment variable '{}' not found, using empty string",
                            var_name
                        );
                        String::new()
                    });

                    result.replace_range(start..=end, &env_value);
                } else {
                    break; // Malformed ${...
                }
            }

            Ok(result)
        }

        /// Mask potentially sensitive values in logs
        #[allow(dead_code)]
        fn mask_sensitive_value(value: &str) -> String {
            let sensitive_patterns = ["token", "key", "secret", "password", "auth"];
            let lower_value = value.to_lowercase();

            if sensitive_patterns
                .iter()
                .any(|&pattern| lower_value.contains(pattern))
            {
                "***MASKED***".to_string()
            } else {
                value.to_string()
            }
        }
    }

    #[derive(Default)]
    pub struct ProducerCounters {
        pub successes: u64,
        pub failures: u64,
        pub bytes_written: u64,
        pub dropped_events: u64,
        pub queue_full_errors: u64,
        pub queue_size: usize,
    }

    #[derive(Default)]
    struct AtomicStats {
        successes: AtomicU64,
        failures: AtomicU64,
        bytes_written: AtomicU64,
        dropped_events: AtomicU64,
        queue_full_errors: AtomicU64,
    }

    impl AtomicStats {
        fn export_counters(&self, queue_size: usize) -> ProducerCounters {
            ProducerCounters {
                successes: self.successes.load(Ordering::Relaxed),
                failures: self.failures.load(Ordering::Relaxed),
                bytes_written: self.bytes_written.load(Ordering::Relaxed),
                dropped_events: self.dropped_events.load(Ordering::Relaxed),
                queue_full_errors: self.queue_full_errors.load(Ordering::Relaxed),
                queue_size,
            }
        }
    }

    /// Internal BES producer that handles the actual gRPC communication
    struct BesProducer {
        client: publish_build_event_client::PublishBuildEventClient<Channel>,
        stream_id: StreamId,
        sequence_number: Arc<AtomicU64>,
        project_id: String,
        stats: Arc<AtomicStats>,
        event_receiver: crossbeam_channel::Receiver<BuckEvent>,
        default_headers: MetadataMap,
        retry_attempts: usize,
    }

    impl BesProducer {
        async fn new(
            config: BesConfig,
            event_receiver: crossbeam_channel::Receiver<BuckEvent>,
            stats: Arc<AtomicStats>,
        ) -> buck2_error::Result<Self> {
            // Build tonic endpoint with proper configuration
            let mut endpoint = tonic::transport::Endpoint::from_shared(config.endpoint.clone())
                .with_buck_error_context(|| format!("Invalid BES endpoint: {}", config.endpoint))?;

            // Apply timeouts if configured
            if let Some(timeout_ms) = config.connection_timeout_ms {
                endpoint = endpoint.timeout(Duration::from_millis(timeout_ms));
            }

            // Connect to endpoint
            let channel = endpoint.connect().await.with_buck_error_context(|| {
                format!("Failed to connect to BES endpoint: {}", config.endpoint)
            })?;

            // Create client with metadata/headers if configured
            let client = publish_build_event_client::PublishBuildEventClient::new(channel);

            // Build default headers for all requests
            let default_headers = Self::build_metadata_map(&config.http_headers)?;
            if !config.http_headers.is_empty() {
                tracing::info!(
                    "BES client configured with {} custom headers",
                    config.http_headers.len()
                );
            }

            let stream_id = StreamId {
                build_id: config.build_id.clone(),
                invocation_id: config.invocation_id.clone(),
                component: stream_id::BuildComponent::Tool as i32,
            };

            Ok(Self {
                client,
                stream_id,
                sequence_number: Arc::new(AtomicU64::new(1)),
                project_id: config.project_id,
                stats,
                event_receiver,
                default_headers,
                retry_attempts: config.retry_attempts,
            })
        }

        /// Build gRPC metadata map from HTTP headers
        fn build_metadata_map(headers: &[HttpHeader]) -> buck2_error::Result<MetadataMap> {
            let mut metadata = MetadataMap::new();
            for header in headers {
                let value = MetadataValue::try_from(header.value.clone()).map_err(|e| {
                    buck2_error::buck2_error!(
                        buck2_error::ErrorTag::Input,
                        "Invalid header value '{}': {}",
                        header.value,
                        e
                    )
                })?;

                // Store headers as known static strings or use try_from for dynamic headers
                // For simplicity, we'll warn and skip unsupported headers for now
                if let Ok(key) = tonic::metadata::MetadataKey::from_str(&header.key) {
                    metadata.insert(key, value);
                } else {
                    tracing::warn!("Skipping invalid metadata key: {}", header.key);
                }
            }
            Ok(metadata)
        }

        async fn run_once(&self, batch_size: usize) -> buck2_error::Result<()> {
            let mut events = Vec::new();

            // Drain available events from the queue up to batch_size
            while let Ok(event) = self.event_receiver.try_recv() {
                // Filter events before adding to batch
                if self.should_send_event(event.data()) {
                    events.push(event);
                    if events.len() >= batch_size {
                        break;
                    }
                }
            }

            if !events.is_empty() {
                self.send_messages_now(events).await?;
            }

            Ok(())
        }

        /// Send multiple events now, bypassing any internal queue
        async fn send_messages_now(&self, events: Vec<BuckEvent>) -> buck2_error::Result<()> {
            // Prepare requests first, handling any errors
            let mut requests = Vec::new();
            for event in events {
                let bes_event = self.transform_buck_event_to_bes(event)?;
                let sequence = self.sequence_number.fetch_add(1, Ordering::SeqCst);

                let ordered_event = OrderedBuildEvent {
                    stream_id: Some(self.stream_id.clone()),
                    sequence_number: sequence as i64,
                    event: Some(bes_event),
                };

                let request = PublishBuildToolEventStreamRequest {
                    ordered_build_event: Some(ordered_event),
                    notification_keywords: vec![],
                    project_id: self.project_id.clone(),
                    check_preceding_lifecycle_events_present: false,
                };

                let event_size = request.encoded_len();
                self.stats
                    .bytes_written
                    .fetch_add(event_size as u64, Ordering::Relaxed);

                requests.push(request);
            }

            // Retry logic with exponential backoff and maximum total timeout
            const MAX_TOTAL_RETRY_TIME_MS: u64 = 30_000; // 30 seconds maximum
            let start_time = std::time::Instant::now();
            let mut last_error = None;

            for attempt in 0..=self.retry_attempts {
                match self.send_with_auth(requests.clone()).await {
                    Ok(_) => {
                        self.stats.successes.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    Err(e) => {
                        last_error = Some(e);

                        // Check if we've exceeded maximum total retry time
                        if start_time.elapsed().as_millis() as u64 > MAX_TOTAL_RETRY_TIME_MS {
                            tracing::warn!("BES request exceeded maximum retry time, giving up");
                            break;
                        }

                        if attempt < self.retry_attempts {
                            let backoff_ms = 100 * (1 << attempt); // Exponential backoff: 100, 200, 400, 800ms

                            // Log with appropriate level based on error type
                            match last_error.as_ref().unwrap().to_string().to_lowercase() {
                                s if s.contains("unauthorized") || s.contains("forbidden") => {
                                    tracing::error!(
                                        "BES authentication failed (attempt {}): {}",
                                        attempt + 1,
                                        last_error.as_ref().unwrap()
                                    );
                                    break; // Don't retry auth failures
                                }
                                s if s.contains("not found") => {
                                    tracing::error!(
                                        "BES endpoint not found (attempt {}): {}",
                                        attempt + 1,
                                        last_error.as_ref().unwrap()
                                    );
                                    break; // Don't retry 404s
                                }
                                _ => {
                                    tracing::warn!(
                                        "BES request failed (attempt {}), retrying in {}ms: {}",
                                        attempt + 1,
                                        backoff_ms,
                                        last_error.as_ref().unwrap()
                                    );
                                }
                            }

                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        }
                    }
                }
            }

            // All retries failed
            self.stats.failures.fetch_add(1, Ordering::Relaxed);
            Err(last_error.unwrap_or_else(|| {
                buck2_error::buck2_error!(
                    buck2_error::ErrorTag::InternalError,
                    "BES request failed with no error details"
                )
            }))
        }

        /// Send request with authentication headers
        async fn send_with_auth(
            &self,
            requests: Vec<PublishBuildToolEventStreamRequest>,
        ) -> buck2_error::Result<()> {
            let mut client = self.client.clone();
            let request_stream = stream::iter(requests);

            // Create request with default headers
            let mut request = Request::new(request_stream);

            // Apply default headers including authentication
            // Merge headers by copying each one individually
            for key_value in self.default_headers.iter() {
                match key_value {
                    tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                        request.metadata_mut().insert(key, value.clone());
                    }
                    tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                        request.metadata_mut().insert_bin(key, value.clone());
                    }
                }
            }

            // Add standard BES headers
            request
                .metadata_mut()
                .insert("content-type", "application/x-protobuf".parse().unwrap());
            request
                .metadata_mut()
                .insert("user-agent", "buck2/unknown".parse().unwrap());

            client
                .publish_build_tool_event_stream(request)
                .await
                .map(|_| ())
                .map_err(|e| {
                    buck2_error::Error::from(e).context("Failed to publish build tool event stream")
                })
        }

        fn transform_buck_event_to_bes(
            &self,
            mut event: BuckEvent,
        ) -> buck2_error::Result<BuildEvent> {
            // Apply smart truncation like Scribe does
            smart_truncate_event(event.data_mut());

            let event_proto: Box<buck2_data::BuckEvent> = event.into();
            let encoded = event_proto.encode_to_vec();

            // Check size limit
            if encoded.len() > BES_MESSAGE_SIZE_LIMIT {
                tracing::warn!(
                    "BES event size {} exceeds limit {}",
                    encoded.len(),
                    BES_MESSAGE_SIZE_LIMIT
                );
                // Could create an error event here like Scribe does
            }

            let any = prost_types::Any {
                type_url: "buck.data.BuckEvent".to_string(),
                value: encoded,
            };

            Ok(BuildEvent {
                event_time: Some(prost_types::Timestamp::from(
                    event_proto.timestamp.unwrap_or_default(),
                )),
                event: Some(build_event::Event::BuildToolEvent(any)),
            })
        }

        fn should_send_event(&self, data: &buck2_data::buck_event::Data) -> bool {
            use buck2_data::buck_event::Data;

            match data {
                Data::SpanStart(s) => {
                    use buck2_data::span_start_event::Data;
                    matches!(s.data, Some(Data::Command(..)))
                }
                Data::SpanEnd(s) => {
                    use buck2_data::span_end_event::Data;
                    match &s.data {
                        Some(Data::Command(..)) => true,
                        Some(Data::ActionExecution(a)) => a.failed || a.execution_kind != 0,
                        Some(Data::Analysis(..)) => true,
                        Some(Data::Load(..)) => true,
                        Some(Data::CacheUpload(..)) => true,
                        Some(Data::Materialization(..)) => true,
                        Some(Data::TestEnd(..)) => true,
                        _ => false,
                    }
                }
                Data::Instant(i) => {
                    use buck2_data::instant_event::Data;
                    matches!(
                        i.data,
                        Some(Data::BuildGraphInfo(..))
                            | Some(Data::StructuredError(..))
                            | Some(Data::ConfigurationCreated(..))
                    )
                }
                Data::Record(r) => {
                    use buck2_data::record_event::Data;
                    matches!(
                        r.data,
                        Some(Data::InvocationRecord(..)) | Some(Data::BuildGraphStats(..))
                    )
                }
            }
        }
    }

    static PRODUCER: OnceLock<Arc<BesClient>> = OnceLock::new();

    /// Internal structure representing a BES client similar to ScribeClient
    struct BesClient {
        event_sender: crossbeam_channel::Sender<BuckEvent>,
        stats: Arc<AtomicStats>,
    }

    impl BesClient {
        fn new(config: BesConfig) -> buck2_error::Result<Self> {
            // Use bounded channel to implement backpressure
            let channel_capacity = config.buffer_size * 10; // Allow some buffering beyond batch size
            let (sender, receiver) = crossbeam_channel::bounded(channel_capacity);
            let stats = Arc::new(AtomicStats::default());

            // Spawn background thread with tokio runtime for BES communication
            std::thread::Builder::new()
                .name("bes-producer".to_owned())
                .spawn({
                    let stats = stats.clone();
                    move || {
                        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
                        runtime.block_on(async {
                            // Initialize the producer
                            match BesProducer::new(config.clone(), receiver, stats).await {
                                Ok(producer) => {
                                    producer_loop(&producer, &config).await;
                                }
                                Err(e) => {
                                    tracing::error!("Failed to initialize BES producer: {}", e);
                                }
                            }
                        });
                    }
                })
                .map_err(|e| {
                    buck2_error::Error::from(e).context("Failed to spawn BES producer thread")
                })?;

            Ok(Self {
                event_sender: sender,
                stats,
            })
        }

        fn export_counters(&self) -> ProducerCounters {
            let queue_size = self.event_sender.len();
            self.stats.export_counters(queue_size)
        }

        /// Offers a single event to BES with backpressure handling
        fn offer(&self, event: BuckEvent) {
            match self.event_sender.try_send(event) {
                Ok(()) => {
                    // Successfully queued
                }
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    // Channel is full - implement backpressure by dropping the event
                    self.stats.queue_full_errors.fetch_add(1, Ordering::Relaxed);
                    self.stats.dropped_events.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        "BES event queue is full, dropping event to prevent blocking Buck2"
                    );
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    // Background thread has died - this is a serious error but don't block Buck2
                    self.stats.dropped_events.fetch_add(1, Ordering::Relaxed);
                    tracing::error!("BES background thread has disconnected, dropping event");
                }
            }
        }

        /// Send multiple events now
        async fn send_messages_now(&self, events: Vec<BuckEvent>) -> buck2_error::Result<()> {
            // For simplicity, just queue them all - the background thread will handle them
            for event in events {
                self.offer(event);
            }
            Ok(())
        }
    }

    /// Task that drives the producer to regularly drain its queue.
    async fn producer_loop(producer: &BesProducer, config: &BesConfig) {
        const SLEEP_INTERVAL: Duration = Duration::from_millis(500);
        let batch_size = config.buffer_size;

        loop {
            match producer.run_once(batch_size).await {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!("BES producer error: {}", e);
                    // Continue processing despite errors
                }
            }
            tokio::time::sleep(SLEEP_INTERVAL).await;
        }
    }

    /// Initializes the BES producer that backs all BES clients, similar to scribe pattern
    fn initialize(config: BesConfig) -> buck2_error::Result<&'static BesClient> {
        Ok(&**PRODUCER.get_or_try_init(|| -> buck2_error::Result<_> {
            let client = Arc::new(BesClient::new(config)?);
            Ok(client)
        })?)
    }

    /// Build Event Service sink that sends Buck2 events to a BES-compatible server
    pub struct BesEventSink {
        bes_client: &'static BesClient,
    }

    impl BesEventSink {
        pub fn new(config: BesConfig) -> buck2_error::Result<Self> {
            let bes_client = initialize(config)?;
            Ok(Self { bes_client })
        }

        pub fn export_counters(&self) -> ProducerCounters {
            self.bes_client.export_counters()
        }

        /// Place this event on an internal queue for batch sending
        pub fn offer(&self, event: BuckEvent) {
            self.bes_client.offer(event);
        }

        /// Send multiple events now
        pub async fn send_messages_now(&self, events: Vec<BuckEvent>) -> buck2_error::Result<()> {
            self.bes_client.send_messages_now(events).await
        }

        /// Send this event now
        pub async fn send_now(&self, event: BuckEvent) -> buck2_error::Result<()> {
            self.bes_client.send_messages_now(vec![event]).await
        }

        fn should_send_event(&self, data: &buck2_data::buck_event::Data) -> bool {
            use buck2_data::buck_event::Data;

            match data {
                Data::SpanStart(s) => {
                    use buck2_data::span_start_event::Data;
                    matches!(s.data, Some(Data::Command(..)))
                }
                Data::SpanEnd(s) => {
                    use buck2_data::span_end_event::Data;
                    match &s.data {
                        Some(Data::Command(..)) => true,
                        Some(Data::ActionExecution(a)) => a.failed || a.execution_kind != 0,
                        Some(Data::Analysis(..)) => true,
                        Some(Data::Load(..)) => true,
                        Some(Data::CacheUpload(..)) => true,
                        Some(Data::Materialization(..)) => true,
                        Some(Data::TestEnd(..)) => true,
                        _ => false,
                    }
                }
                Data::Instant(i) => {
                    use buck2_data::instant_event::Data;
                    matches!(
                        i.data,
                        Some(Data::BuildGraphInfo(..))
                            | Some(Data::StructuredError(..))
                            | Some(Data::ConfigurationCreated(..))
                    )
                }
                Data::Record(r) => {
                    use buck2_data::record_event::Data;
                    matches!(
                        r.data,
                        Some(Data::InvocationRecord(..)) | Some(Data::BuildGraphStats(..))
                    )
                }
            }
        }
    }

    impl EventSink for BesEventSink {
        fn send(&self, event: Event) {
            match event {
                Event::Buck(event) => {
                    if self.should_send_event(event.data()) {
                        self.offer(event);
                    }
                }
                Event::CommandResult(..) => {}
                Event::PartialResult(..) => {}
            }
        }
    }

    impl Clone for BesEventSink {
        fn clone(&self) -> Self {
            Self {
                bes_client: self.bes_client,
            }
        }
    }

    impl EventSinkWithStats for BesEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as _
        }

        fn stats(&self) -> EventSinkStats {
            let counters = self.export_counters();
            EventSinkStats {
                successes: counters.successes,
                failures_invalid_request: 0,
                failures_unauthorized: 0,
                failures_rate_limited: 0,
                failures_pushed_back: 0,
                failures_enqueue_failed: counters.queue_full_errors,
                failures_internal_error: counters.failures,
                failures_timed_out: 0,
                failures_unknown: 0,
                buffered: counters.queue_size as u64,
                dropped: counters.dropped_events,
                bytes_written: counters.bytes_written,
            }
        }
    }
}

#[cfg(not(fbcode_build))]
pub use oss_impl::*;

// Stub implementations for fbcode builds
#[cfg(fbcode_build)]
mod fbcode_stubs {
    use std::sync::Arc;

    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;

    /// Stub configuration for BES (not used in fbcode builds)
    #[derive(Clone, Debug)]
    pub struct BesConfig {
        pub endpoint: String,
        pub project_id: String,
        pub build_id: String,
        pub invocation_id: String,
        pub buffer_size: usize,
        pub retry_attempts: usize,
        pub tls: bool,
        pub http_headers: Vec<()>, // Placeholder since HttpHeader not available
        pub connection_timeout_ms: Option<u64>,
        pub request_timeout_ms: Option<u64>,
    }

    /// Stub BES sink for fbcode builds
    pub struct BesEventSink;

    impl BesEventSink {
        pub fn new(_config: BesConfig) -> buck2_error::Result<Self> {
            Ok(Self)
        }
    }

    impl EventSink for BesEventSink {
        fn send(&self, _event: Event) {
            // No-op in fbcode builds
        }
    }

    impl Clone for BesEventSink {
        fn clone(&self) -> Self {
            Self
        }
    }

    impl EventSinkWithStats for BesEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as _
        }

        fn stats(&self) -> EventSinkStats {
            EventSinkStats::default()
        }
    }
}

#[cfg(fbcode_build)]
pub use fbcode_stubs::*;
