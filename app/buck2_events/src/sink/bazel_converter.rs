/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

#![allow(deprecated)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::env;
use std::io::Write;

use bazel_bep_proto::blaze;
use bazel_bep_proto::build_event_stream as bep;
use bazel_bep_proto::build_event_stream::build_event;
use bazel_bep_proto::build_event_stream::build_event_id;
use bazel_bep_proto::command_line as cl;
use bazel_bep_proto::failure_details as failure;
use flate2::Compression;
use flate2::write::GzEncoder;
use prost::Message as _;
use prost_types::Any;
use prost_types::Struct;
use prost_types::Timestamp;
use prost_types::Value;
use prost_types::value;

pub(crate) const BEP_EVENT_TYPE_URL: &str = "type.googleapis.com/build_event_stream.BuildEvent";

const BUILD_TOOL_VERSION: &str = "buck2";
const DEFAULT_CONFIGURATION_ID: &str = "buck2";
const GENERIC_TARGET_KIND: &str = "buck2 rule";
const TEST_TARGET_KIND: &str = "buck2_test rule";
const INTERRUPTED_EXIT_CODE: i32 = 8;
const MAX_INLINE_FILE_BYTES: usize = 16 * 1024;
const CANONICAL_COMMAND_LINE_LABEL: &str = "canonical";
const ORIGINAL_COMMAND_LINE_LABEL: &str = "original";
const STRUCT_TYPE_URL: &str = "type.googleapis.com/google.protobuf.Struct";
const BUILDBUDDY_VISIBILITY_KEY: &str = "VISIBILITY";
const BUILDBUDDY_PUBLIC_VISIBILITY: &str = "PUBLIC";
const COMMAND_PROFILE_NAME: &str = "command.profile.gz";
const MAX_PROFILE_SPANS: usize = 50_000;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TargetKey {
    label: String,
    configuration: String,
}

#[derive(Clone, Debug)]
struct CompletedTargetState {
    success: bool,
}

#[derive(Clone, Debug)]
struct TestCaseState {
    name: String,
    status: i32,
    duration: Option<prost_types::Duration>,
    details: String,
    message: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ProfileLane {
    Command,
    Loading,
    Analysis,
    Actions,
    ActionStages,
    Tests,
    Materialization,
    Cache,
    Dice,
    Other,
}

impl ProfileLane {
    fn label(self) -> &'static str {
        match self {
            Self::Command => "Buck2 command",
            Self::Loading => "Buck2 loading",
            Self::Analysis => "Buck2 analysis",
            Self::Actions => "Buck2 actions",
            Self::ActionStages => "Buck2 action stages",
            Self::Tests => "Buck2 tests",
            Self::Materialization => "Buck2 materialization",
            Self::Cache => "Buck2 cache and remote",
            Self::Dice => "Buck2 DICE",
            Self::Other => "Buck2 other",
        }
    }

    fn track_base(self) -> u64 {
        match self {
            Self::Command => 1,
            Self::Loading => 1_000,
            Self::Analysis => 2_000,
            Self::Actions => 3_000,
            Self::ActionStages => 4_000,
            Self::Tests => 5_000,
            Self::Materialization => 6_000,
            Self::Cache => 7_000,
            Self::Dice => 8_000,
            Self::Other => 9_000,
        }
    }

    fn tid(self, index: usize) -> u64 {
        self.track_base()
            .saturating_add(u64::try_from(index).unwrap_or(u64::MAX))
    }
}

#[derive(Clone, Debug)]
struct ProfileOpenSpan {
    span_id: u64,
    parent_id: u64,
    name: String,
    lane: ProfileLane,
    category: &'static str,
    start_us: i64,
    args: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
struct ProfileCompletedSpan {
    span_id: u64,
    parent_id: u64,
    name: String,
    lane: ProfileLane,
    category: &'static str,
    start_us: i64,
    duration_us: i64,
    args: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
struct ProfileSpanDetails {
    name: String,
    lane: ProfileLane,
    category: &'static str,
    args: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
struct ProfileFallbackSpan {
    name: String,
    start_us: i64,
    duration_us: i64,
    args: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Default)]
struct CommandProfileBuilder {
    command_started_at_us: Option<i64>,
    command_name: Option<String>,
    open_spans: BTreeMap<u64, ProfileOpenSpan>,
    completed_spans: Vec<ProfileCompletedSpan>,
    dropped_spans: usize,
}

#[derive(Debug, Default)]
pub(crate) struct BazelEventConverter {
    build_metadata: BTreeMap<String, String>,
    saw_started: bool,
    saw_finished: bool,
    progress_count: i32,
    completed_targets: BTreeMap<TargetKey, CompletedTargetState>,
    action_children: BTreeMap<TargetKey, BTreeMap<String, bep::BuildEventId>>,
    emitted_action_child_counts: BTreeMap<TargetKey, usize>,
    emitted_configured_targets: BTreeSet<String>,
    target_outputs: BTreeMap<TargetKey, BTreeMap<String, bep::File>>,
    emitted_output_counts: BTreeMap<TargetKey, usize>,
    emitted_completed_targets: BTreeSet<TargetKey>,
    test_cases: BTreeMap<TargetKey, Vec<TestCaseState>>,
    pending_patterns: Option<Vec<String>>,
    pattern_expanded_emitted: bool,
    emitted_build_tool_logs: bool,
    command_profile: CommandProfileBuilder,
}

impl BazelEventConverter {
    pub(crate) fn new<I>(build_metadata: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        Self {
            build_metadata: build_metadata.into_iter().collect(),
            ..Self::default()
        }
    }

    pub(crate) fn convert(
        &mut self,
        sequence_hint: i64,
        event: &buck2_data::BuckEvent,
    ) -> Vec<bep::BuildEvent> {
        let mut events = Vec::new();

        if !self.saw_started && !is_command_start(event) {
            events.push(self.started_event(event, None));
            self.saw_started = true;
        }

        match event.data.as_ref() {
            Some(buck2_data::buck_event::Data::SpanStart(span_start)) => {
                self.convert_span_start(event, span_start, &mut events);
            }
            Some(buck2_data::buck_event::Data::SpanEnd(span_end)) => {
                self.convert_span_end(event, span_end, &mut events);
            }
            Some(buck2_data::buck_event::Data::Instant(instant)) => {
                self.convert_instant(sequence_hint, event, instant, &mut events);
            }
            Some(buck2_data::buck_event::Data::Record(record)) => {
                self.convert_record(event, record, &mut events);
            }
            None => {}
        }

        events
    }

    fn convert_span_start(
        &mut self,
        event: &buck2_data::BuckEvent,
        span_start: &buck2_data::SpanStartEvent,
        events: &mut Vec<bep::BuildEvent>,
    ) {
        self.command_profile.record_span_start(event, span_start);

        match span_start.data.as_ref() {
            Some(buck2_data::span_start_event::Data::Command(command)) => {
                if !self.saw_started {
                    events.push(self.started_event(event, Some(command)));
                    self.saw_started = true;
                }
                events.push(unstructured_command_line_event(command));
                events.push(original_structured_command_line_event(command));
                events.push(canonical_structured_command_line_event(command));
                events.push(options_parsed_event(command));
                events.push(default_configuration_event());
                events.push(workspace_info_event(command));
                events.push(workspace_status_event(command));
                events.push(build_metadata_event(&build_metadata_from_command(
                    command,
                    &self.build_metadata,
                )));
            }
            Some(buck2_data::span_start_event::Data::CommandCritical(command)) => {
                let metadata = metadata_map_from_hash_map(&command.metadata);
                if !metadata.is_empty() || !self.build_metadata.is_empty() {
                    events.push(self.build_metadata_event(&metadata));
                }
            }
            Some(buck2_data::span_start_event::Data::Analysis(analysis)) => {
                self.push_configured_event(configured_event_from_analysis_start(analysis), events);
            }
            Some(buck2_data::span_start_event::Data::TestDiscovery(test_discovery)) => {
                self.push_configured_event(
                    configured_event_from_test_label(
                        test_discovery.target_label.as_ref(),
                        TEST_TARGET_KIND,
                    ),
                    events,
                );
            }
            _ => {}
        }
    }

    fn convert_span_end(
        &mut self,
        event: &buck2_data::BuckEvent,
        span_end: &buck2_data::SpanEndEvent,
        events: &mut Vec<bep::BuildEvent>,
    ) {
        self.command_profile.record_span_end(event, span_end);

        match span_end.data.as_ref() {
            Some(buck2_data::span_end_event::Data::Command(command)) => {
                self.emit_completed_updates_for_actions(events);
                self.push_finished(finished_event_from_command_end(event, command), events);
                self.push_build_tool_logs(
                    build_tool_logs_event_from_command_end(
                        event,
                        span_end,
                        command,
                        &self.command_profile,
                    ),
                    events,
                );
            }
            Some(buck2_data::span_end_event::Data::Analysis(analysis)) => {
                if let Some(completed) = completed_event_from_analysis_end(analysis) {
                    self.remember_completed(&completed);
                }
            }
            Some(buck2_data::span_end_event::Data::ActionExecution(action)) => {
                if let Some(action_event) = action_event(event, span_end, action) {
                    self.remember_action(&action_event);
                    self.remember_target_outputs(action);
                    events.push(action_event);
                }
            }
            Some(buck2_data::span_end_event::Data::TestDiscovery(test_discovery)) => {
                self.push_configured_event(
                    configured_event_from_test_label(
                        test_discovery.target_label.as_ref(),
                        TEST_TARGET_KIND,
                    ),
                    events,
                );
            }
            Some(buck2_data::span_end_event::Data::TestRun(test_end)) => {
                if let Some(result) =
                    self.test_result_event_from_test_end(event, span_end, test_end)
                {
                    events.push(result);
                }
                if let Some(summary) = self.test_summary_event(event, span_end, test_end) {
                    events.push(summary);
                }
            }
            _ => {}
        }
    }

    fn convert_instant(
        &mut self,
        sequence_hint: i64,
        _event: &buck2_data::BuckEvent,
        instant: &buck2_data::InstantEvent,
        events: &mut Vec<bep::BuildEvent>,
    ) {
        match instant.data.as_ref() {
            Some(buck2_data::instant_event::Data::ConsoleMessage(message)) => {
                events.push(self.progress_event(
                    sequence_hint,
                    Some(message.message.clone()),
                    None,
                ));
            }
            Some(buck2_data::instant_event::Data::ConsoleWarning(warning)) => {
                events.push(self.progress_event(
                    sequence_hint,
                    None,
                    Some(warning.message.clone()),
                ));
            }
            Some(buck2_data::instant_event::Data::StreamingOutput(output)) => {
                events.push(self.progress_event(sequence_hint, None, Some(output.message.clone())));
            }
            Some(buck2_data::instant_event::Data::ActionError(action_error)) => {
                events.push(self.progress_event(
                    sequence_hint,
                    None,
                    Some(action_error_message(action_error)),
                ));
            }
            Some(buck2_data::instant_event::Data::StructuredError(error)) => {
                events.push(self.progress_event(sequence_hint, None, Some(error.payload.clone())));
            }
            Some(buck2_data::instant_event::Data::ReSession(session)) => {
                let mut metadata =
                    BTreeMap::from([("REMOTE_EXECUTION_ENABLED".to_owned(), "1".to_owned())]);
                if !session.session_id.is_empty() {
                    metadata.insert("BUCK2_RE_SESSION_ID".to_owned(), session.session_id.clone());
                }
                events.push(self.build_metadata_event(&metadata));
            }
            Some(buck2_data::instant_event::Data::TargetPatterns(patterns)) => {
                self.remember_target_patterns(patterns);
            }
            Some(buck2_data::instant_event::Data::TargetCfg(cfg)) => {
                let mut metadata = BTreeMap::new();
                if !cfg.target_platforms.is_empty() {
                    metadata.insert("TARGET_PLATFORM".to_owned(), cfg.target_platforms.join(","));
                }
                if !cfg.cli_modifiers.is_empty() {
                    metadata.insert(
                        "BUCK2_CLI_MODIFIERS".to_owned(),
                        cfg.cli_modifiers.join(","),
                    );
                }
                if !metadata.is_empty() {
                    events.push(self.build_metadata_event(&metadata));
                }
            }
            Some(buck2_data::instant_event::Data::BuildGraphInfo(info)) => {
                let mut metadata = BTreeMap::new();
                if info.num_nodes > 0 {
                    metadata.insert(
                        "BUCK2_BUILD_GRAPH_NUM_NODES".to_owned(),
                        info.num_nodes.to_string(),
                    );
                }
                if info.num_edges > 0 {
                    metadata.insert(
                        "BUCK2_BUILD_GRAPH_NUM_EDGES".to_owned(),
                        info.num_edges.to_string(),
                    );
                }
                if let Some(command_name) = info.command_name.as_ref()
                    && !command_name.is_empty()
                {
                    metadata.insert("BUCK2_BUILD_GRAPH_COMMAND".to_owned(), command_name.clone());
                }
                if let Some(backend_name) = info.backend_name.as_ref()
                    && !backend_name.is_empty()
                {
                    metadata.insert("BUCK2_BUILD_GRAPH_BACKEND".to_owned(), backend_name.clone());
                }
                if let Some(isolation_dir) = info.isolation_dir.as_ref()
                    && !isolation_dir.is_empty()
                {
                    metadata.insert("BUCK2_ISOLATION_DIR".to_owned(), isolation_dir.clone());
                }
                if !metadata.is_empty() {
                    events.push(self.build_metadata_event(&metadata));
                }
            }
            Some(buck2_data::instant_event::Data::VersionControlRevision(revision)) => {
                let mut metadata = BTreeMap::new();
                if let Some(revision) = revision.hg_revision.as_ref() {
                    metadata.insert("COMMIT_SHA".to_owned(), revision.clone());
                }
                if let Some(has_local_changes) = revision.has_local_changes {
                    metadata.insert(
                        "HAS_LOCAL_CHANGES".to_owned(),
                        has_local_changes.to_string(),
                    );
                }
                if !metadata.is_empty() {
                    events.push(self.build_metadata_event(&metadata));
                }
            }
            Some(buck2_data::instant_event::Data::TestDiscovery(discovery)) => {
                if let Some(buck2_data::test_discovery::Data::Tests(suite)) =
                    discovery.data.as_ref()
                {
                    if let Some(configured) = configured_event_from_test_label(
                        suite.target_label.as_ref(),
                        TEST_TARGET_KIND,
                    ) {
                        events.push(configured);
                    }
                }
                if let Some(buck2_data::test_discovery::Data::Session(session)) =
                    discovery.data.as_ref()
                    && !session.info.is_empty()
                {
                    events.push(self.progress_event(
                        sequence_hint,
                        Some(session.info.clone()),
                        None,
                    ));
                }
            }
            Some(buck2_data::instant_event::Data::TestResult(result)) => {
                self.remember_test_case(result);
            }
            _ => {}
        }
    }

    fn convert_record(
        &mut self,
        event: &buck2_data::BuckEvent,
        record: &buck2_data::RecordEvent,
        events: &mut Vec<bep::BuildEvent>,
    ) {
        match record.data.as_ref() {
            Some(buck2_data::record_event::Data::InvocationRecord(record)) => {
                self.emit_pending_pattern_expanded(&[], events);
                self.emit_completed_updates_for_actions(events);
                events.push(build_metrics_event(record));
                events.push(self.build_metadata_event(&build_metadata_from_invocation(record)));
                self.push_finished(finished_event_from_invocation_record(event, record), events);
                self.push_build_tool_logs(
                    build_tool_logs_event_from_invocation(record, &self.command_profile),
                    events,
                );
            }
            Some(buck2_data::record_event::Data::BuildGraphStats(stats)) => {
                self.emit_pending_pattern_expanded(&stats.build_targets, events);
                for target in &stats.build_targets {
                    self.push_configured_event(configured_event_from_build_target(target), events);
                    if let Some(completed) = completed_event_from_build_target(target) {
                        self.remember_completed(&completed);
                    }
                }
            }
            _ => {}
        }
    }

    fn push_finished(&mut self, event: bep::BuildEvent, events: &mut Vec<bep::BuildEvent>) {
        if self.saw_finished {
            return;
        }
        self.saw_finished = true;
        events.push(event);
    }

    fn push_build_tool_logs(&mut self, event: bep::BuildEvent, events: &mut Vec<bep::BuildEvent>) {
        if self.emitted_build_tool_logs {
            return;
        }
        self.emitted_build_tool_logs = true;
        events.push(event);
    }

    fn build_metadata_event(&self, metadata: &BTreeMap<String, String>) -> bep::BuildEvent {
        build_metadata_event_with_defaults(&self.build_metadata, metadata)
    }

    fn remember_target_patterns(&mut self, patterns: &buck2_data::ParsedTargetPatterns) {
        let patterns = target_patterns_from_invocation_record(patterns);
        if patterns.is_empty() {
            return;
        }
        self.pending_patterns = Some(patterns);
        self.pattern_expanded_emitted = false;
    }

    fn emit_pending_pattern_expanded(
        &mut self,
        targets: &[buck2_data::BuildTarget],
        events: &mut Vec<bep::BuildEvent>,
    ) {
        if self.pattern_expanded_emitted {
            return;
        }
        let Some(patterns) = self.pending_patterns.clone() else {
            return;
        };
        events.push(pattern_expanded_event(
            patterns,
            configured_target_children_from_build_targets(targets),
        ));
        self.pattern_expanded_emitted = true;
    }

    fn push_configured_event(
        &mut self,
        event: Option<bep::BuildEvent>,
        events: &mut Vec<bep::BuildEvent>,
    ) {
        let Some(event) = event else {
            return;
        };
        let Some(build_event_id::Id::TargetConfigured(id)) =
            event.id.as_ref().and_then(|id| id.id.as_ref())
        else {
            events.push(event);
            return;
        };
        if self.emitted_configured_targets.insert(id.label.clone()) {
            events.push(event);
        }
    }

    fn started_event(
        &self,
        event: &buck2_data::BuckEvent,
        command: Option<&buck2_data::CommandStart>,
    ) -> bep::BuildEvent {
        let cli_args = command.map(|c| c.cli_args.as_slice()).unwrap_or(&[]);
        let options_description = if cli_args.is_empty() {
            BUILD_TOOL_VERSION.to_owned()
        } else {
            cli_args.join(" ")
        };
        let metadata = command.map(|c| &c.metadata);

        bep::BuildEvent {
            id: Some(started_id()),
            children: vec![
                unstructured_command_line_id(),
                structured_command_line_id(ORIGINAL_COMMAND_LINE_LABEL),
                structured_command_line_id(CANONICAL_COMMAND_LINE_LABEL),
                options_parsed_id(),
                workspace_status_id(),
                build_metadata_id(),
                configuration_event_id(DEFAULT_CONFIGURATION_ID),
                workspace_config_id(),
                build_tool_logs_id(),
                build_finished_id(),
            ],
            payload: Some(build_event::Payload::Started(bep::BuildStarted {
                uuid: event.trace_id.clone(),
                start_time_millis: 0,
                start_time: event.timestamp.clone(),
                build_tool_version: BUILD_TOOL_VERSION.to_owned(),
                options_description,
                command: command
                    .map(command_name)
                    .unwrap_or_else(|| "unknown".to_owned()),
                working_directory: metadata
                    .and_then(|m| first_metadata(m, &["CWD", "PWD", "BUILD_WORKING_DIRECTORY"]))
                    .unwrap_or_default(),
                workspace_directory: metadata
                    .and_then(|m| first_metadata(m, &["REPO_ROOT", "WORKSPACE_DIRECTORY"]))
                    .unwrap_or_default(),
                server_pid: 0,
                host: metadata
                    .and_then(|m| first_metadata(m, &["HOST", "BUILD_HOST"]))
                    .unwrap_or_default(),
                user: metadata
                    .and_then(|m| first_metadata(m, &["USER", "BUILD_USER"]))
                    .unwrap_or_default(),
            })),
            last_message: false,
        }
    }

    fn progress_event(
        &mut self,
        sequence_hint: i64,
        stdout: Option<String>,
        stderr: Option<String>,
    ) -> bep::BuildEvent {
        self.progress_count = self.progress_count.saturating_add(1);
        let opaque_count = i32::try_from(sequence_hint)
            .unwrap_or(i32::MAX)
            .saturating_add(self.progress_count);
        bep::BuildEvent {
            id: Some(bep::BuildEventId {
                id: Some(build_event_id::Id::Progress(build_event_id::ProgressId {
                    opaque_count,
                })),
            }),
            children: Vec::new(),
            payload: Some(build_event::Payload::Progress(bep::Progress {
                stdout: stdout.unwrap_or_default(),
                stderr: stderr.unwrap_or_default(),
            })),
            last_message: false,
        }
    }

    fn remember_completed(&mut self, event: &bep::BuildEvent) {
        let Some(build_event_id::Id::TargetCompleted(id)) =
            event.id.as_ref().and_then(|id| id.id.as_ref())
        else {
            return;
        };
        let Some(key) = target_key_from_completed_id(id) else {
            return;
        };
        let success = match event.payload.as_ref() {
            Some(build_event::Payload::Completed(completed)) => completed.success,
            _ => true,
        };
        self.completed_targets
            .entry(key)
            .and_modify(|state| state.success &= success)
            .or_insert(CompletedTargetState { success });
    }

    fn remember_action(&mut self, event: &bep::BuildEvent) {
        let Some(build_event_id::Id::ActionCompleted(id)) =
            event.id.as_ref().and_then(|id| id.id.as_ref())
        else {
            return;
        };
        let Some(key) = target_key_from_action_id(id) else {
            return;
        };
        let action_succeeded = match event.payload.as_ref() {
            Some(build_event::Payload::Action(action)) => action.success,
            _ => true,
        };
        let target = self
            .completed_targets
            .entry(key.clone())
            .or_insert(CompletedTargetState {
                success: action_succeeded,
            });
        target.success &= action_succeeded;
        self.action_children.entry(key).or_default().insert(
            action_child_key(id),
            event.id.clone().expect("action event has id"),
        );
    }

    fn emit_completed_updates_for_actions(&mut self, events: &mut Vec<bep::BuildEvent>) {
        let keys = self.completed_targets.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let children_by_id = self.action_children.get(&key);
            let child_count = children_by_id.map(BTreeMap::len).unwrap_or_default();
            let output_count = self
                .target_outputs
                .get(&key)
                .map(BTreeMap::len)
                .unwrap_or_default();
            let is_first_completion = !self.emitted_completed_targets.contains(&key);
            let child_count_unchanged = self
                .emitted_action_child_counts
                .get(&key)
                .is_some_and(|emitted| *emitted == child_count);
            let output_count_unchanged = self
                .emitted_output_counts
                .get(&key)
                .is_some_and(|emitted| *emitted == output_count);
            if !is_first_completion && child_count_unchanged && output_count_unchanged {
                continue;
            }
            let Some(state) = self.completed_targets.get(&key) else {
                continue;
            };
            let mut children = children_by_id
                .into_iter()
                .flat_map(|children| children.values().cloned())
                .collect::<Vec<_>>();
            let (named_set_events, output_group) = self.output_group_events(&key, &mut children);
            events.extend(named_set_events);
            events.push(completed_event_with_children_and_outputs(
                key.label.clone(),
                key.configuration.clone(),
                state.success,
                children,
                output_group,
            ));
            self.emitted_action_child_counts
                .insert(key.clone(), child_count);
            self.emitted_output_counts.insert(key.clone(), output_count);
            self.emitted_completed_targets.insert(key);
        }
    }

    fn remember_target_outputs(&mut self, action: &buck2_data::ActionExecutionEnd) {
        let Some(key) = action.key.as_ref() else {
            return;
        };
        let Some(target) = action_owner(key) else {
            return;
        };
        let Some(label) = label_for_configured_target(target) else {
            return;
        };
        let key = TargetKey {
            label,
            configuration: configuration_id_for_target(target),
        };
        let outputs = self.target_outputs.entry(key).or_default();
        for output in &action.outputs {
            let Some(file) = bep_file_from_action_output(output) else {
                continue;
            };
            outputs.entry(file.name.clone()).or_insert(file);
        }
    }

    fn output_group_events(
        &self,
        key: &TargetKey,
        children: &mut Vec<bep::BuildEventId>,
    ) -> (Vec<bep::BuildEvent>, Vec<bep::OutputGroup>) {
        let Some(files) = self.target_outputs.get(key) else {
            return (Vec::new(), Vec::new());
        };
        if files.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let named_set = named_set_id_for_target(key);
        children.push(bep::BuildEventId {
            id: Some(build_event_id::Id::NamedSet(named_set.clone())),
        });
        let named_set_event = bep::BuildEvent {
            id: Some(bep::BuildEventId {
                id: Some(build_event_id::Id::NamedSet(named_set.clone())),
            }),
            children: Vec::new(),
            payload: Some(build_event::Payload::NamedSetOfFiles(
                bep::NamedSetOfFiles {
                    files: files.values().cloned().collect(),
                    file_sets: Vec::new(),
                },
            )),
            last_message: false,
        };
        (
            vec![named_set_event],
            vec![bep::OutputGroup {
                name: "default".to_owned(),
                file_sets: vec![named_set],
                incomplete: false,
                inline_files: Vec::new(),
            }],
        )
    }

    fn remember_test_case(&mut self, result: &buck2_data::TestResult) {
        let Some(target) = result.target_label.as_ref() else {
            return;
        };
        let Some(label) = label_for_configured_target(target) else {
            return;
        };
        let key = TargetKey {
            label,
            configuration: configuration_id_for_target(target),
        };
        self.test_cases.entry(key).or_default().push(TestCaseState {
            name: result.name.clone(),
            status: result.status,
            duration: result.duration.clone(),
            details: result.details.clone(),
            message: result.msg.as_ref().map(|message| message.msg.clone()),
        });
    }

    fn test_result_event_from_test_end(
        &mut self,
        event: &buck2_data::BuckEvent,
        span_end: &buck2_data::SpanEndEvent,
        test_end: &buck2_data::TestRunEnd,
    ) -> Option<bep::BuildEvent> {
        let suite = test_end.suite.as_ref()?;
        let target = suite.target_label.as_ref()?;
        let label = label_for_configured_target(target)?;
        let configuration = configuration_id_for_target(target);
        let key = TargetKey {
            label: label.clone(),
            configuration: configuration.clone(),
        };
        let cases = self.test_cases.get(&key).cloned().unwrap_or_default();
        let status = aggregate_test_status(test_end.command_report.as_ref(), &cases);
        let end_time = event.timestamp.clone();
        let outputs = test_action_outputs(&cases, test_end.command_report.as_ref(), status);

        Some(bep::BuildEvent {
            id: Some(test_result_id(label, configuration)),
            children: Vec::new(),
            payload: Some(build_event::Payload::TestResult(bep::TestResult {
                status,
                cached_locally: false,
                test_attempt_start_millis_epoch: 0,
                test_attempt_duration_millis: 0,
                test_action_output: outputs,
                warning: Vec::new(),
                execution_info: test_execution_info(test_end.command_report.as_ref()),
                status_details: test_status_details(&cases, test_end.command_report.as_ref()),
                test_attempt_start: start_time_from_span_end(span_end, end_time.as_ref()),
                test_attempt_duration: test_duration(test_end.command_report.as_ref())
                    .or_else(|| span_end.duration.clone()),
            })),
            last_message: false,
        })
    }

    fn test_summary_event(
        &mut self,
        event: &buck2_data::BuckEvent,
        span_end: &buck2_data::SpanEndEvent,
        test_end: &buck2_data::TestRunEnd,
    ) -> Option<bep::BuildEvent> {
        let suite = test_end.suite.as_ref()?;
        let target = suite.target_label.as_ref()?;
        let label = label_for_configured_target(target)?;
        let configuration = configuration_id_for_target(target);
        let key = TargetKey {
            label: label.clone(),
            configuration: configuration.clone(),
        };
        let cases = self.test_cases.remove(&key).unwrap_or_default();
        let status = aggregate_test_status(test_end.command_report.as_ref(), &cases);
        let end_time = event.timestamp.clone();
        Some(bep::BuildEvent {
            id: Some(test_summary_id(label, configuration)),
            children: Vec::new(),
            payload: Some(build_event::Payload::TestSummary(bep::TestSummary {
                overall_status: status,
                total_run_count: 1,
                run_count: 1,
                attempt_count: 1,
                shard_count: 1,
                passed: test_summary_files(status, test_end.command_report.as_ref(), true),
                failed: test_summary_files(status, test_end.command_report.as_ref(), false),
                total_num_cached: 0,
                first_start_time_millis: 0,
                last_stop_time_millis: 0,
                total_run_duration_millis: 0,
                first_start_time: start_time_from_span_end(span_end, end_time.as_ref()),
                last_stop_time: end_time,
                total_run_duration: test_duration(test_end.command_report.as_ref()),
            })),
            last_message: false,
        })
    }
}

impl CommandProfileBuilder {
    fn record_span_start(
        &mut self,
        event: &buck2_data::BuckEvent,
        span_start: &buck2_data::SpanStartEvent,
    ) {
        let Some(data) = span_start.data.as_ref() else {
            return;
        };
        if let buck2_data::span_start_event::Data::Command(command) = data {
            self.command_name = Some(command_name(command));
            self.command_started_at_us = timestamp_micros(event.timestamp.as_ref());
        }
        let Some(start_us) = timestamp_micros(event.timestamp.as_ref()) else {
            return;
        };
        let Some(details) = profile_span_start_details(data) else {
            return;
        };
        self.open_spans.insert(
            event.span_id,
            ProfileOpenSpan {
                span_id: event.span_id,
                parent_id: event.parent_id,
                name: details.name,
                lane: details.lane,
                category: details.category,
                start_us,
                args: details.args,
            },
        );
    }

    fn record_span_end(
        &mut self,
        event: &buck2_data::BuckEvent,
        span_end: &buck2_data::SpanEndEvent,
    ) {
        let Some(data) = span_end.data.as_ref() else {
            return;
        };
        let duration_us = span_end
            .duration
            .as_ref()
            .map(duration_micros)
            .unwrap_or_default()
            .max(1);
        if let Some(mut open) = self.open_spans.remove(&event.span_id) {
            extend_json_map(&mut open.args, profile_span_end_args(data));
            self.push_completed(ProfileCompletedSpan {
                span_id: open.span_id,
                parent_id: open.parent_id,
                name: open.name,
                lane: open.lane,
                category: open.category,
                start_us: open.start_us,
                duration_us,
                args: open.args,
            });
            return;
        }

        let Some(end_us) = timestamp_micros(event.timestamp.as_ref()) else {
            return;
        };
        let Some(details) = profile_span_end_details(data) else {
            return;
        };
        let mut args = details.args;
        extend_json_map(&mut args, profile_span_end_args(data));
        self.push_completed(ProfileCompletedSpan {
            span_id: event.span_id,
            parent_id: event.parent_id,
            name: details.name,
            lane: details.lane,
            category: details.category,
            start_us: end_us.saturating_sub(duration_us),
            duration_us,
            args,
        });
    }

    fn push_completed(&mut self, span: ProfileCompletedSpan) {
        if self.completed_spans.len() >= MAX_PROFILE_SPANS {
            self.dropped_spans = self.dropped_spans.saturating_add(1);
            return;
        }
        self.completed_spans.push(span);
    }

    fn command_end_profile_gzip(
        &self,
        event: &buck2_data::BuckEvent,
        span_end: &buck2_data::SpanEndEvent,
        command: &buck2_data::CommandEnd,
    ) -> Option<Vec<u8>> {
        let command_duration_us = span_end
            .duration
            .as_ref()
            .map(duration_micros)
            .unwrap_or(1)
            .max(1);
        let end_us = timestamp_micros(event.timestamp.as_ref()).unwrap_or(command_duration_us);
        let command_name = self
            .command_name
            .clone()
            .unwrap_or_else(|| command_end_name(command).to_owned());
        let mut args = serde_json::Map::new();
        args.insert(
            "outcome".to_owned(),
            serde_json::json!(if command.is_success {
                "success"
            } else {
                "failed"
            }),
        );
        args.insert(
            "exit_code".to_owned(),
            serde_json::json!(if command.is_success { 0 } else { 1 }),
        );
        args.insert(
            "trace_id".to_owned(),
            serde_json::json!(event.trace_id.as_str()),
        );
        let fallback = ProfileFallbackSpan {
            name: format!("buck2 {command_name}"),
            start_us: end_us.saturating_sub(command_duration_us),
            duration_us: command_duration_us,
            args,
        };
        gzip_profile_json(self.profile_json(Some(fallback)).ok()?)
    }

    fn invocation_profile_gzip(&self, record: &buck2_data::InvocationRecord) -> Option<Vec<u8>> {
        if self.completed_spans.is_empty() {
            return None;
        }
        let command_duration_us = record
            .command_duration
            .as_ref()
            .or(record.client_walltime.as_ref())
            .map(duration_micros)
            .unwrap_or_default()
            .max(1);
        let fallback_start = self
            .command_started_at_us
            .or_else(|| self.completed_spans.iter().map(|span| span.start_us).min())
            .unwrap_or_default();
        let command = record
            .command_name
            .as_deref()
            .or(self.command_name.as_deref())
            .unwrap_or("unknown");
        let mut args = serde_json::Map::new();
        args.insert(
            "outcome".to_owned(),
            serde_json::json!(invocation_outcome_name(record.outcome)),
        );
        if let Some(exit_code) = record.exit_code {
            args.insert("exit_code".to_owned(), serde_json::json!(exit_code));
        }
        let fallback = ProfileFallbackSpan {
            name: format!("buck2 {command}"),
            start_us: fallback_start,
            duration_us: command_duration_us,
            args,
        };
        gzip_profile_json(self.profile_json(Some(fallback)).ok()?)
    }

    fn profile_json(&self, fallback: Option<ProfileFallbackSpan>) -> serde_json::Result<String> {
        let mut spans = self.completed_spans.clone();
        let has_command_span = spans.iter().any(|span| span.lane == ProfileLane::Command);
        if !has_command_span && let Some(fallback) = fallback {
            spans.push(ProfileCompletedSpan {
                span_id: 0,
                parent_id: 0,
                name: fallback.name,
                lane: ProfileLane::Command,
                category: "command",
                start_us: fallback.start_us,
                duration_us: fallback.duration_us,
                args: fallback.args,
            });
        }
        let base_us = self
            .command_started_at_us
            .or_else(|| spans.iter().map(|span| span.start_us).min())
            .unwrap_or_default();

        spans.sort_by(|left, right| {
            left.start_us
                .cmp(&right.start_us)
                .then_with(|| right.duration_us.cmp(&left.duration_us))
                .then_with(|| left.span_id.cmp(&right.span_id))
        });

        let mut trace_events = Vec::new();
        let mut lane_tracks: BTreeMap<ProfileLane, Vec<i64>> = BTreeMap::new();
        let mut span_tracks: BTreeMap<u64, (u64, ProfileLane)> = BTreeMap::new();

        for span in spans {
            let start_us = span.start_us.saturating_sub(base_us).max(0);
            let duration_us = span.duration_us.max(1);
            let end_us = start_us.saturating_add(duration_us);
            let parent_track = (span.parent_id != 0)
                .then(|| span_tracks.get(&span.parent_id).copied())
                .flatten();
            let tid = if let Some((parent_tid, parent_lane)) = parent_track
                && parent_lane != ProfileLane::Command
                && (parent_lane == span.lane
                    || (parent_lane == ProfileLane::Actions
                        && span.lane == ProfileLane::ActionStages))
            {
                parent_tid
            } else {
                let tracks = lane_tracks.entry(span.lane).or_default();
                let track_index = tracks
                    .iter()
                    .position(|last_end_us| *last_end_us <= start_us)
                    .unwrap_or_else(|| {
                        tracks.push(0);
                        tracks.len() - 1
                    });
                tracks[track_index] = end_us;
                span.lane.tid(track_index)
            };
            span_tracks.insert(span.span_id, (tid, span.lane));
            trace_events.push(serde_json::json!({
                "name": span.name,
                "cat": span.category,
                "ph": "X",
                "ts": start_us,
                "dur": duration_us,
                "pid": 1,
                "tid": tid,
                "args": span.args,
            }));
        }

        if self.dropped_spans > 0 {
            trace_events.push(serde_json::json!({
                "name": "profile spans dropped",
                "cat": "profile",
                "ph": "i",
                "s": "g",
                "ts": 0,
                "pid": 1,
                "args": {
                    "dropped_spans": self.dropped_spans,
                    "max_profile_spans": MAX_PROFILE_SPANS,
                },
            }));
        }

        let mut metadata_events = vec![serde_json::json!({
            "name": "process_name",
            "ph": "M",
            "pid": 1,
            "args": { "name": "buck2" },
        })];
        for (lane, tracks) in &lane_tracks {
            for index in 0..tracks.len() {
                let tid = lane.tid(index);
                let name = if tracks.len() == 1 {
                    lane.label().to_owned()
                } else {
                    format!("{} {}", lane.label(), index + 1)
                };
                metadata_events.push(serde_json::json!({
                    "name": "thread_name",
                    "ph": "M",
                    "pid": 1,
                    "tid": tid,
                    "args": { "name": name },
                }));
            }
        }
        metadata_events.extend(trace_events);
        serialize_trace_events(&metadata_events)
    }
}

pub(crate) fn encode_bep_event(event: &bep::BuildEvent) -> Any {
    Any {
        type_url: BEP_EVENT_TYPE_URL.to_owned(),
        value: event.encode_to_vec(),
    }
}

fn is_command_start(event: &buck2_data::BuckEvent) -> bool {
    matches!(
        event.data,
        Some(buck2_data::buck_event::Data::SpanStart(
            buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Command(_)),
            }
        ))
    )
}

fn started_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::Started(
            build_event_id::BuildStartedId {},
        )),
    }
}

fn structured_command_line_id(label: &str) -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::StructuredCommandLine(
            build_event_id::StructuredCommandLineId {
                command_line_label: label.to_owned(),
            },
        )),
    }
}

fn unstructured_command_line_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::UnstructuredCommandLine(
            build_event_id::UnstructuredCommandLineId {},
        )),
    }
}

fn options_parsed_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::OptionsParsed(
            build_event_id::OptionsParsedId {},
        )),
    }
}

fn workspace_status_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::WorkspaceStatus(
            build_event_id::WorkspaceStatusId {},
        )),
    }
}

fn workspace_config_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::Workspace(
            build_event_id::WorkspaceConfigId {},
        )),
    }
}

fn build_metadata_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::BuildMetadata(
            build_event_id::BuildMetadataId {},
        )),
    }
}

fn build_finished_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::BuildFinished(
            build_event_id::BuildFinishedId {},
        )),
    }
}

fn build_tool_logs_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::BuildToolLogs(
            build_event_id::BuildToolLogsId {},
        )),
    }
}

fn configuration_id(id: impl Into<String>) -> build_event_id::ConfigurationId {
    build_event_id::ConfigurationId { id: id.into() }
}

fn configuration_event_id(id: impl Into<String>) -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::Configuration(configuration_id(id))),
    }
}

fn configuration_id_from_bep(configuration: Option<&build_event_id::ConfigurationId>) -> String {
    configuration
        .map(|configuration| configuration.id.clone())
        .filter(|configuration| !configuration.is_empty())
        .unwrap_or_else(|| DEFAULT_CONFIGURATION_ID.to_owned())
}

fn target_key_from_completed_id(id: &build_event_id::TargetCompletedId) -> Option<TargetKey> {
    if id.label.is_empty() {
        return None;
    }
    Some(TargetKey {
        label: id.label.clone(),
        configuration: configuration_id_from_bep(id.configuration.as_ref()),
    })
}

fn target_key_from_action_id(id: &build_event_id::ActionCompletedId) -> Option<TargetKey> {
    if id.label.is_empty() {
        return None;
    }
    Some(TargetKey {
        label: id.label.clone(),
        configuration: configuration_id_from_bep(id.configuration.as_ref()),
    })
}

fn action_child_key(id: &build_event_id::ActionCompletedId) -> String {
    format!(
        "{}\0{}",
        id.primary_output,
        configuration_id_from_bep(id.configuration.as_ref())
    )
}

fn named_set_id_for_target(key: &TargetKey) -> build_event_id::NamedSetOfFilesId {
    build_event_id::NamedSetOfFilesId {
        id: format!("{}|{}|default", key.label, key.configuration),
    }
}

fn target_configured_id(label: String) -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::TargetConfigured(
            build_event_id::TargetConfiguredId {
                label,
                aspect: String::new(),
            },
        )),
    }
}

fn target_completed_id(label: String, configuration: String) -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::TargetCompleted(
            build_event_id::TargetCompletedId {
                label,
                aspect: String::new(),
                configuration: Some(configuration_id(configuration)),
            },
        )),
    }
}

fn test_result_id(label: String, configuration: String) -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::TestResult(
            build_event_id::TestResultId {
                label,
                run: 1,
                shard: 1,
                attempt: 1,
                configuration: Some(configuration_id(configuration)),
            },
        )),
    }
}

fn test_summary_id(label: String, configuration: String) -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::TestSummary(
            build_event_id::TestSummaryId {
                label,
                configuration: Some(configuration_id(configuration)),
            },
        )),
    }
}

fn command_name(command: &buck2_data::CommandStart) -> String {
    use buck2_data::command_start::Data;

    match command.data.as_ref() {
        Some(Data::Build(_)) => "build",
        Some(Data::Targets(_)) => "targets",
        Some(Data::Query(_)) => "query",
        Some(Data::Cquery(_)) => "cquery",
        Some(Data::Test(_)) => "test",
        Some(Data::Audit(_)) => "audit",
        Some(Data::Docs(_)) => "docs",
        Some(Data::Clean(_)) => "clean",
        Some(Data::Aquery(_)) => "aquery",
        Some(Data::Install(_)) => "install",
        Some(Data::Materialize(_)) => "materialize",
        Some(Data::Profile(_)) => "profile",
        Some(Data::Bxl(_)) => "bxl",
        Some(Data::Lsp(_)) => "lsp",
        Some(Data::FileStatus(_)) => "file-status",
        Some(Data::Starlark(_)) => "starlark",
        Some(Data::Subscribe(_)) => "subscribe",
        Some(Data::Trace(_)) => "trace",
        Some(Data::Ctargets(_)) => "ctargets",
        Some(Data::StarlarkDebugAttach(_)) => "starlark-debug-attach",
        Some(Data::Explain(_)) => "explain",
        Some(Data::ExpandExternalCell(_)) => "expand-external-cell",
        Some(Data::Complete(_)) => "complete",
        Some(Data::Hydration(_)) => "hydration",
        None => "unknown",
    }
    .to_owned()
}

fn command_end_name(command: &buck2_data::CommandEnd) -> &'static str {
    use buck2_data::command_end::Data;

    match command.data.as_ref() {
        Some(Data::Build(_)) => "build",
        Some(Data::Targets(_)) => "targets",
        Some(Data::Query(_)) => "query",
        Some(Data::Cquery(_)) => "cquery",
        Some(Data::Test(_)) => "test",
        Some(Data::Audit(_)) => "audit",
        Some(Data::Docs(_)) => "docs",
        Some(Data::Clean(_)) => "clean",
        Some(Data::Aquery(_)) => "aquery",
        Some(Data::Install(_)) => "install",
        Some(Data::Materialize(_)) => "materialize",
        Some(Data::Profile(_)) => "profile",
        Some(Data::Bxl(_)) => "bxl",
        Some(Data::Lsp(_)) => "lsp",
        Some(Data::FileStatus(_)) => "file-status",
        Some(Data::Starlark(_)) => "starlark",
        Some(Data::Subscribe(_)) => "subscribe",
        Some(Data::Trace(_)) => "trace",
        Some(Data::Ctargets(_)) => "ctargets",
        Some(Data::StarlarkDebugAttach(_)) => "starlark-debug-attach",
        Some(Data::Explain(_)) => "explain",
        Some(Data::ExpandExternalCell(_)) => "expand-external-cell",
        Some(Data::Complete(_)) => "complete",
        Some(Data::Hydration(_)) => "hydration",
        None => "unknown",
    }
}

fn unstructured_command_line_event(command: &buck2_data::CommandStart) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(unstructured_command_line_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::UnstructuredCommandLine(
            bep::UnstructuredCommandLine {
                args: command.cli_args.clone(),
            },
        )),
        last_message: false,
    }
}

fn original_structured_command_line_event(command: &buck2_data::CommandStart) -> bep::BuildEvent {
    let mut sections = Vec::new();
    if let Some(command) = command.cli_args.first() {
        sections.push(chunk_section("command", vec![command.clone()]));
    }
    if command.cli_args.len() > 1 {
        sections.push(chunk_section("arguments", command.cli_args[1..].to_vec()));
    }

    bep::BuildEvent {
        id: Some(structured_command_line_id(ORIGINAL_COMMAND_LINE_LABEL)),
        children: Vec::new(),
        payload: Some(build_event::Payload::StructuredCommandLine(
            cl::CommandLine {
                command_line_label: ORIGINAL_COMMAND_LINE_LABEL.to_owned(),
                sections,
            },
        )),
        last_message: false,
    }
}

fn canonical_structured_command_line_event(command: &buck2_data::CommandStart) -> bep::BuildEvent {
    let mut sections = Vec::new();
    if let Some(executable) = command.cli_args.first() {
        sections.push(chunk_section("executable", vec![executable.clone()]));
    }
    sections.push(chunk_section("command", vec![command_name(command)]));

    let parsed = ParsedCliArgs::new(command);
    let mut options = parsed.options;
    options.extend(client_env_options(command));
    if !options.is_empty() {
        sections.push(option_section("command options", options));
    }
    if !parsed.residue.is_empty() {
        sections.push(chunk_section("residue", parsed.residue));
    }

    bep::BuildEvent {
        id: Some(structured_command_line_id(CANONICAL_COMMAND_LINE_LABEL)),
        children: Vec::new(),
        payload: Some(build_event::Payload::StructuredCommandLine(
            cl::CommandLine {
                command_line_label: CANONICAL_COMMAND_LINE_LABEL.to_owned(),
                sections,
            },
        )),
        last_message: false,
    }
}

fn chunk_section(label: &str, chunks: Vec<String>) -> cl::CommandLineSection {
    cl::CommandLineSection {
        section_label: label.to_owned(),
        section_type: Some(cl::command_line_section::SectionType::ChunkList(
            cl::ChunkList { chunk: chunks },
        )),
    }
}

fn option_section(label: &str, options: Vec<cl::Option>) -> cl::CommandLineSection {
    cl::CommandLineSection {
        section_label: label.to_owned(),
        section_type: Some(cl::command_line_section::SectionType::OptionList(
            cl::OptionList { option: options },
        )),
    }
}

struct ParsedCliArgs {
    options: Vec<cl::Option>,
    residue: Vec<String>,
}

impl ParsedCliArgs {
    fn new(command: &buck2_data::CommandStart) -> Self {
        let args = command.cli_args.as_slice();
        let mut options = Vec::new();
        let mut residue = Vec::new();
        let mut i = command_arg_start(args);
        let mut force_residue = false;
        while i < args.len() {
            let arg = &args[i];
            if force_residue {
                residue.push(arg.clone());
                i += 1;
                continue;
            }
            if arg == "--" {
                force_residue = true;
                i += 1;
                continue;
            }
            if let Some(mut option) = option_from_arg(arg) {
                if option.option_value.is_empty()
                    && i + 1 < args.len()
                    && !args[i + 1].starts_with('-')
                {
                    option.option_value = args[i + 1].clone();
                    option.combined_form = format!("{}={}", arg, option.option_value);
                    i += 1;
                }
                if option.option_value.is_empty() {
                    option.option_value = "1".to_owned();
                }
                options.push(option);
            } else {
                residue.push(arg.clone());
            }
            i += 1;
        }

        Self { options, residue }
    }
}

fn command_arg_start(args: &[String]) -> usize {
    if args.is_empty() {
        return 0;
    }
    // Buck2 command args are generally: executable, command, command args...
    if args.len() > 1 && !args[1].starts_with('-') {
        2
    } else {
        1
    }
}

fn option_from_arg(arg: &str) -> Option<cl::Option> {
    let raw = arg.strip_prefix("--")?;
    if raw.is_empty() {
        return None;
    }
    let had_equals = raw.contains('=');
    let (raw_name, value) = raw
        .split_once('=')
        .map(|(name, value)| (name, value.to_owned()))
        .unwrap_or_else(|| (raw, String::new()));
    let (name, value) = raw_name
        .strip_prefix("no")
        .filter(|name| !name.is_empty())
        .map(|name| (name, "0".to_owned()))
        .unwrap_or((raw_name, value));

    Some(cl::Option {
        combined_form: arg.to_owned(),
        option_name: normalize_option_name(name),
        option_value: if had_equals || value == "0" {
            value
        } else {
            String::new()
        },
        effect_tags: Vec::new(),
        metadata_tags: Vec::new(),
        source: "command line options".to_owned(),
    })
}

fn normalize_option_name(name: &str) -> String {
    name.trim_start_matches('-').replace('-', "_")
}

fn client_env_options(command: &buck2_data::CommandStart) -> Vec<cl::Option> {
    let mut values = BTreeMap::new();
    insert_env_option(
        &mut values,
        "USER",
        first_metadata(
            &command.metadata,
            &["USER", "BUILD_USER", "username", "user"],
        )
        .or_else(|| env::var("USER").ok()),
    );
    insert_env_option(
        &mut values,
        "HOST",
        first_metadata(
            &command.metadata,
            &["HOST", "BUILD_HOST", "hostname", "host"],
        )
        .or_else(|| env::var("HOSTNAME").ok()),
    );
    for key in [
        "CI",
        "GITHUB_ACTOR",
        "GITHUB_REPOSITORY",
        "GITHUB_REF",
        "GITHUB_HEAD_REF",
        "GITHUB_SHA",
        "BUILDKITE_BUILD_CREATOR",
        "BUILDKITE_REPO",
        "BUILDKITE_BRANCH",
        "BUILDKITE_COMMIT",
        "CIRCLE_USERNAME",
        "CIRCLE_REPOSITORY_URL",
        "CIRCLE_BRANCH",
        "CIRCLE_SHA1",
        "GIT_REPOSITORY_URL",
        "GIT_URL",
        "GIT_BRANCH",
        "GIT_COMMIT",
        "REPO_URL",
        "COMMIT_SHA",
    ] {
        insert_env_option(
            &mut values,
            key,
            first_metadata(&command.metadata, &[key]).or_else(|| env::var(key).ok()),
        );
    }

    values
        .into_iter()
        .map(|(key, value)| cl::Option {
            combined_form: format!("--client_env={key}={value}"),
            option_name: "client_env".to_owned(),
            option_value: format!("{key}={value}"),
            effect_tags: Vec::new(),
            metadata_tags: Vec::new(),
            source: "Buck2 metadata".to_owned(),
        })
        .collect()
}

fn insert_env_option(values: &mut BTreeMap<String, String>, key: &str, value: Option<String>) {
    if let Some(value) = value
        && !value.is_empty()
    {
        values.insert(key.to_owned(), value);
    }
}

fn options_parsed_event(command: &buck2_data::CommandStart) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(options_parsed_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::OptionsParsed(bep::OptionsParsed {
            startup_options: Vec::new(),
            explicit_startup_options: Vec::new(),
            cmd_line: command.cli_args.clone(),
            explicit_cmd_line: command.cli_args.clone(),
            invocation_policy: None,
            tool_tag: String::new(),
        })),
        last_message: false,
    }
}

fn workspace_status_event(command: &buck2_data::CommandStart) -> bep::BuildEvent {
    let mut items = Vec::new();
    add_workspace_item(
        &mut items,
        "BUILD_USER",
        first_metadata(
            &command.metadata,
            &["USER", "BUILD_USER", "username", "user"],
        )
        .or_else(|| env::var("USER").ok()),
    );
    add_workspace_item(
        &mut items,
        "BUILD_HOST",
        first_metadata(
            &command.metadata,
            &["HOST", "BUILD_HOST", "hostname", "host"],
        )
        .or_else(|| env::var("HOSTNAME").ok()),
    );
    add_workspace_item(
        &mut items,
        "BUILD_WORKING_DIRECTORY",
        first_metadata(
            &command.metadata,
            &["CWD", "PWD", "BUILD_WORKING_DIRECTORY"],
        ),
    );
    add_workspace_item(
        &mut items,
        "ROLE",
        first_metadata(&command.metadata, &["ROLE"]),
    );
    add_workspace_item(
        &mut items,
        "REPO_URL",
        first_metadata(&command.metadata, &["REPO_URL", "GIT_REPOSITORY_URL"]),
    );
    add_workspace_item(
        &mut items,
        "GIT_BRANCH",
        first_metadata(&command.metadata, &["BRANCH_NAME", "GIT_BRANCH"]),
    );
    add_workspace_item(
        &mut items,
        "COMMIT_SHA",
        first_metadata(&command.metadata, &["COMMIT_SHA", "GIT_COMMIT"]),
    );
    let patterns = target_patterns_from_cli(command);
    if !patterns.is_empty() {
        add_workspace_item(&mut items, "PATTERN", Some(patterns.join(" ")));
    }

    bep::BuildEvent {
        id: Some(workspace_status_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::WorkspaceStatus(
            bep::WorkspaceStatus { item: items },
        )),
        last_message: false,
    }
}

fn add_workspace_item(
    items: &mut Vec<bep::workspace_status::Item>,
    key: &str,
    value: Option<String>,
) {
    if let Some(value) = value
        && !value.is_empty()
    {
        items.push(bep::workspace_status::Item {
            key: key.to_owned(),
            value,
        });
    }
}

fn add_metadata_alias(metadata: &mut BTreeMap<String, String>, key: &str, value: Option<String>) {
    if let Some(value) = value
        && !value.is_empty()
    {
        metadata.entry(key.to_owned()).or_insert(value);
    }
}

fn default_configuration_event() -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(configuration_event_id(DEFAULT_CONFIGURATION_ID)),
        children: Vec::new(),
        payload: Some(build_event::Payload::Configuration(bep::Configuration {
            mnemonic: DEFAULT_CONFIGURATION_ID.to_owned(),
            platform_name: String::new(),
            cpu: env::consts::ARCH.to_owned(),
            make_variable: HashMap::new(),
            is_tool: false,
        })),
        last_message: false,
    }
}

fn workspace_info_event(command: &buck2_data::CommandStart) -> bep::BuildEvent {
    let local_exec_root = first_metadata(
        &command.metadata,
        &["BUCK_OUT", "BUCK2_EXEC_ROOT", "EXEC_ROOT", "REPO_ROOT"],
    )
    .unwrap_or_default();
    bep::BuildEvent {
        id: Some(workspace_config_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::WorkspaceInfo(bep::WorkspaceConfig {
            local_exec_root,
        })),
        last_message: false,
    }
}

fn build_metadata_from_command(
    command: &buck2_data::CommandStart,
    defaults: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut metadata = defaults.clone();
    metadata.extend(command.metadata.iter().map(|(k, v)| (k.clone(), v.clone())));
    add_metadata_alias(
        &mut metadata,
        "USER",
        first_metadata(
            &command.metadata,
            &["USER", "BUILD_USER", "username", "user"],
        )
        .or_else(|| env::var("USER").ok()),
    );
    add_metadata_alias(
        &mut metadata,
        "HOST",
        first_metadata(
            &command.metadata,
            &["HOST", "BUILD_HOST", "hostname", "host"],
        )
        .or_else(|| env::var("HOSTNAME").ok()),
    );
    add_metadata_alias(
        &mut metadata,
        "REPO_URL",
        first_metadata(&command.metadata, &["REPO_URL", "GIT_REPOSITORY_URL"]),
    );
    add_metadata_alias(
        &mut metadata,
        "BRANCH_NAME",
        first_metadata(&command.metadata, &["BRANCH_NAME", "GIT_BRANCH"]),
    );
    add_metadata_alias(
        &mut metadata,
        "COMMIT_SHA",
        first_metadata(&command.metadata, &["COMMIT_SHA", "GIT_COMMIT"]),
    );
    let patterns = target_patterns_from_cli(command);
    if !patterns.is_empty() {
        metadata.insert("PATTERN".to_owned(), patterns.join(" "));
    }
    if env::var("CI").is_ok_and(|ci| !ci.is_empty()) {
        metadata.entry("ROLE".to_owned()).or_insert("CI".to_owned());
    }
    metadata
        .entry(BUILDBUDDY_VISIBILITY_KEY.to_owned())
        .or_insert_with(|| BUILDBUDDY_PUBLIC_VISIBILITY.to_owned());
    metadata
}

fn build_metadata_from_invocation(
    record: &buck2_data::InvocationRecord,
) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    if let Some(command_name) = record.command_name.as_ref()
        && !command_name.is_empty()
    {
        metadata.insert("BUCK2_COMMAND".to_owned(), command_name.clone());
    }
    if !record.re_session_id.is_empty() {
        metadata.insert(
            "BUCK2_RE_SESSION_ID".to_owned(),
            record.re_session_id.clone(),
        );
    }
    if let Some(parsed_target_patterns) = record.parsed_target_patterns.as_ref() {
        let patterns = target_patterns_from_invocation_record(parsed_target_patterns);
        if !patterns.is_empty() {
            metadata.insert("PATTERN".to_owned(), patterns.join(" "));
        }
    }
    metadata
}

fn metadata_map_from_hash_map(metadata: &HashMap<String, String>) -> BTreeMap<String, String> {
    metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn build_metadata_event_with_defaults(
    defaults: &BTreeMap<String, String>,
    metadata: &BTreeMap<String, String>,
) -> bep::BuildEvent {
    let mut merged = defaults.clone();
    merged.extend(metadata.iter().map(|(k, v)| (k.clone(), v.clone())));
    build_metadata_event(&merged)
}

fn build_metadata_event(metadata: &BTreeMap<String, String>) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(build_metadata_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::BuildMetadata(bep::BuildMetadata {
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })),
        last_message: false,
    }
}

fn target_patterns_from_cli(command: &buck2_data::CommandStart) -> Vec<String> {
    ParsedCliArgs::new(command)
        .residue
        .into_iter()
        .filter(|arg| arg.starts_with("//") || arg.starts_with(':') || arg.starts_with('@'))
        .collect()
}

fn build_metrics_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::BuildMetrics(
            build_event_id::BuildMetricsId {},
        )),
    }
}

fn action_cache_statistics(
    record: &buck2_data::InvocationRecord,
) -> Option<blaze::ActionCacheStatistics> {
    let hits = i32_saturating_from_u64(record.run_action_cache_count);
    let misses = i32_saturating_from_u64(
        record
            .run_local_count
            .saturating_add(record.run_remote_count),
    );
    if hits == 0 && misses == 0 {
        return None;
    }

    let miss_details = (misses > 0)
        .then(|| blaze::action_cache_statistics::MissDetail {
            reason: blaze::action_cache_statistics::MissReason::NotCached as i32,
            count: misses,
        })
        .into_iter()
        .collect();

    Some(blaze::ActionCacheStatistics {
        size_in_bytes: 0,
        save_time_in_ms: 0,
        hits,
        misses,
        miss_details,
        load_time_in_ms: 0,
    })
}

fn i32_saturating_from_u64(count: u64) -> i32 {
    i32::try_from(count).unwrap_or(i32::MAX)
}

fn build_metrics_event(record: &buck2_data::InvocationRecord) -> bep::BuildEvent {
    let actions_executed =
        record.run_local_count + record.run_remote_count + record.run_action_cache_count;
    let runner_count = [
        ("local", record.run_local_count, "local"),
        ("remote", record.run_remote_count, "remote"),
        (
            "remote cache hit",
            record.run_action_cache_count,
            "remote-cache",
        ),
        ("skipped", record.run_skipped_count, "skipped"),
    ]
    .into_iter()
    .filter(|(_, count, _)| *count > 0)
    .map(
        |(name, count, exec_kind)| bep::build_metrics::action_summary::RunnerCount {
            name: name.to_owned(),
            count: i32::try_from(count).unwrap_or(i32::MAX),
            exec_kind: exec_kind.to_owned(),
        },
    )
    .collect();

    let wall_time_in_ms = record
        .client_walltime
        .as_ref()
        .or(record.command_duration.as_ref())
        .map(duration_millis)
        .unwrap_or_default();
    let execution_phase_time_in_ms = match (
        record.time_to_first_action_execution_ms,
        record.time_to_last_action_execution_end_ms,
    ) {
        (Some(start), Some(end)) if end >= start => i64::try_from(end - start).unwrap_or(i64::MAX),
        _ => 0,
    };

    bep::BuildEvent {
        id: Some(build_metrics_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::BuildMetrics(bep::BuildMetrics {
            action_summary: Some(bep::build_metrics::ActionSummary {
                actions_created: i64::try_from(actions_executed).unwrap_or(i64::MAX),
                actions_created_not_including_aspects: i64::try_from(actions_executed)
                    .unwrap_or(i64::MAX),
                actions_executed: i64::try_from(actions_executed).unwrap_or(i64::MAX),
                action_data: Vec::new(),
                remote_cache_hits: i64::try_from(record.run_action_cache_count).unwrap_or(i64::MAX),
                runner_count,
                action_cache_statistics: action_cache_statistics(record),
            }),
            memory_metrics: record.peak_process_memory_bytes.map(|peak| {
                bep::build_metrics::MemoryMetrics {
                    used_heap_size_post_build: i64::try_from(peak).unwrap_or(i64::MAX),
                    peak_post_gc_heap_size: i64::try_from(peak).unwrap_or(i64::MAX),
                    peak_post_gc_tenured_space_heap_size: 0,
                    garbage_metrics: Vec::new(),
                }
            }),
            target_metrics: record.analysis_count.map(|count| {
                let count = i64::try_from(count).unwrap_or(i64::MAX);
                bep::build_metrics::TargetMetrics {
                    targets_loaded: 0,
                    targets_configured: count,
                    targets_configured_not_including_aspects: count,
                }
            }),
            package_metrics: None,
            timing_metrics: Some(bep::build_metrics::TimingMetrics {
                cpu_time_in_ms: 0,
                wall_time_in_ms,
                analysis_phase_time_in_ms: record
                    .time_to_first_analysis_ms
                    .and_then(|start| {
                        record
                            .time_to_first_action_execution_ms
                            .and_then(|end| end.checked_sub(start))
                    })
                    .map(|duration| i64::try_from(duration).unwrap_or(i64::MAX))
                    .unwrap_or_default(),
                execution_phase_time_in_ms,
                actions_execution_start_in_ms: record
                    .time_to_first_action_execution_ms
                    .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
                    .unwrap_or_default(),
                critical_path_time: record.critical_path_duration.clone(),
            }),
            cumulative_metrics: record.analysis_count.map(|count| {
                let count = i32::try_from(count).unwrap_or(i32::MAX);
                bep::build_metrics::CumulativeMetrics {
                    num_analyses: count,
                    num_builds: count,
                }
            }),
            artifact_metrics: Some(bep::build_metrics::ArtifactMetrics {
                source_artifacts_read: None,
                output_artifacts_seen: Some(bep::build_metrics::artifact_metrics::FilesMetric {
                    size_in_bytes: record
                        .materialization_output_size
                        .map(|size| i64::try_from(size).unwrap_or(i64::MAX))
                        .unwrap_or_default(),
                    count: 0,
                }),
                output_artifacts_from_action_cache: None,
                top_level_artifacts: None,
            }),
            build_graph_metrics: None,
            worker_metrics: Vec::new(),
            network_metrics: None,
            worker_pool_metrics: None,
            dynamic_execution_metrics: None,
            remote_analysis_cache_statistics: None,
        })),
        last_message: false,
    }
}

fn duration_millis(duration: &prost_types::Duration) -> i64 {
    let millis = i128::from(duration.seconds) * 1_000 + i128::from(duration.nanos) / 1_000_000;
    i64::try_from(millis).unwrap_or(if millis.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

fn optional_duration_millis(duration: Option<&prost_types::Duration>) -> Option<i64> {
    duration.map(duration_millis)
}

fn duration_micros(duration: &prost_types::Duration) -> i64 {
    duration
        .seconds
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(duration.nanos) / 1_000)
}

fn timestamp_micros(timestamp: Option<&Timestamp>) -> Option<i64> {
    let timestamp = timestamp?;
    let micros = i128::from(timestamp.seconds) * 1_000_000 + i128::from(timestamp.nanos) / 1_000;
    Some(i64::try_from(micros).unwrap_or(if micros.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    }))
}

fn target_patterns_from_invocation_record(
    patterns: &buck2_data::ParsedTargetPatterns,
) -> Vec<String> {
    patterns
        .target_patterns
        .iter()
        .map(|pattern| pattern.value.clone())
        .filter(|pattern| !pattern.is_empty())
        .collect()
}

fn invocation_outcome_name(outcome: Option<i32>) -> &'static str {
    match outcome.and_then(|outcome| buck2_data::InvocationOutcome::try_from(outcome).ok()) {
        Some(buck2_data::InvocationOutcome::Success) => "success",
        Some(buck2_data::InvocationOutcome::Failed) => "failed",
        Some(buck2_data::InvocationOutcome::Cancelled) => "cancelled",
        Some(buck2_data::InvocationOutcome::Crashed) => "crashed",
        _ => "unknown",
    }
}

fn profile_span_start_details(
    data: &buck2_data::span_start_event::Data,
) -> Option<ProfileSpanDetails> {
    match data {
        buck2_data::span_start_event::Data::Command(command) => {
            let command_name = command_name(command);
            let mut details = profile_details(
                format!("buck2 {command_name}"),
                ProfileLane::Command,
                "command",
            );
            json_arg_u64(
                &mut details.args,
                "argv_count",
                command.cli_args.len() as u64,
            );
            Some(details)
        }
        buck2_data::span_start_event::Data::CommandCritical(command) => {
            let mut details =
                profile_details("command critical section", ProfileLane::Command, "command");
            json_arg_non_empty(&mut details.args, "dice_version", &command.dice_version);
            Some(details)
        }
        buck2_data::span_start_event::Data::Analysis(analysis) => {
            let target = configured_target_from_analysis_start(analysis)
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "unknown target".to_owned());
            let mut details = profile_details(
                format!("analysis {target}"),
                ProfileLane::Analysis,
                "analysis",
            );
            json_arg_non_empty(&mut details.args, "rule", &analysis.rule);
            Some(details)
        }
        buck2_data::span_start_event::Data::AnalysisResolveQueries(analysis) => {
            let target = analysis
                .standard_target
                .as_ref()
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "unknown target".to_owned());
            let details = profile_details(
                format!("analysis queries {target}"),
                ProfileLane::Analysis,
                "analysis",
            );
            Some(details)
        }
        buck2_data::span_start_event::Data::AnalysisStage(_) => Some(profile_details(
            "evaluate rule",
            ProfileLane::Analysis,
            "analysis",
        )),
        buck2_data::span_start_event::Data::Load(load) => {
            let module = if load.module_id.is_empty() {
                load.cell.as_str()
            } else {
                load.module_id.as_str()
            };
            let mut details =
                profile_details(format!("load {module}"), ProfileLane::Loading, "loading");
            json_arg_non_empty(&mut details.args, "module_id", &load.module_id);
            json_arg_non_empty(&mut details.args, "cell", &load.cell);
            Some(details)
        }
        buck2_data::span_start_event::Data::LoadPackage(load) => {
            let mut details = profile_details(
                format!("list package {}", load.path),
                ProfileLane::Loading,
                "loading",
            );
            json_arg_non_empty(&mut details.args, "path", &load.path);
            Some(details)
        }
        buck2_data::span_start_event::Data::ActionExecution(action) => {
            Some(profile_action_start_details(action))
        }
        buck2_data::span_start_event::Data::ExecutorStage(stage) => {
            profile_executor_stage_name(stage.stage.as_ref())
                .map(|name| profile_details(name, ProfileLane::ActionStages, "action_stage"))
        }
        buck2_data::span_start_event::Data::TestDiscovery(test) => {
            let target = test
                .target_label
                .as_ref()
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "unknown target".to_owned());
            let mut details = profile_details(
                format!("discover tests {target}"),
                ProfileLane::Tests,
                "test",
            );
            json_arg_non_empty(&mut details.args, "suite", &test.suite_name);
            json_arg_non_empty(&mut details.args, "target", &target);
            Some(details)
        }
        buck2_data::span_start_event::Data::TestRun(test) => {
            let suite = test.suite.as_ref();
            let target = suite
                .and_then(|suite| suite.target_label.as_ref())
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "unknown target".to_owned());
            let mut details = profile_details(format!("test {target}"), ProfileLane::Tests, "test");
            if let Some(suite) = suite {
                json_arg_non_empty(&mut details.args, "suite", &suite.suite_name);
                json_arg_u64(
                    &mut details.args,
                    "test_count",
                    suite.test_names.len() as u64,
                );
            }
            json_arg_non_empty(&mut details.args, "target", &target);
            Some(details)
        }
        buck2_data::span_start_event::Data::FileWatcher(_) => Some(profile_details(
            "file watcher sync",
            ProfileLane::Loading,
            "loading",
        )),
        buck2_data::span_start_event::Data::FinalMaterialization(materialization) => {
            let mut details = profile_details(
                "final materialization",
                ProfileLane::Materialization,
                "materialization",
            );
            if let Some(artifact) = materialization.artifact.as_ref() {
                json_arg_non_empty(&mut details.args, "path", &artifact.path);
            }
            Some(details)
        }
        buck2_data::span_start_event::Data::Materialization(materialization) => {
            let mut details = profile_details(
                "materialization",
                ProfileLane::Materialization,
                "materialization",
            );
            if let Some(digest) = materialization.action_digest.as_ref() {
                json_arg_non_empty(&mut details.args, "action_digest", digest);
            }
            Some(details)
        }
        buck2_data::span_start_event::Data::CacheUpload(upload) => Some(
            profile_cache_upload_details(&upload.key, &upload.name, &upload.action_digest),
        ),
        buck2_data::span_start_event::Data::DepFileUpload(upload) => Some(
            profile_cache_upload_details(&upload.key, &upload.name, &upload.remote_dep_file_key),
        ),
        buck2_data::span_start_event::Data::ReUpload(_) => Some(profile_details(
            "remote upload",
            ProfileLane::Cache,
            "remote",
        )),
        buck2_data::span_start_event::Data::DeferredPreparationStage(_) => Some(profile_details(
            "deferred preparation",
            ProfileLane::Materialization,
            "materialization",
        )),
        buck2_data::span_start_event::Data::CreateOutputSymlinks(_) => Some(profile_details(
            "create output symlinks",
            ProfileLane::Materialization,
            "materialization",
        )),
        buck2_data::span_start_event::Data::LocalResources(_) => Some(profile_details(
            "setup local resources",
            ProfileLane::Other,
            "local_resources",
        )),
        buck2_data::span_start_event::Data::ReleaseLocalResources(_) => Some(profile_details(
            "release local resources",
            ProfileLane::Other,
            "local_resources",
        )),
        buck2_data::span_start_event::Data::DynamicLambda(lambda) => {
            let target = dynamic_lambda_target_label(lambda)
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "dynamic lambda".to_owned());
            let mut details = profile_details(
                format!("dynamic lambda {target}"),
                ProfileLane::Analysis,
                "analysis",
            );
            json_arg_u64(
                &mut details.args,
                "dynamic_inputs_bytes",
                lambda.dynamic_inputs_bytes,
            );
            Some(details)
        }
        buck2_data::span_start_event::Data::DiceStateUpdate(_)
        | buck2_data::span_start_event::Data::DiceCriticalSection(_)
        | buck2_data::span_start_event::Data::DiceBlockConcurrentCommand(_)
        | buck2_data::span_start_event::Data::DiceSynchronizeSection(_)
        | buck2_data::span_start_event::Data::DiceCleanup(_) => Some(profile_details(
            profile_dice_span_name(data),
            ProfileLane::Dice,
            "dice",
        )),
        buck2_data::span_start_event::Data::ExclusiveCommandWait(wait) => {
            let mut details = profile_details(
                "wait for exclusive command",
                ProfileLane::Command,
                "command",
            );
            if let Some(command_name) = wait.command_name.as_ref() {
                json_arg_non_empty(&mut details.args, "active_command", command_name);
            }
            Some(details)
        }
        buck2_data::span_start_event::Data::BxlExecution(bxl) => {
            let mut details =
                profile_details(format!("bxl {}", bxl.name), ProfileLane::Other, "bxl");
            json_arg_non_empty(&mut details.args, "name", &bxl.name);
            Some(details)
        }
        buck2_data::span_start_event::Data::BxlDiceInvocation(_) => {
            Some(profile_details("bxl dice", ProfileLane::Dice, "dice"))
        }
        buck2_data::span_start_event::Data::BxlEnsureArtifacts(_) => Some(profile_details(
            "bxl ensure artifacts",
            ProfileLane::Materialization,
            "materialization",
        )),
        buck2_data::span_start_event::Data::ActionErrorHandlerExecution(_) => Some(
            profile_details("action error handler", ProfileLane::Actions, "action"),
        ),
        buck2_data::span_start_event::Data::CqueryUniverseBuild(_) => Some(profile_details(
            "cquery universe build",
            ProfileLane::Analysis,
            "analysis",
        )),
        buck2_data::span_start_event::Data::ComputeDetailedAggregatedMetrics(_) => Some(
            profile_details("compute detailed metrics", ProfileLane::Other, "metrics"),
        ),
        buck2_data::span_start_event::Data::InstallEventInfo(install) => {
            let mut details = profile_details(
                format!("install {}", install.artifact_name),
                ProfileLane::Other,
                "install",
            );
            json_arg_non_empty(&mut details.args, "artifact_name", &install.artifact_name);
            json_arg_non_empty(&mut details.args, "file_path", &install.file_path);
            Some(details)
        }
        buck2_data::span_start_event::Data::ConnectToInstaller(_) => Some(profile_details(
            "connect to installer",
            ProfileLane::Other,
            "install",
        )),
        buck2_data::span_start_event::Data::Fake(_) => None,
        _ => None,
    }
}

fn profile_span_end_details(data: &buck2_data::span_end_event::Data) -> Option<ProfileSpanDetails> {
    match data {
        buck2_data::span_end_event::Data::Command(command) => Some(profile_details(
            format!("buck2 {}", command_end_name(command)),
            ProfileLane::Command,
            "command",
        )),
        buck2_data::span_end_event::Data::Analysis(analysis) => {
            let target = configured_target_from_analysis_end(analysis)
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "unknown target".to_owned());
            let mut details = profile_details(
                format!("analysis {target}"),
                ProfileLane::Analysis,
                "analysis",
            );
            json_arg_non_empty(&mut details.args, "rule", &analysis.rule);
            Some(details)
        }
        buck2_data::span_end_event::Data::ActionExecution(action) => {
            Some(profile_action_end_details(action))
        }
        buck2_data::span_end_event::Data::Load(load) => {
            let module = if load.module_id.is_empty() {
                load.cell.as_str()
            } else {
                load.module_id.as_str()
            };
            let mut details =
                profile_details(format!("load {module}"), ProfileLane::Loading, "loading");
            json_arg_non_empty(&mut details.args, "module_id", &load.module_id);
            json_arg_non_empty(&mut details.args, "cell", &load.cell);
            Some(details)
        }
        buck2_data::span_end_event::Data::LoadPackage(load) => {
            let mut details = profile_details(
                format!("list package {}", load.path),
                ProfileLane::Loading,
                "loading",
            );
            json_arg_non_empty(&mut details.args, "path", &load.path);
            Some(details)
        }
        buck2_data::span_end_event::Data::ExecutorStage(stage) => {
            let _ = stage;
            Some(profile_details(
                "executor stage",
                ProfileLane::ActionStages,
                "action_stage",
            ))
        }
        buck2_data::span_end_event::Data::TestDiscovery(test) => {
            let target = test
                .target_label
                .as_ref()
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "unknown target".to_owned());
            let mut details = profile_details(
                format!("discover tests {target}"),
                ProfileLane::Tests,
                "test",
            );
            json_arg_non_empty(&mut details.args, "suite", &test.suite_name);
            json_arg_non_empty(&mut details.args, "target", &target);
            Some(details)
        }
        buck2_data::span_end_event::Data::TestRun(test) => {
            let suite = test.suite.as_ref();
            let target = suite
                .and_then(|suite| suite.target_label.as_ref())
                .and_then(label_for_configured_target)
                .unwrap_or_else(|| "unknown target".to_owned());
            let mut details = profile_details(format!("test {target}"), ProfileLane::Tests, "test");
            if let Some(suite) = suite {
                json_arg_non_empty(&mut details.args, "suite", &suite.suite_name);
                json_arg_u64(
                    &mut details.args,
                    "test_count",
                    suite.test_names.len() as u64,
                );
            }
            json_arg_non_empty(&mut details.args, "target", &target);
            Some(details)
        }
        buck2_data::span_end_event::Data::CacheUpload(upload) => Some(
            profile_cache_upload_details(&upload.key, &upload.name, &upload.action_digest),
        ),
        buck2_data::span_end_event::Data::DepFileUpload(upload) => Some(
            profile_cache_upload_details(&upload.key, &upload.name, &upload.remote_dep_file_key),
        ),
        buck2_data::span_end_event::Data::Materialization(materialization) => {
            let mut details = profile_details(
                "materialization",
                ProfileLane::Materialization,
                "materialization",
            );
            if let Some(digest) = materialization.action_digest.as_ref() {
                json_arg_non_empty(&mut details.args, "action_digest", digest);
            }
            Some(details)
        }
        buck2_data::span_end_event::Data::FinalMaterialization(materialization) => {
            let mut details = profile_details(
                "final materialization",
                ProfileLane::Materialization,
                "materialization",
            );
            if let Some(artifact) = materialization.artifact.as_ref() {
                json_arg_non_empty(&mut details.args, "path", &artifact.path);
            }
            Some(details)
        }
        buck2_data::span_end_event::Data::CommandCritical(_) => Some(profile_details(
            "command critical section",
            ProfileLane::Command,
            "command",
        )),
        buck2_data::span_end_event::Data::SpanCancelled(_) => Some(profile_details(
            "cancelled span",
            ProfileLane::Other,
            "cancelled",
        )),
        _ => None,
    }
}

fn profile_span_end_args(
    data: &buck2_data::span_end_event::Data,
) -> serde_json::Map<String, serde_json::Value> {
    let mut args = serde_json::Map::new();
    match data {
        buck2_data::span_end_event::Data::Command(command) => {
            json_arg_bool(&mut args, "success", command.is_success);
            json_arg_i64(
                &mut args,
                "exit_code",
                if command.is_success { 0 } else { 1 },
            );
        }
        buck2_data::span_end_event::Data::ActionExecution(action) => {
            json_arg_bool(&mut args, "failed", action.failed);
            json_arg_u64(&mut args, "output_size", action.output_size);
            json_arg_u64(&mut args, "output_count", action.outputs.len() as u64);
            if let Ok(kind) = buck2_data::ActionExecutionKind::try_from(action.execution_kind) {
                json_arg_non_empty(&mut args, "execution_kind", &format!("{kind:?}"));
            }
            if let Some(command) = action.commands.last() {
                json_arg_non_empty(
                    &mut args,
                    "strategy",
                    action_execution_strategy_for_profile(command),
                );
                add_command_profile_args(&mut args, command);
            }
        }
        buck2_data::span_end_event::Data::Analysis(analysis) => {
            if let Some(declared_actions) = analysis.declared_actions {
                json_arg_u64(&mut args, "declared_actions", declared_actions);
            }
            if let Some(declared_artifacts) = analysis.declared_artifacts {
                json_arg_u64(&mut args, "declared_artifacts", declared_artifacts);
            }
            if let Some(profile) = analysis.profile.as_ref() {
                json_arg_u64(
                    &mut args,
                    "starlark_allocated_bytes",
                    profile.starlark_allocated_bytes,
                );
                json_arg_u64(
                    &mut args,
                    "starlark_available_bytes",
                    profile.starlark_available_bytes,
                );
            }
        }
        buck2_data::span_end_event::Data::Load(load) => {
            if let Some(error) = load.error.as_ref() {
                json_arg_non_empty(&mut args, "error", error);
            }
            if let Some(target_count) = load.target_count {
                json_arg_u64(&mut args, "target_count", target_count);
            }
            if let Some(bytes) = load.starlark_peak_allocated_bytes {
                json_arg_u64(&mut args, "starlark_peak_allocated_bytes", bytes);
            }
            if let Some(ticks) = load.starlark_tick_count {
                json_arg_u64(&mut args, "starlark_tick_count", ticks);
            }
            if let Some(instructions) = load.cpu_instruction_count {
                json_arg_u64(&mut args, "cpu_instruction_count", instructions);
            }
        }
        buck2_data::span_end_event::Data::TestDiscovery(test) => {
            json_arg_bool(&mut args, "re_cache_enabled", test.re_cache_enabled);
            if let Some(command) = test.command_report.as_ref() {
                add_command_profile_args(&mut args, command);
            }
        }
        buck2_data::span_end_event::Data::TestRun(test) => {
            if let Some(command) = test.command_report.as_ref() {
                add_command_profile_args(&mut args, command);
            }
        }
        buck2_data::span_end_event::Data::CacheUpload(upload) => {
            json_arg_bool(&mut args, "success", upload.success);
            json_arg_non_empty(&mut args, "error", &upload.error);
            json_arg_u64(&mut args, "file_count", upload.file_digests.len() as u64);
            json_arg_u64(&mut args, "tree_count", upload.tree_digests.len() as u64);
            if let Some(bytes) = upload.output_bytes {
                json_arg_u64(&mut args, "output_bytes", bytes);
            }
            if let Some(code) = upload.re_error_code.as_ref() {
                json_arg_non_empty(&mut args, "re_error_code", code);
            }
        }
        buck2_data::span_end_event::Data::DepFileUpload(upload) => {
            json_arg_bool(&mut args, "success", upload.success);
            json_arg_non_empty(&mut args, "error", &upload.error);
            if let Some(code) = upload.re_error_code.as_ref() {
                json_arg_non_empty(&mut args, "re_error_code", code);
            }
        }
        buck2_data::span_end_event::Data::Materialization(materialization) => {
            json_arg_u64(&mut args, "file_count", materialization.file_count);
            json_arg_u64(&mut args, "total_bytes", materialization.total_bytes);
            json_arg_non_empty(&mut args, "path", &materialization.path);
            json_arg_bool(&mut args, "success", materialization.success);
            if let Some(error) = materialization.error.as_ref() {
                json_arg_non_empty(&mut args, "error", error);
            }
            if let Some(method) = materialization
                .method
                .and_then(|method| buck2_data::MaterializationMethod::try_from(method).ok())
            {
                json_arg_non_empty(&mut args, "method", &format!("{method:?}"));
            }
        }
        buck2_data::span_end_event::Data::CreateOutputSymlinks(symlinks) => {
            json_arg_u64(&mut args, "created", symlinks.created);
        }
        buck2_data::span_end_event::Data::SpanCancelled(_) => {
            json_arg_bool(&mut args, "cancelled", true);
        }
        _ => {}
    }
    args
}

fn profile_details(
    name: impl Into<String>,
    lane: ProfileLane,
    category: &'static str,
) -> ProfileSpanDetails {
    ProfileSpanDetails {
        name: name.into(),
        lane,
        category,
        args: serde_json::Map::new(),
    }
}

fn profile_action_start_details(action: &buck2_data::ActionExecutionStart) -> ProfileSpanDetails {
    let mut details = profile_details(
        profile_action_name(action.key.as_ref(), action.name.as_ref(), action.kind),
        ProfileLane::Actions,
        "action",
    );
    add_action_identity_args(
        &mut details.args,
        action.key.as_ref(),
        action.name.as_ref(),
        action.kind,
    );
    details
}

fn profile_action_end_details(action: &buck2_data::ActionExecutionEnd) -> ProfileSpanDetails {
    let mut details = profile_details(
        profile_action_name(action.key.as_ref(), action.name.as_ref(), action.kind),
        ProfileLane::Actions,
        "action",
    );
    add_action_identity_args(
        &mut details.args,
        action.key.as_ref(),
        action.name.as_ref(),
        action.kind,
    );
    details
}

fn profile_cache_upload_details(
    key: &Option<buck2_data::ActionKey>,
    name: &Option<buck2_data::ActionName>,
    digest: &str,
) -> ProfileSpanDetails {
    let mut details = profile_details(
        format!(
            "upload {}",
            profile_action_name(key.as_ref(), name.as_ref(), 0)
        ),
        ProfileLane::Cache,
        "cache",
    );
    add_action_identity_args(&mut details.args, key.as_ref(), name.as_ref(), 0);
    json_arg_non_empty(&mut details.args, "digest", digest);
    details
}

fn profile_action_name(
    key: Option<&buck2_data::ActionKey>,
    name: Option<&buck2_data::ActionName>,
    kind: i32,
) -> String {
    let mnemonic = profile_action_mnemonic(name, kind);
    let label = key
        .and_then(action_owner)
        .and_then(label_for_configured_target);
    let identifier = name
        .map(|name| name.identifier.as_str())
        .filter(|identifier| !identifier.is_empty());
    match (label, identifier) {
        (Some(label), Some(identifier)) => format!("{mnemonic}:{identifier} {label}"),
        (Some(label), None) => format!("{mnemonic} {label}"),
        (None, Some(identifier)) => format!("{mnemonic}:{identifier}"),
        (None, None) => mnemonic,
    }
}

fn profile_action_mnemonic(name: Option<&buck2_data::ActionName>, kind: i32) -> String {
    if let Some(name) = name
        && !name.category.is_empty()
    {
        return name.category.clone();
    }
    buck2_data::ActionKind::try_from(kind)
        .ok()
        .map(|kind| format!("{kind:?}"))
        .unwrap_or_else(|| "Action".to_owned())
}

fn add_action_identity_args(
    args: &mut serde_json::Map<String, serde_json::Value>,
    key: Option<&buck2_data::ActionKey>,
    name: Option<&buck2_data::ActionName>,
    kind: i32,
) {
    if let Some(label) = key
        .and_then(action_owner)
        .and_then(label_for_configured_target)
    {
        json_arg_non_empty(args, "target", &label);
    }
    if let Some(key) = key {
        json_arg_non_empty(args, "action_key", &key.key);
    }
    if let Some(name) = name {
        json_arg_non_empty(args, "category", &name.category);
        json_arg_non_empty(args, "identifier", &name.identifier);
    }
    if let Ok(kind) = buck2_data::ActionKind::try_from(kind) {
        json_arg_non_empty(args, "action_kind", &format!("{kind:?}"));
    }
}

fn profile_executor_stage_name(
    stage: Option<&buck2_data::executor_stage_start::Stage>,
) -> Option<String> {
    Some(match stage? {
        buck2_data::executor_stage_start::Stage::Re(re) => profile_re_stage_name(re.stage.as_ref()),
        buck2_data::executor_stage_start::Stage::Local(local) => {
            profile_local_stage_name(local.stage.as_ref())
        }
        buck2_data::executor_stage_start::Stage::CacheQuery(query) => match query.cache_type() {
            buck2_data::CacheType::ActionCache => "action cache query".to_owned(),
            buck2_data::CacheType::RemoteDepFileCache => "dep-file cache query".to_owned(),
        },
        buck2_data::executor_stage_start::Stage::CacheHit(hit) => match hit.cache_type() {
            buck2_data::CacheType::ActionCache => "action cache hit".to_owned(),
            buck2_data::CacheType::RemoteDepFileCache => "dep-file cache hit".to_owned(),
        },
        buck2_data::executor_stage_start::Stage::Prepare(_) => "prepare action".to_owned(),
    })
}

fn profile_re_stage_name(stage: Option<&buck2_data::re_stage::Stage>) -> String {
    match stage {
        Some(buck2_data::re_stage::Stage::Execute(_)) => "remote execute",
        Some(buck2_data::re_stage::Stage::Download(_)) => "remote download",
        Some(buck2_data::re_stage::Stage::Queue(_)) => "remote queue",
        Some(buck2_data::re_stage::Stage::WorkerDownload(_)) => "remote worker download",
        Some(buck2_data::re_stage::Stage::WorkerUpload(_)) => "remote worker upload",
        Some(buck2_data::re_stage::Stage::Unknown(_)) => "remote unknown",
        Some(buck2_data::re_stage::Stage::MaterializeFailedInputs(_)) => {
            "remote materialize failed inputs"
        }
        Some(buck2_data::re_stage::Stage::BeforeActionExecution(_)) => "remote before action",
        Some(buck2_data::re_stage::Stage::AfterActionExecution(_)) => "remote after action",
        Some(buck2_data::re_stage::Stage::QueueOverQuota(_)) => "remote queue over quota",
        Some(buck2_data::re_stage::Stage::QueueNoWorkerAvailable(_)) => "remote queue no worker",
        Some(buck2_data::re_stage::Stage::QueueAcquiringDependencies(_)) => {
            "remote queue dependencies"
        }
        Some(buck2_data::re_stage::Stage::QueueCancelled(_)) => "remote queue cancelled",
        None => "remote stage",
    }
    .to_owned()
}

fn profile_local_stage_name(stage: Option<&buck2_data::local_stage::Stage>) -> String {
    match stage {
        Some(buck2_data::local_stage::Stage::Queued(_)) => "local queued",
        Some(buck2_data::local_stage::Stage::Execute(_)) => "local execute",
        Some(buck2_data::local_stage::Stage::MaterializeInputs(_)) => "local materialize inputs",
        Some(buck2_data::local_stage::Stage::PrepareOutputs(_)) => "local prepare outputs",
        Some(buck2_data::local_stage::Stage::AcquireLocalResource(_)) => "local acquire resource",
        Some(buck2_data::local_stage::Stage::WorkerInit(_)) => "worker init",
        Some(buck2_data::local_stage::Stage::WorkerExecute(_)) => "worker execute",
        Some(buck2_data::local_stage::Stage::WorkerQueued(_)) => "worker queued",
        Some(buck2_data::local_stage::Stage::WorkerWait(_)) => "worker wait",
        None => "local stage",
    }
    .to_owned()
}

fn dynamic_lambda_target_label(
    lambda: &buck2_data::DynamicLambdaStart,
) -> Option<&buck2_data::ConfiguredTargetLabel> {
    match lambda.owner.as_ref()? {
        buck2_data::dynamic_lambda_start::Owner::TargetLabel(target) => Some(target),
        buck2_data::dynamic_lambda_start::Owner::BxlKey(_)
        | buck2_data::dynamic_lambda_start::Owner::AnonTarget(_) => None,
    }
}

fn profile_dice_span_name(data: &buck2_data::span_start_event::Data) -> &'static str {
    match data {
        buck2_data::span_start_event::Data::DiceStateUpdate(_) => "dice state update",
        buck2_data::span_start_event::Data::DiceCriticalSection(_) => "dice critical section",
        buck2_data::span_start_event::Data::DiceBlockConcurrentCommand(_) => {
            "dice block concurrent command"
        }
        buck2_data::span_start_event::Data::DiceSynchronizeSection(_) => "dice synchronize",
        buck2_data::span_start_event::Data::DiceCleanup(_) => "dice cleanup",
        _ => "dice",
    }
}

fn action_execution_strategy_for_profile(command: &buck2_data::CommandExecution) -> &'static str {
    command
        .details
        .as_ref()
        .and_then(|details| details.command_kind.as_ref())
        .and_then(|kind| kind.command.as_ref())
        .map(|command| action_execution_strategy(Some(command)))
        .unwrap_or("unknown")
}

fn add_command_profile_args(
    args: &mut serde_json::Map<String, serde_json::Value>,
    command: &buck2_data::CommandExecution,
) {
    let Some(details) = command.details.as_ref() else {
        return;
    };
    if let Some(exit_code) = details.signed_exit_code {
        json_arg_i64(args, "exit_code", i64::from(exit_code));
    }
    json_arg_duration_ms(
        args,
        "wall_time_ms",
        details.metadata.as_ref().and_then(|m| m.wall_time.as_ref()),
    );
    json_arg_duration_ms(
        args,
        "execution_time_ms",
        details
            .metadata
            .as_ref()
            .and_then(|m| m.execution_time.as_ref()),
    );
    json_arg_duration_ms(
        args,
        "queue_duration_ms",
        details
            .metadata
            .as_ref()
            .and_then(|m| m.queue_duration.as_ref()),
    );
}

fn json_arg_non_empty(
    args: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    value: &str,
) {
    if !value.is_empty() {
        args.insert(name.to_owned(), serde_json::json!(value));
    }
}

fn json_arg_bool(args: &mut serde_json::Map<String, serde_json::Value>, name: &str, value: bool) {
    args.insert(name.to_owned(), serde_json::json!(value));
}

fn json_arg_i64(args: &mut serde_json::Map<String, serde_json::Value>, name: &str, value: i64) {
    args.insert(name.to_owned(), serde_json::json!(value));
}

fn json_arg_u64(args: &mut serde_json::Map<String, serde_json::Value>, name: &str, value: u64) {
    args.insert(name.to_owned(), serde_json::json!(value));
}

fn json_arg_duration_ms(
    args: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    duration: Option<&prost_types::Duration>,
) {
    if let Some(duration) = duration {
        json_arg_i64(args, name, duration_millis(duration));
    }
}

fn extend_json_map(
    args: &mut serde_json::Map<String, serde_json::Value>,
    more: serde_json::Map<String, serde_json::Value>,
) {
    for (key, value) in more {
        args.insert(key, value);
    }
}

fn serialize_trace_events(events: &[serde_json::Value]) -> serde_json::Result<String> {
    let mut profile = "{\"traceEvents\":[\n".to_owned();
    for (i, event) in events.iter().enumerate() {
        if i != 0 {
            profile.push_str(",\n");
        }
        profile.push_str(&serde_json::to_string(event)?);
    }
    profile.push_str("\n]}\n");
    Ok(profile)
}

fn gzip_profile_json(profile: String) -> Option<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(profile.as_bytes()).ok()?;
    encoder.finish().ok()
}

fn build_tool_logs_event_from_command_end(
    event: &buck2_data::BuckEvent,
    span_end: &buck2_data::SpanEndEvent,
    command: &buck2_data::CommandEnd,
    profile: &CommandProfileBuilder,
) -> bep::BuildEvent {
    let command_name = command_end_name(command);
    let outcome = if command.is_success {
        "success"
    } else {
        "failed"
    };
    let exit_code = if command.is_success { 0 } else { 1 };
    let duration_ms = optional_duration_millis(span_end.duration.as_ref());
    let summary = serde_json::json!({
        "tool": BUILD_TOOL_VERSION,
        "command": command_name,
        "outcome": outcome,
        "exit_code": exit_code,
        "timings_ms": {
            "command": duration_ms,
        },
    });
    let summary = serde_json::to_string_pretty(&summary).unwrap_or_else(|_| "{}".to_owned());
    let mut logs = vec![file_with_contents("buck2-invocation.json", summary)];
    if let Some(profile) = profile.command_end_profile_gzip(event, span_end, command) {
        logs.push(file_with_bytes(COMMAND_PROFILE_NAME, profile));
    }
    build_tool_logs_event(logs)
}

fn build_tool_logs_event_from_invocation(
    record: &buck2_data::InvocationRecord,
    profile: &CommandProfileBuilder,
) -> bep::BuildEvent {
    let target_patterns = record
        .parsed_target_patterns
        .as_ref()
        .map(target_patterns_from_invocation_record)
        .unwrap_or_default();
    let summary = serde_json::json!({
        "tool": BUILD_TOOL_VERSION,
        "command": record.command_name.as_deref().unwrap_or_default(),
        "outcome": invocation_outcome_name(record.outcome),
        "exit_code": record.exit_code,
        "exit_result_name": record.exit_result_name.as_deref(),
        "target_patterns": target_patterns,
        "tags": &record.tags,
        "re_session_id": record.re_session_id.as_str(),
        "filesystem": record.filesystem.as_str(),
        "cache_hit_rate": record.cache_hit_rate,
        "actions": {
            "local": record.run_local_count,
            "remote": record.run_remote_count,
            "remote_cache": record.run_action_cache_count,
            "remote_dep_file_cache": record.run_remote_dep_file_cache_count,
            "skipped": record.run_skipped_count,
            "fallback": record.run_fallback_count,
            "uploads": record.cache_upload_count,
            "upload_attempts": record.cache_upload_attempt_count,
        },
        "timings_ms": {
            "command": optional_duration_millis(record.command_duration.as_ref()),
            "client_walltime": optional_duration_millis(record.client_walltime.as_ref()),
            "critical_path": optional_duration_millis(record.critical_path_duration.as_ref()),
            "first_action_execution": record.time_to_first_action_execution_ms,
            "last_action_execution_end": record.time_to_last_action_execution_end_ms,
        },
        "remote_execution": {
            "upload_bytes": record.re_upload_bytes,
            "download_bytes": record.re_download_bytes,
            "max_upload_speed": record.re_max_upload_speed,
            "max_download_speed": record.re_max_download_speed,
            "avg_upload_speed": record.re_avg_upload_speed,
            "avg_download_speed": record.re_avg_download_speed,
        },
        "sink": {
            "success_count": record.sink_success_count,
            "failure_count": record.sink_failure_count,
            "dropped_count": record.sink_dropped_count,
            "bytes_written": record.sink_bytes_written,
            "max_buffer_depth": record.sink_max_buffer_depth,
        },
        "event_log": {
            "compressed_size_bytes": record.compressed_event_log_size_bytes,
            "event_count": record.event_count,
            "has_end_of_stream": record.has_end_of_stream,
        },
    });
    let summary = serde_json::to_string_pretty(&summary).unwrap_or_else(|_| "{}".to_owned());
    let mut logs = vec![file_with_contents("buck2-invocation.json", summary)];
    if let Some(profile) = profile
        .invocation_profile_gzip(record)
        .or_else(|| command_profile_gzip(record))
    {
        logs.push(file_with_bytes(COMMAND_PROFILE_NAME, profile));
    }
    build_tool_logs_event(logs)
}

fn build_tool_logs_event(logs: Vec<bep::File>) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(build_tool_logs_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::BuildToolLogs(bep::BuildToolLogs {
            log: logs,
        })),
        last_message: false,
    }
}

fn command_profile_gzip(record: &buck2_data::InvocationRecord) -> Option<Vec<u8>> {
    gzip_profile_json(command_profile_json(record).ok()?)
}

fn command_profile_json(record: &buck2_data::InvocationRecord) -> serde_json::Result<String> {
    let command_duration_us = record
        .command_duration
        .as_ref()
        .or(record.client_walltime.as_ref())
        .map(duration_micros)
        .unwrap_or_default()
        .max(1);
    let command = record.command_name.as_deref().unwrap_or("unknown");
    let outcome = invocation_outcome_name(record.outcome);
    let total_actions = record
        .run_local_count
        .saturating_add(record.run_remote_count)
        .saturating_add(record.run_action_cache_count)
        .saturating_add(record.run_remote_dep_file_cache_count)
        .saturating_add(record.run_skipped_count);

    let mut trace_events = vec![
        serde_json::json!({
            "name": "thread_name",
            "ph": "M",
            "pid": 1,
            "tid": 1,
            "args": { "name": "Buck2 command" },
        }),
        serde_json::json!({
            "name": format!("buck2 {command}"),
            "cat": "command",
            "ph": "X",
            "ts": 0,
            "dur": command_duration_us,
            "pid": 1,
            "tid": 1,
            "args": {
                "outcome": outcome,
                "exit_code": record.exit_code,
                "exit_result_name": record.exit_result_name.as_deref(),
                "cache_hit_rate": record.cache_hit_rate,
                "re_session_id": record.re_session_id.as_str(),
            },
        }),
        serde_json::json!({
            "name": "action count",
            "cat": "buck2",
            "ph": "C",
            "ts": command_duration_us,
            "pid": 1,
            "tid": 1,
            "args": {
                "action": total_actions,
                "local action": record.run_local_count,
                "remote action": record.run_remote_count,
                "local action cache": record.run_action_cache_count,
                "remote dep file cache": record.run_remote_dep_file_cache_count,
                "skipped action": record.run_skipped_count,
            },
        }),
    ];

    if let (Some(first), Some(last)) = (
        record.time_to_first_action_execution_ms,
        record.time_to_last_action_execution_end_ms,
    ) && last >= first
    {
        trace_events.push(serde_json::json!({
            "name": "thread_name",
            "ph": "M",
            "pid": 1,
            "tid": 2,
            "args": { "name": "Buck2 actions" },
        }));
        trace_events.push(serde_json::json!({
            "name": "action execution",
            "cat": "action",
            "ph": "X",
            "ts": first.saturating_mul(1000),
            "dur": last.saturating_sub(first).max(1).saturating_mul(1000),
            "pid": 1,
            "tid": 2,
            "args": {
                "local": record.run_local_count,
                "remote": record.run_remote_count,
                "remote_cache": record.run_action_cache_count,
                "skipped": record.run_skipped_count,
            },
        }));
    }

    if let Some(critical_path) = record.critical_path_duration.as_ref() {
        let critical_path_us = duration_micros(critical_path).max(1);
        trace_events.push(serde_json::json!({
            "name": "thread_name",
            "ph": "M",
            "pid": 1,
            "tid": 3,
            "args": { "name": "Buck2 critical path" },
        }));
        trace_events.push(serde_json::json!({
            "name": "critical path",
            "cat": "critical_path",
            "ph": "X",
            "ts": command_duration_us.saturating_sub(critical_path_us),
            "dur": critical_path_us,
            "pid": 1,
            "tid": 3,
            "args": {},
        }));
    }

    serialize_trace_events(&trace_events)
}

fn configured_target_children_from_build_targets(
    targets: &[buck2_data::BuildTarget],
) -> Vec<bep::BuildEventId> {
    targets
        .iter()
        .filter_map(|target| normalize_buck_label(&target.target))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(target_configured_id)
        .collect()
}

fn pattern_expanded_event(
    patterns: Vec<String>,
    children: Vec<bep::BuildEventId>,
) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(bep::BuildEventId {
            id: Some(build_event_id::Id::Pattern(
                build_event_id::PatternExpandedId {
                    pattern: patterns.clone(),
                },
            )),
        }),
        children,
        payload: Some(build_event::Payload::Expanded(bep::PatternExpanded {
            test_suite_expansions: Vec::new(),
        })),
        last_message: false,
    }
}

fn configured_event_from_analysis_start(
    analysis: &buck2_data::AnalysisStart,
) -> Option<bep::BuildEvent> {
    let target = configured_target_from_analysis_start(analysis)?;
    let label = label_for_configured_target(target)?;
    let mut target_kind = analysis.rule.trim().to_owned();
    if target_kind.is_empty() {
        target_kind = GENERIC_TARGET_KIND.to_owned();
    } else if !target_kind.ends_with(" rule") {
        target_kind.push_str(" rule");
    }
    Some(configured_event(label, target_kind))
}

fn completed_event_from_analysis_end(
    analysis: &buck2_data::AnalysisEnd,
) -> Option<bep::BuildEvent> {
    let target = configured_target_from_analysis_end(analysis)?;
    let label = label_for_configured_target(target)?;
    let configuration = configuration_id_for_target(target);
    Some(completed_event(label, configuration, true))
}

fn configured_event_from_test_label(
    label: Option<&buck2_data::ConfiguredTargetLabel>,
    target_kind: &str,
) -> Option<bep::BuildEvent> {
    Some(configured_event(
        label_for_configured_target(label?)?,
        target_kind.to_owned(),
    ))
}

fn configured_event_from_build_target(target: &buck2_data::BuildTarget) -> Option<bep::BuildEvent> {
    let label = normalize_buck_label(&target.target)?;
    Some(configured_event(label, GENERIC_TARGET_KIND.to_owned()))
}

fn completed_event_from_build_target(target: &buck2_data::BuildTarget) -> Option<bep::BuildEvent> {
    let label = normalize_buck_label(&target.target)?;
    Some(completed_event(
        label,
        if target.configuration.is_empty() {
            DEFAULT_CONFIGURATION_ID.to_owned()
        } else {
            target.configuration.clone()
        },
        true,
    ))
}

fn configured_event(label: String, target_kind: String) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(target_configured_id(label)),
        children: Vec::new(),
        payload: Some(build_event::Payload::Configured(bep::TargetConfigured {
            target_kind,
            test_size: bep::TestSize::Unknown as i32,
            tag: Vec::new(),
        })),
        last_message: false,
    }
}

fn completed_event(label: String, configuration: String, success: bool) -> bep::BuildEvent {
    completed_event_with_children(label, configuration, success, Vec::new())
}

fn completed_event_with_children(
    label: String,
    configuration: String,
    success: bool,
    children: Vec<bep::BuildEventId>,
) -> bep::BuildEvent {
    completed_event_with_children_and_outputs(label, configuration, success, children, Vec::new())
}

fn completed_event_with_children_and_outputs(
    label: String,
    configuration: String,
    success: bool,
    children: Vec<bep::BuildEventId>,
    output_group: Vec<bep::OutputGroup>,
) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(target_completed_id(label.clone(), configuration)),
        children,
        payload: Some(build_event::Payload::Completed(bep::TargetComplete {
            success,
            target_kind: String::new(),
            test_size: bep::TestSize::Unknown as i32,
            output_group,
            important_output: Vec::new(),
            tag: Vec::new(),
            test_timeout_seconds: 0,
            directory_output: Vec::new(),
            failure_detail: (!success)
                .then(|| execution_failure_detail(format!("Target failed: {label}"))),
            test_timeout: None,
        })),
        last_message: false,
    }
}

fn action_event(
    event: &buck2_data::BuckEvent,
    span_end: &buck2_data::SpanEndEvent,
    action: &buck2_data::ActionExecutionEnd,
) -> Option<bep::BuildEvent> {
    let key = action.key.as_ref()?;
    let target = action_owner(key)?;
    let label = label_for_configured_target(target)?;
    let configuration = configuration_id_for_target(target);
    let last_command = action.commands.last();
    let primary_output = action
        .outputs
        .first()
        .map(|o| o.tiny_digest.clone())
        .filter(|v| !v.is_empty())
        .or_else(|| (!key.key.is_empty()).then(|| key.key.clone()))
        .unwrap_or_else(|| label.clone());
    let stdout = last_command
        .and_then(|command| command.details.as_ref())
        .map(|details| details.cmd_stdout.clone())
        .unwrap_or_default();
    let stderr = last_command
        .and_then(|command| command.details.as_ref())
        .map(|details| details.cmd_stderr.clone())
        .unwrap_or_default();
    let exit_code = last_command
        .and_then(|command| command.details.as_ref())
        .and_then(|details| details.signed_exit_code)
        .unwrap_or(if action.failed { 1 } else { 0 });
    let end_time = event.timestamp.clone();
    let start_time = start_time_from_span_end(span_end, end_time.as_ref());
    let command_line = last_command
        .map(command_line_from_execution)
        .unwrap_or_default();
    let mnemonic = action_mnemonic(action);
    let strategy_details =
        action_strategy_details(action, last_command, &label, &mnemonic, exit_code);
    let failure_detail = action.failed.then(|| {
        execution_failure_detail(action_failure_detail_message(
            action, &label, &stderr, exit_code,
        ))
    });

    Some(bep::BuildEvent {
        id: Some(bep::BuildEventId {
            id: Some(build_event_id::Id::ActionCompleted(
                build_event_id::ActionCompletedId {
                    primary_output: primary_output.clone(),
                    label: label.clone(),
                    configuration: Some(configuration_id(configuration.clone())),
                },
            )),
        }),
        children: Vec::new(),
        payload: Some(build_event::Payload::Action(bep::ActionExecuted {
            success: !action.failed,
            exit_code,
            stdout: Some(file_with_contents("stdout", stdout)),
            stderr: Some(file_with_contents("stderr", stderr)),
            label,
            primary_output: Some(file_with_contents("primary_output", primary_output)),
            configuration: Some(configuration_id(configuration)),
            r#type: mnemonic,
            command_line,
            action_metadata_logs: Vec::new(),
            failure_detail,
            start_time,
            end_time,
            strategy_details,
        })),
        last_message: false,
    })
}

fn bep_file_from_action_output(output: &buck2_data::ActionOutput) -> Option<bep::File> {
    let name = if output.path.is_empty() {
        output.tiny_digest.clone()
    } else {
        output.path.clone()
    };
    if name.is_empty() {
        return None;
    }
    Some(bep::File {
        name,
        path_prefix: Vec::new(),
        file: action_output_uri(output).map(bep::file::File::Uri),
        digest: output.digest.clone(),
        length: i64::try_from(output.size).unwrap_or(i64::MAX),
    })
}

fn action_output_uri(output: &buck2_data::ActionOutput) -> Option<String> {
    // Buck2 does not yet know the BES bytestream authority here, so keep this
    // empty for now. The full digest and size are still useful for consumers
    // that can map Buck2 output digests to their own CAS.
    let _ = output;
    None
}

fn finished_event_from_invocation_record(
    event: &buck2_data::BuckEvent,
    record: &buck2_data::InvocationRecord,
) -> bep::BuildEvent {
    let mut exit_code = 1;
    let mut exit_name = "FAILED".to_owned();

    match buck2_data::InvocationOutcome::try_from(record.outcome.unwrap_or_default()) {
        Ok(buck2_data::InvocationOutcome::Success) => {
            exit_code = 0;
            exit_name = "SUCCESS".to_owned();
        }
        Ok(buck2_data::InvocationOutcome::Cancelled) => {
            exit_code = INTERRUPTED_EXIT_CODE;
            exit_name = "INTERRUPTED".to_owned();
        }
        Ok(buck2_data::InvocationOutcome::Crashed) => {
            exit_name = "CRASHED".to_owned();
        }
        Ok(buck2_data::InvocationOutcome::Failed)
        | Ok(buck2_data::InvocationOutcome::Unknown)
        | Err(_) => {}
    }

    if let Some(record_exit_code) = record.exit_code {
        exit_code = i32::try_from(record_exit_code).unwrap_or(i32::MAX);
        if record.exit_result_name.is_none() {
            exit_name = if exit_code == 0 { "SUCCESS" } else { "FAILED" }.to_owned();
        }
    }
    if let Some(record_exit_name) = record.exit_result_name.as_ref()
        && !record_exit_name.is_empty()
    {
        exit_name = record_exit_name.clone();
    }

    finished_event(event.timestamp.clone(), exit_code, exit_name)
}

fn finished_event_from_command_end(
    event: &buck2_data::BuckEvent,
    command: &buck2_data::CommandEnd,
) -> bep::BuildEvent {
    let (exit_code, exit_name) = if command.is_success {
        (0, "SUCCESS")
    } else {
        (1, "FAILED")
    };

    finished_event(event.timestamp.clone(), exit_code, exit_name)
}

fn finished_event(
    timestamp: Option<Timestamp>,
    exit_code: i32,
    exit_name: impl Into<String>,
) -> bep::BuildEvent {
    let exit_name = exit_name.into();
    bep::BuildEvent {
        id: Some(build_finished_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::Finished(bep::BuildFinished {
            overall_success: exit_code == 0,
            finish_time_millis: 0,
            anomaly_report: None,
            exit_code: Some(bep::build_finished::ExitCode {
                name: exit_name.clone(),
                code: exit_code,
            }),
            finish_time: timestamp,
            failure_detail: (exit_code != 0).then(|| {
                execution_failure_detail(format!(
                    "Buck2 invocation failed with {exit_name} ({exit_code})"
                ))
            }),
        })),
        last_message: true,
    }
}

fn execution_failure_detail(message: impl Into<String>) -> failure::FailureDetail {
    failure::FailureDetail {
        message: message.into(),
        category: Some(failure::failure_detail::Category::Execution(
            failure::Execution {
                code: failure::execution::Code::NonActionExecutionFailure as i32,
            },
        )),
    }
}

fn action_failure_detail_message(
    action: &buck2_data::ActionExecutionEnd,
    label: &str,
    stderr: &str,
    exit_code: i32,
) -> String {
    if let Some(message) = action.error.as_ref().and_then(action_error_detail_message) {
        return format!("Action failed for {label}: {message}");
    }
    let stderr = stderr.trim();
    if !stderr.is_empty() {
        return format!("Action failed for {label}: {}", first_line(stderr));
    }
    format!("Action failed for {label} with exit code {exit_code}")
}

fn action_error_detail_message(error: &buck2_data::action_execution_end::Error) -> Option<String> {
    match error {
        buck2_data::action_execution_end::Error::Unknown(message) => {
            (!message.is_empty()).then(|| message.clone())
        }
        buck2_data::action_execution_end::Error::MissingOutputs(_) => {
            Some("missing outputs".to_owned())
        }
        buck2_data::action_execution_end::Error::CommandExecutionError(_) => None,
    }
}

fn first_line(value: &str) -> &str {
    value.lines().next().unwrap_or(value)
}

fn action_owner(key: &buck2_data::ActionKey) -> Option<&buck2_data::ConfiguredTargetLabel> {
    match key.owner.as_ref()? {
        buck2_data::action_key::Owner::TargetLabel(label)
        | buck2_data::action_key::Owner::TestTargetLabel(label)
        | buck2_data::action_key::Owner::LocalResourceSetup(label) => Some(label),
        buck2_data::action_key::Owner::BxlKey(_) | buck2_data::action_key::Owner::AnonTarget(_) => {
            None
        }
    }
}

fn configured_target_from_analysis_start(
    analysis: &buck2_data::AnalysisStart,
) -> Option<&buck2_data::ConfiguredTargetLabel> {
    match analysis.target.as_ref()? {
        buck2_data::analysis_start::Target::StandardTarget(target) => Some(target),
        buck2_data::analysis_start::Target::DynamicLambda(owner) => {
            configured_target_from_dynamic_owner(owner)
        }
        buck2_data::analysis_start::Target::AnonTarget(_) => None,
    }
}

fn configured_target_from_analysis_end(
    analysis: &buck2_data::AnalysisEnd,
) -> Option<&buck2_data::ConfiguredTargetLabel> {
    match analysis.target.as_ref()? {
        buck2_data::analysis_end::Target::StandardTarget(target) => Some(target),
        buck2_data::analysis_end::Target::DynamicLambda(owner) => {
            configured_target_from_dynamic_owner(owner)
        }
        buck2_data::analysis_end::Target::AnonTarget(_) => None,
    }
}

fn configured_target_from_dynamic_owner(
    owner: &buck2_data::DynamicLambdaOwner,
) -> Option<&buck2_data::ConfiguredTargetLabel> {
    match owner.owner.as_ref()? {
        buck2_data::dynamic_lambda_owner::Owner::TargetLabel(target) => Some(target),
        buck2_data::dynamic_lambda_owner::Owner::BxlKey(_)
        | buck2_data::dynamic_lambda_owner::Owner::AnonTarget(_) => None,
    }
}

fn label_for_configured_target(target: &buck2_data::ConfiguredTargetLabel) -> Option<String> {
    label_for_target(target.label.as_ref()?)
}

fn label_for_target(label: &buck2_data::TargetLabel) -> Option<String> {
    label_for_package_and_name(&label.package, &label.name)
}

fn label_for_package_and_name(package: &str, name: &str) -> Option<String> {
    if name.is_empty() {
        return None;
    }
    let label = if package.is_empty() {
        format!(":{}", name)
    } else {
        format!("{package}:{name}")
    };
    normalize_buck_label(&label)
}

fn normalize_buck_label(label: &str) -> Option<String> {
    let label = label.trim();
    if label.is_empty() {
        return None;
    }
    if label.starts_with('@') {
        return Some(label.to_owned());
    }

    let label = label.strip_prefix("//").unwrap_or(label);
    if let Some((cell, rest)) = label.split_once("//") {
        let rest = rest.trim_start_matches('/');
        if cell == "root" {
            Some(format!("//{rest}"))
        } else {
            Some(format!("@{cell}//{rest}"))
        }
    } else if label.starts_with(':') {
        Some(format!("//{label}"))
    } else {
        Some(format!("//{label}"))
    }
}

fn configuration_id_for_target(target: &buck2_data::ConfiguredTargetLabel) -> String {
    target
        .configuration
        .as_ref()
        .map(|configuration| configuration.full_name.clone())
        .filter(|configuration| !configuration.is_empty())
        .unwrap_or_else(|| DEFAULT_CONFIGURATION_ID.to_owned())
}

fn command_line_from_execution(command: &buck2_data::CommandExecution) -> Vec<String> {
    let Some(details) = command.details.as_ref() else {
        return Vec::new();
    };
    let Some(kind) = details.command_kind.as_ref() else {
        return Vec::new();
    };
    let Some(command) = kind.command.as_ref() else {
        return Vec::new();
    };
    match command {
        buck2_data::command_execution_kind::Command::LocalCommand(command) => command.argv.clone(),
        buck2_data::command_execution_kind::Command::WorkerCommand(command) => command.argv.clone(),
        buck2_data::command_execution_kind::Command::WorkerInitCommand(command) => {
            command.argv.clone()
        }
        buck2_data::command_execution_kind::Command::RemoteCommand(_)
        | buck2_data::command_execution_kind::Command::OmittedLocalCommand(_) => Vec::new(),
    }
}

fn action_strategy_details(
    action: &buck2_data::ActionExecutionEnd,
    command: Option<&buck2_data::CommandExecution>,
    label: &str,
    mnemonic: &str,
    exit_code: i32,
) -> Vec<Any> {
    let mut fields = BTreeMap::new();
    insert_string_field(&mut fields, "tool", "buck2");
    insert_string_field(&mut fields, "label", label);
    insert_string_field(&mut fields, "mnemonic", mnemonic);
    insert_bool_field(&mut fields, "success", !action.failed);
    insert_number_field(&mut fields, "exit_code", f64::from(exit_code));
    insert_number_field(&mut fields, "output_count", action.outputs.len() as f64);
    insert_number_field(&mut fields, "output_size_bytes", action.output_size as f64);
    insert_bool_field(&mut fields, "prefers_local", action.prefers_local);
    insert_bool_field(&mut fields, "requires_local", action.requires_local);
    insert_bool_field(
        &mut fields,
        "allows_cache_upload",
        action.allows_cache_upload,
    );

    let command_kind = command
        .and_then(|command| command.details.as_ref())
        .and_then(|details| details.command_kind.as_ref())
        .and_then(|kind| kind.command.as_ref());

    insert_string_field(
        &mut fields,
        "strategy",
        action_execution_strategy(command_kind),
    );

    if let Some(command) = command {
        insert_command_metadata_fields(&mut fields, command);
    }

    if let Some(command_kind) = command_kind {
        insert_command_kind_fields(&mut fields, command_kind);
    }

    vec![struct_any(fields)]
}

fn insert_command_metadata_fields(
    fields: &mut BTreeMap<String, Value>,
    command: &buck2_data::CommandExecution,
) {
    let Some(metadata) = command
        .details
        .as_ref()
        .and_then(|details| details.metadata.as_ref())
    else {
        return;
    };

    insert_optional_duration_field(fields, "wall_time_ms", metadata.wall_time.as_ref());
    insert_optional_duration_field(
        fields,
        "execution_time_ms",
        metadata.execution_time.as_ref(),
    );
    insert_optional_duration_field(
        fields,
        "input_materialization_ms",
        metadata.input_materialization_duration.as_ref(),
    );
    insert_optional_duration_field(
        fields,
        "queue_duration_ms",
        metadata.queue_duration.as_ref(),
    );
}

fn insert_command_kind_fields(
    fields: &mut BTreeMap<String, Value>,
    command: &buck2_data::command_execution_kind::Command,
) {
    match command {
        buck2_data::command_execution_kind::Command::LocalCommand(command) => {
            insert_string_field(fields, "runner", "local");
            insert_argv_fields(fields, &command.argv);
            insert_non_empty_string_field(fields, "action_digest", &command.action_digest);
        }
        buck2_data::command_execution_kind::Command::WorkerCommand(command) => {
            insert_string_field(fields, "runner", "worker");
            insert_argv_fields(fields, &command.argv);
            insert_non_empty_string_field(fields, "action_digest", &command.action_digest);
            insert_number_field(
                fields,
                "fallback_arg_count",
                command.fallback_exe.len() as f64,
            );
        }
        buck2_data::command_execution_kind::Command::WorkerInitCommand(command) => {
            insert_string_field(fields, "runner", "worker-init");
            insert_argv_fields(fields, &command.argv);
        }
        buck2_data::command_execution_kind::Command::RemoteCommand(command) => {
            insert_string_field(fields, "runner", "remote");
            insert_non_empty_string_field(fields, "action_digest", &command.action_digest);
            insert_bool_field(fields, "cache_hit", command.cache_hit);
            if let Ok(cache_hit_type) = buck2_data::CacheHitType::try_from(command.cache_hit_type) {
                insert_string_field(fields, "cache_hit_type", cache_hit_type.as_str_name());
            }
            insert_optional_duration_field(fields, "queue_time_ms", command.queue_time.as_ref());
            if let Some(remote_dep_file_key) = command.remote_dep_file_key.as_ref() {
                insert_non_empty_string_field(fields, "remote_dep_file_key", remote_dep_file_key);
            }
            insert_number_field(
                fields,
                "materialized_failed_input_count",
                command.materialized_inputs_for_failed.len() as f64,
            );
            insert_number_field(
                fields,
                "materialized_failed_output_count",
                command.materialized_outputs_for_failed_actions.len() as f64,
            );
            if let Some(details) = command.details.as_ref() {
                if let Some(session_id) = details.session_id.as_ref() {
                    insert_non_empty_string_field(fields, "remote_session_id", session_id);
                }
                insert_non_empty_string_field(fields, "remote_use_case", &details.use_case);
                insert_bool_field(
                    fields,
                    "remote_persistent_worker",
                    details.persistent_worker,
                );
                insert_platform_field(fields, details.platform.as_ref());
            }
        }
        buck2_data::command_execution_kind::Command::OmittedLocalCommand(command) => {
            insert_string_field(fields, "runner", "local-omitted");
            insert_non_empty_string_field(fields, "action_digest", &command.action_digest);
        }
    }
}

fn action_execution_strategy(
    command: Option<&buck2_data::command_execution_kind::Command>,
) -> &'static str {
    let Some(command) = command else {
        return "unknown";
    };
    match command {
        buck2_data::command_execution_kind::Command::LocalCommand(_)
        | buck2_data::command_execution_kind::Command::OmittedLocalCommand(_) => "local",
        buck2_data::command_execution_kind::Command::WorkerCommand(_)
        | buck2_data::command_execution_kind::Command::WorkerInitCommand(_) => "worker",
        buck2_data::command_execution_kind::Command::RemoteCommand(command) => {
            match buck2_data::CacheHitType::try_from(command.cache_hit_type) {
                Ok(buck2_data::CacheHitType::ActionCache) => "remote-cache",
                Ok(buck2_data::CacheHitType::RemoteDepFileCache) => "remote-dep-file-cache",
                Ok(buck2_data::CacheHitType::Executed) | Err(_) => "remote",
            }
        }
    }
}

fn insert_platform_field(
    fields: &mut BTreeMap<String, Value>,
    platform: Option<&buck2_data::RePlatform>,
) {
    let Some(platform) = platform else {
        return;
    };
    let platform = platform
        .properties
        .iter()
        .filter(|property| !property.name.is_empty())
        .map(|property| format!("{}={}", property.name, property.value))
        .collect::<Vec<_>>();
    if platform.is_empty() {
        return;
    }
    insert_string_field(fields, "remote_platform", platform.join(","));
}

fn insert_argv_fields(fields: &mut BTreeMap<String, Value>, argv: &[String]) {
    if let Some(argv0) = argv.first() {
        insert_non_empty_string_field(fields, "argv0", argv0);
    }
    insert_number_field(fields, "argv_count", argv.len() as f64);
}

fn insert_optional_duration_field(
    fields: &mut BTreeMap<String, Value>,
    name: &str,
    duration: Option<&prost_types::Duration>,
) {
    if let Some(duration) = duration {
        insert_number_field(fields, name, duration_millis(duration) as f64);
    }
}

fn insert_non_empty_string_field(fields: &mut BTreeMap<String, Value>, name: &str, value: &str) {
    if !value.is_empty() {
        insert_string_field(fields, name, value);
    }
}

fn insert_string_field(fields: &mut BTreeMap<String, Value>, name: &str, value: impl Into<String>) {
    fields.insert(
        name.to_owned(),
        Value {
            kind: Some(value::Kind::StringValue(value.into())),
        },
    );
}

fn insert_bool_field(fields: &mut BTreeMap<String, Value>, name: &str, value: bool) {
    fields.insert(
        name.to_owned(),
        Value {
            kind: Some(value::Kind::BoolValue(value)),
        },
    );
}

fn insert_number_field(fields: &mut BTreeMap<String, Value>, name: &str, value: f64) {
    fields.insert(
        name.to_owned(),
        Value {
            kind: Some(value::Kind::NumberValue(value)),
        },
    );
}

fn struct_any(fields: BTreeMap<String, Value>) -> Any {
    Any {
        type_url: STRUCT_TYPE_URL.to_owned(),
        value: Struct { fields }.encode_to_vec(),
    }
}

fn action_mnemonic(action: &buck2_data::ActionExecutionEnd) -> String {
    if let Some(name) = action.name.as_ref()
        && !name.category.is_empty()
    {
        return name.category.clone();
    }
    buck2_data::ActionKind::try_from(action.kind)
        .ok()
        .map(|kind| format!("{kind:?}"))
        .unwrap_or_else(|| "Action".to_owned())
}

fn start_time_from_span_end(
    span_end: &buck2_data::SpanEndEvent,
    end_time: Option<&Timestamp>,
) -> Option<Timestamp> {
    let duration = span_end.duration.as_ref()?;
    let end_time = end_time?;
    let nanos = i128::from(end_time.seconds) * 1_000_000_000 + i128::from(end_time.nanos);
    let duration_nanos = i128::from(duration.seconds) * 1_000_000_000 + i128::from(duration.nanos);
    let start_nanos = nanos.checked_sub(duration_nanos)?;
    Some(Timestamp {
        seconds: (start_nanos / 1_000_000_000) as i64,
        nanos: (start_nanos % 1_000_000_000) as i32,
    })
}

fn file_with_contents(name: &str, contents: String) -> bep::File {
    let mut contents = contents.into_bytes();
    if contents.len() > MAX_INLINE_FILE_BYTES {
        contents.truncate(MAX_INLINE_FILE_BYTES);
        contents.extend_from_slice(b"\n<<truncated>>");
    }
    let length = contents.len() as i64;
    bep::File {
        name: name.to_owned(),
        path_prefix: Vec::new(),
        file: Some(bep::file::File::Contents(contents)),
        digest: String::new(),
        length,
    }
}

fn file_with_bytes(name: &str, contents: Vec<u8>) -> bep::File {
    let length = contents.len() as i64;
    bep::File {
        name: name.to_owned(),
        path_prefix: Vec::new(),
        file: Some(bep::file::File::Contents(contents)),
        digest: String::new(),
        length,
    }
}

fn test_status(status: i32) -> i32 {
    match buck2_data::TestStatus::try_from(status) {
        Ok(buck2_data::TestStatus::Pass) | Ok(buck2_data::TestStatus::ListingSuccess) => {
            bep::TestStatus::Passed as i32
        }
        Ok(buck2_data::TestStatus::Fail) | Ok(buck2_data::TestStatus::Fatal) => {
            bep::TestStatus::Failed as i32
        }
        Ok(buck2_data::TestStatus::Timeout) => bep::TestStatus::Timeout as i32,
        Ok(buck2_data::TestStatus::InfraFailure) => bep::TestStatus::RemoteFailure as i32,
        Ok(buck2_data::TestStatus::ListingFailed) => bep::TestStatus::FailedToBuild as i32,
        Ok(buck2_data::TestStatus::Rerun) => bep::TestStatus::Flaky as i32,
        Ok(buck2_data::TestStatus::Skip) | Ok(buck2_data::TestStatus::Omitted) => {
            bep::TestStatus::Incomplete as i32
        }
        Ok(buck2_data::TestStatus::Unknown)
        | Ok(buck2_data::TestStatus::NotSetTestStatus)
        | Err(_) => bep::TestStatus::NoStatus as i32,
    }
}

fn test_status_from_command_report(command: Option<&buck2_data::CommandExecution>) -> i32 {
    match command.and_then(|command| command.status.as_ref()) {
        Some(buck2_data::command_execution::Status::Success(_)) => bep::TestStatus::Passed as i32,
        Some(buck2_data::command_execution::Status::Timeout(_)) => bep::TestStatus::Timeout as i32,
        Some(buck2_data::command_execution::Status::Failure(_))
        | Some(buck2_data::command_execution::Status::WorkerFailure(_)) => {
            bep::TestStatus::Failed as i32
        }
        Some(buck2_data::command_execution::Status::Error(_)) => {
            bep::TestStatus::RemoteFailure as i32
        }
        Some(buck2_data::command_execution::Status::Cancelled(_)) => {
            bep::TestStatus::Incomplete as i32
        }
        None => bep::TestStatus::NoStatus as i32,
    }
}

fn aggregate_test_status(
    command: Option<&buck2_data::CommandExecution>,
    cases: &[TestCaseState],
) -> i32 {
    let command_status = test_status_from_command_report(command);
    if command_status != bep::TestStatus::NoStatus as i32 {
        return command_status;
    }
    if cases.is_empty() {
        return bep::TestStatus::NoStatus as i32;
    }
    if cases.iter().any(|case| {
        matches!(
            test_status(case.status),
            status if status == bep::TestStatus::Failed as i32
                || status == bep::TestStatus::Timeout as i32
                || status == bep::TestStatus::RemoteFailure as i32
                || status == bep::TestStatus::FailedToBuild as i32
        )
    }) {
        return bep::TestStatus::Failed as i32;
    }
    if cases
        .iter()
        .all(|case| test_status(case.status) == bep::TestStatus::Passed as i32)
    {
        return bep::TestStatus::Passed as i32;
    }
    bep::TestStatus::NoStatus as i32
}

fn test_action_outputs(
    cases: &[TestCaseState],
    command: Option<&buck2_data::CommandExecution>,
    status: i32,
) -> Vec<bep::File> {
    let mut outputs = Vec::new();
    let log = test_log_contents(cases, command);
    if !log.is_empty() {
        outputs.push(file_with_contents("test.log", log));
    }
    let xml = test_xml_contents(cases, status);
    if !xml.is_empty() {
        outputs.push(file_with_contents("test.xml", xml));
    }
    outputs
}

fn test_summary_files(
    status: i32,
    command: Option<&buck2_data::CommandExecution>,
    passed: bool,
) -> Vec<bep::File> {
    let is_passed =
        status == bep::TestStatus::Passed as i32 || status == bep::TestStatus::Flaky as i32;
    if is_passed != passed {
        return Vec::new();
    }
    let log = test_log_contents(&[], command);
    if log.is_empty() {
        Vec::new()
    } else {
        vec![file_with_contents("test.log", log)]
    }
}

fn test_log_contents(
    cases: &[TestCaseState],
    command: Option<&buck2_data::CommandExecution>,
) -> String {
    let mut log = String::new();
    if let Some(details) = command.and_then(|command| command.details.as_ref()) {
        if !details.cmd_stdout.is_empty() {
            log.push_str("stdout:\n");
            log.push_str(&details.cmd_stdout);
            if !details.cmd_stdout.ends_with('\n') {
                log.push('\n');
            }
        }
        if !details.cmd_stderr.is_empty() {
            log.push_str("stderr:\n");
            log.push_str(&details.cmd_stderr);
            if !details.cmd_stderr.ends_with('\n') {
                log.push('\n');
            }
        }
    }
    for case in cases {
        let details = case
            .message
            .as_deref()
            .filter(|message| !message.is_empty())
            .or_else(|| (!case.details.is_empty()).then_some(case.details.as_str()));
        if let Some(details) = details {
            log.push_str(&case.name);
            log.push_str(": ");
            log.push_str(details);
            if !details.ends_with('\n') {
                log.push('\n');
            }
        }
    }
    log
}

fn test_xml_contents(cases: &[TestCaseState], status: i32) -> String {
    if cases.is_empty() {
        return String::new();
    }
    let failures = cases
        .iter()
        .filter(|case| test_status(case.status) != bep::TestStatus::Passed as i32)
        .count();
    let mut xml = format!(
        r#"<testsuite tests="{}" failures="{}" errors="0" skipped="{}">"#,
        cases.len(),
        failures,
        if status == bep::TestStatus::Incomplete as i32 {
            failures
        } else {
            0
        }
    );
    for case in cases {
        xml.push_str(&format!(
            r#"<testcase name="{}" time="{}">"#,
            xml_escape(&case.name),
            duration_seconds(case.duration.as_ref())
        ));
        if test_status(case.status) != bep::TestStatus::Passed as i32 {
            let message = case.message.as_deref().unwrap_or(&case.details);
            xml.push_str(&format!(
                r#"<failure message="{}">{}</failure>"#,
                xml_escape(message),
                xml_escape(&case.details)
            ));
        }
        xml.push_str("</testcase>");
    }
    xml.push_str("</testsuite>");
    xml
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\'', "&apos;")
}

fn duration_seconds(duration: Option<&prost_types::Duration>) -> String {
    let Some(duration) = duration else {
        return "0".to_owned();
    };
    let seconds = duration.seconds as f64 + f64::from(duration.nanos) / 1_000_000_000.0;
    format!("{seconds:.3}")
}

fn test_status_details(
    cases: &[TestCaseState],
    command: Option<&buck2_data::CommandExecution>,
) -> String {
    let failed_cases = cases
        .iter()
        .filter(|case| test_status(case.status) != bep::TestStatus::Passed as i32)
        .map(|case| case.name.clone())
        .collect::<Vec<_>>();
    if !failed_cases.is_empty() {
        return format!("failed tests: {}", failed_cases.join(", "));
    }
    command
        .and_then(|command| command.details.as_ref())
        .map(|details| details.cmd_stderr.clone())
        .filter(|stderr| !stderr.is_empty())
        .unwrap_or_default()
}

fn test_duration(command: Option<&buck2_data::CommandExecution>) -> Option<prost_types::Duration> {
    command
        .and_then(|command| command.details.as_ref())
        .and_then(|details| details.metadata.as_ref())
        .and_then(|metadata| metadata.wall_time.clone())
}

fn test_execution_info(
    command: Option<&buck2_data::CommandExecution>,
) -> Option<bep::test_result::ExecutionInfo> {
    let details = command.and_then(|command| command.details.as_ref())?;
    Some(bep::test_result::ExecutionInfo {
        timeout_seconds: 0,
        strategy: details
            .command_kind
            .as_ref()
            .and_then(|kind| kind.command.as_ref())
            .map(test_strategy)
            .unwrap_or_default(),
        cached_remotely: false,
        exit_code: details.signed_exit_code.unwrap_or_default(),
        hostname: String::new(),
        timing_breakdown: None,
        resource_usage: Vec::new(),
    })
}

fn test_strategy(command: &buck2_data::command_execution_kind::Command) -> String {
    match command {
        buck2_data::command_execution_kind::Command::LocalCommand(_) => "local",
        buck2_data::command_execution_kind::Command::WorkerCommand(_)
        | buck2_data::command_execution_kind::Command::WorkerInitCommand(_) => "worker",
        buck2_data::command_execution_kind::Command::RemoteCommand(_) => "remote",
        buck2_data::command_execution_kind::Command::OmittedLocalCommand(_) => "local",
    }
    .to_owned()
}

fn action_error_message(action_error: &buck2_data::ActionError) -> String {
    let label = action_error
        .key
        .as_ref()
        .and_then(action_owner)
        .and_then(label_for_configured_target)
        .unwrap_or_else(|| "unknown target".to_owned());
    let category = action_error
        .name
        .as_ref()
        .map(|name| name.category.as_str())
        .filter(|category| !category.is_empty())
        .unwrap_or("Action");
    format!("Action failed: {category} for {label}")
}

fn first_metadata(metadata: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        metadata
            .get(*key)
            .filter(|value| !value.is_empty())
            .cloned()
    })
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::GzDecoder;
    use prost::Message as _;

    use super::*;

    fn trace_event(data: buck2_data::buck_event::Data) -> buck2_data::BuckEvent {
        buck2_data::BuckEvent {
            timestamp: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            trace_id: "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa".to_owned(),
            span_id: 1,
            parent_id: 0,
            data: Some(data),
        }
    }

    fn trace_event_at(
        span_id: u64,
        parent_id: u64,
        micros: i64,
        data: buck2_data::buck_event::Data,
    ) -> buck2_data::BuckEvent {
        buck2_data::BuckEvent {
            timestamp: Some(Timestamp {
                seconds: 1 + micros / 1_000_000,
                nanos: ((micros % 1_000_000) * 1_000) as i32,
            }),
            trace_id: "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa".to_owned(),
            span_id,
            parent_id,
            data: Some(data),
        }
    }

    fn decode_profile_from_logs(logs: &bep::BuildToolLogs) -> serde_json::Value {
        let profile_contents = logs
            .log
            .iter()
            .find(|log| log.name == COMMAND_PROFILE_NAME)
            .and_then(|log| log.file.as_ref())
            .unwrap_or_else(|| panic!("expected {COMMAND_PROFILE_NAME}"));
        let profile_contents = match profile_contents {
            bep::file::File::Contents(contents) => contents,
            other => panic!("expected inline command profile, got {other:?}"),
        };
        let mut decoder = GzDecoder::new(profile_contents.as_slice());
        let mut profile = String::new();
        decoder
            .read_to_string(&mut profile)
            .expect("valid gzip profile");
        assert!(profile.contains("\"traceEvents\":[\n"));
        serde_json::from_str(&profile).expect("valid profile json")
    }

    fn configured_target() -> buck2_data::ConfiguredTargetLabel {
        configured_target_with_package("pkg", "main", "cfg")
    }

    fn configured_target_with_package(
        package: &str,
        name: &str,
        configuration: &str,
    ) -> buck2_data::ConfiguredTargetLabel {
        buck2_data::ConfiguredTargetLabel {
            label: Some(buck2_data::TargetLabel {
                package: package.to_owned(),
                name: name.to_owned(),
            }),
            configuration: Some(buck2_data::Configuration {
                full_name: configuration.to_owned(),
            }),
            execution_configuration: None,
        }
    }

    fn struct_string_field<'a>(details: &'a Struct, field: &str) -> Option<&'a str> {
        match details.fields.get(field)?.kind.as_ref()? {
            value::Kind::StringValue(value) => Some(value.as_str()),
            _ => None,
        }
    }

    fn struct_number_field(details: &Struct, field: &str) -> Option<f64> {
        match details.fields.get(field)?.kind.as_ref()? {
            value::Kind::NumberValue(value) => Some(*value),
            _ => None,
        }
    }

    #[test]
    fn command_start_emits_started_and_metadata() {
        let mut converter = BazelEventConverter::default();
        let event = trace_event(buck2_data::buck_event::Data::SpanStart(
            buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Command(
                    buck2_data::CommandStart {
                        cli_args: vec![
                            "buck2".to_owned(),
                            "build".to_owned(),
                            "--remote-cache=grpc://cache.example.com".to_owned(),
                            "//:main".to_owned(),
                        ],
                        metadata: HashMap::from([
                            ("username".to_owned(), "alice".to_owned()),
                            ("hostname".to_owned(), "workstation".to_owned()),
                        ]),
                        data: Some(buck2_data::command_start::Data::Build(
                            buck2_data::BuildCommandStart {},
                        )),
                        ..Default::default()
                    },
                )),
            },
        ));

        let events = converter.convert(1, &event);
        assert!(matches!(
            events[0].payload,
            Some(build_event::Payload::Started(_))
        ));
        assert!(events.iter().any(|event| matches!(
            event.payload,
            Some(build_event::Payload::StructuredCommandLine(_))
        )));
        let canonical = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::StructuredCommandLine(command_line))
                    if command_line.command_line_label == CANONICAL_COMMAND_LINE_LABEL =>
                {
                    Some(command_line)
                }
                _ => None,
            })
            .expect("canonical structured command line");
        let options = canonical
            .sections
            .iter()
            .flat_map(|section| match section.section_type.as_ref() {
                Some(cl::command_line_section::SectionType::OptionList(options)) => {
                    options.option.as_slice()
                }
                _ => &[],
            })
            .collect::<Vec<_>>();
        assert!(options.iter().any(|option| {
            option.option_name == "remote_cache"
                && option.option_value == "grpc://cache.example.com"
        }));
        assert!(options.iter().any(|option| {
            option.option_name == "client_env" && option.option_value == "USER=alice"
        }));
        assert!(
            events
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::OptionsParsed(_))))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::Configuration(_))))
        );
        let build_metadata = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::BuildMetadata(metadata)) => Some(metadata),
                _ => None,
            })
            .expect("build metadata");
        assert_eq!(
            build_metadata
                .metadata
                .get(BUILDBUDDY_VISIBILITY_KEY)
                .map(String::as_str),
            Some(BUILDBUDDY_PUBLIC_VISIBILITY)
        );
        assert_eq!(
            build_metadata.metadata.get("USER").map(String::as_str),
            Some("alice")
        );
        assert_eq!(
            build_metadata.metadata.get("HOST").map(String::as_str),
            Some("workstation")
        );

        let any = encode_bep_event(&events[0]);
        assert_eq!(any.type_url, BEP_EVENT_TYPE_URL);
        let decoded = bep::BuildEvent::decode(any.value.as_slice()).expect("valid BEP event");
        assert!(matches!(
            decoded.payload,
            Some(build_event::Payload::Started(_))
        ));
    }

    #[test]
    fn command_start_preserves_explicit_buildbuddy_visibility() {
        let event = build_metadata_event(&build_metadata_from_command(
            &buck2_data::CommandStart {
                metadata: HashMap::from([(
                    BUILDBUDDY_VISIBILITY_KEY.to_owned(),
                    "PRIVATE".to_owned(),
                )]),
                ..Default::default()
            },
            &BTreeMap::new(),
        ));

        let build_metadata = match event.payload.as_ref() {
            Some(build_event::Payload::BuildMetadata(metadata)) => metadata,
            other => panic!("expected build metadata, got {other:?}"),
        };
        assert_eq!(
            build_metadata
                .metadata
                .get(BUILDBUDDY_VISIBILITY_KEY)
                .map(String::as_str),
            Some("PRIVATE")
        );
    }

    #[test]
    fn command_start_includes_configured_build_metadata() {
        let mut converter = BazelEventConverter::new([
            ("PARENT_RUN_ID".to_owned(), "workflow-run".to_owned()),
            (BUILDBUDDY_VISIBILITY_KEY.to_owned(), "PRIVATE".to_owned()),
        ]);
        let event = trace_event(buck2_data::buck_event::Data::SpanStart(
            buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Command(
                    buck2_data::CommandStart {
                        data: Some(buck2_data::command_start::Data::Build(
                            buck2_data::BuildCommandStart {},
                        )),
                        ..Default::default()
                    },
                )),
            },
        ));

        let events = converter.convert(1, &event);
        let build_metadata = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::BuildMetadata(metadata)) => Some(metadata),
                _ => None,
            })
            .expect("build metadata");
        assert_eq!(
            build_metadata
                .metadata
                .get("PARENT_RUN_ID")
                .map(String::as_str),
            Some("workflow-run")
        );
        assert_eq!(
            build_metadata
                .metadata
                .get(BUILDBUDDY_VISIBILITY_KEY)
                .map(String::as_str),
            Some("PRIVATE")
        );
    }

    #[test]
    fn pattern_expanded_links_resolved_targets() {
        let mut converter = BazelEventConverter::default();
        let patterns = trace_event(buck2_data::buck_event::Data::Instant(
            buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::TargetPatterns(
                    buck2_data::ParsedTargetPatterns {
                        target_patterns: vec![buck2_data::TargetPattern {
                            value: "//:main".to_owned(),
                        }],
                    },
                )),
            },
        ));

        let events = converter.convert(1, &patterns);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::Expanded(_))))
        );

        let build_graph = trace_event(buck2_data::buck_event::Data::Record(
            buck2_data::RecordEvent {
                data: Some(buck2_data::record_event::Data::BuildGraphStats(
                    buck2_data::BuildGraphStats {
                        build_targets: vec![buck2_data::BuildTarget {
                            target: "//:main".to_owned(),
                            configuration: "cfg".to_owned(),
                            configured_graph_size: None,
                        }],
                    },
                )),
            },
        ));
        let events = converter.convert(2, &build_graph);
        let expanded = events
            .iter()
            .find(|event| matches!(event.payload, Some(build_event::Payload::Expanded(_))))
            .expect("PatternExpanded event");

        let Some(build_event_id::Id::Pattern(pattern_id)) =
            expanded.id.as_ref().and_then(|id| id.id.as_ref())
        else {
            panic!("expected PatternExpanded id");
        };
        assert_eq!(pattern_id.pattern, vec!["//:main"]);
        assert!(
            expanded
                .children
                .iter()
                .any(|child| child == &target_configured_id("//:main".to_owned()))
        );
    }

    #[test]
    fn invocation_record_emits_build_metrics() {
        let mut converter = BazelEventConverter::default();
        let event = trace_event(buck2_data::buck_event::Data::Record(
            buck2_data::RecordEvent {
                data: Some(buck2_data::record_event::Data::InvocationRecord(Box::new(
                    buck2_data::InvocationRecord {
                        outcome: Some(buck2_data::InvocationOutcome::Success as i32),
                        run_local_count: 2,
                        run_remote_count: 3,
                        run_action_cache_count: 5,
                        client_walltime: Some(prost_types::Duration {
                            seconds: 1,
                            nanos: 500_000_000,
                        }),
                        analysis_count: Some(7),
                        command_name: Some("build".to_owned()),
                        re_session_id: "re-session".to_owned(),
                        tags: vec!["ci".to_owned()],
                        ..Default::default()
                    },
                ))),
            },
        ));

        let events = converter.convert(1, &event);
        let metrics = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::BuildMetrics(metrics)) => Some(metrics),
                _ => None,
            })
            .expect("build metrics event");
        let action_summary = metrics.action_summary.as_ref().unwrap();
        assert_eq!(action_summary.actions_executed, 10);
        assert_eq!(action_summary.remote_cache_hits, 5);
        let action_cache_statistics = action_summary.action_cache_statistics.as_ref().unwrap();
        assert_eq!(action_cache_statistics.hits, 5);
        assert_eq!(action_cache_statistics.misses, 5);
        assert_eq!(action_cache_statistics.miss_details.len(), 1);
        assert_eq!(
            action_cache_statistics.miss_details[0].reason,
            blaze::action_cache_statistics::MissReason::NotCached as i32
        );
        assert_eq!(action_cache_statistics.miss_details[0].count, 5);
        assert_eq!(
            metrics.timing_metrics.as_ref().unwrap().wall_time_in_ms,
            1500
        );
        assert_eq!(
            metrics.target_metrics.as_ref().unwrap().targets_configured,
            7
        );
        let logs = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::BuildToolLogs(logs)) => Some(logs),
                _ => None,
            })
            .expect("build tool logs event");
        assert_eq!(logs.log[0].name, "buck2-invocation.json");
        assert_eq!(logs.log[1].name, COMMAND_PROFILE_NAME);
        let contents = match logs.log[0].file.as_ref() {
            Some(bep::file::File::Contents(contents)) => contents,
            other => panic!("expected inline invocation summary, got {other:?}"),
        };
        let summary: serde_json::Value =
            serde_json::from_slice(contents).expect("valid summary json");
        assert_eq!(summary["command"], "build");
        assert_eq!(summary["outcome"], "success");
        assert_eq!(summary["re_session_id"], "re-session");
        assert_eq!(summary["actions"]["remote_cache"], 5);
        let profile_contents = match logs.log[1].file.as_ref() {
            Some(bep::file::File::Contents(contents)) => contents,
            other => panic!("expected inline command profile, got {other:?}"),
        };
        let mut decoder = GzDecoder::new(profile_contents.as_slice());
        let mut profile = String::new();
        decoder
            .read_to_string(&mut profile)
            .expect("valid gzip profile");
        assert!(profile.contains("\"traceEvents\":[\n"));
        let profile_json: serde_json::Value =
            serde_json::from_str(&profile).expect("valid profile json");
        assert!(
            profile_json["traceEvents"]
                .as_array()
                .expect("trace events array")
                .iter()
                .any(|event| event["name"] == "buck2 build")
        );
    }

    #[test]
    fn command_end_emits_build_finished() {
        let mut converter = BazelEventConverter::default();
        let events = converter.convert(
            1,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::Command(
                        buck2_data::CommandEnd {
                            data: Some(buck2_data::command_end::Data::Query(
                                buck2_data::QueryCommandEnd {},
                            )),
                            is_success: false,
                            build_result: Some(buck2_data::BuildResult {
                                build_completed: true,
                            }),
                        },
                    )),
                    duration: Some(prost_types::Duration {
                        seconds: 2,
                        nanos: 500_000_000,
                    }),
                    ..Default::default()
                },
            )),
        );

        let finished = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::Finished(finished)) => Some(finished),
                _ => None,
            })
            .expect("build finished");
        assert!(!finished.overall_success);
        let exit_code = finished.exit_code.as_ref().expect("exit code");
        assert_eq!(exit_code.code, 1);
        assert_eq!(exit_code.name, "FAILED");

        let logs = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::BuildToolLogs(logs)) => Some(logs),
                _ => None,
            })
            .expect("build tool logs event");
        assert_eq!(logs.log[1].name, COMMAND_PROFILE_NAME);
        let profile_contents = match logs.log[1].file.as_ref() {
            Some(bep::file::File::Contents(contents)) => contents,
            other => panic!("expected inline command profile, got {other:?}"),
        };
        let mut decoder = GzDecoder::new(profile_contents.as_slice());
        let mut profile = String::new();
        decoder
            .read_to_string(&mut profile)
            .expect("valid gzip profile");
        let profile_json: serde_json::Value =
            serde_json::from_str(&profile).expect("valid profile json");
        assert!(
            profile_json["traceEvents"]
                .as_array()
                .expect("trace events array")
                .iter()
                .any(|event| event["name"] == "buck2 query" && event["dur"] == 2_500_000)
        );
    }

    #[test]
    fn command_profile_includes_buck_spans_and_lanes() {
        let mut converter = BazelEventConverter::default();
        let target = configured_target();
        let command_start = trace_event_at(
            1,
            0,
            0,
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Command(
                    buck2_data::CommandStart {
                        cli_args: vec![
                            "buck2".to_owned(),
                            "build".to_owned(),
                            "//:main".to_owned(),
                        ],
                        data: Some(buck2_data::command_start::Data::Build(
                            buck2_data::BuildCommandStart {},
                        )),
                        ..Default::default()
                    },
                )),
            }),
        );
        converter.convert(1, &command_start);

        let analysis_start = trace_event_at(
            2,
            1,
            1_000,
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Analysis(
                    buck2_data::AnalysisStart {
                        target: Some(buck2_data::analysis_start::Target::StandardTarget(
                            target.clone(),
                        )),
                        rule: "genrule".to_owned(),
                    },
                )),
            }),
        );
        converter.convert(2, &analysis_start);

        let analysis_end = trace_event_at(
            2,
            1,
            11_000,
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::Analysis(
                    buck2_data::AnalysisEnd {
                        target: Some(buck2_data::analysis_end::Target::StandardTarget(
                            target.clone(),
                        )),
                        rule: "genrule".to_owned(),
                        declared_actions: Some(1),
                        ..Default::default()
                    },
                )),
                duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 10_000_000,
                }),
                ..Default::default()
            }),
        );
        converter.convert(3, &analysis_end);

        let action_key = buck2_data::ActionKey {
            owner: Some(buck2_data::action_key::Owner::TargetLabel(target.clone())),
            key: "action-key".to_owned(),
            ..Default::default()
        };
        let action_name = buck2_data::ActionName {
            category: "write".to_owned(),
            identifier: "main".to_owned(),
        };
        let action_start = trace_event_at(
            3,
            1,
            12_000,
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::ActionExecution(
                    buck2_data::ActionExecutionStart {
                        key: Some(action_key.clone()),
                        kind: buck2_data::ActionKind::Write as i32,
                        name: Some(action_name.clone()),
                    },
                )),
            }),
        );
        converter.convert(4, &action_start);

        let action_end = trace_event_at(
            3,
            1,
            32_000,
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::ActionExecution(Box::new(
                    buck2_data::ActionExecutionEnd {
                        key: Some(action_key),
                        kind: buck2_data::ActionKind::Write as i32,
                        name: Some(action_name),
                        execution_kind: buck2_data::ActionExecutionKind::Local as i32,
                        output_size: 42,
                        ..Default::default()
                    },
                ))),
                duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 20_000_000,
                }),
                ..Default::default()
            }),
        );
        converter.convert(5, &action_end);

        let command_end = trace_event_at(
            1,
            0,
            50_000,
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::Command(
                    buck2_data::CommandEnd {
                        data: Some(buck2_data::command_end::Data::Build(
                            buck2_data::BuildCommandEnd::default(),
                        )),
                        is_success: true,
                        ..Default::default()
                    },
                )),
                duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 50_000_000,
                }),
                ..Default::default()
            }),
        );
        let events = converter.convert(6, &command_end);
        let logs = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::BuildToolLogs(logs)) => Some(logs),
                _ => None,
            })
            .expect("build tool logs event");
        let profile_json = decode_profile_from_logs(logs);
        let trace_events = profile_json["traceEvents"]
            .as_array()
            .expect("trace events array");
        assert!(
            trace_events
                .iter()
                .filter(|event| event["ph"] == "X")
                .count()
                >= 3
        );
        assert!(
            trace_events
                .iter()
                .any(|event| { event["ph"] == "M" && event["args"]["name"] == "Buck2 command" })
        );
        assert!(
            trace_events
                .iter()
                .any(|event| { event["ph"] == "M" && event["args"]["name"] == "Buck2 analysis" })
        );
        assert!(
            trace_events
                .iter()
                .any(|event| event["ph"] == "M" && event["args"]["name"] == "Buck2 actions")
        );
        assert!(
            trace_events
                .iter()
                .any(|event| event["name"] == "analysis //pkg:main")
        );
        assert!(
            trace_events
                .iter()
                .any(|event| event["name"] == "write:main //pkg:main")
        );
    }

    #[test]
    fn invocation_record_emits_build_finished_when_command_end_did_not() {
        let mut converter = BazelEventConverter::default();

        let final_events = converter.convert(
            1,
            &trace_event(buck2_data::buck_event::Data::Record(
                buck2_data::RecordEvent {
                    data: Some(buck2_data::record_event::Data::InvocationRecord(Box::new(
                        buck2_data::InvocationRecord {
                            outcome: Some(buck2_data::InvocationOutcome::Failed as i32),
                            exit_code: Some(32),
                            exit_result_name: Some("TESTS_FAILED".to_owned()),
                            ..Default::default()
                        },
                    ))),
                },
            )),
        );

        let finished = final_events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::Finished(finished)) => Some(finished),
                _ => None,
            })
            .expect("build finished");
        assert!(!finished.overall_success);
        let exit_code = finished.exit_code.as_ref().expect("exit code");
        assert_eq!(exit_code.code, 32);
        assert_eq!(exit_code.name, "TESTS_FAILED");
    }

    #[test]
    fn re_session_emits_remote_execution_metadata() {
        let mut converter = BazelEventConverter::default();
        let events = converter.convert(
            1,
            &trace_event(buck2_data::buck_event::Data::Instant(
                buck2_data::InstantEvent {
                    data: Some(buck2_data::instant_event::Data::ReSession(
                        buck2_data::RemoteExecutionSessionCreated {
                            session_id: "re-session".to_owned(),
                            experiment_name: String::new(),
                            persistent_cache_mode: None,
                        },
                    )),
                },
            )),
        );

        let metadata = events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::BuildMetadata(metadata)) => Some(metadata),
                _ => None,
            })
            .expect("build metadata");
        assert_eq!(
            metadata
                .metadata
                .get("REMOTE_EXECUTION_ENABLED")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            metadata
                .metadata
                .get("BUCK2_RE_SESSION_ID")
                .map(String::as_str),
            Some("re-session")
        );
    }

    #[test]
    fn non_command_event_synthesizes_started() {
        let mut converter = BazelEventConverter::default();
        let event = trace_event(buck2_data::buck_event::Data::Instant(
            buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::ConsoleMessage(
                    buck2_data::ConsoleMessage {
                        message: "hello".to_owned(),
                    },
                )),
            },
        ));

        let events = converter.convert(1, &event);
        assert!(matches!(
            events[0].payload,
            Some(build_event::Payload::Started(_))
        ));
        assert!(matches!(
            events[1].payload,
            Some(build_event::Payload::Progress(_))
        ));
    }

    #[test]
    fn buck_cell_labels_are_normalized_for_bazel() {
        assert_eq!(normalize_buck_label("//:main").as_deref(), Some("//:main"));
        assert_eq!(
            normalize_buck_label("//root//:main").as_deref(),
            Some("//:main")
        );
        assert_eq!(
            normalize_buck_label("root//pkg:main").as_deref(),
            Some("//pkg:main")
        );
        assert_eq!(
            normalize_buck_label("//prelude//cpu:x86_64").as_deref(),
            Some("@prelude//cpu:x86_64")
        );
        assert_eq!(
            label_for_target(&buck2_data::TargetLabel {
                package: "root//pkg".to_owned(),
                name: "main".to_owned(),
            })
            .as_deref(),
            Some("//pkg:main")
        );
    }

    #[test]
    fn final_completed_update_links_actions_to_target() {
        let mut converter = BazelEventConverter::default();
        let target = configured_target_with_package("root//", "main", "cfg");

        converter.convert(
            1,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::Analysis(
                        buck2_data::AnalysisEnd {
                            rule: "cxx_binary".to_owned(),
                            target: Some(buck2_data::analysis_end::Target::StandardTarget(
                                target.clone(),
                            )),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            )),
        );
        let action = converter.convert(
            2,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::ActionExecution(Box::new(
                        buck2_data::ActionExecutionEnd {
                            key: Some(buck2_data::ActionKey {
                                key: "action-key".to_owned(),
                                owner: Some(buck2_data::action_key::Owner::TargetLabel(
                                    target.clone(),
                                )),
                                ..Default::default()
                            }),
                            outputs: vec![buck2_data::ActionOutput {
                                tiny_digest: "buck-out/main.o".to_owned(),
                                path: "buck-out/main.o".to_owned(),
                                digest: "abcd:10".to_owned(),
                                size: 10,
                                is_directory: false,
                            }],
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                },
            )),
        );
        let final_events = converter.convert(
            3,
            &trace_event(buck2_data::buck_event::Data::Record(
                buck2_data::RecordEvent {
                    data: Some(buck2_data::record_event::Data::InvocationRecord(Box::new(
                        buck2_data::InvocationRecord {
                            outcome: Some(buck2_data::InvocationOutcome::Success as i32),
                            ..Default::default()
                        },
                    ))),
                },
            )),
        );

        let Some(build_event_id::Id::ActionCompleted(action_id)) =
            action[0].id.as_ref().and_then(|id| id.id.as_ref())
        else {
            panic!("expected ActionCompleted id");
        };
        assert_eq!(action_id.label, "//:main");

        let completed_update = final_events
            .iter()
            .find(|event| {
                matches!(
                    event.id.as_ref().and_then(|id| id.id.as_ref()),
                    Some(build_event_id::Id::TargetCompleted(_))
                ) && !event.children.is_empty()
            })
            .expect("expected TargetComplete update with action children");
        let Some(build_event_id::Id::TargetCompleted(completed_id)) =
            completed_update.id.as_ref().and_then(|id| id.id.as_ref())
        else {
            panic!("expected TargetCompleted id");
        };
        assert_eq!(completed_id.label, "//:main");
        assert!(
            completed_update
                .children
                .iter()
                .any(|child| child == &action[0].id.clone().unwrap())
        );
        let Some(build_event::Payload::Completed(completed)) = completed_update.payload.as_ref()
        else {
            panic!("expected TargetComplete payload");
        };
        assert_eq!(completed.output_group.len(), 1);
        assert!(final_events.iter().any(|event| matches!(
            event.payload,
            Some(build_event::Payload::NamedSetOfFiles(_))
        )));
    }

    #[test]
    fn analysis_only_target_completes_at_invocation_end() {
        let mut converter = BazelEventConverter::default();
        let target = configured_target_with_package("root//", "main", "cfg");

        let analysis_end = converter.convert(
            1,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::Analysis(
                        buck2_data::AnalysisEnd {
                            rule: "cxx_binary".to_owned(),
                            target: Some(buck2_data::analysis_end::Target::StandardTarget(target)),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            )),
        );
        assert!(
            !analysis_end
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::Completed(_))))
        );

        let final_events = converter.convert(
            2,
            &trace_event(buck2_data::buck_event::Data::Record(
                buck2_data::RecordEvent {
                    data: Some(buck2_data::record_event::Data::InvocationRecord(Box::new(
                        buck2_data::InvocationRecord {
                            outcome: Some(buck2_data::InvocationOutcome::Success as i32),
                            ..Default::default()
                        },
                    ))),
                },
            )),
        );

        let completed = final_events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::Completed(completed)) => Some(completed),
                _ => None,
            })
            .expect("analysis-only target completion");
        assert!(completed.success);
    }

    #[test]
    fn configured_targets_are_deduplicated() {
        let mut converter = BazelEventConverter::default();
        let target = configured_target_with_package("root//", "main", "cfg");
        let event = trace_event(buck2_data::buck_event::Data::SpanStart(
            buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Analysis(
                    buck2_data::AnalysisStart {
                        rule: "cxx_binary".to_owned(),
                        target: Some(buck2_data::analysis_start::Target::StandardTarget(target)),
                    },
                )),
            },
        ));

        let first = converter.convert(1, &event);
        assert!(
            first
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::Configured(_))))
        );

        let second = converter.convert(2, &event);
        assert!(
            !second
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::Configured(_))))
        );
    }

    #[test]
    fn target_action_and_test_events_use_stable_ids() {
        let mut converter = BazelEventConverter::default();
        let target = configured_target();

        converter.convert(
            1,
            &trace_event(buck2_data::buck_event::Data::SpanStart(
                buck2_data::SpanStartEvent {
                    data: Some(buck2_data::span_start_event::Data::Command(
                        buck2_data::CommandStart {
                            data: Some(buck2_data::command_start::Data::Build(
                                buck2_data::BuildCommandStart {},
                            )),
                            ..Default::default()
                        },
                    )),
                },
            )),
        );

        let configured = converter.convert(
            2,
            &trace_event(buck2_data::buck_event::Data::SpanStart(
                buck2_data::SpanStartEvent {
                    data: Some(buck2_data::span_start_event::Data::Analysis(
                        buck2_data::AnalysisStart {
                            rule: "cxx_binary".to_owned(),
                            target: Some(buck2_data::analysis_start::Target::StandardTarget(
                                target.clone(),
                            )),
                        },
                    )),
                },
            )),
        );
        let completed = converter.convert(
            3,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::Analysis(
                        buck2_data::AnalysisEnd {
                            rule: "cxx_binary".to_owned(),
                            target: Some(buck2_data::analysis_end::Target::StandardTarget(
                                target.clone(),
                            )),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            )),
        );
        let action = converter.convert(
            4,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::ActionExecution(Box::new(
                        buck2_data::ActionExecutionEnd {
                            key: Some(buck2_data::ActionKey {
                                key: "action-key".to_owned(),
                                owner: Some(buck2_data::action_key::Owner::TargetLabel(
                                    target.clone(),
                                )),
                                ..Default::default()
                            }),
                            name: Some(buck2_data::ActionName {
                                category: "cxx_compile".to_owned(),
                                identifier: "main.cpp".to_owned(),
                            }),
                            failed: true,
                            outputs: vec![buck2_data::ActionOutput {
                                tiny_digest: "buck-out/main.o".to_owned(),
                                path: "buck-out/main.o".to_owned(),
                                digest: "abcd:10".to_owned(),
                                size: 10,
                                is_directory: false,
                            }],
                            commands: vec![buck2_data::CommandExecution {
                                details: Some(buck2_data::CommandExecutionDetails {
                                    signed_exit_code: Some(1),
                                    cmd_stdout: "compiler stdout".to_owned(),
                                    cmd_stderr: "compile failed".to_owned(),
                                    command_kind: Some(buck2_data::CommandExecutionKind {
                                        command: Some(
                                            buck2_data::command_execution_kind::Command::LocalCommand(
                                                buck2_data::LocalCommand {
                                                    argv: vec!["cc".to_owned(), "main.cpp".to_owned()],
                                                    ..Default::default()
                                                },
                                            ),
                                        ),
                                    }),
                                    ..Default::default()
                                }),
                                status: Some(buck2_data::command_execution::Status::Failure(
                                    buck2_data::command_execution::Failure {},
                                )),
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                },
            )),
        );
        let testcase = converter.convert(
            5,
            &trace_event(buck2_data::buck_event::Data::Instant(
                buck2_data::InstantEvent {
                    data: Some(buck2_data::instant_event::Data::TestResult(
                        buck2_data::TestResult {
                            name: "test_passes".to_owned(),
                            status: buck2_data::TestStatus::Pass as i32,
                            target_label: Some(target.clone()),
                            details: "passed".to_owned(),
                            ..Default::default()
                        },
                    )),
                },
            )),
        );
        assert!(testcase.is_empty());
        let test_result = converter.convert(
            6,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::TestRun(
                        buck2_data::TestRunEnd {
                            suite: Some(buck2_data::TestSuite {
                                suite_name: "suite".to_owned(),
                                test_names: vec!["test_passes".to_owned()],
                                target_label: Some(target.clone()),
                            }),
                            command_report: Some(buck2_data::CommandExecution {
                                details: Some(buck2_data::CommandExecutionDetails {
                                    signed_exit_code: Some(0),
                                    cmd_stdout: "test stdout".to_owned(),
                                    command_kind: Some(buck2_data::CommandExecutionKind {
                                        command: Some(
                                            buck2_data::command_execution_kind::Command::LocalCommand(
                                                buck2_data::LocalCommand {
                                                    argv: vec!["test-binary".to_owned()],
                                                    ..Default::default()
                                                },
                                            ),
                                        ),
                                    }),
                                    ..Default::default()
                                }),
                                status: Some(buck2_data::command_execution::Status::Success(
                                    buck2_data::command_execution::Success {},
                                )),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            )),
        );

        let Some(build_event_id::Id::TargetConfigured(configured_id)) =
            configured[0].id.as_ref().and_then(|id| id.id.as_ref())
        else {
            panic!("expected TargetConfigured id");
        };
        assert_eq!(configured_id.label, "//pkg:main");
        assert!(matches!(
            configured[0].payload,
            Some(build_event::Payload::Configured(_))
        ));

        assert!(completed.is_empty());

        let Some(build_event_id::Id::ActionCompleted(action_id)) =
            action[0].id.as_ref().and_then(|id| id.id.as_ref())
        else {
            panic!("expected ActionCompleted id");
        };
        assert_eq!(action_id.label, "//pkg:main");
        assert_eq!(action_id.configuration.as_ref().unwrap().id, "cfg");
        assert!(matches!(
            action[0].payload,
            Some(build_event::Payload::Action(bep::ActionExecuted {
                success: false,
                ..
            }))
        ));
        let Some(build_event::Payload::Action(action_payload)) = action[0].payload.as_ref() else {
            panic!("expected ActionExecuted payload");
        };
        let action_stdout = match action_payload
            .stdout
            .as_ref()
            .and_then(|file| file.file.as_ref())
        {
            Some(bep::file::File::Contents(contents)) => contents,
            other => panic!("expected inline action stdout, got {other:?}"),
        };
        assert_eq!(
            std::str::from_utf8(action_stdout).expect("action stdout utf8"),
            "compiler stdout"
        );
        let action_stderr = match action_payload
            .stderr
            .as_ref()
            .and_then(|file| file.file.as_ref())
        {
            Some(bep::file::File::Contents(contents)) => contents,
            other => panic!("expected inline action stderr, got {other:?}"),
        };
        assert_eq!(
            std::str::from_utf8(action_stderr).expect("action stderr utf8"),
            "compile failed"
        );
        assert_eq!(
            action_payload
                .failure_detail
                .as_ref()
                .map(|detail| detail.message.as_str()),
            Some("Action failed for //pkg:main: compile failed")
        );
        assert_eq!(action_payload.strategy_details.len(), 1);
        assert_eq!(action_payload.strategy_details[0].type_url, STRUCT_TYPE_URL);
        let strategy_details = Struct::decode(action_payload.strategy_details[0].value.as_slice())
            .expect("strategy details struct");
        assert_eq!(
            struct_string_field(&strategy_details, "strategy"),
            Some("local")
        );
        assert_eq!(
            struct_string_field(&strategy_details, "runner"),
            Some("local")
        );
        assert_eq!(
            struct_string_field(&strategy_details, "label"),
            Some("//pkg:main")
        );
        assert_eq!(
            struct_string_field(&strategy_details, "mnemonic"),
            Some("cxx_compile")
        );
        assert_eq!(struct_string_field(&strategy_details, "argv0"), Some("cc"));
        assert_eq!(
            struct_number_field(&strategy_details, "exit_code"),
            Some(1.0)
        );

        let Some(build_event_id::Id::TestResult(test_id)) =
            test_result[0].id.as_ref().and_then(|id| id.id.as_ref())
        else {
            panic!("expected TestResult id");
        };
        assert_eq!(test_id.label, "//pkg:main");
        assert_eq!(test_id.configuration.as_ref().unwrap().id, "cfg");
        assert!(matches!(
            test_result[0].payload,
            Some(build_event::Payload::TestResult(bep::TestResult {
                status,
                ..
            })) if status == bep::TestStatus::Passed as i32
        ));
        let Some(build_event::Payload::TestResult(result)) = test_result[0].payload.as_ref() else {
            panic!("expected TestResult payload");
        };
        assert!(
            result
                .test_action_output
                .iter()
                .any(|file| file.name == "test.log")
        );
        assert!(
            result
                .test_action_output
                .iter()
                .any(|file| file.name == "test.xml")
        );

        let final_events = converter.convert(
            7,
            &trace_event(buck2_data::buck_event::Data::Record(
                buck2_data::RecordEvent {
                    data: Some(buck2_data::record_event::Data::InvocationRecord(Box::new(
                        buck2_data::InvocationRecord {
                            outcome: Some(buck2_data::InvocationOutcome::Failed as i32),
                            exit_code: Some(1),
                            exit_result_name: Some("FAILED".to_owned()),
                            ..Default::default()
                        },
                    ))),
                },
            )),
        );
        let completed = final_events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::Completed(completed)) if !event.children.is_empty() => {
                    Some(completed)
                }
                _ => None,
            })
            .expect("failed target completion");
        assert_eq!(
            completed
                .failure_detail
                .as_ref()
                .map(|detail| detail.message.as_str()),
            Some("Target failed: //pkg:main")
        );
        let finished = final_events
            .iter()
            .find_map(|event| match event.payload.as_ref() {
                Some(build_event::Payload::Finished(finished)) => Some(finished),
                _ => None,
            })
            .expect("build finished");
        assert_eq!(
            finished
                .failure_detail
                .as_ref()
                .map(|detail| detail.message.as_str()),
            Some("Buck2 invocation failed with FAILED (1)")
        );
    }
}
