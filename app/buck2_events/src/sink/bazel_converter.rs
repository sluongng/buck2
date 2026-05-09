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
use std::collections::HashMap;

use bazel_bep_proto::build_event_stream as bep;
use bazel_bep_proto::build_event_stream::build_event;
use bazel_bep_proto::build_event_stream::build_event_id;
use bazel_bep_proto::command_line as cl;
use prost::Message as _;
use prost_types::Any;
use prost_types::Timestamp;

pub(crate) const BEP_EVENT_TYPE_URL: &str = "type.googleapis.com/build_event_stream.BuildEvent";

const BUILD_TOOL_VERSION: &str = "buck2";
const DEFAULT_CONFIGURATION_ID: &str = "buck2";
const GENERIC_TARGET_KIND: &str = "buck2 rule";
const TEST_TARGET_KIND: &str = "buck2_test rule";
const INTERRUPTED_EXIT_CODE: i32 = 8;
const MAX_INLINE_FILE_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TargetKey {
    label: String,
    configuration: String,
}

#[derive(Clone, Debug)]
struct CompletedTargetState {
    success: bool,
}

#[derive(Debug, Default)]
pub(crate) struct BazelEventConverter {
    saw_started: bool,
    progress_count: i32,
    completed_targets: BTreeMap<TargetKey, CompletedTargetState>,
    action_children: BTreeMap<TargetKey, BTreeMap<String, bep::BuildEventId>>,
    emitted_action_child_counts: BTreeMap<TargetKey, usize>,
}

impl BazelEventConverter {
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
        match span_start.data.as_ref() {
            Some(buck2_data::span_start_event::Data::Command(command)) => {
                if !self.saw_started {
                    events.push(self.started_event(event, Some(command)));
                    self.saw_started = true;
                }
                events.push(structured_command_line_event(command));
                if let Some(workspace) = workspace_status_event(command) {
                    events.push(workspace);
                }
                if let Some(metadata) = build_metadata_event_from_command(command) {
                    events.push(metadata);
                }
            }
            Some(buck2_data::span_start_event::Data::CommandCritical(command)) => {
                if let Some(metadata) = build_metadata_event_from_map(&command.metadata) {
                    events.push(metadata);
                }
            }
            Some(buck2_data::span_start_event::Data::Analysis(analysis)) => {
                if let Some(configured) = configured_event_from_analysis_start(analysis) {
                    events.push(configured);
                }
            }
            Some(buck2_data::span_start_event::Data::TestDiscovery(test_discovery)) => {
                if let Some(configured) = configured_event_from_test_label(
                    test_discovery.target_label.as_ref(),
                    TEST_TARGET_KIND,
                ) {
                    events.push(configured);
                }
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
        match span_end.data.as_ref() {
            Some(buck2_data::span_end_event::Data::Command(_)) => {
                self.emit_completed_updates_for_actions(events);
            }
            Some(buck2_data::span_end_event::Data::Analysis(analysis)) => {
                if let Some(completed) = completed_event_from_analysis_end(analysis) {
                    self.remember_completed(&completed);
                    events.push(completed);
                }
            }
            Some(buck2_data::span_end_event::Data::ActionExecution(action)) => {
                if let Some(action) = action_event(event, span_end, action) {
                    self.remember_action(&action);
                    events.push(action);
                }
            }
            Some(buck2_data::span_end_event::Data::TestDiscovery(test_discovery)) => {
                if let Some(configured) = configured_event_from_test_label(
                    test_discovery.target_label.as_ref(),
                    TEST_TARGET_KIND,
                ) {
                    events.push(configured);
                }
            }
            Some(buck2_data::span_end_event::Data::TestRun(test_end)) => {
                if let Some(summary) = test_summary_event(event, span_end, test_end) {
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
            Some(buck2_data::instant_event::Data::TargetPatterns(patterns)) => {
                if let Some(expanded) = pattern_expanded_event(patterns) {
                    events.push(expanded);
                }
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
                if let Some(event) = build_metadata_event(&metadata) {
                    events.push(event);
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
                if let Some(event) = build_metadata_event(&metadata) {
                    events.push(event);
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
                if let Some(event) = build_metadata_event(&metadata) {
                    events.push(event);
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
                if let Some(test_result) = test_result_event(result) {
                    events.push(test_result);
                }
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
                self.emit_completed_updates_for_actions(events);
                if let Some(metadata) = build_metadata_event_from_invocation(record) {
                    events.push(metadata);
                }
                events.push(finished_event_from_invocation_record(event, record));
            }
            Some(buck2_data::record_event::Data::BuildGraphStats(stats)) => {
                for target in &stats.build_targets {
                    if let Some(configured) = configured_event_from_build_target(target) {
                        events.push(configured);
                    }
                    if let Some(completed) = completed_event_from_build_target(target) {
                        self.remember_completed(&completed);
                        events.push(completed);
                    }
                }
            }
            _ => {}
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
                structured_command_line_id("original"),
                workspace_status_id(),
                build_metadata_id(),
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
            let Some(children_by_id) = self.action_children.get(&key) else {
                continue;
            };
            let child_count = children_by_id.len();
            if child_count == 0
                || self
                    .emitted_action_child_counts
                    .get(&key)
                    .is_some_and(|emitted| *emitted == child_count)
            {
                continue;
            }
            let Some(state) = self.completed_targets.get(&key) else {
                continue;
            };
            events.push(completed_event_with_children(
                key.label.clone(),
                key.configuration.clone(),
                state.success,
                children_by_id.values().cloned().collect(),
            ));
            self.emitted_action_child_counts.insert(key, child_count);
        }
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

fn workspace_status_id() -> bep::BuildEventId {
    bep::BuildEventId {
        id: Some(build_event_id::Id::WorkspaceStatus(
            build_event_id::WorkspaceStatusId {},
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

fn configuration_id(id: impl Into<String>) -> build_event_id::ConfigurationId {
    build_event_id::ConfigurationId { id: id.into() }
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
        Some(Data::HydrationPageOut(_)) => "hydration-page-out",
        None => "unknown",
    }
    .to_owned()
}

fn structured_command_line_event(command: &buck2_data::CommandStart) -> bep::BuildEvent {
    let mut sections = Vec::new();
    if let Some(command) = command.cli_args.first() {
        sections.push(chunk_section("command", vec![command.clone()]));
    }
    if command.cli_args.len() > 1 {
        sections.push(chunk_section("arguments", command.cli_args[1..].to_vec()));
    }

    bep::BuildEvent {
        id: Some(structured_command_line_id("original")),
        children: Vec::new(),
        payload: Some(build_event::Payload::StructuredCommandLine(
            cl::CommandLine {
                command_line_label: "original".to_owned(),
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

fn workspace_status_event(command: &buck2_data::CommandStart) -> Option<bep::BuildEvent> {
    let mut items = Vec::new();
    add_workspace_item(
        &mut items,
        "BUILD_USER",
        first_metadata(&command.metadata, &["USER", "BUILD_USER"]),
    );
    add_workspace_item(
        &mut items,
        "BUILD_HOST",
        first_metadata(&command.metadata, &["HOST", "BUILD_HOST"]),
    );
    add_workspace_item(
        &mut items,
        "BUILD_WORKING_DIRECTORY",
        first_metadata(
            &command.metadata,
            &["CWD", "PWD", "BUILD_WORKING_DIRECTORY"],
        ),
    );
    if items.is_empty() {
        return None;
    }

    Some(bep::BuildEvent {
        id: Some(workspace_status_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::WorkspaceStatus(
            bep::WorkspaceStatus { item: items },
        )),
        last_message: false,
    })
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

fn build_metadata_event_from_command(
    command: &buck2_data::CommandStart,
) -> Option<bep::BuildEvent> {
    let mut metadata: BTreeMap<String, String> = command
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if !command.tags.is_empty() {
        metadata.insert("TAGS".to_owned(), command.tags.join(","));
    }
    build_metadata_event(&metadata)
}

fn build_metadata_event_from_invocation(
    record: &buck2_data::InvocationRecord,
) -> Option<bep::BuildEvent> {
    let mut metadata = BTreeMap::new();
    if let Some(command_name) = record.command_name.as_ref()
        && !command_name.is_empty()
    {
        metadata.insert("BUCK2_COMMAND".to_owned(), command_name.clone());
    }
    if !record.tags.is_empty() {
        metadata.insert("TAGS".to_owned(), record.tags.join(","));
    }
    if !record.re_session_id.is_empty() {
        metadata.insert(
            "BUCK2_RE_SESSION_ID".to_owned(),
            record.re_session_id.clone(),
        );
    }
    build_metadata_event(&metadata)
}

fn build_metadata_event_from_map(metadata: &HashMap<String, String>) -> Option<bep::BuildEvent> {
    let metadata: BTreeMap<String, String> = metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    build_metadata_event(&metadata)
}

fn build_metadata_event(metadata: &BTreeMap<String, String>) -> Option<bep::BuildEvent> {
    if metadata.is_empty() {
        return None;
    }
    Some(bep::BuildEvent {
        id: Some(build_metadata_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::BuildMetadata(bep::BuildMetadata {
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })),
        last_message: false,
    })
}

fn pattern_expanded_event(patterns: &buck2_data::ParsedTargetPatterns) -> Option<bep::BuildEvent> {
    let patterns = patterns
        .target_patterns
        .iter()
        .map(|p| p.value.clone())
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();
    if patterns.is_empty() {
        return None;
    }
    Some(bep::BuildEvent {
        id: Some(bep::BuildEventId {
            id: Some(build_event_id::Id::Pattern(
                build_event_id::PatternExpandedId {
                    pattern: patterns.clone(),
                },
            )),
        }),
        children: Vec::new(),
        payload: Some(build_event::Payload::Expanded(bep::PatternExpanded {
            test_suite_expansions: Vec::new(),
        })),
        last_message: false,
    })
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
    bep::BuildEvent {
        id: Some(target_completed_id(label.clone(), configuration)),
        children,
        payload: Some(build_event::Payload::Completed(bep::TargetComplete {
            success,
            target_kind: String::new(),
            test_size: bep::TestSize::Unknown as i32,
            output_group: Vec::new(),
            important_output: Vec::new(),
            tag: Vec::new(),
            test_timeout_seconds: 0,
            directory_output: Vec::new(),
            failure_detail: None,
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
            r#type: action_mnemonic(action),
            command_line,
            action_metadata_logs: Vec::new(),
            failure_detail: None,
            start_time,
            end_time,
            strategy_details: Vec::new(),
        })),
        last_message: false,
    })
}

fn test_result_event(result: &buck2_data::TestResult) -> Option<bep::BuildEvent> {
    let label = label_for_configured_target(result.target_label.as_ref()?)?;
    let configuration = configuration_id_for_target(result.target_label.as_ref()?);
    Some(bep::BuildEvent {
        id: Some(test_result_id(label, configuration)),
        children: Vec::new(),
        payload: Some(build_event::Payload::TestResult(bep::TestResult {
            status: test_status(result.status),
            cached_locally: false,
            test_attempt_start_millis_epoch: 0,
            test_attempt_duration_millis: 0,
            test_action_output: Vec::new(),
            warning: Vec::new(),
            execution_info: None,
            status_details: result.details.clone(),
            test_attempt_start: None,
            test_attempt_duration: result.duration.clone(),
        })),
        last_message: false,
    })
}

fn test_summary_event(
    event: &buck2_data::BuckEvent,
    span_end: &buck2_data::SpanEndEvent,
    test_end: &buck2_data::TestRunEnd,
) -> Option<bep::BuildEvent> {
    let suite = test_end.suite.as_ref()?;
    let label = label_for_configured_target(suite.target_label.as_ref()?)?;
    let configuration = configuration_id_for_target(suite.target_label.as_ref()?);
    let status = test_status_from_command_report(test_end.command_report.as_ref());
    let end_time = event.timestamp.clone();
    Some(bep::BuildEvent {
        id: Some(test_summary_id(label, configuration)),
        children: Vec::new(),
        payload: Some(build_event::Payload::TestSummary(bep::TestSummary {
            overall_status: status,
            total_run_count: suite.test_names.len().max(1) as i32,
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
            total_run_duration: test_end
                .command_report
                .as_ref()
                .and_then(|command| command.details.as_ref())
                .and_then(|details| details.metadata.as_ref())
                .and_then(|metadata| metadata.wall_time.clone()),
        })),
        last_message: false,
    })
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

fn finished_event(
    timestamp: Option<Timestamp>,
    exit_code: i32,
    exit_name: impl Into<String>,
) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(build_finished_id()),
        children: Vec::new(),
        payload: Some(build_event::Payload::Finished(bep::BuildFinished {
            overall_success: exit_code == 0,
            finish_time_millis: 0,
            anomaly_report: None,
            exit_code: Some(bep::build_finished::ExitCode {
                name: exit_name.into(),
                code: exit_code,
            }),
            finish_time: timestamp,
            failure_detail: None,
        })),
        last_message: true,
    }
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
    let Some(details) = command.and_then(|command| command.details.as_ref()) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    if !details.cmd_stdout.is_empty() {
        files.push(file_with_contents("stdout", details.cmd_stdout.clone()));
    }
    if !details.cmd_stderr.is_empty() {
        files.push(file_with_contents("stderr", details.cmd_stderr.clone()));
    }
    files
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
                            "//:main".to_owned(),
                        ],
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

        let any = encode_bep_event(&events[0]);
        assert_eq!(any.type_url, BEP_EVENT_TYPE_URL);
        let decoded = bep::BuildEvent::decode(any.value.as_slice()).expect("valid BEP event");
        assert!(matches!(
            decoded.payload,
            Some(build_event::Payload::Started(_))
        ));
    }

    #[test]
    fn command_end_does_not_emit_build_finished() {
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
                    ..Default::default()
                },
            )),
        );

        assert!(
            !events
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::Finished(_))))
        );
    }

    #[test]
    fn invocation_record_controls_build_finished_status() {
        let mut converter = BazelEventConverter::default();
        let command_end = converter.convert(
            1,
            &trace_event(buck2_data::buck_event::Data::SpanEnd(
                buck2_data::SpanEndEvent {
                    data: Some(buck2_data::span_end_event::Data::Command(
                        buck2_data::CommandEnd {
                            data: Some(buck2_data::command_end::Data::Test(
                                buck2_data::TestCommandEnd {
                                    unresolved_target_patterns: Vec::new(),
                                },
                            )),
                            is_success: true,
                            build_result: Some(buck2_data::BuildResult {
                                build_completed: true,
                            }),
                        },
                    )),
                    ..Default::default()
                },
            )),
        );
        assert!(
            !command_end
                .iter()
                .any(|event| matches!(event.payload, Some(build_event::Payload::Finished(_))))
        );

        let final_events = converter.convert(
            2,
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
        assert_eq!(completed_update.children.len(), 1);
        assert_eq!(completed_update.children[0], action[0].id.clone().unwrap());
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
                            }],
                            commands: vec![buck2_data::CommandExecution {
                                details: Some(buck2_data::CommandExecutionDetails {
                                    signed_exit_code: Some(1),
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
        let test_result = converter.convert(
            5,
            &trace_event(buck2_data::buck_event::Data::Instant(
                buck2_data::InstantEvent {
                    data: Some(buck2_data::instant_event::Data::TestResult(
                        buck2_data::TestResult {
                            status: buck2_data::TestStatus::Pass as i32,
                            target_label: Some(target.clone()),
                            details: "passed".to_owned(),
                            ..Default::default()
                        },
                    )),
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

        let Some(build_event_id::Id::TargetCompleted(completed_id)) =
            completed[0].id.as_ref().and_then(|id| id.id.as_ref())
        else {
            panic!("expected TargetCompleted id");
        };
        assert_eq!(completed_id.label, "//pkg:main");
        assert_eq!(completed_id.configuration.as_ref().unwrap().id, "cfg");

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
    }
}
