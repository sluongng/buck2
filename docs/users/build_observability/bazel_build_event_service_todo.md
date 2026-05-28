# Bazel Build Event Service TODO

This tracker records the remaining Buck2-to-Bazel Build Event Protocol
conversion work. The list is intentionally mutable: update it as code findings
make goals clearer, obsolete, or more urgent.

## P0/P1: BuildBuddy reuse blockers

- [ ] Emit target artifacts through `TargetComplete.output_group`.
  - [x] Add named sets for target outputs.
  - [x] Reference the named sets from the target completion event.
  - [x] Prefer URI-backed files when artifact upload is configured.
  - [ ] Verify local-only target artifacts are present in the configured
        bytestream or add a dedicated upload path for them.
- [x] Make action and test logs render in BuildBuddy.
  - [x] Keep inline file contents as a fallback for debugging.
  - [x] Add an optional bytestream upload path for BEP `File.uri`.
  - [x] Use URI-backed `stdout`, `stderr`, `test.log`, and `test.xml` when
        upload is enabled.
- [x] Emit BuildBuddy-usable command metadata.
  - [x] Emit canonical `StructuredCommandLine` with option-list sections.
  - [x] Emit `OptionsParsed`.
  - [x] Populate workspace status and build metadata with repo, user, host,
        command, target pattern, remote cache, remote executor, and CI values
        when Buck2 knows them.
- [x] Model tests as Bazel test events.
  - [x] Aggregate Buck2 testcase events into one `TestResult` per target run.
  - [x] Emit a matching `TestSummary` with run/shard/attempt counts.
  - [x] Synthesize a minimal `test.xml` from Buck2 testcase details.
- [x] Emit invocation-level metrics, configuration, and tool-log events.
  - [x] Emit `BuildMetrics` from `InvocationRecord` and aggregated Buck2
        metrics.
  - [x] Emit `Configuration` and `WorkspaceInfo` events when known.
  - [x] Emit `BuildToolLogs` with a compact Buck2 invocation summary when file
        upload is enabled.

## P2: Semantic parity

- [x] Populate `PatternExpanded.children` with configured targets when the
      resolved target list is available.
- [x] Clean up target lifecycle semantics.
  - [x] Avoid final target completion on analysis-only events when later build
        or test completion exists.
  - [x] Deduplicate configured and completed events for each target/configuration.
- [ ] Add structured failures.
  - [x] Populate `failure_detail` on failed actions, targets, and
        invocations.
  - [ ] Add test-specific failure details if Bazel BEP grows a direct field
        for test result or summary payloads.
  - [x] Emit `Aborted` for skipped, cancelled, or failed announced children that
        do not receive a normal terminal event.
- [x] Improve remote execution diagnostics.
  - [x] Populate `ActionExecuted.strategy_details` with Buck2 execution
        strategy, action digest, cache-hit type, queue time, remote session,
        and use-case details when they are available.
- [x] Improve action-cache statistics.
  - [x] Populate `BuildMetrics.ActionSummary.action_cache_statistics` with
        conservative hit and miss counters derived from Buck2 invocation
        action counters.

## P3: Remaining Bazel BEP parity

- [ ] Emit `Fetch` events for external downloads.
  - [x] Emit HTTP `Fetch` events for materialized `download_file` artifacts.
  - [x] Emit `Fetch` events for git-backed external cell population.
  - [ ] Emit fetch events for other external resource mechanisms once their
        event payloads carry stable resource URLs.
- [ ] Emit `TestProgress` for active test attempts when Buck2 exposes a stable
      live log URI.
- [x] Emit `TargetSummary`.
- [x] Emit `ConvenienceSymlinksIdentified`.
- [x] Emit `ExecRequestConstructed` for `buck2 run` once the client has
      constructed the post-build argv.

## BuildBuddy follow-up issues

- [ ] Render inline BEP `File.contents` as a fallback when no URI is present.
- [ ] Recover command metadata from `OptionsParsed`, `BuildMetadata`, or the
      original command line when canonical option-list chunks are missing.
- [ ] Match `ActionExecuted` events to targets by label/configuration when the
      BEP stream lacks target graph children.
- [ ] Refresh target detail pages while in-progress BES indexing catches up.

## Validation

- [ ] `cargo build --bin buck2`
- [ ] `./bootstrap/buck2 build //:buck2`
- [ ] Run a local BuildBuddy comparison between a real Bazel invocation and a
      Buck2 invocation using `event_format = "bazel"`.
- [ ] Capture overview, target detail, action log, and test detail screenshots.
