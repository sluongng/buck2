---
oncalls: ['build_infra']
---

# Buck2 BuildBuddy Testing Plan

This file tracks the plan for using Buck2 itself, locally and with BuildBuddy
remote execution, as a regular validation path for this stack. It is written as
a handoff note for future agents and should be updated whenever the commands or
status change.

## Goal

Keep Cargo validation as the release safety net, but make Buck2 validation useful
for day-to-day stack maintenance:

- run focused Buck2 tests locally while editing,
- run the same focused tests through BuildBuddy RBE before publishing,
- grow the target set from smoke tests to topic-specific suites,
- eventually test a newly built Buck2 using the last known-good or released
  Buck2 binary as the bootstrap client.

## Current Status

As of 2026-05-19:

- cargo test -p buck2_re_configuration -p remote_execution -p buck2_events --lib
  passes for this stack.
- cargo build -p buck2 --bin buck2 passes and produces target/debug/buck2.
- target/debug/buck2 --isolation-dir codex-stack-verify build //app/buck2:buck2
  passed through BuildBuddy RBE:
  https://app.buildbuddy.io/invocation/801d8a31-c552-42be-8363-46e2c4ad788c.
- target/debug/buck2 --isolation-dir codex-test-smoke-retry test
  //app/buck2_error_tests:buck2_error_tests passed through BuildBuddy RBE:
  https://app.buildbuddy.io/invocation/02990606-df51-4ea0-b4dd-840f7bd5f5c3.
- target/debug/buck2 --isolation-dir codex-test-smoke-rbe test --remote-only
  --unstable-allow-all-tests-on-re //app/buck2_error_tests:buck2_error_tests
  passed through BuildBuddy RBE:
  https://app.buildbuddy.io/invocation/d6b2b417-fb6e-45e2-9e75-a7beb110b717.
- target/debug/buck2 --isolation-dir codex-test-smoke-local-override test
  -c build.execution_platforms= --local-only --no-remote-cache
  //app/buck2_error_tests:buck2_error_tests passed locally with 2155 local
  commands, 0 remote commands, and 0 cache hits.
- target/debug/buck2 --isolation-dir codex-test-app-rbe2 test --remote-only
  --unstable-allow-all-tests-on-re //app/buck2_error_tests:buck2_error_tests
  passed through BuildBuddy RBE:
  https://app.buildbuddy.io/invocation/7ed3729d-0f74-43ce-b73e-d7ee2a163e81.
- target/debug/buck2 --isolation-dir codex-test-go-local test -c project.ignore=
  -c build.execution_platforms= --local-only --no-remote-cache
  //examples/with_prelude/go/hello/greeting:greeting_test passed locally with
  550 local commands, 0 remote commands, and 0 cache hits.
- target/debug/buck2 --isolation-dir codex-test-go-rbe test -c project.ignore=
  --remote-only --unstable-allow-all-tests-on-re
  //examples/with_prelude/go/hello/greeting:greeting_test failed remotely
  because the current BuildBuddy image has no go binary on PATH.
- The six-target app Rust matrix passed locally with 2943 local commands, 0
  remote commands, and 0 cache hits:
  //app/buck2_action_impl_tests:buck2_action_impl_tests,
  //app/buck2_build_api_tests:buck2_build_api_tests,
  //app/buck2_core:soft_error,
  //app/buck2_error_tests:buck2_error_tests,
  //app/buck2_interpreter_for_build_tests:buck2_interpreter_for_build_tests,
  and //app/buck2_node_tests:buck2_node_tests.
- The same six-target app Rust matrix passed through BuildBuddy RBE with 3302
  remote commands, 0 local commands, and BuildBuddy invocation
  https://app.buildbuddy.io/invocation/ebb750f0-cdb8-400a-ad66-dd8ffa6a7177.
- docs/buck2_lab/greeter_lib:test now passes locally with 10 local commands and
  remotely with 10 remote commands after fixing the OSS rust_test marker and
  local docs labels. Remote invocation:
  https://app.buildbuddy.io/invocation/a4309795-edc3-4ea1-9809-043414bd68c1.
- gazebo/strong_hash_tests:strong_hash_tests is a small non-app rust_test. The
  checked-in gazebo-rust matrix passed locally with 231 local commands:
  https://app.buildbuddy.io/invocation/1bfabf09-531b-42ee-960b-18098abfc462,
  and remotely with 234 remote commands:
  https://app.buildbuddy.io/invocation/9e568691-93af-4916-8e5f-5b00646eeff3.
- prelude//java/tools:test_list_class_names is the first reliable prelude
  python_test probe. It requires --target-platforms prelude//platforms:default;
  without that, configured analysis fails because the checkout has no default
  target platform. It passed locally with 3 local commands:
  https://app.buildbuddy.io/invocation/92b01a8f-89bf-49e6-ba67-31af560a7d41,
  and remotely with 3 remote commands:
  https://app.buildbuddy.io/invocation/0d28d765-fac0-4d1b-bcb6-73cc1c307b9d.
  The same target now has a checked-in runner matrix, verified with
  --matrix prelude-python --mode both --clean-output and --isolation-prefix
  codex-runner-prelude-python. Runner invocations:
  https://app.buildbuddy.io/invocation/d6495b67-4d6a-4464-b8e4-4fd9fb2db44e
  for the local half, and
  https://app.buildbuddy.io/invocation/586e9fef-3761-4b16-ad6f-b7b42c9513eb
  for the remote half.
- prelude//go/tools/gobuckify/lib:gobuckifylib_test also requires
  --target-platforms prelude//platforms:default. It passed locally with 550
  local commands, but failed remotely because the BuildBuddy image has no go
  binary on PATH:
  https://app.buildbuddy.io/invocation/67c44838-c1e8-4b09-8682-af1c88f5e5ce.
- tests/prelude is now queryable after adding OSS shims for py_assertion,
  selects.fmt, go_binary, and go_package. The query
  kind(".*test.*", //tests/prelude/...) with project.ignore= discovers 9
  go_test targets, and kind(".*", //tests/prelude/...) discovers 117 targets.
  A representative Go test, //tests/prelude/go/go_test/basic:test, passed
  locally with 550 local commands:
  https://app.buildbuddy.io/invocation/feb6ee65-6a6f-4f1c-b686-eb163397ec9c.
  The same target failed remotely at prelude//go/tools:copy_goroot because the
  BuildBuddy image has no go binary:
  https://app.buildbuddy.io/invocation/c533a550-feb9-41df-b23e-5fce1650e9ba.
- tests/prelude/command_alias can now be queried after adding an OSS
  py_assertion shim and selects.fmt shim support. A representative build
  assertion, //tests/prelude/command_alias/quoting:check_quoted_args, passed
  locally with 1 local command:
  https://app.buildbuddy.io/invocation/565ed6e1-ed2d-48b4-aa80-904cbabde543,
  and remotely with 1 remote command:
  https://app.buildbuddy.io/invocation/4ca108db-2371-4cbb-a4ed-cb02f72c105c.
  Do not treat the whole command_alias subtree as reliable yet: the full local
  check-target set still hits a symlink target that resolves through the shim
  cell, and the full remote check-target set includes a relative-path case that
  expects buck-out/v2 as a working directory.
  The representative target now has a checked-in runner matrix, verified with
  --matrix prelude-command-alias --mode both --clean-output and
  --isolation-prefix codex-runner-command-alias. Runner invocations:
  https://app.buildbuddy.io/invocation/b9fda5de-602e-4774-a361-b9854a0bedb4
  for the local half, and
  https://app.buildbuddy.io/invocation/f1b69cf9-28c1-4fb3-a553-82825bdf7547
  for the remote half.
- tests/tools/makefile_to_depfile:integration is now queryable after adding an
  OSS cram_test shim that does not depend on a system cram installation. The
  query kind(".*test.*", //tests/tools/...) with project.ignore= discovers that
  single cram_test target. The checked-in tests-tools-cram matrix passed
  locally:
  https://app.buildbuddy.io/invocation/0601b9b1-00bb-45e4-a46a-cf0e6ea57b09,
  and remotely:
  https://app.buildbuddy.io/invocation/ed72168e-2515-4ce2-a9a8-d7345f946f89.
- tests/core and tests/e2e are now queryable with project.ignore= after adding
  OSS shim coverage for buck_e2e_test, bxl_test, check_dependencies_test, the
  Python e2e utility import layout, and minimal pytest/decorator/psutil
  compatibility modules. The query kind(".*test.*", //tests/core/... +
  //tests/e2e/...) discovers 381 targets: 295 under tests/core and 86 under
  tests/e2e. A representative core e2e target,
  //tests/core/analysis:test_cmd_args, now has a checked-in core-e2e-smoke
  matrix. The matrix passed locally with 6502 local commands:
  https://app.buildbuddy.io/invocation/06171416-3c5b-4298-a8a9-82f753c51ee8,
  and remotely with 6907 remote commands:
  https://app.buildbuddy.io/invocation/37162403-bda4-4dc7-bae8-b394bd0af29b.
- tests/select_type_params:test_select_types exercises the same OSS Buck e2e
  runner with 19 test cases. The checked-in select-type-params matrix passed
  locally with 6502 local commands:
  https://app.buildbuddy.io/invocation/dedab503-b3e5-4097-9be9-baf7706377d1,
  and remotely with 6907 remote commands:
  https://app.buildbuddy.io/invocation/c90d57a4-628d-49a9-8469-4affcee0e9de.
- tests/core/build:test_build_response exercises the same OSS Buck e2e runner
  against a build-result failure case and requires pytest's tmp_path fixture.
  The runner now provides tmp_path for tests that request it. The checked-in
  core-build-response matrix passed locally with 6502 local commands:
  https://app.buildbuddy.io/invocation/684f703a-4bb6-48c4-bb91-c00e17ce858b,
  and remotely with 6907 remote commands:
  https://app.buildbuddy.io/invocation/ad1f302f-aac6-4d24-a0a8-e140aaecc43a.
- app_dep_graph_rules:test_buck2_dep_graph is a build-time assertion, not a
  Buck test-runner target. It is now usable in OSS after translating the
  hard-coded internal Buck2 query labels to the root cell. The checked-in
  app-dep-graph matrix passed locally:
  https://app.buildbuddy.io/invocation/26318e82-5e5e-4eb2-a72b-9a532750fcc0,
  and remotely:
  https://app.buildbuddy.io/invocation/01ff5dad-46b4-4aea-bd63-457db78b2431.
  The target has no outputs, so --remote-only validates analysis under the
  remote config but does not schedule remote execution actions.
- buildbuddy/run_buck2_test_matrix.sh is the checked-in entry point for the
  first reliable matrix. It accepts BUCK2=/path/to/buck2 so a future prebuilt
  release binary can drive the same gate.
- After the latest rebase, the runner itself was verified with --mode local
  --matrix smoke --clean-output and --isolation-prefix
  codex-runner-rebased-smoke-local: Pass 7, 2951 local commands, 0 remote
  commands. Local BuildBuddy event stream:
  https://app.buildbuddy.io/invocation/67118bda-abf3-42df-a69c-6ca9a22c49a0.
- After the latest rebase, the runner itself was verified with --mode remote
  --matrix smoke --clean-output and --isolation-prefix
  codex-runner-rebased-smoke-remote: Pass 7, 3310 remote commands, 0 local
  commands. BuildBuddy invocation:
  https://app.buildbuddy.io/invocation/35ac6c7e-1a65-4829-b568-872b20c125a0.
- The recommended combined command was also verified with --mode both
  --matrix smoke --clean-output and --isolation-prefix
  codex-runner-rebased-smoke.
- After a later clean rebase onto origin/main 923538a40a99, the stack tip was
  rechecked with the smoke, prelude-python, and prelude-command-alias matrices
  in both local and remote mode. The tests/prelude Go query still discovers 9
  go_test targets; //tests/prelude/go/go_test/basic:test still passes locally
  and fails remotely because the BuildBuddy image has no go binary.
- buildbuddy/run_stack_test_matrix.sh is the per-commit wrapper for this stack.
  It uses temporary detached worktrees, leaves the main checkout alone, and
  defaults to the app-rust matrix because the docs-lab target is only fixed at
  the stack tip.
- The stack wrapper stages generated test-harness support into old worktrees:
  the current checked-in shim/third-party/rust/BUCK.reindeer, shim/git_fetch.bzl
  when needed by that generated shim, and the BuildBuddy execution-platform
  package for remote mode. For remote mode using the private .buckconfig.local,
  it copies .buckconfig.local and .buckconfig.buildbuddy into the temporary
  worktree so Buck loads them as local config; do not use --keep-worktrees with
  private credentials unless you intend to clean them up yourself.
- The stack wrapper was verified with --mode local --matrix app-rust --limit 1
  on the first rebased stack commit, 871f4d286864. It passed with 2913 local
  commands after copying the generated Rust shim and shim/git_fetch.bzl.
- The stack wrapper was also verified with --mode local --matrix prelude-python
  --limit 1 on the first rebased stack commit, 871f4d286864. It passed with 3
  local commands after copying the generated Rust shim and shim/git_fetch.bzl.
- The stack wrapper was also verified with --matrix prelude-command-alias
  --limit 1 on the first rebased stack commit, 871f4d286864. Local mode passed
  with 1 local command after staging py_assertion.bzl and selects.bzl shim
  support into the temporary worktree. Remote mode passed with 1 remote command
  and BuildBuddy invocation
  https://app.buildbuddy.io/invocation/c88e3609-dcbd-436c-8dc2-ab7fa4c4c198.
- A full local stack sweep was then run with --mode local --matrix app-rust
  --log-dir. The first pre-rebase run passed commits 1-9 and exposed a real
  stack-ordering bug at [bes bep 1/3]: the new BEP converter matched
  Data::HydrationPageOut, but that historical tree only exposed
  Data::Hydration. The stack was rewritten so [bes bep 1/3] uses
  Data::Hydration.
- On the origin/main 4f075abb28f1 rebase, the local app-rust stack sweep
  was rerun on the current hashes. Commits 1-28 passed in
  /tmp/codex-buck2-stack-logs-local-rebased; the first run hit its overall
  timeout as commit 29 started, then commits 29-39 passed in
  /tmp/codex-buck2-stack-logs-local-rebased-tail. Representative results:
  first commit 871f4d286864 passed with 2913 local commands, commit 28
  6397e18d6e19 passed with 2939 local commands, and final tip 493cd6a343e2
  passed with 2943 local commands. The final rebased stack tip is
  493cd6a343e2.
- The older app-rust and gazebo-rust remote stack probes reach BuildBuddy before
  [remote inputs 1/3] after staging the remote config and BuildBuddy package,
  but they still fail because that part of the historical stack predates the
  remote Rust toolchain support: prelude//rust/tools:rustc_cfg reports
  "executable file 'rustc' not found in $PATH". Failing app-rust invocation:
  https://app.buildbuddy.io/invocation/31001b08-8fb1-4fbf-b991-8bbd8f49e55d.
- A manual remote check at [buildbuddy 2/5] with the private config staged in
  the temporary worktree passed for //app/buck2_core:soft_error with 2417
  remote commands:
  https://app.buildbuddy.io/invocation/fe0c7f1e-42a6-4a6c-8dfa-2ed4ce02dc20.
- The remote-capable tail was verified on that rebase with
  --from 6397e18d6e19 --mode remote --matrix app-rust --log-dir
  /tmp/codex-buck2-stack-logs-remote-rebased-tail. All 11 commits from
  [remote inputs 1/3] through the tip passed. Each commit ran 3302 remote
  commands and 0 local commands. Representative invocations: first tail commit
  1b55be597aa3:
  https://app.buildbuddy.io/invocation/52fbeb50-f897-4dc9-affe-16748d3ac103,
  final tip 493cd6a343e2:
  https://app.buildbuddy.io/invocation/cfdfee00-5d1a-48df-b696-47a2dff87a3f.
- The core-e2e-smoke matrix now covers the full rebased stack remotely. A
  limit-1 run covered the first stack commit, f75e50d650aa, in
  /tmp/codex-buck2-stack-logs-core-e2e-remote-full-limit1. The missing
  pre-[remote inputs] range then passed for commits 2-28, 36fbd8ab8a6a through
  579e6fc81816, in
  /tmp/codex-buck2-stack-logs-core-e2e-remote-preremote-rest. The tail was
  covered by
  /tmp/codex-buck2-stack-logs-core-e2e-remote-tail-limit1 and
  /tmp/codex-buck2-stack-logs-core-e2e-remote-tail-rest. Each commit ran
  //tests/core/analysis:test_cmd_args with no local commands; the first commit
  used 6787 remote commands plus 94 cached actions, and later commits ran 6881
  to 6907 remote commands. Representative invocations: first stack commit
  f75e50d650aa:
  https://app.buildbuddy.io/invocation/f8d78c29-5f0d-45f3-ba56-465ee9c90b41,
  first pre-inputs follow-up commit 36fbd8ab8a6a:
  https://app.buildbuddy.io/invocation/a1092673-d483-49a9-a8ca-caaacf1dfb4f,
  last pre-inputs commit 579e6fc81816:
  https://app.buildbuddy.io/invocation/9685a412-f16b-4509-a1ef-ff57398dc1ab,
  and final tail commit before the docs amend:
  https://app.buildbuddy.io/invocation/f666caeb-9f61-4aaa-82e5-c320b9150e84.
- The tests-tools-cram matrix was verified across the stack in both local and
  remote modes after rebasing onto origin/main 923538a40a99. The first run,
  /tmp/codex-buck2-stack-logs-tools-cram-both2, passed commits 1-29, then the
  host killed the local Buck2 process on commit 30 with exit 137. After killing
  stale Buck2 daemons, the tail rerun,
  /tmp/codex-buck2-stack-logs-tools-cram-both2-tail, passed commits 30-40.
  Treat this matrix as reliable, with host resource cleanup needed between long
  repeated sweeps on this workstation.
- The core-e2e-smoke stack wrapper staging path was verified with --mode local
  --matrix core-e2e-smoke --limit 1 after rebasing onto origin/main
  923538a40a99. It passed the first stack commit, f75e50d650aa, after copying
  the current e2e shim support into the temporary worktree. Log dir:
  /tmp/codex-buck2-stack-logs-core-e2e-limit1c.
- A full local core-e2e-smoke stack sweep now passes on the rebased stack. The
  first run, /tmp/codex-buck2-stack-logs-core-e2e-local-full, passed commits
  1-11 from f75e50d650aa through f37acfc8f822, then was stopped manually while
  commit 12 was installing Rust. The resumed run,
  /tmp/codex-buck2-stack-logs-core-e2e-local-tail2, passed all 29 remaining
  commits from 3e70a381f6ad through the stack tip at the time. Each commit ran
  //tests/core/analysis:test_cmd_args locally with roughly 6497-6502 local
  commands and no remote execution. This matrix is useful, but it is much
  heavier than tests-tools-cram: a full clean local sweep is currently an
  hour-plus operation on this workstation.
- After adding the check-dependencies matrix to the final topic, the topic was
  rerun through the stack wrapper with --from HEAD~1 --to HEAD --mode both
  --matrix core-e2e-smoke. It passed locally with 6502 local commands:
  https://app.buildbuddy.io/invocation/9b565ede-6aa8-4eff-8b9f-f5352a70db71,
  and remotely with 6907 remote commands:
  https://app.buildbuddy.io/invocation/b55d8933-5d05-4e4c-896a-b1a93f2881cc.
  Log dir: /tmp/codex-buck2-stack-logs-core-e2e-current-tip-aa9.
- The core-build-response stack wrapper staging path was verified with
  --mode both --matrix core-build-response --limit 1. It passed the first stack
  commit, f75e50d650aa, after copying the current e2e shim support and
  tmp_path-capable OSS runner into the temporary worktree. Log dir:
  /tmp/codex-buck2-stack-logs-core-build-response-limit1. The local half ran
  with 6476 local commands:
  https://app.buildbuddy.io/invocation/e6e58943-93d7-466d-8e8b-cc674ef0f879.
  The remote half reused cached remote actions where available and ran 4 remote
  commands:
  https://app.buildbuddy.io/invocation/e3ab7ae5-f989-46b5-9a9c-f7e313c2064a.
- The app-dep-graph stack wrapper staging path was verified with --mode both
  --matrix app-dep-graph --limit 1. It passed the first stack commit,
  f75e50d650aa, after copying the current app_dep_graph_rules label-translation
  fix into the temporary worktree. Log dir:
  /tmp/codex-buck2-stack-logs-app-dep-graph-limit1.
- The gazebo-rust stack wrapper path was tried with --mode both --limit 1.
  Local mode passed the first stack commit, f75e50d650aa, with 227 local
  commands. The remote half failed at prelude//rust/tools:rustc_cfg with
  "executable file 'rustc' not found in $PATH", the same historical remote
  blocker seen by app-rust before the remote Rust toolchain support lands. Log
  dir: /tmp/codex-buck2-stack-logs-gazebo-rust-limit1.
- The host can hit the Linux file-watch limit after repeated Buck2 daemon runs.
  target/debug/buck2 killall was not enough once old isolation dirs accumulated;
  removing generated isolation dirs under buck-out reduced buck-out from 29G to
  276K and allowed local-only testing to start.
- A scoped source inventory query now succeeds after adding OSS compatibility
  for the bootstrap example cell, app/buck2_explain's Meta-style
  python_binary/main_src usage, the shared example test_utils.bzl load,
  shed/completion_verify's RPM package macro, and ci.remove_labels. The query
  intentionally keeps generated output and standalone example projects out of
  the root-cell walk:

  ~~~bash
  target/debug/buck2 --isolation-dir codex-test-inventory-count \
    uquery \
    -c 'project.ignore=buck-out, examples/hello_world/buck-out, examples/hello_world/output, output, examples/no_prelude, examples/persistent_worker, examples/toolchains/conan_toolchain, examples/vscode, examples/with_prelude/haskell, examples/with_prelude/third-party/haskell, examples/with_prelude/ocaml, examples/with_prelude/third-party/ocaml' \
    'kind(".*test.*", //...)'
  ~~~

  On this machine it discovered 422 labels. A typed follow-up with
  --output-attribute '^buck.type$' counted 382 python_test, 10 go_test,
  9 rust_test, 1 cram_test, 1 java_test, 1 robolectric_test, 1
  android_instrumentation_test, 6 remote-execution example targets named
  tests, and several test-toolchain/configuration targets whose rule names
  match the broad kind pattern. BuildBuddy invocations:
  https://app.buildbuddy.io/invocation/c4cd0b0d-506e-4d33-905c-b9fc7cdf31b3
  and
  https://app.buildbuddy.io/invocation/1eddba68-1b0d-488e-acfd-a25cbf21c642.
- A direct prelude test inventory query now succeeds with
  --target-platforms prelude//platforms:default. It counted 41 java_test,
  11 python_test, 9 kotlin_test, 4 erlang_test, and 2 go_test targets:
  https://app.buildbuddy.io/invocation/247ac35d-efbc-4c1a-8e41-3e2ab10297ec.
- prelude//erlang/common_test/test_exec:ct_executor_SUITE is not ready for the
  matrix. Local analysis currently needs an erlang-default shim target, and this
  host has no erl binary installed, so adding the shim alone would not make the
  test executable.
- prelude//toolchains/android/test/com/facebook/buck/io/pathformat:pathformat is
  the current Java prelude probe. The shim now provides Java, prebuilt jar, dex,
  zip, and Kotlin bootstrap toolchains, which gets the target to real Kotlin
  compilation. Local execution is still blocked by the rolling host JDKs:
  Kotlin's embedded JavaVersion parser rejects both default OpenJDK 26.0.1 and
  GraalVM 25.0.2. The default-JDK local failure was
  https://app.buildbuddy.io/invocation/76136c54-d2b8-4fb8-9cac-4c05488744fe;
  the JDK 25 retry was
  https://app.buildbuddy.io/invocation/65f5b2fe-34d1-44f8-a854-fcd6d832c97d.
  A remote-only probe reached remote javac actions but failed with RE upload
  timeouts:
  https://app.buildbuddy.io/invocation/58169743-ecbb-4ebd-88af-62f92e50e49c.
  Keep this target out of the reliable matrix until a supported JDK, downloaded
  Java toolchain, or remote worker image fix is available.
- The bxl_test shim now uses Buck2's Python BXL template instead of passing a
  .bxl file directly to python_test. That gets BXL macro targets to real test
  execution, but the probed //tests/e2e/bxl/fs:fs_inplace.bxl target is still
  not matrix-ready in OSS because its BXL script asserts internal
  fbsource//fbcode paths. The Python BXL e2e suites are also blocked today
  because they depend on the missing //tests/targets:isolated_targets package.
- check_dependencies_test targets no longer produce a false-positive zero-case
  pass in OSS. The shim now runs Buck2's Python check-dependencies template and
  remaps the minimal OSS fixture graph into an isolated project root, so the
  nested BXL Buck2 command has a .buckconfig locally and on BuildBuddy workers.
  The representative target,
  //tests/e2e/check_dependencies_test:allow_list_and_block_none, now has a
  checked-in check-dependencies matrix. It passed locally with one real test
  case and 6502 local commands:
  https://app.buildbuddy.io/invocation/69ec005a-8707-4e25-b387-1971252a0320,
  and remotely with 6907 remote commands:
  https://app.buildbuddy.io/invocation/e941a3e0-7ff4-4323-8e4b-eb398df639d6.
  The broader check-dependencies-derived package is still not promoted as a
  whole; this matrix intentionally covers the representative allowlist BXL path.

Do not treat broad Buck2 test discovery as solved yet:

- uquery 'kind(".*test.*", //...)' first failed because
  docs/buck2_lab/greeter_lib/BUCK had a parse error around an OSS marker. That
  parse error is fixed in this working tree, and //docs/buck2_lab/... now
  discovers gh_facebook_buck2//docs/buck2_lab/greeter_lib:test.
- After the lab parse fix, uquery 'kind(".*test.*", //...)' next exposed a
  missing shim load from shed/completion_verify/packages/BUCK:
  gh_facebook_buck2_shims_meta//buck2/shed/rpm_download/packages.bzl. That
  shim now exists for analysis, backed by an OSS placeholder download script.
- The root .buckconfig ignores tests and examples under [project].
- Clearing project.ignore entirely is still too broad on a dirty developer
  checkout because it walks generated output directories such as buck-out and
  examples/hello_world/output. Use the scoped source inventory command above
  when the goal is to include tests and supported examples.
- Clearing project.ignore no longer blocks core/e2e discovery on missing
  buck_e2e shim paths. The current shim support makes tests/core and tests/e2e
  queryable and validates one representative Buck e2e target locally and
  remotely. Do not promote the full core/e2e suites yet: the OSS pytest runner
  is intentionally small, and broader execution still needs feature-by-feature
  validation of parametrize, skip/xfail behavior, BXL tests, check-dependencies
  tests, and target-specific toolchains.
- After adding shim coverage for tests/prelude/py_assertion.bzl, selects.fmt,
  go_binary, and go_package, tests/prelude is queryable. Its Go tests remain
  local-only until the BuildBuddy image has Go or the Go toolchain is downloaded
  remotely.
- Broad examples/with_prelude discovery now gets past the shared
  //:test_utils.bzl load. Haskell and OCaml examples still need downloaded SDK
  trees before their third-party packages can be analyzed from this checkout.
- Standalone example projects such as examples/no_prelude,
  examples/persistent_worker, examples/toolchains/conan_toolchain, and
  examples/vscode still need separate-cell treatment before they can be part of
  a root-cell //... inventory.
- The new core/e2e shim path is a targeted OSS compatibility layer, not a full
  replacement for fbcode's python_pytest macro, CI hint macros, Buck2 modifiers,
  or test-selection behavior.
- Querying the prelude cell directly with prelude//... discovers python_test,
  erlang_test, go_test, java_test, and kotlin_test targets. Most still need
  toolchain-specific validation before they can join the reliable matrix.
- Querying //remote_execution/... finds no Buck test targets in this checkout;
  remote_execution coverage is still via Cargo tests for now.

## Current Config Shape

This checkout has a private .buckconfig.local on the developer machine that
includes .buckconfig.buildbuddy and adds credentials. That means plain
target/debug/buck2 commands on this machine use BuildBuddy by default.

For reusable documentation and scripts:

- keep credentials out of the repo,
- pass .buckconfig.buildbuddy explicitly when testing the committed RBE profile
  from a clean checkout,
- use a separate local-only config or explicit --local-only command when the goal
  is to prove local execution,
- use a fresh --isolation-dir for each validation mode so daemons and cached
  config do not hide failures.

## Test Inventory

Known Buck-queryable smoke targets:

- Rust unit tests under app/*_tests, for example
  //app/buck2_error_tests:buck2_error_tests.
- The Go prelude example test //examples/with_prelude/go/hello/greeting:greeting_test
  after clearing project.ignore.
- The lab Rust test //docs/buck2_lab/greeter_lib:test after the OSS marker parse
  fix in docs/buck2_lab/greeter_lib/BUCK.
- A small non-app Rust test, //gazebo/strong_hash_tests:strong_hash_tests.
- A small prelude python_test, prelude//java/tools:test_list_class_names, when
  run with --target-platforms prelude//platforms:default.
- A representative core e2e target, //tests/core/analysis:test_cmd_args, after
  clearing project.ignore.
- A representative core build-result e2e target,
  //tests/core/build:test_build_response, after clearing project.ignore.
- A broader Buck e2e runner probe,
  //tests/select_type_params:test_select_types, after clearing project.ignore.
- A build-time dependency graph assertion,
  //app_dep_graph_rules:test_buck2_dep_graph, after clearing project.ignore.

Current query inventory:

| Query scope | Query status | Rule types / count | Notes |
| --- | --- | --- | --- |
| kind(".*test.*", //app/...) | Pass | rust_test: 6 | Current app Rust matrix. |
| kind(".*test.*", //docs/...) | Pass | rust_test: 1 | docs/buck2_lab/greeter_lib:test. |
| kind(".*test.*", //gazebo/...) | Pass | rust_test: 1 | gazebo/strong_hash_tests:strong_hash_tests runs locally and remotely through the gazebo-rust matrix. |
| kind(".*test.*", prelude//...) | Pass | python_test: 11, erlang_test: 4, go_test: 2, java_test: 41, kotlin_test: 9 | Queryable, but most targets still need toolchain validation. |
| kind(".*test.*", //examples/with_prelude/go/...) with project.ignore= | Pass | go_test: 1 | Local-only today because RBE image lacks go. |
| kind(".*test.*", //examples/with_prelude/android/...) with project.ignore= | Pass | java_test: 1 plus test toolchain support targets | Queryable but not yet execution-validated. |
| kind(".*test.*", //examples/remote_execution/...) with project.ignore= | Pass | custom tests rule: 6 | Build-rule examples, not Buck test-runner targets. |
| kind(".*test.*", //tests/core/...) with project.ignore= | Pass | buck_e2e_test-derived python_test: 295 | Queryable after OSS buck_e2e and pytest-runner shims; //tests/core/analysis:test_cmd_args and //tests/core/build:test_build_response are execution-validated. |
| kind(".*test.*", //tests/e2e/...) with project.ignore= | Pass | buck_e2e_test/bxl/check-dependencies-derived python_test: 86 | Queryable after OSS shims; representative BXL and check_dependencies_test targets are execution-validated, but the full subtree still needs broader fixture coverage. |
| kind(".*test.*", //tests/tools/...) with project.ignore= | Pass | cram_test: 1 | tests/tools/makefile_to_depfile:integration now runs locally and remotely. |
| kind(".*", //tests/prelude/command_alias/...) with project.ignore= | Pass | 56 command_alias and py_assertion build targets | Queryable after py_assertion and selects.fmt OSS shims. |
| kind(".*test.*", //tests/prelude/...) with project.ignore= | Pass | go_test: 9 | Local-only today because RBE image lacks go. |
| kind(".*", //tests/prelude/...) with project.ignore= | Pass | 117 total targets | Queryable after py_assertion, selects.fmt, go_binary, and go_package OSS shims. |
| kind(".*test.*", //examples/with_prelude/...) with project.ignore= | Partially queryable | go/rust/android/toolchain labels; Haskell/OCaml blocked | Shared //:test_utils.bzl load is fixed; SDK-backed third-party packages still need downloaded files or exclusions. |
| //tests/select_type_params:test_select_types with project.ignore= | Pass | python_test: 1, 19 test cases | Runs locally and remotely through the select-type-params matrix. |
| //app_dep_graph_rules:test_buck2_dep_graph with project.ignore= | Pass as build | _test_buck2_dep_graph: 1 | Build-time assertion; buck2 test reports no tests. The remote matrix validates analysis under remote config; the target has no outputs. |
| Scoped kind(".*test.*", //...) source inventory | Pass | 422 labels: python_test 382, go_test 10, rust_test 9, cram_test 1, java/android tests 3, plus example/toolchain matches | Excludes generated output, standalone examples, and Haskell/OCaml SDK example trees. |

Initial local/remote matrix:

| Target | Kind | Local-only status | BuildBuddy remote-only status |
| --- | --- | --- | --- |
| app Rust matrix, six targets under app/ | app Rust unit tests | Pass with -c build.execution_platforms= --local-only --no-remote-cache | Pass with --remote-only --unstable-allow-all-tests-on-re |
| //examples/with_prelude/go/hello/greeting:greeting_test | Go prelude example | Pass with -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Fails because the RBE image lacks go |
| //docs/buck2_lab/greeter_lib:test | prelude rust_test | Pass with -c build.execution_platforms= --local-only --no-remote-cache | Pass with --remote-only --unstable-allow-all-tests-on-re |
| //gazebo/strong_hash_tests:strong_hash_tests | non-app rust_test | Pass with -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Pass with -c project.ignore= --remote-only --unstable-allow-all-tests-on-re |
| prelude//java/tools:test_list_class_names | prelude python_test | Pass with --target-platforms prelude//platforms:default -c build.execution_platforms= --local-only --no-remote-cache | Pass with --target-platforms prelude//platforms:default --remote-only --unstable-allow-all-tests-on-re |
| prelude//go/tools/gobuckify/lib:gobuckifylib_test | prelude go_test | Pass with --target-platforms prelude//platforms:default -c build.execution_platforms= --local-only --no-remote-cache | Fails because the RBE image lacks go |
| //tests/prelude/go/go_test/basic:test | tests/prelude go_test | Pass with -c project.ignore= --target-platforms prelude//platforms:default -c build.execution_platforms= --local-only --no-remote-cache | Fails because the RBE image lacks go |
| //tests/prelude/command_alias/quoting:check_quoted_args | prelude build assertion | Pass with build -c project.ignore= --target-platforms prelude//platforms:default -c build.execution_platforms= --local-only --no-remote-cache | Pass with build -c project.ignore= --target-platforms prelude//platforms:default --remote-only |
| //tests/tools/makefile_to_depfile:integration | OSS cram_test shim | Pass with -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Pass with -c project.ignore= --remote-only --unstable-allow-all-tests-on-re |
| //tests/core/analysis:test_cmd_args | OSS buck_e2e_test shim | Pass with -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Pass with -c project.ignore= --remote-only --unstable-allow-all-tests-on-re |
| //tests/core/build:test_build_response | OSS buck_e2e_test shim with tmp_path | Pass with -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Pass with -c project.ignore= --remote-only --unstable-allow-all-tests-on-re |
| //tests/select_type_params:test_select_types | OSS buck_e2e_test shim | Pass with -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Pass with -c project.ignore= --remote-only --unstable-allow-all-tests-on-re |
| //tests/e2e/check_dependencies_test:allow_list_and_block_none | OSS check_dependencies_test shim and isolated fixture graph | Pass with -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Pass with -c project.ignore= --remote-only --unstable-allow-all-tests-on-re |
| //app_dep_graph_rules:test_buck2_dep_graph | build-time dependency graph assertion | Pass with build -c project.ignore= -c build.execution_platforms= --local-only --no-remote-cache | Pass with build -c project.ignore= --remote-only; no remote actions because the target has no outputs |
| prelude//toolchains/android/test/com/facebook/buck/io/pathformat:pathformat | prelude java_test | Blocked after Kotlin bootstrap because this host only has JDK 25/26, which Kotlin rejects while parsing java.version | Blocked by RE upload timeouts during remote javac actions |

Known test surfaces that still need enablement before they can be part of the
regular Buck2 matrix:

- tests/core/** and tests/e2e/** are queryable through OSS buck_e2e shims, and
  representative analysis, build-result, and select-type targets are in the
  regular matrix. The full suites are not reliable enough for the regular matrix
  until the small pytest-compatible runner has been validated against more
  decorator, fixture, BXL, and check-dependencies patterns.
- tests/e2e/check_dependencies_test has one representative local and remote
  pass after adding an isolated OSS fixture graph. Keep the full package out of
  the regular matrix until the remaining assert/audit/mutual-dependency flavors
  have equivalent fixture coverage.
- tests/tools/makefile_to_depfile:integration is reliable enough for the regular
  matrix. It uses a small OSS cram_test shim and exercises the
  prelude//cxx/tools:dep_file_processor binary locally and remotely.
- tests/prelude/go/** is queryable, and a representative go_test passes
  locally. It is not BuildBuddy-reliable yet because remote workers do not have
  go on PATH.
- tests/prelude/command_alias/** is only partially reliable. A representative
  assertion builds locally and remotely, but the full subtree still has
  symlink-path and remote working-directory blockers.
- Prelude Java/Kotlin tests are not matrix-ready. The minimal shim toolchains
  now exist, but local execution needs a supported JDK older than the current
  host default JDK 26 or GraalVM 25, and the current remote probe timed out
  during action upload.
- Prelude Erlang tests are not matrix-ready. They still need an erlang-default
  shim and an erl runtime on the execution host.
- examples/with_prelude C++, OCaml, and Haskell packages load
  //:test_utils.bzl through a root compatibility helper. OCaml and Haskell still
  need downloaded SDK trees before their third-party packages are queryable.
- Whole-repo test queries still need either additional shim coverage or a scoped
  discovery command that avoids packages known to be unavailable in OSS.
- Android and platform-specific example tests should stay out of the first Linux
  BuildBuddy matrix unless their platform and toolchain requirements are made
  explicit.

Useful discovery commands:

~~~bash
target/debug/buck2 --isolation-dir codex-test-inventory \
  uquery 'kind(".*test.*", //app/buck2_error_tests/...)'

target/debug/buck2 --isolation-dir codex-test-inventory \
  uquery -c project.ignore= \
  'kind(".*test.*", //examples/with_prelude/go/hello/greeting/...)'

target/debug/buck2 --isolation-dir codex-test-inventory-count \
  uquery \
  -c 'project.ignore=buck-out, examples/hello_world/buck-out, examples/hello_world/output, output, examples/no_prelude, examples/persistent_worker, examples/toolchains/conan_toolchain, examples/vscode, examples/with_prelude/haskell, examples/with_prelude/third-party/haskell, examples/with_prelude/ocaml, examples/with_prelude/third-party/ocaml' \
  'kind(".*test.*", //...)'

target/debug/buck2 --isolation-dir codex-test-inventory-types \
  uquery --json --output-attribute '^buck.type$' \
  -c 'project.ignore=buck-out, examples/hello_world/buck-out, examples/hello_world/output, output, examples/no_prelude, examples/persistent_worker, examples/toolchains/conan_toolchain, examples/vscode, examples/with_prelude/haskell, examples/with_prelude/third-party/haskell, examples/with_prelude/ocaml, examples/with_prelude/third-party/ocaml' \
  'kind(".*test.*", //...)' \
  | jq -r '.[]."buck.type"' | sort | uniq -c | sort -nr
~~~

## Validation Commands

Build a current Buck2 first:

~~~bash
cargo build -p buck2 --bin buck2
~~~

Run the current reliable local and BuildBuddy matrix:

~~~bash
buildbuddy/run_buck2_test_matrix.sh --mode both --clean-output
~~~

Run only one side of the matrix:

~~~bash
buildbuddy/run_buck2_test_matrix.sh --mode local --clean-output
buildbuddy/run_buck2_test_matrix.sh --mode remote --clean-output
~~~

Run the current prelude python_test probe locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix prelude-python \
  --mode both \
  --clean-output
~~~

Run the small non-app Rust probe locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix gazebo-rust \
  --mode both \
  --clean-output
~~~

Run the current tests/prelude command_alias build assertion locally and
remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix prelude-command-alias \
  --mode both \
  --clean-output
~~~

Run the current tests/tools cram_test probe locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix tests-tools-cram \
  --mode both \
  --clean-output
~~~

Run the current core Buck e2e smoke target locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix core-e2e-smoke \
  --mode both \
  --clean-output
~~~

Run the current core build-result e2e probe locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix core-build-response \
  --mode both \
  --clean-output
~~~

Run the broader select-type Buck e2e probe locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix select-type-params \
  --mode both \
  --clean-output
~~~

Run the build-time app dependency graph assertion locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix app-dep-graph \
  --mode both \
  --clean-output
~~~

Run the check-dependencies BXL probe locally and remotely:

~~~bash
buildbuddy/run_buck2_test_matrix.sh \
  --matrix check-dependencies \
  --mode both \
  --clean-output
~~~

The core-e2e-smoke matrix can also run across the commit stack, but it is not
as cheap as tests-tools-cram. The full local and remote sweeps are known to
pass on the current stack; rerun the local sweep with:

~~~bash
buildbuddy/run_stack_test_matrix.sh \
  --from origin/main \
  --to HEAD \
  --mode local \
  --matrix core-e2e-smoke \
  --log-dir /tmp/codex-buck2-stack-logs-core-e2e-local
~~~

Rerun the full remote sweep with:

~~~bash
buildbuddy/run_stack_test_matrix.sh \
  --from origin/main \
  --to HEAD \
  --mode remote \
  --matrix core-e2e-smoke \
  --log-dir /tmp/buck2-stack-core-e2e-remote-logs
~~~

Run the current local matrix across every commit in the stack using detached
temporary worktrees:

~~~bash
buildbuddy/run_stack_test_matrix.sh --mode local --matrix app-rust
~~~

For repeated stack checks, use --log-dir to keep the full Buck output per
commit while printing only pass/fail summaries:

~~~bash
buildbuddy/run_stack_test_matrix.sh \
  --mode local \
  --matrix app-rust \
  --log-dir /tmp/buck2-stack-local-logs
~~~

The tests-tools-cram matrix is small enough to run across every commit in both
local and remote modes:

~~~bash
buildbuddy/run_stack_test_matrix.sh \
  --mode both \
  --matrix tests-tools-cram \
  --log-dir /tmp/buck2-stack-tools-cram-logs
~~~

Run the current remote matrix across the part of the stack that has the remote
Rust toolchain support:

~~~bash
buildbuddy/run_stack_test_matrix.sh \
  --from 579e6fc818 \
  --mode remote \
  --matrix app-rust \
  --log-dir /tmp/buck2-stack-remote-logs
~~~

Use a prebuilt or released Buck2 binary instead of target/debug/buck2:

~~~bash
BUCK2=/path/to/buck2 buildbuddy/run_buck2_test_matrix.sh --mode both
~~~

Run the current BuildBuddy-backed smoke test:

~~~bash
target/debug/buck2 --isolation-dir codex-test-smoke-rbe \
  test --remote-only --unstable-allow-all-tests-on-re \
  //app/buck2_error_tests:buck2_error_tests
~~~

Run the first reliable app Rust matrix on BuildBuddy RBE:

~~~bash
target/debug/buck2 --isolation-dir codex-test-app-rbe-matrix \
  test --remote-only --unstable-allow-all-tests-on-re \
  //app/buck2_action_impl_tests:buck2_action_impl_tests \
  //app/buck2_build_api_tests:buck2_build_api_tests \
  //app/buck2_core:soft_error \
  //app/buck2_error_tests:buck2_error_tests \
  //app/buck2_interpreter_for_build_tests:buck2_interpreter_for_build_tests \
  //app/buck2_node_tests:buck2_node_tests
~~~

For local-only validation, start with a narrow smoke target and make the config
explicit:

~~~bash
target/debug/buck2 --isolation-dir codex-test-smoke-local \
  test -c build.execution_platforms= --local-only --no-remote-cache \
  //app/buck2_error_tests:buck2_error_tests
~~~

Run the first reliable app Rust matrix locally:

~~~bash
target/debug/buck2 --isolation-dir codex-test-app-local-matrix \
  test -c build.execution_platforms= --local-only --no-remote-cache \
  //app/buck2_action_impl_tests:buck2_action_impl_tests \
  //app/buck2_build_api_tests:buck2_build_api_tests \
  //app/buck2_core:soft_error \
  //app/buck2_error_tests:buck2_error_tests \
  //app/buck2_interpreter_for_build_tests:buck2_interpreter_for_build_tests \
  //app/buck2_node_tests:buck2_node_tests
~~~

Run an example test locally by clearing the project ignore list:

~~~bash
target/debug/buck2 --isolation-dir codex-test-go-local \
  test -c project.ignore= -c build.execution_platforms= \
  --local-only --no-remote-cache \
  //examples/with_prelude/go/hello/greeting:greeting_test
~~~

Run a representative tests/prelude Go test locally. These targets are not
remote-reliable until Go is available on the BuildBuddy workers:

~~~bash
target/debug/buck2 --isolation-dir codex-tests-prelude-go-local \
  test -c project.ignore= --target-platforms prelude//platforms:default \
  -c build.execution_platforms= --local-only --no-remote-cache \
  //tests/prelude/go/go_test/basic:test
~~~

Run the lab rust_test target locally and remotely:

~~~bash
target/debug/buck2 --isolation-dir codex-test-doclab-local \
  test -c build.execution_platforms= --local-only --no-remote-cache \
  //docs/buck2_lab/greeter_lib:test

target/debug/buck2 --isolation-dir codex-test-doclab-rbe \
  test --remote-only --unstable-allow-all-tests-on-re \
  //docs/buck2_lab/greeter_lib:test
~~~

Run the first reliable prelude python_test locally and remotely. The explicit
target platform is required for prelude tests in this checkout:

~~~bash
target/debug/buck2 --isolation-dir codex-test-prelude-python-local \
  test --target-platforms prelude//platforms:default \
  -c build.execution_platforms= --local-only --no-remote-cache \
  prelude//java/tools:test_list_class_names

target/debug/buck2 --isolation-dir codex-test-prelude-python-rbe \
  test --target-platforms prelude//platforms:default \
  --remote-only --unstable-allow-all-tests-on-re \
  prelude//java/tools:test_list_class_names
~~~

Run the first reliable tests/prelude command_alias build assertion locally and
remotely. This is a build assertion, not a Buck test-runner target:

~~~bash
target/debug/buck2 --isolation-dir codex-command-alias-local \
  build -c project.ignore= --target-platforms prelude//platforms:default \
  -c build.execution_platforms= --local-only --no-remote-cache \
  //tests/prelude/command_alias/quoting:check_quoted_args

target/debug/buck2 --isolation-dir codex-command-alias-rbe \
  build -c project.ignore= --target-platforms prelude//platforms:default \
  --remote-only \
  //tests/prelude/command_alias/quoting:check_quoted_args
~~~

If the daemon cannot start because the OS file-watch limit is exhausted, clear
stale daemons before retrying:

~~~bash
target/debug/buck2 killall
~~~

On this machine, stale generated isolation dirs can also exhaust the watcher
budget. If killall is not enough, remove generated codex isolation dirs under
buck-out before retrying:

~~~bash
find buck-out -maxdepth 1 -mindepth 1 -type d -name 'codex-*' -exec rm -rf {} +
~~~

## Remote-Test Rules

Buck2's OSS test runner uses ExternalRunnerTestInfo. For remote execution, tests
need project-relative paths and project-root execution. The important flags are:

- --unstable-allow-compatible-tests-on-re to run only targets that already set
  the remote-compatible fields,
- --unstable-allow-all-tests-on-re to override those fields during exploratory
  RBE validation.

The first automated RBE matrix should use --remote-only plus
--unstable-allow-all-tests-on-re for smoke coverage, then tighten to
--unstable-allow-compatible-tests-on-re once the test rules are known to be
configured correctly.

## Milestones

1. Keep the lab OSS marker and docs-label fixes, then decide whether to fix or
   exclude the next whole-repo query blocker under shed/completion_verify.
2. Fix inventory blockers so the intended tests/** packages can be queried
   without missing-shim failures.
3. Decide how much generated/test-harness support may be staged into historical
   worktrees. The core-e2e-smoke matrix now validates the full stack locally and
   remotely by copying current Rust and e2e shim support into old worktrees.
   The core-build-response matrix also passes on the first historical commit
   with the copied tmp_path-capable runner.
   Broader remote Rust matrices such as app-rust and gazebo-rust still need
   either a preinstalled Rust image, an earlier squash of the remote Rust
   toolchain support, or a narrower remote range.
4. Decide whether BuildBuddy should provide Go in the default image or whether
   Go examples should stay local-only for now.
5. Expand to topic-specific Buck2 tests for the RBE/BES stack: remote
   client/cache tests, event/BES tests, and representative app tests.
6. Decide where to invoke the full-stack core-e2e-smoke matrix in the
   friendly-fork workflow and how much of the stack should run on every rebase
   versus only before publishing.
7. Add a bootstrap validation path: use the previous known-good Buck2 binary to
   build and test the current Buck2, then use the newly built binary for the
   topic matrix.
8. Only after the matrix is stable, consider replacing some release-time Cargo
   checks with Buck2 checks. Until then, Cargo remains the required release
   validation path.

## Open Questions

- What is the intended OSS mapping for @fbcode//buck2/tests:buck_e2e.bzl?
- Should tests and selected examples stay ignored by default, or should the
  BuildBuddy test matrix use a dedicated config file that clears project.ignore?
- Should the default BuildBuddy image include Go, or should Go tests use a
  remotely downloaded Go toolchain before they can enter the RBE matrix?
- Which test targets should be required per commit in this friendly-fork stack,
  and which should only run on the final stack tip?
- Should successful-action BES uploads stay disabled for the test matrix, or
  should a smaller test matrix enable them to debug remote test execution?
- Should remote Rust toolchain support be moved earlier in the stack so the
  app-rust and gazebo-rust remote matrices can start at the first topic?
- Should BXL macro tests be translated for OSS labels, or should the regular
  matrix prefer isolated Python e2e tests until the internal-path assumptions are
  removed?
