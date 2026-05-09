# BuildBuddy Bazel-Compatible BES Notes

This example can send Buck2 invocations to BuildBuddy using standard Bazel BEP
payloads instead of Buck2 `BuckEvent` payloads:

```ini
[bes]
backend = grpc://localhost:1985
results_url = http://localhost:8080/invocation/
event_format = bazel
```

Run the local comparison script from this directory:

```bash
./run_local_buildbuddy_bazel_bes_compare.sh --keep-running --playwright
```

The script starts or reuses BuildBuddy `master`, runs the Buck2 hello-world
build with `bes.event_format=bazel`, creates a small Bazel workspace, runs a
real Bazel build with the same BES backend, and prints both invocation URLs.
With `--playwright`, browser snapshots and overview screenshots are written to
`output/playwright/bes-compare/`.

## Buck2 v1 Gaps

- Buck2 emits a best-effort Bazel event graph. It closes target-to-action
  `children` edges at the end of the stream so BuildBuddy can hydrate target
  detail pages, but it does not try to model every Bazel lifecycle edge.
- Buck output artifacts and Bazel named sets are not fully modeled yet, so
  target artifact cards may be weaker than a native Bazel invocation.
- Buck structured errors do not have a lossless Bazel `FailureDetail` mapping.
  The v1 converter renders these through progress stderr and action stderr.
- Command-line rendering is simplified into `command` and `arguments`
  sections. It does not yet reconstruct Bazel-style option sections.
- Action events use the Buck action key or first tiny digest as the BEP primary
  output when Buck does not expose a concrete output path.
- Buck cell labels are normalized for Bazel UI compatibility: `root//pkg:target`
  becomes `//pkg:target`, and other cells become external-repo-shaped labels
  such as `@prelude//pkg:target`.

## BuildBuddy Follow-Ups

- BuildBuddy master expects `BuildEvent.bazel_event` for the normal Bazel UI.
  BuckEvent ingestion should remain optional server functionality; Buck2 can
  interoperate without it through this mode.
- BuildBuddy currently labels the Buck2 overview as `bazel vbuck2 build`
  because it assumes the `BuildStarted.build_tool_version` belongs to Bazel.
  A generic tool display would make BEP producers clearer.
- BuildBuddy showed `Unknown CPU`, `Unknown mode`, and `Remote execution off`
  for a Buck2 RBE build. Buck2 does not yet emit the Bazel option/platform and
  remote execution metrics BuildBuddy uses for those badges.
- The Buck2 target count can render as `0 targets` on no-op builds or as the
  emitted Buck target set on builds with analysis events, and the tooltip
  contained `undefined configured`. This suggests BuildBuddy expects Bazel's
  configured target counters or graph closure fields even when target events
  are present.
- The Buck2 invocation showed no artifact cards for this v1 mapping. This is
  expected until Buck2 emits enough named set / output file data, but the UI
  could make "action id without output artifact" states clearer.
- Invocation creation is sensitive to early lifecycle events. BuildBuddy should
  continue to handle a minimal `BuildStarted` plus structured command line for
  non-Bazel tools.
- The UI assumes stable Bazel labels and configuration ids across configured,
  completed, action, and test events. Buck2-compatible rendering benefits from
  keeping those IDs visible in debug views when events do not line up.
- Target detail action cards depend on `TargetComplete.children` pointing to
  matching `ActionCompleted` ids. This is correct for Bazel, but non-Bazel BEP
  producers may need to emit a late `TargetComplete` update after action events
  are known.
- BuildBuddy could expose a clearer "unknown output path" action presentation
  for non-Bazel tools that have action IDs but not Bazel output artifacts.
- The command-line panel is Bazel-centric. A small generic mode for
  `StructuredCommandLine` sections would make Buck2 and other BEP producers
  easier to inspect.
