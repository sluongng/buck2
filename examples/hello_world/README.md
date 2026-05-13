## A simple Hello World project using the buck2-prelude

This example demonstrates how a simple C++ project might be built with Buck2
using the prelude.

In the `toolchains` cell, we define two toolchains needed:
`system_cxx_toolchain` and `system_python_bootstrap_toolchain`, both pulled in
from the prelude. The `BUCK` file at the project root contain a `cxx_binary`
target and its `cxx_library` dependency. `.buckconfig` contains the
configuration to set the target platform for the project:

```
[parser]
target_platform_detector_spec = target:root//...->prelude//platforms:default \
  target:prelude//...->prelude//platforms:default \
  target:toolchains//...->prelude//platforms:default
```

## Setup

Run `buck2 init --git`.

## Sample commands

To view all targets in the project,

```bash
buck2 targets //...
```

To build the main C++ binary,

```bash
buck2 build //:main
```

To run the main C++ binary,

```bash
# Should print "Hello from C++!"
buck2 run //:main
```

## BuildBuddy remote execution setup

This example is preconfigured to:

- send Build Event Service (BES) events to `grpc://localhost:1985`
- show invocation links under `http://localhost:8080/invocation/`
- send Bazel-compatible BEP payloads with `bes.event_format = bazel`
- use `grpc://localhost:1985` for CAS / Action Cache / Execution
- run remote execution on the container image `gcr.io/flame-public/rbe-ubuntu24-04:latest`
- use `/usr/bin/gcc` and `/usr/bin/g++` from that image via the `toolchains//:cxx` toolchain

With your BuildBuddy server running, build once to populate cache, then build
again and inspect invocation details in the UI:

```bash
buck2 build //:main
buck2 build //:main
```

To compare the Buck2 invocation UI against a real Bazel invocation on local
BuildBuddy master:

```bash
./run_local_buildbuddy_bazel_bes_compare.sh --keep-running --playwright
```

See `BUILDBUDDY_BAZEL_BES.md` for compatibility notes and known gaps.
