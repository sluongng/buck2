# rules_cc 0.2.22 C++ syntax corpus

This directory contains unmodified inputs from the latest stable `rules_cc`
module available in the Bazel Central Registry (BCR) on 2026-08-09. The live
BCR lookup selected version 0.2.22.

## Registry and release identity

- Module: `rules_cc`
- Version: `0.2.22`
- BCR entry commit: `9cbe515fad170bd5073b118415e06186e04cad18`
- Release tag target commit: `21e14308c2afc7691f43295acc9852d9a6844f04`
- Archive URL:
  `https://github.com/bazelbuild/rules_cc/releases/download/0.2.22/rules_cc-0.2.22.tar.gz`
- Archive SRI:
  `sha256-gcEKlaXCLYOCdu6Q1xJjXWBCQZ/fyl74gygia2Mh5Ts=`
- Archive SHA-256:
  `81c10a95a5c22d838276ee90d712635d6042419fdfca5ef88328226b6321e53b`
- Strip prefix: `rules_cc-0.2.22`

`registry/source.json` and `registry/MODULE.bazel` are copied from that BCR
entry. The registry applies `module_dot_bazel_version.patch` with strip level
1 and SRI
`sha256-LLadvPdwjZW6kRc67RoVp/1K/iKJ5H9rAF2fLUWMj9Q=`. The patch only changes
the root module version from the release archive's placeholder `0.0.0` to
`0.2.22`; it does not modify the selected Starlark sources. The checked-in
`registry/MODULE.bazel` is therefore the post-materialization module file.

## Selected source files

All paths under `source/` preserve the exact bytes and paths from the release
archive.

| Path | SHA-256 |
| --- | --- |
| `source/cc/cc_binary.bzl` | `8dcd829755abc3feaaf32bf3c8cda0904b8c1b5d350060e768054eb60c22f523` |
| `source/cc/private/link/cpp_link_action.bzl` | `a8eac69bb859c5aa9354e42b0a126cdfbe051c464938ba6d70a3cdeca278dbc5` |
| `source/cc/private/toolchain_config/configure_features.bzl` | `89c22b65f459cf7de09563836ae30c3d89228445a12031fa80855f487be602c5` |
| `LICENSE` | `58d1e17ffe5109a7ae296caafcadfdbe6a7d176f0bc4ab01e12a689b0499d8bd` |
| `registry/MODULE.bazel` | `94df4328edef9e44d38de5e73b037cd348e75e7ae55f4e21bf07878c41a31ebb` |
| `registry/source.json` | `b2d6d6f9c332ce269ad75b89c6f3168d809a66173c9040210fb9bcc733ab42fa` |

The public `cc_binary.bzl` entrypoint represents current C++ rule syntax
without selecting the deprecated legacy test implementation. The two private
fixtures exercise the current linker action and feature-configuration logic.
They contain legacy-named internal data and TODOs because those are still part
of the active 0.2.22 implementation, not because this corpus promises support
for deprecated public APIs.

## Test boundary

The corpus parses these `.bzl` files with Buck2's production Bazel Bzl parser
policy and checks their pinned hashes. It intentionally does not load or
evaluate the module, resolve Bzlmod dependencies, execute repository or module
extensions, expand the C++ rules, select a toolchain, or compile C++ code.
