# rules_cc 0.2.22 C syntax fixtures

These fixtures come from the latest stable, non-yanked Bazel Central Registry
release of `rules_cc` at refresh time. They were materialized from the BCR
entry at commit `9cbe515fad170bd5073b118415e06186e04cad18`.

## Source pin

- Module: `rules_cc`
- Version: `0.2.22`
- Upstream source commit: `21e14308c2afc7691f43295acc9852d9a6844f04`
- Archive URL:
  `https://github.com/bazelbuild/rules_cc/releases/download/0.2.22/rules_cc-0.2.22.tar.gz`
- Archive integrity:
  `sha256-gcEKlaXCLYOCdu6Q1xJjXWBCQZ/fyl74gygia2Mh5Ts=`
- Archive SHA-256:
  `81c10a95a5c22d838276ee90d712635d6042419fdfca5ef88328226b6321e53b`
- Strip prefix: `rules_cc-0.2.22`

The BCR entry applies `module_dot_bazel_version.patch` with strip level 1.
That patch changes only the root module version from `0.0.0` to `0.2.22`.
Its integrity is
`sha256-LLadvPdwjZW6kRc67RoVp/1K/iKJ5H9rAF2fLUWMj9Q=` and its SHA-256 is
`2cb69dbcf7708d95ba91173aed1a15a7fd4afe2289e47f6b005d9f2d458c8fd4`.
The checked-in `registry/MODULE.bazel` is the exact post-patch module file.

Registry metadata hashes:

- `registry/source.json`:
  `b2d6d6f9c332ce269ad75b89c6f3168d809a66173c9040210fb9bcc733ab42fa`
- `registry/MODULE.bazel`:
  `94df4328edef9e44d38de5e73b037cd348e75e7ae55f4e21bf07878c41a31ebb`

Selected source hashes:

- `source/cc/private/compile/compile_build_variables.bzl`:
  `ce81d304a5a3a37334fb4e28b897f84037f4d451e988541a377bb93b90f5d2f8`
- `source/cc/action_names.bzl`:
  `5be05896333f8806c824e97d2b9c6bd4a5e0b03c3cb4f96988f1263359d0a57d`
- `LICENSE`:
  `58d1e17ffe5109a7ae296caafcadfdbe6a7d176f0bc4ab01e12a689b0499d8bd`

## Test boundary

The syntax corpus verifies exact source hashes and parses these current
C-facing `.bzl` files with Buck2's production Bazel `.bzl` policy. The isolated
module evaluator also digest-pins the exact registry `MODULE.bazel`, evaluates
its prefix through the source registration records, and confirms that full
evaluation stops at the currently unsupported `archive_override` directive.
Neither test resolves BCR modules or `load()` labels, loads extensions, expands
registered target patterns, configures C toolchains, or builds C targets.
