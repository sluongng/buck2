# `rules_java` 9.7.1

This fixture contains exact bytes from the latest stable, non-yanked
`rules_java` release selected from Bazel Central Registry entry revision
`aaab3d2c1b83583916930649aaa535fe95f31f38`.

## Registry and release

- Module: `rules_java`
- Version: `9.7.1`
- BCR entry revision: `aaab3d2c1b83583916930649aaa535fe95f31f38`
- Upstream source revision: `1ccc4bed30ee62ee1cf9bee4cc6204139c530d2a`
- Archive URL:
  `https://github.com/bazelbuild/rules_java/releases/download/9.7.1/rules_java-9.7.1.tar.gz`
- Archive integrity:
  `sha256-LsQqi1dgi9DwexEbXhFORkkwrcp5Cm5hvsXLMQnNfGU=`
- Archive SHA-256:
  `2ec42a8b57608bd0f07b111b5e114e464930adca790a6e61bec5cb3109cd7c65`

The annotated upstream tag `9.7.1` dereferences to the source revision above.
The archive has no strip prefix and no BCR patches. `registry/source.json` and
`registry/MODULE.bazel` are exact files from the BCR entry. `source/` contains
exact files from the integrity-verified release archive.

## Retained metadata

- `registry/source.json`:
  `59be399068e8a9a8bd90236112fb9a9d9acc08de745c49e33b4b7b861de6fa99`
- `registry/MODULE.bazel`:
  `aced1dc2cf4282cf56f1ab13b697b88dc676898b2630713d3e82a94371f7eceb`
- `source/MODULE.bazel`:
  `aced1dc2cf4282cf56f1ab13b697b88dc676898b2630713d3e82a94371f7eceb`
- `LICENSE`:
  `c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4`

`MODULE.bazel` is retained only as provenance. The corpus does not parse it as
a BUILD or `.bzl` file and does not claim bzlmod support.

## Parsed syntax fixtures

- `java/extensions.bzl`:
  `7ed7f0baab45ab5bd7925ba9c77ecd98de6e72228fd3d4ee2422f0c5a7a0aaae`
- `java/bazel/rules/bazel_java_library.bzl`:
  `97f87b1bc3c6a5faa186e26d325578e2aca274a8131755a7805976b7ee5fb2f7`
- `java/common/rules/java_runtime.bzl`:
  `12106b7039255da331be1d6a182fc314835f7c4fcf83bdf044ab5528ac562898`
- `java/common/rules/BUILD`:
  `10670193ef22f102c6274ceb517fae77e9426b14e44df3d6d76378f591bed6e4`

The selection covers a current module extension, public Java rule
declaration, toolchain rule declaration, and BUILD package. Files carrying
deprecated or legacy compatibility surfaces are deliberately excluded,
including `java/repositories.bzl`, `java/private/legacy_native.bzl`,
`java/defs.bzl`, and the legacy-bearing Java binary wrappers.

## Test boundary

The test verifies file digests and parses these four selected files with the
production Bazel dialect policy for their actual file kinds. It does not
resolve modules or `load()` statements, evaluate Bazel application globals,
instantiate Java rules or toolchains, or build Java targets.
