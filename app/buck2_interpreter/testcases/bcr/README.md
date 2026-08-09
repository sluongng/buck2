# Latest BCR Starlark syntax corpus

This directory contains exact, selected source files from the latest stable
non-yanked Bazel Central Registry releases used by the Buck2 Bazel-dialect
parser tests. Tests are hermetic and verify each file's SHA-256 before parsing
it with the production policy for its actual file kind.

The corpus proves only that the selected current BUILD and `.bzl` source text
is accepted by Buck2's Bazel syntax and validation policy. It does not resolve
`MODULE.bazel`, loads, repository mappings, module extensions, repository
rules, or toolchains; evaluate Bazel application globals; or build language
targets. Module metadata is retained only as source provenance.

Each module/version directory records the immutable BCR entry revision,
registry metadata hashes, archive URL and integrity, upstream source revision,
selected paths, and content digests. The selected files retain their upstream
license headers and the corresponding upstream license is included alongside
them.

The normal test never contacts BCR or GitHub. Updating a module is a deliberate
source refresh: query the live registry, verify the archive against
`source.json`, materialize the BCR patches, recompute every selected-file
digest, and review the resulting source diff.
