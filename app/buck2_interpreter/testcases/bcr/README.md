# Latest BCR Starlark syntax corpus

This directory contains exact, selected source files from the latest stable
non-yanked Bazel Central Registry releases used by the Buck2 Bazel-dialect
parser tests. Tests are hermetic and verify each file's SHA-256 before parsing
it with the production policy for its actual file kind.

The corpus primarily proves that the selected current BUILD and `.bzl` source
text is accepted by Buck2's Bazel syntax and validation policy. The isolated
module evaluator additionally exercises exact registry `MODULE.bazel` sources
when its supported directive set is sufficient, without resolving loads,
repository mappings, module extensions, repository rules, or registered target
patterns. Module metadata is otherwise retained as source provenance.

Each module/version directory records the immutable BCR entry revision,
registry metadata hashes, archive URL and integrity, upstream source revision,
selected paths, and content digests. The selected files retain their upstream
license headers and the corresponding upstream license is included alongside
them.

The normal test never contacts BCR or GitHub. Updating a module is a deliberate
source refresh: query the live registry, verify the archive against
`source.json`, materialize the BCR patches, recompute every selected-file
digest, and review the resulting source diff.
