# Bazel root MODULE conformance fixture

- Commit: `d99d82e5d21c2795a6b489342886c336ec05509a`
- Upstream path: `MODULE.bazel`
- Git blob: `fc21ddefa3946a651dcb91acea0e19d3f7f3efdb`
- Size: 19,810 bytes
- SHA-256: `fe7dbb51e6164641a989ce9130071010799a4944e69615efde182323a2a6f32d`

`MODULE.bazel` is copied byte-for-byte from the named Git object. Do not
rewrite or format it. The fixture deliberately uses the source repository
root module, not a generated or host-Bazel-normalized representation.

`repo_rules.json` and `combined.json` are compact UTF-8 JSON source manifests
generated without host Bazel from that exact source. They have no trailing
newline and preserve source insertion order:

- `repo_rules.json`: 1,843 bytes, SHA-256
  `1a34874fa85e3985ab9960625c94dec3c9de5901904cf7fa709113ee7478c2c0`
- `combined.json`: 11,514 bytes, SHA-256
  `f6000910fba4aba112d9734b34d04f9511c9239835731503d27b7c715dd51fcd`

The regular-only portion of the combined manifest is 9,591 bytes with SHA-256
`5d2023c1131d64b89fe5ca04d93d7c6976a42d4693255905d872c2fff9bf0366`.
