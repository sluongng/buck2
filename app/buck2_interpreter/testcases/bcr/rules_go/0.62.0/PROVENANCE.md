# rules_go 0.62.0 provenance

This corpus snapshot was resolved on 2026-08-09. `0.62.0` was the latest
stable, non-yanked `rules_go` version listed by the Bazel Central Registry
(BCR).

## Registry entry

- Module: `rules_go`
- Version: `0.62.0`
- BCR entry commit: `a847444fa849906907e568de53a5bb6cb069523e`
- Registry `source.json` SHA-256:
  `36d781c558eb7d3bd49dc5e190455714f174232372f15207ab200b4348bda3e6`
- Registry `MODULE.bazel` SHA-256:
  `8ee616065c3d2b2f7ac0880108316ce8d0c332b3a30aad24e95c0bc124ec853e`

The files under `registry/` are the exact bytes from that immutable BCR
entry. They also matched the live registry when this snapshot was created.

## Release source

- Upstream source commit:
  `c6b35c2367d164f27af4825422d5c70c3365f6fb`
- Archive URL:
  `https://github.com/bazel-contrib/rules_go/releases/download/v0.62.0/rules_go-v0.62.0.zip`
- Archive integrity:
  `sha256-C4BclPs3MNwj3zKSXtR3s/TtN7VgddysbyGMPqe0q0I=`
- Archive SHA-256:
  `0b805c94fb3730dc23df32925ed477b3f4ed37b56075dcac6f218c3ea7b4ab42`
- Archive `strip_prefix`: empty

The BCR entry applies `module_dot_bazel_version.patch` with strip level 1;
its integrity is
`sha256-XNMdK2KLBp81gs8OSObZKitqpe3CTxs/eti7L1PGOy8=`. The patch only stamps
the release version in `MODULE.bazel`. It does not modify any selected syntax
fixture. Files under `source/` are therefore the exact selected bytes after
BCR materialization as well as the exact release archive bytes.

## Selected syntax fixtures

| Release archive path | File kind | SHA-256 |
| --- | --- | --- |
| `examples/basic_gazelle/BUILD.bazel` | BUILD | `e49d0eb00edea0571a72079765a1203d9032b4cdcf3b11c9719967eaeb727169` |
| `go/private/providers.bzl` | `.bzl` | `8a0e1d37080ed5ed75c6b7fde33f56812302d9f2fc0afd8188d6411f4fcdb416` |
| `go/private/rules/library.bzl` | `.bzl` | `5ce524ff9cdea71a0a463b92295c4f124e629535406295d9e9a92f6aa1e47a46` |
| `go/private/rules/transition.bzl` | `.bzl` | `e4f5a4b44419bd8c9e5a508bb489df651649f11669ba14d0acf9ca52b525809a` |
| `go/private/extensions.bzl` | `.bzl` | `10becf03bb5646057f6572359dbce4c72d647a7e9331cffe590d39a59050f11c` |

The upstream Apache-2.0 `LICENSE.txt` is stored unmodified with SHA-256
`cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30`.

## Test boundary

These fixtures test only that the selected current BUILD and `.bzl` source
text parses and validates under Buck2's production Bazel file policies. The
test does not resolve Bzlmod dependencies or `load()` labels, evaluate Bazel
application globals, run module extensions or repository rules, configure
toolchains, analyze targets, or compile Go code.
