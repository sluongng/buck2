# Bazel Starlark compatibility

Status: implemented bounded compatibility surface at Buck2 commit
`ccad6aeccdbf420190d3af9ab9219165432bb680`; runtime enablement landed in
`eb773980c4cf87f6a9828d9f0b107ccfa1f20222`.

This document defines Buck2's Bazel Starlark mode. Compatibility claims are
pinned to Bazel source commit
[`c03d14f3c8069d909557cd33487a45c6b5d93e01`][bazel-pin]. This is not a claim
that arbitrary Bazel projects or Bazel Central Registry modules build with
Buck2.

## Select the dialect

Set the dialect in the root project's `.buckconfig`:

```ini
[buck2]
starlark_dialect = bazel
```

The accepted values are `buck2` and `bazel`. An absent key means `buck2`, so
existing projects keep Buck2 parsing, globals, loading, linting, debugging, and
evaluation. An unknown value is a configuration error.

Buck2 resolves this value once from the root project's assembled configuration
and applies it to every cell. Normal configuration layering still applies, so a
command-line override wins:

```console
buck2 build -c buck2.starlark_dialect=bazel //...
```

Changing the value changes interpreter and DICE identity. A daemon can
therefore run Buck2 mode, then Bazel mode, then Buck2 mode without reusing an
AST or globals from the wrong dialect.

The selector affects the configured Buck2 buildfile (for this surface,
`BUILD.bazel`) and `.bzl` files. BXL and PACKAGE files remain Buck2 languages,
and JSON and TOML keep their existing standard parsers.

## Select `BUILD.bazel`

Bazel mode does not change Buck2's buildfile discovery. Select `BUILD.bazel`
with the existing buildfile configuration:

```ini
[buildfile]
name_v2 = BUILD.bazel
```

Unlike Bazel, this increment does not automatically prefer `BUILD.bazel` over
`BUILD`. The upstream Bazel behavior is documented in
[`build-files.mdx`][bazel-build-files], but it is intentionally outside this
compatibility surface. If a project configures another buildfile name, Bazel
policy still follows the buildfile type; it is not inferred from the filename.

## Syntax matrix

The table describes Bazel mode. “Yes” means the syntax is accepted in that
file kind; it does not imply that every name or API used by the expression is
available.

| Syntax rule | `BUILD.bazel` | `.bzl` |
| --- | --- | --- |
| `def` | No | Yes |
| `lambda` | No | Yes |
| Keyword-only parameters | Yes\* | Yes |
| Positional-only parameter syntax | N/A | No |
| Type annotations and declarations | No | No |
| Buck OSS f-strings | No | No |
| Top-level `if` statements | No | No |
| Top-level `for` statements | No | No |
| `load()` statements | Yes | Yes |
| Call-site `*args` | No | Yes |
| Call-site `**kwargs` | No | Yes |
| Loads after another statement | Yes | No |
| Loading a private `_symbol` | No | No |
| Duplicate names within one `load` | No | No |
| Rebinding a top-level name | Yes | No |
| Automatically re-exporting a load binding | N/A | No |
| Comprehensions | Yes | Yes |
| Conditional expressions | Yes | Yes |

The BUILD parser understands keyword-only parameter syntax, but the subsequent
BUILD validator rejects every `def` and lambda, so no complete BUILD function
definition survives. `def f(*, x): ...` is accepted in `.bzl`; Python's `/`
positional-only parameter syntax is not. Top-level string literals do not end
the `.bzl` load block. Privacy is determined by the original imported name,
not its local alias, and a duplicated name within one `load` is rejected even
in BUILD. Conditional expressions and comprehensions remain available despite
the statement-form control-flow ban. A `.bzl` load binding is file-local, but
code may explicitly assign its value to a public global and export that global.

Bazel's source configures BUILD files with interleaved loads, global load
bindings, and top-level rebinding in
[`PackageFunction`][bazel-build-options]. The restricted BUILD pass rejects
functions, statement-form control flow, and variadic call arguments in
[`DotBazelFileSyntaxChecker`][bazel-build-syntax]. Ordinary `.bzl` files use
the stricter defaults in [`FileOptions`][bazel-file-options]: loads first,
file-local load bindings, no private loads, and no top-level rebinding. The
resolver ignores string-literal statements when enforcing loads first and
reports duplicate load bindings in [`Resolver`][bazel-resolver]. See the
separate [`checkLoadAfterStatement` implementation][bazel-load-order] for the
string-literal exception.

The `buck2` value and an absent value preserve existing Buck2 syntax,
including Buck types and OSS f-strings. They do not adopt any restriction from
the Bazel column.

## Build API matrix

Bazel mode exposes a deliberately small build API. It does not expose Buck2's
prelude, Buck rule functions, `attrs`, `rule`, `select`, or other Buck-only
globals under Bazel names.

| File kind | Direct global | `native` | Supported call |
| --- | --- | --- | --- |
| `BUILD.bazel` | `genrule` | Absent | `genrule(...)` |
| `.bzl` | Absent | Present | `native.genrule(...)` |

The two rows are the complete mode-specific build API. Exact sorted snapshot
tests pin every visible global. BUILD has:

```text
False, None, True, abs, all, any, bool, bytes, chr, dict, dir, enumerate,
fail, float, genrule, getattr, hasattr, hash, int, len, list, max, min, ord,
range, repr, reversed, sorted, str, tuple, type, zip
```

`.bzl` has the same surface except `native` replaces `genrule`:

```text
False, None, True, abs, all, any, bool, bytes, chr, dict, dir, enumerate,
fail, float, getattr, hasattr, hash, int, len, list, max, min, native, ord,
range, repr, reversed, sorted, str, tuple, type, zip
```

This is the exact bounded starlark-rust surface, not Bazel's full global
universe. Pinned Bazel builds [`Starlark.UNIVERSE`][bazel-universe] from its
[`MethodLibrary`][bazel-method-library]. That library includes same-named
`dir`, `enumerate`, `float`, `hash`, `reversed`, and `type`, but Buck2 exposes
starlark-rust implementations and does not claim exact semantic parity for
them. `bytes`, `chr`, and `ord` are starlark-rust additions not present in the
pinned Bazel universe. Conversely, pinned Bazel includes `print`, a
semantics-gated `set`, and application globals such as `struct`; they are
absent here unless a future compatibility increment adds them explicitly.
Bazel's broader application environment is visible in
[`StarlarkGlobalsImpl`][bazel-application-globals].

Buck globals such as `attrs`, `rule`, `select`, `read_config`, and `plugins`
are also absent. No additional Bazel build API is implied by this document.

### `genrule` attributes

Only the following keyword attributes are supported:

| Attribute | Required | Accepted value | Mapping |
| --- | --- | --- | --- |
| `name` | Yes | String | Buck2 target name. |
| `outs` | Yes | One-string list or tuple | Maps to the singular output. |
| `cmd` | Yes | String | Command for the existing Buck2 action backend. |
| `srcs` | No | List or tuple of source labels | Inputs; defaults to empty. |

Positional arguments, unknown attributes, an empty or multi-entry `outs`, and
values of the wrong type fail explicitly. Bazel attributes such as `tools`,
`env`, `exec_properties`, `output_to_bindir`, `tags`, `visibility`, and
`testonly` are not silently ignored; they are unsupported in this increment.

The supported make-variable translation is intentionally bounded:

| User spelling | Buck2 backend spelling | Status |
| --- | --- | --- |
| `$@` | `${OUT}` | Supported. |
| `$(SRCS)` | `${SRCS}` | Supplied sources, or empty. |
| `$$` | `$` | One literal dollar for the shell. |
| Any other `$x` or `$(NAME)` | None | Explicitly unsupported. |

Unterminated `$(...)`, a dangling `$`, and unsupported dollar expressions also
fail before the backend callable is invoked.

### Trusted backend boundary

The adapter reuses Buck2's existing rule analysis and action implementation; it
does not evaluate user files with Buck2 globals. A configured Buck2 prelude in
a dedicated cell or subdirectory is parsed and frozen under the Buck dialect.
Its `native.genrule` callable is retained as a hidden backend value and invoked
by the Bazel adapter. It is not loadable or addressable from Bazel user code.

In Bazel mode:

- no Buck prelude symbols are implicitly imported into user files;
- project package values and additional Buck globals do not leak into the
  Bazel environment;
- BUILD has no `native` object;
- `.bzl` has no direct `genrule`; and
- explicit attempts to load the hidden backend fail.

The configured prelude must export a `native` struct containing `genrule`. A
root-level prelude fails closed because its trusted directory would otherwise
contain every project file. The complete bundled Buck2 prelude, including its
platform and toolchain assumptions, is not part of the verified Bazel surface.

This boundary lets Bazel-shaped calls reuse Buck2's DICE graph, action graph,
materializer, local execution, and remote execution without treating Buck2
Starlark as the Bazel dialect.

## Loading boundary

The verified fixture is deliberately narrow:

```text
BUILD.bazel
  -> macros.bzl
       -> defs.bzl
            -> native.genrule(...)
```

The checked load spelling is the same-package colon-relative form:

```python
load(":macros.bzl", "bazel_genrule")
load(":defs.bzl", "declare_genrule")
```

Those loads work transitively. Loaded bindings are file-local, so a second
`.bzl` file cannot automatically re-export one. An explicit load such as
`load("@prelude//:prelude.bzl", ...)` fails because the configured Buck2
prelude is hidden. General Bazel label syntax, external repository mappings,
canonical repository names, load visibility, and repository-aware
`@repo//pkg:file.bzl` compatibility are deferred. Other spellings accepted by
Buck2's existing loader are not compatibility claims.

## Diagnostics

Negative real-binary fixtures and adapter unit tests lock these diagnostic
classes and stable message fragments:

- **Configuration:** invalid selector values contain
  `` Invalid value for buckconfig `[buck2] starlark_dialect` `` and
  `` Expected one of `buck2` or `bazel` ``. A root-level trusted prelude
  contains
  `dedicated cell or subdirectory`. Backend-shape failures contain
  `Bazel genrule backend is unavailable because the configured Buck2 prelude
  does not export native.genrule`,
  `` `native` is missing from the configured Buck2 prelude ``,
  `` `native` in `prelude.bzl` must be a struct `` or
  `` `native.genrule` is missing ``.
- **Parse and validation:** BUILD rejects with `` `def` is not allowed ``,
  `` `lambda` is not allowed ``, `` `if` cannot be used outside ``,
  `` `for` cannot be used outside ``, `` `*args` call arguments are not
  allowed ``, or `` `**kwargs` call arguments are not allowed ``. `.bzl`
  diagnostics contain `type annotation`, `f-string`,
  `load statements must appear before any other statement`,
  `is private and cannot be imported`, `redeclared at top level`, or
  `load statement defines 'x' more than once`.
- **Name resolution:** stable fragments include `` Variable `native` not
  found ``, `` Variable `genrule` not found ``,
  `` Variable `root_marker` not found ``,
  `` Variable `implicit_package_symbol` not found ``, and
  `` undefined variable `root_marker` ``.
- **Evaluation and attributes:** an unlisted `native` member contains
  `namespace` and `` has no attribute `cc_library` ``. An unknown adapter
  keyword contains `` Found `unknown` extra named parameter ``; a positional
  call contains `Missing named-only parameter`; and wrong types contain
  `` Type of parameter `name` doesn't match ``,
  `` Type of parameter `cmd` doesn't match ``,
  `` Type of parameter `srcs` doesn't match ``, or
  `` Type of parameter `outs` doesn't match ``.
- **Adapter:** zero or multiple outputs contain `exactly one output`.
  Unsupported and malformed commands contain `unsupported Bazel genrule make
  variable`, `unterminated Bazel genrule make variable`, `unsupported Bazel
  genrule dollar expression`, or `dangling $ in Bazel genrule command`.
- **Loading:** an explicit backend load contains `cannot load Buck2 prelude`.
  Attempted automatic load re-export contains `not exported`.

Unsupported operations are errors, not warnings or no-ops.

## Tooling behavior

`buck2 starlark lint` reads the same root-project dialect and uses the same
file-kind parser, validator, and globals as build evaluation. A
file that fails Bazel-mode validation during a build must not pass because lint
silently used the Buck dialect.

Lint does not invoke the production load resolver. Build or query evaluation,
not lint, is authoritative for diagnostics such as explicit hidden-prelude
loads and unavailable loaded symbols.

`buck2 starlark typecheck` also selects the file-kind-specific Bazel globals
and rejects Bazel-disabled type syntax.

The Debug Adapter Protocol (DAP) supports Bazel-aware source parsing and debug
attachment. The attach transaction's mode is only provisional; an active
debugged command or evaluation selects its own mode. Breakpoint source mapping
uses a deliberately permissive union parser, while production evaluation keeps
the exact file-kind policy. Simultaneously active commands with conflicting
dialects fail explicitly instead of guessing.

LSP Bazel-mode global documentation and completion are unsupported. The LSP
still exposes Buck2's global documentation, so it must not be used as evidence
that a Bazel-only or Buck-only global is available in Bazel evaluation.

## Intentional divergences and deferred features

This mode is narrower than Bazel at the pinned commit:

- Bazel has [experimental `.bzl` Starlark type syntax][bazel-types], which can
  be restricted with a path allowlist. Buck2 Bazel mode disables type syntax
  unconditionally in this increment.
- Only the adapter described above is implemented. Bazel native rules,
  `select()`, providers, custom rules, aspects, transitions, platforms,
  toolchains, repository rules, symbolic macros, and semantics flags are
  deferred.
- Automatic `BUILD.bazel` discovery and precedence are deferred.
- Bazel labels and repository mappings beyond the required local load chain are
  deferred.
- Full bundled-prelude platform and toolchain compatibility is deferred; the
  verified path uses a configured dedicated-cell or subdirectory backend.
- BXL and PACKAGE remain Buck2-specific rather than adopting Bazel syntax or
  globals.
- The real-action fixture invokes `sh` and is not verified on Windows.

An API not listed as supported is unsupported even if starlark-rust can parse
the expression containing it.

## Migration example

Within a Buck2 project whose configured dedicated-cell or subdirectory prelude
exports the required hidden `native.genrule` backend, select both the dialect
and buildfile explicitly:

```ini
# .buckconfig
[buck2]
starlark_dialect = bazel

[buildfile]
name_v2 = BUILD.bazel
```

Define the BUILD target through a loaded macro:

```python
# BUILD.bazel
load(":macros.bzl", "bazel_genrule")

bazel_genrule(
    name = "transitive",
    outs = ["bazel.txt"],
    srcs = ["input.txt"],
    cmd = "cat $(SRCS) > $@",
)

genrule(
    name = "direct",
    outs = ["direct.txt"],
    cmd = "printf 'direct-ok\\n' > $@",
)
```

```python
# macros.bzl
load(":defs.bzl", "declare_genrule")

def bazel_genrule(name, outs, cmd, srcs = None):
    declare_genrule(
        name = name,
        outs = outs,
        cmd = cmd,
        srcs = srcs,
    )
```

```python
# defs.bzl
def declare_genrule(name, outs, cmd, srcs = None):
    if srcs == None:
        native.genrule(
            name = name,
            outs = outs,
            cmd = cmd,
        )
    else:
        native.genrule(
            name = name,
            outs = outs,
            cmd = cmd,
            srcs = srcs,
        )
```

With `input.txt` containing `bazel-ok`, the transitive action output is exactly
`bazel-ok\n`; the direct action output is exactly `direct-ok\n`. When migrating
a target, replace Buck-only types and f-strings, move BUILD functions and
statement-form control flow into `.bzl`, call the bounded rule directly in
BUILD and through `native` in `.bzl`, and remove attributes not listed above.

Use a command-line override to compare modes without editing project config.
The second command assumes a separately existing Buck-only target in the
configured buildfile under `//legacy`:

```console
buck2 build -c buck2.starlark_dialect=bazel //:transitive
buck2 build -c buck2.starlark_dialect=buck2 //legacy:typed_target
```

## Verification evidence

Runtime enablement is commit
`eb773980c4cf87f6a9828d9f0b107ccfa1f20222`; exact surface snapshots and type
diagnostics are pinned by follow-up
`ccad6aeccdbf420190d3af9ab9219165432bb680`.

The primary real-daemon verifier invoked:

```text
/home/nb/work/facebook/buck2-bazel-starlark/target/debug/buck2
sha256 27151c510e36d0172db80347f5e6de4840a0c644d67cd8a43a5f8561b5f87a01
```

That binary's 2026-08-09 15:05 Europe/Amsterdam modification time preceded
the 15:37 invocation below, and the e2e command used that absolute path.

At final HEAD `ccad6aeccdbf420190d3af9ab9219165432bb680`, a fresh
`cargo build --locked -p buck2 --bin buck2` passed in 117.84 seconds and
produced:

```text
/tmp/buck2-bazel-starlark-final-cargo.FXQf1mIkvF/debug/buck2
sha256 36a7b725a911bd57682eb164ceb10af850fd03caaf93a23cf905976a1c5a6f4c
```

- BuildBuddy invocation `73340226-d4b1-4716-b3e5-b7272eda05d2` passed all
  seven Bazel-mode e2e tests and recorded 6,442 uncached local actions. The
  real build asserted exact `bazel-ok\n` and `direct-ok\n` bytes, and
  `what-ran` contained the transitive target identity and its `cat` command.
- The same e2e test ran Buck2 -> Bazel -> Buck2 against unchanged source and
  asserted one daemon PID and UUID throughout.
- BuildBuddy invocation `c8b3b85f-a437-41e5-a886-b9d04dfd0788` passed all
  four configuration and DICE regression tests.
- Independent architecture, adapter, and test re-reviews reported no remaining
  findings.

## BCR and bzlmod are a separate follow-on

Bazel Central Registry consumption is not enabled by this Starlark selector.
`MODULE.bazel` requires a separate restricted language and environment plus
registry ordering, discovery and minimal version selection, repository
mappings, integrity-verified materialization, a lockfile/auth contract, and
module-extension/repository-rule boundaries. Those requirements belong to the
separate BCR and bzlmod follow-on design, not this mode.

Until that work is implemented and independently verified, this document does
not claim that arbitrary BCR modules or the Bazel ecosystem are usable as a
Buck2 package manager.

[bazel-pin]: https://github.com/bazelbuild/bazel/tree/c03d14f3c8069d909557cd33487a45c6b5d93e01
[bazel-build-files]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/docs/concepts/build-files.mdx#L10-L42
[bazel-build-options]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/skyframe/PackageFunction.java#L1349-L1359
[bazel-build-syntax]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/packages/DotBazelFileSyntaxChecker.java#L33-L45
[bazel-file-options]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/net/starlark/java/syntax/FileOptions.java#L42-L112
[bazel-resolver]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/net/starlark/java/syntax/Resolver.java#L649-L684
[bazel-load-order]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/net/starlark/java/syntax/Resolver.java#L1336-L1359
[bazel-types]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/skyframe/BzlCompileFunction.java#L246-L286
[bazel-universe]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/net/starlark/java/eval/Starlark.java#L100-L140
[bazel-method-library]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/net/starlark/java/eval/MethodLibrary.java#L38-L1010
[bazel-application-globals]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/analysis/starlark/StarlarkGlobalsImpl.java#L49-L107
