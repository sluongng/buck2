# BCR and bzlmod follow-on design

Status: research and implementation design, not an implementation.

This document describes a separately steered follow-on to Bazel Starlark mode.
It does not claim that Buck2 can currently consume Bazel Central Registry (BCR)
modules. Compatibility statements are pinned to Bazel commit
[`c03d14f3c8069d909557cd33487a45c6b5d93e01`][bazel-pin], inspected from an
isolated checkout on 2026-08-09. Live BCR evidence was rechecked against BCR
`main` at [`c938005636557b613d010b2b0d9f2459c6046031`][bcr-pin] on the same date.

## Outcome

The follow-on should let a Bazel-mode Buck2 project:

1. evaluate a deliberately bounded `MODULE.bazel` language;
2. resolve `bazel_dep` edges through an ordered set of index registries using
   Bazel-compatible discovery and minimal version selection (MVS);
3. turn the selected registry `source.json` records into integrity-verified,
   traversal-safe Buck2 external cells; and
4. build a real target from those cells with Buck2's existing DICE, analysis,
   action, materialization, and local or remote execution engines.

The package-manager boundary ends at a resolved repository graph. It does not
replace Buck2's build graph or execution engine.

## Nonclaims

The first usable increment must not claim any of the following:

- arbitrary BCR module compatibility;
- general Bazel repository-rule or module-extension execution;
- compatibility with every `MODULE.bazel` directive, override, version rule,
  toolchain registration, or yanked-version option;
- source compatibility with arbitrary Bazel `BUILD` and `.bzl` files;
- safe archive extraction merely because the existing `http_archive` prelude
  can invoke `tar` or `unzip`; or
- reproducibility merely because the source archive has an integrity string.

A registry client alone is not bzlmod support. Resolution, repository mappings,
verified materialization, the restricted module language, lock behavior, and a
real build must work together before the package-manager path is usable.

## Upstream contract

### `MODULE.bazel` is its own language and environment

Bazel does not parse `MODULE.bazel` as a BUILD file. Its
[`CompiledModuleFile`][compiled-module-file] parses the file and applies a
`DotBazelFileSyntaxChecker` configured with no `.bzl` loads. The checker rejects
`if`/`for` statements, `def`/lambda, call-site `*args`, and non-literal
`**kwargs`, while allowing comprehensions, conditional expressions, and literal
dictionary `**kwargs`. It then compiles against the separate
`getModuleBazelEnv()` predeclared environment. That environment combines the
ordinary Starlark universal bindings supplied by the interpreter (`None`,
booleans, collections, `len`, `range`, `print`, and the rest of the pinned
universe) with MODULE-specific directives supplied by
[`ModuleFileGlobals`][module-file-globals]. Those directives mutate a
module-specific thread context rather than creating build targets. The initial
compatibility matrix must list the supported universal bindings separately from
the module directives; an implementation must not interpret "only directives"
as permission to remove the Starlark universe.

`CompiledModuleFile` also gives `include` syntax special treatment. Before the
identifier is rebound, it recognizes only a direct top-level
`include("literal label")` with one positional string literal. Indirect calls,
non-literal arguments, and nested uses are rejected. After an assignment to the
identifier `include`, later uses are ordinary Starlark symbol uses. The first
increment rejects the directive before evaluation, but preserves these pinned
AST rules in its validator so later `include()` support does not silently adopt
a different language.

Buck2 therefore needs a third policy in addition to Bazel-mode BUILD and `.bzl`
policies:

```text
MODULE.bazel
  parser policy
  post-parse validator
  module-directive globals
  module thread/evaluation context
  no BUILD rule globals
  no ordinary .bzl load semantics
```

The initial environment should expose only directives supported by the current
resolution increment. At minimum that is `module()` and `bazel_dep()`. Their
first-increment contract includes these Bazel invariants:

- `module()` occurs at most once and before every other module directive;
- the root module may omit `module()`, while a registry dependency may not;
- a registry dependency's declared name and version match the exact requested
  non-empty `ModuleKey`; and
- names are validated with Bazel's module-name rules and versions are parsed
  and normalized with the pinned Bazel `Version` semantics.

Any parsed but unsupported directive must fail with a stable, explicit
diagnostic. It must not be ignored. `include()`, overrides,
`register_toolchains()`, `use_extension()`, `use_repo()`, and `use_repo_rule()`
should be added only with their semantics and tests.

The evaluator returns immutable data such as `ModuleDeclaration`,
`DependencyRequest`, and `OverrideDeclaration`. It cannot register Buck targets,
declare actions, read arbitrary project files, access environment variables, or
perform network I/O. Printing follows the pinned Bazel boundary: registry-backed
dependency module files have a no-op print handler, while the root module and
non-registry-overridden dependency modules may emit diagnostic output. The
first increment has no non-registry overrides, but tests reserve this distinction
for the milestone that adds them.

### Ordered index registries

An index registry is a local directory or static HTTP tree containing registry
metadata, per-version `MODULE.bazel`, and `source.json`; it need not serve the
source archive itself. The [Bazel registry specification][registry-doc] defines
the layout and says earlier repeated `--registry` values take precedence. The
registry copy of `MODULE.bazel`, not a same-named file inside the fetched source
archive, is authoritative for registry dependency resolution.

Bazel's [`ModuleFileFunction`][module-file-function] constructs registry objects
in configured order and selects the first registry whose requested
`MODULE.bazel` exists. Only absence of that exact module file advances to the
next registry. A transport, authentication, checksum, parse, evaluation, or
declared-name/version error is terminal. After choosing a registry, a missing or
invalid `source.json` or referenced patch/overlay is also terminal and never
falls through to another registry. `bazel_registry.json` is optional; absence is
an explicit input state, while a present malformed file is terminal. Module
`metadata.json` is also optional, and the pinned Bazel behavior warns and fails
open to no yanked-version information when it cannot be read or parsed. A root
`single_version_override` may direct one module to a specific registry;
non-registry overrides are a different source kind and must not accidentally
participate in registry search.

The Buck2 follow-on must freeze a root-project Buck config and CLI override
contract for the ordered registry list before implementation. The list is part
of module-graph identity. Supplying an explicit list replaces, rather than
implicitly appends to, the default BCR. An explicit BCR URL is required to put
it back. URL canonicalization must preserve path and distinguish registry
identities while removing fragments and rejecting embedded credentials.

For each lookup the lock records both successful registry files and
not-found results from higher-priority registries. This is required because a
new publication to a higher-priority registry can otherwise silently change a
previously resolved graph. Bazel records the same distinction in
`registryFileHashes` ([lockfile documentation][lockfile-doc]).

### Discovery and MVS

Resolution is a deterministic pipeline:

1. evaluate the root module and root-only overrides;
2. breadth-first discover exact `(module name, requested version)` nodes by
   fetching and evaluating their registry `MODULE.bazel` files;
3. apply root overrides at every edge before further discovery;
4. group discovered nodes by module name, or by the appropriate bucket for a
   supported multiple-version override;
5. choose the maximum Bazel version in each group;
6. rewrite edges to selected nodes and prune nodes unreachable from the root;
7. verify direct-dependency, yanked-version, and compatibility policies that
   the implemented surface promises; and
8. retrieve `source.json` only for selected registry-backed nodes.

The split between discovery and selection is source-backed by
[`BazelModuleResolutionFunction`][module-resolution], while
[`Selection`][selection] documents and implements the maximum-version choice
and reachability pruning. "Minimal" in MVS describes the algorithm's upgrade
policy, not choosing the numerically lowest requested version.

Version parsing, normalization, and ordering follow Bazel's relaxed
[`Version` implementation][version-source], not generic semver. The normative
contract includes arbitrary counts of release segments, alphanumeric release
identifiers, numeric-before-nonnumeric identifier ordering, prereleases sorting
below the corresponding release, and discarded build metadata. Consequently,
`1.2+one` and `1.2+two` have the same normalized identity and registry path.
`Version.EMPTY` is a sentinel used by the root and non-registry overrides and
sorts above every real version; it is never a registry version. Conformance
vectors must cover these cases before graph keys or lock serialization are
frozen. The first increment should reject multiple-version overrides and
nodep/dev-dependency features until their selection behavior is implemented; it
must not flatten them into normal deps.

### Apparent and canonical repository names

Module source and users address repositories by apparent names. Resolution must
translate those names to stable, unique canonical repository identities before
labels enter Buck2 analysis.

For each selected module, keep:

- a `ModuleKey` of the validated module name and normalized selected `Version`,
  with explicit root and non-registry `Version.EMPTY` cases;
- an internal immutable repository ID independent of a user's apparent alias;
- the module's self apparent name (`repo_name`, or module name by default);
- apparent direct-dependency names from each module's `bazel_dep(repo_name=)`;
  and
- a per-repository mapping from apparent name to internal repository ID.

Bazel's [`ModuleKey`][module-key] stores the validated name and `Version` and
derives a canonical repository name. Apparent self and dependency aliases live
on `Module`; [`BazelDepGraphFunction`][dep-graph] and the module mapping code
combine them after selection. Buck2 does not need to expose Bazel's current
plus-separated canonical spelling as a stable public API, because Bazel itself
does not promise that spelling. It does need equivalent identity, visibility,
and owner-relative mapping behavior, including the root repository's empty
apparent alias. Diagnostic and lock output should include the module key and a
deterministic printable canonical name.

Resolved module repositories become cells. `@foo//pkg:target` in Bazel mode is
first interpreted through the owning repository's mapping, then converted to
the corresponding Buck2 cell and target label. A raw Buck2 cell alias must not
bypass that mapping in Bazel mode.

Bazel injects builtin `bazel_tools` as an implicit `Version.EMPTY`
non-registry dependency backed by the Bazel binary's `embedded_tools` content.
Separately, its canonical-name policy reserves unsuffixed names for both
`bazel_tools` and `platforms`; `platforms` is not injected by that mechanism.
Buck2 must explicitly choose and document either a compatible versioned module
or a bounded Buck2 shim with a checked API matrix. A normal versioned module is
not graph-identical to Bazel's builtin, and a hidden mapping from `@bazel_tools`
to Buck2's prelude must not be presented as exact Bazel resolution.

### Registry `source.json` becomes a typed RepoSpec

The registry file describes how to materialize a selected module. Bazel's
[`IndexRegistry`][index-registry] parses `source.json` into typed archive,
`git_repository`, or `local_path` RepoSpecs. Archive RepoSpecs include the
ordered download URLs, SRI integrity, strip prefix, patches, overlays, and the
authoritative registry module-file URL and integrity.

The first Buck2 implementation should accept only the archive source kind,
including Bazel's rule that an absent `type` defaults to `archive`. It must
reject `git_repository` and `local_path` with source-kind diagnostics until
their trust, identity, and portability semantics are designed. Archive support
must include:

- one required source URL and its required SRI value;
- Bazel's download candidate order: configured module mirrors, registry-level
  mirrors, the primary source URL, then `source.json` `mirror_urls` backups;
- a segment-aware optional strip prefix;
- integrity-verified registry overlays and patches in specified order;
- `patch_strip` (default zero) and optional `archive_type` with their pinned
  Bazel meanings; and
- the bytes, URL, and SRI of the registry `MODULE.bazel` that was actually
  evaluated, not only its digest.

The materialized repository must contain that authoritative registry module
file. Match Bazel's pinned ordering: extract the archive, apply registry
overlays, apply registry patches in declared order, delete any packaged root
`MODULE.bazel`, and install the integrity-verified registry `MODULE.bazel`.
Future root override patches may run only after that restoration. This ordering
allows registry patches to apply to a packaged module file while ensuring the
final unoverridden repository agrees with the file used for dependency
resolution.

`source.json` is untrusted data. Parse with duplicate-field detection, bounded
sizes, and unknown-field reporting appropriate to the compatibility promise.
Do not pass arbitrary JSON fields through to command lines.

SRI proves that downloaded bytes match `source.json`; it does not independently
authenticate `source.json`. During the first unlocked update, trust comes from
the explicitly configured registry origin, TLS, and its credential policy. A
compromised registry can publish both a malicious archive and its matching SRI.
Subsequent locked operation additionally authenticates registry metadata by its
recorded digest. Document this trust-on-first-update boundary and leave room for
an organization-supplied signed lock or registry allowlist; do not describe SRI
alone as registry provenance.

## Proposed Buck2 architecture

### Data flow

```text
root buckconfig + root MODULE.bazel + lock policy
        |
        v
restricted module evaluator
        |
        v
ordered registry lookup -> discovery -> MVS -> repo mappings
        |
        v
selected typed RepoSpecs + lock state
        |
        v
verified download -> safe extraction/patching -> directory digest
        |
        v
registry-backed ExternalCellOrigin -> combined CellResolver
        |
        v
existing Buck2 BUILD/.bzl evaluation -> analysis -> actions -> execution
```

The resolution layer should live above Starlark parsing but below target
loading. A cohesive crate can own module AST validation, module records,
version ordering, discovery, MVS, mappings, and lock serialization. Registry
transport and verified repository materialization should be separate from the
pure resolver so the algorithm can be unit-tested without network access.

### DICE keys and invalidation

Use DICE for dependency tracking, not as an opaque memo table around a registry
client. Candidate keys are:

- `RootModuleKey`: content digest, Bazel compatibility pin, and module language
  policy;
- `RegistryFileKey`: canonical registry URL, relative path, lock mode, expected
  metadata hash, refresh generation, and non-secret registry
  credential-provider and authorization/trust-domain identities;
- `EvaluatedModuleKey`: module key plus exact registry module-file digest;
- `ResolvedModuleGraphKey`: root declaration, ordered registries, overrides,
  resolution policy, and selected yanked-version policy;
- `RepoSpecKey`: selected module, owning registry, exact `source.json` digest,
  `bazel_registry.json` digest-or-not-found sentinel, and configured
  module-mirror policy; and
- `RegistryExternalCellKey`: typed RepoSpec, all patch/overlay digests,
  authoritative registry module-file digest, extraction policy version,
  target-platform-independent archive identity, and non-secret source
  credential-provider/trust-domain identity.

Values are immutable and equality compares semantic content. Errors distinguish
persistent input errors from transient transport errors. Credentials themselves
are never serialized in keys, events, or logs; changing credential-provider
configuration invalidates the transport input without publishing secrets.
Registry metadata and source archives may use different credential providers,
so both provider identities participate independently. A mirror-list,
registry-level metadata, source credential, trust-domain, or cache-sharing
policy change must invalidate the RepoSpec or external cell even when the
archive digest is unchanged.

Buck2 currently injects `CellResolver` into DICE and computes external-cell
alias resolvers inside DICE. The module graph must therefore be resolved before
the final transaction cell resolver is assembled, or the resolver must gain an
explicit immutable overlay of resolved module cells. The former is the simpler
first route and avoids a cycle: evaluating registry `MODULE.bazel` consumes raw
registry bytes and does not need target loading from the cells it is creating.

Once assembled, the combined resolver participates in normal DICE identity.
Changing `MODULE.bazel`, registry order, a selected version, source or registry
metadata, mirror policy, credential/trust-domain identity, synthesized cell
policy, or lock policy invalidates the affected module cells and downstream
packages; unrelated configured targets retain normal DICE reuse.

### Synthesized registry-cell configuration

A BCR archive is Bazel source, not a Buck2 cell definition. It normally has
`BUILD` or `BUILD.bazel` files and no `.buckconfig`; Buck2 currently discovers
`BUCK.v2` and `BUCK` by default and parses each external cell's own config.
Registry support must not require or trust an archive-provided `.buckconfig` to
select its language, buildfile names, prelude, aliases, or cell topology.

The resolved graph therefore supplies an immutable synthesized configuration
for every registry cell. It selects Bazel Starlark through the root project's
explicit mode, discovers `BUILD.bazel` and `BUILD` in a documented order, uses
the bounded hidden Buck2 backend chosen by the root, and derives aliases only
from the resolved repository mapping. Archive `.buckconfig`,
`.buckconfig.local`, and `.buckconfig.d` contents are ordinary source data and
are not evaluated for registry cells. CLI/root policy overrides that
intentionally affect all module cells must be represented in the graph/config
identity rather than copied into the archive.

This synthesized view needs a dedicated DICE value consumed by buildfile
discovery, legacy-config lookups, Starlark setup, and the cell-alias resolver.
It cannot be a file silently written into the extracted tree. Tests use an
archive with no Buck config and prove that a malicious archive config cannot
change the dialect, prelude, buildfile names, registry list, or mappings.

### External cells and materialization

Current Buck2 external cells have only `Bundled` and `Git` origins in
`app/buck2_core/src/cells/external.rs`. Their implementation already demonstrates
the desired seam: a DICE-keyed file-ops delegate resolves external-cell paths,
registers directory artifacts with the materializer, and presents them through
the ordinary cell file APIs (`app/buck2_external_cells`).

Add a typed registry archive origin rather than pretending an archive is a Git
cell. Its identity should contain a module/repository identifier, verified
source digest, authoritative module-file digest, patch/overlay digests,
strip-prefix policy, extraction-policy version, and a non-secret source
credential-provider/trust-domain identity. The trust domain scopes the output
namespace and cache policy; matching content digests alone do not merge private
origins. The origin resolves under Buck2's external-cell output namespace and
provides the same `FileOpsDelegate` and explicit expansion behavior as existing
origins.

The existing `actions.download_file` path already validates hexadecimal SHA-1
or SHA-256 while streaming and registers HTTP downloads with the materializer.
However, it is an analysis action, while module resolution must finish before
target analysis. Add a typed SRI parser matching pinned Bazel
[`Checksum.fromSubresourceIntegrity`][checksum-source]: base64 SHA-1, SHA-256,
SHA-384, SHA-512, and BLAKE3 with exact decoded lengths. Reuse the HTTP client,
streaming verification, event model, CAS digest construction, and materializer
primitives, extending streaming validators for algorithms the existing parser
does not support, without fabricating a target action. If an early increment
supports only SHA-256, that is an explicit compatibility-matrix divergence and
all other valid algorithms fail as unsupported rather than malformed.

Verified anonymous public bytes may be shared by digest across modules and
workspaces. Authenticated inputs and their extracted directory trees require a
credential/trust-domain-scoped local namespace and must not be uploaded to or
read from an unscoped shared/remote CAS. Cross-workspace or remote reuse is
allowed only when an explicit cache policy enforces the same authorization
domain. Knowledge of a digest is not authorization to private content.

The existing prelude `http_archive` invokes external archive tools as an action.
That does not establish the security boundary required for registry sources.
Use a bounded Rust extractor, preferably a small separately fuzzed crate, and
register only the fully verified directory tree with the materializer.

### Traversal-safe extraction and patching

Extraction occurs into a fresh private temporary directory. Before writing any
entry, normalize and validate its archive path and link target. Reject:

- absolute, UNC, drive-prefixed, NUL-containing, or parent-traversing paths;
- a path that escapes after applying a segment-aware strip prefix;
- symlink or hardlink targets that resolve outside the extraction root;
- device nodes, FIFOs, sockets, and unsupported sparse/special entries;
- a non-directory entry used as a parent, duplicate incompatible entries, and
  case-fold collisions on case-insensitive hosts;
- writes through an earlier archive symlink;
- unreasonable member counts, individual sizes, total expanded size, path
  length, or compression ratio; and
- patch or overlay paths that fail the same destination checks.

Download SRI is verified before extraction. Each overlay, patch, and registry
module file is verified before application. Apply overlays first, then registry
patches in declared order, and finally replace the root `MODULE.bazel` with the
verified registry copy. Patching must use a bounded parser or a sandboxed
hermetic tool and reject absolute/traversing paths. The final tree is walked
without following links, fingerprinted into Buck2's directory digest
representation, declared to the materializer, and atomically published under
its content- and trust-domain-derived external-cell path. Failures remove the
private temporary directory and never expose a partial cell.

Fuzz archive path normalization, tar and zip entry handling, strip-prefix
logic, symlink chains, patch paths, and malformed SRI. Keep malicious archive
regression fixtures for every rejected class.

## Lockfile and offline behavior

Define a Buck2-owned, versioned lock schema rather than copying Bazel's JSON
shape without its semantics. The durable lock should record:

- schema and extraction-policy versions plus the pinned Bazel compatibility
  commit;
- root `MODULE.bazel` and included-module-file digests, when includes become
  supported;
- ordered canonical registry identities;
- every remote registry file URL mapped to its SHA-256 or to an explicit
  not-found sentinel needed for precedence;
- selected module keys, dependency edges, overrides, canonical repository IDs,
  and per-repository apparent-name mappings;
- selected-yanked-version decisions;
- the exact typed RepoSpec and `source.json` digest for every selected module;
- archive SRI, verified byte size and digest, patch/overlay digests, and final
  materialized directory digest; and
- eventually, module-extension transitive code/usage/input digests and generated
  RepoSpecs.

Bazel's current lock records the remote resolution inputs and extension results,
not an authoritative serialized solved module graph. Its hashes address
registry bytes in the repository cache; hashes are not substitutes for those
bytes. Buck2 should likewise persist every successful locked registry response
in a verified local/CAS registry-byte store. Anonymous public bytes are keyed by
SHA-256. Authenticated bytes are keyed by the non-secret registry
authorization/trust-domain identity plus SHA-256, distinct from source and
mirror trust domains; matching URLs, providers, or digests do not authorize
cross-domain reuse. Negative entries require no blob. The lock records the
expected digest and the byte store provides the content from which Buck2 reruns
the pure resolver. Population is atomic with successful lock update, reads
reverify size and digest, retention keeps blobs referenced by live locks, and
garbage collection cannot remove a blob during a transaction. Selected nodes
and mappings may be stored as deterministic audit data, but reads must recompute
and compare them rather than trusting them in place of MVS. This keeps the
input-hash reproducibility model source-compatible while making graph drift
inspectable.

Bazel deliberately ignores lock hashes for `file:` registries and omits their
absolute URLs from the durable lock because it treats local registries as
user-controlled, nonportable inputs. Buck2 intentionally chooses a stricter
contract: `file:` registries are allowed only under an explicitly configured
workspace-relative registry root, their canonical identities contain no host
absolute path, and their files are copied into the same verified byte store.
Machine-absolute local registries are update-only, cannot produce a portable
lock, and are rejected in `error` mode. This is a documented compatibility
divergence rather than a claim about Bazel's lock behavior.

Support `update`, `refresh`, and `error` modes. `update` may fetch missing
inputs and atomically rewrite a canonical lock after successful resolution; it
may reuse hashes and not-found records already in a matching lock. `refresh`
increments a transaction-injected refresh generation and revalidates mutable
registry files and negative lookups. Unlocked network reads and transient
transport errors are also scoped to the command generation so one daemon cannot
retain them indefinitely. `error` uses no refresh generation because every
input must come from the matching lock.

`error` requires a complete matching lock, does no registry network lookup, and
loads both registry files and archives only from verified local download/CAS
state. A missing registry-byte blob, missing archive blob, missing
registry-file hash, source mismatch, or stale resolution input is a hard error
that names the digest and the update command. Never silently fall back to online
resolution in error mode. A fresh machine succeeds only after the lock's
referenced registry and archive blobs have been imported into its authorized
local state.

Stable serialization, deterministic ordering, atomic replacement, versioned
migrations, and concurrent-writer tests are entry requirements. Lock data is
untrusted on read and receives the same bounds and path validation as network
metadata.

## Authentication

Registry metadata and archive URLs can require different credentials. Use an
origin-scoped credential-provider interface shared with Buck2's HTTP client.
The initial provider can support explicitly configured credential helpers;
`.netrc` support may follow if its profile, permission, and precedence rules are
specified. Bazel's credential tests show credential helpers overriding netrc
for registry HTTP requests ([source][credential-tests]).

Security requirements:

- never allow userinfo in configured registry URLs;
- require HTTPS for remote registries and sources by default; permit plaintext
  HTTP only through an explicit insecure policy (or the loopback test fixture),
  never send credentials over it, and reject HTTPS-to-HTTP redirects;
- never store authorization headers, tokens, passwords, helper stdout, or
  environment secret values in the lock, CAS key, DICE serialization, events,
  errors, or build reports;
- scope returned headers to the exact origin and redirect policy;
- do not forward registry credentials to source-archive or mirror origins;
- resolve source and mirror credentials independently and include their
  non-secret provider and authorization-domain identities in DICE and local
  cache namespaces;
- apply the configured repository-download network policy to registry-provided
  source and mirror URLs, and reject unsupported schemes or remote-registry
  attempts to select local `file:` sources;
- bound redirect count, reapply scheme/origin policy at every hop, and enforce
  the same private-network/DNS-rebinding egress policy before each connection;
- redact URL queries and sensitive headers in diagnostics;
- bound helper execution time/output and validate its JSON protocol; and
- include only a non-secret provider/configuration identity in invalidation.

Hermetic tests must cover unauthenticated 401, helper success, plaintext and
downgrade refusal, cross-origin redirect/header isolation, private-network
policy, private-cache isolation, and absence of the token from logs, events,
CAS metadata, and lock output.

## Module extensions and repository rules

Module extensions are not syntax sugar for registry lookup. Bazel loads the
extension `.bzl`, aggregates typed tags from all module usages, evaluates it
with a `module_ctx`, records repository-rule calls, computes extension-specific
repository mappings, and materializes generated RepoSpecs. The official
[extension documentation][extension-doc] and
[`SingleExtensionEvalFunction`][extension-eval] show this separate evaluation
phase. Lock entries include extension code and usage digests, environmental
inputs, evaluation factors, and generated RepoSpecs.

Accordingly:

- `use_extension`, extension proxies/tags, `use_repo`, `use_repo_rule`, and
  repository-rule globals remain explicit unsupported diagnostics in the
  registry-only milestone;
- a module depending on an extension-generated repository is not usable merely
  because its archive was fetched;
- arbitrary repository-rule execution must have declared file, environment,
  network, process, and platform inputs before it can be cached hermetically;
  and
- generated repositories need their own canonical identities and repo mappings,
  including visibility between repositories from the same extension.

Extension support should first define a typed repository-operation capability
API (verified download, safe extraction, generated files, patches, and bounded
process execution). Implement a small allowlisted set of repository rules on
that API before exposing a general Starlark `repository_ctx`. DICE can then key
extension evaluation by transitive `.bzl` digest, all tag usages, platform and
environment factors, declared reads, tool inputs, and generated RepoSpecs.

Until that work lands, documentation and diagnostics must say "registry module
resolution and archive materialization", not "BCR ecosystem compatibility".

## Staged follow-on commit roadmap

Each prefix must compile and keep default Buck2 behavior unchanged.

1. **Pure module model and restricted evaluator.** Add `MODULE.bazel` file kind,
   parser/validator, pinned universal bindings, `module()` and `bazel_dep()`
   globals, root/dependency declaration invariants, Bazel version normalization
   and ordering, immutable model records, and rejection tests for BUILD globals,
   loads, control flow, actions, and unsupported directives. Preserve the pinned
   `include` AST classification while rejecting the directive. Test one-call
   and ordering rules, root omission, dependency name/version mismatch,
   universal bindings, and root/registry/non-registry print handlers. Keep
   registry mode unavailable in production at this prefix.
2. **Index registry transport and lock inputs.** Freeze the ordered registry and
   lock-mode config contract. Add bounded HTTP/file index access, typed metadata,
   MODULE-only first-containing-registry behavior, credential-provider
   interface, verified registry-byte storage, file-registry portability rules,
   registry file hashes/not-found records, and hermetic server tests. Still do
   not expose module cells.
3. **Discovery, MVS, and repository mappings.** Implement the pure graph
   pipeline, root overrides included only where fully supported, canonical IDs,
   per-repository mappings, yanked policy, deterministic diagnostics, and Bazel
   source-derived conformance vectors.
4. **Verified archive RepoSpecs.** Parse archive-only `source.json`, implement
   SRI and mirror ordering, reuse Buck2 HTTP/CAS primitives, build the safe Rust
   extractor and patch/overlay pipeline, restore the authoritative registry
   module file in Bazel order, scope authenticated bytes by trust domain, and
   fuzz path/security boundaries.
5. **Registry-backed external cells.** Add the external origin, DICE-keyed
   file-ops delegate, content- and trust-domain-derived output paths,
   graph-synthesized Bazel cell configuration, combined cell resolver, label
   mapping, explicit expansion, and materializer events. Atomically enable the
   production feature only when the hermetic real build passes without an
   archive-provided Buck config.
6. **Lockfile completion and offline/error mode.** Add canonical atomic writes,
   strict reads, registry-byte and archive CAS/offline replay, missing-blob and
   stale-input diagnostics, concurrent update tests, and secret-scanning tests.
7. **Compatibility breadth.** Add directives and source kinds one semantic unit
   at a time. `register_toolchains`, overrides, `include()`, `git_repository`,
   and `local_path` require dedicated behavior and tests.
8. **Module extensions and repository rules.** Separately design and implement
   tag aggregation, extension identity, typed repository operations, generated
   RepoSpecs, mappings, lazy evaluation, and lock integration. This is a major
   milestone, not a registry-client patch.

## Entry criteria for implementation

Do not begin the follow-on product stack until the following are agreed:

- the Bazel Starlark mode has stable file-kind routing, globals, load behavior,
  config identity, and a real daemon/action verifier;
- the supported initial `MODULE.bazel` directive matrix and diagnostics are
  written down, including universal bindings and root/dependency declaration
  invariants;
- the root registry-list, lock-mode, override, credential-helper, and offline
  config contracts are fixed;
- relaxed-version and MVS conformance vectors are extracted from the pinned
  Bazel source/tests;
- internal canonical repository IDs and the Bazel-label-to-cell mapping are
  specified independently of display spelling;
- the archive extractor threat model, resource limits, symlink policy, and
  fuzzer corpus are reviewed;
- the lock schema has a versioning and atomic-update strategy;
- the registry-byte store, source trust-domain identity, and synthesized
  registry-cell configuration are specified;
- every unsupported source kind and module directive has an explicit failure;
  and
- the hermetic fixture below is reviewed as the primary acceptance test.

## Primary hermetic verifier

Create a test-owned static HTTP registry and source server bound to loopback.
Do not depend on BCR or GitHub for the acceptance test. The fixture should
contain two ordered registries and small deterministic archives:

- root requests `aaa@1.0` and `chooser@1.0`;
- `aaa@1.0` requests `shared@1.0` and exposes an apparent alias;
- `chooser@1.0` requests `shared@1.1`;
- MVS selects `shared@1.1`, rewrites both edges, and produces distinct
  per-repository mappings;
- the first registry misses one selected module and the second supplies it;
- source archives contain Bazel `BUILD.bazel`/`.bzl` files, no `.buckconfig`,
  and a target that consumes a source from the transitive module; one negative
  archive contains hostile files in every Buck config location that must be
  ignored; and
- the built action writes exact bytes such as `bcr-ok\n`.

The verifier runs a clean Buck2 binary through a real daemon and proves:

1. registry request order and not-found lock entries, plus terminal malformed
   higher-priority `MODULE.bazel` and missing/invalid selected `source.json`;
2. exact discovery graph, MVS selection, canonical IDs, and apparent mappings;
3. SRI-verified archive, overlay, and patch handling followed by restoration of
   the authoritative registry `MODULE.bazel`;
4. materialization as Buck2 external cells with synthesized Bazel buildfile
   discovery, ignored archive Buck config, and recorded directory digests;
5. a real analysis/action build across a transitive module boundary, plus
   `what-ran` evidence and exact output bytes;
6. second build cache reuse without another archive transfer;
7. `error`-mode success with the HTTP server stopped and all registry/archive
   bytes available in local verified state, plus a missing-registry-blob hard
   failure with no network fallback;
8. source, registry metadata, or lock tampering fails before target evaluation;
9. a traversal archive, escaping symlink, and traversal patch all fail without
   creating any path outside the private extraction root or publishing a cell;
10. helper authentication works, and identical registry/source digests at the
    same URL and provider under different principals remain in distinct trust
    domains; secrets do not appear in logs or lock data;
11. changing root module content, registry order, mirror policy, or credential
    provider in the same daemon invalidates the correct graph/cells without
    restarting it; and
12. workspace-relative `file:` registry locks replay portably while
    machine-absolute registries fail in `error` mode.

Model, registry-client, resolver, or extractor unit tests support this verifier
but cannot replace the real target build.

## Public `bazel_skylib@1.9.2` smoke

This public smoke detects drift in real BCR metadata and archive handling. It is
not the hermetic acceptance test and is not evidence that arbitrary
`bazel_skylib` BUILD targets are compatible with Buck2.

Evidence reverified on 2026-08-09:

- live BCR `main` was
  `c938005636557b613d010b2b0d9f2459c6046031`;
- [`metadata.json`][skylib-metadata] had SHA-256
  `f63ad3e30bb3768e37ce5d580b88c37a622ae751f5dae2f2a416d74e5f72e1c9`,
  listed `1.9.2`, and did not mark it yanked;
- the registry [`MODULE.bazel`][skylib-module] declares module name
  `bazel_skylib`, version `1.9.2`, direct dependencies on `platforms@0.0.10` and
  `rules_license@1.0.0`, and toolchain registrations; its SHA-256 was
  `8c51259b0f4481475586dbfede7591e57b75702e637f840f6138eec80c34b270`;
- [`source.json`][skylib-source] points to the 1.9.2 GitHub release archive with
  empty `strip_prefix` and SRI
  `sha256-N837xvrv6pT3s3dgowXJjAiYERbCvJ6CHjtCMiH62Mg=`; its SHA-256 was
  `41cbde7546542dee2f26e3f10bf1c4ac57909943194b7ac48cc5238a01893aa8`;
- downloading that URL produced exactly 44,716 bytes;
- its SHA-256 was
  `37cdfbc6faefea94f7b37760a305c98c08981116c2bc9e821e3b423221fad8c8`;
  and
- base64-encoding those digest bytes produced exactly the SRI payload above.

The source/materialization public smoke first resolves live BCR `main` and
requires it to equal the golden commit above. It then fetches the three files
from the live BCR endpoint, not only immutable commit-pinned URLs, and requires
their SHA-256 values to equal the three goldens. Either mismatch is a drift
failure and never regenerates goldens automatically. After those checks it
constructs the typed RepoSpec for the explicit `bazel_skylib@1.9.2` module key,
fetches the archive, verifies byte count and SRI, safely extracts it, restores
the verified registry `MODULE.bazel`, and verifies selected files such as
`MODULE.bazel` and `lib/paths.bzl` are in the materialized cell. The immutable
links below remain review evidence, not the live-main drift probe. This narrow
probe does not count as module-graph resolution.

The first public smoke may stop at verified cell materialization because this
module's registry `MODULE.bazel` uses `register_toolchains()` and its source
uses a broader Bazel rule surface. A later compatibility milestone may build a
real `bazel_skylib` target, and may call the same path through full module-graph
resolution, only after those APIs and its transitive module directives are
implemented. The hermetic fixture remains the proof of a real package-manager
build.

## Source index

Primary Bazel sources were read from an isolated checkout at the pinned commit.
The important evidence links are permanent GitHub blobs rather than moving
docs.

[bazel-pin]: https://github.com/bazelbuild/bazel/commit/c03d14f3c8069d909557cd33487a45c6b5d93e01
[bcr-pin]: https://github.com/bazelbuild/bazel-central-registry/commit/c938005636557b613d010b2b0d9f2459c6046031
[compiled-module-file]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/CompiledModuleFile.java
[module-file-globals]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/ModuleFileGlobals.java
[registry-doc]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/site/en/external/registry.md
[module-file-function]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/ModuleFileFunction.java
[lockfile-doc]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/site/en/external/lockfile.md
[module-resolution]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/BazelModuleResolutionFunction.java
[selection]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/Selection.java
[version-source]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/Version.java
[module-key]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/ModuleKey.java
[dep-graph]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/BazelDepGraphFunction.java
[index-registry]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/IndexRegistry.java
[checksum-source]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/repository/downloader/Checksum.java
[credential-tests]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/test/py/bazel/bzlmod/bzlmod_credentials_test.py
[extension-doc]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/site/en/external/extension.md
[extension-eval]: https://github.com/bazelbuild/bazel/blob/c03d14f3c8069d909557cd33487a45c6b5d93e01/src/main/java/com/google/devtools/build/lib/bazel/bzlmod/SingleExtensionEvalFunction.java
[skylib-metadata]: https://github.com/bazelbuild/bazel-central-registry/blob/c938005636557b613d010b2b0d9f2459c6046031/modules/bazel_skylib/metadata.json
[skylib-module]: https://github.com/bazelbuild/bazel-central-registry/blob/c938005636557b613d010b2b0d9f2459c6046031/modules/bazel_skylib/1.9.2/MODULE.bazel
[skylib-source]: https://github.com/bazelbuild/bazel-central-registry/blob/c938005636557b613d010b2b0d9f2459c6046031/modules/bazel_skylib/1.9.2/source.json
