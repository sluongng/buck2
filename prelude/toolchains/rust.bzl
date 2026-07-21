# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//rust:rust_toolchain.bzl", "PanicRuntime", "RustToolchainInfo")

_RUST_STATIC_DIST = "https://static.rust-lang.org/dist"

_DEFAULT_TRIPLE = select({
    "prelude//os:linux": select({
        "prelude//cpu:arm64": "aarch64-unknown-linux-gnu",
        "prelude//cpu:riscv64": "riscv64gc-unknown-linux-gnu",
        "prelude//cpu:x86_64": "x86_64-unknown-linux-gnu",
    }),
    "prelude//os:macos": select({
        "prelude//cpu:arm64": "aarch64-apple-darwin",
        "prelude//cpu:x86_64": "x86_64-apple-darwin",
    }),
    "prelude//os:windows": select({
        "prelude//cpu:arm64": select({
            # Rustup's default ABI for the host on Windows is MSVC, not GNU.
            # When you do `rustup install stable` that's the one you get. It
            # makes you opt in to GNU by `rustup install stable-gnu`.
            "DEFAULT": "aarch64-pc-windows-msvc",
            "prelude//abi:gnu": "aarch64-pc-windows-gnu",
            "prelude//abi:msvc": "aarch64-pc-windows-msvc",
        }),
        "prelude//cpu:x86_64": select({
            "DEFAULT": "x86_64-pc-windows-msvc",
            "prelude//abi:gnu": "x86_64-pc-windows-gnu",
            "prelude//abi:msvc": "x86_64-pc-windows-msvc",
        }),
    }),
})

def _strip_quotes(value):
    value = value.strip()
    if (value.startswith("\"") and value.endswith("\"")) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value

def _parse_rust_toolchain(contents):
    channel = None
    components = []

    for raw_line in contents.splitlines():
        line = raw_line.split("#")[0].strip()
        if not line:
            continue

        # Support the non-TOML rust-toolchain format:
        #
        #   nightly-2026-02-28
        if "=" not in line and not line.startswith("["):
            channel = _strip_quotes(line)
            continue

        if line.startswith("channel"):
            channel = _strip_quotes(line.split("=", 1)[1])
        elif line.startswith("components"):
            raw_components = line.split("=", 1)[1].strip()
            if not raw_components.startswith("[") or not raw_components.endswith("]"):
                fail("only single-line rust-toolchain component lists are supported")
            for raw_component in raw_components[1:-1].split(","):
                component = _strip_quotes(raw_component.strip())
                if component:
                    components.append(component)

    if channel == None:
        fail("rust-toolchain does not define a channel")

    return struct(channel = channel, components = components)

def _channel_manifest_url(base_url, channel):
    # Rustup channels like `nightly-2026-02-28` use the dated dist path but
    # still fetch `channel-rust-nightly.toml`.
    if channel.startswith("nightly-"):
        date = channel[len("nightly-"):]
        if len(date) == 10:
            return "{}/{}/channel-rust-nightly.toml".format(base_url, date)
    return "{}/channel-rust-{}.toml".format(base_url, channel)

def _component_package(component):
    return {
        "clippy": "clippy-preview",
        "rustfmt": "rustfmt-preview",
    }.get(component, component)

def _dedupe(values):
    seen = {}
    deduped = []
    for value in values:
        if value not in seen:
            seen[value] = True
            deduped.append(value)
    return deduped

def _manifest_package(manifest, package, triple):
    headers = [
        "[pkg.{}.target.{}]".format(package, triple),
        "[pkg.{}.target.\"*\"]".format(package),
    ]

    for header in headers:
        in_section = False
        available = None
        url = None
        sha256 = None

        for raw_line in manifest.splitlines():
            line = raw_line.strip()
            if line == header:
                in_section = True
                continue
            if in_section and line.startswith("["):
                break
            if not in_section or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = _strip_quotes(value.strip())
            if key == "available":
                available = value == "true"
            elif key == "xz_url":
                url = value
            elif key == "xz_hash":
                sha256 = value

        if in_section:
            if available != True:
                fail("Rust package `{}` is not available for `{}`".format(package, triple))
            if url == None or sha256 == None:
                fail("Rust package `{}` for `{}` is missing xz_url/xz_hash".format(package, triple))
            return struct(url = url, sha256 = sha256)

    fail("Rust package `{}` is not listed for `{}`".format(package, triple))

def _sanitize_output_name(value):
    return value.replace("*", "all").replace("-", "_").replace(".", "_")

_COMMON_ATTRS = {
    "allow_lints": attrs.list(attrs.string(), default = []),
    "clippy_toml": attrs.option(attrs.dep(providers = [DefaultInfo]), default = None),
    "default_edition": attrs.option(attrs.string(), default = None),
    "deny_lints": attrs.list(attrs.string(), default = []),
    "doctests": attrs.bool(default = False),
    "nightly_features": attrs.bool(default = False),
    "report_unused_deps": attrs.bool(default = False),
    "rustc_binary_flags": attrs.list(attrs.arg(), default = []),
    "rustc_flags": attrs.list(attrs.arg(), default = []),
    "rustc_target_triple": attrs.string(default = _DEFAULT_TRIPLE),
    "rustc_test_flags": attrs.list(attrs.arg(), default = []),
    "rustdoc_flags": attrs.list(attrs.arg(), default = []),
    "warn_lints": attrs.list(attrs.string(), default = []),
}

def _rust_toolchain_info(ctx, compiler: RunInfo, clippy_driver: RunInfo, rustdoc: RunInfo, sysroot_path: Artifact | None = None) -> RustToolchainInfo:
    return RustToolchainInfo(
        allow_lints = ctx.attrs.allow_lints,
        clippy_driver = clippy_driver,
        clippy_toml = ctx.attrs.clippy_toml[DefaultInfo].default_outputs[0] if ctx.attrs.clippy_toml else None,
        compiler = compiler,
        default_edition = ctx.attrs.default_edition,
        panic_runtime = PanicRuntime("unwind"),
        deny_lints = ctx.attrs.deny_lints,
        doctests = ctx.attrs.doctests,
        nightly_features = ctx.attrs.nightly_features,
        report_unused_deps = ctx.attrs.report_unused_deps,
        rustc_binary_flags = ctx.attrs.rustc_binary_flags,
        rustc_flags = ctx.attrs.rustc_flags,
        rustc_target_triple = ctx.attrs.rustc_target_triple,
        rustc_test_flags = ctx.attrs.rustc_test_flags,
        rustdoc = rustdoc,
        rustdoc_flags = ctx.attrs.rustdoc_flags,
        sysroot_path = sysroot_path,
        warn_lints = ctx.attrs.warn_lints,
    )

def _system_rust_toolchain_impl(ctx):
    return [
        DefaultInfo(),
        _rust_toolchain_info(
            ctx,
            compiler = RunInfo(args = ctx.attrs.compiler),
            clippy_driver = RunInfo(args = ctx.attrs.clippy_driver),
            rustdoc = RunInfo(args = ctx.attrs.rustdoc),
        ),
    ]

system_rust_toolchain = rule(
    impl = _system_rust_toolchain_impl,
    attrs = _COMMON_ATTRS | {
        "clippy_driver": attrs.list(attrs.string(), default = ["clippy-driver"]),
        "compiler": attrs.list(attrs.string(), default = ["rustc"]),
        "rustdoc": attrs.list(attrs.string(), default = ["rustdoc"]),
    },
    is_toolchain_rule = True,
)

def _rust_toolchain_impl(ctx):
    return [
        DefaultInfo(),
        _rust_toolchain_info(
            ctx,
            compiler = ctx.attrs.compiler[RunInfo],
            clippy_driver = ctx.attrs.clippy_driver[RunInfo],
            rustdoc = ctx.attrs.rustdoc[RunInfo],
            sysroot_path = ctx.attrs.sysroot,
        ),
    ]

rust_toolchain = rule(
    impl = _rust_toolchain_impl,
    attrs = _COMMON_ATTRS | {
        "clippy_driver": attrs.exec_dep(providers = [RunInfo]),
        "compiler": attrs.exec_dep(providers = [RunInfo]),
        "rustdoc": attrs.exec_dep(providers = [RunInfo]),
        "sysroot": attrs.source(allow_directory = True),
    },
    is_toolchain_rule = True,
)

def _downloaded_rust_toolchain_impl(ctx):
    rust_toolchain_file = ctx.attrs.rust_toolchain
    base_url = ctx.attrs.base_url.rstrip("/")
    base_components = ctx.attrs.base_components
    extra_components = ctx.attrs.extra_components
    rustc_host_triple = ctx.attrs.rustc_host_triple

    manifest = ctx.actions.declare_output("rust-channel.toml", has_content_based_path = False)
    toolchain = ctx.actions.declare_output("rust-toolchain", dir = True, has_content_based_path = False)

    def download_manifest(ctx, artifacts, outputs):
        parsed = _parse_rust_toolchain(artifacts[rust_toolchain_file].read_string())
        download = ctx.actions.write(
            "download_rust_manifest.sh",
            [
                "#!/bin/sh",
                "set -eu",
                "curl -fsSL \"$1\" -o \"$2\"",
            ],
            is_executable = True,
            has_content_based_path = False,
        )
        ctx.actions.run(
            cmd_args([
                "/bin/sh",
                download,
                _channel_manifest_url(base_url, parsed.channel),
                outputs[manifest].as_output(),
            ]),
            category = "download_rust_manifest",
            prefer_remote = True,
        )

    ctx.actions.dynamic_output(
        dynamic = [rust_toolchain_file],
        inputs = [],
        outputs = [manifest.as_output()],
        f = download_manifest,
    )

    def install_toolchain(ctx, artifacts, outputs):
        parsed = _parse_rust_toolchain(artifacts[rust_toolchain_file].read_string())
        manifest_contents = artifacts[manifest].read_string()
        components = _dedupe(base_components + parsed.components + extra_components)
        archives = []

        for component in components:
            package = _component_package(component)
            package_info = _manifest_package(manifest_contents, package, rustc_host_triple)
            archives.append(package_info.url)
            archives.append(package_info.sha256)

        installer = ctx.actions.write(
            "install_rust_toolchain.sh",
            [
                "#!/bin/sh",
                "set -eu",
                "out=\"$1\"",
                "shift",
                "rm -rf \"$out\"",
                "work=\"${out}.install\"",
                "rm -rf \"$work\"",
                "mkdir -p \"$work\" \"$out\"",
                "i=0",
                "while [ \"$#\" -gt 0 ]; do",
                "  url=\"$1\"",
                "  sha256=\"$2\"",
                "  shift 2",
                "  i=$((i + 1))",
                "  unpack=\"$work/$i\"",
                "  archive=\"$work/$i.tar.xz\"",
                "  mkdir -p \"$unpack\"",
                "  curl -fsSL \"$url\" -o \"$archive\"",
                "  echo \"$sha256  $archive\" | sha256sum -c -",
                "  tar -xJf \"$archive\" -C \"$unpack\"",
                "  install_sh=\"$(find \"$unpack\" -mindepth 2 -maxdepth 2 -name install.sh -print | sed -n '1p')\"",
                "  if [ -z \"$install_sh\" ]; then",
                "    echo \"could not find install.sh in $archive\" >&2",
                "    exit 1",
                "  fi",
                "  sh \"$install_sh\" --prefix=\"$out\" --disable-ldconfig",
                "done",
                "rm -rf \"$work\"",
            ],
            is_executable = True,
            has_content_based_path = False,
        )
        ctx.actions.run(
            cmd_args(["/bin/sh", installer, outputs[toolchain].as_output()] + archives),
            category = "install_rust_toolchain",
            prefer_remote = True,
        )

    ctx.actions.dynamic_output(
        dynamic = [rust_toolchain_file, manifest],
        inputs = [],
        outputs = [toolchain.as_output()],
        f = install_toolchain,
    )

    suffix = ctx.attrs.binary_suffix
    rustc = toolchain.project("bin/rustc" + suffix)
    rustdoc = toolchain.project("bin/rustdoc" + suffix)
    clippy_driver = toolchain.project("bin/clippy-driver" + suffix)

    return [
        DefaultInfo(
            default_output = toolchain,
            sub_targets = {
                "clippy-driver": [DefaultInfo(default_output = clippy_driver)],
                "rustc": [DefaultInfo(default_output = rustc)],
                "rustdoc": [DefaultInfo(default_output = rustdoc)],
            },
        ),
        _rust_toolchain_info(
            ctx,
            # Rust tools load shared libraries from sibling directories at
            # runtime, so remote actions need the whole toolchain tree.
            compiler = RunInfo(args = cmd_args(rustc, hidden = toolchain)),
            clippy_driver = RunInfo(args = cmd_args(clippy_driver, hidden = toolchain)),
            rustdoc = RunInfo(args = cmd_args(rustdoc, hidden = toolchain)),
            sysroot_path = toolchain,
        ),
    ]

downloaded_rust_toolchain = rule(
    impl = _downloaded_rust_toolchain_impl,
    attrs = _COMMON_ATTRS | {
        "base_components": attrs.list(attrs.string(), default = ["rustc", "rust-std", "cargo"]),
        "base_url": attrs.string(default = _RUST_STATIC_DIST),
        "binary_suffix": attrs.string(default = select({
            "DEFAULT": "",
            "prelude//os:windows": ".exe",
        })),
        "extra_components": attrs.list(attrs.string(), default = ["clippy"]),
        "rust_toolchain": attrs.source(),
        "rustc_host_triple": attrs.string(default = _DEFAULT_TRIPLE),
    },
    is_toolchain_rule = True,
)
