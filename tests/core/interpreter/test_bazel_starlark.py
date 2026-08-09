# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# pyre-strict

import json

from buck2.tests.e2e_util.api.buck import Buck
from buck2.tests.e2e_util.asserts import expect_failure
from buck2.tests.e2e_util.buck_workspace import buck_test


_BAZEL = "buck2.starlark_dialect=bazel"
_BUCK2 = "buck2.starlark_dialect=buck2"


async def _daemon_identity(buck: Buck) -> tuple[int, str]:
    status = json.loads((await buck.status()).stdout)
    return (
        status["process_info"]["pid"],
        status["daemon_constraints"]["daemon_id"],
    )


@buck_test(skip_for_os=["windows"])
async def test_bazel_genrule_build_and_same_daemon_mode_transitions(
    buck: Buck,
) -> None:
    # Keep both fixtures in the unchanged workspace. Each command evaluates only
    # the package for its selected dialect, so switching modes exercises DICE
    # identity without rewriting source files or restarting the daemon.
    buck_build = await buck.build("root//buck:buck")
    buck_output = buck_build.get_build_report().output_for_target("root//buck:buck")
    assert buck_output.read_bytes() == b"buck-ok\n"
    initial_identity = await _daemon_identity(buck)

    bazel_build = await buck.build(
        "root//bazel:transitive",
        "root//bazel:direct",
        "-c",
        _BAZEL,
    )
    bazel_report = bazel_build.get_build_report()
    transitive_output = bazel_report.output_for_target("root//bazel:transitive")
    direct_output = bazel_report.output_for_target("root//bazel:direct")
    assert transitive_output.read_bytes() == b"bazel-ok\n"
    assert direct_output.read_bytes() == b"direct-ok\n"

    what_ran = (await buck.log("what-ran", "--format", "json")).stdout
    action_records = [
        json.loads(line) for line in what_ran.splitlines() if line.strip()
    ]
    assert any(
        "root//bazel:transitive" in record.get("identity", "")
        for record in action_records
    ), action_records
    assert any(
        "cat" in " ".join(
            record.get("reproducer", {}).get("details", {}).get("command", [])
        )
        for record in action_records
        if "root//bazel:transitive" in record.get("identity", "")
    ), action_records
    bazel_identity = await _daemon_identity(buck)

    explicit_buck2 = await buck.build("root//buck:buck", "-c", _BUCK2)
    explicit_buck2_output = explicit_buck2.get_build_report().output_for_target(
        "root//buck:buck"
    )
    assert explicit_buck2_output.read_bytes() == b"buck-ok\n"
    final_identity = await _daemon_identity(buck)

    assert initial_identity == bazel_identity == final_identity


@buck_test()
async def test_bazel_environment_placement_and_genrule_boundary(buck: Buck) -> None:
    cases = [
        ("root//negative/build_native:", r"Variable `native` not found"),
        ("root//negative/bzl_direct:", r"Variable `genrule` not found"),
        ("root//negative/root_import:", r"Variable `root_marker` not found"),
        (
            "root//negative/package_import:",
            r"Variable `implicit_package_symbol` not found",
        ),
        (
            "root//negative/native_cc_library:",
            r"namespace.*has no attribute `cc_library`",
        ),
        (
            "root//negative/unknown_kwarg:",
            r"Found `unknown` extra named parameter",
        ),
        ("root//negative/zero_outs:", r"exactly one output"),
        ("root//negative/multiple_outs:", r"exactly one output"),
        ("root//negative/prelude_load:", r"cannot load Buck2 prelude"),
        ("root//negative/load_reexport:", r"not exported"),
    ]

    for target, diagnostic in cases:
        await expect_failure(
            buck.uquery(target, "-c", _BAZEL),
            stderr_regex=diagnostic,
        )

    overlap_config = str(buck.cwd / "overlap.buckconfig")
    await expect_failure(
        buck.uquery(
            "root//bazel:",
            "--config-file",
            overlap_config,
            "-c",
            _BAZEL,
        ),
        stderr_regex=r"dedicated cell or subdirectory",
    )


@buck_test()
async def test_bazel_backend_configuration_diagnostics(buck: Buck) -> None:
    cases = [
        (
            "bad_native.buckconfig",
            r"`native` in `prelude.bzl` must be a struct",
        ),
        (
            "missing_genrule.buckconfig",
            r"`native.genrule` is missing",
        ),
    ]
    for config_name, diagnostic in cases:
        await expect_failure(
            buck.uquery(
                "root//bazel:",
                "--config-file",
                str(buck.cwd / config_name),
                "-c",
                _BAZEL,
            ),
            stderr_regex=diagnostic,
        )


@buck_test()
async def test_bazel_build_syntax_negatives_use_the_real_binary(buck: Buck) -> None:
    cases = [
        ("root//negative/syntax_build/def:", r"`def` is not allowed"),
        ("root//negative/syntax_build/lambda:", r"`lambda` is not allowed"),
        ("root//negative/syntax_build/top_if:", r"`if` cannot be used outside"),
        ("root//negative/syntax_build/top_for:", r"`for` cannot be used outside"),
        (
            "root//negative/syntax_build/star_args:",
            r"`\*args` call arguments are not allowed",
        ),
        (
            "root//negative/syntax_build/star_kwargs:",
            r"`\*\*kwargs` call arguments are not allowed",
        ),
    ]

    for target, diagnostic in cases:
        await expect_failure(
            buck.uquery(target, "-c", _BAZEL),
            stderr_regex=diagnostic,
        )


@buck_test()
async def test_bazel_bzl_syntax_negatives_use_the_real_linter(buck: Buck) -> None:
    cases = [
        ("negative/syntax/types.bzl", r"type annotation"),
        ("negative/syntax/fstrings.bzl", r"f-string"),
        (
            "negative/syntax/late_load.bzl",
            r"load statements must appear before any other statement",
        ),
        ("negative/syntax/private_load.bzl", r"is private and cannot be imported"),
        ("negative/syntax/duplicate_binding.bzl", r"redeclared at top level"),
        ("negative/syntax/top_if.bzl", r"`if` cannot be used outside"),
        ("negative/syntax/top_for.bzl", r"`for` cannot be used outside"),
    ]

    for path, diagnostic in cases:
        await expect_failure(
            buck.starlark("lint", path, "-c", _BAZEL),
            stdout_regex=diagnostic,
        )


@buck_test()
async def test_bazel_starlark_lint_accepts_bazel_globals(buck: Buck) -> None:
    await buck.starlark(
        "lint",
        "bazel/defs.bzl",
        "bazel/macros.bzl",
        "-c",
        _BAZEL,
    )
    await expect_failure(
        buck.starlark("lint", "negative/implicit_bzl.bzl", "-c", _BAZEL),
        stdout_regex=r"undefined variable `root_marker`",
    )


@buck_test()
async def test_bazel_starlark_typecheck_uses_bazel_environment(buck: Buck) -> None:
    await buck.starlark("typecheck", "bazel/defs.bzl", "-c", _BAZEL)
    await expect_failure(
        buck.starlark("typecheck", "negative/syntax/types.bzl", "-c", _BAZEL),
        stderr_regex=r"type annotation",
    )
