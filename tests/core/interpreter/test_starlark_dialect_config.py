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
_BAZEL_SYNTAX_ERROR = "type annotation"


async def _daemon_identity(buck: Buck) -> tuple[int, str]:
    status = json.loads((await buck.status()).stdout)
    return (
        status["process_info"]["pid"],
        status["daemon_constraints"]["daemon_id"],
    )


@buck_test()
async def test_starlark_dialect_changes_in_one_daemon(buck: Buck) -> None:
    absent = await buck.uquery("root//:")
    first_identity = await _daemon_identity(buck)

    await expect_failure(
        buck.uquery("root//:", "-c", _BAZEL),
        stderr_regex=_BAZEL_SYNTAX_ERROR,
    )
    bazel_identity = await _daemon_identity(buck)

    explicit_buck2 = await buck.uquery("root//:", "-c", _BUCK2)
    final_identity = await _daemon_identity(buck)

    assert absent.stdout == explicit_buck2.stdout
    assert "root//:root_ok" in absent.stdout
    assert first_identity == bazel_identity == final_identity


@buck_test()
async def test_starlark_dialect_config_precedence_and_validation(buck: Buck) -> None:
    config_file = str(buck.cwd / "bazel.buckconfig")
    await expect_failure(
        buck.uquery("root//:", "--config-file", config_file),
        stderr_regex=_BAZEL_SYNTAX_ERROR,
    )

    overridden = await buck.uquery(
        "root//:",
        "--config-file",
        config_file,
        "-c",
        _BUCK2,
    )
    assert "root//:root_ok" in overridden.stdout

    invalid = await expect_failure(
        buck.uquery("root//:", "-c", "buck2.starlark_dialect=invalid")
    )
    assert "[buck2] starlark_dialect" in invalid.stderr
    assert "invalid" in invalid.stderr
    assert "buck2" in invalid.stderr
    assert "bazel" in invalid.stderr


@buck_test()
async def test_starlark_dialect_is_selected_from_root_config(buck: Buck) -> None:
    # The child cell requests Bazel in its own config. The root omits the key,
    # so the project-wide selector remains the Buck2 default.
    child = await buck.uquery("child//:")
    assert "child//:child_ok" in child.stdout


@buck_test()
async def test_starlark_lint_uses_the_selected_dialect(buck: Buck) -> None:
    await expect_failure(
        buck.starlark("lint", "defs.bzl", "-c", _BAZEL),
        stdout_regex=_BAZEL_SYNTAX_ERROR,
    )
