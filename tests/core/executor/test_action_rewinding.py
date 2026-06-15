# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# pyre-strict

import hashlib
import os

import pytest
from buck2.tests.e2e_util.api.buck import Buck
from buck2.tests.e2e_util.buck_workspace import buck_test
from buck2.tests.e2e_util.helper.utils import read_what_ran


requires_buildbuddy_remote = pytest.mark.skipif(
    not os.environ.get("BUILDBUDDY_API_KEY"),
    reason="requires BuildBuddy remote execution credentials",
)

PRODUCER_CONTENT = "generated input for action rewind\n"
PRODUCER_DIGEST = (
    f"{hashlib.sha1(PRODUCER_CONTENT.encode()).hexdigest()}:{len(PRODUCER_CONTENT)}"
)
CONSUMER_CONTENT = f"consumer saw: {PRODUCER_CONTENT}"
TARGET = "root//:consumer"
REMOTE_ARGS = [
    "--remote-only",
    "--no-remote-cache",
]


async def _restart_with_test_env(buck: Buck, env: dict[str, str]) -> None:
    for key, value in env.items():
        buck.set_env(key, value)
    await buck.kill()


async def _assert_remote_producer_and_consumer_ran(buck: Buck) -> None:
    what_ran = await read_what_ran(buck)
    actions = {
        entry["identity"]: entry["reproducer"]["executor"]
        for entry in what_ran
        if "reproducer" in entry
    }

    assert any("root//:producer" in identity for identity in actions), actions
    assert any("root//:consumer" in identity for identity in actions), actions
    assert all(executor == "Re" for executor in actions.values()), actions


@buck_test()
@requires_buildbuddy_remote
async def test_generated_input_reported_missing_is_materialized_and_reuploaded(
    buck: Buck,
) -> None:
    await _restart_with_test_env(
        buck,
        {
            "BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE": PRODUCER_DIGEST,
        },
    )
    result = await buck.build(
        TARGET,
        *REMOTE_ARGS,
    )

    output = result.get_build_report().output_for_target(TARGET)
    assert output.read_text() == CONSUMER_CONTENT
    await _assert_remote_producer_and_consumer_ran(buck)


@buck_test()
@requires_buildbuddy_remote
async def test_rewinds_generated_input_evicted_from_remote_cache_mid_build(
    buck: Buck,
) -> None:
    await _restart_with_test_env(
        buck,
        {
            "BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE": PRODUCER_DIGEST,
            "BUCK2_TEST_FAIL_RE_DOWNLOAD_DIGESTS_ONCE": PRODUCER_DIGEST,
        },
    )
    result = await buck.build(
        TARGET,
        *REMOTE_ARGS,
    )

    output = result.get_build_report().output_for_target(TARGET)
    assert output.read_text() == CONSUMER_CONTENT
    await _assert_remote_producer_and_consumer_ran(buck)
