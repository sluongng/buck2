# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//decls:common.bzl", "buck")
load("@prelude//decls:re_test_common.bzl", "re_test_common")
load("@prelude//test:inject_test_run_info.bzl", "inject_test_run_info")
load("@prelude//tests:re_utils.bzl", "get_re_executors_from_props")

_CRAM_RUNNER = """#!/usr/bin/env python3
import difflib
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def parse_test(path):
    commands = []
    current = None

    with open(path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\\n")
            if line.startswith("  $ "):
                if current is not None:
                    commands.append(current)
                current = [line[4:], []]
            elif line.startswith("  > ") and current is not None:
                current[0] += "\\n" + line[4:]
            elif line.startswith("  ") and current is not None:
                current[1].append(line[2:])
            elif line.strip():
                raise ValueError("{}: unsupported cram line: {!r}".format(path, line))

    if current is not None:
        commands.append(current)

    return commands


def run_test(path):
    ok = True
    test_dir = str(Path(path).parent)

    with tempfile.TemporaryDirectory(prefix="buck2-cram-") as cram_tmp:
        env = os.environ.copy()
        env["TESTDIR"] = test_dir
        env["CRAMTMP"] = cram_tmp

        for command, expected in parse_test(path):
            proc = subprocess.run(
                command,
                cwd=os.getcwd(),
                env=env,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            actual = proc.stdout.splitlines()

            if proc.returncode != 0 or actual != expected:
                ok = False
                print("{}: command failed: {}".format(path, command), file=sys.stderr)
                if proc.returncode != 0:
                    print("exit code: {}".format(proc.returncode), file=sys.stderr)
                diff = difflib.unified_diff(
                    expected,
                    actual,
                    fromfile="expected",
                    tofile="actual",
                    lineterm="",
                )
                for line in diff:
                    print(line, file=sys.stderr)

    return ok


def main(argv):
    ok = True
    for path in argv[1:]:
        if path.endswith(".t"):
            ok = run_test(path) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
"""

def _test_path(package, src):
    if package:
        return package + "/" + src.short_path
    return src.short_path

def _cram_test_impl(ctx):
    runner = ctx.actions.write("cram_runner.py", _CRAM_RUNNER, is_executable = True)
    tests = [
        _test_path(ctx.label.package, src)
        for src in ctx.attrs.srcs
        if src.short_path.endswith(".t")
    ]

    if not tests:
        fail("cram_test requires at least one .t source")

    labels = list(ctx.attrs.labels)
    if "buck2_run_from_project_root" not in labels:
        labels.append("buck2_run_from_project_root")

    command = cmd_args([runner] + tests, hidden = ctx.attrs.srcs)
    re_executor, executor_overrides = get_re_executors_from_props(ctx)

    return inject_test_run_info(
        ctx,
        ExternalRunnerTestInfo(
            type = "cram",
            command = [command],
            env = ctx.attrs.env,
            labels = labels,
            contacts = ctx.attrs.contacts,
            default_executor = re_executor,
            executor_overrides = executor_overrides,
            run_from_project_root = True,
            use_project_relative_paths = True,
        ),
    ) + [
        DefaultInfo(),
    ]

cram_test = rule(
    impl = _cram_test_impl,
    attrs = {
        "contacts": attrs.list(attrs.string(), default = []),
        "env": attrs.dict(key = attrs.string(), value = attrs.arg(), sorted = False, default = {}),
        "labels": attrs.list(attrs.string(), default = []),
        "srcs": attrs.list(attrs.source(), default = []),
    } | buck.inject_test_env_arg() | re_test_common.test_args(),
)
