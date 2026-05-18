# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the root directory of this source tree. You may
# select, at your option, one of the above-listed licenses.

import asyncio
import importlib
import inspect
import os
from pathlib import Path
import stat
import sys
import tempfile
import traceback
from typing import Any, Callable

import __test_modules__
from buck2.tests.e2e_util.buck_workspace import buck_fixture


def _markers(fn: Callable[..., Any], name: str | None = None) -> list[Any]:
    markers = list(getattr(fn, "__pytest_marks__", []))
    if name is None:
        return markers
    return [marker for marker in markers if marker.name == name]


def _parametrize_cases(fn: Callable[..., Any]) -> list[tuple[dict[str, Any], str]]:
    cases = [({}, "")]
    for marker in _markers(fn, "parametrize"):
        raw_names = marker.args[0]
        values = marker.args[1]
        names = [name.strip() for name in raw_names.split(",")]
        next_cases = []
        for base, suffix in cases:
            for value in values:
                if len(names) == 1:
                    value_tuple = (value,)
                else:
                    value_tuple = tuple(value)
                params = dict(base)
                params.update(dict(zip(names, value_tuple)))
                label = suffix + "[" + ",".join(str(v) for v in value_tuple) + "]"
                next_cases.append((params, label))
        cases = next_cases
    return cases


def _is_skipped(fn: Callable[..., Any]) -> str | None:
    for marker in _markers(fn, "skip"):
        return marker.kwargs.get("reason", "skipped")
    for marker in _markers(fn, "skipif"):
        condition = bool(marker.args[0]) if marker.args else False
        if condition:
            return marker.kwargs.get("reason", "skipped")
    return None


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _run_case(
    module_name: str,
    fn_name: str,
    fn: Callable[..., Any],
    params: dict[str, Any],
    label: str,
) -> tuple[bool, str]:
    skip_reason = _is_skipped(fn)
    case_name = module_name + "." + fn_name + label
    if skip_reason is not None:
        print("SKIP " + case_name + ": " + skip_reason)
        return True, case_name

    signature = inspect.signature(fn)
    kwargs = dict(params)
    buck_markers = _markers(fn, "buck_test")
    needs_buck = "buck" in signature.parameters
    needs_tmp_path = "tmp_path" in signature.parameters
    unsupported = set(signature.parameters) - set(kwargs) - {"buck", "tmp_path"}
    if unsupported:
        raise RuntimeError(
            case_name
            + " uses unsupported pytest fixture(s): "
            + ", ".join(sorted(unsupported))
        )

    os.environ["PYTEST_CURRENT_TEST"] = case_name + " (call)"
    with tempfile.TemporaryDirectory() as tmp_dir:
        if needs_tmp_path:
            kwargs["tmp_path"] = Path(tmp_dir)

        if needs_buck:
            if not buck_markers:
                raise RuntimeError(
                    case_name + " needs the buck fixture but has no buck_test marker"
                )
            with _fbpython_on_path():
                _absolutize_env_path("TEST_EXECUTABLE")
                _absolutize_env_path("NANO_PRELUDE")
                _absolutize_env_path("TEST_REPO_DATA")
                async with buck_fixture(buck_markers[-1].args[0]) as buck:
                    kwargs["buck"] = buck
                    await _maybe_await(fn(**kwargs))
        else:
            await _maybe_await(fn(**kwargs))
    return True, case_name


def _absolutize_env_path(name: str) -> None:
    value = os.environ.get(name)
    if value and not os.path.isabs(value):
        os.environ[name] = str(Path(value).resolve())


class _fbpython_on_path:
    def __enter__(self) -> None:
        self.bin_dir = tempfile.TemporaryDirectory()
        self.old_path = os.environ.get("PATH", "")
        fbpython = os.path.join(self.bin_dir.name, "fbpython")
        with open(fbpython, "w") as script:
            script.write("#!/bin/sh\nexec '{}' \"$@\"\n".format(sys.executable))
        os.chmod(fbpython, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        os.environ["PATH"] = self.bin_dir.name + os.pathsep + self.old_path

    def __exit__(self, exc_type: Any, exc: BaseException | None, traceback: Any) -> None:
        os.environ["PATH"] = self.old_path
        self.bin_dir.cleanup()


async def _run() -> int:
    failures = []
    passed = 0

    for module_name in __test_modules__.TEST_MODULES:
        module = importlib.import_module(module_name)
        for fn_name in sorted(dir(module)):
            if not fn_name.startswith("test_"):
                continue
            fn = getattr(module, fn_name)
            if not callable(fn):
                continue
            for params, label in _parametrize_cases(fn):
                try:
                    _, case_name = await _run_case(
                        module_name,
                        fn_name,
                        fn,
                        params,
                        label,
                    )
                    passed += 1
                    print("PASS " + case_name)
                except BaseException:
                    case_name = module_name + "." + fn_name + label
                    failures.append(case_name)
                    print("FAIL " + case_name, file=sys.stderr)
                    traceback.print_exc()

    print(
        "Ran {} test case(s), {} failure(s)".format(
            passed + len(failures),
            len(failures),
        )
    )
    return 1 if failures else 0


def main() -> None:
    sys.exit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
