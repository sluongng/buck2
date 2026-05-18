# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

from __future__ import annotations

from typing import Any, Callable


class FixtureRequest:
    pass


class Function:
    pass


class SkipTest(Exception):
    pass


class XFail(Exception):
    pass


class _ExceptionInfo:
    def __init__(self) -> None:
        self.value: BaseException | None = None


class _Raises:
    def __init__(self, exception: type[BaseException]) -> None:
        self.exception = exception
        self.info = _ExceptionInfo()

    def __enter__(self) -> _ExceptionInfo:
        return self.info

    def __exit__(self, exc_type: Any, exc: BaseException | None, traceback: Any) -> bool:
        if exc_type is None:
            raise AssertionError("DID NOT RAISE {}".format(self.exception.__name__))
        if not issubclass(exc_type, self.exception):
            return False
        self.info.value = exc
        return True


class _Approx:
    def __init__(self, expected: float, rel: float = 1e-6, abs: float = 1e-12) -> None:
        self.expected = expected
        self.rel = rel
        self.abs = abs

    def __eq__(self, actual: object) -> bool:
        if not isinstance(actual, (float, int)):
            return False
        tolerance = max(self.abs, self.rel * abs(self.expected))
        return abs(float(actual) - self.expected) <= tolerance


class MarkInfo:
    def __init__(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        self.name = name
        self.args = args
        self.kwargs = kwargs


def _attach_mark(fn: Callable[..., Any], mark_info: MarkInfo) -> Callable[..., Any]:
    marks = list(getattr(fn, "__pytest_marks__", []))
    marks.append(mark_info)
    setattr(fn, "__pytest_marks__", marks)
    return fn


class _Mark:
    def __getattr__(self, name: str) -> Callable[..., Any]:
        def mark_factory(*args: Any, **kwargs: Any) -> Callable[..., Any]:
            mark_info = MarkInfo(name, args, kwargs)
            if len(args) == 1 and callable(args[0]) and not kwargs:
                return _attach_mark(args[0], mark_info)

            def apply(fn: Callable[..., Any]) -> Callable[..., Any]:
                return _attach_mark(fn, mark_info)

            return apply

        return mark_factory


mark = _Mark()


def fixture(*args: Any, **kwargs: Any) -> Callable[..., Any]:
    def apply(fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn

    if len(args) == 1 and callable(args[0]) and not kwargs:
        return args[0]
    return apply


def raises(exception: type[BaseException]) -> _Raises:
    return _Raises(exception)


def approx(expected: float, rel: float = 1e-6, abs: float = 1e-12) -> _Approx:
    return _Approx(expected, rel=rel, abs=abs)


def skip(reason: str = "") -> None:
    raise SkipTest(reason)


def xfail(reason: str = "") -> None:
    raise XFail(reason)


def fail(reason: str = "") -> None:
    raise AssertionError(reason)
