# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

import functools
import inspect
from typing import Any, Callable


def decorator(caller: Callable[..., Any], fn: Callable[..., Any]) -> Callable[..., Any]:
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        result = caller(fn, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        return caller(fn, *args, **kwargs)

    wrapper = async_wrapper if inspect.iscoroutinefunction(fn) else sync_wrapper
    functools.update_wrapper(wrapper, fn)
    for attr in ("__pytest_marks__",):
        if hasattr(fn, attr):
            setattr(wrapper, attr, getattr(fn, attr))
    return wrapper
