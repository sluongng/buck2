# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@shim//build_defs:python_binary.bzl", _python_binary = "python_binary")
load("@shim//build_defs:python_library.bzl", _python_library = "python_library")

python_binary = _python_binary
python_library = _python_library
