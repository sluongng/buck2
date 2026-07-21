# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def go_package(name, package_name = None, cgo = None, **kwargs):
    native.go_library(
        name = name,
        package_name = package_name,
        srcs = glob(["*.go"]),
        override_cgo_enabled = cgo,
        **kwargs
    )
