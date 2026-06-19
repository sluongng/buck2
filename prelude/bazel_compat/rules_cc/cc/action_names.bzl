# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

ACTION_NAMES = struct(
    c_compile = "c-compile",
    cpp_compile = "c++-compile",
    cpp_link_executable = "c++-link-executable",
    cpp_link_nodeps_dynamic_library = "c++-link-nodeps-dynamic-library",
    cpp_link_dynamic_library = "c++-link-dynamic-library",
    assemble = "assemble",
    preprocess_assemble = "preprocess-assemble",
    linkstamp_compile = "linkstamp-compile",
)
