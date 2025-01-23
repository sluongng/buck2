/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

use std::io;

fn main() -> io::Result<()> {
    let proto_files = &[
        "proto/google/api/annotations.proto",
        "proto/google/api/client.proto",
        "proto/google/api/field_behavior.proto",
        "proto/google/api/http.proto",
        "proto/google/api/launch_stage.proto",
        "proto/google/devtools/build/v1/build_events.proto",
        "proto/google/devtools/build/v1/build_status.proto",
        "proto/google/devtools/build/v1/publish_build_event.proto",
    ];

    buck2_protoc_dev::configure()
        .setup_protoc()
        .compile(proto_files, &["./proto/"])
}
