/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

use std::env;
use std::io;
use std::path::PathBuf;

use prost_wkt_build::*;

fn main() -> io::Result<()> {
    let proto_files = &[
        "proto/build/bazel/remote/execution/v2/remote_execution.proto",
        "proto/build/bazel/semver/semver.proto",
        "proto/google/api/annotations.proto",
        "proto/google/api/client.proto",
        "proto/google/api/http.proto",
        "proto/google/bytestream/bytestream.proto",
        "proto/google/longrunning/operations.proto",
        "proto/google/rpc/code.proto",
        "proto/google/rpc/status.proto",
    ];

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = out.join("descriptors.bin");

    buck2_protoc_dev::configure()
        .setup_protoc()
        .type_attribute(".", "#[derive(serde::Serialize,serde::Deserialize)]")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .file_descriptor_set_path(&descriptor_file)
        .compile(proto_files, &["./proto/"])
        .unwrap();

    let descriptor_bytes = std::fs::read(descriptor_file).unwrap();
    let descriptor = FileDescriptorSet::decode(&descriptor_bytes[..]).unwrap();
    prost_wkt_build::add_serde(out, descriptor);

    Ok(())
}
