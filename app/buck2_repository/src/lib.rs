/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Data types and transport primitives for Bazel index registries.
//!
//! The models ignore unknown fields, matching Bazel's Gson decoding while
//! exposing only bounded, typed data to consumers. `docs_url` is explicitly
//! accepted compatibility metadata. `metadata.json` likewise ignores fields
//! other than `yanked_versions`: the registry metadata schema contains
//! descriptive fields that resolution does not consume. Duplicate fields are
//! always rejected. Selected optional collection and primitive fields treat
//! explicit null as absent, matching Bazel's initialized Gson fields;
//! required fields and the source discriminator remain strict. `MODULE.bazel`
//! remains opaque bytes owned by the caller.

mod file_registry;
mod path;
mod registry_json;

pub use file_registry::FileRegistryRead;
pub use file_registry::FileRegistryReadError;
pub use file_registry::read_file_registry;
pub use path::RegistryFileKind;
pub use path::RegistryPath;
pub use path::RegistryPathError;
pub use registry_json::ArchiveSourceJson;
pub use registry_json::BazelRegistryJson;
pub use registry_json::MetadataJson;
pub use registry_json::RegistryJsonError;
pub use registry_json::SourceJson;
pub use registry_json::SourceUrl;
