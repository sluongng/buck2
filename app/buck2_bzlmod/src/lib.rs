/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Pure data types used by Bazel module resolution.
//!
//! This crate intentionally has no evaluator, filesystem, network, or server
//! dependencies. It defines the immutable values exchanged by those layers.

mod declaration;
mod module_key;
mod module_name;
mod version;

pub use declaration::DependencyRepoName;
pub use declaration::DependencyRequest;
pub use declaration::DependencyRequestError;
pub use declaration::LocalPathOverride;
pub use declaration::ModuleDeclaration;
pub use declaration::ModuleFile;
pub use declaration::ModuleOverride;
pub use declaration::PatchLabel;
pub use declaration::PatchLabelParseError;
pub use declaration::SingleVersionOverride;
pub use module_key::ModuleKey;
pub use module_key::ModuleKeyParseError;
pub use module_name::ModuleName;
pub use module_name::ModuleNameParseError;
pub use version::Version;
pub use version::VersionParseError;
