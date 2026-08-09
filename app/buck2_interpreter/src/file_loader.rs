/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;

use allocative::Allocative;
use buck2_core::bzl::ImportPath;
use buck2_error::conversion::from_any_with_tag;
use derivative::Derivative;
use dupe::Dupe;
use starlark::codemap::FileSpan;
use starlark::environment::FrozenModule;
use starlark::eval::FileLoader;
use starlark::values::OwnedFrozen;
use starlark::values::OwnedFrozenRef;
use starlark::values::Value;
use starlark::values::structs::StructRef;
use starlark_map::ordered_map::OrderedMap;

use crate::paths::module::OwnedStarlarkModulePath;
use crate::paths::module::StarlarkModulePath;

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = Input)]
enum FileLoaderError {
    #[error("`native` is missing from the configured Buck2 prelude")]
    NativeMissing,
    #[error("`native` in `prelude.bzl` must be a struct")]
    NativeMustBeStruct,
    #[error("`native.genrule` is missing from the configured Buck2 prelude")]
    NativeGenruleMissing,
}

#[derive(Default, Clone, Allocative, Debug, pagable::Pagable)]
pub struct LoadedModules {
    pub map: OrderedMap<OwnedStarlarkModulePath, LoadedModule>,
}

impl LoadedModules {
    pub fn imports(&self) -> impl Iterator<Item = &ImportPath> {
        self.map.values().map(|module| match module.path() {
            StarlarkModulePath::LoadFile(p)
            | StarlarkModulePath::JsonFile(p)
            | StarlarkModulePath::TomlFile(p) => p,
            _ => panic!("imports should only be bzl, json, or toml files"),
        })
    }
}

pub trait LoadResolver {
    fn resolve_load(
        &self,
        path: &str,
        location: Option<&FileSpan>,
    ) -> buck2_error::Result<OwnedStarlarkModulePath>;
}

pub struct ModuleDeps(pub Vec<LoadedModule>);

impl ModuleDeps {
    pub fn get_loaded_modules(&self) -> LoadedModules {
        let mut map = OrderedMap::with_capacity(self.0.len());
        for dep in &*self.0 {
            map.insert(dep.path().to_owned(), dep.dupe());
        }
        LoadedModules { map }
    }
}

#[derive(Clone, Dupe, Allocative, Debug, pagable::Pagable)]
pub struct LoadedModule(Arc<LoadedModuleData>);

#[derive(Derivative, Allocative, pagable::Pagable)]
#[derivative(Debug)]
struct LoadedModuleData {
    path: OwnedStarlarkModulePath,
    #[derivative(Debug = "ignore")]
    loaded_modules: LoadedModules,
    #[derivative(Debug = "ignore")]
    env: FrozenModule,
}

impl LoadedModule {
    pub fn new(
        path: OwnedStarlarkModulePath,
        loaded_modules: LoadedModules,
        env: FrozenModule,
    ) -> Self {
        Self(Arc::new(LoadedModuleData {
            path,
            loaded_modules,
            env,
        }))
    }

    pub fn loaded_modules(&self) -> &LoadedModules {
        &self.0.loaded_modules
    }

    pub fn imports(&self) -> impl Iterator<Item = &ImportPath> {
        self.0.loaded_modules.imports()
    }

    pub fn path(&self) -> StarlarkModulePath<'_> {
        self.0.path.borrow()
    }

    pub fn env(&self) -> &FrozenModule {
        &self.0.env
    }

    /// The prelude's `native` struct, whose members are exposed as extra globals when evaluating
    /// `BUCK` files.
    ///
    /// The result borrows this module; use [`OwnedFrozenRef::add_to_heap`] to read the members on
    /// another heap.
    pub fn native_globals_for_buck_files(
        &self,
    ) -> buck2_error::Result<Option<OwnedFrozenRef<'_, StructRef<'static>>>> {
        let Some(native) = self
            .0
            .env
            .get_option_ref("native")
            .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))?
        else {
            return Ok(None);
        };
        let native = native.try_map(|v| {
            StructRef::from_value(v)
                .ok_or_else(|| buck2_error::Error::from(FileLoaderError::NativeMustBeStruct))
        })?;
        Ok(Some(native))
    }

    /// Obtain an owned reference to the trusted prelude's `native.genrule` implementation.
    /// The returned value retains the frozen module heap and can safely outlive this borrow.
    pub fn bazel_genrule_backend(&self) -> buck2_error::Result<OwnedFrozen<Value<'static>>> {
        let native = self
            .0
            .env
            .get_option_ref("native")
            .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Input))?
            .ok_or(FileLoaderError::NativeMissing)?;
        let native: OwnedFrozenRef<'_, StructRef<'static>> = native.try_map(|value| {
            StructRef::from_value(value).ok_or(FileLoaderError::NativeMustBeStruct)
        })?;
        let genrule = native.try_map(|native: StructRef<'_>| {
            native
                .iter()
                .find_map(|(name, value)| (name.as_str() == "genrule").then_some(value))
                .ok_or(FileLoaderError::NativeGenruleMissing)
        })?;
        Ok(genrule.to_owned())
    }
}

pub struct InterpreterFileLoader {
    loaded_modules: LoadedModules,
    info: Arc<dyn LoadResolver>,
}

impl InterpreterFileLoader {
    pub fn new(loaded_modules: LoadedModules, info: Arc<dyn LoadResolver>) -> Self {
        Self {
            loaded_modules,
            info,
        }
    }
}

fn to_diagnostic(err: &buck2_error::Error, id: &str) -> buck2_error::Error {
    buck2_error::buck2_error!(
        buck2_error::ErrorTag::Tier0,
        "UnknownError in {}: {}",
        id,
        err
    )
}

impl InterpreterFileLoader {
    /// Used for looking up modules by id.
    fn find_module(&self, id: StarlarkModulePath) -> buck2_error::Result<&FrozenModule> {
        match self.loaded_modules.map.get(&id) {
            Some(v) => Ok(&v.0.env),
            None => Err(to_diagnostic(
                &buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "Should have had an env for {}. had <{:?}>",
                    id,
                    self.loaded_modules.map.keys().collect::<Vec<_>>()
                ),
                &id.to_string(),
            )),
        }
    }

    pub fn loaded_module(&self, id: StarlarkModulePath<'_>) -> buck2_error::Result<&LoadedModule> {
        self.loaded_modules.map.get(&id).ok_or_else(|| {
            to_diagnostic(
                &buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "Should have had a loaded module for {}",
                    id
                ),
                &id.to_string(),
            )
        })
    }
}

impl FileLoader for InterpreterFileLoader {
    /// The Interpreter will call this to resolve and load imports for load()
    /// statements.
    fn load(&self, path: &str) -> starlark::Result<FrozenModule> {
        match self.info.resolve_load(path, None) {
            Ok(import) => Ok(self.find_module(import.borrow())?.dupe()),
            Err(e) => Err(to_diagnostic(&e, path).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use buck2_error::buck2_error;
    use starlark::environment::Module;

    use super::*;
    use crate::testing::Buck2TestHeapName;

    struct TestLoadResolver {}

    impl LoadResolver for TestLoadResolver {
        fn resolve_load(
            &self,
            path: &str,
            _location: Option<&FileSpan>,
        ) -> buck2_error::Result<OwnedStarlarkModulePath> {
            match path {
                "//some/package:import.bzl" => Ok(OwnedStarlarkModulePath::LoadFile(
                    ImportPath::testing_new("root//some/package:import.bzl"),
                )),
                "cell1//next/package:import.bzl" => Ok(OwnedStarlarkModulePath::LoadFile(
                    ImportPath::testing_new("cell1//next/package:import.bzl"),
                )),
                "alias2//last/package:import.bzl" => Ok(OwnedStarlarkModulePath::LoadFile(
                    ImportPath::testing_new("cell2//last/package:import.bzl"),
                )),
                _ => Err(buck2_error!(buck2_error::ErrorTag::Tier0, "error")),
            }
        }
    }

    fn resolver() -> Arc<TestLoadResolver> {
        Arc::new(TestLoadResolver {})
    }

    fn env(name: StarlarkModulePath) -> FrozenModule {
        // Test
        // ast-grep-ignore: rust/buck2-no-starlark-module
        Module::with_temp_heap(|m| {
            m.set("name", m.heap().alloc(name.to_string()));
            m.freeze_named(Buck2TestHeapName::frozen_heap_name())
        })
        .unwrap()
    }

    fn loaded_modules() -> LoadedModules {
        let mut loaded_modules = LoadedModules::default();
        let resolver = resolver();

        let mut insert = |path| {
            let import_path = resolver.resolve_load(path, None).unwrap();
            let module = LoadedModule::new(
                import_path.clone(),
                LoadedModules::default(),
                env(import_path.borrow()),
            );
            loaded_modules.map.insert(import_path, module);
        };

        // Insert the same things that the TestLoadResolver supports.
        insert("//some/package:import.bzl");
        insert("cell1//next/package:import.bzl");
        insert("alias2//last/package:import.bzl");

        loaded_modules
    }

    #[test]
    fn no_resolution() -> buck2_error::Result<()> {
        let path = "some//random:file.bzl".to_owned();
        let loader = InterpreterFileLoader::new(loaded_modules(), resolver());
        match loader.load(&path) {
            Ok(_) => panic!("Expected load failure for {path}"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(&path),
                    "Error should reference the failed path: {msg}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn missing_in_loaded_modules() -> buck2_error::Result<()> {
        let path = "cell1//next/package:import.bzl".to_owned();
        let resolver = resolver();
        let id = resolver.resolve_load(&path, None)?;

        let mut loaded_modules = loaded_modules();
        loaded_modules.map.remove(&id);
        let loader = InterpreterFileLoader::new(loaded_modules, resolver);
        match loader.load(&path) {
            Ok(_) => panic!("Expected load failure for {path}"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("Should have had an env"),
                    "Error should indicate missing module: {msg}"
                );
                let id_str = id.to_string();
                assert!(
                    msg.contains(&id_str),
                    "Error should reference the missing module {id_str}: {msg}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn valid_load() -> buck2_error::Result<()> {
        let path = "cell1//next/package:import.bzl".to_owned();
        let resolver = resolver();
        let id = resolver.resolve_load(&path, None)?.to_string();

        let loader = InterpreterFileLoader::new(loaded_modules(), resolver);
        let loaded = loader.load(&path)?;

        let v = loaded.get("name").unwrap();
        assert_eq!(v.as_ref().value().unpack_str(), Some(id.as_str()));

        Ok(())
    }

    #[test]
    fn valid_find() -> buck2_error::Result<()> {
        let path = "cell1//next/package:import.bzl".to_owned();
        let resolver = resolver();
        let resolved = resolver.resolve_load(&path, None)?;
        let borrow = resolved.borrow();

        let loader = InterpreterFileLoader::new(loaded_modules(), resolver);
        let found = loader.find_module(borrow)?;

        let v = found.get("name").unwrap();
        assert_eq!(
            v.as_ref().value().unpack_str(),
            Some(borrow.to_string().as_str())
        );

        Ok(())
    }

    #[test]
    fn bazel_backend_reports_missing_native_stably() {
        let path =
            OwnedStarlarkModulePath::LoadFile(ImportPath::testing_new("root//prelude:prelude.bzl"));
        let module = LoadedModule::new(
            path,
            LoadedModules::default(),
            // `env` deliberately exports only `name`.
            env(StarlarkModulePath::LoadFile(&ImportPath::testing_new(
                "root//prelude:prelude.bzl",
            ))),
        );
        let error = module.bazel_genrule_backend().unwrap_err().to_string();
        assert!(
            error.contains("`native` is missing from the configured Buck2 prelude"),
            "unexpected error: {error}"
        );
    }
}
