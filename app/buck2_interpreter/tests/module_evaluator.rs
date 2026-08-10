/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use buck2_bzlmod::DependencyRepoName;
use buck2_bzlmod::ModuleFile;
use buck2_bzlmod::ModuleKey;
use buck2_bzlmod::ModuleName;
use buck2_bzlmod::Version;
use buck2_interpreter::module_evaluator::ModuleFileEvalKind;
use buck2_interpreter::module_evaluator::ModuleFileEvaluation;
use buck2_interpreter::module_evaluator::evaluate_module_file;
use buck2_interpreter::module_evaluator::module_file_global_names_for_audit;

fn root(source: &str) -> buck2_error::Result<ModuleFile> {
    evaluate_module_file(
        "MODULE.bazel",
        source.to_owned(),
        ModuleFileEvalKind::Root {
            ignore_dev_dependencies: false,
        },
    )
    .map(|evaluated| evaluated.module_file().clone())
}

fn root_ignoring_dev(source: &str) -> buck2_error::Result<ModuleFile> {
    evaluate_module_file(
        "MODULE.bazel",
        source.to_owned(),
        ModuleFileEvalKind::Root {
            ignore_dev_dependencies: true,
        },
    )
    .map(|evaluated| evaluated.module_file().clone())
}

fn dependency(source: &str, name: &str, version: &str) -> buck2_error::Result<ModuleFile> {
    evaluate_module_file(
        "@registry//:MODULE.bazel",
        source.to_owned(),
        ModuleFileEvalKind::Dependency {
            expected: ModuleKey::new(
                ModuleName::parse(name).unwrap(),
                Version::parse(version).unwrap(),
            ),
        },
    )
    .map(|evaluated| evaluated.module_file().clone())
}

fn root_with_diagnostics(source: &str) -> buck2_error::Result<ModuleFileEvaluation> {
    evaluate_module_file(
        "MODULE.bazel",
        source.to_owned(),
        ModuleFileEvalKind::Root {
            ignore_dev_dependencies: false,
        },
    )
}

fn dependency_with_diagnostics(
    source: &str,
    name: &str,
    version: &str,
) -> buck2_error::Result<ModuleFileEvaluation> {
    evaluate_module_file(
        "@registry//:MODULE.bazel",
        source.to_owned(),
        ModuleFileEvalKind::Dependency {
            expected: ModuleKey::new(
                ModuleName::parse(name).unwrap(),
                Version::parse(version).unwrap(),
            ),
        },
    )
}

fn assert_rejected(result: buck2_error::Result<ModuleFile>, expected: &str) {
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains(expected),
        "expected error containing `{expected}`, actual error:\n{error}"
    );
}

#[test]
fn evaluates_pinned_bazel_root_module_snippet() {
    // Derived from Bazel d99d82 ModuleFileFunctionTest.testRootModule.
    let file = root(
        r#"
module(
    name = "aaa",
    version = "0.1+ignored",
    compatibility_level = 4,
    bazel_compatibility = [">=8.0.0", "-9.1.0"],
)
bazel_dep(name = "bbb", version = "1.0")
bazel_dep(name = "ccc", version = "2.0", repo_name = "see")
bazel_dep(name = "ddd", version = "3.0", repo_name = None)
bazel_dep(name = "ggg", repo_name = "gee", max_compatibility_level = 5)
"#,
    )
    .unwrap();

    let declaration = file.declaration().unwrap();
    assert_eq!(declaration.name().unwrap().as_str(), "aaa");
    assert_eq!(declaration.version().normalized(), "0.1");
    assert_eq!(declaration.repo_name(), Some("aaa"));
    assert_eq!(
        declaration.bazel_compatibility(),
        [Box::<str>::from(">=8.0.0"), Box::<str>::from("-9.1.0")]
    );

    let deps = file.dependencies();
    assert_eq!(deps.len(), 4);
    assert_eq!(deps[0].module().to_string(), "bbb@1.0");
    assert_eq!(
        deps[0].repo_name(),
        &DependencyRepoName::Apparent("bbb".into())
    );
    assert_eq!(deps[1].module().to_string(), "ccc@2.0");
    assert_eq!(
        deps[1].repo_name(),
        &DependencyRepoName::Apparent("see".into())
    );
    assert_eq!(deps[2].module().to_string(), "ddd@3.0");
    assert!(deps[2].is_nodep());
    assert_eq!(deps[3].module().to_string(), "ggg@_");
    assert_eq!(
        deps[3].repo_name(),
        &DependencyRepoName::Apparent("gee".into())
    );
}

#[test]
fn evaluates_dependency_and_ignores_its_dev_dependencies() {
    // Representative registry-style snippet derived from pinned Bazel MODULE
    // tests: dependency metadata must match, and all dependency dev edges are
    // ignored after their arguments are validated.
    let file = dependency(
        r#"
module(name = "rules_cc", version = "0.2.22", bazel_compatibility = [">=8.0.0"])
bazel_dep(name = "bazel_features", version = "1.50.0")
bazel_dep(name = "rules_shell", version = "0.2.0", dev_dependency = True)
bazel_dep(name = "googletest", version = "1.17.0", repo_name = None, dev_dependency = True)
"#,
        "rules_cc",
        "0.2.22",
    )
    .unwrap();

    assert_eq!(file.dependencies().len(), 1);
    assert_eq!(
        file.dependencies()[0].module().to_string(),
        "bazel_features@1.50.0"
    );
    assert!(!file.dependencies()[0].is_dev_dependency());
}

#[test]
fn root_dev_policy_preserves_or_discards_edges() {
    let source = r#"
bazel_dep(name = "regular", version = "1.0")
bazel_dep(name = "dev", version = "2.0", dev_dependency = True)
bazel_dep(name = "nodep_dev", version = "3.0", repo_name = None, dev_dependency = True)
"#;
    let retained = root(source).unwrap();
    assert_eq!(retained.dependencies().len(), 3);
    assert!(retained.dependencies()[1].is_dev_dependency());
    assert!(retained.dependencies()[2].is_nodep());

    let ignored = root_ignoring_dev(source).unwrap();
    assert_eq!(ignored.dependencies().len(), 1);
    assert_eq!(
        ignored.dependencies()[0].module().to_string(),
        "regular@1.0"
    );
}

#[test]
fn module_omission_and_empty_values_are_finalized_before_context_checks() {
    let file = root("bazel_dep(name = \"bbb\", version = \"1.0\")").unwrap();
    let declaration = file.declaration().unwrap();
    assert!(declaration.name().is_none());
    assert!(declaration.version().is_empty());
    assert_eq!(declaration.repo_name(), Some(""));

    let explicit = root("module(name = \"\", repo_name = \"\")").unwrap();
    assert_eq!(explicit.declaration(), Some(declaration));

    assert_rejected(
        dependency("bazel_dep(name = \"bbb\", version = \"1.0\")", "aaa", "1.0"),
        "declares a different name ()",
    );
}

#[test]
fn dependency_metadata_matches_registry_and_override_keys() {
    assert_rejected(
        dependency("module(name = \"other\", version = \"1.0\")", "aaa", "1.0"),
        "declares a different name (other)",
    );
    assert_rejected(
        dependency("module(name = \"aaa\", version = \"2.0\")", "aaa", "1.0"),
        "declares a different version (2.0)",
    );

    let overridden = dependency("module(name = \"aaa\", version = \"27.4\")", "aaa", "").unwrap();
    assert_eq!(
        overridden.declaration().unwrap().version().normalized(),
        "27.4"
    );
}

#[test]
fn module_must_be_first_and_called_once() {
    assert_rejected(
        root("module(name = \"aaa\")\nmodule(name = \"aaa\")"),
        "can only be called once",
    );
    assert_rejected(
        root("bazel_dep(name = \"bbb\")\nmodule(name = \"aaa\")"),
        "must be called before any other functions",
    );
}

#[test]
fn validates_names_versions_repo_names_and_compatibility() {
    assert_rejected(root("module(name = \"Bad\")"), "invalid module name 'Bad'");
    assert_rejected(
        root("bazel_dep(name = \"bad.\")"),
        "invalid module name 'bad.'",
    );
    assert_rejected(
        root("module(name = \"aaa\", version = \"1..0\")"),
        "Invalid version in module()",
    );
    assert_rejected(
        root("bazel_dep(name = \"aaa\", version = \"1..0\")"),
        "Invalid version in bazel_dep()",
    );
    assert_rejected(
        root("module(name = \"aaa\", repo_name = \"_bad\")"),
        "invalid user-provided repo name '_bad'",
    );
    assert_rejected(
        root("bazel_dep(name = \"aaa\", repo_name = \"bad+name\")"),
        "invalid user-provided repo name 'bad+name'",
    );
    for compatibility in ["8.0.0", ">=8.0", "=>8.0.0", ">=8.x.0"] {
        assert_rejected(
            root(&format!(
                "module(name = \"aaa\", bazel_compatibility = [\"{compatibility}\"])"
            )),
            "invalid version argument",
        );
    }
    assert_rejected(
        root("module(name = \"aaa\", bazel_compatibility = {\">=8.0.0\": True})"),
        "for bazel_compatibility, got dict, want sequence",
    );
    assert_rejected(
        root("module(name = \"aaa\", bazel_compatibility = [\">=8.0.0\", 1])"),
        "at index 1 of bazel_compatibility, got element of type int, want string",
    );
    let tuple =
        root("module(name = \"aaa\", bazel_compatibility = (\">=8.0.0\", \"-9.1.0\"))").unwrap();
    assert_eq!(tuple.declaration().unwrap().bazel_compatibility().len(), 2);
    assert_rejected(
        root("module(name = \"aaa\", repo_name = None)"),
        "repo_name",
    );
}

#[test]
fn deprecated_compatibility_levels_return_root_only_warnings() {
    let evaluated = root_with_diagnostics(
        "module(name = \"aaa\", compatibility_level = 0)\n\
         bazel_dep(name = \"bbb\", max_compatibility_level = 99)",
    )
    .unwrap();
    assert_eq!(
        evaluated
            .module_file()
            .declaration()
            .unwrap()
            .name()
            .unwrap()
            .as_str(),
        "aaa"
    );
    assert_eq!(evaluated.module_file().dependencies().len(), 1);
    assert_eq!(evaluated.warnings().len(), 2);
    assert!(
        evaluated.warnings()[0]
            .location()
            .contains("MODULE.bazel:1")
    );
    assert!(
        evaluated.warnings()[0]
            .message()
            .contains("attribute 'compatibility_level' in module() is a no-op")
    );
    assert!(
        evaluated.warnings()[1]
            .location()
            .contains("MODULE.bazel:2")
    );
    assert!(
        evaluated.warnings()[1]
            .message()
            .contains("attribute 'max_compatibility_level' in bazel_dep() is a no-op")
    );

    let defaults = root_with_diagnostics(
        "module(name = \"aaa\", compatibility_level = -1)\n\
         bazel_dep(name = \"bbb\", max_compatibility_level = -1)",
    )
    .unwrap();
    assert!(defaults.warnings().is_empty());

    let dependency = dependency_with_diagnostics(
        "module(name = \"aaa\", version = \"1.0\", compatibility_level = 4)\n\
         bazel_dep(name = \"bbb\", max_compatibility_level = 5)",
        "aaa",
        "1.0",
    )
    .unwrap();
    assert!(dependency.warnings().is_empty());

    assert_rejected(
        root("module(name = \"aaa\", compatibility_level = \"4\")"),
        "parameter 'compatibility_level' got value of type 'string', want 'int'",
    );
    assert_rejected(
        root("bazel_dep(name = \"aaa\", max_compatibility_level = \"4\")"),
        "parameter 'max_compatibility_level' got value of type 'string', want 'int'",
    );
}

#[test]
fn apparent_name_collisions_fail_but_distinct_aliases_and_nodeps_do_not() {
    let module_collision = root(
        "module(name = \"aaa\", repo_name = \"same\")\n\
         bazel_dep(name = \"bbb\", repo_name = \"same\")",
    )
    .unwrap_err()
    .to_string();
    assert!(module_collision.contains("repo name 'same'"));
    assert!(module_collision.contains("by a bazel_dep at MODULE.bazel:2"));
    assert!(module_collision.contains("as the module's own repo name at MODULE.bazel:1"));

    let dependency_collision = root(
        "bazel_dep(name = \"aaa\", repo_name = \"same\")\n\
         bazel_dep(name = \"bbb\", repo_name = \"same\")",
    )
    .unwrap_err()
    .to_string();
    assert!(dependency_collision.contains("by a bazel_dep at MODULE.bazel:2"));
    assert!(dependency_collision.contains("already defined by a bazel_dep at MODULE.bazel:1"));

    let file = root(
        r#"
bazel_dep(name = "same_module", version = "1.0", repo_name = "one")
bazel_dep(name = "same_module", version = "2.0", repo_name = "two")
bazel_dep(name = "same_module", version = "3.0", repo_name = None)
bazel_dep(name = "same_module", version = "4.0", repo_name = None)
"#,
    )
    .unwrap();
    assert_eq!(file.dependencies().len(), 4);
    assert!(file.dependencies()[2].is_nodep());
    assert!(file.dependencies()[3].is_nodep());
}

#[test]
fn ignored_dev_alias_is_still_reserved() {
    assert_rejected(
        dependency(
            r#"
module(name = "aaa", version = "1.0")
bazel_dep(name = "dev", version = "1.0", repo_name = "same", dev_dependency = True)
bazel_dep(name = "regular", version = "1.0", repo_name = "same")
"#,
            "aaa",
            "1.0",
        ),
        "already defined by a bazel_dep",
    );
}

#[test]
fn literal_kwargs_and_standard_expression_helpers_work() {
    let file = root(
        r#"
name = "root"
module(**{"name": name, "version": str(len([1, 2])) + ".0"})
bazel_dep(**{"name": "dep", "version": "1.0"})
"#,
    )
    .unwrap();
    assert_eq!(file.declaration().unwrap().version().normalized(), "2.0");
    assert_eq!(file.dependencies().len(), 1);
}

#[test]
fn validation_precedence_matches_pinned_bazel() {
    assert_rejected(
        root(
            "module(name = \"aaa\", version = \"1..0\", repo_name = \"_bad\", \
             bazel_compatibility = [\"bad\"])",
        ),
        "invalid user-provided repo name '_bad'",
    );
    assert_rejected(
        root(
            "module(name = \"aaa\", version = \"1..0\", \
             bazel_compatibility = [\"bad\"])",
        ),
        "Invalid version in module()",
    );
    assert_rejected(
        root(
            "module(name = \"aaa\", version = \"1..0\", repo_name = \"_bad\", \
             compatibility_level = 999999999999999999999999999999)",
        ),
        "got 999999999999999999999999999999 for compatibility_level, want value in signed 32-bit range",
    );

    assert_rejected(
        root(
            "bazel_dep(name = \"bad.\", version = \"1..0\", repo_name = \"_bad\", \
             max_compatibility_level = 999999999999999999999999999999)",
        ),
        "invalid module name 'bad.'",
    );
    assert_rejected(
        root(
            "bazel_dep(name = \"aaa\", version = \"1..0\", repo_name = \"_bad\", \
             max_compatibility_level = 999999999999999999999999999999)",
        ),
        "Invalid version in bazel_dep()",
    );
    assert_rejected(
        root(
            "bazel_dep(name = \"aaa\", version = \"1.0\", repo_name = \"_bad\", \
             max_compatibility_level = 999999999999999999999999999999)",
        ),
        "got 999999999999999999999999999999 for max_compatibility_level, want value in signed 32-bit range",
    );
}

#[test]
fn exact_global_surface_includes_only_isolated_phase_two_capabilities() {
    assert_eq!(
        module_file_global_names_for_audit(),
        [
            "False",
            "None",
            "True",
            "abs",
            "all",
            "any",
            "bazel_dep",
            "bool",
            "bytes",
            "chr",
            "dict",
            "dir",
            "enumerate",
            "fail",
            "float",
            "getattr",
            "hasattr",
            "hash",
            "int",
            "len",
            "list",
            "max",
            "min",
            "module",
            "ord",
            "print",
            "range",
            "repr",
            "reversed",
            "sorted",
            "str",
            "tuple",
            "type",
            "zip",
        ]
    );
}

#[test]
fn print_is_permitted_but_discarded() {
    let evaluated =
        root_with_diagnostics("print(\"debug\", 1)\nmodule(name = \"aaa\")\nprint(\"done\")")
            .unwrap();
    assert_eq!(
        evaluated
            .module_file()
            .declaration()
            .unwrap()
            .name()
            .unwrap()
            .as_str(),
        "aaa"
    );
    assert!(evaluated.warnings().is_empty());
}

#[test]
fn build_action_filesystem_and_network_globals_are_absent() {
    for (source, expected) in [
        ("glob([\"**\"])", "glob"),
        ("read_config(\"x\", \"y\")", "read_config"),
        ("native.genrule(name = \"x\")", "native"),
        ("ctx.actions.run()", "ctx"),
        (
            "repository_ctx.download(\"https://example.invalid\")",
            "repository_ctx",
        ),
        ("http_archive(name = \"x\", urls = [])", "http_archive"),
    ] {
        assert_rejected(root(source), expected);
    }
    assert_rejected(
        root("load(\"//:defs.bzl\", \"x\")"),
        "not allowed in this dialect",
    );
}

#[test]
fn later_phase_module_directives_fail_explicitly() {
    for directive in [
        "single_version_override(module_name = \"aaa\", version = \"1.0\")",
        "multiple_version_override(module_name = \"aaa\", versions = [\"1.0\"])",
        "archive_override(module_name = \"aaa\", urls = [])",
        "git_override(module_name = \"aaa\", remote = \"https://example.invalid\", commit = \"abc\")",
        "local_path_override(module_name = \"aaa\", path = \"../aaa\")",
        "use_extension(\"//:extensions.bzl\", \"ext\")",
        "use_repo(None, \"repo\")",
        "use_repo_rule(\"//:repo.bzl\", \"repo_rule\")",
        "register_toolchains(\"//:toolchain\")",
        "register_execution_platforms(\"//:platform\")",
    ] {
        let name = directive.split_once('(').unwrap().0;
        assert_rejected(root(directive), &format!("Variable `{name}` not found"));
    }
}

#[test]
fn a_failed_evaluation_cannot_publish_or_contaminate_state() {
    assert_rejected(
        root(
            "bazel_dep(name = \"good\", version = \"1.0\")\n\
             bazel_dep(name = \"bad.\", version = \"2.0\")",
        ),
        "invalid module name 'bad.'",
    );

    let next = root("pass").unwrap();
    assert!(next.declaration().is_some());
    assert!(next.dependencies().is_empty());
}
