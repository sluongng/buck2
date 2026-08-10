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
use buck2_bzlmod::ExtensionUseKind;
use buck2_bzlmod::ModuleFile;
use buck2_bzlmod::ModuleKey;
use buck2_bzlmod::ModuleName;
use buck2_bzlmod::ModuleOverride;
use buck2_bzlmod::RawAttributeValue;
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
            "archive_override",
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
            "git_override",
            "hasattr",
            "hash",
            "int",
            "len",
            "list",
            "local_path_override",
            "max",
            "min",
            "module",
            "multiple_version_override",
            "ord",
            "print",
            "range",
            "repr",
            "reversed",
            "single_version_override",
            "sorted",
            "str",
            "tuple",
            "type",
            "use_extension",
            "use_repo",
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
        "use_repo_rule(\"//:repo.bzl\", \"repo_rule\")",
        "register_toolchains(\"//:toolchain\")",
        "register_execution_platforms(\"//:platform\")",
    ] {
        let name = directive.split_once('(').unwrap().0;
        assert_rejected(root(directive), &format!("Variable `{name}` not found"));
    }
}

#[test]
fn regular_extension_uses_coalesce_and_preserve_all_source_ordered_events() {
    let file = root(
        r#"
module(name = "root", version = "1.2", repo_name = "self")
a = use_extension("//pkg:defs.notbzl", "ext")
getattr(a, "tag-name")(**{
    "attr-name": 123456789012345678901234567890,
    "sequence": ["x", (True, False)],
    "mapping": {"first": 1, "second": [2]},
})
b = use_extension("@self//pkg:defs.notbzl", "ext", dev_dependency = True)
[b.item(value = i) for i in range(2)]
use_repo(a, "plain", alias = "{name}.{version}")
"#,
    )
    .unwrap();

    let [extension] = file.extension_uses() else {
        panic!("expected one coalesced extension use")
    };
    assert_eq!(extension.first_use_ordinal(), 0);
    match extension.kind() {
        ExtensionUseKind::Regular {
            extension_file,
            extension_name,
        } => {
            assert_eq!(extension_file.as_str(), "@self//pkg:defs.notbzl");
            assert_eq!(extension_name.as_str(), "ext");
        }
    }
    assert_eq!(extension.proxies().len(), 2);
    assert_eq!(extension.proxies()[0].ordinal(), 1);
    assert_eq!(
        extension.proxies()[0].exported_name().unwrap().as_str(),
        "a"
    );
    assert_eq!(extension.proxies()[1].ordinal(), 3);
    assert!(extension.proxies()[1].is_dev_dependency());

    let tags = extension.tags();
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].ordinal(), 2);
    assert_eq!(tags[0].class_name(), "tag-name");
    assert_eq!(tags[0].attributes()[0].name(), "attr-name");
    assert_eq!(
        tags[0].attributes()[0].value(),
        &RawAttributeValue::Integer(
            buck2_bzlmod::RawInteger::parse_decimal("123456789012345678901234567890").unwrap()
        )
    );
    assert_eq!(tags[1].ordinal(), 4);
    assert_eq!(tags[2].ordinal(), 5);
    assert!(tags[1].is_dev_dependency());
    assert!(tags[2].is_dev_dependency());
    assert!(
        tags.iter()
            .all(|tag| tag.location().as_str().contains("MODULE.bazel:"))
    );

    let imports = extension.proxies()[0].imports();
    assert_eq!(imports.len(), 2);
    assert_eq!(imports[0].ordinal(), 6);
    assert_eq!(imports[0].local_name().as_str(), "plain");
    assert_eq!(imports[0].exported_name().as_str(), "plain");
    assert_eq!(imports[1].ordinal(), 7);
    assert_eq!(imports[1].local_name().as_str(), "alias");
    assert_eq!(imports[1].exported_name().as_str(), "root.1.2");
    assert_eq!(imports[0].location(), imports[1].location());
}

#[test]
fn main_repository_extension_label_forms_coalesce_without_a_module_directive() {
    let file = root(
        r#"
a = use_extension("defs", "ext")
b = use_extension(":defs", "ext")
c = use_extension("//:defs", "ext")
d = use_extension("@//:defs", "ext")
"#,
    )
    .unwrap();
    let [extension] = file.extension_uses() else {
        panic!("expected one extension use")
    };
    let ExtensionUseKind::Regular { extension_file, .. } = extension.kind();
    assert_eq!(extension_file.as_str(), "//:defs");
    assert_eq!(extension.proxies().len(), 4);
}

#[test]
fn extension_proxy_export_uses_first_assignment_and_inline_proxy_stays_unnamed() {
    let file = root(
        r#"
first = use_extension("//:ext.bzl", "ext")
second = first
use_extension("//:ext.bzl", "ext").tag()
"#,
    )
    .unwrap();
    let [extension] = file.extension_uses() else {
        panic!("expected one extension use")
    };
    assert_eq!(extension.proxies().len(), 2);
    assert_eq!(
        extension.proxies()[0].exported_name().unwrap().as_str(),
        "first"
    );
    assert!(extension.proxies()[1].exported_name().is_none());
    assert_eq!(extension.tags()[0].ordinal(), 3);

    let located = root(
        "e = use_extension(\"//:ext.bzl\", \"ext\")\n\
         e.tag()\n\
         use_repo(e, \"repo\")",
    )
    .unwrap();
    let extension = &located.extension_uses()[0];
    assert_eq!(
        extension.proxies()[0].location().as_str(),
        "MODULE.bazel:1:5-39"
    );
    assert_eq!(
        extension.tags()[0].location().as_str(),
        "MODULE.bazel:2:1-8"
    );
    assert_eq!(
        extension.proxies()[0].imports()[0].location().as_str(),
        "MODULE.bazel:3:1-20"
    );
}

#[test]
fn ignored_dev_extension_is_detached_but_reserves_imports_and_event_gaps() {
    let file = root_ignoring_dev(
        r#"
dev = use_extension("//:dev.bzl", "dev", dev_dependency = True)
dev.anything(value = None)
use_repo(dev, "ignored_repo")
regular = use_extension("//:regular.bzl", "regular")
regular.tag(value = 1)
"#,
    )
    .unwrap();
    let [extension] = file.extension_uses() else {
        panic!("expected only the retained extension use")
    };
    assert_eq!(extension.first_use_ordinal(), 4);
    assert_eq!(extension.proxies()[0].ordinal(), 5);
    assert_eq!(extension.tags()[0].ordinal(), 6);

    assert_rejected(
        root_ignoring_dev(
            r#"
dev = use_extension("//:dev.bzl", "dev", dev_dependency = True)
use_repo(dev, "same")
regular = use_extension("//:regular.bzl", "regular")
use_repo(regular, "same")
"#,
        ),
        "repo name 'same'",
    );
}

#[test]
fn dependency_dev_extension_is_detached_and_ignored_tag_still_validates_kwargs_shape() {
    let file = dependency(
        r#"
module(name = "dep", version = "1.0")
dev = use_extension("//:dev.bzl", "dev", dev_dependency = True)
dev.tag(value = None)
regular = use_extension("//:regular.bzl", "regular")
"#,
        "dep",
        "1.0",
    )
    .unwrap();
    let [extension] = file.extension_uses() else {
        panic!("expected only the regular extension")
    };
    assert_eq!(extension.first_use_ordinal(), 3);
    assert_eq!(extension.proxies()[0].ordinal(), 4);

    assert_rejected(
        root_ignoring_dev(
            "dev = use_extension(\"//:dev.bzl\", \"dev\", dev_dependency = True)\n\
             dev.tag(**{1: \"value\"})",
        ),
        "not an identifier",
    );
}

#[test]
fn extension_calls_validate_isolation_call_shape_and_raw_value_graphs() {
    assert_rejected(
        root("use_extension(\"bad label\", \"not-valid\")"),
        "extension name is not a valid identifier",
    );
    assert_rejected(
        root("use_extension(\"//:ext.bzl\", \"ext\", isolate = True)"),
        "experimental isolated-extension-usages semantics",
    );
    assert_rejected(
        root("use_extension(\"//:ext.bzl\", \"ext\", isolate = False)"),
        "experimental isolated-extension-usages semantics",
    );
    assert_rejected(
        root("use_extension(\"@@bad//:label\", \"not-valid\", isolate = True)"),
        "experimental isolated-extension-usages semantics",
    );
    assert_rejected(
        root("use_extension(\"@@bad//:label\", \"not-valid\", isolate = False)"),
        "experimental isolated-extension-usages semantics",
    );
    assert_rejected(
        root("use_extension(\"@@bad//:label\", \"not-valid\", isolate = None)"),
        "experimental isolated-extension-usages semantics",
    );
    assert_rejected(
        root("use_extension(\"@@bad//:label\", \"ext\")"),
        "canonical repository labels are not accepted",
    );
    assert_rejected(
        root("e = use_extension(\"//:ext.bzl\", \"ext\")\ne.tag(1)"),
        "positional",
    );
    assert_rejected(
        root("e = use_extension(\"//:ext.bzl\", \"ext\")\ne.tag(value = None)"),
        "unsupported module extension tag attribute value",
    );
    assert_rejected(
        root("e = use_extension(\"//:ext.bzl\", \"ext\")\ne.tag(value = len)"),
        "unsupported module extension tag attribute value",
    );
    assert_rejected(
        root("e = use_extension(\"//:ext.bzl\", \"ext\")\ne.tag(value = {1: \"x\"})"),
        "dictionary key",
    );
    assert_rejected(
        root(
            "e = use_extension(\"//:ext.bzl\", \"ext\")\n\
             value = []\n\
             value.append(value)\n\
             e.tag(value = value)",
        ),
        "cyclic module extension tag attribute value",
    );

    let depth_64 = format!("{}0{}", "[".repeat(64), "]".repeat(64));
    root(&format!(
        "e = use_extension(\"//:ext.bzl\", \"ext\")\ne.tag(value = {depth_64})"
    ))
    .unwrap();
    let depth_65 = format!("{}0{}", "[".repeat(65), "]".repeat(65));
    assert_rejected(
        root(&format!(
            "e = use_extension(\"//:ext.bzl\", \"ext\")\ne.tag(value = {depth_65})"
        )),
        "nesting exceeds 64 levels",
    );

    let shared = root(
        "e = use_extension(\"//:ext.bzl\", \"ext\")\n\
         value = [1, {\"nested\": True}]\n\
         e.tag(first = value, second = value)",
    )
    .unwrap();
    assert_eq!(shared.extension_uses()[0].tags()[0].attributes().len(), 2);
}

#[test]
fn use_repo_validates_global_local_and_usage_wide_exported_collisions() {
    assert_rejected(
        root("use_repo(None, 1)"),
        "expected a module extension proxy, got NoneType",
    );
    assert_rejected(
        root("e = use_extension(\"//:ext.bzl\", \"ext\")\nuse_repo(e, 1)"),
        "repository name got value of type 'int', want 'string'",
    );
    assert_rejected(
        root(
            "e = use_extension(\"//:ext.bzl\", \"ext\")\n\
             use_repo(e, \"same\")\n\
             use_repo(e, \"same\")",
        ),
        "repo name 'same'",
    );
    assert_rejected(
        root(
            "e = use_extension(\"//:ext.bzl\", \"ext\")\n\
             use_repo(e, one = \"exported\")\n\
             use_repo(e, two = \"exported\")",
        ),
        "exported as 'exported' by module extension 'ext' is already imported",
    );
    assert_rejected(
        root(
            "one = use_extension(\"//:ext.bzl\", \"ext\")\n\
             two = use_extension(\"//:ext.bzl\", \"ext\")\n\
             use_repo(one, local_one = \"exported\")\n\
             use_repo(two, local_two = \"exported\")",
        ),
        "already imported",
    );
    assert_rejected(
        root_ignoring_dev(
            "dev = use_extension(\"//:dev.bzl\", \"dev_ext\", dev_dependency = True)\n\
             use_repo(dev, one = \"exported\")\n\
             use_repo(dev, two = \"exported\")",
        ),
        "exported as 'exported' by module extension 'dev_ext' is already imported",
    );
    assert_rejected(
        root(
            "e = use_extension(\"//:ext.bzl\", \"ext\")\n\
             use_repo(e, \"{name}\")",
        ),
        "invalid user-provided repo name '{name}'",
    );

    for source in [
        "module(name = \"root\", repo_name = \"same\")\n\
         e = use_extension(\"//:ext.bzl\", \"ext\")\n\
         use_repo(e, \"same\")",
        "bazel_dep(name = \"dep\", repo_name = \"same\")\n\
         e = use_extension(\"//:ext.bzl\", \"ext\")\n\
         use_repo(e, \"same\")",
        "one = use_extension(\"//:one.bzl\", \"ext\")\n\
         two = use_extension(\"//:two.bzl\", \"ext\")\n\
         use_repo(one, \"same\")\n\
         use_repo(two, \"same\")",
    ] {
        assert_rejected(root(source), "repo name 'same'");
    }
}

#[test]
fn evaluates_pinned_bazel_root_override_shapes_in_order() {
    // Compactly preserves the override shapes used by Bazel d99d82's root
    // MODULE.bazel: empty and pinned versions, ordered patch vectors, and one
    // opaque local path.
    let file = root(
        r#"
module(name = "bazel", version = "9.0.0")
single_version_override(module_name = "grpc", patches = ["//third_party/grpc:grpc.patch"], patch_strip = 1)
single_version_override(module_name = "c-ares", version = "1.34.5")
single_version_override(module_name = "rules_jvm_external", version = "6.8", patches = ["//third_party:jvm-1.patch", "//third_party:jvm-2.patch"], patch_strip = 1)
single_version_override(module_name = "rules_graalvm", patches = ["//:g1.patch", "//:g2.patch", "//:g3.patch", "//:g4.patch", "//:g5.patch"], patch_strip = 1)
single_version_override(module_name = "googleapis", version = "1.0", patches = ["//:googleapis.patch"], patch_strip = 1)
single_version_override(module_name = "protobuf", version = "35.1", patches = ["//:protobuf.patch"], patch_strip = 1)
single_version_override(module_name = "grpc-java", patches = ["//:j1.patch", "//:j2.patch", "//:j3.patch", "//:j4.patch"], patch_strip = 1)
local_path_override(module_name = "remoteapis", path = "./third_party/remoteapis")
"#,
    )
    .unwrap();

    assert_eq!(file.overrides().len(), 8);
    let names: Vec<_> = file
        .overrides()
        .iter()
        .map(|value| value.module_name().as_str())
        .collect();
    assert_eq!(
        names,
        [
            "grpc",
            "c-ares",
            "rules_jvm_external",
            "rules_graalvm",
            "googleapis",
            "protobuf",
            "grpc-java",
            "remoteapis",
        ]
    );
    let grpc = file.overrides()[0].as_single_version().unwrap();
    assert!(grpc.version().is_empty());
    assert_eq!(grpc.patches()[0].as_str(), "//third_party/grpc:grpc.patch");
    assert_eq!(grpc.patch_strip(), 1);
    assert_eq!(
        file.overrides()[7].as_local_path().unwrap().path(),
        "./third_party/remoteapis"
    );
}

#[test]
fn override_values_preserve_opaque_fields_and_normalize_patch_labels() {
    let file = root(
        r#"
module(name = "root", repo_name = "self")
single_version_override(
    module_name = "dep",
    registry = "opaque registry value",
    patches = ["//:root.patch", ":relative.patch", "@self//pkg:self.patch", "@@canonical+repo//other:canonical.patch"],
    patch_cmds = ("printf one", "printf two"),
    patch_strip = -7,
)
local_path_override(module_name = "local", path = "")
local_path_override(module_name = "relative", path = "../somewhere")
local_path_override(module_name = "absolute", path = "/somewhere")
"#,
    )
    .unwrap();

    let single = file.overrides()[0].as_single_version().unwrap();
    assert!(single.version().is_empty());
    assert_eq!(single.registry(), "opaque registry value");
    assert_eq!(
        single
            .patches()
            .iter()
            .map(|label| label.as_str())
            .collect::<Vec<_>>(),
        [
            "//:root.patch",
            "//:relative.patch",
            "//pkg:self.patch",
            "@@canonical+repo//other:canonical.patch",
        ]
    );
    assert_eq!(
        single
            .patch_cmds()
            .iter()
            .map(|command| command.as_ref())
            .collect::<Vec<_>>(),
        ["printf one", "printf two"]
    );
    assert_eq!(single.patch_strip(), -7);
    assert_eq!(file.overrides()[1].as_local_path().unwrap().path(), "");
    assert_eq!(
        file.overrides()[2].as_local_path().unwrap().path(),
        "../somewhere"
    );
    assert_eq!(
        file.overrides()[3].as_local_path().unwrap().path(),
        "/somewhere"
    );
}

#[test]
fn override_arguments_are_named_only_and_fully_validated() {
    assert_rejected(
        root("single_version_override(\"dep\")"),
        "Missing named-only parameter `module_name`",
    );
    assert_rejected(
        root("local_path_override(\"dep\", \"../dep\")"),
        "Missing named-only parameter `module_name`",
    );
    assert_rejected(
        root("single_version_override(module_name = \"bad.\")"),
        "invalid module name 'bad.'",
    );
    assert_rejected(
        root("single_version_override(module_name = \"dep\", version = \"1..0\")"),
        "Invalid version in single_version_override()",
    );
    assert_rejected(
        root("single_version_override(module_name = \"dep\", patches = {\"//:p\": True})"),
        "for patches, got dict, want sequence",
    );
    assert_rejected(
        root("single_version_override(module_name = \"dep\", patch_cmds = [\"ok\", 1])"),
        "at index 1 of patch_cmds, got element of type int, want string",
    );
    assert_rejected(
        root("single_version_override(module_name = \"dep\", patch_strip = 2147483648)"),
        "want value in signed 32-bit range",
    );
    assert_rejected(
        root(
            "single_version_override(module_name = \"dep\", patches = [\"@unknown_repo//:p.patch\"] )",
        ),
        "only patches in the main repository can be applied",
    );
    assert_rejected(
        root("single_version_override(module_name = \"dep\", patches = [\"//pkg/../bad.patch\"] )"),
        "invalid label",
    );
    assert_rejected(
        root(
            "single_version_override(module_name = \"dep\", \
             patches = [\"pkg:relative.patch\"], patch_cmds = [1], \
             patch_strip = 2147483648)",
        ),
        "invalid label",
    );
}

#[test]
fn patch_label_parser_matches_pinned_bazel_forms_and_rejects_malformed_inputs() {
    let file = root(
        r#"
module(name = "root", repo_name = "self")
single_version_override(
    module_name = "dep",
    patches = [
        "foo/target.patch",
        "@self",
        "@@canonical+repo",
        "@@canonical+repo//pkg:unicode-π.patch",
        "//pkg:legacy/.",
    ],
)
"#,
    )
    .unwrap();
    assert_eq!(
        file.overrides()[0]
            .as_single_version()
            .unwrap()
            .patches()
            .iter()
            .map(|label| label.as_str())
            .collect::<Vec<_>>(),
        [
            "//:foo/target.patch",
            "//:self",
            "@@canonical+repo//:canonical+repo",
            "@@canonical+repo//pkg:unicode-π.patch",
            "//pkg:legacy",
        ]
    );

    for malformed in [
        "@@bad repo//:p.patch",
        "@@bad~repo//:p.patch",
        "foo/bar:relative.patch",
        "//pkg:bad:target",
        "///pkg:target",
        "//pkg/:target",
        "//pkg//nested:target",
        "//.../pkg:target",
        "//pkg:/target",
        "//pkg:target/",
        "//pkg:../target",
        "//pkg:target/../bad",
        "//pkg:target//bad",
        "//pkg:bad\\target",
    ] {
        assert_rejected(
            root(&format!(
                "single_version_override(module_name = \"dep\", patches = [{malformed:?}])"
            )),
            "invalid label",
        );
    }
}

#[test]
fn ignored_override_contexts_validate_but_do_not_store_or_deduplicate() {
    let ignored = root_ignoring_dev(
        "single_version_override(module_name = \"dep\")\n\
         local_path_override(module_name = \"dep\", path = \"one\")\n\
         local_path_override(module_name = \"dep\", path = \"two\")",
    )
    .unwrap();
    assert!(ignored.overrides().is_empty());

    let dependency_file = dependency(
        "module(name = \"aaa\", version = \"1.0\")\n\
         single_version_override(module_name = \"dep\")\n\
         local_path_override(module_name = \"dep\", path = \"one\")",
        "aaa",
        "1.0",
    )
    .unwrap();
    assert!(dependency_file.overrides().is_empty());

    assert_rejected(
        root_ignoring_dev("single_version_override(module_name = \"bad.\")"),
        "invalid module name 'bad.'",
    );
    assert_rejected(
        dependency(
            "module(name = \"aaa\", version = \"1.0\")\n\
             single_version_override(module_name = \"dep\", patches = [\"@unknown//:p\"])",
            "aaa",
            "1.0",
        ),
        "only patches in the main repository can be applied",
    );
}

#[test]
fn override_duplicates_are_cross_kind_and_override_can_coexist_with_dep() {
    assert_rejected(
        root(
            "single_version_override(module_name = \"dep\")\n\
             local_path_override(module_name = \"dep\", path = \"../dep\")",
        ),
        "multiple overrides for dep dep found",
    );
    let file = root(
        "bazel_dep(name = \"dep\", version = \"1.0\")\n\
         single_version_override(module_name = \"dep\", version = \"1.0\")",
    )
    .unwrap();
    assert_eq!(file.dependencies().len(), 1);
    assert_eq!(file.overrides().len(), 1);
    assert!(matches!(
        file.overrides()[0],
        ModuleOverride::SingleVersion(_)
    ));
}

#[test]
fn root_finalization_matches_lower_pin_alias_and_self_override_quirks() {
    assert_rejected(
        root(
            "module(name = \"root\")\n\
             bazel_dep(name = \"dep\", version = \"2.0\")\n\
             single_version_override(module_name = \"dep\", version = \"1.0\")",
        ),
        "lower than the version '2.0' requested by the root module",
    );

    let unpinned = root(
        "bazel_dep(name = \"dep\", version = \"2.0\")\n\
         single_version_override(module_name = \"dep\", patches = [\"//:dep.patch\"])",
    )
    .unwrap();
    assert!(
        unpinned.overrides()[0]
            .as_single_version()
            .unwrap()
            .version()
            .is_empty()
    );

    // Bazel's root check indexes deps by apparent repo name. An explicit
    // different alias and nodep therefore skip this lower-bound check.
    let aliased = root(
        "bazel_dep(name = \"dep\", version = \"2.0\", repo_name = \"alias\")\n\
         single_version_override(module_name = \"dep\", version = \"1.0\")",
    )
    .unwrap();
    assert_eq!(aliased.overrides().len(), 1);
    let nodep = root(
        "bazel_dep(name = \"dep\", version = \"2.0\", repo_name = None)\n\
         single_version_override(module_name = \"dep\", version = \"1.0\")",
    )
    .unwrap();
    assert_eq!(nodep.overrides().len(), 1);

    assert_rejected(
        root(
            "module(name = \"root\")\n\
             local_path_override(module_name = \"root\", path = \".\")",
        ),
        "invalid override for the root module found",
    );
}

#[test]
fn unsupported_override_globals_fail_stably_in_all_contexts() {
    for directive in [
        "multiple_version_override(module_name = \"aaa\", versions = [\"1.0\"])",
        "archive_override(module_name = \"aaa\", urls = [])",
        "git_override(module_name = \"aaa\", remote = \"https://example.invalid\", commit = \"abc\")",
    ] {
        let name = directive.split_once('(').unwrap().0;
        assert_rejected(
            root_ignoring_dev(directive),
            &format!("{name}() is not supported by the Buck2 Bazel MODULE dialect yet"),
        );
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
