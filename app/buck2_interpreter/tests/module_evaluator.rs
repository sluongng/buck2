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
use buck2_bzlmod::RepoRuleInvocationEvaluationProjection;
use buck2_bzlmod::Version;
use buck2_interpreter::module_evaluator::ModuleFileEvalKind;
use buck2_interpreter::module_evaluator::ModuleFileEvaluation;
use buck2_interpreter::module_evaluator::evaluate_module_file;
use buck2_interpreter::module_evaluator::module_file_global_names_for_audit;
use serde_json::Value as JsonValue;
use sha2::Digest;
use sha2::Sha256;

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

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn source_line(location: &str) -> usize {
    location.split(':').nth(1).unwrap().parse().unwrap()
}

fn pinned_repo_rule_json(value: &RawAttributeValue) -> JsonValue {
    match value {
        RawAttributeValue::String(value) => JsonValue::String(value.to_string()),
        RawAttributeValue::Bool(value) => JsonValue::Bool(*value),
        RawAttributeValue::Sequence(values) => {
            JsonValue::Array(values.iter().map(pinned_repo_rule_json).collect())
        }
        RawAttributeValue::Integer(_) | RawAttributeValue::Dict(_) => {
            panic!("pinned repository-rule fixture only contains string, bool, and sequence values")
        }
    }
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
fn evaluates_digest_pinned_bazel_root_extensions_through_the_registration_gate() {
    const COMMIT: &str = "d99d82e5d21c2795a6b489342886c336ec05509a";
    const SOURCE: &str =
        include_str!("../testcases/bazel/d99d82e5d21c2795a6b489342886c336ec05509a/MODULE.bazel");
    const REPO_RULE_GOLDEN: &[u8] = include_bytes!(
        "../testcases/bazel/d99d82e5d21c2795a6b489342886c336ec05509a/repo_rules.json"
    );
    const COMBINED_GOLDEN: &[u8] =
        include_bytes!("../testcases/bazel/d99d82e5d21c2795a6b489342886c336ec05509a/combined.json");

    assert_eq!(SOURCE.len(), 19_810);
    assert_eq!(
        sha256(SOURCE.as_bytes()),
        "fe7dbb51e6164641a989ce9130071010799a4944e69615efde182323a2a6f32d"
    );
    assert_eq!(REPO_RULE_GOLDEN.len(), 1_843);
    assert_eq!(
        sha256(REPO_RULE_GOLDEN),
        "1a34874fa85e3985ab9960625c94dec3c9de5901904cf7fa709113ee7478c2c0"
    );
    assert_eq!(COMBINED_GOLDEN.len(), 11_514);
    assert_eq!(
        sha256(COMBINED_GOLDEN),
        "f6000910fba4aba112d9734b34d04f9511c9239835731503d27b7c715dd51fcd"
    );
    let combined_golden = std::str::from_utf8(COMBINED_GOLDEN).unwrap();
    assert!(combined_golden.starts_with(&format!(
        "[\"pinned_root_extension_golden_v1\",\"{COMMIT}\""
    )));
    // The combined source manifest additionally retains non-semantic binding
    // and positional-call syntax that the pure model intentionally omits.
    // Its digest pins that full source oracle; semantic runtime comparison is
    // exhaustive for repository rules below and inventory-based for regular
    // extensions.

    // The full unchanged source gets through every regular and innate
    // extension call. Registration directives are a principled later-phase
    // boundary and remain at their original source location.
    let full_error = root(SOURCE).unwrap_err().to_string();
    assert!(full_error.contains("register_execution_platforms"));
    assert!(full_error.contains("MODULE.bazel:505"), "{full_error}");

    let registration = "register_execution_platforms(\"//:default_host_platform\")";
    let (extension_section, _) = SOURCE.split_once(registration).unwrap();
    let file = root(extension_section).unwrap();
    assert_eq!(file.extension_uses().len(), 11);
    assert_eq!(
        file.extension_uses()
            .iter()
            .map(|use_value| use_value.tags().len())
            .sum::<usize>(),
        20
    );
    assert_eq!(
        file.extension_uses()
            .iter()
            .flat_map(|use_value| use_value.proxies())
            .flat_map(|proxy| proxy.imports())
            .map(|import| import.location().as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        10
    );
    assert_eq!(
        file.extension_uses()
            .iter()
            .flat_map(|use_value| use_value.proxies())
            .map(|proxy| proxy.imports().len())
            .sum::<usize>(),
        55
    );

    assert_eq!(file.repo_rule_uses().len(), 4);
    assert_eq!(
        file.repo_rule_uses()
            .iter()
            .map(|use_value| use_value.invocations().len())
            .sum::<usize>(),
        9
    );

    let repo_rule_golden: JsonValue = serde_json::from_slice(REPO_RULE_GOLDEN).unwrap();
    let golden_groups = repo_rule_golden.as_array().unwrap();
    let expected_factory_lines = [403_u64, 482, 486, 491];
    let expected_bindings = [
        "http_file",
        "list_source_repository",
        "winsdk_configure",
        "local_repository",
    ];
    assert_eq!(golden_groups.len(), file.repo_rule_uses().len());
    for (index, (golden_group, use_value)) in
        golden_groups.iter().zip(file.repo_rule_uses()).enumerate()
    {
        let golden_group = golden_group.as_array().unwrap();
        assert_eq!(golden_group[0].as_str(), Some("repo_rule"));
        assert_eq!(
            golden_group[1].as_u64(),
            Some(expected_factory_lines[index])
        );
        assert_eq!(golden_group[2].as_str(), Some(expected_bindings[index]));
        assert_eq!(
            golden_group[3].as_str(),
            Some(use_value.id().bzl_file().as_str())
        );
        assert_eq!(
            golden_group[4].as_str(),
            Some(use_value.id().rule_name().as_str())
        );

        let golden_calls = golden_group[5].as_array().unwrap();
        assert_eq!(golden_calls.len(), use_value.invocations().len());
        for (golden_call, invocation) in golden_calls.iter().zip(use_value.invocations()) {
            let golden_call = golden_call.as_array().unwrap();
            assert_eq!(golden_call[0].as_str(), Some("call"));
            assert_eq!(
                golden_call[1].as_u64(),
                Some(source_line(invocation.location().as_str()) as u64)
            );

            let golden_attributes = golden_call[2].as_array().unwrap();
            let structural_name = golden_attributes[0].as_array().unwrap();
            assert_eq!(structural_name[0].as_str(), Some("name"));
            assert_eq!(
                structural_name[1].as_str(),
                Some(invocation.repository_name().as_str())
            );
            assert_eq!(golden_attributes.len(), invocation.attributes().len() + 1);
            for (golden_attribute, attribute) in
                golden_attributes[1..].iter().zip(invocation.attributes())
            {
                let golden_attribute = golden_attribute.as_array().unwrap();
                assert_eq!(golden_attribute[0].as_str(), Some(attribute.name()));
                assert_eq!(
                    golden_attribute[1],
                    pinned_repo_rule_json(attribute.value())
                );
            }
        }
    }

    assert_eq!(
        file.repo_rule_uses()
            .iter()
            .map(|use_value| (
                use_value.id().bzl_file().as_str(),
                use_value.id().rule_name().as_str(),
                use_value.invocations().len()
            ))
            .collect::<Vec<_>>(),
        [
            (
                "@bazel_tools//tools/build_defs/repo:http.bzl",
                "http_file",
                6
            ),
            (
                "//src/test/shell/bazel:list_source_repository.bzl",
                "list_source_repository",
                1
            ),
            ("//tools/res:winsdk_configure.bzl", "winsdk_configure", 1),
            (
                "@bazel_tools//tools/build_defs/repo:local.bzl",
                "local_repository",
                1
            ),
        ]
    );
    assert_eq!(
        file.repo_rule_uses()
            .iter()
            .flat_map(|use_value| use_value.invocations())
            .map(|invocation| source_line(invocation.location().as_str()))
            .collect::<Vec<_>>(),
        [406, 413, 420, 427, 434, 441, 484, 488, 493]
    );
    assert_eq!(
        file.repo_rule_uses()[0]
            .invocations()
            .iter()
            .map(|invocation| invocation.repository_name().as_str())
            .collect::<Vec<_>>(),
        [
            "jq_linux_amd64",
            "jq_linux_arm64",
            "jq_linux_s390x",
            "jq_macos_amd64",
            "jq_macos_arm64",
            "jq_windows_amd64",
        ]
    );
    assert!(
        file.repo_rule_uses()
            .iter()
            .flat_map(|use_value| use_value.invocations())
            .all(|invocation| !invocation.is_dev_dependency())
    );
    let first = &file.repo_rule_uses()[0].invocations()[0];
    assert_eq!(
        first
            .attributes()
            .iter()
            .map(|attribute| attribute.name())
            .collect::<Vec<_>>(),
        ["executable", "integrity", "urls"]
    );
    let last = &file.repo_rule_uses()[3].invocations()[0];
    assert_eq!(last.repository_name().as_str(), "kythe_release");
    assert_eq!(last.attributes()[0].name(), "path");
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
            "use_repo_rule",
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
        "register_toolchains(\"//:toolchain\")",
        "register_execution_platforms(\"//:platform\")",
    ] {
        let name = directive.split_once('(').unwrap().0;
        assert_rejected(root(directive), &format!("Variable `{name}` not found"));
    }
}

#[test]
fn repo_rule_proxies_are_binding_agnostic_and_aggregate_by_structural_owner_identity() {
    let file = root(
        r#"
assigned = use_repo_rule("opaque bzl", "_private rule")
assigned(name = "one", value = 1)
alias = assigned
alias(name = "two", value = ["x", True])
use_repo_rule("opaque bzl", "_private rule")(name = "three")
unused = use_repo_rule("//:unused.bzl", "unused")
left = use_repo_rule("a b", "c")
right = use_repo_rule("a", "b c")
left(name = "left")
right(name = "right")
"#,
    )
    .unwrap();

    assert_eq!(file.repo_rule_uses().len(), 3);
    let aggregate = &file.repo_rule_uses()[0];
    assert_eq!(aggregate.id().owner(), &ModuleKey::ROOT);
    assert_eq!(aggregate.id().bzl_file().as_str(), "opaque bzl");
    assert_eq!(aggregate.id().rule_name().as_str(), "_private rule");
    assert_eq!(aggregate.first_use_ordinal(), 0);
    assert_eq!(
        aggregate
            .invocations()
            .iter()
            .map(|value| (value.ordinal(), value.repository_name().as_str()))
            .collect::<Vec<_>>(),
        [(1, "one"), (2, "two"), (4, "three")]
    );
    assert_eq!(
        aggregate.invocations()[0].location().as_str(),
        "MODULE.bazel:3:1-34"
    );
    assert_eq!(
        aggregate.invocations()[1].location().as_str(),
        "MODULE.bazel:5:1-41"
    );
    assert_eq!(
        aggregate.invocations()[2].location().as_str(),
        "MODULE.bazel:6:1-61"
    );

    // Keep the raw tuple structurally typed instead of reproducing Bazel's
    // unchecked space-delimited identity collision for invalid spellings.
    assert_eq!(file.repo_rule_uses()[1].id().bzl_file().as_str(), "a b");
    assert_eq!(file.repo_rule_uses()[1].id().rule_name().as_str(), "c");
    assert_eq!(file.repo_rule_uses()[2].id().bzl_file().as_str(), "a");
    assert_eq!(file.repo_rule_uses()[2].id().rule_name().as_str(), "b c");
}

#[test]
fn repo_rule_identity_is_scoped_to_the_evaluated_module_owner() {
    let root_file = root("r = use_repo_rule(\"\", \"\")\nr(name = \"root_repo\")").unwrap();
    let dependency_file = dependency(
        "module(name = \"dep\", version = \"1.0\")\n\
         r = use_repo_rule(\"\", \"\")\n\
         r(name = \"dep_repo\")",
        "dep",
        "1.0",
    )
    .unwrap();
    let root_id = root_file.repo_rule_uses()[0].id();
    let dependency_id = dependency_file.repo_rule_uses()[0].id();
    assert_eq!(root_id.bzl_file().as_str(), "");
    assert_eq!(root_id.rule_name().as_str(), "");
    assert_ne!(root_id, dependency_id);
    assert_eq!(root_id.owner(), &ModuleKey::ROOT);
    assert_eq!(dependency_id.owner().to_string(), "dep@1.0");
}

#[test]
fn repo_rule_calls_derive_fixed_tag_and_import_projections_from_raw_arguments() {
    let file = root(
        r#"
r = use_repo_rule("//:repo.bzl", "rule")
r(name = "z_dev", dev_dependency = True, first = 1)
r(name = "z", sequence = ["x", (True, False)], mapping = {"k": 2})
r(name = "a_dev", dev_dependency = True)
r(name = "a", huge = 123456789012345678901234567890)
"#,
    )
    .unwrap();
    let [use_value] = file.repo_rule_uses() else {
        panic!("expected one repository rule use")
    };
    assert_eq!(use_value.invocations().len(), 4);
    assert!(use_value.invocations()[0].is_dev_dependency());
    assert_eq!(use_value.invocations()[0].attributes()[0].name(), "first");
    assert_eq!(use_value.invocations()[1].attributes().len(), 2);
    assert_eq!(
        use_value.invocations()[3].attributes()[0].value(),
        &RawAttributeValue::Integer(
            buck2_bzlmod::RawInteger::parse_decimal("123456789012345678901234567890").unwrap()
        )
    );

    let evaluation = use_value.evaluation_projection();
    assert_eq!(RepoRuleInvocationEvaluationProjection::TAG_NAME, "repo");
    assert_eq!(
        evaluation.invocations()[0]
            .attributes()
            .last()
            .unwrap()
            .name(),
        "name"
    );
    assert_eq!(
        evaluation.invocations()[0].repository_name().as_str(),
        "z_dev"
    );
    let mapping = use_value.repository_mapping_projection();
    assert_eq!(
        mapping
            .regular_imports()
            .iter()
            .map(|value| (value.local_name().as_str(), value.exported_name().as_str()))
            .collect::<Vec<_>>(),
        [("a", "a"), ("z", "z")]
    );
    assert_eq!(
        mapping
            .dev_imports()
            .iter()
            .map(|value| value.local_name().as_str())
            .collect::<Vec<_>>(),
        ["a_dev", "z_dev"]
    );
}

#[test]
fn ignored_repo_rule_dev_calls_validate_name_but_only_leave_event_gaps() {
    let file = root_ignoring_dev(
        r#"
r = use_repo_rule("not a label", "not a rule")
r(name = "ignored", dev_dependency = True, unsupported = len)
r(name = "kept", value = 1)
"#,
    )
    .unwrap();
    let [use_value] = file.repo_rule_uses() else {
        panic!("expected retained repository rule use")
    };
    assert_eq!(use_value.first_use_ordinal(), 0);
    assert_eq!(use_value.invocations()[0].ordinal(), 2);
    assert_eq!(
        use_value.invocations()[0].repository_name().as_str(),
        "kept"
    );

    assert_rejected(
        root_ignoring_dev(
            "r = use_repo_rule(\"bad\", \"bad\")\n\
             r(name = \"_invalid\", dev_dependency = True, unsupported = len)",
        ),
        "invalid user-provided repo name '_invalid'",
    );
    assert_rejected(
        root(
            "r = use_repo_rule(\"bad\", \"bad\")\n\
             r(name = \"regular\", unsupported = len)",
        ),
        "unsupported module extension tag attribute value",
    );

    let dependency = dependency(
        r#"
module(name = "dep", version = "1.0")
r = use_repo_rule("bad", "bad")
r(name = "ignored", dev_dependency = True, unsupported = len)
"#,
        "dep",
        "1.0",
    )
    .unwrap();
    assert!(dependency.repo_rule_uses().is_empty());
}

#[test]
fn repo_rule_call_shape_and_global_name_collisions_are_restricted() {
    root(
        "r = use_repo_rule(repo_rule_bzl_file = \"x\", repo_rule_name = \"r\")\n\
         r(name = \"named_factory\")",
    )
    .unwrap();
    for (source, expected) in [
        (
            "r = use_repo_rule(\"//:r.bzl\", \"r\")\nr()",
            "requires named-only parameter 'name'",
        ),
        (
            "r = use_repo_rule(\"//:r.bzl\", \"r\")\nr(\"x\")",
            "positional",
        ),
        (
            "r = use_repo_rule(\"//:r.bzl\", \"r\")\nr(name = 1)",
            "parameter 'name' got value of type 'int'",
        ),
        (
            "r = use_repo_rule(\"//:r.bzl\", \"r\")\nr(dev_dependency = 1)",
            "parameter 'dev_dependency' got value of type 'int'",
        ),
        (
            "r = use_repo_rule(\"//:r.bzl\", \"r\")\nr(\"x\", name = 1)",
            "parameter 'name' got value of type 'int'",
        ),
    ] {
        assert_rejected(root(source), expected);
    }

    for source in [
        "module(name = \"same\")\nr = use_repo_rule(\"x\", \"r\")\nr(name = \"same\")",
        "bazel_dep(name = \"dep\", repo_name = \"same\")\nr = use_repo_rule(\"x\", \"r\")\nr(name = \"same\")",
        "e = use_extension(\"//:e.bzl\", \"e\")\nuse_repo(e, \"same\")\nr = use_repo_rule(\"x\", \"r\")\nr(name = \"same\")",
        "one = use_repo_rule(\"x\", \"r\")\ntwo = use_repo_rule(\"y\", \"r\")\none(name = \"same\")\ntwo(name = \"same\")",
    ] {
        assert_rejected(root(source), "repo name 'same'");
    }

    assert_rejected(
        root(
            "bazel_dep(name = \"dep\", repo_name = \"same\")\n\
             r = use_repo_rule(\"x\", \"r\")\n\
             r(name = \"same\", unsupported = len)",
        ),
        "repo name 'same'",
    );

    // An ignored invocation neither reserves its valid name nor converts its
    // unsupported attributes.
    root_ignoring_dev(
        "r = use_repo_rule(\"x\", \"r\")\n\
         r(name = \"same\", dev_dependency = True, unsupported = len)\n\
         bazel_dep(name = \"dep\", repo_name = \"same\")",
    )
    .unwrap();
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
