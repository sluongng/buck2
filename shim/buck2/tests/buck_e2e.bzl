# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@shim//build_defs/lib:oss.bzl", "translate_target")

def _fix_label(label):
    return translate_target(label)

def _fix_labels(labels):
    return [_fix_label(label) for label in (labels or [])]

def _fix_named_set(values):
    if values == None:
        return []
    if type(values) == type({}):
        return {_fix_label(src): dest for src, dest in values.items()}
    return [_fix_label(value) for value in values]

def _fix_env_value(value):
    if type(value) != type(""):
        return value
    return value.replace("fbcode//buck2/", "root//").replace("//buck2/", "root//")

def _fix_env(env):
    return {key: _fix_env_value(value) for key, value in (env or {}).items()}

def _buck2_executable(use_compiled_buck2_client_and_tpx = False):
    _unused = use_compiled_buck2_client_and_tpx  # @unused
    return "$(location root//app/buck2:buck2-bin)"

def _base_module_for_package():
    package = native.package_name()
    if package.startswith("tests/"):
        return "buck2." + package.replace("/", ".")
    return None

def buck_e2e_test(
        name,
        executable,
        use_buck_api = True,
        contacts = None,
        base_module = None,
        data = None,
        data_dir = None,
        srcs = None,
        labels = None,
        deps = None,
        env = None,
        resources = None,
        skip_for_os = (),
        pytest_config = None,
        pytest_marks = None,
        pytest_expr = None,
        pytest_confcutdir = None,
        serialize_test_cases = None,
        require_nano_prelude = None,
        cfg_modifiers = None,
        ci_srcs = [],
        ci_deps = [],
        compatible_with = None,
        **kwargs):
    _unused = (
        pytest_config,
        pytest_marks,
        pytest_expr,
        pytest_confcutdir,
        cfg_modifiers,
        ci_srcs,
        ci_deps,
        compatible_with,
        kwargs,
    )  # @unused

    env = _fix_env(env)
    env["RUST_BACKTRACE"] = "1"
    env["RUST_LIB_BACKTRACE"] = "0"
    env["TEST_EXECUTABLE"] = executable
    env["PYTEST_ADDOPTS"] = "-vv --tb=native --no-header --no-summary"
    env.setdefault("BUCK2_RUNTIME_THREADS", "8")
    env.setdefault("BUCK2_MAX_BLOCKING_THREADS", "8")
    env.setdefault(
        "BUCK2_E2E_TEST_FLAVOR",
        "isolated" if native.package_name().startswith("tests/core/") else "any",
    )

    if serialize_test_cases:
        labels = list(labels or []) + ["serialize_test_cases"]

    if data and data_dir:
        fail("`data` and `data_dir` cannot be used together")

    if data_dir:
        native.filegroup(
            name = data_dir,
            srcs = [data_dir],
            visibility = ["PUBLIC"],
        )
        env["TEST_REPO_DATA"] = "$(location :{d})/{d}".format(d = data_dir)

    if data:
        env["TEST_REPO_DATA"] = "$(location {})".format(_fix_label(data))

    if require_nano_prelude == None:
        require_nano_prelude = data_dir != None
    if require_nano_prelude:
        env["NANO_PRELUDE"] = "$(location root//tests/e2e_util/nano_prelude:nano_prelude)"

    deps = _fix_labels(deps)
    deps.extend([
        "@shim//buck2/tests:oss_pytest_runner",
        "root//tests/e2e_util:utilities",
    ])
    if use_buck_api:
        deps.append("root//tests/e2e_util/api:api")

    resources = _fix_named_set(resources)
    if type(resources) == type({}):
        resources.setdefault("root//tests/e2e_util:conftest.py", "conftest.py")
    else:
        resources.append("root//tests/e2e_util:conftest.py")

    if base_module == None:
        base_module = _base_module_for_package()

    native.python_test(
        name = name,
        base_module = base_module,
        contacts = contacts or [],
        deps = deps,
        env = env,
        labels = list(labels or []),
        main_module = "buck2.tests.oss_pytest_runner",
        manifest_module_entries = {
            "fbmake": {
                "build_rule": "{}//{}:{}".format(native.get_cell_name(), native.package_name(), name),
            },
        },
        resources = resources,
        srcs = _fix_named_set(srcs),
        visibility = ["PUBLIC"],
    )

def buck2_e2e_test(
        name,
        test_with_compiled_buck2 = True,
        test_with_deployed_buck2 = False,
        test_with_reverted_buck2 = False,
        use_compiled_buck2_client_and_tpx = False,
        skip_deployed_buck2_version_dep = False,
        deps = (),
        env = None,
        skip_for_os = (),
        use_buck_api = True,
        contacts = None,
        base_module = None,
        data = None,
        data_dir = None,
        srcs = (),
        labels = (),
        resources = None,
        pytest_config = None,
        pytest_marks = None,
        pytest_expr = None,
        pytest_confcutdir = None,
        serialize_test_cases = None,
        require_nano_prelude = None,
        ci_srcs = [],
        ci_deps = [],
        compatible_with = None,
        **kwargs):
    _unused = kwargs  # @unused
    common = {
        "base_module": base_module,
        "ci_deps": ci_deps,
        "ci_srcs": ci_srcs,
        "compatible_with": compatible_with,
        "contacts": contacts,
        "data": data,
        "data_dir": data_dir,
        "labels": labels,
        "pytest_confcutdir": pytest_confcutdir,
        "pytest_config": pytest_config,
        "pytest_expr": pytest_expr,
        "pytest_marks": pytest_marks,
        "require_nano_prelude": require_nano_prelude,
        "resources": resources,
        "serialize_test_cases": serialize_test_cases,
        "skip_for_os": skip_for_os,
        "srcs": srcs,
        "use_buck_api": use_buck_api,
    }

    if not test_with_compiled_buck2 and not test_with_deployed_buck2:
        fail("Must set one of `test_with_compiled_buck2` or `test_with_deployed_buck2` for " + name)

    if test_with_compiled_buck2:
        compiled_env = dict(env or {})
        compiled_env["BUCK2_HARD_ERROR"] = "true"
        buck_e2e_test(
            name = name + ("_with_compiled_buck2" if test_with_deployed_buck2 else ""),
            deps = deps,
            env = compiled_env,
            executable = _buck2_executable(use_compiled_buck2_client_and_tpx),
            **common
        )

    if test_with_deployed_buck2:
        deployed_env = dict(env or {})
        deployed_env["BUCK2_HARD_ERROR"] = "false"
        deployed_deps = list(deps or [])
        _unused = skip_deployed_buck2_version_dep  # @unused
        buck_e2e_test(
            name = name,
            deps = deployed_deps,
            env = deployed_env,
            executable = "buck2",
            **common
        )

    if test_with_reverted_buck2:
        previous_env = dict(env or {})
        previous_env["BUCK2_CHANNEL"] = "previous"
        previous_env["BUCK2_HARD_ERROR"] = "false"
        buck_e2e_test(
            name = name + "_with_reverted_buck2",
            deps = deps,
            env = previous_env,
            executable = "buck2",
            **common
        )

def buck2_core_tests(extra_attrs = {}, target_extra_attrs = {}):
    items = set([item.split("/")[0] for item in glob(["**/*", "**/.*"])])
    generated_targets = []

    for item in list(items):
        if item in ["TARGETS", "TARGETS.v2", "BUCK", "BUCK.v2"]:
            continue
        if item.startswith("test_") and item.endswith("_data"):
            if item[:-5] + ".py" not in items:
                fail("Test data directory {} exists but has no matching test!".format(item))
            continue
        if item.startswith("test_") and item.endswith(".py"):
            target = item[:-3]
            generated_targets.append(target)

            attrs = dict(extra_attrs)
            attrs.update(target_extra_attrs.get(target) or {})

            data_dir = target + "_data"
            if data_dir not in items:
                data_dir = None
            if "data_dir" in attrs:
                fail("May not set data dir in `extra_attrs`")
            attrs["data_dir"] = data_dir
            attrs.setdefault("srcs", [item])
            attrs["deps"] = list(attrs.get("deps") or []) + [
                "root//tests/e2e_util:utils",
                "root//tests/e2e_util:golden",
            ]

            buck2_e2e_test(name = target, **attrs)
            continue
        fail("Expected all directory entries to look like `test_*_data` or `test_*.py`, not {}".format(item))

    for target in target_extra_attrs.keys():
        if target not in generated_targets:
            fail("No such target {}".format(target))
