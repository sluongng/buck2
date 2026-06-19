#!/usr/bin/env bash
set -euo pipefail

MODE=both
MATRIX=smoke
BUCK2_BIN=${BUCK2:-target/debug/buck2}
ISOLATION_PREFIX=${ISOLATION_PREFIX:-buildbuddy-buck2-tests}
REMOTE_CONFIG_FILE=${BUILDBUDDY_CONFIG_FILE:-}
CLEAN_OUTPUT=0

usage() {
    cat <<'EOF'
Usage: buildbuddy/run_buck2_test_matrix.sh [options]

Runs the first Buck2-based smoke matrix that is known to work both locally and
with BuildBuddy remote execution.

Options:
  --mode local|remote|both      Select which execution mode to run. Default: both.
  --matrix smoke|app-rust|gazebo-rust|prelude-python|prelude-command-alias|tests-tools-cram|core-e2e-smoke|core-build-response|select-type-params|check-dependencies|app-dep-graph
                                Select targets. Default: smoke.
                                smoke: app Rust targets plus docs lab rust_test.
                                app-rust: targets present across this stack.
                                gazebo-rust: small non-app Rust test.
                                prelude-python: small prelude python_test probe.
                                prelude-command-alias: build assertion probe.
                                tests-tools-cram: OSS cram_test probe.
                                core-e2e-smoke: OSS buck_e2e_test probe.
                                core-build-response: OSS buck_e2e build-result probe.
                                select-type-params: broader OSS buck_e2e_test probe.
                                check-dependencies: OSS BXL check-dependencies probe.
                                app-dep-graph: build-time dependency graph assertion.
  --buck2 PATH                  Buck2 binary to use. Default: $BUCK2 or target/debug/buck2.
  --isolation-prefix NAME       Isolation dir prefix. Default: buildbuddy-buck2-tests.
  --remote-config-file PATH     Extra buckconfig file for remote mode.
                                Default: $BUILDBUDDY_CONFIG_FILE, or .buckconfig.buildbuddy
                                when .buckconfig.local is absent.
  --clean-output                Kill Buck2 daemons and remove this script's buck-out dirs first.
  -h, --help                    Show this help.

The remote mode expects BuildBuddy credentials to come from BUILDBUDDY_API_KEY,
the user's private .buckconfig.local, or another config passed with
--remote-config-file.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --matrix)
            MATRIX="$2"
            shift 2
            ;;
        --buck2)
            BUCK2_BIN="$2"
            shift 2
            ;;
        --isolation-prefix)
            ISOLATION_PREFIX="$2"
            shift 2
            ;;
        --remote-config-file)
            REMOTE_CONFIG_FILE="$2"
            shift 2
            ;;
        --clean-output)
            CLEAN_OUTPUT=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

case "$MODE" in
    local | remote | both) ;;
    *)
        echo "--mode must be local, remote, or both; got: $MODE" >&2
        exit 2
        ;;
esac

case "$MATRIX" in
    smoke | app-rust | gazebo-rust | prelude-python | prelude-command-alias | tests-tools-cram | core-e2e-smoke | core-build-response | select-type-params | check-dependencies | app-dep-graph) ;;
    *)
        echo "--matrix must be smoke, app-rust, gazebo-rust, prelude-python, prelude-command-alias, tests-tools-cram, core-e2e-smoke, core-build-response, select-type-params, check-dependencies, or app-dep-graph; got: $MATRIX" >&2
        exit 2
        ;;
esac

if [[ ! -x "$BUCK2_BIN" ]]; then
    echo "Buck2 binary is not executable: $BUCK2_BIN" >&2
    echo "Set BUCK2=/path/to/buck2 or run: cargo build -p buck2 --bin buck2" >&2
    exit 2
fi

MATRIX_TARGETS=()
MATRIX_CONFIG_ARGS=()
MATRIX_COMMAND=test
TARGET_PLATFORM_ARGS=()

case "$MATRIX" in
    smoke | app-rust)
        TARGET_PLATFORM_ARGS=(--target-platforms prelude//platforms:default)
        MATRIX_TARGETS=(
            //app/buck2_action_impl_tests:buck2_action_impl_tests
            //app/buck2_build_api_tests:buck2_build_api_tests
            //app/buck2_core:soft_error
            //app/buck2_error_tests:buck2_error_tests
            //app/buck2_interpreter_for_build_tests:buck2_interpreter_for_build_tests
            //app/buck2_node_tests:buck2_node_tests
            prelude//toolchains/android/src/com/facebook/buck/util/zip:zip_scrubber_main
        )

        if [[ "$MATRIX" == "smoke" ]]; then
            MATRIX_TARGETS+=(
                //docs/buck2_lab/greeter_lib:test
            )
        fi
        ;;
    prelude-python)
        TARGET_PLATFORM_ARGS=(--target-platforms prelude//platforms:default)
        MATRIX_TARGETS=(
            prelude//java/tools:test_list_class_names
        )
        ;;
    gazebo-rust)
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        MATRIX_TARGETS=(
            //gazebo/strong_hash_tests:strong_hash_tests
        )
        ;;
    prelude-command-alias)
        MATRIX_COMMAND=build
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        TARGET_PLATFORM_ARGS=(--target-platforms prelude//platforms:default)
        MATRIX_TARGETS=(
            //tests/prelude/command_alias/quoting:check_quoted_args
        )
        ;;
    tests-tools-cram)
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        MATRIX_TARGETS=(
            //tests/tools/makefile_to_depfile:integration
        )
        ;;
    core-e2e-smoke)
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        MATRIX_TARGETS=(
            //tests/core/analysis:test_cmd_args
        )
        ;;
    core-build-response)
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        MATRIX_TARGETS=(
            //tests/core/build:test_build_response
        )
        ;;
    select-type-params)
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        MATRIX_TARGETS=(
            //tests/select_type_params:test_select_types
        )
        ;;
    check-dependencies)
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        MATRIX_TARGETS=(
            //tests/e2e/check_dependencies_test:allow_list_and_block_none
        )
        ;;
    app-dep-graph)
        MATRIX_COMMAND=build
        MATRIX_CONFIG_ARGS=(-c project.ignore=)
        MATRIX_TARGETS=(
            //app_dep_graph_rules:test_buck2_dep_graph
        )
        ;;
esac

clean_all() {
    "$BUCK2_BIN" killall >/dev/null 2>&1 || true
    rm -rf "buck-out/$ISOLATION_PREFIX-local" "buck-out/$ISOLATION_PREFIX-rbe"
}

clean_mode() {
    local mode="$1"
    "$BUCK2_BIN" killall >/dev/null 2>&1 || true
    rm -rf "buck-out/$ISOLATION_PREFIX-$mode"
}

if [[ "$CLEAN_OUTPUT" == 1 ]]; then
    clean_all
fi

run_local() {
    "$BUCK2_BIN" --isolation-dir "$ISOLATION_PREFIX-local" \
        "$MATRIX_COMMAND" "${MATRIX_CONFIG_ARGS[@]}" \
        "${TARGET_PLATFORM_ARGS[@]}" \
        -c build.execution_platforms= --local-only --no-remote-cache \
        "${MATRIX_TARGETS[@]}"
}

run_remote() {
    local remote_config_args=()

    if [[ -n "$REMOTE_CONFIG_FILE" ]]; then
        remote_config_args=(--config-file "$REMOTE_CONFIG_FILE")
    elif [[ ! -f .buckconfig.local && -f .buckconfig.buildbuddy ]]; then
        remote_config_args=(--config-file .buckconfig.buildbuddy)
    fi

    command=(
        "$BUCK2_BIN"
        --isolation-dir "$ISOLATION_PREFIX-rbe"
        "$MATRIX_COMMAND"
        "${remote_config_args[@]}"
        "${MATRIX_CONFIG_ARGS[@]}"
        "${TARGET_PLATFORM_ARGS[@]}"
        --remote-only
    )

    if [[ "$MATRIX_COMMAND" == "test" ]]; then
        command+=(--unstable-allow-all-tests-on-re)
    fi

    command+=("${MATRIX_TARGETS[@]}")
    "${command[@]}"
}

case "$MODE" in
    local)
        run_local
        ;;
    remote)
        run_remote
        ;;
    both)
        run_local
        if [[ "$CLEAN_OUTPUT" == 1 ]]; then
            clean_mode local
        fi
        run_remote
        ;;
esac
