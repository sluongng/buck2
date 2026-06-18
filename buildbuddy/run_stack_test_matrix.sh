#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
ROOT_DIR=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)

FROM_REF=origin/main
TO_REF=HEAD
MODE=local
MATRIX=app-rust
BUCK2_BIN=${BUCK2:-target/debug/buck2}
REMOTE_CONFIG_FILE=${BUILDBUDDY_CONFIG_FILE:-}
PASS_REMOTE_CONFIG_FILE=
STAGE_REMOTE_CONFIG=0
WORKTREE_ROOT=${WORKTREE_ROOT:-${TMPDIR:-/tmp}/buck2-stack-test-matrix-$$}
PREPARE_RUST_SHIMS=auto
RUST_SHIM_SOURCE=auto
LIMIT=
LOG_DIR=
DRY_RUN=0
KEEP_WORKTREES=0

usage() {
    cat <<'EOF'
Usage: buildbuddy/run_stack_test_matrix.sh [options]

Runs buildbuddy/run_buck2_test_matrix.sh over each commit in a stack using
temporary detached git worktrees. The main checkout is not modified. Successful
temporary worktrees are removed immediately unless --keep-worktrees is set.

Options:
  --from REF                 Base ref, exclusive. Default: origin/main.
  --to REF                   Tip ref, inclusive. Default: HEAD.
  --mode local|remote|both   Matrix mode to run. Default: local.
  --matrix smoke|app-rust|gazebo-rust|prelude-python|prelude-command-alias|tests-tools-cram|core-e2e-smoke|core-build-response|select-type-params|check-dependencies|app-dep-graph
                             Matrix target set. Default: app-rust.
                             app-rust is the portable choice for historical
                             commits in the current stack.
  --buck2 PATH               Buck2 binary to use. Default: $BUCK2 or
                             target/debug/buck2 from the main checkout.
  --remote-config-file PATH  Extra buckconfig file for remote mode. Default:
                             $BUILDBUDDY_CONFIG_FILE, or the main checkout's
                             .buckconfig.local when present.
  --prepare-rust-shims auto|always|never
                             Prepare shim/third-party/rust/BUCK.reindeer in
                             temporary worktrees. Default: auto, which runs only
                             when BUCK.reindeer is absent.
  --rust-shim-source PATH|auto|none
                             Source BUCK.reindeer to copy when preparing Rust
                             shims. Default: auto, which uses the main checkout's
                             tracked shim when present and falls back to Reindeer.
                             Use none to force Reindeer generation. Copying a
                             shim also copies known generated-shim support files
                             from the main checkout when the commit lacks them.
  --worktree-root PATH       Directory for temporary worktrees. Default:
                             $TMPDIR/buck2-stack-test-matrix-$$.
  --limit N                  Only run the first N commits from the range.
  --log-dir PATH             Save each commit's full matrix output to this
                             directory and print only a short pass/fail summary.
  --dry-run                  Print planned commits and commands without running.
  --keep-worktrees           Do not remove temporary worktrees on exit.
  -h, --help                 Show this help.

Remote mode needs BuildBuddy credentials. Passing the main checkout's private
.buckconfig.local is intentional for old commits that predate the committed
BuildBuddy profile. The default private config is copied into temporary
worktrees so Buck can load it as the worktree's local config; explicit
--remote-config-file values are passed through instead. When remote mode uses
that profile, the wrapper also copies the checked-in BuildBuddy
execution-platform package into historical worktrees that predate it.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --from)
            FROM_REF="$2"
            shift 2
            ;;
        --to)
            TO_REF="$2"
            shift 2
            ;;
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
        --remote-config-file)
            REMOTE_CONFIG_FILE="$2"
            shift 2
            ;;
        --prepare-rust-shims)
            PREPARE_RUST_SHIMS="$2"
            shift 2
            ;;
        --rust-shim-source)
            RUST_SHIM_SOURCE="$2"
            shift 2
            ;;
        --worktree-root)
            WORKTREE_ROOT="$2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        --log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --keep-worktrees)
            KEEP_WORKTREES=1
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

USES_REMOTE=0
if [[ "$MODE" != "local" ]]; then
    USES_REMOTE=1
fi

case "$MATRIX" in
    smoke | app-rust | gazebo-rust | prelude-python | prelude-command-alias | tests-tools-cram | core-e2e-smoke | core-build-response | select-type-params | check-dependencies | app-dep-graph) ;;
    *)
        echo "--matrix must be smoke, app-rust, gazebo-rust, prelude-python, prelude-command-alias, tests-tools-cram, core-e2e-smoke, core-build-response, select-type-params, check-dependencies, or app-dep-graph; got: $MATRIX" >&2
        exit 2
        ;;
esac

case "$PREPARE_RUST_SHIMS" in
    auto | always | never) ;;
    *)
        echo "--prepare-rust-shims must be auto, always, or never; got: $PREPARE_RUST_SHIMS" >&2
        exit 2
        ;;
esac

if [[ -n "$LIMIT" && ! "$LIMIT" =~ ^[0-9]+$ ]]; then
    echo "--limit must be a non-negative integer; got: $LIMIT" >&2
    exit 2
fi

if [[ "$BUCK2_BIN" != /* ]]; then
    BUCK2_BIN="$ROOT_DIR/$BUCK2_BIN"
fi

if [[ ! -x "$BUCK2_BIN" ]]; then
    echo "Buck2 binary is not executable: $BUCK2_BIN" >&2
    echo "Set BUCK2=/path/to/buck2 or run: cargo build -p buck2 --bin buck2" >&2
    exit 2
fi

if [[ "$USES_REMOTE" == 1 ]]; then
    if [[ -z "$REMOTE_CONFIG_FILE" && -f "$ROOT_DIR/.buckconfig.local" ]]; then
        REMOTE_CONFIG_FILE="$ROOT_DIR/.buckconfig.local"
        STAGE_REMOTE_CONFIG=1
    elif [[ -n "$REMOTE_CONFIG_FILE" && "$REMOTE_CONFIG_FILE" != /* ]]; then
        REMOTE_CONFIG_FILE="$ROOT_DIR/$REMOTE_CONFIG_FILE"
    fi

    if [[ "$STAGE_REMOTE_CONFIG" != 1 ]]; then
        PASS_REMOTE_CONFIG_FILE="$REMOTE_CONFIG_FILE"
    elif [[ "$KEEP_WORKTREES" == 1 ]]; then
        echo "Warning: --keep-worktrees will leave staged BuildBuddy credentials under $WORKTREE_ROOT" >&2
    fi
fi

if [[ -n "$LOG_DIR" && "$LOG_DIR" != /* ]]; then
    LOG_DIR="$ROOT_DIR/$LOG_DIR"
fi

case "$RUST_SHIM_SOURCE" in
    auto)
        if [[ -f "$ROOT_DIR/shim/third-party/rust/BUCK.reindeer" ]]; then
            RUST_SHIM_SOURCE="$ROOT_DIR/shim/third-party/rust/BUCK.reindeer"
        else
            RUST_SHIM_SOURCE=
        fi
        ;;
    none)
        RUST_SHIM_SOURCE=
        ;;
    *)
        if [[ "$RUST_SHIM_SOURCE" != /* ]]; then
            RUST_SHIM_SOURCE="$ROOT_DIR/$RUST_SHIM_SOURCE"
        fi

        if [[ ! -f "$RUST_SHIM_SOURCE" ]]; then
            echo "Rust shim source does not exist: $RUST_SHIM_SOURCE" >&2
            exit 2
        fi
        ;;
esac

mapfile -t COMMITS < <(git -C "$ROOT_DIR" rev-list --reverse "$FROM_REF..$TO_REF")

if [[ -n "$LIMIT" ]]; then
    COMMITS=("${COMMITS[@]:0:LIMIT}")
fi

if [[ ${#COMMITS[@]} -eq 0 ]]; then
    echo "No commits to verify in range $FROM_REF..$TO_REF"
    exit 0
fi

RUNNER="$ROOT_DIR/buildbuddy/run_buck2_test_matrix.sh"
if [[ ! -x "$RUNNER" ]]; then
    echo "Matrix runner is not executable: $RUNNER" >&2
    exit 2
fi

echo "Verifying ${#COMMITS[@]} commit(s) from $FROM_REF..$TO_REF"
echo "Mode: $MODE"
echo "Matrix: $MATRIX"
echo "Buck2: $BUCK2_BIN"
echo "Prepare Rust shims: $PREPARE_RUST_SHIMS"
if [[ -n "$RUST_SHIM_SOURCE" ]]; then
    echo "Rust shim source: $RUST_SHIM_SOURCE"
fi
if [[ "$USES_REMOTE" == 1 && -n "$REMOTE_CONFIG_FILE" ]]; then
    echo "Remote config: $REMOTE_CONFIG_FILE"
    if [[ "$STAGE_REMOTE_CONFIG" == 1 ]]; then
        echo "Remote config staging: .buckconfig.local in each temporary worktree"
    fi
fi
if [[ -n "$LOG_DIR" ]]; then
    echo "Log dir: $LOG_DIR"
fi
echo "Worktree root: $WORKTREE_ROOT"

WORKTREES=()
cleanup() {
    if [[ "$KEEP_WORKTREES" == 1 ]]; then
        return
    fi

    for worktree in "${WORKTREES[@]:-}"; do
        git -C "$ROOT_DIR" worktree remove --force "$worktree" >/dev/null 2>&1 || true
    done
    rmdir "$WORKTREE_ROOT" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$WORKTREE_ROOT"
if [[ -n "$LOG_DIR" && "$DRY_RUN" != 1 ]]; then
    mkdir -p "$LOG_DIR"
fi

remove_successful_worktree() {
    local worktree="$1"

    if [[ "$KEEP_WORKTREES" == 1 ]]; then
        return
    fi

    git -C "$ROOT_DIR" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}

prepare_worktree() {
    local worktree="$1"

    if [[ "$PREPARE_RUST_SHIMS" == "never" ]]; then
        return
    fi

    if [[ "$PREPARE_RUST_SHIMS" == "auto" && -f "$worktree/shim/third-party/rust/BUCK.reindeer" ]]; then
        return
    fi

    if [[ -n "$RUST_SHIM_SOURCE" ]]; then
        echo "Copying shim/third-party/rust/BUCK.reindeer from $RUST_SHIM_SOURCE"
        mkdir -p "$worktree/shim/third-party/rust"
        cp "$RUST_SHIM_SOURCE" "$worktree/shim/third-party/rust/BUCK.reindeer"
        for support_file in shim/git_fetch.bzl; do
            if [[ -f "$ROOT_DIR/$support_file" && ( "$PREPARE_RUST_SHIMS" == "always" || ! -f "$worktree/$support_file" ) ]]; then
                echo "Copying $support_file from $ROOT_DIR"
                mkdir -p "$worktree/$(dirname "$support_file")"
                cp "$ROOT_DIR/$support_file" "$worktree/$support_file"
            fi
        done
        return
    fi

    if [[ ! -x "$worktree/bootstrap/reindeer" ]]; then
        echo "Cannot generate Rust shims: $worktree/bootstrap/reindeer is missing or not executable" >&2
        exit 1
    fi

    echo "Generating shim/third-party/rust/BUCK.reindeer with Reindeer"
    (cd "$worktree" && ./bootstrap/reindeer --third-party-dir shim/third-party/rust buckify)
}

prepare_matrix_worktree() {
    local worktree="$1"

    case "$MATRIX" in
        prelude-command-alias)
            for support_file in shim/buck2/tests/prelude/py_assertion.bzl shim/tools/build_defs/selects.bzl; do
                echo "Copying $support_file from $ROOT_DIR"
                mkdir -p "$worktree/$(dirname "$support_file")"
                cp "$ROOT_DIR/$support_file" "$worktree/$support_file"
            done
            ;;
        tests-tools-cram)
            support_file=shim/tools/build_defs/cram_test.bzl
            echo "Copying $support_file from $ROOT_DIR"
            mkdir -p "$worktree/$(dirname "$support_file")"
            cp "$ROOT_DIR/$support_file" "$worktree/$support_file"
            ;;
        core-e2e-smoke | core-build-response | select-type-params | check-dependencies)
            for support_file in \
                shim/BUCK \
                shim/rust-toolchain \
                shim/rust_toolchain.bzl \
                shim/build_defs/python_library.bzl \
                shim/buck2/tests/BUCK \
                shim/buck2/tests/buck_e2e.bzl \
                shim/buck2/tests/oss_pytest_runner.py \
                shim/third-party/pypi/decorator/BUCK \
                shim/third-party/pypi/decorator/decorator.py \
                shim/third-party/pypi/psutil/BUCK \
                shim/third-party/pypi/psutil/psutil.py \
                shim/third-party/pypi/pytest/BUCK \
                shim/third-party/pypi/pytest/pytest.py; do
                echo "Copying $support_file from $ROOT_DIR"
                mkdir -p "$worktree/$(dirname "$support_file")"
                cp "$ROOT_DIR/$support_file" "$worktree/$support_file"
            done
            if [[ "$MATRIX" == "check-dependencies" ]]; then
                for support_file in \
                    shim/tools/build_defs/check_dependencies_test.bzl \
                    tests/e2e_util/test_bxl_check_dependencies_template.py \
                    tests/e2e/check_dependencies_test/BUCK \
                    tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/.buckconfig \
                    tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/TARGETS.fixture \
                    tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/defs.bzl \
                    tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/tests/check_dependencies_test.bxl \
                    tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/tests/dependencies_test_util.bzl; do
                    echo "Copying $support_file from $ROOT_DIR"
                    mkdir -p "$worktree/$(dirname "$support_file")"
                    cp "$ROOT_DIR/$support_file" "$worktree/$support_file"
                done
            fi
            ;;
        app-dep-graph)
            for support_file in app_dep_graph_rules/rules.bzl app_dep_graph_rules/test_impl.bzl; do
                echo "Copying $support_file from $ROOT_DIR"
                mkdir -p "$worktree/$(dirname "$support_file")"
                cp "$ROOT_DIR/$support_file" "$worktree/$support_file"
            done
            ;;
    esac
}

prepare_remote_worktree() {
    local worktree="$1"

    if [[ "$USES_REMOTE" != 1 ]]; then
        return
    fi

    for support_file in buildbuddy/BUCK buildbuddy/defs.bzl buildbuddy/toolchains/BUCK prelude/toolchains/go/defs.bzl prelude/toolchains/go/releases.bzl; do
        if [[ -f "$ROOT_DIR/$support_file" && ! -f "$worktree/$support_file" ]]; then
            echo "Copying $support_file from $ROOT_DIR"
            mkdir -p "$worktree/$(dirname "$support_file")"
            cp "$ROOT_DIR/$support_file" "$worktree/$support_file"
        fi
    done

    if [[ "$STAGE_REMOTE_CONFIG" == 1 ]]; then
        echo "Copying .buckconfig.local from $REMOTE_CONFIG_FILE"
        cp "$REMOTE_CONFIG_FILE" "$worktree/.buckconfig.local"
        if [[ -f "$ROOT_DIR/.buckconfig.buildbuddy" ]]; then
            echo "Copying .buckconfig.buildbuddy from $ROOT_DIR"
            cp "$ROOT_DIR/.buckconfig.buildbuddy" "$worktree/.buckconfig.buildbuddy"
        fi
    fi
}

idx=0
for commit in "${COMMITS[@]}"; do
    idx=$((idx + 1))
    short=${commit:0:12}
    subject=$(git -C "$ROOT_DIR" log -1 --format=%s "$commit")
    worktree="$WORKTREE_ROOT/$idx-$short"
    isolation_prefix="stack-$idx-$short"

    echo
    echo "[$idx/${#COMMITS[@]}] $short $subject"

    if [[ "$DRY_RUN" == 1 ]]; then
        printf 'git worktree add --detach %q %q\n' "$worktree" "$commit"
        if [[ "$PREPARE_RUST_SHIMS" != "never" ]]; then
            if [[ -n "$RUST_SHIM_SOURCE" ]]; then
                printf '(cd %q && [ -f shim/third-party/rust/BUCK.reindeer ] || { mkdir -p shim/third-party/rust && cp %q shim/third-party/rust/BUCK.reindeer; })\n' "$worktree" "$RUST_SHIM_SOURCE"
                printf '(cd %q && [ -f shim/git_fetch.bzl ] || { mkdir -p shim && cp %q shim/git_fetch.bzl; })\n' "$worktree" "$ROOT_DIR/shim/git_fetch.bzl"
            else
                printf '(cd %q && [ -f shim/third-party/rust/BUCK.reindeer ] || ./bootstrap/reindeer --third-party-dir shim/third-party/rust buckify)\n' "$worktree"
            fi
        fi
        if [[ "$USES_REMOTE" == 1 ]]; then
            printf 'mkdir -p %q\n' "$worktree/buildbuddy/toolchains"
            printf 'mkdir -p %q\n' "$worktree/prelude/toolchains/go"
            printf '[ -f %q ] || cp %q %q\n' "$worktree/buildbuddy/BUCK" "$ROOT_DIR/buildbuddy/BUCK" "$worktree/buildbuddy/BUCK"
            printf '[ -f %q ] || cp %q %q\n' "$worktree/buildbuddy/defs.bzl" "$ROOT_DIR/buildbuddy/defs.bzl" "$worktree/buildbuddy/defs.bzl"
            printf '[ -f %q ] || cp %q %q\n' "$worktree/buildbuddy/toolchains/BUCK" "$ROOT_DIR/buildbuddy/toolchains/BUCK" "$worktree/buildbuddy/toolchains/BUCK"
            printf '[ -f %q ] || cp %q %q\n' "$worktree/prelude/toolchains/go/defs.bzl" "$ROOT_DIR/prelude/toolchains/go/defs.bzl" "$worktree/prelude/toolchains/go/defs.bzl"
            printf '[ -f %q ] || cp %q %q\n' "$worktree/prelude/toolchains/go/releases.bzl" "$ROOT_DIR/prelude/toolchains/go/releases.bzl" "$worktree/prelude/toolchains/go/releases.bzl"
            if [[ "$STAGE_REMOTE_CONFIG" == 1 ]]; then
                printf 'cp %q %q\n' "$REMOTE_CONFIG_FILE" "$worktree/.buckconfig.local"
                printf 'cp %q %q\n' "$ROOT_DIR/.buckconfig.buildbuddy" "$worktree/.buckconfig.buildbuddy"
            fi
        fi
        if [[ "$MATRIX" == "prelude-command-alias" ]]; then
            printf '(cd %q && mkdir -p shim/buck2/tests/prelude shim/tools/build_defs && cp %q shim/buck2/tests/prelude/py_assertion.bzl && cp %q shim/tools/build_defs/selects.bzl)\n' "$worktree" "$ROOT_DIR/shim/buck2/tests/prelude/py_assertion.bzl" "$ROOT_DIR/shim/tools/build_defs/selects.bzl"
        fi
        if [[ "$MATRIX" == "tests-tools-cram" ]]; then
            printf '(cd %q && mkdir -p shim/tools/build_defs && cp %q shim/tools/build_defs/cram_test.bzl)\n' "$worktree" "$ROOT_DIR/shim/tools/build_defs/cram_test.bzl"
        fi
        if [[ "$MATRIX" == "core-e2e-smoke" || "$MATRIX" == "core-build-response" || "$MATRIX" == "select-type-params" || "$MATRIX" == "check-dependencies" ]]; then
            printf '(cd %q && mkdir -p shim/build_defs shim/buck2/tests shim/third-party/pypi/decorator shim/third-party/pypi/psutil shim/third-party/pypi/pytest && cp %q shim/BUCK && cp %q shim/rust-toolchain && cp %q shim/rust_toolchain.bzl && cp %q shim/build_defs/python_library.bzl && cp %q shim/buck2/tests/BUCK && cp %q shim/buck2/tests/buck_e2e.bzl && cp %q shim/buck2/tests/oss_pytest_runner.py && cp %q shim/third-party/pypi/decorator/BUCK && cp %q shim/third-party/pypi/decorator/decorator.py && cp %q shim/third-party/pypi/psutil/BUCK && cp %q shim/third-party/pypi/psutil/psutil.py && cp %q shim/third-party/pypi/pytest/BUCK && cp %q shim/third-party/pypi/pytest/pytest.py)\n' \
                "$worktree" \
                "$ROOT_DIR/shim/BUCK" \
                "$ROOT_DIR/shim/rust-toolchain" \
                "$ROOT_DIR/shim/rust_toolchain.bzl" \
                "$ROOT_DIR/shim/build_defs/python_library.bzl" \
                "$ROOT_DIR/shim/buck2/tests/BUCK" \
                "$ROOT_DIR/shim/buck2/tests/buck_e2e.bzl" \
                "$ROOT_DIR/shim/buck2/tests/oss_pytest_runner.py" \
                "$ROOT_DIR/shim/third-party/pypi/decorator/BUCK" \
                "$ROOT_DIR/shim/third-party/pypi/decorator/decorator.py" \
                "$ROOT_DIR/shim/third-party/pypi/psutil/BUCK" \
                "$ROOT_DIR/shim/third-party/pypi/psutil/psutil.py" \
                "$ROOT_DIR/shim/third-party/pypi/pytest/BUCK" \
                "$ROOT_DIR/shim/third-party/pypi/pytest/pytest.py"
        fi
        if [[ "$MATRIX" == "check-dependencies" ]]; then
            printf '(cd %q && mkdir -p shim/tools/build_defs tests/e2e_util tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/tests && cp %q shim/tools/build_defs/check_dependencies_test.bzl && cp %q tests/e2e_util/test_bxl_check_dependencies_template.py && cp %q tests/e2e/check_dependencies_test/BUCK && cp %q tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/.buckconfig && cp %q tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/TARGETS.fixture && cp %q tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/defs.bzl && cp %q tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/tests/check_dependencies_test.bxl && cp %q tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/tests/dependencies_test_util.bzl)\n' \
                "$worktree" \
                "$ROOT_DIR/shim/tools/build_defs/check_dependencies_test.bzl" \
                "$ROOT_DIR/tests/e2e_util/test_bxl_check_dependencies_template.py" \
                "$ROOT_DIR/tests/e2e/check_dependencies_test/BUCK" \
                "$ROOT_DIR/tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/.buckconfig" \
                "$ROOT_DIR/tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/TARGETS.fixture" \
                "$ROOT_DIR/tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/defs.bzl" \
                "$ROOT_DIR/tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/tests/check_dependencies_test.bxl" \
                "$ROOT_DIR/tests/e2e/check_dependencies_test/test_check_dependencies_oss_data/tests/dependencies_test_util.bzl"
        fi
        if [[ "$MATRIX" == "app-dep-graph" ]]; then
            printf '(cd %q && mkdir -p app_dep_graph_rules && cp %q app_dep_graph_rules/rules.bzl && cp %q app_dep_graph_rules/test_impl.bzl)\n' "$worktree" "$ROOT_DIR/app_dep_graph_rules/rules.bzl" "$ROOT_DIR/app_dep_graph_rules/test_impl.bzl"
        fi
        printf '(cd %q && %q --mode %q --matrix %q --buck2 %q --isolation-prefix %q --clean-output' \
            "$worktree" "$RUNNER" "$MODE" "$MATRIX" "$BUCK2_BIN" "$isolation_prefix"
        if [[ "$USES_REMOTE" == 1 && -n "$PASS_REMOTE_CONFIG_FILE" ]]; then
            printf ' --remote-config-file %q' "$PASS_REMOTE_CONFIG_FILE"
        fi
        printf ')'
        if [[ -n "$LOG_DIR" ]]; then
            printf ' > %q 2>&1' "$LOG_DIR/$idx-$short.log"
        fi
        printf '\n'
        continue
    fi

    git -C "$ROOT_DIR" worktree add --detach "$worktree" "$commit"
    WORKTREES+=("$worktree")
    prepare_worktree "$worktree"
    prepare_matrix_worktree "$worktree"
    prepare_remote_worktree "$worktree"

    command=(
        "$RUNNER"
        --mode "$MODE"
        --matrix "$MATRIX"
        --buck2 "$BUCK2_BIN"
        --isolation-prefix "$isolation_prefix"
        --clean-output
    )

    if [[ "$USES_REMOTE" == 1 && -n "$PASS_REMOTE_CONFIG_FILE" ]]; then
        command+=(--remote-config-file "$PASS_REMOTE_CONFIG_FILE")
    fi

    if [[ -n "$LOG_DIR" ]]; then
        log_file="$LOG_DIR/$idx-$short.log"
        echo "Log: $log_file"
        if (cd "$worktree" && "${command[@]}") >"$log_file" 2>&1; then
            echo "[$idx/${#COMMITS[@]}] PASS $short"
            remove_successful_worktree "$worktree"
        else
            status=$?
            echo "[$idx/${#COMMITS[@]}] FAIL $short (exit $status)"
            echo "Last 80 log lines:"
            tail -80 "$log_file" || true
            exit "$status"
        fi
    else
        (cd "$worktree" && "${command[@]}")
        remove_successful_worktree "$worktree"
    fi
done
