#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_local_buildbuddy_bes_rbe.sh [options]

Sets up a local BuildBuddy BES+RBE test flow for Buck2 examples/hello_world:
1) Reuse or create a BuildBuddy worktree on branch sluongng/codex-buck2-events.
2) Run //enterprise/server and //enterprise/server/cmd/executor.
3) Build //:main in examples/hello_world with BES+RBE enabled.
4) Print Buck2 output and the BuildBuddy invocation URL.

Options:
  --buildbuddy-repo PATH    BuildBuddy repo path (default: $HOME/work/buildbuddy-io/buildbuddy)
  --branch NAME             BuildBuddy branch name (default: sluongng/codex-buck2-events)
  --worktree-dir PATH       Explicit worktree path to create if needed
  --keep-running            Keep server/executor running after script exits
  --no-reuse-running        Always start fresh server/executor processes
  -h, --help                Show this help

Environment:
  BUCK2_BIN                 Buck2 binary (default: buck2)
  BAZEL_BIN                 Bazel binary (default: bazelisk, fallback: bazel)
EOF
}

log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELLO_WORLD_DIR="$SCRIPT_DIR"
BUCK2_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

BUILDBUDDY_REPO="${HOME}/work/buildbuddy-io/buildbuddy"
BRANCH="sluongng/codex-buck2-events"
WORKTREE_DIR=""
KEEP_RUNNING=0
REUSE_RUNNING=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --buildbuddy-repo)
      [[ $# -ge 2 ]] || die "Missing value for --buildbuddy-repo"
      BUILDBUDDY_REPO="$2"
      shift 2
      ;;
    --branch)
      [[ $# -ge 2 ]] || die "Missing value for --branch"
      BRANCH="$2"
      shift 2
      ;;
    --worktree-dir)
      [[ $# -ge 2 ]] || die "Missing value for --worktree-dir"
      WORKTREE_DIR="$2"
      shift 2
      ;;
    --keep-running)
      KEEP_RUNNING=1
      shift
      ;;
    --no-reuse-running)
      REUSE_RUNNING=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

[[ -d "$BUILDBUDDY_REPO/.git" ]] || die "BuildBuddy repo not found: $BUILDBUDDY_REPO"
[[ -f "$HELLO_WORLD_DIR/.buckconfig" ]] || die "Missing hello_world .buckconfig: $HELLO_WORLD_DIR/.buckconfig"

BUCK2_BIN="${BUCK2_BIN:-buck2}"
if ! command -v "$BUCK2_BIN" >/dev/null 2>&1; then
  die "Buck2 binary not found: $BUCK2_BIN"
fi

if [[ -n "${BAZEL_BIN:-}" ]]; then
  :
elif command -v bazelisk >/dev/null 2>&1; then
  BAZEL_BIN="bazelisk"
elif command -v bazel >/dev/null 2>&1; then
  BAZEL_BIN="bazel"
else
  die "Could not find bazelisk or bazel in PATH"
fi

LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/buck2-buildbuddy-bes-rbe.XXXXXX")"
SERVER_LOG="$LOG_DIR/buildbuddy-server.log"
EXECUTOR_LOG="$LOG_DIR/buildbuddy-executor.log"
BUCK2_OUTPUT_LOG="$LOG_DIR/buck2-build.log"
BUILD_ID_FILE="$LOG_DIR/buck2-build-id.txt"

SERVER_PID=""
EXECUTOR_PID=""

cleanup() {
  local exit_code=$?
  if [[ "$KEEP_RUNNING" -eq 0 ]]; then
    if [[ -n "$EXECUTOR_PID" ]] && kill -0 "$EXECUTOR_PID" 2>/dev/null; then
      log "Stopping executor (pid=$EXECUTOR_PID)"
      kill "$EXECUTOR_PID" 2>/dev/null || true
    fi
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
      log "Stopping server (pid=$SERVER_PID)"
      kill "$SERVER_PID" 2>/dev/null || true
    fi
  fi
  if [[ "$exit_code" -ne 0 ]]; then
    printf 'Logs:\n'
    printf '  %s\n' "$SERVER_LOG"
    printf '  %s\n' "$EXECUTOR_LOG"
    printf '  %s\n' "$BUCK2_OUTPUT_LOG"
  fi
}
trap cleanup EXIT

find_existing_worktree_for_branch() {
  local repo="$1"
  local branch="$2"
  local want_ref="refs/heads/$branch"
  local current_path=""
  local line=""

  while IFS= read -r line; do
    case "$line" in
      worktree\ *)
        current_path="${line#worktree }"
        ;;
      branch\ *)
        if [[ "${line#branch }" == "$want_ref" ]]; then
          printf '%s\n' "$current_path"
          return 0
        fi
        ;;
    esac
  done < <(git -C "$repo" worktree list --porcelain)

  return 1
}

ensure_worktree() {
  local repo="$1"
  local branch="$2"
  local worktree_dir="${3:-}"
  local existing=""

  git -C "$repo" worktree prune
  if existing="$(find_existing_worktree_for_branch "$repo" "$branch")"; then
    log "Reusing existing BuildBuddy worktree: $existing"
    printf '%s\n' "$existing"
    return 0
  fi

  if [[ -z "$worktree_dir" ]]; then
    local branch_slug
    branch_slug="$(printf '%s' "$branch" | tr '/' '-')"
    worktree_dir="$(dirname "$repo")/buildbuddy-worktrees/$branch_slug"
  fi

  mkdir -p "$(dirname "$worktree_dir")"

  if [[ -e "$worktree_dir" ]]; then
    die "Requested worktree path already exists but is not registered for this branch: $worktree_dir"
  fi

  if git -C "$repo" show-ref --verify --quiet "refs/heads/$branch"; then
    log "Creating BuildBuddy worktree: $worktree_dir (branch: $branch)"
    git -C "$repo" worktree add "$worktree_dir" "$branch" >/dev/null
  elif git -C "$repo" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
    log "Creating BuildBuddy worktree from origin/$branch: $worktree_dir"
    git -C "$repo" worktree add -b "$branch" "$worktree_dir" "origin/$branch" >/dev/null
  else
    die "Branch not found locally or in origin: $branch"
  fi

  printf '%s\n' "$worktree_dir"
}

is_ready() {
  local url="$1"
  [[ "$(curl -fsS "$url" 2>/dev/null || true)" == "OK" ]]
}

wait_for_ready() {
  local name="$1"
  local url="$2"
  local timeout_s="$3"
  local pid="${4:-}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    if is_ready "$url"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

parse_bes_results_url() {
  local buckconfig="$1"
  awk -F '=' '
    /^\[/ { in_bes = ($0 == "[bes]") }
    in_bes && $1 ~ /^[[:space:]]*results_url[[:space:]]*$/ {
      v = $2
      sub(/^[[:space:]]+/, "", v)
      sub(/[[:space:]]+$/, "", v)
      print v
      exit
    }
  ' "$buckconfig"
}

build_invocation_url() {
  local base_url="$1"
  local build_id="$2"
  if [[ "$base_url" == *"%s"* ]]; then
    printf '%s\n' "${base_url//%s/$build_id}"
  elif [[ "$base_url" == */ || "$base_url" == *= ]]; then
    printf '%s%s\n' "$base_url" "$build_id"
  else
    printf '%s/%s\n' "$base_url" "$build_id"
  fi
}

WORKTREE_PATH="$(ensure_worktree "$BUILDBUDDY_REPO" "$BRANCH" "$WORKTREE_DIR")"
log "Using BuildBuddy worktree: $WORKTREE_PATH"

SERVER_READY_URL="http://localhost:8080/readyz?server-type=buildbuddy-server"
EXECUTOR_READY_URL="http://localhost:8888/readyz?server-type=prod-buildbuddy-executor"

if [[ "$REUSE_RUNNING" -eq 1 ]] && is_ready "$SERVER_READY_URL"; then
  log "BuildBuddy server already healthy; reusing existing instance."
else
  log "Starting BuildBuddy server target //enterprise/server"
  (
    cd "$WORKTREE_PATH"
    "$BAZEL_BIN" run //enterprise/server -- --config_file=enterprise/config/buildbuddy.local.yaml
  ) >"$SERVER_LOG" 2>&1 &
  SERVER_PID="$!"

  if ! wait_for_ready "server" "$SERVER_READY_URL" 300 "$SERVER_PID"; then
    tail -n 120 "$SERVER_LOG" >&2 || true
    die "BuildBuddy server failed to become ready (see $SERVER_LOG)"
  fi
fi

if [[ "$REUSE_RUNNING" -eq 1 ]] && is_ready "$EXECUTOR_READY_URL"; then
  log "BuildBuddy executor already healthy; reusing existing instance."
else
  log "Starting BuildBuddy executor target //enterprise/server/cmd/executor"
  (
    cd "$WORKTREE_PATH"
    "$BAZEL_BIN" run //enterprise/server/cmd/executor -- \
      --config_file=enterprise/config/executor.local.yaml \
      --executor.app_target=grpc://localhost:1985 \
      --executor.enable_firecracker=false
  ) >"$EXECUTOR_LOG" 2>&1 &
  EXECUTOR_PID="$!"

  if ! wait_for_ready "executor" "$EXECUTOR_READY_URL" 300 "$EXECUTOR_PID"; then
    tail -n 120 "$EXECUTOR_LOG" >&2 || true
    die "BuildBuddy executor failed to become ready (see $EXECUTOR_LOG)"
  fi
fi

log "Running Buck2 build in examples/hello_world with BES+RBE enabled"
set +e
(
  cd "$HELLO_WORLD_DIR"
  "$BUCK2_BIN" build //:main \
    --prefer-remote \
    --write-build-id "$BUILD_ID_FILE" \
    --config "bes.backend=grpc://localhost:1985" \
    --config "bes.results_url=http://localhost:8080/invocation/" \
    --config "buck2_re_client.engine_address=grpc://localhost:1985" \
    --config "buck2_re_client.action_cache_address=grpc://localhost:1985" \
    --config "buck2_re_client.cas_address=grpc://localhost:1985" \
    --config "buck2_re_client.tls=false" \
    --config "build.execution_platforms=root//platforms:buildbuddy-rbe"
) 2>&1 | tee "$BUCK2_OUTPUT_LOG"
build_status=${PIPESTATUS[0]}
set -e

build_id=""
if [[ -s "$BUILD_ID_FILE" ]]; then
  build_id="$(tr -d '[:space:]' < "$BUILD_ID_FILE")"
else
  build_id="$(grep -Eo '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' "$BUCK2_OUTPUT_LOG" | tail -n1 || true)"
fi

results_url="$(parse_bes_results_url "$HELLO_WORLD_DIR/.buckconfig")"
if [[ -z "$results_url" ]]; then
  results_url="http://localhost:8080/invocation/"
fi

invocation_url=""
if [[ -n "$build_id" ]]; then
  invocation_url="$(build_invocation_url "$results_url" "$build_id")"
fi

printf '\n'
printf 'Buck2 terminal output log: %s\n' "$BUCK2_OUTPUT_LOG"
if [[ -n "$invocation_url" ]]; then
  printf 'BuildBuddy invocation URL: %s\n' "$invocation_url"
else
  printf 'BuildBuddy invocation URL: unavailable (could not determine build ID)\n'
fi
printf 'BuildBuddy server log: %s\n' "$SERVER_LOG"
printf 'BuildBuddy executor log: %s\n' "$EXECUTOR_LOG"

exit "$build_status"
