#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_local_buildbuddy_bazel_bes_compare.sh [options]

Runs a local BuildBuddy BES comparison:
1) Start or reuse BuildBuddy master.
2) Start or reuse a local executor for the Buck2 hello_world RBE build.
3) Run Buck2 with bes.event_format=bazel.
4) Run a comparable Bazel invocation against the same BES backend.
5) Print both invocation URLs and, optionally, capture BuildBuddy screenshots.

Options:
  --buildbuddy-repo PATH    BuildBuddy repo path (default: $HOME/work/buildbuddy-io/buildbuddy)
  --buildbuddy-branch NAME  BuildBuddy branch/worktree to use (default: master)
  --worktree-dir PATH       Explicit BuildBuddy worktree path to create if needed
  --output-dir PATH         Artifact directory (default: output/buildbuddy-bes-compare/<timestamp>)
  --keep-running            Keep BuildBuddy server/executor running after script exits
  --no-reuse-running        Always start fresh server/executor processes
  --playwright              Capture overview screenshots for both invocations
  -h, --help                Show this help

Environment:
  BUCK2_BIN                 Buck2 binary (default: buck2)
  BAZEL_BIN                 Bazel binary (default: bazelisk, fallback: bazel)
  CODEX_HOME                Codex home for the Playwright wrapper (default: $HOME/.codex)
EOF
}

log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr '[:upper:]' '[:lower:]'
  elif [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
  else
    python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELLO_WORLD_DIR="$SCRIPT_DIR"
BUCK2_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

BUILDBUDDY_REPO="${HOME}/work/buildbuddy-io/buildbuddy"
BUILDBUDDY_BRANCH="master"
WORKTREE_DIR=""
OUTPUT_DIR=""
KEEP_RUNNING=0
REUSE_RUNNING=1
CAPTURE_PLAYWRIGHT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --buildbuddy-repo)
      [[ $# -ge 2 ]] || die "Missing value for --buildbuddy-repo"
      BUILDBUDDY_REPO="$2"
      shift 2
      ;;
    --buildbuddy-branch|--branch)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      BUILDBUDDY_BRANCH="$2"
      shift 2
      ;;
    --worktree-dir)
      [[ $# -ge 2 ]] || die "Missing value for --worktree-dir"
      WORKTREE_DIR="$2"
      shift 2
      ;;
    --output-dir)
      [[ $# -ge 2 ]] || die "Missing value for --output-dir"
      OUTPUT_DIR="$2"
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
    --playwright)
      CAPTURE_PLAYWRIGHT=1
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
command -v "$BUCK2_BIN" >/dev/null 2>&1 || die "Buck2 binary not found: $BUCK2_BIN"
if [[ "$BUCK2_BIN" == */* ]]; then
  BUCK2_BIN="$(cd "$(dirname "$BUCK2_BIN")" && pwd)/$(basename "$BUCK2_BIN")"
fi

if [[ -n "${BAZEL_BIN:-}" ]]; then
  command -v "$BAZEL_BIN" >/dev/null 2>&1 || die "Bazel binary not found: $BAZEL_BIN"
elif command -v bazelisk >/dev/null 2>&1; then
  BAZEL_BIN="bazelisk"
elif command -v bazel >/dev/null 2>&1; then
  BAZEL_BIN="bazel"
else
  die "Could not find bazelisk or bazel in PATH"
fi

if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$BUCK2_REPO/output/buildbuddy-bes-compare/$(date +%Y%m%d-%H%M%S)"
elif [[ "$OUTPUT_DIR" != /* ]]; then
  OUTPUT_DIR="$BUCK2_REPO/$OUTPUT_DIR"
fi

mkdir -p "$OUTPUT_DIR"
LOG_DIR="$OUTPUT_DIR/logs"
PLAYWRIGHT_DIR="$BUCK2_REPO/output/playwright/bes-compare"
BAZEL_WS="$OUTPUT_DIR/bazel-workspace"
mkdir -p "$LOG_DIR" "$PLAYWRIGHT_DIR" "$BAZEL_WS"

SERVER_LOG="$LOG_DIR/buildbuddy-server.log"
EXECUTOR_LOG="$LOG_DIR/buildbuddy-executor.log"
BUCK2_OUTPUT_LOG="$LOG_DIR/buck2-build.log"
BAZEL_OUTPUT_LOG="$LOG_DIR/bazel-build.log"
BUILD_ID_FILE="$LOG_DIR/buck2-build-id.txt"

SERVER_PID=""
EXECUTOR_PID=""
BUCK2_INVOCATION_URL=""
BAZEL_INVOCATION_URL=""

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
    printf '  %s\n' "$BAZEL_OUTPUT_LOG"
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
  [[ ! -e "$worktree_dir" ]] || die "Worktree path already exists: $worktree_dir"

  if git -C "$repo" show-ref --verify --quiet "refs/heads/$branch"; then
    log "Creating BuildBuddy worktree: $worktree_dir (branch: $branch)"
    git -C "$repo" worktree add "$worktree_dir" "$branch" >/dev/null
  elif git -C "$repo" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
    log "Creating BuildBuddy worktree from origin/$branch: $worktree_dir"
    git -C "$repo" worktree add -b "$branch" "$worktree_dir" "origin/$branch" >/dev/null
  else
    die "BuildBuddy branch not found locally or in origin: $branch"
  fi

  printf '%s\n' "$worktree_dir"
}

is_ready() {
  local url="$1"
  [[ "$(curl -fsS "$url" 2>/dev/null || true)" == "OK" ]]
}

wait_for_ready() {
  local url="$1"
  local timeout_s="$2"
  local pid="${3:-}"
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

write_bazel_workspace() {
  cat >"$BAZEL_WS/MODULE.bazel" <<'EOF'
module(name = "bes_compare")
EOF
  cat >"$BAZEL_WS/BUILD.bazel" <<'EOF'
genrule(
    name = "main",
    outs = ["main.txt"],
    cmd = "echo 'Hello from a Bazel BES comparison build' > $@",
)
EOF
}

capture_invocation() {
  local name="$1"
  local url="$2"
  local snapshot="$PLAYWRIGHT_DIR/$name-snapshot.txt"
  local screenshot="$PLAYWRIGHT_DIR/$name-overview.png"
  local screenshot_log="$PLAYWRIGHT_DIR/$name-screenshot.txt"

  [[ -n "$url" ]] || return 0
  command -v npx >/dev/null 2>&1 || die "npx is required for Playwright capture"

  local codex_home="${CODEX_HOME:-$HOME/.codex}"
  local pwcli="$codex_home/skills/playwright/scripts/playwright_cli.sh"
  [[ -f "$pwcli" ]] || die "Playwright wrapper not found: $pwcli"

  log "Capturing BuildBuddy UI for $name"
  (
    cd "$PLAYWRIGHT_DIR"
    bash "$pwcli" open "$url" --headed >/dev/null
    bash "$pwcli" snapshot >"$snapshot" || true
    if bash "$pwcli" screenshot >"$screenshot_log"; then
      generated_path=""
      generated_path="$(
        sed -n 's/.*](\(.*\)).*/\1/p' "$screenshot_log" | head -n1
      )"
      if [[ -n "$generated_path" ]]; then
        if [[ "$generated_path" != /* ]]; then
          generated_path="$PWD/$generated_path"
        fi
        if [[ -f "$generated_path" ]]; then
          cp "$generated_path" "$screenshot"
        fi
      fi
    fi
  )
}

WORKTREE_PATH="$(ensure_worktree "$BUILDBUDDY_REPO" "$BUILDBUDDY_BRANCH" "$WORKTREE_DIR")"
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

  if ! wait_for_ready "$SERVER_READY_URL" 300 "$SERVER_PID"; then
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

  if ! wait_for_ready "$EXECUTOR_READY_URL" 300 "$EXECUTOR_PID"; then
    tail -n 120 "$EXECUTOR_LOG" >&2 || true
    die "BuildBuddy executor failed to become ready (see $EXECUTOR_LOG)"
  fi
fi

log "Running Buck2 hello_world with Bazel-compatible BES events"
(
  cd "$HELLO_WORLD_DIR"
  "$BUCK2_BIN" clean \
    --config "bes.backend=" \
    --isolation-dir "buildbuddy-bes-compare" >/dev/null 2>&1 || true
)
set +e
(
  cd "$HELLO_WORLD_DIR"
  "$BUCK2_BIN" build //:main \
    --isolation-dir "buildbuddy-bes-compare" \
    --prefer-remote \
    --write-build-id "$BUILD_ID_FILE" \
    --config "bes.backend=grpc://localhost:1985" \
    --config "bes.results_url=http://localhost:8080/invocation/" \
    --config "bes.event_format=bazel" \
    --config "buck2_re_client.engine_address=grpc://localhost:1985" \
    --config "buck2_re_client.action_cache_address=grpc://localhost:1985" \
    --config "buck2_re_client.cas_address=grpc://localhost:1985" \
    --config "buck2_re_client.tls=false" \
    --config "build.execution_platforms=root//platforms:buildbuddy-rbe"
) 2>&1 | tee "$BUCK2_OUTPUT_LOG"
buck2_status=${PIPESTATUS[0]}
set -e

buck2_build_id=""
if [[ -s "$BUILD_ID_FILE" ]]; then
  buck2_build_id="$(tr -d '[:space:]' <"$BUILD_ID_FILE")"
else
  buck2_build_id="$(grep -Eo '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' "$BUCK2_OUTPUT_LOG" | tail -n1 || true)"
fi
if [[ -n "$buck2_build_id" ]]; then
  BUCK2_INVOCATION_URL="$(build_invocation_url "http://localhost:8080/invocation/" "$buck2_build_id")"
fi

write_bazel_workspace
BAZEL_INVOCATION_ID="$(new_uuid)"
BAZEL_INVOCATION_URL="$(build_invocation_url "http://localhost:8080/invocation/" "$BAZEL_INVOCATION_ID")"

log "Running comparable Bazel build"
set +e
(
  cd "$BAZEL_WS"
  "$BAZEL_BIN" build //:main \
    --bes_backend=grpc://localhost:1985 \
    --bes_results_url=http://localhost:8080/invocation/ \
    --invocation_id="$BAZEL_INVOCATION_ID" \
    --color=no \
    --curses=no
) 2>&1 | tee "$BAZEL_OUTPUT_LOG"
bazel_status=${PIPESTATUS[0]}
set -e

if [[ "$CAPTURE_PLAYWRIGHT" -eq 1 ]]; then
  capture_invocation "buck2" "$BUCK2_INVOCATION_URL"
  capture_invocation "bazel" "$BAZEL_INVOCATION_URL"
fi

printf '\n'
printf 'Artifacts directory: %s\n' "$OUTPUT_DIR"
printf 'Buck2 terminal output log: %s\n' "$BUCK2_OUTPUT_LOG"
printf 'Bazel terminal output log: %s\n' "$BAZEL_OUTPUT_LOG"
printf 'BuildBuddy server log: %s\n' "$SERVER_LOG"
printf 'BuildBuddy executor log: %s\n' "$EXECUTOR_LOG"
if [[ -n "$BUCK2_INVOCATION_URL" ]]; then
  printf 'Buck2 invocation URL: %s\n' "$BUCK2_INVOCATION_URL"
else
  printf 'Buck2 invocation URL: unavailable (could not determine build ID)\n'
fi
printf 'Bazel invocation URL: %s\n' "$BAZEL_INVOCATION_URL"
if [[ "$CAPTURE_PLAYWRIGHT" -eq 1 ]]; then
  printf 'Playwright artifacts: %s\n' "$PLAYWRIGHT_DIR"
fi

printf '\n'
printf 'export BUCK2_INVOCATION_URL=%q\n' "$BUCK2_INVOCATION_URL"
printf 'export BAZEL_INVOCATION_URL=%q\n' "$BAZEL_INVOCATION_URL"

if [[ "$buck2_status" -ne 0 || "$bazel_status" -ne 0 ]]; then
  exit 1
fi
