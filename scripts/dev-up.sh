#!/usr/bin/env bash
# dev-up.sh — start full local dev stack for cdc-system.
#
# Idempotent: skips any service whose port is already LISTEN.
# Build artifacts placed under /tmp; logs under /tmp/<svc>.log.
# Usage:
#   ./scripts/dev-up.sh             # boot all (auth + cms + admin-api + fe)
#   ./scripts/dev-up.sh status      # report which ports are live
#   ./scripts/dev-up.sh stop        # kill processes started by this script
#
# Assumes Docker stack (gpay-postgres, gpay-postgres-cdc, gpay-postgres-dest,
# gpay-postgres-source, gpay-mongo, gpay-mariadb, gpay-redis, gpay-kafka,
# gpay-kafka-connect, gpay-schema-registry, gpay-nats, gpay-otel,
# gpay-cdc-worker) already running. This script does NOT manage Docker.
#
# Design notes:
#   - cdc-auth-service binds 8081       (Fiber)
#   - cdc-cms-service  binds 8083       (Fiber, JWT-protected /api/*)
#   - centralized-data-service admin-api binds 127.0.0.1:8090
#                                        (Gin, Phase F1 auth + rate limit)
#   - cdc-cms-web Vite dev binds 5173 (IPv6)
#
# Brain (Antigravity) authored this script per workspace
# feature-system-refactor-2026-05 / B2.4. Brain edits .sh (config), not
# .go/.ts/.js/.py/.sql (CLAUDE.md §12).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="/tmp"
PID_DIR="/tmp/cdc-dev-pids"
mkdir -p "$PID_DIR"

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

port_busy() {
  # `lsof` exits 0 if any matching line; suppress output.
  lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

build_go() {
  # build_go <service-dir> <binary-out> <pkg>
  local svc_dir="$1"
  local bin_out="$2"
  local pkg="$3"
  echo "  ↳ go build $svc_dir → $bin_out"
  ( cd "$svc_dir" && go build -o "$bin_out" "$pkg" )
}

start_bg() {
  # start_bg <svc-name> <cwd> <command> [arg ...]
  local svc="$1"; shift
  local cwd="$1"; shift
  local logf="$LOG_DIR/${svc}.log"
  local pidf="$PID_DIR/${svc}.pid"
  echo "  ↳ start $svc → log:$logf"
  ( cd "$cwd" && nohup "$@" >"$logf" 2>&1 & echo $! >"$pidf" )
  # Brief settle so subsequent port_busy reflects reality.
  sleep 2
}

ensure_port() {
  # ensure_port <port> <max-wait-sec>
  local port="$1"
  local maxw="${2:-15}"
  local i=0
  while ! port_busy "$port"; do
    i=$((i+1))
    if [ "$i" -ge "$maxw" ]; then
      echo "  ✗ port $port did not come up in ${maxw}s"
      return 1
    fi
    sleep 1
  done
  echo "  ✓ port $port LISTEN"
}

# -----------------------------------------------------------------------
# Service starters
# -----------------------------------------------------------------------

start_auth() {
  echo "[auth] cdc-auth-service :8081"
  if port_busy 8081; then
    echo "  → already LISTEN — skip"
    return 0
  fi
  build_go "$REPO_ROOT/cdc-auth-service" "/tmp/cdc-auth-service" "./cmd/server"
  start_bg "cdc-auth-service" "$REPO_ROOT/cdc-auth-service" "/tmp/cdc-auth-service"
  ensure_port 8081 20
}

start_cms() {
  echo "[cms] cdc-cms-service :8083"
  if port_busy 8083; then
    echo "  → already LISTEN — skip"
    return 0
  fi
  build_go "$REPO_ROOT/cdc-cms-service" "/tmp/cms-server" "./cmd/server"
  start_bg "cms-server" "$REPO_ROOT/cdc-cms-service" "/tmp/cms-server"
  ensure_port 8083 20
}

start_admin_api() {
  echo "[admin-api] centralized-data-service admin-api :8090"
  if port_busy 8090; then
    echo "  → already LISTEN — skip"
    return 0
  fi
  build_go "$REPO_ROOT/centralized-data-service" "/tmp/cdc-admin" "./cmd/admin-api"
  # ADMIN_API_DEV=true relaxes Phase F1 boot fail-fast for local dev.
  start_bg "cdc-admin" "$REPO_ROOT/centralized-data-service" \
    env ADMIN_API_DEV=true "/tmp/cdc-admin"
  ensure_port 8090 20
}

start_fe() {
  echo "[fe] cdc-cms-web vite :5173"
  if port_busy 5173; then
    echo "  → already LISTEN — skip"
    return 0
  fi
  if [ ! -d "$REPO_ROOT/cdc-cms-web/node_modules" ]; then
    echo "  ↳ npm install (first run)"
    ( cd "$REPO_ROOT/cdc-cms-web" && npm install --silent )
  fi
  start_bg "cdc-cms-web" "$REPO_ROOT/cdc-cms-web" \
    "$REPO_ROOT/cdc-cms-web/node_modules/.bin/vite"
  ensure_port 5173 30
}

# -----------------------------------------------------------------------
# Sub-commands
# -----------------------------------------------------------------------

cmd_status() {
  echo "Port status:"
  for p in 8081 8083 8090 5173; do
    if port_busy "$p"; then
      lsof -nP -iTCP:"$p" -sTCP:LISTEN | awk 'NR==2 {printf "  :%s LISTEN  pid=%s  cmd=%s\n", "'"$p"'", $2, $1}'
    else
      echo "  :$p  not listening"
    fi
  done
}

cmd_stop() {
  echo "Stopping services started by this script (pidfiles in $PID_DIR):"
  shopt -s nullglob
  for pidf in "$PID_DIR"/*.pid; do
    pid="$(cat "$pidf")"
    name="$(basename "$pidf" .pid)"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" && echo "  ✓ killed $name (pid=$pid)"
    else
      echo "  • $name (pid=$pid) already gone"
    fi
    rm -f "$pidf"
  done
}

cmd_up() {
  echo "Starting cdc-system local dev stack…"
  echo "Repo root: $REPO_ROOT"
  echo
  start_auth
  start_cms
  start_admin_api
  start_fe
  echo
  cmd_status
  echo
  echo "Smoke:"
  curl -sS -m 3 -o /dev/null -w "  auth /health → %{http_code}\n"     "http://localhost:8081/health"  || true
  curl -sS -m 3 -o /dev/null -w "  cms  /health → %{http_code}\n"     "http://localhost:8083/health"  || true
  curl -sS -m 3 -o /dev/null -w "  admin /healthz → %{http_code}\n"   "http://localhost:8090/healthz" || true
  curl -sS -m 3 -o /dev/null -w "  fe   /      → %{http_code}\n"      "http://localhost:5173/"        || true
  echo
  echo "Login (operator):"
  echo '  curl -sS -X POST http://localhost:8081/api/auth/login \'
  echo '    -H "Content-Type: application/json" \'
  echo '    -d "{\"username\":\"admin\",\"password\":\"admin123\"}"'
}

# -----------------------------------------------------------------------
# Entry
# -----------------------------------------------------------------------

case "${1:-up}" in
  up)     cmd_up ;;
  status) cmd_status ;;
  stop)   cmd_stop ;;
  *)
    echo "Usage: $0 {up|status|stop}" >&2
    exit 2
    ;;
esac
