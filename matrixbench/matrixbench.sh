#!/bin/bash
# matrixbench.sh - build, deploy, and run matrixbench against rhel-backend.
#
# Assumes:
#   - "rhel-backend" resolves (via /etc/hosts or SSH config) to 192.168.95.2
#   - passwordless / key-based root SSH to rhel-backend
#   - go is installed locally for the 'build' step (needs internet for gonum)
#
# Usage:
#   ./matrixbench.sh build
#   ./matrixbench.sh deploy
#   ./matrixbench.sh generate [dim] [size-gb]
#   ./matrixbench.sh run-local [sessions] [duration]
#   ./matrixbench.sh run-webdav [proxy|direct] [sessions] [duration]

set -euo pipefail

BACKEND_HOST="rhel-backend"
PROXY_HOST="${PROXY_HOST:-192.168.95.1}"   # set PROXY_HOST env var if you alias this too
BACKEND_DIRECT_HOST="${BACKEND_HOST}"

BIN_LOCAL="./matrixbench"
BIN_REMOTE="/usr/local/bin/matrixbench"

DATASET_DIR="/mnt/webdav_ram/matrixbench/dataset"
RESULTS_DIR="/mnt/webdav_ram/matrixbench/results"
STATS_REMOTE_DIR="/var/log/matrixbench"

cmd_build() {
  echo "==> building matrixbench (needs internet for gonum)"
  go mod tidy
  GOOS=linux GOARCH=amd64 go build -o "$BIN_LOCAL" .
  echo "==> built $BIN_LOCAL"
}

cmd_deploy() {
  echo "==> deploying to $BACKEND_HOST"
  scp "$BIN_LOCAL" "root@${BACKEND_HOST}:${BIN_REMOTE}"
  ssh "root@${BACKEND_HOST}" chmod +x "$BIN_REMOTE"
  echo "==> deployed"
}

cmd_generate() {
  local dim="${1:-4096}"
  local size_gb="${2:-20}"
  echo "==> checking free RAM on $BACKEND_HOST"
  ssh "root@${BACKEND_HOST}" free -h
  echo "==> ensuring results dir exists with correct ownership/SELinux context"
  ssh "root@${BACKEND_HOST}" \
    "mkdir -p '$RESULTS_DIR' && chown apache:apache '$RESULTS_DIR' && chmod 0775 '$RESULTS_DIR' && restorecon -R '$(dirname "$RESULTS_DIR")'"
  echo "==> generating dataset on $BACKEND_HOST (dim=$dim size=${size_gb}GB)"
  ssh "root@${BACKEND_HOST}" \
    "$BIN_REMOTE" generate -dir "$DATASET_DIR" -size-gb "$size_gb" -dim "$dim" -force
}

cmd_run_local() {
  local sessions="${1:-20}"
  local duration="${2:-30m}"
  ssh "root@${BACKEND_HOST}" mkdir -p "$STATS_REMOTE_DIR"
  echo "==> running local (on-box) test: $sessions sessions, $duration"
  ssh "root@${BACKEND_HOST}" \
    "$BIN_REMOTE" run -transport local \
      -dir "$DATASET_DIR" -results-dir "$RESULTS_DIR" \
      -sessions "$sessions" -duration "$duration" \
      -stats "${STATS_REMOTE_DIR}/stats-local-$(date +%s).jsonl" \
      -report-interval 10s
}

cmd_run_webdav() {
  local path="${1:-proxy}"    # proxy | direct
  local sessions="${2:-20}"
  local duration="${3:-30m}"

  local target_host
  case "$path" in
    proxy)  target_host="$PROXY_HOST" ;;
    direct) target_host="$BACKEND_DIRECT_HOST" ;;
    *) echo "run-webdav: first arg must be 'proxy' or 'direct'" >&2; exit 2 ;;
  esac

  echo "==> running WebDAV test from THIS client against $path ($target_host): $sessions sessions, $duration"
  "$BIN_LOCAL" run -transport webdav \
    -webdav-url "http://${target_host}:8080/dav" \
    -sessions "$sessions" -duration "$duration" \
    -stats "matrixbench_stats-webdav-${path}-$(date +%s).jsonl" \
    -report-interval 10s
}

case "${1:-}" in
  build)       cmd_build ;;
  deploy)      cmd_deploy ;;
  generate)    shift; cmd_generate "$@" ;;
  run-local)   shift; cmd_run_local "$@" ;;
  run-webdav)  shift; cmd_run_webdav "$@" ;;
  *)
    echo "Usage: $0 {build|deploy|generate [dim] [size-gb]|run-local [sessions] [duration]|run-webdav {proxy|direct} [sessions] [duration]}" >&2
    exit 1
    ;;
esac