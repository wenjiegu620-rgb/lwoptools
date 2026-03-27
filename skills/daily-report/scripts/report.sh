#!/usr/bin/env bash
# daily-report/scripts/report.sh
# 本地封装：优先走远程机器查询，输出 Markdown 到 stdout。

set -euo pipefail

log() {
  echo "[daily-report][$(date '+%H:%M:%S')] $*" >&2
}

TARGET_DATE="${1:-}"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_MD="/tmp/daily_report_${TARGET_DATE:-$(date +%F)}.md"
REMOTE_ERR="/tmp/daily_report_remote_err.log"

USE_REMOTE="${DAILY_REPORT_USE_REMOTE:-1}"
REMOTE_HOST="${DAILY_REPORT_REMOTE_HOST:-root@139.224.244.183}"
REMOTE_BASE_DIR="${DAILY_REPORT_REMOTE_SKILL_DIR:-/root/.agents/skills/daily-report}"
SSH_KEY="${DAILY_REPORT_SSH_KEY:-$HOME/.ssh/delivery_tracker_rds}"
SSH_CONNECT_TIMEOUT="${DAILY_REPORT_SSH_CONNECT_TIMEOUT:-12}"

SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout="${SSH_CONNECT_TIMEOUT}")
if [[ -f "$SSH_KEY" ]]; then
  SSH_ARGS+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
fi

run_local_query() {
  log "开始本地查询（DAILY_REPORT_USE_REMOTE=${USE_REMOTE}）"
  if [[ -n "$TARGET_DATE" ]]; then
    python3 "$BASE_DIR/scripts/query.py" --date "$TARGET_DATE" > "$OUT_MD"
  else
    python3 "$BASE_DIR/scripts/query.py" > "$OUT_MD"
  fi
}

run_remote_query() {
  log "远程拉取开始（${REMOTE_HOST}）"
  local remote_cmd
  if [[ -n "$TARGET_DATE" ]]; then
    remote_cmd="python3 '$REMOTE_BASE_DIR/scripts/query.py' --date '$TARGET_DATE'"
  else
    remote_cmd="python3 '$REMOTE_BASE_DIR/scripts/query.py'"
  fi

  if ! ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "$remote_cmd" \
    > "$OUT_MD" 2> >(tee "$REMOTE_ERR" >&2); then
    log "远程查询失败，最近错误："
    tail -n 30 "$REMOTE_ERR" >&2 || true
    exit 1
  fi
  log "远程拉取完成"
}

if [[ "$USE_REMOTE" == "1" ]]; then
  run_remote_query
else
  run_local_query
fi

if [[ ! -s "$OUT_MD" ]]; then
  echo "查询结果为空：$OUT_MD" >&2
  exit 1
fi

cat "$OUT_MD"
