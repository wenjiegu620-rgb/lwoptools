#!/usr/bin/env bash
# delivery-tracker/scripts/report.sh
# 用于封装：查询 → 渲染图片 → 输出图片路径

set -euo pipefail

log() {
  echo "[delivery-tracker][$(date '+%H:%M:%S')] $*" >&2
}

PROJECT="${1:-}"
if [[ -z "$PROJECT" ]]; then
  echo "用法: report.sh <project_name>" >&2
  exit 1
fi

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_MD="/tmp/delivery_${PROJECT}_report.md"
OUT_IMG="/tmp/delivery_${PROJECT}_report.png"
REMOTE_ERR="/tmp/delivery_${PROJECT}_remote_err.log"
USE_REMOTE="${DELIVERY_USE_REMOTE:-1}"
REMOTE_HOST="${DELIVERY_REMOTE_HOST:-root@139.224.244.183}"
REMOTE_BASE_DIR="${DELIVERY_REMOTE_SKILL_DIR:-/root/.agents/skills/delivery-tracker}"
SSH_KEY="${DELIVERY_SSH_KEY:-$HOME/.ssh/delivery_tracker_rds}"
SSH_CONNECT_TIMEOUT="${DELIVERY_SSH_CONNECT_TIMEOUT:-12}"

SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout="${SSH_CONNECT_TIMEOUT}")
if [[ -f "$SSH_KEY" ]]; then
  SSH_ARGS+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
fi

run_local_query() {
  log "开始本地查询（DELIVERY_USE_REMOTE=${USE_REMOTE}）"
  python3 "$BASE_DIR/scripts/query.py" --project "$PROJECT" > "$OUT_MD"
}

run_remote_query() {
  log "远程拉取开始：${PROJECT}（${REMOTE_HOST}）"
  if ! ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" \
    "python3 '$REMOTE_BASE_DIR/scripts/query.py' --project '$PROJECT' --no-save" \
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

# 2) 渲染成 PNG（加宽为 1300px，避免表格右侧被截断）
log "开始渲染 PNG"
python3 "$BASE_DIR/scripts/render.py" --width 1300 --output "$OUT_IMG" < "$OUT_MD" >/dev/null
log "PNG 渲染完成：$OUT_IMG"

# 3) 输出图片路径（给上层调用）
echo "$OUT_IMG"
