#!/bin/bash
# source_ingest_loop.sh — 云端 24h 自动上片循环
#
# 只复用现有轮子：autoingest.py（缺口规划/预算/失败记忆）
#                  source_pull.py（Hills UA/GET/单源并发 1/全速下载）
#                  pc_register.py --file-id --tgdb（teldrive → state_key）
#                  post_ingest.py + resource_state.py（刮削/生命周期）
#
# 用户 2026-11-11：不再人为压低下载速度；保留请求级安全规则。
set -u
PIPE=/opt/embytg/pipeline
PY=/usr/bin/python3
LOG=$PIPE/logs/source_ingest.log
QUEUE=$PIPE/.autoingest_queue.json
STATE=$PIPE/.source_ingest_state.json
mkdir -p "$PIPE/logs"

# 单实例：systemd 重启/人工执行不会并发写 tg.db
exec 9>/run/embytg-sourceingest.lock
if ! flock -n 9; then
  echo "[$(date '+%F %T')] another source-ingest instance holds the lock; sleep"
  sleep 300
  exit 0
fi

log() { echo "[$(date '+%F %T')] $*" >> "$LOG"; }

log "=== cloud source ingest loop start (pid=$$) ==="
cycle=0
while true; do
  cycle=$((cycle + 1))
  # 6 小时重新规划一次（80 部/轮，约 8 轮覆盖全库）
  if [ "$cycle" -eq 1 ] || [ $((cycle % 12)) -eq 0 ]; then
    log "cycle=$cycle plan"
    nice -n 10 "$PY" "$PIPE/autoingest.py" --plan --limit-series 80 >> "$LOG" 2>&1
  fi

  log "cycle=$cycle run (max_gb=50 max_files=5 max_attempts=3 limit=unlimited)"
  nice -n 10 "$PY" "$PIPE/autoingest.py" --run \
    --max-gb 50 --max-files 5 --max-attempts 3 --limit-mbps 0 >> "$LOG" 2>&1
  rc=$?
  printf '{"ts":"%s","cycle":%d,"rc":%d}\n' "$(date '+%F %T')" "$cycle" "$rc" > "$STATE"
  log "cycle=$cycle done rc=$rc"

  # 控制日志体积，保留最近一份
  if [ "$(stat -c%s "$LOG" 2>/dev/null || echo 0)" -gt 50000000 ]; then
    cp -f "$LOG" "$LOG.1"
    : > "$LOG"
    log "rotated log"
  fi
  sleep 1800
done
