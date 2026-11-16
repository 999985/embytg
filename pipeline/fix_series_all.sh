#!/bin/bash
# fix_series_all.sh —— 先并发预热 ffprobe 缓存，再逐部 refresh（Emby serial）
# 依据 E011：shim 缓存命中 ~67ms，未命中 1.5-4.5s；预热 8 并发 → refresh 快 4 倍
set -u
cd /opt/embytg/pipeline
LOG=/tmp/fix_series_all.log
: > "$LOG"

echo "[$(date '+%H:%M:%S')] 1/2 生成计划" | tee -a "$LOG"
python3 refresh_series.py --plan >>"$LOG" 2>&1 || exit 1

echo "[$(date '+%H:%M:%S')] 2/2 预热 ffprobe 缓存（8 并发）" | tee -a "$LOG"
python3 - <<'PY' >>"$LOG" 2>&1
import json, subprocess, os, time
plan = json.load(open('/tmp/refresh_plan.json'))
dirs = []
for p in plan:
    pass
# 从 plan 反查目录名：用 Emby API 太慢，直接按库扫目录
print('需要 refresh 的剧：%d' % len(plan))
PY

# 用 API 拿路径太慢，改用：对每个库整体预热（只预热需要 refresh 的目录会更省，但整体更简单可靠）
for lib in 国产剧 日韩剧 欧美剧 港台剧 国漫 日漫 综艺; do
  D="/opt/embytg/mnt/embytg/TV/$lib"
  [ -d "$D" ] || continue
  echo "[$(date '+%H:%M:%S')] 预热 $lib" | tee -a "$LOG"
  ./prewarm_ffprobe.sh "$D" 8 2>&1 | tail -1 | tee -a "$LOG"
done

echo "[$(date '+%H:%M:%S')] 3/3 逐部 refresh" | tee -a "$LOG"
python3 refresh_series.py --apply --wait 24 >>"$LOG" 2>&1
echo "[$(date '+%H:%M:%S')] 全部完成" | tee -a "$LOG"
