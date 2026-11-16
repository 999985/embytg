#!/usr/bin/env bash
# 磁盘水位守护：cache 涨破阈值或磁盘可用跌破阈值时，停掉消费循环 + 触发 rclone 清理。
# 用法: nohup bash disk_watch.sh >> disk_watch.log 2>&1 &
CACHE_MAX_MB=45000
AVAIL_MIN_MB=12000
cd "$(dirname "$0")"
while true; do
  CACHE_MB=$(du -sm /opt/embytg/cache 2>/dev/null | awk '{print $1}')
  AVAIL_MB=$(df -m / | awk 'NR==2{print $4}')
  ts=$(date '+%H:%M:%S')
  ALERT=0
  if [ -n "$CACHE_MB" ] && [ "$CACHE_MB" -gt "$CACHE_MAX_MB" ]; then ALERT=1; fi
  if [ -n "$AVAIL_MB" ] && [ "$AVAIL_MB" -lt "$AVAIL_MIN_MB" ]; then ALERT=1; fi
  if [ "$ALERT" = "1" ]; then
    echo "$ts ALERT cache=${CACHE_MB}M avail=${AVAIL_MB}M  -> kill consumer, clean cache"
    pkill -f "tgpipe.py apply --from-queue --loop" 2>/dev/null
    RPID=$(pgrep -f "rclone mount embytg:" | head -1)
    if [ -n "$RPID" ]; then
      kill -USR1 "$RPID" 2>/dev/null && echo "$ts sent SIGUSR1 to rclone $RPID"
    fi
  fi
  echo "$ts cache=${CACHE_MB}M avail=${AVAIL_MB}M consumer=$(pgrep -fc 'tgpipe.py apply --from-queue --loop' 2>/dev/null)"
  sleep 60
done