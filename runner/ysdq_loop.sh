#!/bin/bash
# 机器人补片常驻循环（管家版 2026-11-19）
# 每轮最多 5 条、单轮硬超时 300s（卡住的轮次快速回收，不拖死整个循环），
# 轮间 30s；脚本内 MIN_GAP=60/号 保证不因发太快触发 FloodWait。
cd /opt/embytg/pipeline || exit 1
while true; do
  PYTHONUNBUFFERED=1 timeout -k 10 300 /opt/embytg-runner/venv/bin/python -u ysdq_auto.py \
      --from-pool --limit 5 --slots 4 >> /opt/embytg/logs/ysdq_loop.log 2>&1
  echo "[$(date +%H:%M:%S)] 轮次结束 rc=$?" >> /opt/embytg/logs/ysdq_loop.log
  sleep 30
done
