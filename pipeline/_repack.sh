#!/bin/bash
# 重打一份「含最终 E005-E007 文档」的备份包；旧包按新时间戳保留，不覆盖。
cd /opt/embytg/pipeline || exit 1
BK=/opt/embytg/backup

echo "=== 1) STOP LOOP (release flock) ==="
# 2026-11-10 20:07 运维守护：systemd 单实例是唯一 owner，先停 unit 免得 60s 后 ReStart 抢锁
systemctl stop embytg-finalize 2>/dev/null || true
pkill -f "auto_finalize.py --loop" || true
sleep 3
ps -eo pid,args | grep "[a]uto_finalize.py --loop" && echo "!! still running" || echo "loop stopped"

echo "=== 2) RESET STATE (keep old archive + LATEST backup) ==="
if [ -f "$BK/finalize_state.json" ]; then cp "$BK/finalize_state.json" "$BK/finalize_state.done_$(date +%s).json"; fi
rm -f "$BK/finalize_state.json" "$BK/LATEST.json"

echo "=== 3) FORCE-PACK with fresh docs ==="
python3 -u auto_finalize.py --force-pack

echo "=== 4) VERIFY NEW LATEST ==="
python3 - <<'PY'
import json
d=json.load(open("/opt/embytg/backup/LATEST.json",encoding="utf-8"))
print("state=%s archive=%s sha=%s size=%d files=%d missing=%s"
      % (d["state"],d["archive_name"],d["sha256"][:16],d["size_bytes"],
         d["files_included"],d["files_missing"]))
print("checks=",d["checks"])
PY

echo "=== 5) RESTART LOOP (from DONE, standby) ==="
if systemctl is-enabled --quiet embytg-finalize 2>/dev/null; then
  systemctl start embytg-finalize && echo "STARTED via systemd (embytg-finalize)"
else
  nohup python3 -u auto_finalize.py --loop >> "$BK/auto_finalize.stdout.log" 2>&1 < /dev/null &
  echo "NEW_PID=$!"
fi
sleep 2
ps -eo pid,etime,args | grep "[a]uto_finalize.py --loop" || true
echo "=== REPACK_DONE ==="
