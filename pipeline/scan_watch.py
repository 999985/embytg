#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scan_watch.py —— 谁在触发 Emby 全库扫描？跑了多久？卡住没有？

事实（2026-11-13 查证）：
- 17 个库 `EnableRealtimeMonitor=False`，`Scan media library` 的 `Triggers=[]` → **Emby 自己不会扫**
- 只有脚本显式 `POST /ScheduledTasks/Running/{id}` 才会启动，项目里只有两处：
  `ops1_autopilot.py` 的 L3 第 5/6 步、`ingest_sop.sh` 第 5/6 步（都调 `_scan_now.py`）
- 扫描在 FUSE 上跑 ffprobe，常 30–175 分钟；卡住后会被取消/重启，于是看起来"一直在扫"

用法：
    python3 scan_watch.py            # 人看
    python3 scan_watch.py --json     # autoops 用
"""
import json
import sys
import time
import urllib.request

CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
B = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
T = CFG["emby_token"]


def main():
    d = json.loads(urllib.request.urlopen(
        urllib.request.Request(B + "/ScheduledTasks", headers={"X-Emby-Token": T}), timeout=60).read().decode())
    scan = [t for t in d if "Scan media" in (t.get("Name") or "")][0]
    res = scan.get("LastExecutionResult") or {}
    running = scan.get("State") == "Running"
    minutes = None
    if running and res.get("StartTimeUtc"):
        from datetime import datetime, timezone
        try:
            st = datetime.fromisoformat(res["StartTimeUtc"].replace("Z", "+00:00"))
            minutes = round((datetime.now(timezone.utc) - st).total_seconds() / 60, 1)
        except Exception:
            pass
    out = {"state": scan.get("State"), "progress": round(scan.get("CurrentProgressPercentage") or 0, 1),
           "minutes": minutes, "stuck": bool(running and (minutes or 0) > 60),
           "last": {"status": res.get("Status"), "start": res.get("StartTimeUtc"),
                    "end": res.get("EndTimeUtc"), "key": res.get("Key")}}
    if "--json" in sys.argv:
        print(json.dumps(out, ensure_ascii=False))
        return 0
    print("== Emby 全库扫描 ==")
    print("  当前: %s %.1f%%%s" % (out["state"], out["progress"],
                                   ("，已跑 %.0f 分钟" % minutes) if minutes else ""))
    print("  上次: %s  %s → %s  触发键=%s" % (out["last"]["status"], out["last"]["start"],
                                              out["last"]["end"], out["last"]["key"]))
    if out["stuck"]:
        print("  ⚠ 卡死判定：>60 分钟（FUSE 上 ffprobe 慢），建议看 ffprobe 进程")
    print("  触发来源：只有脚本显式 POST /ScheduledTasks/Running；项目内调用点 ="
          " ops1_autopilot L3 第5/6步、ingest_sop.sh 第5/6步（均调用 _scan_now.py）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
