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
import os
import sys
import time
import urllib.request

CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
B = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
T = CFG["emby_token"]

HERE = os.path.dirname(os.path.abspath(__file__))
OBS = os.path.join(HERE, "logs", ".scan_obs.json")
SCAN_LIMIT_MIN = 60          # 观测到的 Running 时长超过 60 分钟 = 视为卡死


def _obs_load():
    try:
        with open(OBS, encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:                                          # noqa: BLE001
        return {}


def _obs_save(d):
    try:
        os.makedirs(os.path.dirname(OBS), exist_ok=True)
        tmp = OBS + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f)
        os.replace(tmp, OBS)
    except Exception:                                          # noqa: BLE001
        pass


def observe(scan, now=None):
    """返回本次扫描的**真实**已跑时长（分钟）与卡死判定。

    2026-11-26（E218）：**不能再拿 `LastExecutionResult.StartTimeUtc` 当本次开始时间。**
    Emby 4.10 只在执行**结束**时才写 LastExecutionResult；State=Running 期间这个字段
    仍是**上一次**执行的结果。20:17 起的那次扫描真跑了 23 分钟，但它上一次执行是
    11-19 的（Cancelled），于是 autoops 算出「已跑 10281.7 分钟」，P1 误报并顺手把
    一次正常扫描取消掉（20:40:21 Cancelled）。

    正确口径：自己观测「第一次看到 Running 的时刻」并落盘（进程重启也不丢），
    时长 = now - first_seen。另外用 LastExecutionResult 的 (Start,End) 键做守卫：
    键变了 = 中间有执行结束过 → 当前这轮是新的，重新计时（覆盖「两次巡检之间
    扫描结束又重启」这种观测盲区）。

    返回 dict：state / progress / minutes / stuck / observed_from
    """
    now = time.time() if now is None else float(now)
    res = scan.get("LastExecutionResult") or {}
    running = scan.get("State") == "Running"
    pct = round(float(scan.get("CurrentProgressPercentage") or 0), 1)
    res_key = "%s|%s" % (res.get("StartTimeUtc"), res.get("EndTimeUtc"))
    obs = _obs_load()

    if not running:
        # 记下当前执行键：下次看到 Running 时若键没变，说明这还是同一轮（不可能，因为
        # 非 Running 键已固定）→ 总之清 active，下次 Running 必然从 0 起算。
        if obs.get("active") or obs.get("res_key") != res_key:
            _obs_save({"active": False, "res_key": res_key})
        return {"state": scan.get("State"), "progress": pct, "minutes": None,
                "stuck": False, "observed_from": None}

    first = obs.get("first_seen") if obs.get("active") else None
    if first and obs.get("res_key") not in (None, res_key):
        first = None                     # 中间有执行结束 → 当前是新的一轮
    if not first:
        first = now
    minutes = round((now - float(first)) / 60, 1)
    _obs_save({"active": True, "first_seen": float(first), "res_key": res_key,
               "pct": pct, "seen_at": now})
    return {"state": scan.get("State"), "progress": pct, "minutes": minutes,
            "stuck": minutes > SCAN_LIMIT_MIN, "observed_from": float(first)}


def main():
    d = json.loads(urllib.request.urlopen(
        urllib.request.Request(B + "/ScheduledTasks", headers={"X-Emby-Token": T}), timeout=60).read().decode())
    scan = [t for t in d if "Scan media" in (t.get("Name") or "")][0]
    res = scan.get("LastExecutionResult") or {}
    obs = observe(scan)
    minutes = obs["minutes"]
    out = {"state": scan.get("State"), "progress": obs["progress"],
           "minutes": minutes, "stuck": bool(obs["stuck"]),
           "last": {"status": res.get("Status"), "start": res.get("StartTimeUtc"),
                    "end": res.get("EndTimeUtc"), "key": res.get("Key")}}
    if "--json" in sys.argv:
        print(json.dumps(out, ensure_ascii=False))
        return 0
    print("== Emby 全库扫描 ==")
    print("  当前: %s %.1f%%%s" % (out["state"], out["progress"],
                                   ("，已跑 %.0f 分钟" % minutes) if minutes is not None else ""))
    print("  上次: %s  %s → %s  触发键=%s" % (out["last"]["status"], out["last"]["start"],
                                              out["last"]["end"], out["last"]["key"]))
    if out["stuck"]:
        print("  ⚠ 卡死判定：观测到的 Running 已 >%d 分钟，建议看 ffprobe 进程" % SCAN_LIMIT_MIN)
    print("  触发来源：只有脚本显式 POST /ScheduledTasks/Running；项目内调用点 ="
          " ops1_autopilot L3 第5/6步、ingest_sop.sh 第5/6步（均调用 _scan_now.py）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
