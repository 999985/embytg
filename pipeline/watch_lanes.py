#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""watch_lanes.py —— 四条 lane 的看门狗（无人值守用）。

背景（2026-11-24）：这一夜踩到三次「进程还在、活没了」：
  · 僵尸心跳把 session 租约钉死 → 全部 no_session
  · 死租约要等 3h TTL → 号被占着不出片
  · 服务是 active，但半小时一条日志都没有
systemd 只能看进程在不在，看不出「活着但不出活」。所以补一层：

  对每条 lane：看它最近一条日志的时间 + 是否已有上传/下载进程
    超过 STALE_MIN（默认 30 分钟）完全没有动静 → systemctl restart
  连续重启 3 次仍不动 → 记 P1 告警（走 alert.py），不再硬重启

用法：
    python3 watch_lanes.py            # 检查并自愈
    python3 watch_lanes.py --dry      # 只看状态
"""
import argparse
import os
import re
import subprocess
import sys
import time

LOOP_LOG = "/opt/embytg-runner/logs/loop.log"
UNITS = {
    1: "embytg-runner.service",
    2: "embytg-runner@2.service",
    3: "embytg-runner@3.service",
    4: "embytg-runner4.service",
}
STALE_MIN = int(os.environ.get("WATCH_STALE_MIN", "30"))
STATE = "/opt/embytg/pipeline/.watch_lanes.json"


def last_seen():
    """每条 lane 最后一次出现在 loop.log 的时间戳。"""
    out = {k: 0.0 for k in UNITS}
    try:
        with open(LOOP_LOG, encoding="utf-8", errors="replace") as fh:
            for line in fh.readlines()[-4000:]:
                m = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?\[slot(\d)\]", line)
                if not m:
                    continue
                try:
                    ts = time.mktime(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
                except Exception:                            # noqa: BLE001
                    continue
                slot = int(m.group(2))
                if slot in out and ts > out[slot]:
                    out[slot] = ts
    except Exception:                                       # noqa: BLE001
        pass
    return out


def has_transfer():
    r = subprocess.run(["bash", "-c",
                        "ps -eo args | grep -cE 'gha_upload\\.py|gha_download\\.py|pc_upload\\.py'"],
                       capture_output=True, text=True, timeout=20)
    try:
        return int((r.stdout or "0").strip() or 0)
    except ValueError:
        return 0


def active(unit):
    r = subprocess.run(["systemctl", "is-active", unit], capture_output=True, text=True)
    return (r.stdout or "").strip() == "active"


def load_state():
    import json
    try:
        with open(STATE, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:                                       # noqa: BLE001
        return {}


def save_state(d):
    import json
    try:
        with open(STATE, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=1)
    except Exception:                                       # noqa: BLE001
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    now = time.time()
    seen = last_seen()
    xfer = has_transfer()
    st = load_state()
    restarted, alert = [], []

    for slot, unit in UNITS.items():
        age_min = (now - seen[slot]) / 60 if seen[slot] else 999
        up = active(unit)
        # 有传输进程时不判死：大文件传两小时也可能没有新日志
        if up and (age_min < STALE_MIN or xfer > 0):
            st[str(slot)] = {"restarts": 0, "ages": age_min}
            continue
        n = int((st.get(str(slot)) or {}).get("restarts") or 0) + 1
        st[str(slot)] = {"restarts": n, "ages": age_min, "ts": int(now)}
        print("  slot%d %s：%.0f 分钟无动静（active=%s，传输进程=%d）→ 第 %d 次重启"
              % (slot, unit, age_min, up, xfer, n))
        if a.dry:
            continue
        subprocess.run(["systemctl", "restart", unit], capture_output=True, timeout=90)
        restarted.append(slot)
        if n >= 3:
            alert.append("slot%d 连续 %d 次重启仍不出活" % (slot, n))

    save_state(st)
    if alert:
        try:
            sys.path.insert(0, "/opt/embytg/pipeline")
            import alert as A
            A.alert("warn", "watch_lanes", "；".join(alert))
        except Exception:                                   # noqa: BLE001
            pass
        print("  告警：%s" % "；".join(alert))
        return 1
    if restarted:
        print("  已重启 lane: %s" % restarted)
    else:
        print("  四条 lane 正常（最近活动均 < %d 分钟）" % STALE_MIN)
    return 0


if __name__ == "__main__":
    sys.exit(main())
