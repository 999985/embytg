#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tgpipe 定时器入口：队列不足才补扫，然后按批转存。

**为什么不能把 `scan --queue` 和 `apply --from-queue` 直接写成两条 ExecStart：**
`scan --queue` 是**整体重建**（`tgstore.save_json` 对 pending 表 DELETE+INSERT），
于是每条定时器都会把上一轮还没消费的条目冲掉 —— 实测某一轮把 75 条待转存清零，
表现为「转存明明是通的，但每天只出几条」。

所以这里加一层闸：先数 pending，够用就只消费、不重建。
"""
import os
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

MIN_PENDING = int(os.environ.get("TGPIPE_MIN_PENDING", "30"))
BATCH = int(os.environ.get("TGPIPE_BATCH", "20"))
CHANNELS = os.environ.get("TGPIPE_CHANNELS", "luolimeimei,yingshi9999")
SCAN_LIMIT = os.environ.get("TGPIPE_SCAN_LIMIT", "200")


def pending_count():
    """**未消费**条数（总数 - cursor）。

    2026-11-20 事故：原来只数 pending 表行数。队列消费完后行还在（cursor=36/36），
    于是「pending=36 ≥ 阈值 30」永远成立 → 永远不补扫 → 转存链路静默停摆，
    表面上定时器每分钟都成功退出。判据必须和 apply 用同一个真源（tgstore 的 cursor），
    否则两处口径不一致就必然再停一次。
    """
    try:
        sys.path.insert(0, HERE)
        import tgpipe as T
        q = T.load_json(T.PENDING, {"items": [], "cursor": 0})
        total = len(q.get("items") or [])
        cursor = int(q.get("cursor") or 0)
        return max(0, total - cursor)
    except Exception as e:                             # noqa: BLE001
        print("# 读 pending 失败: %s" % e, file=sys.stderr, flush=True)
        return 0


def run(*args):
    cmd = [PY, os.path.join(HERE, "tgpipe.py")] + list(args)
    print("+ tgpipe.py " + " ".join(args), flush=True)
    return subprocess.call(cmd, cwd=HERE)


def main():
    n = pending_count()
    print("# pending=%d（补扫阈值 %d）" % (n, MIN_PENDING), flush=True)
    if n < MIN_PENDING:
        run("scan", "--queue", "--limit", SCAN_LIMIT, "--channels", CHANNELS)
    else:
        print("# 队列够用，本轮只消费不重建", flush=True)
    return run("apply", "--from-queue", "--max-items", str(BATCH))


if __name__ == "__main__":
    sys.exit(main())
