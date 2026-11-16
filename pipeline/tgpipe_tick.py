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
    """队列真源是 tg.db 的 pending 表（tg_pending.json 只是遗留文件）。"""
    path = os.path.join(HERE, "tg.db")
    try:
        con = sqlite3.connect("file:%s?mode=ro" % path, uri=True, timeout=10)
    except Exception as e:                             # noqa: BLE001
        print("# 打不开 tg.db: %s" % e, file=sys.stderr, flush=True)
        return 0
    try:
        return int(con.execute("select count(*) from pending").fetchone()[0])
    except Exception as e:                             # noqa: BLE001
        print("# 读 pending 失败: %s" % e, file=sys.stderr, flush=True)
        return 0
    finally:
        con.close()


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
