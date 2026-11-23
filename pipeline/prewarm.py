#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""prewarm.py —— 把最近入库的片子「头部」读进 teldrive 磁盘缓存。

为什么需要（2026-11-24，用户口径：起播和播放都要非常舒服）：
    teldrive 的串流有 12GB 稀疏 range 缓存。缓存命中时首字节 0.01s，
    冷读则取决于 Telegram 当时的状况（实测 1.7s ~ 9s 波动，无法靠调参消除）。
    所以提升「起播」最可靠的办法不是调 chunk，而是**提前把头部读进缓存**。
    一集只预热前 32MB，播放器起播需要的索引和首段就都在本地了。

用法：
    python3 prewarm.py --hours 72 --limit 40          # 常规：最近 3 天，最多 40 集
    python3 prewarm.py --limit 5 --dry                # 只看会预热哪些
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse as up
import urllib.request as ur

HERE = os.path.dirname(os.path.abspath(__file__))
PIPE = HERE
HEAD_MB = 32
API = "http://127.0.0.1:8080"


def api_token():
    out = os.popen("bash /opt/embytg/api.sh 2>/dev/null").read().strip().splitlines()
    return out[-1] if out else ""


def recent_items(hours, limit):
    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(PIPE, "tg.db"), uri=True, timeout=20)
    try:
        rows = con.execute(
            "select rel, size, landed_at from resource_state "
            "where landed_at > ? and kind in ('tv','episode','movie') "
            "order by landed_at desc limit ?", (int(time.time()) - hours * 3600, limit)).fetchall()
    finally:
        con.close()
    return rows


def file_id_of(name):
    r = subprocess.run(["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive",
                        "-d", "teldrive", "-t", "-A", "-c",
                        "select id from files where kind='file' and name=%s limit 1;"
                        % ("'" + name.replace("'", "''") + "'")],
                       capture_output=True, text=True, timeout=20)
    return (r.stdout or "").strip()


def warm(fid, mb=HEAD_MB):
    tok = api_token()
    if not tok:
        return False, "no api token"
    req = ur.Request(
        "%s/v1/files/%s/content" % (API, fid),
        headers={"Authorization": "Bearer " + tok,
                 "Range": "bytes=0-%d" % (mb * 1024 * 1024 - 1)})
    t0 = time.time()
    try:
        with ur.urlopen(req, timeout=120) as r:
            n = 0
            while True:
                b = r.read(1 << 20)
                if not b:
                    break
                n += len(b)
        return True, "%.1fMB %.1fs" % (n / 1e6, time.time() - t0)
    except Exception as e:                                  # noqa: BLE001
        return False, str(e)[:70]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=72)
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--mb", type=int, default=HEAD_MB)
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    rows = recent_items(a.hours, a.limit)
    print("  最近 %dh 入库 %d 条，开始预热头部 %dMB" % (a.hours, len(rows), a.mb))
    ok = fail = skip = 0
    for rel, size, ts in rows:
        name = os.path.basename(str(rel or ""))
        if not name:
            continue
        fid = file_id_of(name)
        if not fid:
            skip += 1
            continue
        if a.dry:
            print("   [dry] %s" % name[:60])
            continue
        good, detail = warm(fid, a.mb)
        if good:
            ok += 1
            print("   ✔ %-46s %s" % (name[:46], detail))
        else:
            fail += 1
            print("   ✘ %-46s %s" % (name[:46], detail))
    print("  预热完成：成功 %d / 失败 %d / 未匹配 %d" % (ok, fail, skip))
    return 0


if __name__ == "__main__":
    sys.exit(main())
