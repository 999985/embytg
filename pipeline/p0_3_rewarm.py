#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""改名后 ffprobe 缓存预热点（E011/P0-3 配套），支持多份 journal 串联。

renumber_todb / todb_titles 多次改名后，Emby 重扫时 path 变了 → shim key 变了 → 全 miss →
每文件重新 ffprobe（~2s）。本脚本把「最初路径」的缓存复制到「最终路径」的 key 上。

用法: python3 p0_3_rewarm.py renumber_xxx.json todb_titles_xxx.json [...]
"""
import hashlib
import json
import os
import sys

CACHE = "/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/cache"
HOST = "/opt/embytg/mnt/embytg"
CTR = "/mnt/embytg"
ARGV = "|-threads|0|-v|info|-print_format|json|-show_streams|-show_chapters|-show_format|-show_data"


def key(path, size):
    return hashlib.sha256(("%s|%s|%s" % (path, size, ARGV)).encode()).hexdigest()


def to_ctr(p):
    return CTR + p[len(HOST):] if p.startswith(HOST) else None


def chain(paths):
    """final -> original（沿 journal 反向串联）"""
    inv = {}
    for jp in paths:
        try:
            j = json.load(open(jp))
        except Exception:
            continue
        for d in j.get("done") or []:
            origin = inv.pop(d["from"], d["from"])
            inv[d["to"]] = origin
    return inv


def main(paths):
    m = chain(paths)
    print("最终路径 %d 条" % len(m))
    hit = miss = nocache = 0
    for final, origin in m.items():
        if not os.path.exists(final):
            miss += 1
            continue
        size = os.path.getsize(final)
        a, b = to_ctr(origin), to_ctr(final)
        if not a or not b:
            miss += 1
            continue
        fa = os.path.join(CACHE, key(a, size) + ".json")
        fb = os.path.join(CACHE, key(b, size) + ".json")
        if os.path.exists(fb):
            hit += 1
            continue
        if not os.path.exists(fa):
            nocache += 1
            continue
        try:
            with open(fa, "rb") as f:
                data = f.read()
            tmp = fb + ".tmp"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, fb)
            hit += 1
        except Exception:
            miss += 1
    print("预热 %d 条；原路径无缓存 %d 条；缺失/失败 %d 条" % (hit, nocache, miss))


if __name__ == "__main__":
    main(sys.argv[1:])
