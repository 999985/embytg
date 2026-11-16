#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 renumber journal 取受影响剧集目录，分批 POST /Library/Media/Updated 让 Emby 重扫。"""
import json
import os
import sys
import time
import urllib.request

CFG = "/opt/embytg/config.json"
_conf = json.load(open(CFG))
TOKEN = _conf.get("emby_token", "")
EMBY = "http://127.0.0.1:8096"
MNT = "/opt/embytg/mnt/embytg"
CTR = "/mnt/embytg"


def post(items):
    body = json.dumps({"Updates": [{"Path": p, "UpdateType": "Modified"} for p in items]}).encode()
    req = urllib.request.Request(EMBY + "/Library/Media/Updated", data=body,
                                 headers={"X-Emby-Token": TOKEN, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status


def main(path):
    j = json.load(open(path))
    dirs = set()
    for d in j["done"]:
        # .../TV/<lib>/<Series>/Season NN/file -> <Series 目录>
        p = d["to"]
        i = p.find("/TV/")
        rest = p[i + 4:].split("/")
        if len(rest) >= 3:
            dirs.add("/".join(rest[:2]))
    paths = [CTR + "/TV/" + x for x in sorted(dirs)]
    print("受影响剧集 %d 部" % len(paths))
    ok = 0
    for i in range(0, len(paths), 50):
        chunk = paths[i:i + 50]
        try:
            st = post(chunk)
            ok += len(chunk)
            print("  batch %d -> %s (%d)" % (i // 50 + 1, st, len(chunk)))
        except Exception as e:
            print("  batch %d FAIL %s" % (i // 50 + 1, e))
        time.sleep(0.5)
    print("已通知 %d 条路径" % ok)


if __name__ == "__main__":
    main(sys.argv[1])
