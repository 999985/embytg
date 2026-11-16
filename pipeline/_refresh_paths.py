#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 journal 里变动的剧集目录，逐个刷新对应 Emby Series（比整库重扫快得多）。

用法: python3 _refresh_paths.py fix_naming_20261109_181925.json [...]
"""
import json
import os
import sys
import urllib.parse
import urllib.request

import embytg_conf as C

E = C.cfg("emby_url", "http://127.0.0.1:8096").rstrip("/")
T = C.cfg("emby_api_key") or C.cfg("emby_token")
HOST = "/opt/embytg/mnt/embytg"
CTR = "/mnt/embytg"


def get(u):
    return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={"X-Emby-Token": T}), timeout=60))


def post(u):
    r = urllib.request.Request(u, data=b"", method="POST", headers={"X-Emby-Token": T})
    with urllib.request.urlopen(r, timeout=60) as x:
        return x.status


dirs = set()
for jp in sys.argv[1:]:
    j = json.load(open(jp))
    for d in j.get("done") or []:
        p = d["from"] if d["from"].startswith(HOST) else d["to"]
        # .../TV/库/剧集目录/Season NN/file -> 剧集目录
        parts = p.split(os.sep)
        if "TV" in parts:
            i = parts.index("TV")
            if len(parts) > i + 2:
                dirs.add(os.sep.join(parts[: len(parts) - 2]))

print("受影响剧集目录 %d 个" % len(dirs))
series = get(E + "/Items?Recursive=true&IncludeItemTypes=Series&Fields=Path&Limit=5000")["Items"]
by_path = {}
for s in series:
    p = (s.get("Path") or "").replace(CTR, HOST)
    by_path[p] = s["Id"]
ok = miss = 0
for d in sorted(dirs):
    sid = by_path.get(d)
    if not sid:
        miss += 1
        if miss <= 5:
            print("   ? 未找到:", d.replace(HOST, ""))
        continue
    try:
        post(E + "/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default&ImageRefreshMode=Default&ReplaceAllMetadata=false&ReplaceAllImages=false" % sid)
        ok += 1
    except Exception as e:
        print("   ! %s %s" % (os.path.basename(d)[:40], str(e)[:60]))
print("已触发刷新 %d 部，未找到 %d 部" % (ok, miss))
