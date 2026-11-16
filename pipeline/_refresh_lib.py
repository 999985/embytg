#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""触发指定库的全量刷新（Recursive）。用法: python3 _refresh_lib.py 国产剧 日韩剧 ..."""
import json
import sys
import urllib.request

import embytg_conf as C

E = C.cfg("emby_url", "http://127.0.0.1:8096").rstrip("/")
T = C.cfg("emby_api_key") or C.cfg("emby_token")


def get(u):
    return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={"X-Emby-Token": T}), timeout=60))


def post(u):
    r = urllib.request.Request(u, data=b"", method="POST", headers={"X-Emby-Token": T})
    with urllib.request.urlopen(r, timeout=120) as x:
        return x.status


want = set(sys.argv[1:])
libs = get(E + "/Library/MediaFolders")["Items"]
for l in libs:
    if want and l["Name"] not in want:
        continue
    if len(sys.argv) > 1 and "剧" not in l["Name"]:
        continue
    code = post(E + "/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default&ImageRefreshMode=Default&ReplaceAllMetadata=false&ReplaceAllImages=false" % l["Id"])
    print("%-14s id=%-6s -> %s" % (l["Name"], l["Id"], code))
