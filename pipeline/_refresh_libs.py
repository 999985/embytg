#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 journal 找受影响的库，按库 POST /Items/{id}/Refresh?Recursive=true。"""
import json
import sys
import urllib.request

c = json.load(open("/opt/embytg/config.json"))
T = c["emby_token"]
B = "http://127.0.0.1:8096"


def api(p, method="GET"):
    req = urllib.request.Request(B + p, headers={"X-Emby-Token": T}, method=method)
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read().decode("utf-8", "ignore")
        return json.loads(body) if body.strip().startswith(("{", "[")) else body


def main(path):
    j = json.load(open(path))
    libs = set()
    for d in j["done"]:
        p = d["to"]
        i = p.find("/TV/")
        if i < 0:
            continue
        rest = p[i + 4:].split("/")
        if len(rest) >= 2:
            libs.add(rest[0])
    print("受影响库: %s" % sorted(libs))
    roots = api("/Library/MediaFolders").get("Items") or []
    byname = {r["Name"]: r["Id"] for r in roots}
    for name in sorted(libs):
        rid = byname.get(name)
        if not rid:
            print("  ! 库未找到: %s" % name)
            continue
        st = api("/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default"
                 "&ImageRefreshMode=Default&ReplaceAllMetadata=false&ReplaceAllImages=false" % rid,
                 method="POST")
        print("  POST /Items/%s/Refresh (%s) -> %s" % (rid, name, st))


if __name__ == "__main__":
    main(sys.argv[1])
