"""测：近 N 天新增条目里，文件名画质 vs 实际分辨率（宽度优先）是否一致。"""
import json
import re
import sys
import time
import urllib.parse as up
import urllib.request as ur

sys.path.insert(0, "/opt/embytg/pipeline")
from embytg_conf import cfg                              # noqa: E402

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
b = cfg("emby_url").rstrip("/")
tk = cfg("emby_token")
since = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - DAYS * 86400))

bad = []
total = 0
start = 0
while True:
    q = up.urlencode({"Recursive": "true", "IncludeItemTypes": "Movie,Episode",
                      "MinDateCreated": since, "Fields": "Path,MediaSources,MediaStreams",
                      "StartIndex": start, "Limit": 300, "api_key": tk})
    d = json.loads(ur.urlopen("%s/Items?%s" % (b, q), timeout=90).read())
    items = d.get("Items") or []
    if not items:
        break
    for it in items:
        total += 1
        ms = (it.get("MediaSources") or [{}])[0]
        v = [x for x in (ms.get("MediaStreams") or []) if x.get("Type") == "Video"]
        w = (v[0].get("Width") if v else 0) or 0
        h = (v[0].get("Height") if v else 0) or 0
        if not w and not h:
            continue
        act = "2160p" if (w >= 3000 or h >= 1800) else ("1080p" if (w >= 1700 or h >= 1000) else "720p")
        m = re.search(r"- ?(2160p|1080p|720p|480p)\b", it.get("Path") or "", re.I)
        if not m:
            continue
        nm = m.group(1).lower()
        if nm == "480p":
            nm = "720p"
        if nm != act:
            bad.append("%s 名=%s 实=%sx%s→%s" % ((it.get("Name") or "")[:26], m.group(1), w, h, act))
    start += len(items)
    if start >= d.get("TotalRecordCount", 0):
        break

print("近 %d 天条目 %d，画质命名不一致 %d" % (DAYS, total, len(bad)))
for x in bad[:12]:
    print("   ", x)
