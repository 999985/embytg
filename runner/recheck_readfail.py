import json
import subprocess
import sys
import urllib.parse as up
import urllib.request as ur

sys.path.insert(0, "/opt/embytg/pipeline")
from embytg_conf import cfg                              # noqa: E402

TARGETS = ["黑吃黑 S03E05", "权欲 S05E04", "欢乐家长群 S01E39", "钢铁森林 S01E22",
           "紧急呼救 S01E03", "权欲 S05E01", "隐锋 S01E08"]

b = cfg("emby_url").rstrip("/")
tk = cfg("emby_token")
items = []
start = 0
while True:
    q = up.urlencode({"Recursive": "true", "IncludeItemTypes": "Movie,Episode",
                      "Fields": "Path,MediaSources", "StartIndex": start, "Limit": 500, "api_key": tk})
    d = json.loads(ur.urlopen("%s/Items?%s" % (b, q), timeout=90).read())
    its = d.get("Items") or []
    if not its:
        break
    for x in its:
        if not ((x.get("MediaSources") or [{}])[0].get("Size") or 0):
            items.append(x.get("Path") or "")
    start += len(its)
    if start >= d.get("TotalRecordCount", 0):
        break

out = []
for p in items:
    base = p.split("/")[-1]
    if not any(t in base for t in TARGETS):
        continue
    fp = p.replace("/mnt/embytg", "/opt/embytg/mnt/embytg")
    # 先预热头部，再给足 90 秒
    subprocess.run(["timeout", "60", "dd", "if=" + fp, "of=/dev/null", "bs=1M", "count=8"],
                   capture_output=True)
    pr = subprocess.run(["timeout", "90", "ffprobe", "-v", "error", "-show_entries", "format=duration",
                         "-of", "default=nw=1", fp], capture_output=True, text=True)
    dur = (pr.stdout or "").strip()
    if pr.returncode == 0:
        tag, msg = "OK", dur
    elif pr.returncode == 124:
        tag, msg = "timeout", "90s 未读完"
    else:
        lines = (pr.stderr or "").strip().splitlines()
        tag, msg = "bad", (lines[-1][:40] if lines else "无 stderr")
    line = "  %-8s %-40s %s" % (tag, base[:40], msg)
    print(line, flush=True)
    out.append(line)
open("/tmp/readfail_check.txt", "w", encoding="utf-8").write("\n".join(out))
print("\n".join(out))
