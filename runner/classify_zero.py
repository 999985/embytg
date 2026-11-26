import json
import subprocess
import sys
import urllib.parse as up
import urllib.request as ur

sys.path.insert(0, "/opt/embytg/pipeline")
from embytg_conf import cfg                              # noqa: E402

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

PSQL = ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive", "-t", "-A", "-F", "|", "-c"]
out = ["零字节 %d 条" % len(items)]
res = {"good": 0, "noparts": 0, "broken": 0, "readfail": 0}
for p in items:
    base = p.split("/")[-1]
    r = subprocess.run(PSQL + ["select count(p.part_no) from files f left join file_parts p on p.file_id=f.id "
                               "where f.name='%s';" % base.replace("'", "''")], capture_output=True, text=True)
    nparts = (r.stdout or "0").strip()
    fp = p.replace("/mnt/embytg", "/opt/embytg/mnt/embytg")
    pr = subprocess.run(["timeout", "25", "ffprobe", "-v", "error", "-show_entries", "format=duration",
                         "-of", "default=nw=1", fp], capture_output=True, text=True)
    if nparts == "0":
        tag = "noparts"
    elif pr.returncode == 0:
        tag = "good"
    elif pr.returncode == 124:
        tag = "readfail"
    else:
        tag = "broken"
    res[tag] += 1
    out.append("  %-9s parts=%-2s %s" % (tag, nparts, base[:46]))
out.append("小计: %s" % res)
open("/tmp/zero_class.txt", "w", encoding="utf-8").write("\n".join(out))
print("\n".join(out))
