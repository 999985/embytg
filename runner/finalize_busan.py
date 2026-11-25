"""釜山行2 洗版收尾：登记 parts → 校验可读 → 替换 Emby 条目。"""
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse as up
import urllib.request as ur

sys.path.insert(0, "/opt/embytg/pipeline")
import tgpipe as TP                      # noqa: E402
from embytg_conf import cfg              # noqa: E402

NAME = "釜山行2：半岛 (2020) {tmdb=581392} - 720p.mp4"
LIB = "Movies/动作片"
P = json.load(open("/opt/embytg-runner/media.busan/parts.json", encoding="utf-8"))
parts = [(int(m), int(s)) for m, s in P["parts"]]
print("  parts:", len(parts), "%.2fGB" % (sum(s for _, s in parts) / 1e9))

# 1) 清掉同名旧登记（如果有）
subprocess.run(["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive", "-c",
                "delete from file_parts where file_id in (select id from files where name='%s'); "
                "delete from files where name='%s';" % (NAME, NAME)], capture_output=True)

# 2) 登记
root = TP.ensure_folder("Movies")
root = TP.ensure_folder("动作片", root)
fid, total = TP.register(root, NAME, parts)
print("  teldrive:", fid[:8], "%.2fGB" % (total / 1e9))

# 3) 台账
rel = "%s/%s" % (LIB, NAME)
con = sqlite3.connect("/opt/embytg/pipeline/tg.db", timeout=30)
con.execute("delete from resource_state where rel like '%釜山行2%'")
con.execute("insert into resource_state(path,rel,lib,kind,title,tmdb,size,landed_at) values(?,?,?,?,?,?,?,?)",
            ("/opt/embytg/mnt/embytg/" + rel, rel, LIB, "movie", "动作片", 581392, total, int(time.time())))
con.commit()
con.close()
print("  台账已写")

# 4) 刷新挂载 + 验证可读（关键：上一版就是这里读不出来）
subprocess.run(["rclone", "rc", "vfs/refresh", "dir=/Movies/动作片"], capture_output=True)
time.sleep(5)
F = "/opt/embytg/mnt/embytg/" + rel
print("  挂载大小 %.2fGB" % (os.path.getsize(F) / 1e9))
r = subprocess.run(["timeout", "120", "ffprobe", "-v", "error", "-show_entries",
                    "format=duration:stream=width,height", "-of", "default=nw=1", F],
                   capture_output=True, text=True)
print("  ffprobe rc=%d %s" % (r.returncode, (r.stdout or r.stderr).strip().replace("\n", " ")[:120]))

# 5) Emby：删掉指向已不存在 1080p 路径的空条目，再扫新文件
b = cfg("emby_url").rstrip("/"); tk = cfg("emby_token")
for iid in (173856,):
    try:
        req = ur.Request("%s/Items/%s?api_key=%s" % (b, iid, tk), method="DELETE")
        with ur.urlopen(req, timeout=30) as rr:
            print("  删空条目 %s → %s" % (iid, rr.status))
    except Exception as e:                                # noqa: BLE001
        print("  删空条目 %s: %s" % (iid, str(e)[:50]))
time.sleep(3)
os.environ["EMBY_SCAN_DEDUP_SEC"] = "0"
import emby_scan                                          # noqa: E402
print("  scan:", emby_scan.scan_paths(["/mnt/embytg/" + rel]))
time.sleep(15)
q = up.urlencode({"SearchTerm": "釜山行2", "Recursive": "true",
                  "Fields": "Path,MediaSources,RunTimeTicks"})
d = json.loads(ur.urlopen("%s/Items?%s&api_key=%s" % (b, q, tk), timeout=40).read())
for it in d.get("Items") or []:
    ms = (it.get("MediaSources") or [{}])[0]
    t = (it.get("RunTimeTicks") or 0) / 1e7
    print("  Emby: %s | %.2fGB | %sx%s | %d分 | %s" % (
        it.get("Name"), (ms.get("Size") or 0) / 1e9, ms.get("Width") or 0,
        ms.get("Height") or 0, int(t) // 60, (it.get("Path") or "")[-28:]))
