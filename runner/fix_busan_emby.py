import sys, json, time, urllib.parse as up, urllib.request as ur, sqlite3
sys.path.insert(0, "/opt/embytg/pipeline")
import emby_scan
from embytg_conf import cfg

NAME = "\u91dc\u5c71\u884c2\uff1a\u534a\u5c9b (2020) {tmdb=581392} - 1080p.mp4"
LIB = "Movies/\u52a8\u4f5c\u7247"
b = cfg("emby_url").rstrip("/")
tk = cfg("emby_token")
q = up.urlencode({"SearchTerm": "\u91dc\u5c71\u884c2", "Recursive": "true", "Fields": "Path,MediaSources"})
d = json.loads(ur.urlopen("%s/Items?%s&api_key=%s" % (b, q, tk), timeout=40).read())
for it in d.get("Items") or []:
    p = it.get("Path") or ""
    if "\u91dc\u5c71\u884c2" in (it.get("Name") or "") and not p.endswith("1080p.mp4"):
        try:
            req = ur.Request("%s/Items/%s?api_key=%s" % (b, it["Id"], tk), method="DELETE")
            with ur.urlopen(req, timeout=30) as r:
                print("  del stale item %s -> %s" % (it["Id"], r.status))
        except Exception as e:                                # noqa: BLE001
            print("  del failed:", str(e)[:50])
time.sleep(3)
print("  scan:", emby_scan.scan_paths(["/mnt/embytg/" + LIB + "/" + NAME]))
con = sqlite3.connect("/opt/embytg/pipeline/tg.db", timeout=30)
con.execute("update resource_state set rel=?, path=? where rel like '%\u91dc\u5c71\u884c2%'",
            (LIB + "/" + NAME, "/opt/embytg/mnt/embytg/" + LIB + "/" + NAME))
con.commit(); con.close()
print("  ledger -> 1080p")
time.sleep(12)
d = json.loads(ur.urlopen("%s/Items?%s&api_key=%s" % (b, q, tk), timeout=40).read())
for it in d.get("Items") or []:
    if "\u91dc\u5c71\u884c2" in (it.get("Name") or ""):
        ms = (it.get("MediaSources") or [{}])[0]
        print("  Emby: %s | %.2fGB | %s" % (it.get("Name"), (ms.get("Size") or 0) / 1e9,
              (it.get("Path") or "")[-26:]))
