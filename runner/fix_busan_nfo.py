import json
import sys
import time
import urllib.parse as up
import urllib.request as ur

sys.path.insert(0, "/opt/embytg/pipeline")
import nfo_gen                                            # noqa: E402
from embytg_conf import cfg                               # noqa: E402

NAME = "\u91dc\u5c71\u884c2\uff1a\u534a\u5c9b (2020) {tmdb=581392} - 1080p.mp4"
REL = "Movies/\u52a8\u4f5c\u7247/" + NAME
MOUNT_PATH = "/opt/embytg/mnt/embytg/" + REL

u = ("https://api.themoviedb.org/3/movie/581392?language=zh-CN&append_to_response=credits")
req = ur.Request(u, headers=nfo_gen.HDR)
md = json.loads(ur.urlopen(req, timeout=45).read())
print("  TMDB: %s (%s) 简介 %d 字" % (md.get("title"), md.get("release_date"), len(md.get("overview") or "")))

nfo, poster = nfo_gen.write_movie_nfo(MOUNT_PATH, md)
print("  NFO:", nfo[-40:])
print("  poster:", (poster or "下载失败")[-40:])

b = cfg("emby_url").rstrip("/")
tk = cfg("emby_token")
for u2 in ("%s/Items/174583/Refresh?MetadataRefreshMode=FullRefresh&ImageRefreshMode=FullRefresh&api_key=%s" % (b, tk),):
    try:
        r = ur.Request(u2, method="POST")
        with ur.urlopen(r, timeout=30) as rr:
            print("  refresh ->", rr.status)
    except Exception as e:                                # noqa: BLE001
        print("  refresh failed:", str(e)[:50])

time.sleep(15)
q = up.urlencode({"Ids": "174583", "Fields": "Overview,ImageTags,PremiereDate", "api_key": tk})
it = (json.loads(ur.urlopen("%s/Items?%s" % (b, q), timeout=40).read()).get("Items") or [{}])[0]
print("  简介长度:", len(it.get("Overview") or ""))
print("  图片:", it.get("ImageTags") or {})
print("  上映:", it.get("PremiereDate"))
