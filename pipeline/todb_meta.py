#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""todb_meta.py —— 把 TODB 的 episode_title / episode_description 直接写进 Emby 条目

用户（m00638）在 Emby 里手动把某集「标题」改成了 `第1期：朱亚文杨超越联手推理爱情` ——
即 Emby 的 Episode Name 应该是 TODB 的集标题，而不是文件名。本脚本按同样方式批量写：

  Name         = TODB episode_title
  Overview     = TODB episode_description（有才写）
  PremiereDate = TODB date_air（有才写，且当前为空）

做法沿用 E007 的结论：GET /Users/{uid}/Items/{id} 取完整对象 → 改字段 → POST /Items/{id}
（POST 必须传完整对象，否则未给字段会被 null 化）。不碰挂载、不写 NFO、不产生上传。

用法（服务器 cd /opt/embytg/pipeline）：
  python3 todb_meta.py --scan --series 103147        # 只统计
  python3 todb_meta.py --apply --series 103147
  python3 todb_meta.py --apply --lib 国产剧 --workers 8
"""
import argparse
import json
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import todb_titles as TT

c = json.load(open("/opt/embytg/config.json"))
T = c["emby_token"]
B = "http://127.0.0.1:8096"


def api(path, method="GET", body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(B + path, data=data, method=method,
                                 headers={"X-Emby-Token": T, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "ignore")
            return r.status, (json.loads(raw) if raw.strip().startswith(("{", "[")) else raw)
    except Exception as e:
        return 0, str(e)[:120]


def admin_uid():
    _, us = api("/Users")
    for u in us if isinstance(us, list) else []:
        if (u.get("Policy") or {}).get("IsAdministrator") or u.get("Name") == "admin":
            return u.get("Id")
    return (us or [{}])[0].get("Id")


UID = None


def libraries():
    _, d = api("/Library/MediaFolders")
    return d.get("Items") or []


def series_in_lib(lib_id):
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                                "Fields": "ProviderIds,Path", "Limit": 2000})
    _, d = api("/Items?ParentId=%s&%s" % (lib_id, q))
    return d.get("Items") or []


def episodes(sid):
    out, start = [], 0
    while True:
        q = urllib.parse.urlencode({"StartIndex": start, "Limit": 500,
                                    "Fields": "Path,ProviderIds,Overview,Name",
                                    "SortBy": "SortName"})
        _, d = api("/Shows/%s/Episodes?%s" % (sid, q))
        items = d.get("Items") or []
        out.extend(items)
        if not items or len(out) >= d.get("TotalRecordCount", 0):
            break
        start += len(items)
    return out


def series_tmdb(s):
    pid = (s.get("ProviderIds") or {})
    for k in ("Tmdb", "TmdbId"):
        if pid.get(k):
            try:
                return int(pid[k])
            except Exception:
                pass
    return None


def title_of(s):
    return TT.series_title(s.get("Path") or s.get("Name") or "")


def work_episode(uid, e, info, apply):
    name = e.get("Name") or ""
    want = info["t"]
    if not want or name == want:
        return "same"
    if not apply:
        return "dry"
    st, full = api("/Users/%s/Items/%s" % (uid, e["Id"]))
    if st != 200 or not isinstance(full, dict):
        return "get_fail:%s" % st
    full["Name"] = want
    if info.get("d"):
        full["Overview"] = info["d"]
    if info.get("air") and not full.get("PremiereDate"):
        full["PremiereDate"] = info["air"] + "T00:00:00.0000000Z"
    st2, _ = api("/Items/%s" % e["Id"], "POST", full, timeout=180)
    if st2 not in (200, 204):
        return "post_fail:%s" % st2
    return "ok"


def run(apply=False, lib=None, tid=None, workers=6):
    global UID
    UID = admin_uid()
    print("admin uid=%s" % UID)
    stats = {}
    lock = threading.Lock()
    t0 = time.time()
    libs = libraries()
    for L in libs:
        lname = L.get("Name")
        if lib and lname != lib:
            continue
        if not lib and lname not in ("国产剧", "日韩剧", "欧美剧", "港台剧"):
            continue
        ss = series_in_lib(L["Id"])
        for s in ss:
            mid = series_tmdb(s)
            if not mid or (tid and str(mid) != str(tid)):
                continue
            title, year = title_of(s)
            vid, _ = TT.resolve(title, year)
            if not vid:
                continue
            eps = TT.todb_eps(vid)
            items = episodes(s["Id"])
            jobs = []
            for e in items:
                se, ep = e.get("ParentIndexNumber"), e.get("IndexNumber")
                if se is None or ep is None:
                    continue
                info = eps.get((int(se), int(ep)))
                if not info:
                    continue
                jobs.append((e, info))
            if not jobs:
                continue
            with ThreadPoolExecutor(max_workers=workers) as ex:
                res = list(ex.map(lambda j: work_episode(UID, j[0], j[1], apply), jobs))
            for r in res:
                stats[r] = stats.get(r, 0) + 1
            print("== %-24s todb=%-7s 集=%d 处理=%-4d %.0fs" % (
                s["Name"][:24], vid, len(items), len(jobs), time.time() - t0), flush=True)
    print("\n结果: %s  耗时 %.0fs" % (stats, time.time() - t0))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    run(apply=a.apply, lib=a.lib, tid=a.series, workers=a.workers)
