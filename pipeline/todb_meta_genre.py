#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把本地 NFO 的元数据写回 Emby 库，修复被 TMDB/TVDB 覆盖掉的字段。

背景（2026-11-13，用户指出「不要 TVDB，以 https://theotherdb.org/ 为准」）：
  项目定位是「脚本前置生成 NFO，Emby 只读 NFO」。但 17 个库的
  Series.MetadataFetchers 曾经是 ['TheTVDB','TheMovieDb'] —— 里面没有 'Nfo'，
  于是每次刷新 Emby 都只按联网 provider 填库：
    - 703 部剧里 482 部 genre 变成英文（Mini-Series/Drama/Comedy…）
    - 剧名被改成英文（百日谎言 -> 100 Days of Deception）
  NFO 文件本身从未被改过，坏的是 Emby 库里的记录。

  配置已由 fix_providers.py 改成 ['Nfo']（不再联网）。
  但只改配置不够：Emby 的 FullRefresh 依然不会把 genre 从 NFO 换掉，
  实测 title/plot/premiered 能读到，genre 读不进去。

2026-11-13 实测出来的写入方法（三条缺一不可）：
  1) 取完整对象：GET /Users/{uid}/Items/{id}（E007：POST 必须传完整对象，
     否则未给字段被 null 化）
  2) Genres 和 GenreItems 必须同时写：
       Genres      = ['喜剧','剧情']                    ← 显示投影
       GenreItems  = [{'Name':'喜剧'}, {'Name':'剧情'}]  ← 真实数据
     只写 Genres 不生效（POST 返回 204 但值不变），必须两个都写。
  3) LockedFields 加 'Genres'，否则下次任何刷新都会再被 provider 覆盖。

数据源：NFO 文件（Kodi 规范），也就是 pipeline 里 nfo_gen.py 以 TODB 为准
生成的那一份。本脚本不联网、不查 TMDB/TVDB，只读磁盘上的 NFO。

用法：
  python3 todb_meta_genre.py --dry                 # 只报告会改哪些
  python3 todb_meta_genre.py --lib 日韩剧
  python3 todb_meta_genre.py --only-bad --workers 4
  python3 todb_meta_genre.py --series 138826 --dry
"""
import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from embytg_conf import cfg

E = cfg("emby_url")
T = cfg("emby_token")
HDR = {"X-Emby-Token": T, "Content-Type": "application/json"}
MNT = cfg("mount") or "/opt/embytg/mnt/embytg"
CN = re.compile(r"[\u4e00-\u9fff]")


def get(path, timeout=120):
    with urllib.request.urlopen(urllib.request.Request(E + path, headers=HDR),
                                timeout=timeout) as f:
        raw = f.read()
    return json.loads(raw) if raw.strip() else None


def post(path, obj, timeout=120):
    r = urllib.request.Request(E + path, method="POST", headers=HDR,
                               data=json.dumps(obj, ensure_ascii=False).encode())
    with urllib.request.urlopen(r, timeout=timeout) as f:
        return f.getcode()


def read_nfo_meta(series_path):
    """从 tvshow.nfo 读 title / genres / plot / year。

    series_path 是 Emby 里的 /mnt/embytg/... 路径，需要映射到本机挂载点。
    """
    if not series_path:
        return None
    rel = series_path
    m = re.search(r"/(?:TV|Movies)/", series_path)
    if m:
        rel = series_path[m.start():]
    local = os.path.join(MNT, rel.lstrip("/"))
    nfo = os.path.join(local, "tvshow.nfo")
    if not os.path.exists(nfo):
        # 电影是 <文件名>.nfo
        return None
    try:
        root = ET.parse(nfo).getroot()
    except Exception:                                       # noqa: BLE001
        return None
    genres = [(g.text or "").strip() for g in root.findall("genre")]
    genres = [g for g in genres if g]
    return {
        "title": (root.findtext("title") or "").strip(),
        "genres": genres,
        "plot": (root.findtext("plot") or "").strip(),
        "year": (root.findtext("year") or "").strip(),
    }


def is_bad(genres):
    g = [x for x in (genres or []) if x]
    if not g:
        return True
    return not any(CN.search(x) for x in g)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series", type=int)
    ap.add_argument("--only-bad", action="store_true")
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()

    uid = (get("/Users") or [{}])[0].get("Id")
    if not uid:
        print("拿不到用户 ID")
        return 1

    if not a.series:
        items = (get("/Items?IncludeItemTypes=Series&Recursive=true"
                     "&Limit=100000&Fields=Genres,Path,Name") or {}).get("Items") or []
        if a.lib:
            items = [i for i in items if ("/%s/" % a.lib) in (i.get("Path") or "")]
    else:
        _sitems = (get("/Items?Ids=%d&Fields=Genres,Path,Name" % a.series) or {}).get("Items") or []
        items = _sitems

    todo = []
    for i in items:
        if a.only_bad and not is_bad(i.get("Genres")):
            continue
        meta = read_nfo_meta(i.get("Path"))
        if not meta or not meta["genres"]:
            continue
        cur = i.get("Genres") or []
        if cur == meta["genres"]:
            continue
        todo.append((i, meta))

    print("范围: %s | 需修 %d 部 / 共 %d 部 | 并发 %d"
          % (a.lib or ("series %s" % a.series if a.series else "全部"),
             len(todo), len(items), a.workers))
    for i, meta in todo[:6]:
        print("  %-24s %s -> %s" % ((i.get("Name") or "")[:22],
                                    (i.get("Genres") or [])[:3], meta["genres"][:3]))
    if a.dry:
        print("  ...（--dry，不写入）")
        return 0

    lock = threading.Lock()
    stat = Counter()
    t0 = time.time()

    def one(pair):
        it, meta = pair
        iid = it["Id"]
        try:
            full = get("/Users/%s/Items/%s" % (uid, iid)) or {}
            touched = []
            if meta["genres"] and full.get("Genres") != meta["genres"]:
                full["Genres"] = meta["genres"]
                full["GenreItems"] = [{"Name": g} for g in meta["genres"]]
                touched.append("Genres")
            # 剧名同样会被 provider 改成英文（百日谎言 -> 100 Days of Deception）。
            # NFO 的 <title> 是 TODB 口径，以它为准。
            if meta["title"] and full.get("Name") != meta["title"]:
                full["Name"] = meta["title"]
                touched.append("Name")
            if not touched:
                with lock:
                    stat["skip"] += 1
                return True
            locked = [x for x in (full.get("LockedFields") or []) if x not in touched]
            full["LockedFields"] = locked + touched
            post("/Items/%s" % iid, full)
            with lock:
                stat["ok"] += 1
            return True
        except Exception as e:                              # noqa: BLE001
            with lock:
                stat["fail"] += 1
                print("  ✗ %s %s" % ((it.get("Name") or "")[:22], str(e)[:50]))
            return False

    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(one, p) for p in todo]
        for n, _ in enumerate(as_completed(futs), 1):
            if n % 50 == 0:
                print("  %d/%d ok=%d fail=%d (%.0fs)"
                      % (n, len(todo), stat["ok"], stat["fail"], time.time() - t0),
                      flush=True)

    print("\n完成: ok=%d fail=%d，用时 %.1f 秒"
          % (stat["ok"], stat["fail"], time.time() - t0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
