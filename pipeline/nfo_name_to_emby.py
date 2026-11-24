#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nfo_name_to_emby.py —— 按 E007 的「NFO 是唯一真源 + 直写」把 Episode 的 Name 写成 NFO 里的 <title>

为什么需要它（文档依据）：
  - PROJECT.md §一：刮削是 `nfo_gen.py` 直接生成 NFO 写进 TG 挂载，**不靠 Emby 刮**；
    Emby 扫描只是本地文件 IO。所以「Emby 里 Name 不对」不是刮削没跑，而是 NFO 没被读。
  - ERRORS E007：**别再指望 Refresh Metadata 重读本地 NFO**（本环境 provider 阶段不跑），
    要按 Luke 的做法 `GET /Users/{uid}/Items/{id}` 取完整对象 → 覆盖字段 → `POST /Items/{id}` → 复核。

与已有脚本的分工：
  - `nfo_to_emby.py`  → Overview / Genres（Series/Movie 级）
  - `todb_meta.py`    → Name/Overview/PremiereDate（走 TODB 在线查询，慢、且季错位时查不到）
  - 本脚本            → Episode **Name**，直接读磁盘 NFO，规模无关、不联网、不依赖季结构

用法：
  cd /opt/embytg/pipeline
  python3 nfo_name_to_emby.py --scan                 # 只统计差异
  python3 nfo_name_to_emby.py --apply --workers 8
  python3 nfo_name_to_emby.py --apply --lib 综艺
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
from concurrent.futures import ThreadPoolExecutor

c = json.load(open("/opt/embytg/config.json"))
T = c["emby_token"]
B = c.get("emby_url", "http://127.0.0.1:8096").rstrip("/")
HOST = "/opt/embytg/mnt/embytg"
CTR = "/mnt/embytg"
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S)


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


def libs():
    _, d = api("/Library/MediaFolders")
    return [x for x in (d.get("Items") or []) if x.get("CollectionType") == "tvshows"]


def episodes(lib_id):
    out, start = [], 0
    while True:
        q = urllib.parse.urlencode({"ParentId": lib_id, "Recursive": "true", "IncludeItemTypes": "Episode",
                                    "Fields": "Name,Path", "Limit": 500, "StartIndex": start})
        _, d = api("/Items?" + q)
        it = d.get("Items") or []
        out += it
        if len(out) >= (d.get("TotalRecordCount") or 0) or not it:
            return out
        start += len(it)


def nfo_title(path):
    host = path.replace(CTR, HOST)
    nfo = os.path.splitext(host)[0] + ".nfo"
    if not os.path.exists(nfo):
        return None, "no_nfo"
    try:
        with open(nfo, encoding="utf-8", errors="ignore") as f:
            s = f.read()
    except OSError:
        return None, "nfo_unreadable"
    m = TITLE_RE.search(s)
    if not m or not m.group(1).strip():
        return None, "no_title"
    return m.group(1).strip(), "ok"


def fix_one(uid, e, apply):
    want, why = nfo_title(e.get("Path") or "")
    if not want:
        return why
    cur = (e.get("Name") or "").strip()
    if cur == want:
        return "same"
    if not apply:
        return "dry"
    st, full = api("/Users/%s/Items/%s" % (uid, e["Id"]))
    if st != 200 or not isinstance(full, dict):
        return "get_fail:%s" % st
    full["Name"] = want
    st2, _ = api("/Items/%s" % e["Id"], "POST", full, timeout=180)
    if st2 not in (200, 204):
        return "post_fail:%s" % st2
    return "ok"


def run(apply=False, lib=None, workers=8, series_dirs=None):
    uid = admin_uid()
    print("admin uid=%s" % uid, flush=True)
    stats, lock = {}, threading.Lock()
    t0 = time.time()
    filters = [x.rstrip("/") for x in (series_dirs or []) if x]
    # 宿主路径（/opt/embytg/mnt/embytg/...）与容器路径（/mnt/embytg/...）都收：
    # Emby 的 Item.Path 是容器路径，传宿主路径会静默匹配 0 条（2026-11-24 踩过一次，
    # 看着「定向集=0」以为没缺口，实际是根本没查）。
    filters = [f.replace(HOST, CTR, 1) if f.startswith(HOST) else f for f in filters]
    _matched = 0
    for L in libs():
        name = L.get("Name")
        if lib and name != lib:
            continue
        eps = episodes(L["Id"])
        if filters:
            eps = [e for e in eps
                   if any(d in (e.get("Path") or "") for d in filters)]
            _matched += len(eps)
            print("== %-8s 定向集=%d（过滤 %d 剧目录）" % (
                name, len(eps), len(filters)), flush=True)
        if not eps:
            continue
        with ThreadPoolExecutor(max_workers=workers) as ex:
            res = list(ex.map(lambda e: fix_one(uid, e, apply), eps))
        with lock:
            for r in res:
                stats[r] = stats.get(r, 0) + 1
        print("== %-8s 集=%-5d 累计=%s  %.0fs" % (name, len(eps), stats, time.time() - t0), flush=True)
    if filters and _matched == 0:
        print("! 过滤目录一条都没匹配上（Emby Item.Path 是容器路径 %s/...）：%s"
              % (CTR, filters), file=sys.stderr)
    print("\n结果: %s  耗时 %.0fs" % (stats, time.time() - t0), flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series-dir", action="append", default=[],
                    help="只处理路径包含该目录的集（可重复，post_ingest 定向收尾用）")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    run(apply=a.apply, lib=a.lib, workers=a.workers, series_dirs=a.series_dir)
