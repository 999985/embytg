#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""让 Emby 从本地 NFO 重新读取元数据，修复被 TMDB/TVDB 覆盖的英文 genre。

背景（2026-11-13）：
  17 个库的 Series.MetadataFetchers 曾是 ['TheTVDB','TheMovieDb']，
  里面没有 'Nfo'，所以 Emby 刷新时只按 TMDB 填库，把本地 NFO 的中文 genre
  覆盖成英文（703 部里 482 部）。NFO 文件本身没被动过 —— 只是 Emby 库脏了。
  配置已由 fix_providers.py 改成 ['Nfo']，但已入库条目不会自动重读。

做法：
  对每个 Series 发 POST /Items/{id}/Refresh?Recursive=true&MetadataRefreshMode=FullRefresh
  FullRefresh = 丢弃库里的元数据，完全按当前 Fetchers（也就是 Nfo）重建。
  只动元数据，不动文件，不动 ItemId，不动播放记录。

限速与可恢复：
  --workers 并发数（默认 3）。rclone 网络挂载经不起高并发，3 已经够。
  已处理进度写在 .refetch_progress.json，中断后重跑会跳过已完成的条目。

用法：
  python3 refetch_meta_from_nfo.py --dry            # 只统计要刷多少
  python3 refetch_meta_from_nfo.py --lib 日韩剧      # 只刷一个库
  python3 refetch_meta_from_nfo.py --only-bad        # 只刷 genre 仍是英文的
  python3 refetch_meta_from_nfo.py --workers 3
"""
import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from embytg_conf import cfg

E = cfg("emby_url")
T = cfg("emby_token")
HDR = {"X-Emby-Token": T, "Content-Type": "application/json"}
HERE = os.path.dirname(os.path.abspath(__file__))
PROGRESS = os.path.join(HERE, ".refetch_progress.json")
CN = re.compile(r"[\u4e00-\u9fff]")


def api(path, method="GET", timeout=120):
    r = urllib.request.Request(E + path, method=method, headers=HDR)
    with urllib.request.urlopen(r, timeout=timeout) as f:
        raw = f.read()
        return json.loads(raw) if raw.strip() else None


def load_progress():
    try:
        return set(json.load(open(PROGRESS, encoding="utf-8")))
    except Exception:                                       # noqa: BLE001
        return set()


def save_progress(done):
    tmp = PROGRESS + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sorted(done), f)
    os.replace(tmp, PROGRESS)


def is_foreign(genres):
    """genre 里有内容但不是中文 → 被 TMDB 覆盖过。"""
    g = [x for x in (genres or []) if x]
    if not g:
        return False
    return not any(CN.search(x) for x in g)


def collect(lib=None):
    q = "/Items?IncludeItemTypes=Series&Recursive=true&Limit=100000&Fields=Genres,Path"
    items = (api(q) or {}).get("Items") or []
    out = []
    for i in items:
        p = i.get("Path") or ""
        if lib and ("/%s/" % lib) not in p:
            continue
        out.append(i)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--only-bad", action="store_true",
                    help="只刷 genre 仍是非中文的条目")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--reset", action="store_true", help="清空进度")
    a = ap.parse_args()

    if a.reset and os.path.exists(PROGRESS):
        os.remove(PROGRESS)

    items = collect(a.lib)
    if a.only_bad:
        items = [i for i in items if is_foreign(i.get("Genres"))]

    done = load_progress()
    todo = [i for i in items if i.get("Id") not in done]

    print("范围: %s | 待刷 %d 部（已完成 %d）| 并发 %d"
          % (a.lib or "全部库", len(todo), len(items) - len(todo), a.workers))
    if a.dry:
        for i in todo[:8]:
            print("  %s  %s" % (i["Id"][:8], (i.get("Name") or "")[:28]))
        print("  ...（--dry，不实际执行）")
        return 0

    lock = threading.Lock()
    stat = Counter()
    t0 = time.time()

    def one(it):
        iid = it["Id"]
        # 2026-11-13 两个坑：
        # 1) 之前写成 ...&ImageRefreshMode=None 会返回 HTTP 400，而脚本没检查状态码，
        #    485 次全部“成功”其实一次都没刷成。
        # 2) FullRefresh 只是「按当前 Fetchers 重读」，Emby 仍会保留库里已有的值；
        #    要把 TMDB/TVDB 灌进去的英文 genre 换掉，必须带 ReplaceAllMetadata=true。
        url = ("/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=FullRefresh"
               "&ReplaceAllMetadata=true" % iid)
        try:
            r = urllib.request.Request(E + url, method="POST", headers=HDR)
            with urllib.request.urlopen(r, timeout=60) as f:
                code = f.getcode()
            if code not in (200, 204):
                raise RuntimeError("HTTP %s" % code)
            with lock:
                stat["ok"] += 1
                done.add(iid)
            return True
        except Exception as e:                              # noqa: BLE001
            with lock:
                stat["fail"] += 1
                print("  ✗ %s %s" % ((it.get("Name") or "")[:24], str(e)[:50]))
            return False

    try:
        with ThreadPoolExecutor(max_workers=a.workers) as ex:
            futs = [ex.submit(one, i) for i in todo]
            for n, _ in enumerate(as_completed(futs), 1):
                if n % 25 == 0:
                    save_progress(done)
                    el = time.time() - t0
                    rate = n / max(el, 1e-9)
                    print("  %d/%d  ok=%d fail=%d  %.1f/s  预计剩余 %.0f 分钟"
                          % (n, len(todo), stat["ok"], stat["fail"], rate,
                             (len(todo) - n) / max(rate, 1e-9) / 60), flush=True)
    finally:
        save_progress(done)

    print("\n完成: ok=%d fail=%d 用时 %.1f 分钟"
          % (stat["ok"], stat["fail"], (time.time() - t0) / 60))
    print("Emby 需要时间消化 FullRefresh，等 10-30 分钟后重新统计 genre 语种。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
