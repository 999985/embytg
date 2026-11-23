#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""movie_catalog_sync.py —— 枚举各源站的**电影**目录，和库内电影比对，列出缺口。

为什么单独写（2026-11-23）：
    用户问「斗破苍穹剧场版：双帝之战」为什么没收录 —— 因为 catalog_sync 只抓
    `IncludeItemTypes=Series`，**电影/剧场版根本不在发现范围里**。实测 emos
    上就有两部斗破剧场版（7.2Mbps / 7.32GB），台账里 0 条。
    这不是漏了一部片，是漏了整类资源。

用法：
    python3 movie_catalog_sync.py --pages 10            # 每源抓 10 页（2000 部）摸底
    python3 movie_catalog_sync.py --pages 999 --write   # 全量并写 .movie_new.json
"""
import argparse
import json
import os
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import src_guard as SG          # noqa: E402

PAGE = 200
OUT = os.path.join(HERE, ".movie_new.json")
DB = os.path.join(HERE, "tg.db")


def library_movies():
    """库内已有电影的 tmdb 集合 + 片名集合（双保险：有些条目没 tmdb）。"""
    tmdb, names = set(), set()
    try:
        con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=20)
        try:
            for (t,) in con.execute(
                    "select distinct tmdb from resource_state "
                    "where lib like 'Movies/%' and tmdb is not null"):
                tmdb.add(str(t))
            for (r,) in con.execute(
                    "select rel from resource_state where lib like 'Movies/%'"):
                names.add(os.path.basename(str(r or ""))[:40])
        finally:
            con.close()
    except Exception as e:                                   # noqa: BLE001
        print("  读台账失败: %s" % str(e)[:60])
    return tmdb, names


def guard():
    cls = [n for n in dir(SG) if "Guard" in n][0]
    return getattr(SG, cls)()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=10)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()

    lib_tmdb, lib_names = library_movies()
    print("  库内电影: tmdb %d 个 / 文件名 %d 个" % (len(lib_tmdb), len(lib_names)))

    g = guard()
    found, per_src = {}, {}
    for st in g.order():
        got, new = 0, 0
        for page in range(a.pages):
            try:
                d = g.get(st, "/Items?IncludeItemTypes=Movie&Recursive=true"
                              "&Fields=ProviderIds,ProductionYear&Limit=%d&StartIndex=%d"
                          % (PAGE, page * PAGE))
            except Exception as e:                           # noqa: BLE001
                print("  %-22s 第 %d 页失败: %s" % (st.label, page, str(e)[:40]))
                break
            items = (d.get("Items") or []) if isinstance(d, dict) else []
            if not items:
                break
            for it in items:
                tmdb = str((it.get("ProviderIds") or {}).get("Tmdb") or "")
                name = (it.get("Name") or "").strip()
                if not name:
                    continue
                if (tmdb and tmdb in lib_tmdb) or (not tmdb and name[:40] in lib_names):
                    continue
                key = tmdb or ("name:" + name)
                if key in found:
                    continue
                found[key] = {"tmdb": tmdb, "name": name,
                              "year": it.get("ProductionYear") or "",
                              "source": st.label, "id": it.get("Id")}
                new += 1
            got += len(items)
            time.sleep(a.sleep)
        per_src[st.label] = {"scan": got, "new": new}
        print("  %-22s 抓到 %5d 部，其中库内没有 %4d 部" % (st.label, got, new))

    print("  合计：源站有、库内没有的电影 %d 部" % len(found))
    if a.write:
        with open(OUT, "w", encoding="utf-8") as fh:
            json.dump({"generated_at": int(time.time()), "count": len(found),
                       "per_source": per_src, "items": list(found.values())},
                      fh, ensure_ascii=False, indent=1)
        print("  已写 %s" % OUT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
