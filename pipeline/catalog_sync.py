#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""catalog_sync.py —— 把源站的「全部剧集目录」拉下来，和库内比对，生成新剧队列。

2026-11-22 实测（这是关键发现）：
    源站 lite.cn2gias.uk  剧集总数 28716
         xqd.enic65.eu.org  9209
         39.96 mirror       6831
    我们库内只有 989 部剧 / 已知 tmdb 5031 个
    抽样 lite 前 1000 部：439 部不在库里（44%）
    → 单 lite 一站就有约 1.26 万部新剧，而我们一直在手工维护的
      3000 条 new 队列里打转。

用户原话：「正常来说源站很多剧呀」——对，是我们没有去枚举目录。

用法：
    python3 catalog_sync.py --pages 40            # 每源抓 40 页（每页 200 → 8000 部）
    python3 catalog_sync.py --pages 999 --write    # 全量并写队列
"""
import argparse
import json
import os
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import src_guard as SG  # noqa: E402

OUT = os.path.join(HERE, ".catalog_new.json")
PAGE = 200


def library_tmdb():
    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                          uri=True, timeout=30)
    try:
        return {str(r[0]) for r in con.execute(
            "select distinct tmdb from resource_state where tmdb is not null")}
    finally:
        con.close()


def guard():
    cls = [n for n in dir(SG) if "Guard" in n][0]
    return getattr(SG, cls)()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=40)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.4)
    a = ap.parse_args()

    lib = library_tmdb()
    print("  库内已知 tmdb: %d" % len(lib))
    g = guard()
    found = {}
    for st in g.order():
        got = 0
        for page in range(a.pages):
            try:
                d = g.get(st, "/Items?IncludeItemTypes=Series&Recursive=true"
                              "&Fields=ProviderIds,ProductionYear&Limit=%d&StartIndex=%d"
                          % (PAGE, page * PAGE))
            except Exception as e:                        # noqa: BLE001
                print("  %-22s 第 %d 页失败: %s" % (st.label, page, str(e)[:40]))
                break
            items = (d.get("Items") or []) if isinstance(d, dict) else []
            if not items:
                break
            for it in items:
                tmdb = str((it.get("ProviderIds") or {}).get("Tmdb") or "")
                name = it.get("Name") or ""
                year = it.get("ProductionYear") or ""
                if not tmdb or tmdb in lib or tmdb in found:
                    continue
                found[tmdb] = {"tmdb": tmdb, "series": name, "year": year,
                               "source": st.label, "id": it.get("Id")}
            got += len(items)
            time.sleep(a.sleep)
        print("  %-22s 抓了 %d 部，累计新剧 %d" % (st.label, got, len(found)))

    print("  合计：源站里不在库的新剧 %d 部" % len(found))
    if a.write:
        json.dump({"generated_at": int(time.time()), "count": len(found),
                   "items": list(found.values())},
                  open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print("  已写 %s" % OUT)
        # 合并进 new 队列（保留原队列里没有的）
        qf = os.path.join(HERE, ".gha_queue_new.json")
        try:
            q = json.load(open(qf, encoding="utf-8"))
        except Exception:                                 # noqa: BLE001
            q = {"items": []}
        have = {str(x.get("tmdb")) for x in (q.get("items") or [])}
        add = 0
        for tmdb, row in found.items():
            if tmdb in have:
                continue
            # 2026-11-22：必须带上 video_id（源站里这部剧的 Id），
            # 否则 new lane 拿到条目也取不到分集列表 → 全部 no_candidate。
            # 这是「目录抓到了 8120 部，调度却报没活」的根因。
            q.setdefault("items", []).append({
                "tmdb": int(tmdb), "series": row["series"], "year": row["year"],
                "lib": "", "lane": "new",
                "source_label": row["source"], "video_id": row.get("id"),
                "missing": None,
            })
            add += 1
        json.dump(q, open(qf, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print("  new 队列新增 %d 条，现在 %d 条" % (add, len(q.get("items") or [])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
