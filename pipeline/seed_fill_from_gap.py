#!/usr/bin/env python3
"""seed_fill_from_gap.py —— 把「该补还没补」的季塞进 fill 队列。

缺口口径见 GOAL.md 与 gap_report.py：
    TMDB 该季集数 N − 库里该季实有集数
分层：A(≤5)、B(6-100)、C(>100)。
**只把 A/B 塞进队列**——C 类是长篇动画（哆啦A梦 2577 集、柯南 1221 集），
收了几十集的长番，塞进队列只会把真正该补的剧挤出去。

用法：
    python3 seed_fill_from_gap.py            # 合并进 .gha_queue_fill.json
    python3 seed_fill_from_gap.py --dry-run
"""
import argparse
import collections
import json
import os
import re
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
QUEUE = os.path.join(HERE, ".gha_queue_fill.json")
COUNTS = os.path.join(HERE, ".tmdb_season_counts.json")
DB = os.path.join(HERE, "tg.db")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-missing", type=int, default=100, help="超过这个数判 C 类，不入队")
    a = ap.parse_args()

    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=25)
    rows = con.execute("select rel, tmdb from resource_state "
                       "where kind='episode' and rel like 'TV/%'").fetchall()
    con.close()

    seas = collections.defaultdict(lambda: {"eps": set(), "tmdb": None, "lib": ""})
    for rel, tmdb in rows:
        m = re.search(r"^(TV/[^/]+)/([^/]+)/Season (\d+)/", rel)
        e = re.search(r"S\d+E(\d+)", rel.split("/")[-1])
        if not (m and e):
            continue
        k = (m.group(2), int(m.group(3)))
        seas[k]["eps"].add(int(e.group(1)))
        seas[k]["tmdb"] = seas[k]["tmdb"] or tmdb
        seas[k]["lib"] = m.group(1)

    try:
        cnts = json.load(open(COUNTS, encoding="utf-8"))
    except Exception:                                        # noqa: BLE001
        cnts = {}
    try:
        q = json.load(open(QUEUE, encoding="utf-8"))
    except Exception:                                        # noqa: BLE001
        q = {"items": []}
    items = q.get("items") or []
    have = {(i.get("dir") or i.get("series"), i.get("season")) for i in items}
    added = skipped_c = skipped_no_tmdb = 0
    for (name, sn), v in sorted(seas.items()):
        if sn == 0 or not v["tmdb"] or (name, sn) in have:
            continue
        n = cnts.get("%s:S%s" % (v["tmdb"], sn), 0)
        if not n:
            skipped_no_tmdb += 1
            continue
        if max(v["eps"]) > n + 2:            # 季号/集号与 TMDB 对不上，交 gap_report 单列
            continue
        miss = [e for e in range(1, n + 1) if e not in v["eps"]]
        if not miss:
            continue
        if len(miss) > a.max_missing:        # C 类
            skipped_c += 1
            continue
        items.append({"lane": "fill", "lib": v["lib"].split("/")[-1], "dir": name,
                      "series": name.split(" (")[0], "season": sn,
                      "missing": miss, "tmdb": v["tmdb"]})
        added += 1

    print("  fill 队列 %d → %d（新增 %d 季）｜跳过 C 类 %d 季｜TMDB 季集数缺失 %d 季"
          % (len(items) - added, len(items), added, skipped_c, skipped_no_tmdb))
    if a.dry_run:
        print("  （dry-run，未写盘）")
        return 0
    q["items"] = items
    json.dump(q, open(QUEUE, "w", encoding="utf-8"), ensure_ascii=False)
    print("  已写入 %s" % QUEUE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
