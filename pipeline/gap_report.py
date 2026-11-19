#!/usr/bin/env python3
"""gap_report.py —— 缺口统计（口径写死在 GOAL.md 里，别用 max-集号猜）。

为什么不用「最大集号 − 实有集数」：综艺、日更剧的编号根本不是 1..N，
按 max 猜会得到「探索新境 S2 缺 1207 集」这种胡说。真实口径是：
    TMDB 该季的集数 N  −  库里该季**已达标**的集数
且只在集号 ≤ N 时才计（超过 N 的集号说明我们的季号/集号与 TMDB 不一致，
整季标记为 numbering_mismatch，单独报，不混进缺口）。

用法：
    python3 gap_report.py                    # 全库汇总
    python3 gap_report.py --top 10           # 缺口最大的 10 部
    python3 gap_report.py --json out.json
"""
import argparse
import collections
import json
import os
import re
import sqlite3
import sys
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from embytg_conf import cfg                                  # noqa: E402

CACHE = os.path.join(HERE, ".tmdb_season_counts.json")


def tmdb_season_count(tmdb, season):
    cache = {}
    if os.path.exists(CACHE):
        try:
            cache = json.load(open(CACHE, encoding="utf-8"))
        except Exception:                                    # noqa: BLE001
            cache = {}
    key = "%s:S%s" % (tmdb, season)
    if key in cache:
        return cache[key]
    n = 0
    try:
        api = cfg("tmdb_api").rstrip("/")
        tok = cfg("tmdb_token")
        url = "%s/tv/%s/season/%s?language=zh-CN" % (api, tmdb, season)
        req = urllib.request.Request(url, headers={"Authorization": "Bearer %s" % tok})
        with urllib.request.urlopen(req, timeout=15) as f:
            d = json.loads(f.read())
        n = len(d.get("episodes") or [])
    except Exception:                                        # noqa: BLE001
        n = -1                                               # 取不到，标记未知
    cache[key] = n
    try:
        json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception:                                        # noqa: BLE001
        pass
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=0)
    ap.add_argument("--json")
    a = ap.parse_args()

    # 低码率以 Emby 实测为准（.lowq_library.json 是 daily_ops 从 Emby 的
    # RunTimeTicks 算出来的）；台账 dur_sec 大量为空，拿它算码率会得出「0 集未达标」。
    lowq = collections.Counter()
    try:
        for x in json.load(open(os.path.join(HERE, ".lowq_library.json"), encoding="utf-8")):
            lowq[(x.get("series") or "", str(x.get("name") or ""))] = 1
    except Exception:                                        # noqa: BLE001
        pass

    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"), uri=True, timeout=25)
    rows = con.execute("select rel, size, dur_sec, width, height, tmdb from resource_state "
                       "where kind='episode' and rel like 'TV/%'").fetchall()
    con.close()

    # probed = 有 dur_sec 的集数；未探测的不能算「未达标」，否则台账滞后会把数字撑爆
    seas = collections.defaultdict(lambda: {"eps": set(), "ok": 0, "probed": 0, "tmdb": None})
    for rel, sz, dur, w, h, tmdb in rows:
        m = re.search(r"/([^/]+)/Season (\d+)/", rel)
        e = re.search(r"S\d+E(\d+)", rel.split("/")[-1])
        if not (m and e):
            continue
        k = (m.group(1), int(m.group(2)))
        seas[k]["eps"].add(int(e.group(1)))
        seas[k]["tmdb"] = seas[k]["tmdb"] or tmdb
        if dur:
            seas[k]["probed"] += 1
            mbps = (sz or 0) * 8 / dur / 1e6
            floor = 2.0 if (w or 0) >= 3000 or (h or 0) >= 1800 else (
                0.8 if (w or 0) >= 1700 or (h or 0) >= 1000 else 0.4)
            if mbps >= floor:
                seas[k]["ok"] += 1

    rows_out, mismatch, unknown = [], [], []
    for (name, sn), v in seas.items():
        if sn == 0 or not v["tmdb"]:
            continue
        n = tmdb_season_count(v["tmdb"], sn)
        if n < 0:
            unknown.append((name, sn))
            continue
        if n == 0:
            continue
        if max(v["eps"]) > n + 2:
            mismatch.append((name, sn, max(v["eps"]), n))
            continue
        rows_out.append({"dir": name, "season": sn, "tmdb": v["tmdb"],
                         "tmdb_count": n, "have": len(v["eps"]), "ok": v["ok"],
                         "missing": max(0, n - len(v["eps"])),
                         "below_floor": sum(1 for _n in lowq if _n[0] == name),
                         "unprobed": max(0, len(v["eps"]) - v["probed"])})
    # 分层：长篇动画（缺 >100 集）不是「漏抓」，是「收了几十集的长番」，
    # 混进缺口会让「最全」永远达不到。A=连载差 1-5 集，B=完结剧季内缺 6-100，C=长番。
    for r in rows_out:
        r["cls"] = "A" if r["missing"] <= 5 else ("B" if r["missing"] <= 100 else "C")
    rows_out.sort(key=lambda x: -x["missing"])
    by_cls = collections.Counter()
    miss_cls = collections.Counter()
    for r in rows_out:
        by_cls[r["cls"]] += 1
        miss_cls[r["cls"]] += r["missing"]
    total_missing = sum(r["missing"] for r in rows_out)
    total_below = sum(r["below_floor"] for r in rows_out)
    print("  可比季 %d 个：缺 %d 集，另有 %d 集在库但未达标"
          % (len(rows_out), total_missing, total_below))
    print("  分层：A(≤5) %d 季/%d 集｜B(6-100) %d 季/%d 集｜C(>100) %d 季/%d 集"
          % (by_cls["A"], miss_cls["A"], by_cls["B"], miss_cls["B"],
             by_cls["C"], miss_cls["C"]))
    print("  编号不一致（季号/集号与 TMDB 对不上，不计入缺口）：%d 季" % len(mismatch))
    print("  TMDB 取不到：%d 季" % len(unknown))
    if a.top:
        for r in rows_out[:a.top]:
            print("   %-26s S%-2d 缺 %3d/%d（未达标 %d）"
                  % (r["dir"][:26], r["season"], r["missing"], r["tmdb_count"], r["below_floor"]))
    if a.json:
        json.dump({"seasons": rows_out, "mismatch": mismatch,
                   "total_missing": total_missing, "total_below": total_below},
                  open(a.json, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
