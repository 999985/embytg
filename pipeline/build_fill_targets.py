#!/usr/bin/env python3
"""build_fill_targets.py —— 从台账反推「补缺目标」，产出 fill 队列的输入。

## 为什么需要它（2026-11-19）

`build_three_queues.py --lane fill` 读的是 `.autoingest_queue.json`，而那个文件的
生产者是 `autoingest` 链路 —— 该链路早已退役，文件冻结在 11-18 06:27 的 36 条。
后果：fill 队列永远是那 36 条老目标，**新剧缺集永远进不来**。
实测《航海王》(tmdb=37854) 库里只有 S01E01，而 fill 队列里一条都没有。

于是这里补上生产者：直接从 `resource_state`（真源）+ TMDB 季集数算出每部剧缺哪些集。

## 判据

- 只处理「库里已有 ≥1 集」的剧（全新剧走 new 车道，不归 fill）。
- 跳过 Season 00（特别篇）：编号体系不同，补了会污染正片。
- 单季集数 > 800 视为 TMDB 数据异常，跳过该季（航海王按篇章拆季，不会触发）。
- `--limit-series` / `--limit-missing-per-series` 控制单轮规模，长连载剧分多轮推进。

## 用法

    python3 build_fill_targets.py --dry          # 只看统计
    python3 build_fill_targets.py                # 写 .autoingest_queue.json
    python3 build_fill_targets.py --focus 37854 --focus-missing 20
        # 长连载剧专项：把某部剧排到最前，本轮只放 20 集
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "tg.db")
CACHE = os.path.join(HERE, ".tmdb_season_cache.json")
OUT = os.path.join(HERE, ".autoingest_queue.json")
CONF = "/opt/embytg/config.json"
EP_RE = re.compile(r"S(\d{2})E(\d{2,4})", re.I)


def cfg(k):
    try:
        return json.load(open(CONF)).get(k)
    except Exception:                                      # noqa: BLE001
        return None


def tmdb_season_counts(tmdb_id, season=None):
    """返回 {season: 集数}；只查需要的那一季时就传 season。"""
    key = "%d|%s" % (tmdb_id, season if season is not None else "all")
    try:
        cache = json.load(open(CACHE, encoding="utf-8"))
    except Exception:                                      # noqa: BLE001
        cache = {}
    if key in cache and isinstance(cache[key], dict):
        return cache[key].get("counts") or {}
    tok = cfg("tmdb_token")
    base = "https://api.themoviedb.org/3/tv/%d" % tmdb_id
    counts = {}
    try:
        if season is not None:
            url = base + "/season/%d?language=zh-CN" % season
            req = urllib.request.Request(url, headers={"Authorization": "Bearer " + tok})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            eps = d.get("episodes") or []
            if eps:
                counts[int(season)] = len(eps)
        else:
            req = urllib.request.Request(base + "?language=zh-CN",
                                         headers={"Authorization": "Bearer " + tok})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            for s in d.get("seasons") or []:
                n = int(s.get("season_number") or 0)
                if n <= 0:
                    continue
                counts[n] = int(s.get("episode_count") or 0)
    except Exception:                                      # noqa: BLE001
        return {}
    if counts:
        cache[key] = {"counts": counts, "ts": int(time.time())}
        try:
            tmp = CACHE + ".tmp"
            json.dump(cache, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
            os.replace(tmp, CACHE)
        except Exception:                                  # noqa: BLE001
            pass
    return counts


def load_have():
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=30)
    rows = con.execute(
        "select tmdb, lib, title, rel from resource_state "
        "where kind='episode' and tmdb is not null and rel is not null").fetchall()
    con.close()
    have = {}
    for tmdb, lib, title, rel in rows:
        m = EP_RE.search(rel or "")
        if not m:
            continue
        sm = re.search(r"/Season\s*(\d+)", rel or "", re.I)
        season = int(sm.group(1)) if sm else int(m.group(1))
        ep = int(m.group(2))
        key = int(tmdb)
        s = have.setdefault(key, {"lib": (lib or "").split("/")[-1], "title": title,
                                  "eps": {}})
        s["eps"].setdefault(season, set()).add(ep)
    return have


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-series", type=int, default=400)
    ap.add_argument("--limit-missing-per-series", type=int, default=50)
    ap.add_argument("--focus", type=int, default=0, help="强制排到最前的 tmdb id")
    ap.add_argument("--focus-missing", type=int, default=0, help="focus 剧本轮最多放几集")
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    have = load_have()
    items = []
    for tmdb, s in have.items():
        counts = tmdb_season_counts(tmdb)
        if not counts:
            continue
        for season in sorted(s["eps"]):
            total = int(counts.get(season) or 0)
            if total <= 0 or total > 800:
                continue
            have_eps = s["eps"][season]
            missing = [e for e in range(1, total + 1) if e not in have_eps]
            if not missing:
                continue
            items.append({
                "tmdb": tmdb, "lib": s["lib"], "dir": s["title"],
                "series": (s["title"] or "").split(" (")[0],
                "season": season, "missing": missing[: a.limit_missing_per_series],
                "total_missing": len(missing),
            })

    # 小缺口优先（能快速出片），长连载剧按轮次推进
    items.sort(key=lambda x: (x["total_missing"], x["tmdb"], x["season"]))
    items = items[: a.limit_series]

    focus = [x for x in items if x["tmdb"] == a.focus] if a.focus else []
    if a.focus:
        # focus 剧可能被 limit 截掉，单独补算
        if not focus and a.focus in have:
            s = have[a.focus]
            counts = tmdb_season_counts(a.focus)
            for season in sorted(s["eps"]):
                total = int(counts.get(season) or 0)
                miss = [e for e in range(1, total + 1) if e not in s["eps"][season]]
                if miss:
                    focus.append({"tmdb": a.focus, "lib": s["lib"], "dir": s["title"],
                                  "series": (s["title"] or "").split(" (")[0],
                                  "season": season, "missing": miss, "total_missing": len(miss)})
        if a.focus_missing:
            for x in focus:
                x["missing"] = x["missing"][: a.focus_missing]
        items = focus + [x for x in items if x["tmdb"] != a.focus]

    total_eps = sum(len(x["missing"]) for x in items)
    print("  系列 %d 个 / 缺集 %d 集" % (len(items), total_eps))
    for x in items[:5]:
        print("   %-24s S%02d 缺 %d（本轮 %d）" % (x["series"][:24], x["season"],
                                                x["total_missing"], len(x["missing"])))
    if a.focus:
        f = next((x for x in items if x["tmdb"] == a.focus), None)
        print("  focus %s: %s" % (a.focus, ("S%02d 本轮 %d 集" % (f["season"], len(f["missing"]))) if f else "无缺口"))

    if a.dry:
        return 0
    if os.path.exists(OUT):
        os.replace(OUT, OUT + ".bak.%s" % time.strftime("%H%M"))
    meta = {"ts": int(time.time()), "source": "build_fill_targets.py",
            "series": len(items), "episodes": total_eps}
    tmp = OUT + ".tmp"
    json.dump({**meta, "items": items}, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
    os.replace(tmp, OUT)
    print("  已写 %s（系列 %d / 缺集 %d）" % (OUT, len(items), total_eps))
    return 0


if __name__ == "__main__":
    sys.exit(main())
