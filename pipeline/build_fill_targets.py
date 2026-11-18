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


_CACHE = None
_API_LEFT = [60]
_DEADLINE = [0.0]


def _cache_load():
    global _CACHE
    if _CACHE is None:
        try:
            _CACHE = json.load(open(CACHE, encoding="utf-8"))
        except Exception:                                  # noqa: BLE001
            _CACHE = {}
    return _CACHE


def _cache_save():
    if _CACHE is None:
        return
    try:
        tmp = CACHE + ".tmp"
        json.dump(_CACHE, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
        os.replace(tmp, CACHE)
    except Exception:                                      # noqa: BLE001
        pass


def tmdb_season_counts(tmdb_id, season=None):
    """返回 {season: 集数}。

    关键性能点（2026-11-19 实测）：缓存文件 14MB，**每次调用都 json.load + 回写**
    会让 800 部剧跑成 >300s（超时）。现在进程内只加载一次、结束时写一次，
    并且优先按单季缓存键 `<tmdb>|<season>` 命中（现有缓存就是这种键）。
    """
    cache = _cache_load()
    if season is not None:
        hit = cache.get("%d|%d" % (tmdb_id, season))
        if isinstance(hit, dict) and hit.get("counts"):
            return {int(k): int(v) for k, v in hit["counts"].items()}
    else:
        hit = cache.get("%d|all" % tmdb_id)
        if isinstance(hit, dict) and hit.get("counts"):
            return {int(k): int(v) for k, v in hit["counts"].items()}

    if _API_LEFT[0] <= 0 or time.time() > _DEADLINE[0]:
        return {}
    _API_LEFT[0] -= 1
    tok = str(cfg("tmdb_token") or "")
    base = "https://api.themoviedb.org/3/tv/%d" % tmdb_id
    counts = {}
    try:
        if season is not None:
            url = base + "/season/%d?language=zh-CN" % season
            req = urllib.request.Request(url, headers={"Authorization": "Bearer " + tok})
            with urllib.request.urlopen(req, timeout=20) as r:
                d = json.loads(r.read())
            eps = d.get("episodes") or []
            if eps:
                counts[int(season)] = len(eps)
        else:
            req = urllib.request.Request(base + "?language=zh-CN",
                                         headers={"Authorization": "Bearer " + tok})
            with urllib.request.urlopen(req, timeout=20) as r:
                d = json.loads(r.read())
            for s in d.get("seasons") or []:
                n = int(s.get("season_number") or 0)
                if n > 0:
                    counts[n] = int(s.get("episode_count") or 0)
    except Exception:                                      # noqa: BLE001
        return {}
    if counts:
        key = "%d|%s" % (tmdb_id, season if season is not None else "all")
        cache[key] = {"counts": {str(k): v for k, v in counts.items()}, "ts": int(time.time())}
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
    ap.add_argument("--api-budget", type=int, default=60, help="单轮最多打几次 TMDB")
    ap.add_argument("--time-budget", type=int, default=120, help="单轮最多跑几秒")
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    _API_LEFT[0] = a.api_budget
    _DEADLINE[0] = time.time() + a.time_budget
    have = load_have()
    items = []
    # focus 剧必须最先算：否则 800 部剧的扫描会先把 TMDB 预算吃光，
    # 轮到它时预算为 0 → 算出「无缺口」（2026-11-19 实测踩到）。
    focus_items = []
    if a.focus and a.focus in have:
        sfs = have[a.focus]
        for season in sorted(sfs["eps"]):
            cnt = tmdb_season_counts(a.focus, season)
            total = int(cnt.get(season) or 0)
            if total <= 0 or total > 800:
                continue
            miss = [e for e in range(1, total + 1) if e not in sfs["eps"][season]]
            if miss:
                focus_items.append({"tmdb": a.focus, "lib": sfs["lib"], "dir": sfs["title"],
                                    "series": (sfs["title"] or "").split(" (")[0],
                                    "season": season, "missing": miss,
                                    "total_missing": len(miss)})
                break                     # 长连载剧一次只推进一个季，补齐再下一季
        if a.focus_missing:
            for x in focus_items:
                x["missing"] = x["missing"][: a.focus_missing]
        print("  focus %d: %d 季有缺口，本轮 %d 集" % (
            a.focus, len(focus_items), sum(len(x["missing"]) for x in focus_items)),
            file=sys.stderr, flush=True)

    done = 0
    for tmdb, s in have.items():
        if tmdb == a.focus:
            continue
        done += 1
        if done % 100 == 0:
            print("  …已扫 %d 部（API 余 %d）" % (done, _API_LEFT[0]), file=sys.stderr, flush=True)
        for season in sorted(s["eps"]):
            counts = tmdb_season_counts(tmdb, season)
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
    if focus_items:
        items = focus_items + [x for x in items if x["tmdb"] != a.focus]
    # focus 剧钉在最前（长连载剧专项），其余按缺口从小到大排
    items.sort(key=lambda x: (0 if (a.focus and x["tmdb"] == a.focus) else 1,
                              x["total_missing"], x["tmdb"], x["season"]))
    items = items[: a.limit_series]

    total_eps = sum(len(x["missing"]) for x in items)
    print("  系列 %d 个 / 缺集 %d 集" % (len(items), total_eps))
    for x in items[:5]:
        print("   %-24s S%02d 缺 %d（本轮 %d）" % (x["series"][:24], x["season"],
                                                x["total_missing"], len(x["missing"])))
    if a.focus:
        f = next((x for x in items if x["tmdb"] == a.focus), None)
        print("  focus %s: %s" % (a.focus, ("S%02d 本轮 %d 集" % (f["season"], len(f["missing"]))) if f else "无缺口"))

    _cache_save()
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
