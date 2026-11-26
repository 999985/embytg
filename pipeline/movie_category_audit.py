"""审计：电影当前分类 vs 按 TMDB 类型顺序应得分类。

规则（新）：按 TMDB 返回的 genres 顺序取第一个能映射到库的类型；
不再用手写的 GENRE_PRIO（它把动作排在恐怖前面，釜山行2 因此进了动作片）。

用法：
    python3 movie_category_audit.py --sample 400
    python3 movie_category_audit.py --all --json /tmp/cat_audit.json
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request as ur
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/opt/embytg/pipeline")
import nfo_gen                                              # noqa: E402

HERE = "/opt/embytg/pipeline"
CACHE = os.path.join(HERE, ".tmdb_genres_cache.json")

GENRE_LIB = {
    16: "动画片", 99: "纪录片", 10402: "纪录片",
    878: "科幻片", 14: "科幻片",
    27: "惊悚片", 53: "惊悚片", 9648: "惊悚片",
    80: "犯罪片",
    28: "动作片", 12: "动作片", 37: "动作片",
    10752: "战争片",
    35: "喜剧片",
    18: "剧情片", 10749: "剧情片", 10751: "剧情片", 36: "剧情片", 10770: "剧情片",
}


def load_cache():
    try:
        with open(CACHE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                        # noqa: BLE001
        return {}


def save_cache(d):
    try:
        with open(CACHE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
    except Exception:                                        # noqa: BLE001
        pass


def fetch(tid):
    u = "https://api.themoviedb.org/3/movie/%s?language=zh-CN" % tid
    d = json.loads(ur.urlopen(ur.Request(u, headers=nfo_gen.HDR), timeout=30).read())
    return [g["id"] for g in (d.get("genres") or [])]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--json")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()

    con = sqlite3.connect("file:%s/tg.db?mode=ro" % HERE, uri=True, timeout=20)
    rows = con.execute("select rel, lib, tmdb from resource_state "
                       "where kind='movie' and tmdb is not null and tmdb!='' "
                       "order by landed_at desc").fetchall()
    con.close()
    if a.sample:
        rows = rows[:a.sample]
    print("  待审 %d 部" % len(rows))

    cache = load_cache()
    todo = [r for r in rows if str(r[2]) not in cache]
    print("  需请求 TMDB: %d（缓存命中 %d）" % (len(todo), len(rows) - len(todo)))

    def work(r):
        try:
            gids = fetch(r[2])
            return str(r[2]), gids
        except Exception:                                    # noqa: BLE001
            return str(r[2]), None

    if todo:
        with ThreadPoolExecutor(max_workers=a.workers) as ex:
            for tid, gids in ex.map(work, todo):
                if gids is not None:
                    cache[tid] = gids
        save_cache(cache)

    changes = {}
    detail = []
    for rel, lib, tid in rows:
        gids = cache.get(str(tid))
        if not gids:
            continue
        want = None
        for g in gids:
            if g in GENRE_LIB:
                want = GENRE_LIB[g]
                break
        want = want or "其他片"
        cur = (lib or "").split("/")[-1]
        if want != cur:
            key = "%s → %s" % (cur, want)
            changes[key] = changes.get(key, 0) + 1
            detail.append({"rel": rel, "cur": cur, "want": want, "genres": gids, "tmdb": tid})

    print("  分类不一致: %d 部" % len(detail))
    for k, v in sorted(changes.items(), key=lambda x: -x[1])[:10]:
        print("    %-18s %d" % (k, v))
    if a.json:
        json.dump(detail, open(a.json, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print("  明细: %s" % a.json)


if __name__ == "__main__":
    main()
