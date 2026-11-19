#!/usr/bin/env python3
"""dedupe_movies.py —— 同一部电影多份文件时留最优、其余直接删。

用户 2026-11-19：「机器人这个有点问题 很多结果有两部电影甚至更多」。
根因：电影的去重键没统一 —— 机器人转存落地成 `片名 (年) {tmdb=N} - 1080p.mp4`，
源站下载落地成 `片名 (年) [tmdbid-N].mkv` 或 `片名 (年) {tmdb=N} [2160p][H.265]…`，
三种写法同一部片互相认不出，于是一部片进三条。

本脚本按 **tmdb id 优先**（三种写法都能抽出来）+ 片名年份兜底分组，
每组保留 best_pick 排名最高的一份，其余走 drop_old 五处清理。

用法：
    python3 dedupe_movies.py --dry-run
    python3 dedupe_movies.py --limit 10 --apply
    python3 dedupe_movies.py --apply
"""
import argparse
import collections
import os
import re
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import best_pick as BP                                        # noqa: E402

DB = os.path.join(HERE, "tg.db")


def tmdb_of(rel):
    for pat in (r"\{tmdb=(\d+)\}", r"\[tmdbid-(\d+)\]", r"tmdb=(\d+)"):
        m = re.search(pat, rel)
        if m:
            return m.group(1)
    return ""


def label_of(name):
    n = name.lower()
    if "2160" in n or "4k" in n:
        return "2160p"
    if "1080" in n:
        return "1080p"
    if "720" in n:
        return "720p"
    return "sd"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=25)
    rows = con.execute("select rel, size, dur_sec, width, height, vcodec "
                       "from resource_state where kind='movie'").fetchall()
    con.close()

    groups = collections.defaultdict(list)
    for rel, size, dur, w, h, vc in rows:
        tid = tmdb_of(rel)
        if tid:
            key = ("tmdb", tid)
        else:
            m = re.match(r"^(.*?)\s*\((\d{4})\)", os.path.basename(rel))
            key = ("name", (m.group(1).strip(), m.group(2))) if m else ("one", rel)
        groups[key].append({"rel": rel, "size": size or 0, "duration": dur or 0,
                            "vcodec": vc or "", "label": label_of(os.path.basename(rel))})

    dups = {k: v for k, v in groups.items() if len(v) > 1}
    print("  同片多版本 %d 组，可删 %d 个" % (len(dups), sum(len(v) - 1 for v in dups.values())))
    done = 0
    freed = 0
    for key in sorted(dups, key=lambda k: -len(dups[k])):
        if a.limit and done >= a.limit:
            break
        items = dups[key]
        ranked = BP.rank_candidates([{"label": i["label"], "size": i["size"],
                                      "duration": i["duration"], "vcodec": i["vcodec"]}
                                     for i in items])
        if not ranked:
            continue
        win = ranked[0]
        keep = next(i["rel"] for i in items if i["size"] == win["size"] and i["label"] == win["label"])
        kill = [i for i in items if i["rel"] != keep]
        print("  留 %-34s %5dMB，删 %d 个" % (os.path.basename(keep)[:34],
              next(i["size"] for i in items if i["rel"] == keep) / 1e6, len(kill)))
        if not a.apply:
            done += 1
            continue
        for k in kill:
            subprocess.run([sys.executable, os.path.join(HERE, "drop_old.py"),
                            "--path", k["rel"], "--force"], capture_output=True, text=True, timeout=300)
            freed += k["size"]
        done += 1
    print("  处理 %d 组%s%s" % (done, "，释放 %.1fGB" % (freed / 1e9) if a.apply else "",
                               "" if a.apply else "（dry-run）"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
