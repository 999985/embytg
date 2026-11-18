#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the three GHA lanes from existing tg.db state.

This is a planner only: it never calls a source server and never writes media.
It deliberately reuses the project's existing state:
  * resource_state: real probed width/height for upgrade decisions
  * .autoingest_queue.json: confirmed missing episodes in existing series
  * todb_video: whole-series candidates for the new-series lane
"""
import argparse
import datetime
import json
import os
import re
import sqlite3

HERE = "/opt/embytg/pipeline"
DB = os.path.join(HERE, "tg.db")
FILL_IN = os.path.join(HERE, ".autoingest_queue.json")
OUT_NEW = os.path.join(HERE, ".gha_queue_new.json")
OUT_FILL = os.path.join(HERE, ".gha_queue_fill.json")
OUT_UPGRADE = os.path.join(HERE, ".gha_queue_upgrade.json")
SE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
ANCHOR = re.compile(r"\{tmdb=(\d+)\}", re.I)
YEAR = re.compile(r"\((\d{4})\)")


def norm_title(s):
    s = ANCHOR.sub("", s or "")
    s = YEAR.sub("", s)
    s = re.sub(r"\s*第[一二三四五六七八九十0-9]+季\s*$", "", s)
    return re.sub(r"[\s\-_·:：!！?？.,，'\"“”‘’（）()\[\]【】]+", "", s).lower()


def load_json(path, default):
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return default


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def quality_of(width, height, rel):
    w, h = int(width or 0), int(height or 0)
    if h >= 1800 or w >= 3000:
        return "2160p", 4
    if h >= 1400 or w >= 2400:
        return "1440p", 3
    if h >= 1000 or w >= 1800:
        return "1080p", 2
    if h >= 700 or w >= 1200:
        return "720p", 1
    if h > 0 or w > 0:
        return "sd", 0
    low = (rel or "").lower()
    if "2160p" in low or "4k" in low:
        return "2160p", 4
    if "1080p" in low:
        return "1080p", 2
    if "720p" in low:
        return "720p", 1
    return "sd", 0


def classify_new_lib(countrys, languages):
    cs = {str(x).upper() for x in (countrys or [])}
    ls = " ".join(str(x) for x in (languages or []))
    if cs & {"HK", "TW"}:
        return "港台剧"
    if cs & {"JP", "KR"} or "日本語" in ls or "한국" in ls:
        return "日韩剧"
    if cs & {"US", "GB", "FR", "DE", "CA", "AU", "ES", "IT"}:
        return "欧美剧"
    return "国产剧"


def load_fill():
    data = load_json(FILL_IN, {})
    items = []
    for x in data.get("items") or []:
        item = dict(x)
        item["lane"] = "fill"
        item["missing"] = sorted({int(e) for e in (x.get("missing") or [])})
        if item["missing"]:
            items.append(item)
    return items


def load_upgrade(limit):
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=30)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "select tmdb, lib, title, rel, width, height, size from resource_state "
        "where kind='episode' and tmdb is not null and rel is not null"
    ).fetchall()
    con.close()
    series = {}
    for r in rows:
        m = SE.search(r["rel"] or "")
        if not m:
            continue
        sm = re.search(r"/Season\s*(\d+)", r["rel"] or "", re.I)
        season = int(sm.group(1)) if sm else int(m.group(1))
        ep = int(m.group(2))
        q, rank = quality_of(r["width"], r["height"], r["rel"])
        key = int(r["tmdb"])
        s = series.setdefault(key, {
            "lane": "upgrade", "tmdb": key,
            "lib": (r["lib"] or "").split("/")[-1],
            "dir": r["title"], "series": (r["title"] or "").split(" (")[0],
            "eps": {},
        })
        s["eps"][(season, ep)] = {
            "quality": q, "rank": rank, "rel": r["rel"],
            "size": int(r["size"] or 0),
        }
    candidates = []
    for sid, s in series.items():
        eps = s.pop("eps")
        qmax = max((v["rank"] for v in eps.values()), default=0)
        for (season, ep), v in sorted(eps.items()):
            if v["rank"] >= 4:
                continue
            candidates.append({
                **s,
                "season": season,
                "ep": ep,
                "current_quality": v["quality"],
                "current_rel": v["rel"],
                "current_size": v.get("size") or 0,
                "target_quality": "2160p",
                "priority": (2 if qmax < 4 else 1, v["rank"], -season, -ep),
            })
    candidates.sort(key=lambda x: (x["priority"], x["tmdb"], x["season"], x["ep"]))
    # 2026-11-19：原来直接 candidates[:limit]，把「整部剧都没有 2160p」的剧（priority=2）
    # 整部挤掉 —— 航海王 1180 集一条都进不了 upgrade 队列。
    # 现在每部剧先取 PER_SERIES 条，再回填剩下的名额，长剧至少能进得来。
    PER_SERIES = int(os.environ.get("UPGRADE_PER_SERIES", "3"))
    picked, seen, rest = [], {}, []
    for x in candidates:
        k = (x["tmdb"], x["season"])
        n = seen.get(k, 0)
        if n < PER_SERIES and len(picked) < limit:
            picked.append(x)
            seen[k] = n + 1
        else:
            rest.append(x)
    for x in rest:
        if len(picked) >= limit:
            break
        picked.append(x)
    for x in picked:
        x.pop("priority", None)
    return picked


def load_new(limit, current_year):
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=30)
    con.row_factory = sqlite3.Row
    existing = set()
    for r in con.execute("select title from resource_state where kind='episode' and title is not null group by title"):
        existing.add(norm_title(r["title"]))
    rows = con.execute(
        "select video_id,title,origin_title,year,countrys,languages from todb_video "
        "where video_type='tv' and coalesce(find_state,'unsearched')='unsearched' "
        "and year is not null and year>=2018 and year<=? "
        "order by year desc, video_id desc limit ?",
        (current_year, max(limit * 8, limit)),
    ).fetchall()
    con.close()
    out = []
    for r in rows:
        if norm_title(r["title"]) in existing:
            continue
        try:
            countrys = json.loads(r["countrys"] or "[]")
            languages = json.loads(r["languages"] or "[]")
        except Exception:  # noqa: BLE001
            countrys, languages = [], []
        out.append({
            "lane": "new",
            "video_id": int(r["video_id"]),
            "series": r["title"],
            "origin_title": r["origin_title"],
            "year": int(r["year"]),
            "lib": classify_new_lib(countrys, languages),
            "tmdb": None,
            "target_quality": "auto",
        })
        if len(out) >= limit:
            break
    return out


def main():
    ap = argparse.ArgumentParser()
    # 2026-11-14（E091）：`--lane new` / `--lane all` 会**毁掉** new 队列。
    # 这里生成的 new 条目来自 todb_video 表，只有 series/tmdb/video_id，
    # 没有 source_label —— runner 认不出源站，整条跳过（NOJOB reason=no_job）。
    # 实测：500 条里 500 条缺 source_label，队列看着有货，lane 一条也领不到。
    # new 队列唯一正确的生产者是 build_new_queue_from_sources.py（带源站标签）。
    ap.add_argument("--lane", choices=("new", "fill", "upgrade"),
                    default="fill",
                    help="fill/upgrade 由本脚本生成；new 必须用 "
                         "build_new_queue_from_sources.py（要带 source_label）")
    ap.add_argument("--new-limit", type=int, default=500)
    ap.add_argument("--upgrade-limit", type=int, default=2000)
    a = ap.parse_args()
    now = datetime.datetime.now().astimezone()
    meta = {"generated_at": now.isoformat(timespec="seconds"), "source": DB}
    result = {}
    if a.lane == "new":
        # 硬拦：这条路径产出的条目 runner 用不了，运行了只会把好队列覆盖成残废。
        cur = 0
        try:
            cur = len(json.load(open(OUT_NEW, encoding="utf-8")).get("items") or [])
        except Exception:                                  # noqa: BLE001
            pass
        raise SystemExit(
            "拒绝生成 new 队列：本脚本产出的条目缺少 source_label，"
            "runner 无法领活。当前 new 队列 %d 条保持不动。\n"
            "请用：python3 build_new_queue_from_sources.py --limit 3000 --apply" % cur)
    if a.lane in ("all", "fill"):
        fill = load_fill()
        write_json(OUT_FILL, {**meta, "lane": "fill", "items": fill})
        result["fill"] = len(fill)
        result["fill_path"] = OUT_FILL
    if a.lane in ("all", "upgrade"):
        upgrade = load_upgrade(a.upgrade_limit)
        write_json(OUT_UPGRADE, {**meta, "lane": "upgrade", "items": upgrade})
        result["upgrade"] = len(upgrade)
        result["upgrade_path"] = OUT_UPGRADE
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
