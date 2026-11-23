#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the new-series queue from real source-library catalogs.

This is metadata-only: one recursive Series listing per source, no media
download.  It fixes the main scaling problem of the old TODB-only queue:
candidates are selected from titles that a source actually exposes.
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gha_state as GS  # noqa: E402
import src_guard as SG  # noqa: E402

DB = os.path.join(HERE, "tg.db")
OUT = os.path.join(HERE, ".gha_queue_new.json")
YEAR = re.compile(r"\((\d{4})\)")
ANCHOR = re.compile(r"(\{tmdb=\d+\}|\[tmdbid-\d+\]|\{tmdbid=\d+\})", re.I)
SEASON = re.compile(
    r"(?:\s*第[一二三四五六七八九十0-9]+季|\s*Season\s*\d+|\s+S\d{1,2})$",
    re.I,
)
LIB_DIRS = ("国漫", "日漫", "日韩剧", "欧美剧", "港台剧", "国产剧", "综艺")
LIB_ALIAS = {
    "国产剧": "国产剧",
    "大陆剧": "国产剧",
    "日韩剧": "日韩剧",
    "韩剧": "日韩剧",
    "日剧": "日韩剧",
    "欧美剧": "欧美剧",
    "美剧": "欧美剧",
    "英剧": "欧美剧",
    "港台剧": "港台剧",
    "港剧": "港台剧",
    "台剧": "港台剧",
    "国漫": "国漫",
    "日漫": "日漫",
    "动漫": "国漫",
    "综艺": "综艺",
}


def norm_title(value):
    s = str(value or "").replace("\u200b", "").replace("\ufeff", "")
    s = ANCHOR.sub("", s)
    s = YEAR.sub("", s)
    s = SEASON.sub("", s)
    return re.sub(
        r"[\s\-_·:：!！?？.,，'\"“”‘’（）()\[\]【】]+", "", s
    ).lower()


def item_tmdb(item):
    try:
        return int((item.get("ProviderIds") or {}).get("Tmdb") or 0)
    except (TypeError, ValueError):
        return 0


def item_year(item):
    try:
        year = int(item.get("ProductionYear") or 0)
    except (TypeError, ValueError):
        year = 0
    if year < 1900:
        m = YEAR.search(str(item.get("Path") or ""))
        year = int(m.group(1)) if m else 0
    return year


def lib_from_path(path):
    parts = [p for p in str(path or "").replace("\\", "/").split("/") if p]
    for part in reversed(parts):
        name = LIB_ALIAS.get(part.strip())
        if name:
            return name
    return ""


def lib_from_item(item):
    genres = {str(x).lower() for x in (item.get("Genres") or [])}
    locs = {str(x).upper() for x in (item.get("ProductionLocations") or [])}
    lang = str(item.get("OriginalLanguage") or "").lower()
    animation = any("动画" in x or "anime" in x for x in genres)
    reality = any("真人秀" in x or "reality" in x for x in genres)
    if reality:
        return "综艺"
    if animation:
        if locs & {"JP"} or lang in ("jpn", "ja"):
            return "日漫"
        if locs & {"CN", "HK", "TW"} or lang in ("zho", "zh"):
            return "国漫"
    lib = lib_from_path(item.get("Path"))
    if lib:
        return lib
    if locs & {"HK", "TW"} or lang in ("zho", "zh"):
        return "港台剧" if locs & {"HK", "TW"} else "国产剧"
    if locs & {"JP", "KR"} or lang in ("jpn", "ja", "kor", "ko"):
        return "日韩剧"
    if locs & {"US", "GB", "FR", "DE", "CA", "AU", "ES", "IT"}:
        return "欧美剧"
    return "国产剧"


def existing_library():
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=30)
    rows = con.execute(
        "select distinct tmdb, title from resource_state "
        "where kind='episode' and title is not null"
    ).fetchall()
    con.close()
    tmdb = set()
    titles = set()
    for tmdb_id, title in rows:
        if tmdb_id:
            tmdb.add(int(tmdb_id))
        nt = norm_title(title)
        if nt:
            titles.add(nt)
    return tmdb, titles


def list_source(guard, label):
    """列一个源的 Series 目录。

    2026-11-14：`Limit` 不能硬编码 10000。实测 `lite.cn2gias.uk` 对
    `Limit=10000/1000` 返回 HTTP 400，对 **500 及以下**完全正常 ——
    它目录里有 28684 部剧（最大的一个源），却因为一个分页参数被整源跳过，
    在报表里显示成 "HTTP Error 400"。这类"整源静默缺席"最伤：
    监控不报错，只是那个源的所有内容都不会进队列。

    降级阶梯 10000 -> 1000 -> 500 -> 200，任一档成功即用。
    """
    st = guard.states[label]
    base = SG.ITEMS_PREFIX.get(label, "")
    last = None
    for lim in ("10000", "1000", "500", "200"):
        q = urllib.parse.urlencode({
            "Recursive": "true",
            "IncludeItemTypes": "Series",
            "Limit": lim,
            "Fields": (
                "ProviderIds,ProductionYear,Path,OriginalLanguage,"
                "ProductionLocations,Genres,ChildCount,RecursiveItemCount"
            ),
        })
        try:
            data = guard.get(st, base + "/Items?" + q, search=False, timeout=60)
        except Exception as e:                              # noqa: BLE001
            last = e
            # 2026-11-14：**超时/504 也要降级**，不能只有 400 才降。
            # 实测 39.96.113.82-mirror（出片占 69% 的主力源）对
            # `Limit=500` 用 252s 后返回 504 —— 旧逻辑在这里直接 raise，
            # 整个源被跳过，队列重建跑 540s 还没完（被 timeout 杀掉）。
            # 大 Limit 对慢源是致命的，降档重试才是对的做法。
            msg = str(e)
            if "400" in msg or "504" in msg or "timed out" in msg.lower() or "timeout" in msg.lower():
                continue
            raise
        return data.get("Items") or [], int(data.get("TotalRecordCount") or 0)
    raise last if last else RuntimeError("list_source failed: " + label)


# 2026-11-14（E077）：单源最多占队列的这个比例，防止大源垄断。
PER_SOURCE_SHARE = 0.35

# 2026-11-14（E079）：源的实测优先级。数值越大越优先。
# 由人工实测下载速度后写入 config/source_priority.json；
# 文件不存在时全部为 0（退回旧行为，只按集数排）。
PRIORITY_FILE = os.path.join(HERE, "config", "source_priority.json")


def load_source_priority():
    """读源优先级。**逐个字段容错** —— 一个非数值字段不能整份作废。

    2026-11-14：首版用 `{k: int(v) for ...}`，而配置里带 `_note` / `_measured`
    这类说明字段（字符串/字典），于是 `int()` 抛异常、整份返回 {}，
    优先级静默失效 —— 表现是「配置文件明明写对了，排序却没变」。
    """
    try:
        d = json.load(open(PRIORITY_FILE, encoding="utf-8"))
    except Exception:                                      # noqa: BLE001
        return {}
    out = {}
    for k, v in (d or {}).items():
        if str(k).startswith("_"):
            continue                       # 说明字段
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue                       # 单个坏值跳过，不影响其他源
    # 2026-11-20：静态文件会腐烂 —— 源站状态天天变，而数字是人写的。
    # 有产出的源必须永远排在没产出的源前面，所以消费方每轮按**真实 ok 产出**
    # 重排一次（reprioritize_sources.rank），不再依赖有人记得跑那个脚本。
    # 回归用例 test_producing_source_outranks_dead 就是这条不变式。
    try:
        import reprioritize_sources as _RP
        _budget = {}
        try:
            with open(os.path.join(HERE, ".gha_src_budget.json"), encoding="utf-8") as f:
                _budget = (json.load(f) or {}).get("sources") or {}
        except Exception:                                  # noqa: BLE001
            pass
        live = _RP.rank(d, _budget)
        if live:
            out = live
    except Exception:                                      # noqa: BLE001
        pass                               # 重排失败退回静态值，不阻断建队列
    return out


# 2026-11-14（E078）：源站没填集数时的默认值。取 12（一季典型长度），
# 只为让条目能进队列；真实集数以源站实际枚举为准，不影响入库判断。
UNKNOWN_EPISODES = 12


def build(limit=2000, apply=False):
    existing_tmdb, existing_titles = existing_library()
    guard = SG.Guard(verbose=False)
    candidates = {}
    stats = {}
    try:
        # 2026-11-15（E110）：跳过永久禁用的源。
        # 否则每 2 小时的队列重建会把它 1050 条废条目重新灌回来，
        # 而它在 gha_state 里已被 disabled —— 队列有货但一条都领不动。
        # 2026-11-16（E144）：disabled 的正源是 config/sources_static.json；
        # 运行时预算文件只是副本（日切会清空），两边都看才不会漏。
        _disabled = set()
        try:
            import gha_state as _GS
            _conf = {k: dict(v) for k, v in _GS.static_source_conf().items()
                     if isinstance(v, dict)}
            _bd = _GS._read_json(_GS.BUDGET_FILE, {})
            for _l, _r in (_bd.get("sources") or {}).items():
                if not isinstance(_r, dict):
                    continue
                _cur = _conf.setdefault(_l, {})
                for _k in ("disabled", "daily_cap"):
                    if _k in _r and _k not in _cur:
                        _cur[_k] = _r[_k]
            _disabled = {l for l, r in _conf.items() if r.get("disabled")}
        except Exception:                                  # noqa: BLE001
            _disabled = set()
        for label in sorted(guard.states):
            if label in _disabled:
                stats[label] = {"disabled": True}
                continue
            try:
                items, total = list_source(guard, label)
            except Exception as exc:  # noqa: BLE001
                stats[label] = {"error": str(exc)[:160]}
                continue
            stats[label] = {"listed": len(items), "total": total, "new": 0}
            for item in items:
                name = str(item.get("Name") or "").strip()
                if not name:
                    continue
                tmdb = item_tmdb(item)
                nt = norm_title(name)
                if tmdb and tmdb in existing_tmdb:
                    continue
                if nt and nt in existing_titles:
                    continue
                episodes = int(
                    item.get("RecursiveItemCount")
                    or item.get("ChildCount")
                    or 0
                )
                # 2026-11-14（E078）：集数 0 **不等于**「没有集」，不能直接丢。
                # 实测 xqd.enic65.eu.org 有 7364 部，但 RecursiveItemCount /
                # ChildCount 全部返回 0 —— 它不填这两个字段。旧逻辑 `if <1: continue`
                # 把整整一个源的 7364 部全丢了，而且不报错，报表上只显示 new=0。
                # 这是「整源静默缺席」的又一例：源可用、配额无限、一条都进不来。
                # 集数未知时给一个保守默认值，让它至少能进队列参与试片。
                if episodes < 1:
                    episodes = UNKNOWN_EPISODES
                key = ("tmdb", tmdb) if tmdb else ("title", nt, item_year(item))
                row = {
                    "lane": "new",
                    "video_id": int(zlib.crc32(
                        ("%s|%s" % (label, item.get("Id") or name)).encode("utf-8")
                    ) & 0x7FFFFFFF),
                    "series": name,
                    "origin_title": name,
                    "year": item_year(item),
                    "lib": lib_from_item(item),
                    "tmdb": tmdb or None,
                    "target_quality": "auto",
                    "episode_count": episodes,
                    "source_label": label,
                    "source_series_id": str(item.get("Id") or ""),
                }
                old = candidates.get(key)
                if not old or row["episode_count"] > old["episode_count"]:
                    candidates[key] = row
            stats[label]["new"] = sum(
                1 for row in candidates.values() if row["source_label"] == label
            )
    finally:
        GS.charge_guard(guard)

    rows = sorted(
        candidates.values(),
        key=lambda x: (-int(x.get("episode_count") or 0),
                       -int(x.get("year") or 0),
                       x.get("series") or ""),
    )
    deduped = {}
    for row in rows:
        key = (norm_title(row.get("series")), int(row.get("year") or 0))
        old = deduped.get(key)
        if not old:
            deduped[key] = row
            continue
        # Same title/year is the same show even when one source omitted TMDB.
        if (row.get("tmdb") and not old.get("tmdb")) or (
            bool(row.get("tmdb")) == bool(old.get("tmdb"))
            and int(row.get("episode_count") or 0) > int(old.get("episode_count") or 0)
        ):
            deduped[key] = row
    # 2026-11-14（E077）：不能只按「集数」排序后截断 —— 单源会垄断整个队列。
    # 实测 emby.chapanda.site 的中位集数是 151，其他源 27~37（它的计数口径
    # 按「部/含特别篇」，与别源不同源不可比），于是它永远排最前，
    # 吃掉 45% 的队列名额（2680/6000），而它是 1MB/s 的 STRM 站，
    # 根本下不动 —— 半条队排在一个传不动的源上。
    #
    # 改成「按源分摊名额」：每源最多占 PER_SOURCE_SHARE 的比例，
    # 剩下的名额再按集数优先级回填。这样大源仍能多进，但不会独占。
    # 2026-11-14（E079）：排序第一关键字改为「源能不能下」。
    # 实测同日不同源的可下载速率差三个数量级：
    #   yuntv 28.7 MB/s、emby.bangumi.ca 24.6 MB/s
    #   ct.cdn.bgp.yt 0.06、39.96.113.82-mirror 0.00、xqd 0.00
    # 而旧排序只看集数 —— 集数多的恰好是传不动的源，于是整条队列
    # 排在「下不动」的内容上，产能归零。集数多不等于能下。
    # 权重来自 config/source_priority.json（人工实测后落盘，可随时改）。
    _prio = load_source_priority()

    def _sort_key(x):
        lab = x.get("source_label") or "?"
        return (-_prio.get(lab, 0),
                -int(x.get("episode_count") or 0),
                -int(x.get("year") or 0),
                x.get("series") or "")

    rows = sorted(deduped.values(), key=_sort_key)
    total_cap = max(1, int(limit))
    per_src_cap = max(1, int(total_cap * PER_SOURCE_SHARE))
    picked, overflow, per_count = [], [], {}
    for row in rows:
        lab = row.get("source_label") or "?"
        if per_count.get(lab, 0) < per_src_cap:
            picked.append(row)
            per_count[lab] = per_count.get(lab, 0) + 1
        else:
            overflow.append(row)
        if len(picked) >= total_cap:
            break
    # 名额没用满（源不够多）就用溢出池按原优先级回填
    if len(picked) < total_cap:
        picked.extend(overflow[:total_cap - len(picked)])
    rows = picked
    # 2026-11-21: interleave by source so the queue head is not monopolized by one source
    _buckets = {}
    for _r in rows:
        _buckets.setdefault(_r.get("source_label") or "?", []).append(_r)
    _order = [k for k, _ in sorted(_buckets.items(), key=lambda kv: -_prio.get(kv[0], 0))]
    _inter = []
    while any(_buckets.values()):
        for _l in _order:
            if _buckets[_l]:
                _inter.append(_buckets[_l].pop(0))
    rows = _inter
    result = {
        "ok": True,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "source": "source-library-catalog",
        "candidate_total": len(candidates),
        "selected": len(rows),
        "episode_total": sum(int(x.get("episode_count") or 0) for x in rows),
        "sources": stats,
        "session": guard.summary(),
        "applied": bool(apply),
    }
    if apply:
        tmp = OUT + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({
                "generated_at": result["generated_at"],
                "source": result["source"],
                "lane": "new",
                "items": rows,
            }, fh, ensure_ascii=False, indent=1)
        os.chmod(tmp, 0o600)
        os.replace(tmp, OUT)
    print(json.dumps(result, ensure_ascii=False, indent=1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    build(limit=args.limit, apply=args.apply)


if __name__ == "__main__":
    main()
