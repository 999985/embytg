#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""todb_pull_episodes.py — 批量抓 TODB 分集标题，补齐库里缺的。

为什么需要（2026-11-15）：
    todb_episode 只有 280 行，而 todb_video 有 378945 部、todb_season 有 2747 季。
    也就是说整个「分集标题」链条只抓过 280 条 —— 而库里已经有 13433 集需要标题。
    这是「第 5 集」这类占位名的根源：数据根本没抓过，不是抓不到。

    接口一直存在：`/video/{id}/season/{n}/episode/all` 返回
    {episode_id, episode_number, episode_title, date_air, runtime}。
    todb_resolve.py 只用它来「匹配磁盘上已有的集」，从来没有回写数据库。

策略：
    · 只抓「库里已有、但 todb_episode 里没标题」的剧，避免全量 37 万部的无谓请求
    · 每部剧：拉 season/all → 对每个季拉 episode/all → upsert 进 todb_episode
    · 增量：已有真实标题的集跳过（占位标题允许覆盖）
    · 限速：复用 src_guard 的节奏，不额外加压

用法：
    python3 todb_pull_episodes.py --limit 100          # 试抓 100 部
    python3 todb_pull_episodes.py --apply              # 写入
    python3 todb_pull_episodes.py --apply --limit 500
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DB = os.path.join(HERE, "tg.db")

PLACEHOLDER = re.compile(r"^第\s*\d+\s*[集期]$|^Episode\s*\d+$|^\d+$", re.I)


def api(path, **params):
    """调 TODB。复用 todb_resolve 的 api()，保持前缀/UA 一致。"""
    import todb_resolve
    return todb_resolve.api(path, **params)


SKIP_FILE = os.path.join(HERE, ".todb_no_title.json")


def _no_title_set():
    """TODB 确实没有真实标题的 video_id 集合（避免每轮重复抓）。"""
    try:
        with open(SKIP_FILE, encoding="utf-8") as fh:
            d = json.load(fh)
        return set(str(x) for x in (d.get("vids") or []))
    except Exception:                                      # noqa: BLE001
        return set()


def _mark_no_title(vid):
    s = _no_title_set()
    s.add(str(vid))
    try:
        tmp = SKIP_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"vids": sorted(s)[-5000:]}, fh)
        os.replace(tmp, SKIP_FILE)
    except Exception:                                      # noqa: BLE001
        pass


def need_videos(limit):
    """库里已有、但分集标题缺失的剧。

    2026-11-15：判据是「该剧在 todb_episode 里一条真实标题都没有」。
    全量 37 万部逐部抓不现实（一部 200 集就是 200 次请求），
    而**真正需要的只有我们库里已经收了的那些**。
    """
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=20)
    try:
        # **按库里实际集数排序**：先补最需要的，而不是随机抓冷门剧。
        # 之前按 video_id 倒序，抓到的是 Muchacha de barrio(1979) 这种
        # 西语老剧 —— 抓了也用不上。库里集数多的才是真缺口。
        rows = con.execute(
            "select v.video_id, v.title, v.year, r.c as need "
            "from todb_video v "
            "join ("
            "  select tmdb, count(*) c from resource_state "
            "  where kind='episode' and tmdb is not null and tmdb>0 "
            "  group by tmdb"
            ") r on r.tmdb = v.tmdb_id "
            "where not exists ("
            "  select 1 from todb_episode e where e.video_id = v.video_id"
            "  and e.title is not null and e.title != ''"
            "  and e.title not glob '第[0-9]*集'"
            ") order by r.c desc limit ?",
            (max(1, limit) + len(_no_title_set()),)).fetchall()
    finally:
        con.close()
    _skip = _no_title_set()
    rows = [r for r in rows if str(r[0]) not in _skip]
    return rows[:max(1, limit)]


def existing_titles(con, video_id):
    """已有真实标题的 (season, ep) 集合；占位标题不算有。"""
    out = set()
    try:
        for (s, e, t) in con.execute(
                "select season_number, episode_number, title from todb_episode "
                "where video_id=?", (video_id,)):
            if t and not PLACEHOLDER.match(str(t).strip()):
                out.add((int(s or 0), int(e or 0)))
    except Exception:                                      # noqa: BLE001
        pass
    return out


def pull_one(con, video_id, apply=False):
    """抓一部剧的全部分集。返回 (写入条数, 跳过条数)。"""
    wrote = skip = 0
    try:
        seasons = api("/video/%s/season/all" % video_id)
    except Exception:                                      # noqa: BLE001
        return 0, 0
    rows = seasons if isinstance(seasons, list) else (seasons.get("data") or [])
    # 2026-11-15（E112 补）：**TODB 返回 0 季 = 这部剧在它那边没有季集数据。**
    # 实测「谁摘下了月亮的羽毛？」season/all 返回 0 季，
    # 于是 pull_one 写 0 条 → 不触发"无真标题"标记 → 下轮又被选中 →
    # 永远循环。这类剧要直接进跳过名单。
    if not rows:
        _mark_no_title(video_id)
        return 0, 0
    have = existing_titles(con, video_id)
    for s in rows:
        sn = int(s.get("season_number") or 0)
        try:
            eps = api("/video/%s/season/%d/episode/all" % (video_id, sn))
        except Exception:                                  # noqa: BLE001
            continue
        eps = eps if isinstance(eps, list) else (eps.get("data") or [])
        for e in eps:
            en = int(e.get("episode_number") or 0)
            title = (e.get("episode_title") or "").strip()
            if not title:
                continue
            if (sn, en) in have:
                skip += 1
                continue
            wrote += 1
            if apply:
                try:
                    con.execute(
                        "insert or replace into todb_episode"
                        "(episode_id, video_id, season_number, episode_number,"
                        " date_air, title, parts_count) "
                        "values(?,?,?,?,?,?,?)",
                        (int(e.get("episode_id") or 0), int(video_id), sn, en,
                         e.get("date_air") or "", title, 0))
                except Exception:                          # noqa: BLE001
                    pass
    if apply:
        con.commit()
    # 2026-11-15（E112）：抓完一集真标题都没有 → 记下来，下次别再抓。
    # 实测「人鱼小姐」TODB 只给「第1集」这种占位，脚本每轮都重新选中它，
    # 246 集反复抓两遍、白烧请求。TODB 不会一夜之间补上标题。
    if wrote and not skip:
        try:
            import sqlite3 as _s
            _c = _s.connect("file:%s?mode=ro" % DB, uri=True, timeout=10)
            try:
                _n = _c.execute(
                    "select count(*) from todb_episode where video_id=? "
                    "and title is not null and title != '' "
                    "and title not glob '第[0-9 ]*[集期]' "
                    "and title not glob 'Episode[0-9 ]*'",
                    (int(video_id),)).fetchone()[0]
            finally:
                _c.close()
            if _n == 0:
                _mark_no_title(video_id)
        except Exception:                                  # noqa: BLE001
            pass
    return wrote, skip


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    vids = need_videos(a.limit)
    print("待处理 %d 部剧%s" % (len(vids), "（写入）" if a.apply else "（dry-run）"))

    con = sqlite3.connect(DB, timeout=30)
    total_w = total_s = 0
    t0 = time.time()
    for i, row in enumerate(vids, 1):
        vid, title, year = row[0], row[1], row[2]
        w, s = pull_one(con, vid, apply=a.apply)
        total_w += w
        total_s += s
        if w:
            print("  [%d/%d] %s (%s) +%d 集 (跳过 %d)"
                  % (i, len(vids), str(title)[:24], year, w, s), flush=True)
    con.close()
    print("完成：写入 %d 集，跳过 %d 集，用时 %.0fs"
          % (total_w, total_s, time.time() - t0))


if __name__ == "__main__":
    main()
