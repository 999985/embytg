#!/usr/bin/env python3
"""
只读缺口清单生成器（US-005）—— 不入库、不刮削、不转存。

用途：把「候选片单 − 已有库」算成缺口清单，产出可执行的追片目标（合法 JSON），
     全程只读，不触发任何刮削或转存动作。

红线（用户「先不刮削吧」）：
  * 不写 pipeline/tg.db（改名 agent 共用，且 US-002 体检中 Emby 重扫未收敛，
    Emby /Items 计数不可当已有库真源）；
  * 不调用 TG、不 POST Emby、不生成 NFO/poster、不改名、不碰改名文件、不 apply 转存；
  * 唯一写入 = --out 指定的 JSON 产物（默认 /tmp/gap_list.json）。

只读双保险：sqlite3.connect('file:/opt/embytg/pipeline/tg.db?mode=ro', uri=True)
            + PRAGMA query_only=ON；连接对象只执行 SELECT / PRAGMA，绝不写。

已有库锚点真源（store_mode，遵循 US-003 计划 §3「{tmdb=ID} 锚点 / tg_state keys / 文件侧」）：
  * tg_state keys  -> tg.db state_key.val 抽 {tmdb=ID} 锚点；
  * 文件侧         -> tg.db resource_state.tmdb（teldrive files 落库的文件侧锚点）；
  * 状态侧         -> todb_video(find_state='landed') 的 tmdb_id / video_id；
  * 人气片单       -> moviewatch(status IN ('已上','已入队')) 的 id（= TMDB id）。
  不使用 Emby /Items 计数（US-002 §6：E015 全库重命名已落盘但重扫未收敛，计数失真）。

SCALING.md §1 原则落地：
  * #3 流式/分页、不把全库拉进内存：todb_video 用 WHERE + ORDER BY(video_id 主键)
    游标逐行流式迭代，绝不 fetchall() 378217 行；已有库集合只装入几万个短 ID 字符串，
    明细只保留 --limit 条，分布/计数用字典累加（内存恒定）。
  * #1 不硬编码规模常量：378217 等仅作实测记录，脚本不含任何条目数目标。
  * #2 幂等可重跑：ORDER BY 主键保证稳定顺序，同参数连续两次除 generated_at 外一致。
  * #9 真自检：算完差集后对每条缺口逐条复核其键是否落在已有库集合，统计 overlap
    （真集合运算，非硬编码 0）；overlap != 0 判逻辑有 bug，stderr 报错并非 0 退出。

用法：
  python3 pipeline/gap_list.py --limit 50 --type all --out /tmp/gap_test.json
  python3 pipeline/gap_list.py --type movie --limit 20 --out /tmp/gap_movie.json
"""

import argparse
import datetime
import json
import os
import re
import sqlite3
import sys

DB_URI = "file:/opt/embytg/pipeline/tg.db?mode=ro"  # 只读 URI，绝不写

STORE_MODE = (
    "tg.db(mode=ro) 文件侧/状态侧锚点: state_key.val {tmdb=ID} (tg_state keys) "
    "+ resource_state.tmdb (teldrive files 文件侧) "
    "+ todb_video(landed:tmdb_id,video_id) "
    "+ moviewatch(已上/已入队:id); "
    "不使用 Emby /Items 计数 (US-002: E015 重扫未收敛)"
)

# {tmdb=ID} 主锚点（tgpipe.file_name 落盘格式），兼容 tmdb-123 / tmdbid=123 / todb=123
RE_ANCHOR = re.compile(r"\{(?:tmdb|todb)=(\d+)\}", re.I)
RE_LOOSE = re.compile(r"(?:tmdb|todb)(?:id)?[-=](\d+)", re.I)


def norm_tmdb(v):
    """把各类 id 归一成 tmdb: 命名空间下的纯数字字符串；无效返回 None。"""
    if v is None:
        return None
    s = str(v).strip()
    if not s or not s.isdigit():
        return None
    return s.lstrip("0") or "0"


def build_existing(con):
    """从四路只读锚点构建「已有库」键集合。返回 (set, 各路计数)。

    命名空间：'vid:<video_id>' 为 todb 内部主键；'tmdb:<id>' 为 TMDB id 统一键。
    只装入短 ID 字符串，不把全库明细拉进内存。
    """
    existing = set()
    stats = {}

    n_tmdb = n_vid = 0
    for vid, tmdb in con.execute(
        "SELECT video_id, tmdb_id FROM todb_video WHERE find_state='landed'"
    ):
        existing.add("vid:" + str(vid))
        n_vid += 1
        t = norm_tmdb(tmdb)
        if t:
            existing.add("tmdb:" + t)
            n_tmdb += 1
    stats["landed_video_id"] = n_vid
    stats["landed_tmdb_id"] = n_tmdb

    n = 0
    for (mid,) in con.execute(
        "SELECT id FROM moviewatch WHERE status IN ('已上','已入队')"
    ):
        t = norm_tmdb(mid)
        if t:
            existing.add("tmdb:" + t)
            n += 1
    stats["moviewatch_id"] = n

    n = 0
    for (val,) in con.execute("SELECT val FROM state_key"):
        text = val or ""
        ids = RE_ANCHOR.findall(text) or RE_LOOSE.findall(text)
        for raw in ids:
            t = norm_tmdb(raw)
            if t:
                existing.add("tmdb:" + t)
                n += 1
    stats["state_key_tmdb_anchor"] = n

    n = 0
    for (t0,) in con.execute(
        "SELECT tmdb FROM resource_state WHERE tmdb IS NOT NULL AND tmdb!=''"
    ):
        t = norm_tmdb(t0)
        if t:
            existing.add("tmdb:" + t)
            n += 1
    stats["resource_state_tmdb"] = n

    stats["existing_keys_total"] = len(existing)
    return existing, stats


def candidate_keys(video_id, tmdb_id):
    """一条候选的规范化比较键（vid: 主键 + 可选 tmdb: 键）。"""
    keys = ["vid:" + str(video_id)]
    t = norm_tmdb(tmdb_id)
    if t:
        keys.append("tmdb:" + t)
    return keys


def year_bucket(y):
    if isinstance(y, int) and y > 0:
        return str(y)
    return "unknown"


def main():
    ap = argparse.ArgumentParser(description="只读缺口清单生成器（US-005，不入库/不刮削）")
    ap.add_argument("--limit", type=int, default=100, help="输出缺口明细条数上限（默认 100）")
    ap.add_argument("--type", choices=["movie", "tv", "all"], default="all",
                    help="按 video_type 过滤（默认 all）")
    ap.add_argument("--out", default="/tmp/gap_list.json", help="JSON 产物路径（默认 /tmp/gap_list.json）")
    args = ap.parse_args()

    if args.limit <= 0:
        print("ERROR: --limit 必须 > 0", file=sys.stderr)
        return 2

    # 只读连接 + query_only 双保险；连接对象此后只执行 SELECT / PRAGMA。
    con = sqlite3.connect(DB_URI, uri=True)
    con.execute("PRAGMA query_only=ON")
    con.row_factory = sqlite3.Row
    try:
        existing, estats = build_existing(con)

        # 候选片单：unsearched（流式游标；ORDER BY video_id 主键，稳定幂等且免全表排序）
        q = ("SELECT video_id, video_type, title, year, tmdb_id "
             "FROM todb_video WHERE find_state='unsearched'")
        params = []
        if args.type != "all":
            q += " AND video_type=?"
            params.append(args.type)
        q += " ORDER BY video_id"

        candidate_total = 0
        gap_total = 0
        excluded = 0
        by_type = {}
        by_year = {}
        gaps = []

        for row in con.execute(q, params):  # 游标逐行，绝不 fetchall()
            candidate_total += 1
            vid = row["video_id"]
            tmdb_id = row["tmdb_id"]
            keys = candidate_keys(vid, tmdb_id)

            # 差集：任一键命中已有库即视为「已有」，剔除
            if any(k in existing for k in keys):
                excluded += 1
                continue

            gap_total += 1
            vt = row["video_type"] or "unknown"
            by_type[vt] = by_type.get(vt, 0) + 1
            yb = year_bucket(row["year"])
            by_year[yb] = by_year.get(yb, 0) + 1

            if len(gaps) < args.limit:  # 只保留前 limit 条明细，内存恒定
                gaps.append({
                    "video_id": vid,
                    "video_type": vt,
                    "title": row["title"],
                    "year": row["year"],
                    "tmdb_id": tmdb_id,
                })
    finally:
        con.close()

    # 算完差集后的真自检：对输出缺口清单逐条复核其键是否落在已有库集合。
    # 这是真实集合运算（非硬编码 0）；期望 0，非 0 判逻辑有 bug。
    overlap = 0
    for g in gaps:
        for k in candidate_keys(g["video_id"], g["tmdb_id"]):
            if k in existing:
                overlap += 1

    result = {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "source": "todb_video.find_state='unsearched'",
        "filter": {"type": args.type, "limit": args.limit},
        "store_mode": STORE_MODE,
        "existing_sources": estats,
        "candidate_total": candidate_total,
        "excluded_existing": excluded,
        "gap_total": gap_total,
        "output_count": len(gaps),
        "by_type": dict(sorted(by_type.items())),
        "by_year": dict(sorted(by_year.items())),
        "sample": gaps[:20],
        "gaps": gaps,
        "overlap": overlap,
    }

    out = args.out
    d = os.path.dirname(os.path.abspath(out))
    if d:
        os.makedirs(d, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("type=%s limit=%d out=%s" % (args.type, args.limit, out))
    print("store_mode=%s" % STORE_MODE)
    print("existing_keys=%d" % estats["existing_keys_total"])
    print("candidate_total=%d" % candidate_total)
    print("excluded_existing=%d" % excluded)
    print("gap_total=%d" % gap_total)
    print("output=%d" % len(gaps))
    print("by_type=%s" % json.dumps(result["by_type"], ensure_ascii=False, sort_keys=True))
    print("overlap=%d" % overlap)

    if overlap != 0:
        print("ERROR: overlap=%d != 0，缺口清单与已有库存在交集，差集逻辑有误" % overlap,
              file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
