#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_ingest_worklist.py — 从「建好的数据库」生成入库工单（只读，不入库）。

为什么需要它（2026-11-09 用户「根据建好的数据库去入库」）：
  tg.db 里 moviewatch / todb_video 是**片单与台账**，不含 TG 消息 ID。
  tgpipe.py apply 的 item 必须带 ch/msg/size/qual/season/lib/mid 才能引用式转发，
  所以「入库」必须分两步：① 本工具排候选工单 → ② moviewatch.py ingest / tgpipe.py pull
  找源拿到 ch/msg → ③ apply 转发登记。本工具只做 ①，绝不伪造消息 ID。

安全（SCALING §1 + 用户「基础要牢」）：
  - sqlite3.connect('file:...?mode=ro', uri=True) + PRAGMA query_only=ON 双保险
  - 不写 tg.db、不写 tg_pending.json、不调用 TG、不 POST Emby、不生成 NFO/poster
  - 不 apply / 不 scan / 不改 CHANNELS / 不碰改名 agent 的文件
  - 只读 SELECT + 游标逐行，绝不把 378217 行拉进内存
  - 输出写到独立 worklist 文件，与生产队列完全隔离

用法：
  python3 build_ingest_worklist.py --limit 20 --out /opt/embytg/pipeline/ingest_worklist.json
  python3 build_ingest_worklist.py --limit 20 --kind movie --verify-only
"""
import argparse
import datetime
import json
import os
import sqlite3
import sys

DB_URI = "file:/opt/embytg/pipeline/tg.db?mode=ro"
DEFAULT_OUT = "/opt/embytg/pipeline/ingest_worklist.json"

# 已有库锚点：这些状态的片不应再进追片工单
LANDED_STATUSES = ("已上", "已入队")


def connect():
    con = sqlite3.connect(DB_URI, uri=True)
    con.execute("PRAGMA query_only=ON")
    con.row_factory = sqlite3.Row
    return con


def collect(con, limit, kind):
    """游标逐行取 unseen 高人气片，排除已有库状态。"""
    where = "status='unseen'"
    params = []
    if kind in ("movie", "tv"):
        where += " AND kind=?"
        params.append(kind)
    params.append(limit)
    rows = []
    for r in con.execute(
        "SELECT id,kind,name_zh,name_orig,year,popularity,genres,status "
        "FROM moviewatch WHERE %s ORDER BY popularity DESC LIMIT ?" % where,
        params,
    ):
        rows.append(dict(r))
    return rows


def stats(con):
    out = {}
    for k, n in con.execute("SELECT status,count(*) FROM moviewatch GROUP BY status"):
        out[k] = n
    out["_todb_video"] = con.execute("SELECT count(*) FROM todb_video").fetchone()[0]
    out["_find_state"] = {
        k: n for k, n in con.execute(
            "SELECT coalesce(find_state,'<null>'),count(*) FROM todb_video GROUP BY find_state"
        )
    }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--kind", choices=["movie", "tv", "all"], default="all")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--verify-only", action="store_true", help="只打印统计，不写工单文件")
    a = ap.parse_args()
    if a.limit <= 0:
        print("ERROR: --limit 必须 > 0", file=sys.stderr)
        return 2

    con = connect()
    s = stats(con)
    rows = collect(con, a.limit, a.kind)

    # 真自检：工单里不得出现任何已有库状态（差集正确性）
    leaked = [r["id"] for r in rows if r["status"] in LANDED_STATUSES]
    print("# moviewatch 状态分布: %s" % json.dumps(s, ensure_ascii=False))
    print("# 候选数=%d kind=%s leaked_landed=%d" % (len(rows), a.kind, len(leaked)))
    if leaked:
        print("ERROR: 工单混入已有库条目 %s" % leaked[:10], file=sys.stderr)
        return 1

    if a.verify_only:
        print("# verify-only，未写文件")
        return 0

    wl = {
        "generated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": "tg.db (mode=ro) / moviewatch.status=unseen",
        "db": DB_URI,
        "filter": {"kind": a.kind, "limit": a.limit},
        "moviewatch_stats": s,
        "candidate_count": len(rows),
        "leaked_landed": len(leaked),
        # 注意：这里只有片名与 TMDB id，**没有 ch/msg**。不能直接喂 apply。
        "items": [
            {
                "mv_id": r["id"],
                "tmdb_id": r["id"],  # moviewatch.id 即 TMDB id（moviewatch.py sync 用 id 列存 TMDB id）
                "kind": r["kind"],
                "name_zh": r["name_zh"],
                "name_orig": r["name_orig"],
                "year": r["year"],
                "popularity": r["popularity"],
                "genres": r["genres"],
                "status": r["status"],
                "needs": ["找源拿 ch/msg", "双源准入", "画质择优", "apply 引用转发"],
            }
            for r in rows
        ],
        "not_included_by_design": [
            "TG 消息 ID (ch/msg) —— 必须由 ingest 或 pull 真实搜到，绝不伪造",
            "size / qual / season / lib —— 由 tgpipe.py 解析 caption 后生成",
            "todb_video 的 373182 条 unsearched —— 全量候选见 gap_list.py，本工单只取人气最高的 moviewatch unseen",
        ],
        "sop": [
            "0) 开闸：Scan media library State=Idle 且 Episode 两轮稳定 且 find cache/vfs -type f|wc -l == 0",
            "1) 找源：python3 moviewatch.py ingest --n 5 --bots <bot>（会写 tg_pending.json items 与 moviewatch status）",
            "   或只读反查：python3 tgpipe.py pull --want '<片名>' --limit 20",
            "2) 预演：python3 tgpipe.py apply --from-queue --dry --max-items 5",
            "3) 小批量真实入库：python3 tgpipe.py apply --from-queue --loop --max-items 5",
            "   默认增量通知 POST /Library/Media/Updated（E008），EMBYTG_REFRESH_MODE=full 可回滚",
            "4) 复核：python3 lib_reconcile.py --json；python3 finalize_meta.py --audit",
            "5) 收敛后再补 NFO（用户本轮『先不刮削』，此步留到下一轮）",
        ],
        "guard": "本文件为只读工单，不触发任何外部写；执行 SOP 前先读 docs/OPS_JOURNAL.md 最新条目",
    }
    tmp = a.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(wl, f, ensure_ascii=False, indent=2)
    os.replace(tmp, a.out)
    print("# 工单已写 %s (%d 条，%d bytes)" % (a.out, len(rows), os.path.getsize(a.out)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
