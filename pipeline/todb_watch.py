# -*- coding: utf-8 -*-
"""todb_watch.py —— TODB 影片库状态机（用户 m01031「维护好 todb 数据库」）

配合 todb_sync.py（全量清单 378217 条已落 tg.db）使用。职责：
  1. 给 todb_video 加业务状态列（迁移，幂等）
  2. 对账：把我们库里已有的 tmdb ID → 反查 TODB video_id → 标 landed
  3. 状态机：区分「有哪些/没找过/没资源/现在没有以后有/连载未完结」
  4. 状态分布查询

用法：
  python3 todb_watch.py --migrate                 # 加列（幂等）
  python3 todb_watch.py --status                 # 状态分布
  python3 todb_watch.py --reconcile [--limit N]  # 对账（默认全量，断点续跑）
  python3 todb_watch.py --map-batch N            # 给 unsearched 片批量补 tmdb_id（网关 B externalIds）

网关分工见 docs/TODB_API.md：
  A = https://theotherdb.org/api（公开，全量清单/季集）
  B = https://api.theotherdb.org（Bearer 密钥，externalIds/反查）
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request

DB = "/opt/embytg/pipeline/tg.db"
CFG = "/opt/embytg/config.json"
PENDING = "/opt/embytg/pipeline/tg_pending.json"

_conf = json.load(open(CFG))
GW_B = _conf.get("todb_api_gateway", "https://api.theotherdb.org")
GW_A = _conf.get("todb_api", "https://theotherdb.org") + "/api"
KEY = _conf.get("todb_api_key", "")
PG = _conf.get("pg_query", "docker exec -i teldrive-postgres psql -U teldrive -d teldrive -At")


def db():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


def gw_a(path, retries=3):
    """网关 A：公开，无密钥（详情/季集）"""
    h = {"accept": "application/json", "user-agent": "Mozilla/5.0 Chrome/156.0 Safari/537.36"}
    for i in range(retries):
        try:
            req = urllib.request.Request(GW_A + path, headers=h)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(1.5 * (i + 1))


def backfill(c, n):
    """回填 todb_status/expect_eps：网关 A /video/{id} + /season/all。
    只处理还没回填过的（todb_status IS NULL），断点续跑靠该列。
    对 find_state=landed/partial 的片优先，因为连载状态最有用。"""
    rows = c.execute("""SELECT video_id, video_type FROM todb_video
        WHERE todb_status IS NULL
        ORDER BY (find_state='landed') DESC, (find_state='partial') DESC, video_id DESC
        LIMIT ?""", (n,)).fetchall()
    print(f"回填 {len(rows)} 条")
    t0, ok = time.time(), 0
    for i, r in enumerate(rows, 1):
        vid, vtype = r["video_id"], r["video_type"]
        d = gw_a(f"/video/{vid}")
        status = (d or {}).get("status") if isinstance(d, dict) else None
        expect = 0
        if vtype == "tv":
            sa = gw_a(f"/video/{vid}/season/all")
            if isinstance(sa, list):
                # 排除 S0 特别篇，其余季 episode_count 求和
                expect = sum(int(s.get("episode_count") or 0) for s in sa
                             if int(s.get("season_number") or 0) > 0)
                # 有季结构的顺带落 todb_season（幂等）
                for s in sa:
                    c.execute("""INSERT OR IGNORE INTO todb_season(
                        season_id,video_id,season_number,episode_count,date_air,synced_at)
                        VALUES(?,?,?,?,?,?)""",
                              (s.get("season_id"), vid, s.get("season_number"),
                               s.get("episode_count"), s.get("date_air"), int(time.time())))
        c.execute("""UPDATE todb_video SET todb_status=?, expect_eps=?,
                    last_checked_at=? WHERE video_id=?""",
                  (status, expect, int(time.time()), vid))
        ok += 1
        if i % 50 == 0:
            c.commit()
            print(f"  {i}/{len(rows)} ok={ok} {time.time()-t0:.0f}s "
                  f"eta={(len(rows)-i)*(time.time()-t0)/i:.0f}s", flush=True)
    c.commit()
    print(f"DONE 回填 {ok} 条（status/expect_eps）")


def gw_b(path, retries=3):
    """网关 B：Bearer 密钥"""
    h = {"Authorization": "Bearer " + KEY, "accept": "application/json",
         "user-agent": "Mozilla/5.0 Chrome/156.0 Safari/537.36"}
    for i in range(retries):
        try:
            req = urllib.request.Request(GW_B + path, headers=h)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(1.5 * (i + 1))


COLS = [
    ("find_state", "TEXT DEFAULT 'unsearched'"),       # unsearched/searching/found/no_resource/partial/landed
    ("todb_status", "TEXT"),                           # rumored/planned/released/canceled
    ("has_meta", "INTEGER DEFAULT 1"),                 # TODB 有片名元数据（几乎恒 1）
    ("expect_eps", "INTEGER DEFAULT 0"),               # 应有集数（season/all 求和，排 S0）
    ("landed_eps", "INTEGER DEFAULT 0"),               # 我们库已入集数
    ("tmdb_id", "TEXT"), ("imdb_id", "TEXT"), ("tvdb_id", "TEXT"),
    ("last_checked_at", "INTEGER"), ("next_check_at", "INTEGER"),
    ("checked_count", "INTEGER DEFAULT 0"),           # 搜过几次（no_resource 复核节奏）
]


def migrate(c):
    have = {r["name"] for r in c.execute("PRAGMA table_info(todb_video)")}
    for name, decl in COLS:
        if name not in have:
            c.execute(f"ALTER TABLE todb_video ADD COLUMN {name} {decl}")
            print(f"  + 列 {name}")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tv_find ON todb_video(find_state)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tv_next ON todb_video(next_check_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tv_tmdb ON todb_video(tmdb_id)")
    c.commit()
    print("migrate OK，列数:", len(c.execute("PRAGMA table_info(todb_video)").fetchall()))


def lib_tmdb_ids():
    """从 teldrive.files 的目录名里抽 {tmdb=ID}（我们库唯一真源）。"""
    q = ("select distinct name from files where deleted_at is null "
         "and (name like '%{tmdb=%' or name like '%{todb=%')")
    out = subprocess.run(["bash", "-lc",
                          f'{PG} -c "{q}"'],
                         capture_output=True, text=True, timeout=120)
    import re
    tmdb, todb = set(), set()
    for line in (out.stdout or "").splitlines():
        for m in re.finditer(r"\{tmdb=(\d+)\}", line):
            tmdb.add(int(m.group(1)))
        for m in re.finditer(r"\{todb=(\d+)\}", line):
            todb.add(int(m.group(1)))
    return tmdb, todb


def reconcile(c, limit=0):
    """我们库 tmdb ID → 网关 B getVideoIdByExternalId → TODB video_id → 标 landed。
    断点续跑：find_state='landed' 的跳过；结果写 todb_watch_meta 记录已查 tmdb。"""
    c.execute("""CREATE TABLE IF NOT EXISTS todb_watch_meta(
        k TEXT PRIMARY KEY, v TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS todb_tmdb_tried(
        tmdb INTEGER PRIMARY KEY, video_ids TEXT, tried_at INTEGER)""")
    c.commit()
    tmdb_ids, todb_ids = lib_tmdb_ids()
    print(f"库内目录抽到 tmdb={len(tmdb_ids)} 个 / todb={len(todb_ids)} 个")

    # 直接有 todb= 的先标
    for vid in todb_ids:
        c.execute("UPDATE todb_video SET find_state='landed' WHERE video_id=?", (vid,))

    tried = {r[0] for r in c.execute("SELECT tmdb FROM todb_tmdb_tried")}
    todo = [t for t in sorted(tmdb_ids) if t not in tried]
    if limit:
        todo = todo[:limit]
    print(f"待反查 {len(todo)} 个 tmdb（已查 {len(tried)}）")
    t0 = time.time()
    hit = miss = 0
    for i, tid in enumerate(todo, 1):
        d = gw_b(f"/video/getVideoIdByExternalId?type=tmdb&value={tid}")
        vids = [it["video_id"] for it in d] if isinstance(d, list) else []
        for vid in vids:
            c.execute("""UPDATE todb_video SET find_state='landed', tmdb_id=?,
                        last_checked_at=? WHERE video_id=?""",
                      (str(tid), int(time.time()), vid))
            hit += 1
        if not vids:
            miss += 1
        c.execute("INSERT OR REPLACE INTO todb_tmdb_tried VALUES(?,?,?)",
                  (tid, json.dumps(vids), int(time.time())))
        if i % 50 == 0:
            c.commit()
            print(f"  {i}/{len(todo)} hit={hit} miss={miss} {time.time()-t0:.0f}s "
                  f"eta={(len(todo)-i)*(time.time()-t0)/i:.0f}s", flush=True)
    c.commit()
    print(f"DONE 反查 {len(todo)} 个，命中 TODB video {hit} 条，无映射 {miss} 个")


def map_batch(c, n):
    """给 find_state='unsearched' 且 tmdb_id 空的片，用网关 B externalIds 补外部 ID。
    目的：TODB→TMDB 映射落库，供后续按 tmdb 搜源/对账。断点续跑靠 tmdb_id IS NULL。"""
    rows = c.execute("""SELECT video_id FROM todb_video
        WHERE tmdb_id IS NULL AND (find_state='unsearched' OR find_state IS NULL)
        ORDER BY video_id DESC LIMIT ?""", (n,)).fetchall()
    print(f"补映射 {len(rows)} 条")
    t0, ok = time.time(), 0
    for i, r in enumerate(rows, 1):
        vid = r["video_id"]
        d = gw_b(f"/video/{vid}/externalIds")
        if not isinstance(d, list):
            continue
        ex = {it.get("type"): str(it.get("value")) for it in d if it.get("value")}
        c.execute("""UPDATE todb_video SET tmdb_id=?, imdb_id=?, tvdb_id=?,
                    last_checked_at=? WHERE video_id=?""",
                  (ex.get("tmdb"), ex.get("imdb"), ex.get("tvdb_id") or ex.get("tvdb"),
                   int(time.time()), vid))
        ok += 1
        if i % 50 == 0:
            c.commit()
            print(f"  {i}/{len(rows)} ok={ok} {time.time()-t0:.0f}s", flush=True)
    c.commit()
    print(f"DONE 补 {ok} 条映射")


def status(c):
    print("=== todb_video 总量 ===")
    print(" ", c.execute("SELECT count(*) FROM todb_video").fetchone()[0])
    print("=== 按 video_type ===")
    for r in c.execute("SELECT video_type,count(*) FROM todb_video GROUP BY video_type"):
        print(f"  {r[0]}: {r[1]}")
    print("=== 按 find_state ===")
    for r in c.execute("SELECT COALESCE(find_state,'<null>'),count(*) FROM todb_video GROUP BY find_state ORDER BY 2 DESC"):
        print(f"  {r[0]}: {r[1]}")
    print("=== tmdb_id 映射覆盖 ===")
    print("  有 tmdb_id:", c.execute("SELECT count(*) FROM todb_video WHERE tmdb_id IS NOT NULL AND tmdb_id!=''").fetchone()[0])
    print("=== todb_status（网关 A 详情回填值，未回填则全 NULL）===")
    for r in c.execute("SELECT COALESCE(todb_status,'<null>'),count(*) FROM todb_video GROUP BY todb_status ORDER BY 2 DESC LIMIT 8"):
        print(f"  {r[0]}: {r[1]}")
    print("=== 季/集/已反查 ===")
    for t in ["todb_season", "todb_episode", "todb_tmdb_tried"]:
        try:
            print(f"  {t}:", c.execute(f"SELECT count(*) FROM {t}").fetchone()[0])
        except Exception:
            print(f"  {t}: <无表>")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--migrate", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--reconcile", action="store_true")
    ap.add_argument("--map-batch", type=int, default=0)
    ap.add_argument("--backfill", type=int, default=0, help="回填 status/expect_eps 的条数")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    c = db()
    if a.migrate:
        migrate(c)
    elif a.reconcile:
        reconcile(c, a.limit)
    elif a.map_batch:
        map_batch(c, a.map_batch)
    elif a.backfill:
        backfill(c, a.backfill)
    else:
        status(c)
