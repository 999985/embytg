# -*- coding: utf-8 -*-
"""TODB 专注路线：全量清单 + 季/集结构 落 SQLite（tg.db）

用法：
  python3 todb_sync.py --pages 0        # 全量 3782 页（约 44 分钟）
  python3 todb_sync.py --pages 5        # 试跑 5 页
  python3 todb_sync.py --detail 1146    # 拉单剧季+集结构
  python3 todb_sync.py --status         # 看进度

表设计（tg.db，与 moviewatch 共库）：
  todb_video(video_id PK, video_type, title, origin_title, date_air, year,
             countrys, languages, status, synced_at)          -- 清单层
  todb_season(season_id PK, video_id, season_number, episode_count,
              date_air, synced_at)                            -- 季层（按需拉）
  todb_episode(episode_id PK, video_id, season_number, episode_number,
               date_air, title, parts_count)                  -- 集层（按需拉）
  todb_meta(k PK, v)                                          # cursor/进度标量
"""
import argparse
import json
import sqlite3
import sys
import time
import urllib.request

DB = "/opt/embytg/pipeline/tg.db"
B = "https://theotherdb.org/api"


def get(path, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(B + path, headers={
                "accept": "application/json", "user-agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(2 * (i + 1))


def db():
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS todb_video(
      video_id INTEGER PRIMARY KEY, video_type TEXT, title TEXT,
      origin_title TEXT, date_air TEXT, year INTEGER, countrys TEXT,
      languages TEXT, status TEXT DEFAULT 'list', synced_at INTEGER);
    CREATE TABLE IF NOT EXISTS todb_season(
      season_id INTEGER PRIMARY KEY, video_id INTEGER, season_number INTEGER,
      episode_count INTEGER, date_air TEXT, synced_at INTEGER);
    CREATE TABLE IF NOT EXISTS todb_episode(
      episode_id INTEGER PRIMARY KEY, video_id INTEGER, season_number INTEGER,
      episode_number INTEGER, date_air TEXT, title TEXT, parts_count INTEGER);
    CREATE TABLE IF NOT EXISTS todb_meta(k TEXT PRIMARY KEY, v TEXT);
    CREATE INDEX IF NOT EXISTS idx_todb_video_title ON todb_video(title);
    CREATE INDEX IF NOT EXISTS idx_todb_video_year ON todb_video(year);
    """)
    return c


def sync_pages(c, max_pages):
    total = int(get("/video/list?page=1&page_size=1")["total"])
    pages = (total + 99) // 100 if not max_pages else max_pages
    done = int(c.execute("SELECT v FROM todb_meta WHERE k='list_page'").fetchone()[0] or 0) if \
        c.execute("SELECT v FROM todb_meta WHERE k='list_page'").fetchone() else 0
    t0, n = time.time(), 0
    for p in range(done + 1, pages + 1):
        r = get(f"/video/list?page={p}&page_size=100")
        rows = [(it["video_id"], it.get("video_type"), it.get("video_title"),
                 it.get("origin_title"), it.get("date_air"),
                 (it.get("date_air") or "")[:4] or None,
                 json.dumps(it.get("origin_countrys") or [], ensure_ascii=False),
                 json.dumps(it.get("original_languages") or [], ensure_ascii=False),
                 int(time.time()))
                for it in r["items"]]
        c.executemany("""INSERT INTO todb_video(video_id,video_type,title,origin_title,
            date_air,year,countrys,languages,synced_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(video_id) DO UPDATE SET title=excluded.title,
            date_air=excluded.date_air, synced_at=excluded.synced_at""", rows)
        c.execute("INSERT INTO todb_meta(k,v) VALUES('list_page',?) "
                  "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(p),))
        c.commit()
        n += len(rows)
        if p % 50 == 0 or p == pages:
            el = time.time() - t0
            print(f"page {p}/{pages} rows={n} {el:.0f}s eta={((pages-p)*el/max(p-done,1)):.0f}s", flush=True)
    print(f"DONE rows={n} total_in_db={c.execute('SELECT count(*) FROM todb_video').fetchone()[0]}")


def sync_detail(c, vid):
    sa = get(f"/video/{vid}/season/all")
    for s in sa:
        c.execute("""INSERT INTO todb_season(season_id,video_id,season_number,
            episode_count,date_air,synced_at) VALUES(?,?,?,?,?,?)
            ON CONFLICT(season_id) DO UPDATE SET episode_count=excluded.episode_count""",
                  (s["season_id"], vid, s["season_number"], s["episode_count"],
                   s.get("date_air"), int(time.time())))
        eps = get(f"/video/{vid}/season/{s['season_number']}/episode/all")
        for e in eps:
            c.execute("""INSERT OR REPLACE INTO todb_episode(episode_id,video_id,
                season_number,episode_number,date_air,title,parts_count)
                VALUES(?,?,?,?,?,?,?)""",
                      (e["episode_id"], vid, s["season_number"], e["episode_number"],
                       e.get("date_air"), e.get("episode_title"), e.get("parts_count")))
    c.commit()
    n = c.execute("SELECT count(*) FROM todb_episode WHERE video_id=?", (vid,)).fetchone()[0]
    print(f"video {vid}: {len(sa)} seasons, {n} episodes synced")


def status(c):
    for q, label in [
        ("SELECT count(*) FROM todb_video", "todb_video"),
        ("SELECT count(*) FROM todb_season", "todb_season"),
        ("SELECT count(*) FROM todb_episode", "todb_episode"),
        ("SELECT count(*) FROM todb_video WHERE video_type='movie'", "  movie"),
        ("SELECT count(*) FROM todb_video WHERE video_type='tv'", "  tv"),
    ]:
        print(f"{label}: {c.execute(q).fetchone()[0]}")
    cur = c.execute("SELECT v FROM todb_meta WHERE k='list_page'").fetchone()
    print(f"list_page cursor: {cur[0] if cur else 0}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=0)
    ap.add_argument("--detail", type=int)
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()
    c = db()
    if a.status:
        status(c)
    elif a.detail:
        sync_detail(c, a.detail)
    else:
        sync_pages(c, a.pages)
