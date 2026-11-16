#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MOVIEWATCH — 主动按片补全系统（影片库 + 状态对账 + 自动上片流水线）。

数据源：TMDB discover/movie + discover/tv（按 popularity.desc 拉全量清单）。
状态库：复用已建的 tg.db（SQLite，P0-1），moviewatch 表。
同步： 每小时拉 TMDB 人气清单 -> upsert -> 对账现有库标"已上"。
流水线：对 status=unseen 的片，搜源 -> tgpipe 引用转存(零流量) -> 回写已上+画质。

用法：
  python3 moviewatch.py sync --limit 300      # 拉 TMDB 清单 + 对账现有库（地基）
  python3 moviewatch.py status                # 查看 moviewatch 状态统计
  python3 moviewatch.py ingest --n 5          # 对未上的片自动走流水线（后续版）
  python3 moviewatch.py new --limit 50        # 列出人气最高的未上片

设计见 docs/MOVIEWATCH.md
"""
import argparse, asyncio, json, os, re, sqlite3, sys, time, urllib.parse, urllib.request, concurrent.futures

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import embytg_conf  # noqa: E402  (提供 cfg())

TMDB_API = embytg_conf.cfg("tmdb_api") or "https://api.themoviedb.org/3"
TMDB_TK = embytg_conf.cfg("tmdb_token")
DB = embytg_conf.cfg("sqlite_db") or "/opt/embytg/pipeline/tg.db"

PER_PAGE = 20          # TMDB 每页返回条数
MAX_WORKERS = 8        # 中文名详情并发


def norm_title(s):
    s = (s or "").strip()
    s = re.sub(r"[（(].*?[)）]", "", s)          # 去括号
    s = re.sub(r"[^\w\u4e00-\u9fff]+", "", s)     # 去符号
    return s.lower()


def http_json(url, headers=None, timeout=20):
    req = urllib.request.Request(url, headers=headers or {"accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        return {"__err__": str(e)[:160]}


def tmdb_list(kind, page, tk):
    ep = "movie" if kind == "movie" else "tv"
    q = urllib.parse.urlencode({"sort_by": "popularity.desc", "page": page, "language": "en-US"})
    return http_json(f"{TMDB_API}/discover/{ep}?{q}", {"Authorization": f"Bearer {tk}"})


def tmdb_detail(kind, mid, tk):
    ep = "movie" if kind == "movie" else "tv"
    return http_json(f"{TMDB_API}/{ep}/{mid}?language=zh-CN", {"Authorization": f"Bearer {tk}"})


# ---------------- 状态库（tg.db moviewatch 表） ----------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS moviewatch (
  id        INTEGER PRIMARY KEY,
  kind      TEXT NOT NULL,
  name_zh   TEXT,
  name_orig TEXT,
  year      INTEGER,
  popularity REAL,
  genres    TEXT,
  status    TEXT NOT NULL DEFAULT 'unseen',
  quality   TEXT,
  src       TEXT,
  checked_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mw_status ON moviewatch(status);
CREATE INDEX IF NOT EXISTS idx_mw_pop   ON moviewatch(popularity DESC);
CREATE TABLE IF NOT EXISTS tg_entity_cache (
  username   TEXT PRIMARY KEY,
  peer_type  TEXT,
  peer_id    INTEGER,
  access_hash INTEGER,
  resolved_at INTEGER
);
"""


def db_conn():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init_db(c):
    c.executescript(SCHEMA)
    c.commit()


def upsert_row(c, id, kind, name_zh, name_orig, year, pop, genres):
    c.execute("""INSERT INTO moviewatch (id,kind,name_zh,name_orig,year,popularity,genres,checked_at)
                 VALUES (?,?,?,?,?,?,?,?)
                 ON CONFLICT(id) DO UPDATE SET
                   kind=excluded.kind, name_zh=excluded.name_zh, name_orig=excluded.name_orig,
                   year=excluded.year, popularity=excluded.popularity, genres=excluded.genres,
                   checked_at=excluded.checked_at""",
              (id, kind, name_zh, name_orig, year, pop, json.dumps(genres, ensure_ascii=False), int(time.time())))


# ---------------- 对账现有库（复用 tgpipe sql 逻辑：docker exec teldrive-postgres） ----------------
PG = ["docker", "exec", "-i", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive",
      "-v", "ON_ERROR_STOP=1", "-At"]


def sql(q):
    import subprocess
    p = subprocess.run(PG, input=q, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"psql: {p.stderr.strip()}")
    return p.stdout.strip()


def existing_tmdb_ids(c):
    """扫 teldrive.files 已入库文件名，提取 {tmdb=ID} 集合。返回 set[str]"""
    ids = set()
    try:
        out = sql("select name from teldrive.files where kind='file' and status='active'")
        for n in out.splitlines():
            if n.lower().endswith(".nfo"):
                continue
            mm = re.search(r"tmdb(?:id)?[-=](\d+)", n, re.I)
            if mm:
                ids.add(mm.group(1))
    except Exception as e:
        print("# existing_tmdb_ids: psql 失败 " + str(e)[:80], file=sys.stderr)
    return ids


# ---------------- sync：拉 TMDB 清单 + 对账 ----------------
def sync(limit, tk):
    c = db_conn()
    init_db(c)
    pages = max(1, (limit + PER_PAGE - 1) // PER_PAGE)
    seen = 0
    t0 = time.time()
    for kind in ("movie", "tv"):
        for p in range(1, pages + 1):
            d = tmdb_list(kind, p, tk)
            if "__err__" in d:
                print(f"# {kind} page{p} ERR {d['__err__'][:80]}", file=sys.stderr)
                continue
            rows = d.get("results") or []
            if not rows:
                break
            # 并发拉中文名详情
            def fill(r):
                det = tmdb_detail(kind, r.get("id"), tk)
                if "__err__" in det:
                    return r, None, None, None, None
                return (r, det.get("title") or det.get("name"),
                        det.get("original_title") or det.get("original_name"),
                        det.get("genres") or [])
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                for (r, zh, orig, genres) in ex.map(fill, rows):
                    date = r.get("release_date") or r.get("first_air_date") or ""
                    y = int(date[:4]) if date[:4].isdigit() else None
                    gnames = [g.get("name") for g in (genres or []) if isinstance(g, dict)]
                    upsert_row(c, r["id"], kind, zh, orig or r.get("title") or r.get("name"),
                               y, r.get("popularity"), gnames)
                    seen += 1
            c.commit()
    # 对账现有库
    have = existing_tmdb_ids(c)
    cur = time.time()
    n_up = 0
    for row in c.execute("SELECT id,status FROM moviewatch WHERE status IN ('unseen','无资源')"):
        if str(row["id"]) in have:
            c.execute("UPDATE moviewatch SET status='已上', checked_at=? WHERE id=?", (cur, row["id"]))
            n_up += 1
    c.commit()
    tot = c.execute("SELECT count(*) FROM moviewatch").fetchone()[0]
    st = {}
    for r in c.execute("SELECT status,count(*) AS n FROM moviewatch GROUP BY status"):
        st[r["status"]] = r["n"]
    c.close()
    print(f"# sync 完成: 写入/刷新 {seen} 条, 对账标记已上 {n_up}, 库总 {tot}, "
          f"状态 {json.dumps(st,ensure_ascii=False)}, 耗时 {time.time()-t0:.1f}s")
    return {"written": seen, "reconciled": n_up, "total": tot, "status": st}


# ---------------- status ----------------
def status():
    c = db_conn()
    init_db(c)
    st = {}
    for r in c.execute("SELECT status,count(*) n FROM moviewatch GROUP BY status ORDER BY n DESC"):
        st[r["status"]] = r["n"]
    tot = sum(st.values())
    c.close()
    print(f"moviewatch 共 {tot} 条:")
    for k, v in st.items():
        print(f"  {k:10s} {v}")
    return st


# ---------------- new：列人气最高的未上片 ----------------
def new(limit):
    c = db_conn()
    init_db(c)
    rows = c.execute("""SELECT id,kind,name_zh,name_orig,year,popularity
                        FROM moviewatch WHERE status='unseen'
                        ORDER BY popularity DESC LIMIT ?""", (limit,)).fetchall()
    c.close()
    for i, r in enumerate(rows, 1):
        y = f"({r['year']})" if r["year"] else ""
        print(f"{i:3d}. [{r['kind']}] {r['name_zh'] or r['name_orig']} {y} pop={r['popularity']:.0f} id={r['id']}")
    return [dict(r) for r in rows]


# ---------------- ingest：极搜跨频道搜源 -> 生成待上片 pending items ----------------
# 复用 tgpipe 的转存链路：本命令只做"找源"，把命中的 (ch, msgid) 写成 tg_pending.json items，
# 再交给 `python3 tgpipe.py apply --from-queue --loop` 转存（复用去重/禁转/双号/NFO 全链路）。

def parse_tme_link(url):
    """从 MessageEntityTextUrl.url 提取 (channel, msgid)。支持 t.me/<chan>/<msgid>。"""
    m = re.search(r"t\.me/([A-Za-z0-9_]{4,})/(\d+)", url or "")
    if not m:
        return None
    return m.group(1), int(m.group(2))


def looks_ad(doc, size, dur, fn=""):
    """复用 tgpipe 无广告判据：size<120MB 或 dur<480s 视为广告/片花。
    注意 telethon 返回的 size/dur 可能是 str，先安全转数字。"""
    def num(x):
        try:
            f = float(x)
            return f if f > 0 else 0
        except (TypeError, ValueError):
            return 0
    size, dur = num(size), num(dur)
    if size and size < 120 * 1024 * 1024:
        return True
    if dur and dur < 480:
        return True
    AD_RE = r"预告|片花|花絮|彩蛋|宣传片|广告|敬请期待|特辑|幕后|making|teaser|trailer"
    if re.search(AD_RE, fn or "", re.I):
        return True
    return False


def quality_of(doc):
    """从 document attributes 提取画质：2160p/1080p/720p/SD/未知。"""
    from telethon.tl.types import DocumentAttributeVideo
    for a in (getattr(doc, "attributes", None) or []):
        if isinstance(a, DocumentAttributeVideo):
            try:
                h = int(a.h or 0)
            except (TypeError, ValueError):
                h = 0
            if h >= 2160: return "2160p"
            if h >= 1080: return "1080p"
            if h >= 720:  return "720p"
            if h > 0:     return "SD"
    return "未知"


# ---------------- TG 实体缓存（根治 ResolveUsername flood wait） ----------------
# 极搜返回的第三方频道每个都是新 username，get_entity 每次解析一次 ResolveUsername，
# 批量解析触发账号级 flood wait（393s/160s）。把 (username -> peer_id/access_hash)
# 持久化到 tg.db，只有"全新的 username"才真正 Resolve，重复频道零 RPC。

async def resolve_peer(c, username, c0):
    """返回 telethon InputPeerChannel，优先用 tg.db 实体缓存，miss 才 ResolveUsername。
    c: TelegramClient; c0: 独立的 sqlite 连接。"""
    from telethon.tl.types import InputPeerChannel
    row = c0.execute("SELECT peer_id,access_hash FROM tg_entity_cache WHERE username=?", (username,)).fetchone()
    if row and row["access_hash"]:
        return InputPeerChannel(row["peer_id"], row["access_hash"])
    try:
        ent = await c.get_entity(username)
    except Exception:
        return None
    c0.execute("""INSERT INTO tg_entity_cache (username, peer_type, peer_id, access_hash, resolved_at)
                  VALUES (?,?,?,?,?) ON CONFLICT(username) DO UPDATE SET
                    peer_type=excluded.peer_type, peer_id=excluded.peer_id,
                    access_hash=excluded.access_hash, resolved_at=excluded.resolved_at""",
               (username, ent.__class__.__name__, getattr(ent, "id", 0),
                getattr(ent, "access_hash", 0), int(time.time())))
    c0.commit()
    return InputPeerChannel(ent.id, ent.access_hash) if getattr(ent, "access_hash", None) else ent


def ingest(n, tk, bots):
    """对未上的高人气片，用多个 TG 搜索 bot 搜源，命中的写成 tg_pending items。
    bots 可逗号分隔多个，如 --bots jisou,soso,smss,kuai 提高命中率。"""
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.types import MessageMediaDocument, MessageEntityTextUrl

    PENDING = embytg_conf.cfg("pending_file") or "/opt/embytg/pipeline/tg_pending.json"
    SESS = embytg_conf.cfg("tg_session")
    API_ID = embytg_conf.cfg("tg_api_id")
    API_HASH = embytg_conf.cfg("tg_api_hash")

    c = db_conn()
    init_db(c)
    rows = c.execute("""SELECT id,kind,name_zh,name_orig,year FROM moviewatch
                        WHERE status='unseen' ORDER BY popularity DESC LIMIT ?""",
                     (n,)).fetchall()
    c.close()
    if not rows:
        print("# 没有 unseen 的片"); return

    # 读现有 pending 队列（不覆盖）
    q = {}
    try:
        q = json.load(open(PENDING))
    except Exception:
        pass
    q.setdefault("items", [])
    q.setdefault("cursor", 0)
    known = {(it.get("ch"), it.get("msg")) for it in q["items"]}

    tc = TelegramClient(StringSession(open(SESS).read().strip()), API_ID, API_HASH)
    # 实体缓存连接（独立，避免与 asyncio 竞争；init_db 会建 tg_entity_cache 表）
    cache_c = db_conn()
    init_db(cache_c)
    async def run():
        await tc.connect()
        botlist = [b.strip() for b in bots.split(",") if b.strip()]
        added = 0
        for r in rows:
            rid, kind, zh, orig, year = r["id"], r["kind"], r["name_zh"], r["name_orig"], r["year"]
            # 搜索用英文原名优先（欧美剧 caption 多为英文原名，中文名常搜不到/匹配不上）
            query = orig or zh or ""
            match_zh = norm_title(zh or "")   # 片名校验：中文名或英文原名任一命中即可
            match_orig = norm_title(orig or "")
            if not query:
                continue
            print(f"# 搜: [{kind}] {query} (id={rid})")
            links = set()
            for bot in botlist:
                try:
                    ent = await tc.get_entity(bot)
                    await tc.send_message(ent, query)
                    await asyncio.sleep(5)
                except Exception as e:
                    print(f"    {bot} ERR {str(e)[:60]}"); continue
                async for m in tc.iter_messages(ent, limit=25):
                    if m.out:
                        continue
                    for e in (m.entities or []):
                        if isinstance(e, MessageEntityTextUrl):
                            hit = parse_tme_link(e.url)
                            if hit:
                                links.add(hit)
            print(f"    累计命中 {len(links)} 个引用")
            # 按频道去重：每 uname 只 resolve_peer 解析一次（实体缓存），组内复用。
            by_chan = {}
            for uname, mid in links:
                if (uname, mid) in known:
                    continue
                by_chan.setdefault(uname, []).append(mid)
            # 全量收集命中候选：{chan,msg,qual,size,dur,fn}，过滤广告 + 片名校验
            cand = []
            for uname, mids in list(by_chan.items())[:8]:
                sent_ent = await resolve_peer(tc, uname, cache_c)
                if sent_ent is None:
                    await asyncio.sleep(3)
                    continue
                for mid in mids[:3]:
                    try:
                        msgs = await tc.get_messages(sent_ent, ids=[mid])
                        m = msgs[0] if msgs else None
                        if not m or not isinstance(m.media, MessageMediaDocument):
                            continue
                        doc = m.media.document
                        fn = ""
                        for a in doc.attributes:
                            if getattr(a, "file_name", None):
                                fn = a.file_name
                        size = doc.size or 0
                        dur = getattr(doc, "duration", None)
                        if looks_ad(size, dur, fn):
                            continue
                        # 片名校验：caption 或文件名须含查询片名（中文名 或 英文原名 任一命中），防无关文件混入
                        cap = norm_title((m.message or "") + " " + (fn or ""))
                        if match_zh and match_zh in cap:
                            pass
                        elif match_orig and match_orig in cap:
                            pass
                        else:
                            continue
                        qual = quality_of(doc)
                        cand.append({"ch": uname, "msg": mid, "qual": qual,
                                     "size": size, "dur": dur, "fn": fn[:40]})
                    except Exception as e:
                        await asyncio.sleep(2)
                await asyncio.sleep(1.5)
            # 择优：quality 优先级（2160p>1080p>720p>SD）> 文件大，每片只入队质量最高的
            if cand:
                qrank = {"2160p": 0, "1080p": 1, "720p": 2, "SD": 3, "未知": 4}
                best = sorted(cand, key=lambda c: (qrank.get(c["qual"], 9), -c["size"]))[0]
                it = {"ch": best["ch"], "msg": best["msg"], "title": query,
                      "raw_title": f"{query} [{best['qual']}]",
                      "key": f"{best['ch']}:{best['msg']}", "src": "jisou",
                      "qual": best["qual"], "done": None}
                q["items"].append(it)
                known.add((best["ch"], best["msg"]))
                got = 1
                print(f"    -> 入队 {best['ch']}/{best['msg']} {best['qual']} {best['size']/1e6:.0f}MB"
                      f"（从 {len(cand)} 个候选中择优）")
            else:
                got = 0
            if got:
                c = db_conn()
                c.execute("UPDATE moviewatch SET status='已入队', checked_at=? WHERE id=?", (int(time.time()), rid))
                c.commit(); c.close()
            else:
                c = db_conn()
                c.execute("UPDATE moviewatch SET status='无资源', checked_at=? WHERE id=?", (int(time.time()), rid))
                c.commit(); c.close()
            added += 1
        await tc.disconnect()
    asyncio.run(run())
    cache_c.close()
    json.dump(q, open(PENDING, "w"), ensure_ascii=False)
    print(f"# ingest 完成: 处理 {len(rows)} 片, 队列现有 {len(q['items'])} 条 pending。"
          f"下一步: python3 tgpipe.py apply --from-queue --loop --max-items 20")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    s = sub.add_parser("sync")
    s.add_argument("--limit", type=int, default=300)
    t = sub.add_parser("status")
    n = sub.add_parser("new")
    n.add_argument("--limit", type=int, default=50)
    i = sub.add_parser("ingest")
    i.add_argument("--n", type=int, default=5)
    i.add_argument("--bots", default="jisou,soso,smss,kuai", help="逗号分隔多个 TG 搜索 bot，提高命中率")
    args = ap.parse_args()
    if args.cmd == "sync":
        sync(args.limit, TMDB_TK)
    elif args.cmd == "status":
        status()
    elif args.cmd == "new":
        new(args.limit)
    elif args.cmd == "ingest":
        ingest(args.n, TMDB_TK, args.bots)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()