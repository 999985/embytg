# -*- coding: utf-8 -*-
"""resource_state.py —— 资源全生命周期台账（用户 m01311：数据库里加「是否被刮削」）

一张表看清每个资源走到哪一步，四道关卡各有时间戳：

  landed   已转存到 teldrive（files 表有记录，字节在 TG）
  probed   ffprobe 探测过（Emby 有 MediaStreams → 能出分辨率/编码/字幕数）
  scraped  NFO + poster 已生成（teldrive 里有同名 .nfo，Emby 能读元数据）
  emby     Emby 索引已收录（/Items 能查到，客户端可见可播）

stage 取四者最靠后的一档。数据全部来自三源：teldrive.files（落盘+NFO）、
Emby /Items（收录+MediaStreams）、目录树（parent_id 还原路径）。不走 FUSE find，
万级条目秒级完成，规模无关。

用法（服务器 cd /opt/embytg/pipeline）：
  python3 resource_state.py --scan              # 全量对账三源合一
  python3 resource_state.py --status            # 各阶段数量 + 缺口
  python3 resource_state.py --list --stage scraped --limit 20
  python3 resource_state.py --missing           # 卡住的资源
  python3 resource_state.py --stage landed      # 只看某一档
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import time
import urllib.request
from datetime import datetime, timezone

DB = "/opt/embytg/pipeline/tg.db"
CFG = "/opt/embytg/config.json"
MNT = "/opt/embytg/mnt/embytg"
_conf = json.load(open(CFG))
PG = _conf.get("pg_query", "docker exec -i teldrive-postgres psql -U teldrive -d teldrive -At")
EMBY = "http://127.0.0.1:8096"
TOKEN = _conf.get("emby_token", "REPLACE_ME")
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".ts", ".m4v", ".mov", ".wmv", ".flv", ".webm")
NFO_EXT = (".nfo",)
ART_EXT = (".jpg", ".jpeg", ".png", ".webp")


def db():
    c = sqlite3.connect(DB, timeout=60)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def api(path, timeout=180):
    r = urllib.request.Request(EMBY + path, headers={"X-Emby-Token": TOKEN})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as f:
            return json.loads(f.read() or b"null")
    except Exception as e:
        print("  ! api %s -> %s" % (path[:60], str(e)[:120]))
        return None


def sh(cmd, timeout=300):
    return (subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True,
                           timeout=timeout).stdout or "")


SCHEMA = """
CREATE TABLE IF NOT EXISTS resource_state(
  path        TEXT PRIMARY KEY,
  rel         TEXT,
  lib         TEXT,
  kind        TEXT,
  title       TEXT,
  tmdb        INTEGER,
  size        INTEGER,
  landed_at   INTEGER,
  probed_at   INTEGER,
  scraped_at  INTEGER,
  poster_at   INTEGER,
  emby_at     INTEGER,
  emby_item   TEXT,
  width       INTEGER, height INTEGER, dur_sec INTEGER,
  vcodec      TEXT, acodec TEXT, subs INTEGER,
  stage       TEXT,
  note        TEXT
);
CREATE INDEX IF NOT EXISTS idx_rs_stage ON resource_state(stage);
CREATE INDEX IF NOT EXISTS idx_rs_lib   ON resource_state(lib);
CREATE INDEX IF NOT EXISTS idx_rs_tmdb  ON resource_state(tmdb);
CREATE INDEX IF NOT EXISTS idx_rs_scraped ON resource_state(scraped_at);
-- 2026-11-13 可扩展性：gha_finish 每完成一集要按 rel 精确查 3 次
-- （取画质尺寸、取 stage、取旧 rel 的 stage）。没有索引就是全表扫描，
-- EXPLAIN 实测 SCAN resource_state，20 万行时会成为上片收尾的瓶颈。
-- 加了之后 EXPLAIN 变成 SEARCH ... USING INDEX idx_rs_rel。
CREATE INDEX IF NOT EXISTS idx_rs_rel ON resource_state(rel);
"""


def init(c):
    for stmt in SCHEMA.split(";"):
        if stmt.strip():
            c.execute(stmt)
    c.commit()


def td_tree():
    """一次拿全 teldrive 文件树：id -> {name,parent,kind,size,epoch}。
    用 row_to_json 输出，避开分隔符转义问题（文件名里可能有任意字符）。"""
    q = ("select row_to_json(t) from (select id, name, parent_id, kind, size, "
         "coalesce(extract(epoch from created_at)::bigint,0) as ts "
         "from files where deleted_at is null) t")
    out = sh('%s -c "%s"' % (PG, q), timeout=300)
    nodes = {}
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            o = json.loads(line)
        except Exception:
            continue
        nodes[o["id"]] = {"name": o.get("name") or "", "parent": o.get("parent_id") or "",
                          "kind": o.get("kind") or "", "size": int(o.get("size") or 0),
                          "ts": int(o.get("ts") or 0)}
    return nodes


def build_paths(nodes):
    """parent_id 链还原每个节点的相对路径（相对 teldrive 根）。"""
    cache = {}
    stack_guard = {}

    def path_of(nid):
        if nid in cache:
            return cache[nid]
        if nid not in nodes:
            return ""
        stack_guard[nid] = stack_guard.get(nid, 0) + 1
        if stack_guard[nid] > 3:      # 防环
            return ""
        n = nodes[nid]
        par = n["parent"]
        if not par or par in ("", "NULL", "00000000-0000-0000-0000-000000000000"):
            r = n["name"]
        else:
            base = path_of(par)
            r = (base.rstrip("/") + "/" + n["name"]).lstrip("/") if base else n["name"]
        cache[nid] = r
        return r

    for nid in nodes:
        path_of(nid)
    return cache


def emby_index():
    """Emby 全量 Movie/Episode：宿主路径 → ItemId / MediaStreams / DateCreated。
    Emby 返回的是容器内路径 /mnt/embytg/...，必须转成宿主 /opt/embytg/mnt/embytg/..."""
    out, start = {}, 0
    while True:
        q = ("/Items?Recursive=true&IncludeItemTypes=Movie,Episode"
             "&Fields=Path,MediaStreams,DateCreated,RunTimeTicks"
             "&EnableImages=false&EnableTotalRecordCount=false"
             "&Limit=2000&StartIndex=%d" % start)
        d = api(q)
        if not isinstance(d, dict):
            break
        items = d.get("Items") or []
        if not items:
            break
        for it in items:
            p = it.get("Path") or ""
            if not p:
                continue
            hp = p.replace("/mnt/embytg/", MNT + "/") if p.startswith("/mnt/embytg/") else p
            streams = it.get("MediaStreams") or []
            out[hp] = {"item": it.get("Id"), "created": it.get("DateCreated"),
                       "rt": int(it.get("RunTimeTicks") or 0),
                       "streams": streams}
        start += len(items)
        if len(items) < 2000:
            break
    return out


def parse_ts(s):
    """Emby 的 DateCreated 是 UTC（尾部 Z），必须按 UTC 转 epoch。

    旧实现用 time.mktime()（按**本地**时区解释），在 CST+0800 主机上把
    00:31:57 CST 记成了前一天 16:31:57，负偏差正好 8h。后果不是「差几秒」：
    resource_state 里 17444/17444 条 emby_at 全部早于 landed_at，
    「最近三单 claim→…→Emby 收录」的时间链永远不可能成立，
    任何按 emby_at 排序/取近 N 分钟/判落库延迟的逻辑都静默失真。
    """
    if not s:
        return 0
    txt = s.strip()
    try:
        # 2026-11-13T16:31:57.0000000Z / 2026-11-13T16:31:57+00:00 / 无时区的本地时间
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if dt.tzinfo is None:                      # 无时区 → 按本机时区
            return int(dt.timestamp())
        return int(dt.astimezone(timezone.utc).timestamp())
    except Exception:
        pass
    try:
        return int(time.mktime(time.strptime(txt[:19], "%Y-%m-%dT%H:%M:%S")))
    except Exception:
        return 0


def pick_streams(streams):
    w = h = subs = 0
    vcodec = acodec = ""
    for s in streams or []:
        t = (s.get("Type") or "").lower()
        if t == "video" and not vcodec:
            w = int(s.get("Width") or 0)
            h = int(s.get("Height") or 0)
            vcodec = (s.get("Codec") or "").upper()
        elif t == "audio" and not acodec:
            acodec = (s.get("Codec") or "").upper()
        elif t == "subtitle":
            subs += 1
    return w, h, vcodec, acodec, subs



def _stage_reset(c):
    """建立/清空影子表 resource_state_new（结构与索引与真表完全一致）。

    2026-11-13 血案：原来是「DELETE FROM resource_state; commit」再逐条重建，
    而整次重建要跑几分钟。**这几分钟里所有并发读者看到的是空台账**——
    实测 daily_ops 报「tgb=0 / 差异 13199」、post_ingest 可能据此判定
    「没有受影响的剧」而空跑。五路并发下这不是偶发，是必然。

    正确做法：全部写进影子表（没人读它），最后**一次事务内原子换名**。
    读者要么看到旧的完整快照，要么看到新的完整快照，永远看不到空表/半表。
    """
    ddl = c.execute("SELECT sql FROM sqlite_master WHERE type='table' "
                    "AND name='resource_state'").fetchone()[0]
    # 2026-11-13 修：真源 DDL 用的是 `CREATE TABLE IF NOT EXISTS resource_state`，
    # 只替换 "CREATE TABLE resource_state" 会匹配失败 → ddl 原样仍是真表名 →
    # `table "resource_state" already exists`，**影子表重建永远跑不起来**
    # （= 台账再也不会全量刷新，rename/删除只能留下幽灵记录）。
    # 现在按正则改写成影子表名，并确保不带 IF NOT EXISTS 以外的任何变化。
    # SQLite 从 sqlite_master 取回的 DDL 里表名可能带双引号：
    #   CREATE TABLE "resource_state"(   ← 实际返回的就是这个
    # 所以既不匹配 "CREATE TABLE resource_state"，也不匹配带 IF NOT EXISTS 的写法。
    ddl = re.sub(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["\'`]?resource_state["\'`]?',
                 "CREATE TABLE resource_state_new", ddl, count=1)
    if "resource_state_new" not in ddl:
        raise RuntimeError("_stage_reset: 无法从真表 DDL 派生影子表，DDL=%r" % ddl[:200])
    c.execute("DROP TABLE IF EXISTS resource_state_new")
    c.execute(ddl)
    for (isql,) in c.execute("SELECT sql FROM sqlite_master WHERE type='index' "
                             "AND tbl_name='resource_state' AND sql IS NOT NULL").fetchall():
        # SQLite 的索引名是**全局**的，不是 per-table：沿用 idx_rs_* 会撞名报
        # "index idx_rs_stage already exists"。影子表一律用 _new 后缀（插入时就建好
        # 索引，换表时不用再等），换表后 _stage_swap 再把索引名改回规范名。
        isql = isql.replace(" ON resource_state", " ON resource_state_new")
        for nm in ("idx_rs_stage", "idx_rs_lib", "idx_rs_tmdb",
                   "idx_rs_scraped", "idx_rs_rel"):
            isql = isql.replace(" " + nm + " ", " " + nm + "_new ")
        c.execute(isql)
    c.commit()


def _stage_swap(c):
    """一次事务内把影子表换成真表。毫秒级，且原子。"""
    # Python 的 sqlite3 在 legacy isolation 下会隐式持有事务，
    # 不先收尾就 BEGIN IMMEDIATE 会报 "cannot start a transaction within a transaction"。
    c.commit()
    c.execute("BEGIN IMMEDIATE")
    try:
        # 依赖 resource_state 的**视图**挡住 ALTER TABLE RENAME：
        # SQLite 会把视图里 FROM 的表名一起改写成 resource_state_old，
        # 于是 RENAME 时校验看见「no such table: resource_state_old」直接失败，
        # 更糟的是失败后视图已被写坏（reconcile.py 建的 v_episode_health 就是这样
        # 从 resource_state 变成 resource_state_old 的）。所以换表前先备份视图 SQL、
        # 删视图、换表、再按原 SQL 重建 —— 全程仍在同一事务内。
        views = {n: s for n, s in c.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='view' AND sql IS NOT NULL"
            " AND sql LIKE '%resource_state%'").fetchall()}
        for n in views:
            c.execute("DROP VIEW IF EXISTS %s" % n)
        c.execute("DROP TABLE IF EXISTS resource_state_old")
        c.execute("ALTER TABLE resource_state RENAME TO resource_state_old")
        c.execute("ALTER TABLE resource_state_new RENAME TO resource_state")
        c.execute("DROP TABLE resource_state_old")
        # 索引名改回规范名（旧的 idx_rs_* 已随 old 表一起消失）
        for nm, col in (("idx_rs_stage", "stage"), ("idx_rs_lib", "lib"),
                        ("idx_rs_tmdb", "tmdb"), ("idx_rs_scraped", "scraped_at"),
                        ("idx_rs_rel", "rel")):
            c.execute("DROP INDEX IF EXISTS %s_new" % nm)
            c.execute("CREATE INDEX IF NOT EXISTS %s ON resource_state(%s)" % (nm, col))
        # 视图按换表前抓到的原 SQL 重建（原 SQL 里写的是 resource_state）
        for nm, vsql in views.items():
            c.execute("DROP VIEW IF EXISTS %s" % nm)
            c.execute(vsql)
        c.commit()
    except Exception:
        c.rollback()
        raise

def scan(c):
    print("=== 1/3 teldrive 文件树 ===")
    t0 = time.time()
    # 全量重建：写进影子表，最后原子换名（见 _stage_reset/_stage_swap 的注释）。
    # 旧实现是 DELETE + 立刻 commit，导致重建期间读者看到空台账。
    # 「必须全量重建」这个结论本身没错（改名/删除后的幽灵记录只能靠全量清掉），
    # 错的只是**换表的时机**。
    _stage_reset(c)
    nodes = td_tree()
    paths = build_paths(nodes)
    print("  节点 %d 个 %.1fs" % (len(nodes), time.time() - t0))

    print("=== 2/3 按目录聚合 NFO / poster ===")
    dir_nfo, dir_art = {}, {}
    for nid, n in nodes.items():
        if n["kind"] != "file":
            continue
        p = paths.get(nid) or ""
        if not p:
            continue
        d, base = os.path.split(p)
        low = base.lower()
        if low.endswith(NFO_EXT):
            dir_nfo.setdefault(d, []).append((base, n["ts"]))
        elif low.endswith(ART_EXT):
            dir_art.setdefault(d, []).append((base, n["ts"]))
    print("  有 NFO 目录 %d / 有图目录 %d" % (len(dir_nfo), len(dir_art)))

    print("=== 3/3 Emby 索引（MediaStreams）===")
    t0 = time.time()
    em = emby_index()
    print("  Emby 收录 %d 条 %.1fs" % (len(em), time.time() - t0))

    print("=== 落库 ===")
    t0 = time.time()
    now = int(time.time())
    cnt = 0
    for nid, node in nodes.items():
        if node["kind"] != "file":
            continue
        rel = paths.get(nid) or ""
        if not rel.lower().endswith(VIDEO_EXT):
            continue
        full = os.path.join(MNT, rel)
        parts = rel.split("/")
        lib = "/".join(parts[:2]) if len(parts) >= 2 else ""
        d = os.path.dirname(rel)
        nfos = dir_nfo.get(d) or []
        arts = dir_art.get(d) or []
        kind = "episode" if "/TV/" in full else "movie"
        if kind == "episode":
            # 剧集 NFO 在 Series 根目录（Season XX 的父），不在 Season 目录里
            parent = os.path.dirname(d)
            nfos = (dir_nfo.get(parent) or []) + nfos
            arts = (dir_art.get(parent) or []) + arts
        scraped = max([t for _, t in nfos], default=0)
        poster = max([t for _, t in arts], default=0)
        e = em.get(full)
        w = h = subs = 0
        dur = 0
        vcodec = acodec = ""
        probed = emby_at = 0
        item = ""
        if e:
            emby_at = parse_ts(e.get("created")) or now
            item = e.get("item") or ""
            # dur_sec 以前从没写过（全表 NULL），导致 daily_ops/gap_report 只能绕道
            # Emby 现查。RunTimeTicks 是 100ns 单位；<1s 或 0 视为未知。
            _rt = int(e.get("rt") or 0)
            if _rt > 10000000:
                dur = int(_rt / 10000000)
            if e.get("streams"):
                w, h, vcodec, acodec, subs = pick_streams(e["streams"])
                if w and h:
                    probed = emby_at
        m = re.search(r"\{tmdb=(\d+)\}", full)
        tmdb = int(m.group(1)) if m else None
        title = os.path.basename(d)
        if kind == "episode":
            title = os.path.basename(os.path.dirname(d)) or title
        if emby_at and scraped and probed:
            stage = "emby"
        elif scraped and probed:
            stage = "scraped"
        elif probed:
            stage = "probed"
        elif emby_at and scraped:
            stage = "emby_nofprobe"
        elif emby_at:
            stage = "emby_only"
        else:
            stage = "landed"
        c.execute("""INSERT INTO resource_state_new(
            path, rel, lib, kind, title, tmdb, size, landed_at, probed_at,
            scraped_at, poster_at, emby_at, emby_item, width, height, dur_sec,
            vcodec, acodec, subs, stage, note)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(path) DO UPDATE SET
              rel=excluded.rel, lib=excluded.lib, kind=excluded.kind,
              title=excluded.title, tmdb=excluded.tmdb, size=excluded.size,
              landed_at=excluded.landed_at, probed_at=excluded.probed_at,
              scraped_at=excluded.scraped_at, poster_at=excluded.poster_at,
              emby_at=excluded.emby_at, emby_item=excluded.emby_item,
              width=excluded.width, height=excluded.height,
              dur_sec=excluded.dur_sec,
              vcodec=excluded.vcodec, acodec=excluded.acodec,
              subs=excluded.subs, stage=excluded.stage""",
                  (full, rel, lib, kind, title, tmdb, node["size"], node["ts"],
                   probed, scraped, poster, emby_at, item, w, h, dur,
                   vcodec, acodec, subs, stage, ""))
        cnt += 1
        if cnt % 5000 == 0:
            c.commit()
            print("  %d 条 %.0fs" % (cnt, time.time() - t0), flush=True)
    _stage_swap(c)
    print("DONE 落库 %d 条并原子换表 %.1fs" % (cnt, time.time() - t0))


def status(c):
    tot = c.execute("SELECT count(*) FROM resource_state").fetchone()[0]
    print("=== resource_state 总量 %d ===" % tot)
    if not tot:
        return
    print("=== 按 stage ===")
    for r in c.execute("SELECT COALESCE(stage,'<null>'),count(*) FROM resource_state "
                       "GROUP BY stage ORDER BY 2 DESC"):
        print("  %-14s %6d  %5.1f%%" % (r[0], r[1], r[1] * 100.0 / tot))
    print("=== 按 kind ===")
    for r in c.execute("SELECT kind,count(*) FROM resource_state GROUP BY kind"):
        print("  %-10s %d" % (r[0], r[1]))
    print("=== 四道关卡覆盖 ===")
    for col, name in [("landed_at", "landed 落盘"), ("probed_at", "probed 已探测"),
                      ("scraped_at", "scraped 已刮削"), ("poster_at", "poster 有图"),
                      ("emby_at", "emby 已收录")]:
        n = c.execute("SELECT count(*) FROM resource_state WHERE %s>0" % col).fetchone()[0]
        print("  %-16s %6d  %5.1f%%" % (name, n, n * 100.0 / tot))
    print("=== 画质分布 ===")
    for r in c.execute("""SELECT CASE
            WHEN height>=2160 THEN '2160p' WHEN height>=1080 THEN '1080p'
            WHEN height>=720 THEN '720p' WHEN height>0 THEN 'SD' ELSE '未探测' END q,
            count(*) FROM resource_state GROUP BY q ORDER BY 2 DESC"""):
        print("  %-8s %6d" % (r[0], r[1]))
    print("=== 字幕覆盖（已探测）===")
    for r in c.execute("""SELECT CASE WHEN subs>0 THEN '有字幕' ELSE '无字幕' END s,
            count(*) FROM resource_state WHERE probed_at>0 GROUP BY s"""):
        print("  %-8s %6d" % (r[0], r[1]))
    print("=== 分库：有落盘但没 NFO（刮削缺口）===")
    rows = c.execute("""SELECT lib, count(*) FROM resource_state
        WHERE landed_at>0 AND scraped_at=0 GROUP BY lib ORDER BY 2 DESC LIMIT 12""").fetchall()
    if not rows:
        print("  无")
    for r in rows:
        print("  %-24s %d" % (r[0], r[1]))


def list_stage(c, stage, limit):
    rows = c.execute("""SELECT rel, stage, width, height, vcodec, acodec, subs, size,
        scraped_at, emby_at FROM resource_state WHERE stage=?
        ORDER BY landed_at DESC LIMIT ?""", (stage, limit)).fetchall()
    for r in rows:
        print("  [%s] %dx%d %s/%s 字幕%d %.2fGB nfo=%s emby=%s\n     %s"
              % (r["stage"], r["width"] or 0, r["height"] or 0, r["vcodec"] or "-",
                 r["acodec"] or "-", r["subs"] or 0, (r["size"] or 0) / 2**30,
                 "Y" if r["scraped_at"] else "-", "Y" if r["emby_at"] else "-",
                 r["rel"][-110:]))


def missing(c):
    now = int(time.time())
    print("=== 落盘超 2h 仍无 NFO（刮削漏网）===")
    rows = c.execute("""SELECT lib, count(*) FROM resource_state
        WHERE landed_at>0 AND scraped_at=0 AND landed_at < ?
        GROUP BY lib ORDER BY 2 DESC""", (now - 7200,)).fetchall()
    print("  无" if not rows else "")
    for r in rows:
        print("  %-24s %d" % (r[0], r[1]))
    print("=== 有 NFO 但 Emby 未收录（等索引）===")
    rows = c.execute("""SELECT lib, count(*) FROM resource_state
        WHERE scraped_at>0 AND emby_at=0 GROUP BY lib ORDER BY 2 DESC""").fetchall()
    print("  无" if not rows else "")
    for r in rows:
        print("  %-24s %d" % (r[0], r[1]))
    print("=== 已收录但无 MediaStreams（ffprobe 欠账）===")
    n = c.execute("SELECT count(*) FROM resource_state WHERE emby_at>0 AND probed_at=0").fetchone()[0]
    print("  %d 条" % n)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--stage", default="landed")
    ap.add_argument("--missing", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    # 2026-11-14：`--scan` 有**两个并发调用方**：
    #   ① embytg-rsscan.timer 每 30 分钟
    #   ② gha_finish.py:598  每收完一集就调一次（五路 lane 并行时几分钟一次）
    # 两者撞上时，A 刚 `CREATE TABLE resource_state_new`，B 的
    # `DROP TABLE IF EXISTS resource_state_new` 把它删了 → A 插入报
    # `no such table: resource_state_new`，整次重建失败、台账不刷新。
    # 实测 08:38:54 embytg-rsscan 就是这么挂的（exit-code 1）。
    # 单实例锁：拿不到锁就**等**（不报错），因为重建是幂等的，排队做完即可。
    if a.scan:
        import fcntl
        lock_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 ".resource_state.lock")
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
        except Exception:                                          # noqa: BLE001
            pass
        try:
            c = db()
            init(c)
            scan(c)
            status(c)
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
        raise SystemExit(0)

    c = db()
    init(c)
    if a.list:
        list_stage(c, a.stage, a.limit or 20)
    elif a.missing:
        missing(c)
    else:
        status(c)
