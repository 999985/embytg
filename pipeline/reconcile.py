#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""reconcile.py —— 五套账本的对账器（架构级，替代「出事再修」）

## 为什么需要它（这是今晚那串 bug 的共同根因）

这条链路上有 **5 套互相独立的账本**，靠「尽力而为的通知」串起来：

```
① Telegram 频道        字节真正存放的地方
② teldrive PG files    每个文件在哪几个 TG 分片（权威的"存在性"）
③ 挂载视图 (rclone)     文件看得见 / 读不读得出（可能假活）
④ tg.db resource_state 我们这一侧的处理状态（landed/scraped/emby + emby_item）
⑤ Emby library.db      用户实际能看到的条目
```

任何两本账之间出现裂缝，就是一类 bug：

| 裂缝 | 表现 | 今晚的实例 |
|---|---|---|
| ②有 ④无 | 挂在盘上、台账里没有 | 国家宝藏/我是歌手 2 部（用户在搜不到） |
| ②有 ⑤无 | 文件在、用户看不到 | 同上（Media/Updated 喂目录 204 但不建条目） |
| ②有 但读不出 | ffprobe 挂死 → 容器僵尸 | E028（rclone 无超时） |
| ④有 ⑤无 | 台账说入库了，Emby 里没有 | lib_reconcile 的 gaps 被当成「新片尾部」搪塞过去 |
| ④有 但 ③无 | 幽灵台账 | 改名/删除后没清 |

之前修的都是**单个裂缝**，所以修完一个还会长下一个。真正的修法只有两种：
**（a）让裂缝不可能出现（物理约束，如符号链接）；（b）每轮把 5 本账摊开做逐条对账。**
本脚本做（b）——它不猜、不解释、不"属正常尾部"，所有差异都出条目级清单。

## 与 lib_reconcile.py 的区别（重要）

`lib_reconcile.py` 只比**每库总条数**（td 文件数 vs Emby Episode 数）。总数相等也可能
错位（少 1 集多 1 集），而总数不等时它只会说「缺口 ≈ N 集」——正是这种"计数级"口径
让 2 个孤儿存在了很久却一直被解释成"新片/改名尾部"。本脚本是**条目级**：
每个 (库, 剧目录, 季, 集, 画质) 具体缺在哪一本账里，逐一列出。

## 用法

```
python3 reconcile.py                 # 只报（人类可读）
python3 reconcile.py --json          # 机器可读
python3 reconcile.py --repair        # 有界自动修（默认关）
python3 reconcile.py --repair --only series_missing_emby
python3 reconcile.py --lib 综艺      # 只看一个库
```

`--repair` 只做三件**可逆且有界**的事，绝不删数据：
1. `series_missing_emby` → 对该库发**一次** `POST /Items/{libItemId}/Refresh`
   （库级刷新是整库 ffprobe，很贵，所以每库每轮最多一次，且写 state 记住）
2. `episode_missing_emby` → 对该 Series 发 `POST /Items/{sid}/Refresh?Recursive=true`
3. 其余类别只报，不修（需要人判断或走专门的脚本）

结果同时写进 tg.db 的 `reconcile_gaps` 表（每轮重建），方便 SQL 直接查：
```
sqlite3 pipeline/tg.db "select kind,lib,count(*) from reconcile_gaps group by 1,2"
```
"""
import argparse
import fcntl
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict

import embytg_conf as C

HERE = os.path.dirname(os.path.abspath(__file__))
TGDB = os.path.join(HERE, "tg.db")
STATE = os.path.join(HERE, "logs", "reconcile_state.json")
E = C.cfg("emby_url", "http://127.0.0.1:8096").rstrip("/")
T = C.cfg("emby_api_key") or C.cfg("emby_token") or ""
HDR = {"X-Emby-Token": T, "Content-Type": "application/json"}
PREFIX = "/mnt/embytg"          # Emby 容器内看到的挂载根
SE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
VIDEO = (".mp4", ".mkv", ".m4v", ".avi", ".ts", ".mov")
DB = "teldrive-postgres"
DBU = "teldrive"
DBNAME = "teldrive"


# ---------------------------------------------------------------- 基础
def emby(path, method="GET", body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(E + path, method=method, headers=HDR, data=data)
    with urllib.request.urlopen(r, timeout=timeout) as f:
        raw = f.read()
    return json.loads(raw) if raw.strip() else None


def psql(sql, timeout=600):
    r = subprocess.run(["docker", "exec", DB, "psql", "-U", DBU, "-d", DBNAME,
                        "-tAc", sql], capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or "")[:200])
    return (r.stdout or "").strip()


def key_of(rel):
    """相对路径 → (lib, series_dir, season, ep, quality)。quality 取文件名末尾标记。"""
    if not rel:
        return None
    if rel.startswith("TV/"):
        rel = rel[3:]
    parts = rel.split("/")
    if len(parts) < 3:
        return None
    lib, sdir = parts[0], parts[1]
    name = parts[-1]
    if not name.lower().endswith(VIDEO):
        return None
    m = SE.search(name)
    if not m:
        return None
    q = ""
    qm = re.search(r"-\s*(\d{3,4}p)", name)
    if qm:
        q = qm.group(1)
    return (lib, sdir, int(m.group(1)), int(m.group(2)), q)


# ---------------------------------------------------------------- 三本账
def ledger_teldrive():
    """② teldrive PG：从根往下建路径，得到所有视频文件。

    2026-11-13 踩坑：第一版是「从文件往上拼」（join f.id = u.parent_id）。
    看着对，实测只拼出**裸文件名**——因为递归 CTE 里每行的起点是文件自己，
    "最深一层"并不等于"到根的那条链"，distinct on (id) ... lvl desc 选出来的
    仍是 lvl 0。改成**从根往下**（parent_id is null 的顶层文件夹 → 逐层拼子项），
    路径唯一且不会歧义。改完实测 17,427 个视频文件。
    """
    sql = ("with recursive down as ("
           " select id, name::text as path from files"
           " where parent_id is null and kind='folder' and deleted_at is null"
           " union all"
           " select f.id, d.path || '/' || f.name::text from files f"
           " join down d on f.parent_id = d.id where f.deleted_at is null)"
           " select path from down where path ~* '\\.(mp4|mkv|m4v|avi|ts|mov)$'")
    out = {}
    for rel in psql(sql).splitlines():
        rel = rel.strip()
        if not rel:
            continue
        k = key_of(rel)
        if k:
            out.setdefault(k, []).append(rel)
    return out


def ledger_tgdb():
    """④ tg.db resource_state：我们这一侧的处理状态。"""
    out = {}
    con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=60)
    for rel, stage, emby_at, emby_item, scraped_at, landed_at in con.execute(
            "select rel,stage,emby_at,emby_item,scraped_at,landed_at "
            "from resource_state where rel is not null"):
        k = key_of(rel)
        if not k:
            continue
        out[k] = {"rel": rel, "stage": stage or "", "emby_at": emby_at or 0,
                  "emby_item": emby_item or "",
                  "has_nfo": bool(scraped_at), "landed_at": landed_at or 0}
    con.close()
    return out


def ledger_emby():
    """⑤ Emby：Series（按 tmdb）+ Episode（按 Path）。"""
    series, eps = {}, {}
    d = emby("/Items?Recursive=true&IncludeItemTypes=Series&Limit=100000"
             "&Fields=ProviderIds,Path,Name") or {}
    for i in d.get("Items") or []:
        tm = (i.get("ProviderIds") or {}).get("Tmdb")
        series[i["Id"]] = {"tmdb": str(tm) if tm else "", "name": i.get("Name"),
                           "path": i.get("Path") or ""}
    start = 0
    while True:
        d = emby("/Items?Recursive=true&IncludeItemTypes=Episode&StartIndex=%d"
                 "&Limit=5000&Fields=Path,SeriesId,ParentIndexNumber,IndexNumber,Size"
                 % start) or {}
        its = d.get("Items") or []
        if not its:
            break
        for i in its:
            k = key_of((i.get("Path") or "").replace("/mnt/embytg/TV/", "TV/", 1))
            if k:
                eps[k] = {"id": i["Id"], "series_id": i.get("SeriesId")}
        start += len(its)
        if start >= (d.get("TotalRecordCount") or 0):
            break
    return series, eps


def series_tmdb_index(series):
    idx = {}
    for sid, s in series.items():
        if s["tmdb"]:
            idx.setdefault(s["tmdb"], sid)
    return idx


# ---------------------------------------------------------------- 对账
def reconcile(lib_filter=None):
    td = ledger_teldrive()
    tg = ledger_tgdb()
    series, eps = ledger_emby()
    by_tmdb = series_tmdb_index(series)

    gaps = []

    def add(kind, key, detail, path=""):
        if lib_filter and key and key[0] != lib_filter:
            return
        gaps.append({"kind": kind,
                     "lib": key[0] if key else "",
                     "series": key[1] if key else "",
                     "season": key[2] if key else None,
                     "ep": key[3] if key else None,
                     "quality": key[4] if key else "",
                     "detail": detail, "path": path})

    # 0) 重复体检：同 Path 多条目 / 同集多文件（E115、E128 的护栏）
    #    只记 gaps，不影响五账计数；出现即说明扫描并发或重传又发生了。
    _by_path = {}
    for it in eps.values():
        _by_path.setdefault(it.get("Path") or "", []).append(it)
    for _p, _its in _by_path.items():
        if _p and len(_its) > 1:
            k0 = next(iter(_its))
            gaps.append({"kind": "dup_path",
                         "lib": k0.get("lib", ""), "series": k0.get("series", ""),
                         "season": k0.get("season"), "ep": k0.get("ep"),
                         "quality": k0.get("quality", ""),
                         "detail": "同 Path 有 %d 个条目：%s" %
                                   (len(_its), ",".join(str(x.get("Id")) for x in _its)),
                         "path": _p})
    _by_ep = {}
    for k, it in eps.items():
        _by_ep.setdefault(k, []).append(it)
    for k, _its in _by_ep.items():
        _sz = {it.get("Size") for it in _its}
        if (len({it.get("Path") for it in _its}) > 1
                and _sz and None not in _sz and len(_sz) == 1):
            gaps.append({"kind": "dup_file", "lib": k[0], "series": k[1],
                         "season": k[2], "ep": k[3], "quality": k[4],
                         "detail": "同一集落了 %d 个不同文件，字节相同" % len(_its),
                         "path": ""})

    # 1) teldrive 有、tg.db 无 → 台账缺口
    for k in td:
        if k not in tg:
            add("unregistered", k, "teldrive 有文件但 tg.db 无记录")
    # 2) tg.db 有、teldrive 无 → 幽灵台账
    for k in tg:
        if k not in td:
            add("ghost_ledger", k, "tg.db 有记录但 teldrive 无文件")
    # 3) 文件在、Emby 无 Episode → 用户看不到
    for k in td:
        if k not in eps:
            add("episode_missing_emby", k,
                "文件在但 Emby 无 Episode（Series %s）"
                % ("存在" if _series_of(k) else "缺失"),
                PREFIX + "/" + (td[k][0] if td[k] else ""))
    # 4) Emby 有 Episode、teldrive 无 → 幽灵条目
    for k in eps:
        if k not in td:
            add("ghost_emby", k, "Emby 有条目但 teldrive 无文件")
    # 5) 缺 NFO
    for k, v in tg.items():
        if not v["has_nfo"]:
            add("no_nfo", k, "tg.db scraped_at 为空")

    # 6) 整剧级：挂载目录存在、Emby 无 Series
    sdirs = {}
    for k in td:
        m = re.search(r"\{tmdb=(\d+)\}", k[1])
        sdirs.setdefault(k[1], (k[0], m.group(1) if m else ""))
    for sdir, (lib, tmdb) in sorted(sdirs.items()):
        if lib_filter and lib != lib_filter:
            continue
        if not tmdb:
            continue
        if tmdb not in by_tmdb:
            smp = ""
            for _k, _v in td.items():
                if _k[1] == sdir and _v:
                    smp = PREFIX + "/" + _v[0]
                    break
            gaps.append({"kind": "series_missing_emby", "lib": lib, "series": sdir,
                         "season": None, "ep": None, "quality": "",
                         "detail": "tmdb=%s 在 Emby 无 Series（用户搜不到）" % tmdb,
                         "path": smp})
    return gaps, {"teldrive": len(td), "tgdb": len(tg), "emby_eps": len(eps),
                  "emby_series": len(series)}


def _series_of(key):
    return key[1] if key else ""


# ---------------------------------------------------------------- 修补
def load_state():
    try:
        return json.load(open(STATE, encoding="utf-8"))
    except Exception:                                       # noqa: BLE001
        return {}


def save_state(st):
    """合并写：并发实例各自只更新自己的键，不互相覆盖。

    2026-11-16：本文件的状态从「只有 repair 冷却」变成「repair 冷却 + gaps_seen」。
    两个进程（systemd timer 的 --repair 与手工 --json）会同时读改写同一个 JSON，
    整体覆盖会让对方的键凭空消失（repair 冷却丢了 = 同一剧每 15min 被刷一次；
    gaps_seen 丢了 = 差异年龄永远从 0 开始，孤儿永远长不大）。
    """
    cur = load_state()
    cur.update(st or {})
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    tmp = STATE + ".tmp.%d" % os.getpid()
    json.dump(cur, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
    os.replace(tmp, STATE)


def annotate_ages(gaps):
    """给每条差异算「已经存在多久」（age_min），首次发现时间持久化。

    为什么需要（2026-11-16 自醒）：五账对账每 15min 跑一次，但「差异存在」与
    「差异卡住」是两件事 —— 上传刚完成、tg.db 还没写台账的那 1~2 分钟里，
    unregistered 也是 True（实测 09:03 报 1 条，09:04 自动消失）。没有年龄，
    下游只能二选一：全部忽略（真孤儿被漏掉，E035 就是这么漏了几天）或全部当
    阻塞（每 15min 一次假报警 → 告警疲劳）。**年龄是唯一能分开两者的判据。**

    首次发现时间记在 logs/reconcile_state.json 的 gaps_seen；差异消失即删除，
    重现时重新计时（不继承旧年龄，避免「修好了又坏」被算成一次长期故障）。
    20 万集时这个字典也只保留最老的 2000 条，防止无界增长。
    """
    st = load_state()
    seen_all = st.get("gaps_seen") or {}
    now = time.time()
    seen_now = {}
    for g in gaps:
        key = "%s|%s|%s|%s|%s|%s" % (g["kind"], g["lib"], g["series"],
                                     g["season"], g["ep"], g["quality"])
        first = min(float(seen_all.get(key) or now), now)
        seen_now[key] = first
        g["age_min"] = round((now - first) / 60.0, 1)
    if len(seen_now) > 2000:
        keep = sorted(seen_now.items(), key=lambda kv: kv[1])[:2000]
        seen_now = dict(keep)
    st["gaps_seen"] = seen_now
    save_state(st)
    return gaps


def repair(gaps, only=None, cooldown_h=12):
    """只做有界、可逆的修补。每库/每剧每 cooldown_h 小时最多一次。"""
    st = load_state()
    now = time.time()
    cool = cooldown_h * 3600
    done = Counter()

    def allow(k):
        last = float(st.get(k) or 0)
        if now - last < cool:
            return False
        st[k] = now
        return True

    # 2026-11-13（E036）：优先 TargetedScan 的 /Library/ScanPath —— 它是 O(1) 的，
    # 能自建 Series/Season/Episode。只有在插件不可用或 ScanPath 失败时才退回库级刷新
    # （库级刷新 = 整库递归 ffprobe，综艺库 865 集要十几分钟，20 万集时代不可接受）。
    import emby_scan
    _scanpath_ok = emby_scan.have_scanpath()

    if _scanpath_ok and (not only or "series_missing_emby" in only):
        # 冷却：同一条路径 6 小时内只试一次。挂进短周期（15 分钟）后，没有冷却
        # 就会对"扫不出来的路径"每 15 分钟重试一次，白白制造 Emby 元数据刷新风暴。
        todo = [g for g in gaps if g["kind"] == "series_missing_emby" and g.get("path")
                and allow("scanpath:" + g["path"])]
        if todo:
            r = emby_scan.scan_paths([g["path"] for g in todo])
            print("  ✓ ScanPath 建剧 %d 条：Created=%d Failed=%d"
                  % (len(todo), r.get("Created", 0), r.get("Failed", 0)))
            for k in ("Created", "Refreshed"):
                done["series_missing_emby"] += r.get(k, 0)
            if r.get("Created") or r.get("Refreshed"):
                pass
            # 只要有建成的，就不再触发库级刷新
            if r.get("Created") or r.get("Refreshed"):
                only = (only or []) + ["__skip_library_refresh__"]

    libs = set() if "__skip_library_refresh__" in (only or []) else \
        {g["lib"] for g in gaps if g["kind"] == "series_missing_emby"}
    if libs and (not only or "series_missing_emby" in only):
        vfs = emby("/Library/VirtualFolders") or []
        lib_id = {v.get("Name"): (v.get("ItemId") or v.get("Id")) for v in vfs}
        for lib in sorted(libs):
            iid = lib_id.get(lib)
            if not iid:
                print("  ! 找不到库 %s 的 ItemId" % lib)
                continue
            if not allow("series_missing_emby:%s" % lib):
                print("  · 库 %s 在冷却期内，跳过（避免反复整库 ffprobe）" % lib)
                continue
            try:
                emby("/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default" % iid,
                     "POST")
                print("  ✓ 已对库 %s 发一次库级 Refresh（建新 Series 用）" % lib)
                done["series_missing_emby"] += 1
            except Exception as e:                          # noqa: BLE001
                print("  ✗ 库 %s 刷新失败：%s" % (lib, str(e)[:60]))

    if _scanpath_ok and (not only or "episode_missing_emby" in only):
        todo = [g for g in gaps if g["kind"] == "episode_missing_emby" and g.get("path")
                and allow("scanpath:" + g["path"])]
        if todo:
            r = emby_scan.scan_paths([g["path"] for g in todo][:200])
            print("  ✓ ScanPath 补集 %d 条：Created=%d Refreshed=%d Failed=%d"
                  % (len(todo), r.get("Created", 0), r.get("Refreshed", 0),
                     r.get("Failed", 0)))
            done["episode_missing_emby"] += r.get("Created", 0) + r.get("Refreshed", 0)
            only = (only or []) + ["__skip_series_refresh__"]

    if not only or ("episode_missing_emby" in only
                    and "__skip_series_refresh__" not in only):
        sids = defaultdict(set)
        for g in gaps:
            if g["kind"] != "episode_missing_emby":
                continue
            m = re.search(r"\{tmdb=(\d+)\}", g["series"])
            if not m:
                continue
            sid = series_id_by_tmdb(m.group(1))
            if sid:
                sids[sid].add(g["series"])
        for sid, names in list(sids.items())[:50]:
            if not allow("series_refresh:%s" % sid):
                continue
            try:
                emby("/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default" % sid,
                     "POST")
                done["episode_missing_emby"] += 1
            except Exception:                               # noqa: BLE001
                pass

    save_state(st)
    return done


_sid_cache = {}


def series_id_by_tmdb(tmdb):
    if tmdb in _sid_cache:
        return _sid_cache[tmdb]
    d = emby("/Items?Recursive=true&IncludeItemTypes=Series&Limit=5000"
             "&Fields=ProviderIds&StartIndex=0") or {}
    for i in d.get("Items") or []:
        t = (i.get("ProviderIds") or {}).get("Tmdb")
        if t:
            _sid_cache[str(t)] = i["Id"]
    return _sid_cache.get(str(tmdb))


# ---------------------------------------------------------------- 落账
def write_gaps(gaps):
    """把本轮差异写进 tg.db，并建立单集健康视图（对 tg.db 的增强）。

    以前 tg.db 只记「我们做了什么」（landed/scraped/emby_at），不记「和外面差什么」。
    现在多了两张东西：
      reconcile_gaps   每轮重建的差异清单（kind/lib/series/season/ep/detail）
      v_episode_health 把 resource_state 与 reconcile_gaps 合成一张单集健康视图

    这样任何一集的状态都能用一句 SQL 问出来，而不是靠翻脚本日志：
      sqlite3 pipeline/tg.db "select lib,count(*) from v_episode_health
                              where health<>'ok' group by 1"
    """
    # 2026-11-13 血泪：两个 reconcile 进程并发写 tg.db 时，前台读到过 resource_state=0
    # （瞬时幻读，数据其实没丢）。tg.db 的写必须**单一写者**：加 flock，并且整段在一个
    # IMMEDIATE 事务里，任何异常整体回滚，绝不留半截状态。
    lk = open(os.path.join(HERE, ".tgdb_write.lock"), "a+")
    try:
        fcntl.flock(lk, fcntl.LOCK_EX)
        con = sqlite3.connect(TGDB, timeout=120, isolation_level=None)
        con.execute("BEGIN IMMEDIATE")
        con.execute("""CREATE TABLE IF NOT EXISTS reconcile_gaps(
            ts INTEGER, kind TEXT, lib TEXT, series TEXT,
            season INTEGER, ep INTEGER, quality TEXT, detail TEXT,
            age_min REAL)""")
        # 2026-11-16：老库的 reconcile_gaps 只有 8 列（没有 age_min）。
        # 差异「存在多久」是分辨「在途」与「孤儿」的唯一判据，必须落库让下游
        # （status_now / selfcheck / 闸门）能读到，不能只留在这轮进程的内存里。
        _cols = [r[1] for r in con.execute("PRAGMA table_info(reconcile_gaps)")]
        if "age_min" not in _cols:
            con.execute("ALTER TABLE reconcile_gaps ADD COLUMN age_min REAL")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rg_kind ON reconcile_gaps(kind, lib)")
        con.execute("DELETE FROM reconcile_gaps")
        now = int(time.time())
        con.executemany(
            "insert into reconcile_gaps values(?,?,?,?,?,?,?,?,?)",
            [(now, g["kind"], g["lib"], g["series"], g["season"], g["ep"],
              g["quality"], g["detail"][:200], g.get("age_min")) for g in gaps])
        # 单集健康视图：一集在「文件 / NFO / Emby」三个环节的落点
        con.execute("DROP VIEW IF EXISTS v_episode_health")
        con.execute("""
        CREATE VIEW v_episode_health AS
        SELECT r.lib, r.title, r.tmdb, r.rel, r.stage,
               (COALESCE(r.landed_at, 0) > 0)  AS has_file,
               (COALESCE(r.scraped_at, 0) > 0) AS has_nfo,
               (COALESCE(r.emby_at, 0) > 0)    AS has_emby,
               CASE
                 WHEN COALESCE(r.landed_at, 0) = 0  THEN 'no_file'
                 WHEN COALESCE(r.scraped_at, 0) = 0 THEN 'no_nfo'
                 WHEN COALESCE(r.emby_at, 0) = 0    THEN 'not_in_emby'
                 ELSE 'ok'
               END AS health
        FROM resource_state r
        """)
        con.execute("COMMIT")
        con.close()
        return True
    except Exception as e:                                  # noqa: BLE001
        try:
            con.execute("ROLLBACK")
        except Exception:                                   # noqa: BLE001
            pass
        print("  ! 写 reconcile_gaps 失败（已回滚）：%s" % str(e)[:80])
        return False
    finally:
        try:
            fcntl.flock(lk, fcntl.LOCK_UN)
        finally:
            lk.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--repair", action="store_true")
    ap.add_argument("--only", action="append")
    ap.add_argument("--lib")
    ap.add_argument("--cooldown-h", type=float, default=12)
    a = ap.parse_args()

    t0 = time.time()
    gaps, stats = reconcile(a.lib)
    # 差异要带年龄：先标注首次发现时间，再落库。顺序不能反 —— write_gaps 会把
    # age_min 写进 reconcile_gaps，下游（status_now）就靠它区分「在途」与「孤儿」。
    annotate_ages(gaps)
    write_gaps(gaps)

    cnt = Counter(g["kind"] for g in gaps)
    if a.json:
        print(json.dumps({"stats": stats, "counts": dict(cnt),
                          "gaps": gaps[:2000]}, ensure_ascii=False, indent=1))
    else:
        print("== 五账对账（%.1fs）==" % (time.time() - t0))
        print("  teldrive 文件键 %d | tg.db 台账 %d | Emby 集 %d | Emby 剧 %d"
              % (stats["teldrive"], stats["tgdb"], stats["emby_eps"], stats["emby_series"]))
        print("  差异 %d 条：" % len(gaps))
        for k, n in cnt.most_common():
            print("    %-22s %d" % (k, n))
        for k in cnt:
            ex = [g for g in gaps if g["kind"] == k][:3]
            for g in ex:
                print("      · %-22s %s/%s S%sE%s %s  (age %.0fmin)"
                      % (k, g["lib"], g["series"][:22], g["season"], g["ep"],
                         g["detail"][:44], g.get("age_min") or 0))

    if a.repair:
        print("\n== 有界修补 ==")
        done = repair(gaps, a.only, a.cooldown_h)
        print("  ", dict(done) or "无事可做")

    # 退出码语义（2026-11-13 修）：作为 systemd oneshot 的 ExecStart 时，
    # 非 0 会让 unit 天天显示 failed —— 只要有差异就红，红久了就没人看了（狼来了）。
    # 正确语义：
    #   0 = 对账完成（有差异但 --repair 已处理完，或本来就没差异）
    #   1 = 没加 --repair 却存在差异（人工排查用，提示"需要修"）
    #   2 = 发生了异常（真正的错误，必须红）
    if a.repair:
        return 0
    return 0 if not gaps else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:                                       # noqa: BLE001
        import traceback
        traceback.print_exc()
        sys.exit(2)          # 2 = 真异常（与"有差异"的 1 区分开）
