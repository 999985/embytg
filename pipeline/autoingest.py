#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""autoingest.py —— 无人值守自动入库（有闸门、可审计、只补"确定的缺口"）

策略（为什么只补缺口）
--------------------
随机拉新片会撞车（运维任务已在 TG 拉）、会拉重复、会超预算。**缺口**是确定性的：
磁盘某季比数据源少几集 → 去源服找那几集 → 补完即停。

两道闸门
--------
1. `ingest_quota`：日 ≤300GB、月 ≤9000GB
2. 本脚本：单轮 ≤ `--max-gb`（默认 30）、单轮 ≤ `--max-files`（默认 3）

流程
----
    python3 autoingest.py --plan          # 扫库找缺口（disk vs TMDB 集数），写 .autoingest_queue.json
    python3 autoingest.py --run           # 从队列取，调 source_pull.py 拉取并装库（受预算限制）
    python3 autoingest.py --backfill      # 只读检查：历史自动入库文件是否缺 tg.db.state_key
    python3 autoingest.py --backfill --apply  # 用 pc_register.py --file-id --tgdb 补登记
    python3 autoingest.py --show          # 看队列

2026-11-11 用户授权云端 24h 上片：
- 源站下载不再人为压到“正常观看节奏”，source_pull 默认 0=不限速；
- 仍保留 Hills UA、只 GET、单源并发 1、元数据间隔、单会话 ≤12 次、连败熔断；
- 每个文件装入 teldrive 后必须反查 files/file_parts，再用 pc_register.py --tgdb
  写 tg.db.state_key，否则 tgpipe 去重台账会漏记；
- 不再为 teldrive 内落地写 .ingest_manual.jsonl，避免与 resource_state 重复计费。
"""
import argparse
import json
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
MNT = "/opt/embytg/mnt/embytg/TV"
MNT_ROOT = "/opt/embytg/mnt/embytg"
QUEUE = os.path.join(HERE, ".autoingest_queue.json")
FAILMEM = os.path.join(HERE, ".autoingest_failed.json")   # 失败记忆：别每轮都去撞同一堵墙
SERIES_PREFIX = "@series:"                                # 系列级失败记忆键前缀（整部剧没源）
GHA_CLAIM = os.path.join(HERE, ".gha_claim.json")         # GitHub Actions 正在处理的任务
GHA_CLAIM_DIR = os.path.join(HERE, ".gha_claims")
ORACLE_MEDIA_DISABLED = os.path.join(HERE, ".oracle_media_ingest.disabled")
CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
TGDB = os.path.join(HERE, "tg.db")
PC_REGISTER = os.path.join(HERE, "pc_register.py")
SE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
TMDB = CFG["tmdb_api"]
KEY = CFG["tmdb_api_key"]


def tmdb(path, **q):
    import urllib.parse
    import urllib.request
    q.update(api_key=KEY, language="zh-CN")
    u = TMDB + path + "?" + urllib.parse.urlencode(q)
    with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=25) as f:
        return json.loads(f.read().decode())


def disk_seasons(sp):
    """{season: {ep: path}}"""
    out = {}
    for dp, _, fs in os.walk(sp):
        if not os.path.basename(dp).startswith("Season"):
            continue
        for fn in fs:
            if not fn.lower().endswith((".mp4", ".mkv")):
                continue
            m = SE.search(fn)
            if m:
                out.setdefault(int(m.group(1)), {})[int(m.group(2))] = os.path.join(dp, fn)
    return out


def plan(limit_series=60):
    q = []
    seen = set()          # 去重：同一 (剧目录, 季) 只排一次（11-10 自审发现重复项）
    libs = [d for d in os.listdir(MNT) if os.path.isdir(os.path.join(MNT, d))]
    cursor = 0
    if os.path.exists(QUEUE):
        try:
            cursor = json.load(open(QUEUE, encoding="utf-8")).get("cursor", 0)
        except Exception:                                          # noqa: BLE001
            pass
    series_all = []
    for lib in libs:
        for d in sorted(os.listdir(os.path.join(MNT, lib))):
            if os.path.isdir(os.path.join(MNT, lib, d)):
                series_all.append((lib, d))
    series_all.sort()
    batch = series_all[cursor:cursor + limit_series]
    fail = load_fail()
    hist = blocked_series_from_history(fail)
    skipped_series = 0
    for lib, d in batch:
        series = d.split(" (")[0]
        if hot_series(fail, series) or series in hist:
            skipped_series += 1
            continue
        m = re.search(r"\{tmdb=(\d+)\}", d)
        if not m:
            continue
        sp = os.path.join(MNT, lib, d)
        ds = disk_seasons(sp)
        if not ds:
            continue
        try:
            meta = tmdb("/tv/%s" % m.group(1))
        except Exception:                                          # noqa: BLE001
            continue
        want = {s["season_number"]: s["episode_count"] for s in meta.get("seasons", [])
                if (s.get("season_number") or 0) > 0}
        for sn, eps in ds.items():
            if sn not in want or not want[sn]:
                continue
            have = set(eps)
            miss = sorted(set(range(1, want[sn] + 1)) - have)
            if miss and len(miss) <= 12:          # 缺太多说明季号错，不归自动入库
                key = (lib, d, sn)
                if key in seen:
                    continue
                seen.add(key)
                q.append({"lib": lib, "dir": d, "series": d.split(" (")[0], "season": sn,
                          "missing": miss, "tmdb": m.group(1)})
    json.dump({"ts": time.strftime("%F %T"), "cursor": (cursor + limit_series) % max(len(series_all), 1),
               "total_series": len(series_all), "items": q},
              open(QUEUE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("plan 完成：扫 %d 部，发现 %d 个待补季 → %s（系列级熔断跳过 %d 部）"
          % (len(batch), len(q), QUEUE, skipped_series))
    for x in q[:10]:
        print("  %-10s S%-2s 缺 %s" % (x["series"][:10], x["season"], x["missing"]))


def budget_left():
    import ingest_quota as Q
    c = Q.cfg()
    d_gb, _, m_gb, _ = Q.usage()
    return min(c["daily_gb"] - d_gb, c["safety_gb"] - m_gb)


def dedupe_items(items):
    """同一 (lib,dir,season) 合并缺集，避免 queue 里出现重复季。"""
    merged = {}
    order = []
    for x in items:
        k = (x.get("lib"), x.get("dir"), int(x.get("season") or 0))
        if k not in merged:
            y = dict(x)
            y["missing"] = sorted(set(int(e) for e in (x.get("missing") or [])))
            merged[k] = y
            order.append(k)
        else:
            merged[k]["missing"] = sorted(set(merged[k]["missing"]) |
                                          set(int(e) for e in (x.get("missing") or [])))
    return [merged[k] for k in order]


def qual_of(name):
    m = re.search(r"(2160p|1080p|720p|4K)", name, re.I)
    return m.group(1).lower().replace("4k", "2160p") if m else "sd"


def state_key_for(tmdb, season, ep, name):
    return "tv|id:%d|(%d, %d)|%s" % (int(tmdb), int(season), int(ep), qual_of(name))


def gha_claimed(key):
    """GitHub Actions 已领取的任务，autoingest 必须避让，防止双下载。"""
    paths = [GHA_CLAIM]
    try:
        paths += [os.path.join(GHA_CLAIM_DIR, x) for x in os.listdir(GHA_CLAIM_DIR)
                  if x.endswith(".json")]
    except Exception:                                          # noqa: BLE001
        pass
    for path in paths:
        try:
            d = json.load(open(path, encoding="utf-8"))
            if (d.get("status") == "running" and d.get("key") == key
                    and time.time() - float(d.get("ts") or 0) < 6 * 3600):
                return True
        except Exception:                                      # noqa: BLE001
            continue
    return False


def state_key_exists(key):
    con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=30)
    try:
        return con.execute("select 1 from state_key where k=?", (key,)).fetchone() is not None
    finally:
        con.close()


def resource_has_episode(item, ep, qual):
    """state_key 缺失时，用 resource_state 的 tmdb+季集+画质兜底判断是否已有同画质文件。

    2026-11-13 可扩展性：原实现 `tmdb=? and rel like ? and rel like ?`，
    虽然先用 tmdb 索引收敛，但一部 1200 集的剧仍要扫描该剧全部行做两次 LIKE，
    单次判断就是 O(该剧集数)。改为按 (season, ep) 精确定位：
    先用 tmdb 索引拿到该季的相对路径前缀，再用范围查询定位到具体那一集。
    """
    try:
        con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=30)
        season = int(item["season"])
        ep = int(ep)
        # 先取该剧该季的任一 rel 推出目录前缀（只读 1 行，走 idx_rs_tmdb）
        row = con.execute(
            "select rel from resource_state where tmdb=? and rel is not null limit 1",
            (int(item["tmdb"]),)).fetchone()
        if not row:
            con.close()
            return False
        rel0 = row[0] or ""
        # 目录前缀形如 TV/<lib>/<剧名 (年) {tmdb=N}/Season 01/
        cut = rel0.find("/Season ")
        prefix = rel0[:cut + 9] if cut >= 0 else ""
        hit = None
        if prefix:
            # 该季所有文件按路径排序，范围查询只落在这一季
            hi = prefix[:-1] + chr(ord(prefix[-1]) + 1)
            for (rel,) in con.execute(
                    "select rel from resource_state where tmdb=? and rel>=? and rel<? "
                    "and rel is not null", (int(item["tmdb"]), prefix, hi)):
                name = (rel or "").rsplit("/", 1)[-1]
                m = SE.search(name)
                if m and int(m.group(1)) == season and int(m.group(2)) == ep:
                    hit = rel
                    break
        con.close()
        return bool(hit) and (qual in (hit or ""))
    except Exception:                                              # noqa: BLE001
        return False


def scan_idle():
    """Emby 全库扫描 Running 时不启动新下载，避免与扫描抢 FUSE/磁盘。"""
    try:
        base = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
        req = urllib.request.Request(base + "/ScheduledTasks",
                                     headers={"X-Emby-Token": CFG["emby_token"]})
        with urllib.request.urlopen(req, timeout=20) as f:
            tasks = json.loads(f.read().decode())
        for t in tasks:
            if t.get("Key") == "RefreshLibrary":
                return (t.get("State") or "").lower() != "running"
        return True
    except Exception as e:                                         # noqa: BLE001
        print("  ! 无法确认 Emby 扫描状态：%s" % str(e)[:100])
        return False


def disk_ok(min_free_gb=20):
    free = shutil.disk_usage("/opt").free / 1e9
    if free < min_free_gb:
        print("  ! 磁盘余量 %.1fGB < %.0fGB，暂停下载" % (free, min_free_gb))
        return False
    return True


_TD_CACHE = {"ts": 0.0, "nodes": {}, "paths": {}, "set": None}


def td_snapshot(force=False):
    """复用 resource_state 的 teldrive 文件树；10 秒缓存，避免每集重建。"""
    import resource_state as RS
    if not force and _TD_CACHE["nodes"] and time.time() - _TD_CACHE["ts"] < 10:
        return _TD_CACHE["nodes"], _TD_CACHE["paths"]
    nodes = RS.td_tree()
    paths = RS.build_paths(nodes)
    _TD_CACHE.update({"ts": time.time(), "nodes": nodes, "paths": paths,
                      "set": None})
    return nodes, paths


def _td_path_set():
    """把 td_snapshot() 的 paths 反转成 set，供 O(1) 成员判断。

    2026-11-15 可扩展性修复（周度体检发现）：
    td_path_exists() 原来写的是 `rel in paths.values()` —— values() 是 dict_values
    视图，`in` 只能线性扫描，**不是**哈希查找。全库 17724 条时单次约 0.5ms，
    但 check_existing() 每集都调一次，一轮扫描几十集就是几十毫秒；
    按 20 万条 × 1000 集推算要 200 秒 —— 超长连载剧（海贼王/柯南）会直接卡死领片。
    现在改成 set 成员判断，O(1)，与库大小无关。
    """
    snap = _TD_CACHE
    if snap.get("set") is not None:
        return snap["set"]
    _, paths = td_snapshot()
    s = set(paths.values())
    _TD_CACHE["set"] = s
    return s


def td_path_exists(target):
    """teldrive 文件树里已有同相对路径 → 说明字节层已上片。"""
    rel = os.path.relpath(target, MNT_ROOT).replace("\\", "/")
    return rel in _td_path_set()


def psql_rows(sql):
    import resource_state as RS
    cmd = shlex.split(RS.PG) + ["-F", "|"]
    p = subprocess.run(cmd, input=sql.encode("utf-8"), stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, timeout=120)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode("utf-8", "replace")[:300])
    return p.stdout.decode("utf-8", "replace").strip()


def wait_td_manifest(target, timeout=900):
    """等 rclone/teldrive 把目标文件真正落库，返回 (file_id, manifest)。"""
    rel = os.path.relpath(target, MNT_ROOT).replace("\\", "/")
    local_size = os.path.getsize(target)
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        nodes, paths = td_snapshot(force=True)
        fid = next((nid for nid, p in paths.items()
                    if p == rel and nodes[nid].get("kind") == "file"), None)
        if fid:
            ts = nodes[fid].get("size") or 0
            if ts == local_size:
                rows = psql_rows(
                    "select part_no,channel_id,message_id,plain_size,stored_size "
                    "from file_parts where file_id='%s' order by part_no;" % fid)
                parts = []
                for line in rows.splitlines():
                    f = line.split("|")
                    if len(f) >= 5:
                        parts.append({"part_no": int(f[0]), "channel_id": int(f[1]),
                                      "message_id": int(f[2]),
                                      "plain_size": int(f[3]), "stored_size": int(f[4])})
                if parts:
                    return fid, {
                        "name": os.path.basename(target),
                        "mime_type": "video/x-matroska" if target.lower().endswith(".mkv") else "video/mp4",
                        "channel_id": parts[0]["channel_id"],
                        "parts": parts,
                    }
            last = "teldrive size=%s local=%s parts待完成" % (ts, local_size)
        else:
            last = "等待 teldrive 文件记录"
        time.sleep(5)
    raise TimeoutError("等待 teldrive 落库超时: %s (%s)" % (rel, last or "无"))


def register_state_key(target, item, ep):
    """复用 pc_register.py 的 --file-id/--tgdb 路径，绝不手写 state_key SQL。"""
    rel = os.path.relpath(target, MNT_ROOT).replace("\\", "/")
    folders = os.path.dirname(rel)
    fid, manifest = wait_td_manifest(target)
    cmd = [sys.executable, PC_REGISTER, "--folders", folders, "--file-id", fid,
           "--tgdb", "--ch", "source_cloud", "--tmdb", str(item["tmdb"]),
           "--lib", item["lib"]]
    p = subprocess.run(cmd, input=json.dumps(manifest, ensure_ascii=False),
                       capture_output=True, text=True, timeout=180)
    out = ((p.stdout or "") + (p.stderr or "")).strip()
    return p.returncode, out[-500:], fid


def parse_target(target):
    """从库内路径反推 backfill 需要的 lib/dir/season/ep/tmdb。"""
    rel = os.path.relpath(target, MNT_ROOT).replace("\\", "/")
    parts = rel.split("/")
    if len(parts) < 5 or parts[0] != "TV":
        return None
    m = re.search(r"\{tmdb=(\d+)\}", parts[2])
    sm = re.search(r"Season\s*(\d+)", parts[3], re.I)
    em = SE.search(os.path.basename(target))
    if not (m and sm and em):
        return None
    return {
        "lib": parts[1], "dir": parts[2], "series": parts[2].split(" (")[0],
        "season": int(sm.group(1)), "ep": int(em.group(2)), "tmdb": m.group(1),
    }


def backfill(apply=False):
    """历史 autoingest 成功文件补 state_key；默认 dry-run。"""
    log = os.path.join(HERE, "logs", "autoingest.log")
    targets = []
    if not os.path.exists(log):
        print("没有 autoingest.log")
        return 0
    for line in open(log, encoding="utf-8", errors="ignore"):
        try:
            r = json.loads(line)
        except Exception:                                          # noqa: BLE001
            continue
        t = r.get("target") or ""
        if r.get("ok") and t and os.path.exists(t) and t not in targets:
            targets.append(t)
    todo = 0
    for t in targets:
        meta = parse_target(t)
        if not meta:
            print("  ? 无法解析 %s" % t)
            continue
        key = state_key_for(meta["tmdb"], meta["season"], meta["ep"], os.path.basename(t))
        if state_key_exists(key):
            print("  OK  %s" % key)
            continue
        todo += 1
        if not apply:
            print("  WOULD %s  %s" % (key, os.path.basename(t)))
            continue
        try:
            rc, out, fid = register_state_key(t, meta, meta["ep"])
            print("  %s %s rc=%d file_id=%s %s" % (
                "✔" if rc == 0 else "✘", key, rc, fid, out[:160]))
        except Exception as e:                                     # noqa: BLE001
            print("  ✘ %s %s" % (key, str(e)[:180]))
    print("backfill：目标 %d，缺登记 %d，模式=%s" % (len(targets), todo, "APPLY" if apply else "DRY"))
    return 0


def target_path(item, ep):
    """沿用该季已有文件的命名风格（剧名/画质）"""
    sp = os.path.join(MNT, item["lib"], item["dir"], "Season %02d" % item["season"])
    if not os.path.isdir(sp):
        # 用别的季当模板
        base = os.path.join(MNT, item["lib"], item["dir"])
        subs = [x for x in os.listdir(base) if x.startswith("Season")]
        if not subs:
            return None
        sp = os.path.join(base, subs[0])
    cand = [f for f in os.listdir(sp) if f.lower().endswith((".mp4", ".mkv"))]
    qual = "2160p" if any("2160p" in f for f in cand) else ("1080p" if any("1080p" in f for f in cand) else "")
    ext = os.path.splitext(cand[0])[1] if cand else ".mkv"
    name = "%s S%02dE%02d%s%s" % (item["series"], item["season"], ep, (" - " + qual) if qual else "", ext)
    return os.path.join(MNT, item["lib"], item["dir"], "Season %02d" % item["season"], name)


def load_fail():
    try:
        return json.load(open(FAILMEM, encoding="utf-8"))
    except Exception:                                              # noqa: BLE001
        return {}


def save_fail(d):
    json.dump(d, open(FAILMEM, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def blocked_series_from_history(fail):
    """把「某剧所有失败记录都是 no_source」自动视为整部剧无源。

    无需人工跑 CLI：11-10 的射雕/山神/尚公主 12+10+6 集记录本身已是证据。
    返回 {series: 记录数}；非 no_source 记录（如仅 404）不在此列。
    """
    by = {}
    for k, r in fail.items():
        if k.startswith(SERIES_PREFIX) or "|" not in k:
            continue
        series = k.rsplit("|", 2)[0]
        by.setdefault(series, []).append(r)
    return {s: len(rs) for s, rs in by.items()
            if len(rs) >= 2 and all(x.get("reason") == "no_source" for x in rs)}


def hot(fail, series, season, ep, days=7, times=2):
    """同一集失败 ≥2 次且冷却期内 → 跳过。
    no_source（所有源都没有）冷却 30 天：重试没有意义，等 TG 补源或换源。

    先看系列级：整部剧都没源时，逐集判断毫无意义（11-10 复盘）。
    这条也保护 GitHub Actions 领取路径（gha_next 调的就是本函数）。"""
    if hot_series(fail, series) or series in blocked_series_from_history(fail):
        return True
    k = "%s|%d|%d" % (series, season, ep)
    r = fail.get(k)
    if not r or r.get("n", 0) < times:
        return False
    d = 30 if r.get("reason") == "no_source" else days
    return (time.time() - r.get("ts", 0)) < d * 86400


def series_cooldown_days(reason):
    """系列级冷却：整部剧没有任何源 = 30 天；直链坏/不稳定 = 7 天。"""
    return 30 if reason == "no_source_series" else 7


def hot_series(fail, series, now=None):
    """整部剧级熔断。11-10 复盘：射雕/山神/尚公主 三部剧 46 集
    逐个去撞同一批源，吃掉当天 85 次失败里的 56 次——
    集级记忆救不了「整部剧都不在源上」，必须系列级记一次。"""
    r = fail.get(SERIES_PREFIX + series)
    if not r:
        return False
    now = time.time() if now is None else now
    return (now - r.get("ts", 0)) < series_cooldown_days(r.get("reason")) * 86400


def block_series(fail, series, reason="no_source_series", note=""):
    """写系列级失败记忆（调用方负责 save_fail）。"""
    k = SERIES_PREFIX + series
    rec = fail.get(k, {"n": 0})
    rec["n"] = rec.get("n", 0) + 1
    rec["ts"] = time.time()
    rec["reason"] = reason
    rec["last"] = (note or "")[-160:]
    fail[k] = rec
    return rec


def rebuild_series_blocks(apply=False):
    """把历史失败记忆里「整部剧所有集都是 no_source」的剧收口成系列级记忆。
    默认只读打印；--apply 才写。"""
    fail = load_fail()
    cands = sorted(blocked_series_from_history(fail).items(), key=lambda x: -x[1])
    for series, n in cands:
        print("  %-40s %2d 集全 no_source → 系列级 30 天" % (series[:40], n))
        if apply and not hot_series(fail, series):
            block_series(fail, series, "no_source_series",
                         "backfill: %d eps all no_source" % n)
    if apply and cands:
        save_fail(fail)
    print("系列级收口：候选 %d 部，模式=%s" % (len(cands), "APPLY" if apply else "DRY"))
    return 0


def run(max_gb=30.0, max_files=3, limit_mbps=0.0, max_attempts=12):
    if os.path.exists(ORACLE_MEDIA_DISABLED):
        print("甲骨文媒体数据面已禁用（GitHub Actions 接管）；拒绝 autoingest --run")
        return 0
    if not os.path.exists(QUEUE):
        print("没有队列，先 --plan")
        return 0
    data = json.load(open(QUEUE, encoding="utf-8"))
    data["items"] = dedupe_items(data.get("items") or [])
    if not disk_ok():
        return 0
    if not scan_idle():
        print("  Emby 扫描 Running，本轮不下载")
        return 0
    left = budget_left()
    cap = min(left, max_gb)
    print("预算：今日剩余 %.1f GB，本轮上限 %.1f GB，最多 %d 个文件" % (left, cap, max_files))
    done = used = attempts = 0
    installed = []
    reg_failed = []
    remaining = []
    for item in data["items"]:
        if used >= cap or done >= max_files or attempts >= max_attempts:
            remaining.append(item)
            continue
        fail = load_fail()
        if hot_series(fail, item["series"]):
            sr = fail[SERIES_PREFIX + item["series"]]
            print("  - 跳过 %s 整部（系列级熔断：%s，%d 天冷却）"
                  % (item["series"][:14], sr.get("reason"),
                     series_cooldown_days(sr.get("reason"))))
            with open(os.path.join(HERE, "logs", "autoingest.log"), "a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": time.strftime("%F %T"), "event": "skip_series",
                                    "series": item["series"], "season": item["season"],
                                    "reason": sr.get("reason")}, ensure_ascii=False) + "\n")
            continue
        series_err = 0
        miss = list(item["missing"])
        for ep in miss:
            if used >= cap or done >= max_files or attempts >= max_attempts:
                remaining.append({**item, "missing": miss[miss.index(ep):]})
                break
            tgt = target_path(item, ep)
            if not tgt or os.path.exists(tgt):
                continue
            qual = qual_of(os.path.basename(tgt))
            key = state_key_for(item["tmdb"], item["season"], ep, os.path.basename(tgt))
            if gha_claimed(key):
                print("  - 跳过 %s S%02dE%02d（GitHub Actions 已领取）"
                      % (item["series"][:12], item["season"], ep))
                continue
            # 上片前防重（用户 2026-11-11 明确要求先广播+查重）：
            # state_key、teldrive 实际路径、resource_state 任一命中都必须跳过；
            # 不能要求三者同时存在，否则历史只写了其中一套库的集会被重复下载。
            try:
                has_state = state_key_exists(key)
                has_res = resource_has_episode(item, ep, qual)
                has_td = td_path_exists(tgt)
            except Exception as e:                                  # noqa: BLE001
                print("  ! 查重失败，保守跳过 %s S%02dE%02d：%s"
                      % (item["series"][:12], item["season"], ep, str(e)[:80]))
                continue
            if has_state or has_res or has_td:
                why = ",".join(x for x, ok in (("state_key", has_state),
                                               ("resource_state", has_res),
                                               ("teldrive", has_td)) if ok)
                print("  - 跳过 %s S%02dE%02d（防重命中：%s）"
                      % (item["series"][:12], item["season"], ep, why))
                with open(os.path.join(HERE, "logs", "autoingest.log"), "a", encoding="utf-8") as f:
                    f.write(json.dumps({"ts": time.strftime("%F %T"), "event": "duplicate",
                                        "series": item["series"], "season": item["season"],
                                        "ep": ep, "key": key, "why": why,
                                        "target": tgt}, ensure_ascii=False) + "\n")
                continue
            fail = load_fail()
            if hot(fail, item["series"], item["season"], ep):
                print("  - 跳过 %s S%02dE%02d（7 天内已失败 %d 次）"
                      % (item["series"][:12], item["season"], ep, fail["%s|%d|%d" % (item["series"], item["season"], ep)]["n"]))
                continue
            cmd = [sys.executable, os.path.join(HERE, "source_pull.py"), "--series", item["series"],
                   "--season", str(item["season"]), "--ep", str(ep),
                   "--limit-mbps", str(limit_mbps), "--apply", "--install", tgt]
            attempts += 1
            t0 = time.time()
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            out = (r.stdout or "") + (r.stderr or "")
            ok = r.returncode == 0 and "已装入" in out
            no_src = (r.returncode in (4, 6))
            gb = 0.0
            m = re.search(r"本次需要 ([\d.]+) GB", out)
            if m:
                gb = float(m.group(1))
            if ok:
                # 源站 Size 可能是错误的（实测 2.59GB 被报成 9.51GB）；
                # 成功后一律按实际落地文件记账，避免提前吃满 --max-gb。
                try:
                    gb = os.path.getsize(tgt) / 1e9
                except OSError:
                    gb = 0.0
            print("  %s %s S%02dE%02d (%.1fGB, %.0fs)"
                  % ("✔" if ok else "✘", item["series"][:14], item["season"], ep, gb, time.time() - t0))
            with open(os.path.join(HERE, "logs", "autoingest.log"), "a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": time.strftime("%F %T"), "series": item["series"], "season": item["season"],
                                    "ep": ep, "ok": ok, "gb": gb, "target": tgt,
                                    "tail": out[-300:]}, ensure_ascii=False) + "\n")
            if ok:
                used += gb
                done += 1
                series_err = 0          # 成功即打断"同剧连败"计数
                installed.append(tgt)
                miss.remove(ep)
                # 有成功落地 → 之前的系列级/集级失败记忆作废
                try:
                    f3 = load_fail()
                    k3 = "%s|%d|%d" % (item["series"], item["season"], ep)
                    if f3.pop(k3, None) is not None or \
                            f3.pop(SERIES_PREFIX + item["series"], None) is not None:
                        save_fail(f3)
                except Exception:                                  # noqa: BLE001
                    pass
                try:
                    rc, reg_out, fid = register_state_key(tgt, item, ep)
                    print("    state_key %s rc=%d file_id=%s %s"
                          % ("OK" if rc == 0 else "FAIL", rc, fid, reg_out[:120]))
                    with open(os.path.join(HERE, "logs", "autoingest.log"), "a", encoding="utf-8") as f:
                        f.write(json.dumps({"ts": time.strftime("%F %T"), "event": "register",
                                            "series": item["series"], "season": item["season"],
                                            "ep": ep, "ok": rc == 0, "file_id": fid,
                                            "tail": reg_out[-300:]}, ensure_ascii=False) + "\n")
                    if rc != 0:
                        reg_failed.append(tgt)
                except Exception as e:                             # noqa: BLE001
                    print("    state_key FAIL %s" % str(e)[:180])
                    reg_failed.append(tgt)
            else:
                f2 = load_fail()
                k = "%s|%d|%d" % (item["series"], item["season"], ep)
                rec = f2.get(k, {"n": 0, "ts": time.time()})
                rec["n"] = rec.get("n", 0) + 1
                rec["ts"] = time.time()
                rec["last"] = (out or "")[-160:]
                if no_src:
                    rec["reason"] = "no_source"      # 所有源都没有 → 交给 TG 补源，不再反复重试
                    rec["n"] = max(rec.get("n", 0), 2)
                f2[k] = rec
                series_err += 1
                if no_src and r.returncode == 6:
                    # 整部剧都不在任何源：立刻收口，不再试该剧其余集。
                    block_series(f2, item["series"], "no_source_series", out)
                    print("    → 所有源都没有这部剧，标记系列级 30 天，跳过其余 %d 集"
                          % len(miss))
                elif series_err >= 3:
                    # 同一部剧连续 3 集失败（404/无源混合）→ 本轮停止，避免把预算耗在一棵树上。
                    block_series(f2, item["series"],
                                 "no_source_episode" if no_src else "direct_link_fail", out)
                    print("    → 同一部剧连续 %d 集失败，本轮停止该剧（系列冷却 %d 天）"
                          % (series_err, series_cooldown_days(
                              "no_source_episode" if no_src else "direct_link_fail")))
                save_fail(f2)
                if (no_src and r.returncode == 6) or series_err >= 3:
                    break
        if miss:
            remaining.append({**item, "missing": miss})
    data["items"] = remaining
    data["last_run"] = time.strftime("%F %T")
    json.dump(data, open(QUEUE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("本轮完成 %d 个，用 %.1f GB；队列剩 %d 项" % (done, used, len(remaining)))
    if done and installed:
        notify(sorted(set(os.path.dirname(p) for p in installed)))
        # 强制收尾：NFO → Emby Name → Refresh → 验收（用户要求"一定按流程来"）
        try:
            r = subprocess.run([sys.executable, os.path.join(HERE, "post_ingest.py"),
                                "--since", "today"], capture_output=True, text=True, timeout=3600)
            print("  收尾 post_ingest rc=%d：%s" % (r.returncode,
                  ((r.stdout or "") + (r.stderr or "")).strip().splitlines()[-1][:80]))
        except Exception as e:                                      # noqa: BLE001
            print("  收尾失败:", str(e)[:80])
        try:
            r = subprocess.run([sys.executable, os.path.join(HERE, "resource_state.py"), "--scan"],
                               capture_output=True, text=True, timeout=1800)
            print("  resource_state rc=%d：%s" % (r.returncode,
                  ((r.stdout or "") + (r.stderr or "")).strip().splitlines()[-1][:100]))
        except Exception as e:                                      # noqa: BLE001
            print("  resource_state 失败:", str(e)[:100])
        if reg_failed:
            print("  ! %d 个文件 state_key 未完成，下一轮 backfill 会补" % len(reg_failed))
    return 0


def notify(paths):
    """入库后让 Emby 感知（增量通知 + 单剧 refresh），不等全库扫描"""
    import urllib.request
    B = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
    T = CFG["emby_token"]
    def call(p, method="GET", body=None):
        req = urllib.request.Request(B + p, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"X-Emby-Token": T, "Content-Type": "application/json"})
        return urllib.request.urlopen(req, timeout=60).read()
    try:
        call("/Library/Media/Updated", "POST", {"Updates": [{"Path": p.replace("/opt/embytg/mnt/embytg", "/mnt/embytg"),
                                                             "UpdateType": "Created"} for p in paths]})
        print("  已通知 Emby：%d 个路径" % len(paths))
    except Exception as e:                                          # noqa: BLE001
        print("  通知 Emby 失败:", str(e)[:60])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--backfill", action="store_true",
                    help="检查/补历史自动入库文件的 tg.db.state_key（默认 dry-run）")
    ap.add_argument("--rebuild-series-blocks", action="store_true",
                    help="把历史「整部剧全 no_source」收口成系列级记忆（默认 dry-run）")
    ap.add_argument("--apply", action="store_true",
                    help="配合 --backfill 真正写库")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--max-gb", type=float, default=30.0)
    ap.add_argument("--max-files", type=int, default=3)
    ap.add_argument("--max-attempts", type=int, default=12,
                    help="本轮最多尝试多少个目标（含 no_source），防止反复搜源")
    ap.add_argument("--limit-mbps", type=float, default=0.0,
                    help="0=不限速（默认），直接交给 source_pull.py")
    ap.add_argument("--limit-series", type=int, default=60)
    a = ap.parse_args()
    if a.plan:
        return plan(a.limit_series)
    if a.run:
        return run(a.max_gb, a.max_files, a.limit_mbps, a.max_attempts)
    if a.backfill:
        return backfill(a.apply)
    if a.rebuild_series_blocks:
        return rebuild_series_blocks(a.apply)
    if a.show:
        d = json.load(open(QUEUE, encoding="utf-8")) if os.path.exists(QUEUE) else {"items": []}
        items = dedupe_items(d.get("items", []))
        print("队列生成于 %s，共 %d 项（去重后 %d）" % (d.get("ts"), len(d.get("items", [])), len(items)))
        for x in items[:20]:
            print("  %-12s S%-2s 缺 %s" % (x["series"][:12], x["season"], x["missing"][:12]))
        return 0
    ap.print_help()


if __name__ == "__main__":
    sys.exit(main() or 0)
