#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gha_state.py — shared lane claims and cross-process source budget.

The three logical lanes (new/fill/upgrade) run in separate jobs.  The upgrade
queue is split into two mutually exclusive runner shards (``upgrade`` and
``upgrade2``) so two claims can run without selecting the same episode.
share one source-server policy and must not race on claims or source limits.
This module is deliberately small: it only stores JSON state and uses flock;
media download/registration/scraping stay in the project's existing scripts.
"""
import contextlib
import fcntl
import json
import math
import os
import re
import sqlite3
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CLAIM_DIR = os.path.join(HERE, ".gha_claims")
LEGACY_CLAIM = os.path.join(HERE, ".gha_claim.json")
LANES = ("new", "new2", "new3", "new4", "new5", "fill", "upgrade", "upgrade2")
CLAIM_TTL = int(os.getenv("GHA_CLAIM_TTL", str(24 * 3600)))

BUDGET_FILE = os.path.join(HERE, ".gha_src_budget.json")
BUDGET_LOCK = os.path.join(HERE, ".gha_src_budget.lock")
BURST_WINDOW = 20 * 60
# 2026-11-25（自省）：源站延迟只在这里落一份 EMA（跨日保留）。
# 判据不是"慢就罚"，而是"同一件事，慢源要花 20 倍墙钟"：aaa.yusen6 平均
# 11.3s/次、8 天 1597 次 ≈ 5 小时纯等待，而 bangumi 只有 0.47s。
# 3s 是实测分界（本项目 8 个源里快源 0.5-0.7s，慢源 9.6-11.3s），中间没有样本。
SRC_SLOW_MS = float(os.environ.get("EMBYTG_SRC_SLOW_MS") or 3000)
DEFAULT_BUDGET = {
    "daily": int(os.getenv("GHA_SRC_DAILY", "300")),
    "burst_20m": int(os.getenv("GHA_SRC_BURST_20M", "60")),
    "fail_cooldown_sec": int(os.getenv("GHA_SRC_FAIL_COOLDOWN", "600")),
}


def _read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                      # noqa: BLE001
        return default


def _write_json_atomic(path, obj):
    tmp = path + ".tmp.%d" % os.getpid()
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def _write_budget_guarded(d):
    """写预算文件前，禁止用「空计数」覆盖「有计数」的同日数据，并强制日上限。

    2026-11-13 事故 1：观测到 .gha_src_budget.json 所有源的 day 在同一秒被清成 0
    （此前 bangumi=301 / ct.cdn=297 …），runner_report 因此报出「配额 1000000」。
    成因是读-改-写在跨进程下不互斥时，某个进程拿到了 _empty_budget()（跨日分支
    或读到损坏文件），随后把自己的空结构整体写回，覆盖了别的进程刚记的计数。
    配额是硬顶，被清零等于当天可以无限请求源站 —— 这条必须挡住。

    2026-11-13 事故 2（连续跑三次任务时发现）：emby.bangumi.ca 与
    goemby.entertang.work 都出现了 day=301，**超出上限 1**。原因是两个进程
    同时读到 299，各自 +1 后分别写回 300 和 301 —— 典型的读-改-写竞态。
    模块顶部早就 import 了 fcntl 且定义了 BUDGET_LOCK，但本函数从没用过。

    注意：所有调用方（charge_guard / record_source_* / eligible_sources）
    已经在 `with _budget_lock():` 内，**不能在这里再开一个 fd 加 LOCK_EX** ——
    实测同进程内对同一文件用两个 fd 加 LOCK_EX 会阻塞，直接死锁。
    所以这里用 _BUDGET_LOCK_DEPTH 计数：已在锁内就只做裁剪+落盘，不再加锁；
    被单独调用（深度 0）时自己加锁，保证互斥。

    规则：
      1) 同日且新计数总和 < 现有总和 → 拒绝覆盖，合并保留较大值
      2) 任何源的 day 超过其日上限 → 压回上限（附 budget_clip 标记，便于审计）
    """
    limit_default = int(DEFAULT_BUDGET["daily"])
    if _BUDGET_LOCK_DEPTH > 0:
        _budget_merge_and_clip(d, limit_default)
        return
    with _budget_lock():
        _budget_merge_and_clip(d, limit_default)


def _budget_merge_and_clip(d, limit_default):
    try:
        cur = _read_json(BUDGET_FILE, None)
        if isinstance(cur, dict) and cur.get("date") == d.get("date"):
            cur_sum = sum(int((v or {}).get("day") or 0) for v in (cur.get("sources") or {}).values())
            new_sum = sum(int((v or {}).get("day") or 0) for v in (d.get("sources") or {}).values())
            if new_sum < cur_sum:
                # 合并：保留两边较大的 day，events 取并集（去重）
                merged = dict(d)
                msrc = dict(d.get("sources") or {})
                for lab, crec in (cur.get("sources") or {}).items():
                    nrec = dict(msrc.get(lab) or {})
                    cday = int((crec or {}).get("day") or 0)
                    nday = int(nrec.get("day") or 0)
                    if cday > nday:
                        nrec["day"] = cday
                        ev = list(set((crec or {}).get("events") or []) |
                                  set(nrec.get("events") or []))
                        nrec["events"] = sorted(ev)
                    for k in ("download_fails", "fails"):
                        if int((crec or {}).get(k) or 0) > int(nrec.get(k) or 0):
                            nrec[k] = int(crec.get(k) or 0)
                    for k in ("cooldown_until", "download_cooldown_until"):
                        if float((crec or {}).get(k) or 0) > float(nrec.get(k) or 0):
                            nrec[k] = float(crec.get(k) or 0)
                    msrc[lab] = nrec
                merged["sources"] = msrc
                d = merged
    except Exception:                                          # noqa: BLE001
        pass

    # 硬裁剪：任何源不得超过日上限
    clipped = {}
    src = dict(d.get("sources") or {})
    for lab, rec in list(src.items()):
        if not isinstance(rec, dict):
            continue
        cap = int(rec.get("limit") or limit_default)
        used = int(rec.get("day") or 0)
        if used > cap:
            nrec = dict(rec)
            nrec["budget_clip"] = {"from": used, "to": cap,
                                   "at": time.strftime("%Y-%m-%d %H:%M:%S")}
            nrec["day"] = cap
            src[lab] = nrec            # 必须回写，否则裁剪不生效
            clipped[lab] = used
    if clipped:
        d = dict(d)
        d["sources"] = src
        d["clipped"] = clipped
        d["clipped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    _write_json_atomic(BUDGET_FILE, d)


def claim_path(lane):
    return os.path.join(CLAIM_DIR, lane + ".json")


# ── 条目级 claim（2026-11-20）────────────────────────────────────────────
# 用户口径：「多来一个人任务也多一点，少一个人就少做点事」。
# 原来 claim 是「每 lane 一个文件」，同 lane 只能一个节点干活 —— 加节点不会加吞吐。
# 这里把锁粒度降到「一集」：谁抢到 key 谁干，抢不到换下一集。

ITEM_TTL = int(os.environ.get("JOB_CLAIM_TTL") or 14400)


def _claim_db():
    con = sqlite3.connect(os.path.join(HERE, "tg.db"), timeout=30)
    con.execute("create table if not exists job_claim("
                "key text primary key, worker text, ts integer, ttl integer)")
    return con


# ── worker 健康度（2026-11-20）───────────────────────────────────────────
# 失败要归因，不能一律算到 worker 头上：
#   源站没资源/404  → 供给问题，不影响 worker 评价
#   FloodWait/鉴权  → session 问题，换号即可，不是机器坏
#   超时/网络/中断   → worker 问题，连续出现就该退出有效产能

_SOURCE_ERRS = ("no_resource", "not_found", "404", "source_down", "no_candidate",
                "no_hd", "empty", "gap")
_AUTH_ERRS = ("flood", "authkey", "unauthorized", "session", "banned", "deleted")


def classify_failure(reason):
    r = str(reason or "").lower()
    if any(k in r for k in _AUTH_ERRS):
        return "auth"
    if any(k in r for k in _SOURCE_ERRS):
        return "source"
    return "worker"


def worker_id(run_id=None, explicit=None):
    import re as _re
    if explicit:
        return str(explicit)
    m = _re.search(r"slot(\d+)", str(run_id or ""))
    return "slot%s" % m.group(1) if m else "local"


def record_outcome(run_id, ok=True, reason=None, explicit=None):
    """记一次结果。异常绝不外抛——统计不能影响主链路。"""
    try:
        w = worker_id(run_id, explicit)
        kind = "ok" if ok else classify_failure(reason)
        con = _claim_db()
        con.execute("create table if not exists worker_stat("
                    "worker text primary key, ok integer default 0, "
                    "fail_worker integer default 0, fail_source integer default 0, "
                    "fail_auth integer default 0, consec_fail integer default 0, "
                    "last_ts integer, last_reason text)")
        con.execute("insert or ignore into worker_stat(worker) values(?)", (w,))
        if kind == "ok":
            con.execute("update worker_stat set ok=ok+1, consec_fail=0, "
                        "last_ts=?, last_reason='' where worker=?", (int(time.time()), w))
        else:
            # 只有 worker 类失败累计「连续失败」；源站无资源/风控不连坐，
            # 否则一个坏源就能把健康机器踢出产能。
            _consec = "consec_fail=consec_fail+1, " if kind == "worker" else ""
            con.execute("update worker_stat set fail_%s=fail_%s+1, %slast_ts=?, "
                        "last_reason=? where worker=?" % (kind, kind, _consec),
                        (int(time.time()), str(reason)[:80], w))
        con.commit()
        return kind
    except Exception:                                          # noqa: BLE001
        return None


def worker_health(min_rate=0.5, consec_limit=3, min_samples=3):
    """返回 {worker: {rate, consec, healthy, ...}}。

    min_samples 之前一律视为健康（乐观启动）——新机器不该因为样本少被判死。
    只有 worker 类失败计入成功率；source/auth 失败不拉低评价。
    """
    out = {}
    try:
        con = _claim_db()
        con.execute("create table if not exists worker_stat("
                    "worker text primary key, ok integer default 0, "
                    "fail_worker integer default 0, fail_source integer default 0, "
                    "fail_auth integer default 0, consec_fail integer default 0, "
                    "last_ts integer, last_reason text)")
        rows = con.execute("select worker, ok, fail_worker, fail_source, fail_auth, "
                           "consec_fail, last_ts, last_reason from worker_stat").fetchall()
        con.commit()
        con.close()
    except Exception:                                          # noqa: BLE001
        return out
    for w, ok, fw, fs, fa, cf, ts, why in rows:
        judged = int(ok or 0) + int(fw or 0)
        rate = (int(ok or 0) / judged) if judged else 1.0
        healthy = True
        if judged >= min_samples and rate < min_rate:
            healthy = False
        if int(cf or 0) >= consec_limit:
            healthy = False
        out[w] = {"ok": int(ok or 0), "fail_worker": int(fw or 0),
                  "fail_source": int(fs or 0), "fail_auth": int(fa or 0),
                  "rate": round(rate, 3), "consec": int(cf or 0),
                  "healthy": healthy, "last_reason": why or "", "last_ts": ts}
    return out


def item_key(job):
    """条目级 claim 的唯一 key = 剧集身份（不含画质）。

    2026-11-20：原来直接拿 job["key"]（state key，形如
    `tv|id:248086|(1, 8)|1080p`）当 claim key —— 画质不同就是不同 key，
    两个 worker 可能同时下载同一集的 1080p 和 2160p，白烧两份配额。
    """
    raw = str(job.get("key") or job.get("target_rel") or "")
    parts = raw.split("|")
    if len(parts) >= 4:                     # tv|id:xxx|(S, E)|quality
        raw = "|".join(parts[:3])
    return "%s|S%sE%s" % (raw, int(job.get("season") or 0), int(job.get("ep") or 0))


def claim_item(key, worker, ttl=None):
    """抢一集。返回 True 表示抢到；别人已抢且未过期返回 False。"""
    if not key:
        return True
    ttl = int(ttl or ITEM_TTL)
    now = int(time.time())
    con = _claim_db()
    try:
        con.execute("delete from job_claim where ts + ttl < ?", (now,))
        cur = con.execute("insert or ignore into job_claim(key, worker, ts, ttl) "
                          "values(?,?,?,?)", (key, worker, now, ttl))
        con.commit()
        if cur.rowcount:
            return True
        row = con.execute("select worker from job_claim where key=?", (key,)).fetchone()
        return bool(row and row[0] == worker)
    finally:
        con.close()


def release_item(key, worker=None):
    if not key:
        return 0
    con = _claim_db()
    try:
        if worker:
            n = con.execute("delete from job_claim where key=? and worker=?",
                            (key, worker)).rowcount
        else:
            n = con.execute("delete from job_claim where key=?", (key,)).rowcount
        con.commit()
        return n
    finally:
        con.close()


def reap_items():
    con = _claim_db()
    try:
        n = con.execute("delete from job_claim where ts + ttl < ?",
                        (int(time.time()),)).rowcount
        con.commit()
        return n
    finally:
        con.close()


def active_claim(path):
    d = _read_json(path, None)
    if not isinstance(d, dict):
        return None
    if d.get("status") != "running":
        return None
    if time.time() - float(d.get("ts") or 0) >= CLAIM_TTL:
        return None
    return d


def read_claim(lane):
    return active_claim(claim_path(lane))


_JOB_ALIASES = {
    "tmdb": ("tmdb", "tmdb_id", "tmdbid", "tmdbid"),
    "series": ("series", "series_name", "name", "title"),
    "season": ("season", "s"),
    "ep": ("ep", "episode", "e"),
    "target_rel": ("target_rel", "target", "rel", "path"),
    "filename": ("filename", "file", "basename"),
    "folders": ("folders", "folder", "dir"),
    "year": ("year",),
    "url": ("url", "src", "source_url", "download_url"),
    "video_id": ("video_id", "vid", "show_id"),
    "lib": ("lib", "library", "lib_name"),
    "source_label": ("source_label", "source", "src_label"),
}
_SE2 = re.compile(r"[Ss](\d{1,2})\s*[Ee](\d{1,4})")


def norm_job(j):
    """把三套队列 schema 归一成一套字段（2026-11-23）。

    现状是三份「待办」用三种结构表达同一件事，必然出错：
        fill 队列   {tmdb, series, missing[]}
        new  队列   {tmdb, video_id, lib, target_quality}
        目录同步    {tmdb, series, year, source}
    GHA 那次 ``no_candidate`` 就是生产者与消费者字段名不一致导致的 ——
    补进来的目录条目缺 lane 期望的 video_id/url。

    这里做一层归一：任何生产者只要语义对，字段叫什么都行。
    """
    if not isinstance(j, dict):
        return j
    out = dict(j)
    for canon, alts in _JOB_ALIASES.items():
        if out.get(canon) in (None, "", [], {}):
            for a in alts:
                v = j.get(a)
                if v not in (None, "", [], {}):
                    out[canon] = v
                    break
    # 季集从名字里补（new 队列只有 series 名，没有 season/ep）
    for field in ("series", "filename", "target_rel"):
        v = out.get(field)
        if isinstance(v, str):
            m = _SE2.search(v)
            if m:
                if out.get("season") in (None, "", 0):
                    out["season"] = int(m.group(1))
                if out.get("ep") in (None, "", 0):
                    out["ep"] = int(m.group(2))
                break
    return out


def landed_at_of(rel):
    """某条已入库路径的落地时间（epoch）；查不到返回 0（2026-11-23）。

    用途：upgrade lane 的「刚入库先别洗」闸门 —— 今天 19/19 次洗版都是
    3 天内刚入库的集，等于先落 1080p 再回头重下 4K。等 48 小时再洗，
    4K 批次也齐了，一次能洗多集，少下无效流量。
    """
    if not rel:
        return 0.0
    try:
        con = sqlite3.connect(os.path.join(HERE, "tg.db"), timeout=20)
        try:
            row = con.execute("select landed_at from resource_state where rel=? limit 1",
                              (str(rel),)).fetchone()
        finally:
            con.close()
        return float(row[0]) if row and row[0] else 0.0
    except Exception:                                       # noqa: BLE001
        return 0.0


def source_success_rate():
    """源站命中率 {label: ok/day}，用于把好源排到前面（2026-11-23）。

    今天实测：emos 2/300（0.7%）、bangumi 74/287（26%）。而 new 队列里
    emos 有 1050 条、bangumi 只有 268 条 —— 按队列顺序试，等于把有限的
    重试次数全烧在死源上，最后报 no_job。**顺序也是产能。**
    """
    try:
        with open(os.path.join(HERE, ".gha_src_budget.json"), encoding="utf-8") as fh:
            d = json.load(fh)
    except Exception:                                       # noqa: BLE001
        return {}
    out = {}
    for name, rec in (d.get("sources") or {}).items():
        if not isinstance(rec, dict):
            continue
        day = float(rec.get("day") or 0)
        ok = float(rec.get("ok") or 0)
        out[str(name)] = (ok / day) if day else 0.5
    return out


def read_claims(include_legacy=True):
    """返回所有活跃 claim —— **以 tg.db.job_claim 为唯一真源**。

    2026-11-20：原来按 LANES 逐个 read_claim(lane)，只认 <lane>.json，
    改成 cap 文件后一个新文件都看不到 —— 改为 glob 全部。

    2026-11-23（架构修正）：此前「容量判定读 .gha_claims/*.json、互斥判定读
    job_claim」两套状态并存。两边不一致时，删一边另一边仍生效 —— 本会话已踩：
    删了 DB 行容量没释放，因为文件还在。现在反过来：
      · DB 决定「有没有这条 claim」（能不能占容量）
      · 文件只提供可读元数据（series / source_label / run_id），
        用 worker + 时间戳 ±180s 与 DB 行配对
      · 配不上的文件直接忽略，不再虚占产能
    """
    import glob as _glob
    # 先收集文件里的可读元数据
    files = []
    for path in sorted(_glob.glob(os.path.join(CLAIM_DIR, "*.json"))):
        d = active_claim(path)
        if not d:
            continue
        d.setdefault("lane", os.path.basename(path).split(".")[0])
        files.append(d)

    rows = []
    try:
        con = _claim_db()
        rows = con.execute(
            "select key, worker, ts, ttl from job_claim where ts+ttl>=?",
            (int(time.time()),)).fetchall()
    except Exception:                                       # noqa: BLE001
        rows = []

    if not rows:
        # DB 不可用（或真的空）时才退回文件，避免整体瞎掉
        return files

    out = []
    used = set()
    for key, worker, ts, ttl in rows:
        best, best_dt = None, 1e9
        for i, d in enumerate(files):
            if i in used:
                continue
            dw = str(d.get("worker") or "")
            if dw and worker and dw.split(":")[-1] != str(worker).split(":")[-1]:
                continue
            dt = abs(float(d.get("ts") or ts) - float(ts or 0))
            if dt < best_dt and dt <= 180:
                best, best_dt = i, dt
        if best is not None:
            used.add(best)
            item = dict(files[best])
        else:
            item = {"lane": str(key).split("|")[0], "run_id": None}
        item.update({"key": key, "worker": worker, "ts": ts, "ttl": ttl})
        out.append(item)
    return out


def find_claim(run_id, include_failed=False):
    """Return (path, job) for a claim with this run_id/run_key.

    ``gha_finish`` must be able to resume a claim it marked ``failed`` after a
    transient registration/scraping error.  Keep the default active-only
    behaviour for every other caller.
    """
    # 2026-11-20：claim 已按 run 分文件，必须 glob 全部，否则新文件找不到。
    import glob as _glob
    paths = sorted(_glob.glob(os.path.join(CLAIM_DIR, "*.json"))) + [LEGACY_CLAIM]
    now = time.time()
    for path in paths:
        d = _read_json(path, None)
        if not isinstance(d, dict) or str(d.get("run_id")) != str(run_id):
            continue
        status = d.get("status")
        if status == "running":
            if now - float(d.get("ts") or 0) < CLAIM_TTL:
                return path, d
            continue
        if include_failed and status == "failed":
            if now - float(d.get("ts") or 0) < CLAIM_TTL:
                return path, d
    return None, None


def claim_path_for(lane, job):
    """每 run 一个 claim 文件：<lane>.<run_id>.json

    2026-11-20：原来 <lane>.json 一个 lane 只有一个文件，两个 worker 同跑同 lane
    会互相覆盖，gha_finish 按 run_id 找 claim 时会串到别人的任务上。
    """
    rid = str(job.get("run_id") or job.get("job_id") or "anon")
    rid = "".join(ch for ch in rid if ch.isalnum() or ch in "-_")[:48]
    return os.path.join(CLAIM_DIR, "%s.%s.json" % (lane, rid))


def write_claim(lane, job):
    os.makedirs(CLAIM_DIR, mode=0o700, exist_ok=True)
    _write_json_atomic(claim_path_for(lane, job), job)


def save_claim(path, job):
    _write_json_atomic(path, job)


def release_claim(path):
    try:
        os.remove(path)
        return True
    except FileNotFoundError:
        return False


def leased_sources(claims):
    return {str(d.get("source_label")) for d in claims if d.get("source_label")}


def leased_source_load(claims):
    """每个源当前被多少个 claim 占用（软租约用）。

    2026-11-13：租约原来是「整源独占」—— 只要一个源被某个 claim 用了，
    eligible_sources() 就把它整个排除。实测后果：8 个源里只有 3 个能产出
    （其余日配额打满或高失败率），而这 3 个全被 3 个活动 claim 独占，
    于是第 4/5 台机器永远 NOJOB，五路产能实际只有三路。
    源站配额是按「请求次数」计费的，不是按并发数，所以同一源并发使用
    不会多花配额；真正要防的只是把源站打到限流。
    改成软租约：已占用的源排到最后，只有在没别的选择时才复用。
    """
    load = {}
    for d in claims:
        lab = str(d.get("source_label") or "")
        if lab:
            load[lab] = load.get(lab, 0) + 1
    return load


_BUDGET_LOCK_DEPTH = 0


@contextlib.contextmanager
def _budget_lock():
    """跨进程互斥锁，并维护进程内重入深度。

    2026-11-13：_write_budget_guarded 需要知道自己是否已在锁内。
    实测同一进程内对同一文件用两个 fd 加 LOCK_EX 会阻塞（不是可重入锁），
    所以必须用深度计数，而不是在写函数里无条件再加一次锁。
    """
    global _BUDGET_LOCK_DEPTH
    os.makedirs(os.path.dirname(BUDGET_LOCK), exist_ok=True)
    fd = os.open(BUDGET_LOCK, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        _BUDGET_LOCK_DEPTH += 1
        try:
            yield
        finally:
            _BUDGET_LOCK_DEPTH -= 1
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _today():
    # 日预算按 **北京时间** 换日（2026-11-13）：
    # 原来用 time.strftime("%F") 取宿主机本地日期，而 runner/queue 脚本跑在 UTC 语境，
    # 结果每天 08:00 CST 才换日，北京时间 00:00–08:00 一直在用前一天的配额
    # （表现为「上午就显示 day=300 已打满」，把可用配额人为压缩 1/3）。
    # 环境变量 GHA_BUDGET_TZ_OFFSET 可覆盖（秒，默认 +8h）。
    off = int(os.getenv("GHA_BUDGET_TZ_OFFSET", str(8 * 3600)))
    return time.strftime("%F", time.gmtime(time.time() + off))


def _empty_budget():
    return {"version": 1, "date": _today(), "rr": 0,
            "limits": dict(DEFAULT_BUDGET), "sources": {}}


# ---------- 静态源配置（E144） ----------
# 病根（2026-11-16 06:xx 自省）：`disabled` / `daily_cap` 这类**配置**和当天计数
# 存在同一个 `.gha_src_budget.json` 里，而 `_load_budget_unlocked()` 跨日直接
# `return _empty_budget()` —— 于是每天北京时间 00:00 把配置一起清掉。
#
# 当天实测代价：video.emos.best 的 `disabled` 在 00:00 被清空 → 08:00 前
# 又烧满 300 次配额、ok=0（E110 早已判定它是魔改客户端，/Items 全 404）；
# emby.bangumi.ca / ct.cdn.bgp.yt 的 `daily_cap=0`（E069 实测无限制）同样丢失，
# 被降级成礼貌值 300 —— 最好的源反而被自己的兜底限制掐住。
#
# 修法分两层：
#   ① 配置搬到 git 跟踪的 `config/sources_static.json`，任何一次读预算都会合并它，
#      运行时文件被删/被日切/被写坏都不会丢配置；
#   ② 日切时保留旧记录里所有**非当天计数**字段（按排除法，加新静态字段不用改代码）。
STATIC_CONF = os.path.join(os.path.dirname(HERE), "config", "sources_static.json")

# 每个源记录里属于「当天计数/瞬时状态」的键。除此之外的键一律视为静态配置，跨日保留。
_DAILY_REC_KEYS = frozenset((
    "day", "events", "fails", "cooldown_until", "download_fails",
    "download_cooldown_until", "last", "ok", "budget_clip",
    "last_download_error", "ok_at",
))
# 跨日重建时显式写回的零值，保证下游（runner_report / quota_history 等直接读文件的地方）
# 拿到的形状与 `_source_record()` 一致，不用各自写 `.get() or 0`。
# 用函数而不是常量字典：dict 里的 `events` 是 list，共享同一个对象会被下游写坏。
def _daily_reset():
    return {"day": 0, "events": [], "fails": 0, "cooldown_until": 0,
            "download_fails": 0, "download_cooldown_until": 0,
            "last": 0, "ok": 0}


def static_source_conf():
    """读 `config/sources_static.json` 的 sources 段（坏文件/缺文件返回 {}）。"""
    d = _read_json(STATIC_CONF, None)
    src = (d or {}).get("sources") if isinstance(d, dict) else None
    return src if isinstance(src, dict) else {}


def _apply_static(d):
    """把静态配置合并进预算结构：静态优先，且**不许覆盖当天计数**。

    例外（E181）：`cap_mode: "auto"` 的源，静态**不得覆盖 `daily_cap`**。
    根因：`src_budget_rebalance.py` 按实测命中率写进运行时文件的额度，
    每次读预算都会被静态里的 `daily_cap: 0`（= 不限）盖回去 —— 于是
    「零产出的源被压到 20」这件事从来没真正生效过，2026-11-18 两个源
    在「不限」状态下各烧 166/156 次、产出 0。自动额度 = 静态只声明模式，
    具体数值归运行时（learned）。
    """
    if not isinstance(d.get("sources"), dict):
        d["sources"] = {}
    for label, conf in static_source_conf().items():
        if not isinstance(conf, dict):
            continue
        rec = d["sources"].setdefault(str(label), {})
        auto_cap = str(conf.get("cap_mode") or "").strip().lower() == "auto"
        for k, v in conf.items():
            if k in _DAILY_REC_KEYS:
                continue                   # 配置写错成 day/ok 时忽略，别污染计数
            if k == "daily_cap" and auto_cap:
                continue                   # 自动额度：数值由 rebalance 学习，静态不覆盖
            rec[k] = v
    return d


def _rollover_budget(old):
    """跨日：计数归零，但**配置必须原样带过来**（E144）。

    · limits：运维手工调过的全局阈值跨日保留。
    · sources[*]：只保留非 `_DAILY_REC_KEYS` 的字段（disabled / daily_cap / note …）。
    """
    new = _empty_budget()
    if isinstance(old, dict):
        lim = old.get("limits")
        if isinstance(lim, dict):
            new["limits"].update({k: v for k, v in lim.items() if k in DEFAULT_BUDGET})
        for label, rec in (old.get("sources") or {}).items():
            if not isinstance(rec, dict):
                continue
            keep = {k: v for k, v in rec.items() if k not in _DAILY_REC_KEYS}
            new["sources"][str(label)] = dict(_daily_reset(), **keep)
    return _apply_static(new)


def _load_budget_unlocked():
    d = _read_json(BUDGET_FILE, None)
    if not isinstance(d, dict) or d.get("date") != _today():
        return _rollover_budget(d if isinstance(d, dict) else None)
    d.setdefault("version", 1)
    d.setdefault("rr", 0)
    limits = dict(DEFAULT_BUDGET)
    limits.update(d.get("limits") or {})
    d["limits"] = limits
    d.setdefault("sources", {})
    return _apply_static(d)


def _source_record(d, label, now):
    rec = d["sources"].setdefault(label, {})
    rec.setdefault("day", 0)
    rec.setdefault("events", [])
    rec.setdefault("fails", 0)
    rec.setdefault("cooldown_until", 0)
    rec.setdefault("download_fails", 0)
    rec.setdefault("download_cooldown_until", 0)
    rec.setdefault("last", 0)
    # 2026-11-13：产能归因。以前只记"打了多少枪"（day=hits）和"失败几次"，
    # **从不记"打中多少"**，于是"sources 里谁在白烧配额"永远只能靠感觉判断。
    # ok = 当天成功出片数（与 day 同生命周期，日切随预算整体重置）。
    rec.setdefault("ok", 0)
    rec["events"] = [float(x) for x in rec.get("events") or []
                     if now - float(x) < BURST_WINDOW]
    return rec


def _source_yield_penalty(rec, now):
    """返回该源今日应按多大比例打折使用配额（0.0-1.0，1.0=不打折）。

    2026-11-13：4 个源日配额打满，但 emby.chapanda.site 与 ibukifuko.tech
    各烧掉 300 次却入库 0 集（last_download_error = source_too_slow /
    source_preflight_not_video）。配额是硬顶，被零产出源吃掉就等于直接减产。
    这里按「下载失败次数」动态降权：失败越多，允许用的日配额越少。
      download_fails 0     → 100%
      download_fails 1-2   → 75%
      download_fails 3-5   → 50%
      download_fails 6-9   → 30%
      download_fails >=10  → 15%（基本只留探测用）
    次日 download_fails 归零，权重自动恢复 —— 不引入人工干预。
    """
    f = int(rec.get("download_fails") or 0)
    if f <= 0:
        return 1.0
    if f <= 2:
        return 0.75
    if f <= 5:
        return 0.5
    if f <= 9:
        return 0.4    # 2026-11-14：原为 0.3，但下一档为 0.35 —— 失败更多反而额度更高。
                     # 由回归测试 test_penalty_is_monotonic 抓到的。
    # 2026-11-14：最低档从 0.15 提到 **0.35**。
    # 0.15 意味着 eff_cap=45；而一旦碰到 eff_cap，
    # 这个源就再也没机会下载、也就永远不会成功、
    # download_fails 永远不会被 record_source_success 清零 ——
    # **一个自封的死锁**。实测 emby.chapanda.site day=67/ok=0/dl_fails=10
    # 被锁死整天，而它占队列 45%（2688 条）。
    # 保留一份探针额度，让它能靠自己的成功走出来。
    return 0.35


def _eff_cap_of(rec, limits, now):
    """单个源今天允许的请求上限。**唯一的额度计算入口**（两处调用必须一致）。

    三档语义（`rec["daily_cap"]`，写在 `.gha_src_budget.json` 的 sources[label] 里）：

    | daily_cap | 含义 | 适用 |
    |---|---|---|
    | 未声明 | 全局 daily × 惩罚 × 加成，硬顶 400 | 默认（礼貌值，防静默封号） |
    | N > 0  | 固定 N，**不受惩罚回收** | 源站有限流但阈值已知更高 |
    | 0      | 不限（仍受 burst_20m 与 1.6s 间隔约束） | 源站本身无请求限制 |

    为什么"不限"也只放日额度、不放频率：`SOURCE_SERVERS.md` 定的
    「单源并发=1、间隔≥1.6s、burst 窗口」才是防封的核心，那是**频率**规则；
    日额度只是**总量**礼貌值。源站既然不限总量，就没必要自己卡自己。

    `emby.chapanda.site` 的教训：它被封是因为**旧 UA**（浏览器串），
    不是频率 —— 所以解封后恢复正常额度即可，不必额外设限。

    2026-11-14（E104）：**固定 `daily_cap` 不得绕过下载失败惩罚。**
    原来的写法 `if per > 0: return per` 让 daily_cap 成为后门：
    chapanda 的注记写着「下载永远 source_too_slow，只用于元数据校验」，
    却带着 `daily_cap: 40` —— 惩罚系数 0.35 被整个跳过，它每天照样拿 40 次
    去下必然失败的文件，每次领活吃掉一个 claim 和几十分钟。
    实测当天它 `download_fails=20 / ok=0`，占掉了 lane 大量空转时间。

    正确语义分两件事：
      - `daily_cap` 是**礼貌值**（怕被封号）→ 可以覆盖默认的日总量。
      - `_source_yield_penalty` 是**它做不出活的证据**（下不动 / 不是视频）
        → 必须照样打折。一个明天下不动、今天却给了全额的源，
        是在拿唯一的硬资源（claim + lane 时间）去换零产出。
    因此：N>0 → `min(N, N × penalty)`；0（不限）同理受惩罚约束，
    只有 penalty=1.0（近期零失败）时才真正不限。
    """
    per = rec.get("daily_cap")
    pen = _source_yield_penalty(rec, now)
    if per is not None:
        per = int(per)
        if per == 0:
            # 「不限」= 不设礼貌上限，但仍受失败率约束（E104）。
            # 2026-11-21 回归：原来这里直接 return 1e9，把 pen 整个跳过 ——
            # `test_unlimited_cap_still_penalised_when_failing` 当场变红。
            # 语义只能是：**近期零失败**（pen=1.0）才真正不限；
            # 有下载失败证据的源退回「默认日额度 × 惩罚」，用有限额度约束它。
            if pen >= 1.0:
                return 10 ** 9
            return max(1, int(int(limits["daily"]) * pen))
        if per > 0:
            return max(1, int(per * pen)) if pen < 1.0 else per
    return min(400, int(int(limits["daily"]) * pen
                        * _source_yield_bonus(rec, now)))


def _source_yield_bonus(rec, now):
    """高产出源的额度加成（1.0-1.5）。

    2026-11-14：配额模型只有惩罚没有奖励 ——
    39.96.113.82-mirror 今日 ok=32（命中率 10.7%，全部源里最高），
    却和零产出源一样被卡在 300 次，08:16 就打满。
    队伍剩下 270 次额度，而能花的源全没了条目。

    **惩罚只能减少浪费，不能发现优秀。** 只有惩罚的系统会把
    好源和坏源一起压平，最后变成"大家都平均地少干"。

    判据：day>=60（样本足够）且命中率>=5% → 1.5 倍。
    """
    day = int(rec.get("day") or 0)
    ok = int(rec.get("ok") or 0)
    if day >= 60 and ok and ok * 100.0 / day >= 5.0:
        return 1.5
    return 1.0


def _source_yield_tier(rec):
    """命中率档位：0 = 主力 / 1 = 一般 / 2 = 低效 / 3 = 今天一集没出。

    2026-11-25（自省）：原来排序里只有 `zero_yield` 一个**二值**（`ok == 0` 才
    算坏），于是 4.4% 命中率的 yuntv 与 99% 的 bangumi 在轮转里**完全等价** ——
    两个源都"有产出"，谁近 20 分钟用得少谁先上。实测 8 天：yuntv 780 次请求
    只出 34 集（22.9 次/集，全源平均 5.57），aaa 11.3s/次拿了 1597 次，
    而 99% 的产出压在 bangumi 一个源上。**命中率是"多少"，不是"有没有"。**

    分档依据（不是拍脑袋，是实测分布）：bangumi 196% / emos 11% 属主力；
    0 产出的 6 个源全部落在第 3 档。`day < 20` 一律 0 档 —— 新源必须先给
    探索位，否则永远没机会证明自己（E104 的自封死锁同一个教训）。
    """
    day = int(rec.get("day") or 0)
    ok = int(rec.get("ok") or 0)
    if day < 20:
        return 0
    if ok <= 0:
        return 3
    rate = ok / float(day)
    if rate >= 0.10:
        return 0
    if rate >= 0.03:
        return 1
    return 2


def eligible_sources(labels, leased=None, load=None):
    """Return a rotated, least-used source order allowed by the shared budget.

    ``leased`` 参数保留兼容：传进来的源视为「已被占用」，不再直接排除，
    而是排在最后（软租约）。见 leased_source_load() 的说明。
    ``load`` 是 {source: 当前占用数}，占用越多排越后。
    """
    labels = [str(x) for x in labels if x]
    leased = {str(x) for x in (leased or set()) if x}
    load = {str(k): int(v or 0) for k, v in (load or {}).items()}
    now = time.time()
    with _budget_lock():
        d = _load_budget_unlocked()
        limits = d["limits"]
        rows = []
        for label in labels:
            rec = _source_record(d, label, now)
            # 2026-11-15（E110）：**永久禁用的源。**
            # video.emos.best 是魔改客户端：只有 /Library/MediaFolders 能用，
            # 所有 /Items 内容接口全 404 —— 300 次请求 0 成功，
            # 队列里 1050 条全是废条目。它不会被"冷却"自动淘汰，
            # 因为失败计数会随时间衰减，于是反复回池、反复白烧。
            # disabled=true 是显式的永久出局，只能人工摘除。
            if rec.get("disabled"):
                continue
            if rec["cooldown_until"] > now or rec["download_cooldown_until"] > now:
                continue
            # 按下载失败率动态收紧该源今日允许的上限（配额硬顶别浪费在零产出源上）
            # 硬顶：加成后不超过 400/天。
            # 300 是"礼貌值"（实测全天零 429），但礼貌值上加倍
            # 必须有上界 —— 被静默封号是永久失去源，比少出几集代价大得多。
            #
            # 2026-11-14：**逐源覆盖**。有些源站自己不限流（用户实测
            # `emby.bangumi.ca` / `ct.cdn.bgp.yt` 无限制），对它们套 300/400
            # 的礼貌值纯属浪费。声明方式：`.gha_src_budget.json` 的
            # sources[label].daily_cap。三档语义见 `_eff_cap_of()`。
            # 2026-11-21 用户口径：**源站不设量子上限**，只要「不频繁请求」。
            # 原来的 300/400 日上限 + 20 分钟 burst 上限，会把整天配额用完后
            # 让所有 lane（含 GHA）一律 source_budget 空转。
            # 现在：日上限只作为「人工显式配置」生效（sources[label].daily_cap），
            # 未配置即不限；burst 检查放宽到同样的语义。
            _explicit_cap = ((d.get("sources") or {}).get(label) or {}).get("daily_cap")
            # daily_cap=0 的语义是「不限」（见 `_eff_cap_of` 三档表），
            # 不能被当成「上限 0」直接判死 —— 那会让显式放开的源一个请求都发不出去。
            if _explicit_cap is not None and int(_explicit_cap) > 0:
                if int(rec.get("day") or 0) >= int(_explicit_cap):
                    continue
            # 2026-11-26（自省）：**有失败证据的源，必须真的被自己的有效额度卡住。**
            #
            # 上面那段只卡「显式 daily_cap > 0」，而本项目所有源都声明
            # `daily_cap: 0`（= 不限量，2026-11-21 用户口径）。于是
            # `_eff_cap_of()` 按 download_fails 算出的惩罚额度**从来没被执行过**：
            # 它只在下面节流器里参与 `_head += max(0, cap - day)`，
            # 超额源贡献 0 余量之后**照样被返回、照样继续烧请求**。
            #
            # 实测 2026-11-26 09:00（`.gha_src_budget.json` + 15 天 quota_history）：
            #   ibukifuko day=245 ok=0 df=23 → pen=0.35 → cap=105（超了 140 次）
            #   ct.cdn    day=211 ok=0 df=54 → cap=105（超了 106 次）
            #   goemby    day=160 ok=0 df=26 → cap=105（超了  55 次）
            #   三家 + xqd/lite/yuntv 在 15 天台账里入库合计 **0 集**
            # 当天 2496 次请求里 ~1080 次落在「当天 0 产出」的源上 = 次/集 8.96 的主因。
            #
            # 语义边界（两条都不能破）：
            #   ① pen == 1.0（近期零下载失败）的源仍然是 10**9 —— 不限量，
            #      不违反「源站不设量子上限」；
            #   ② 惩罚下限 0.35 → 最低仍有 ~105 次/天探针额度，
            #      坏了能靠自己成功走出来（E104 自封死锁的教训）；
            #      次日 download_fails 归零，额度自动恢复。
            if int(rec.get("day") or 0) >= _eff_cap_of(rec, limits, now):
                continue
            _burst = int(limits.get("burst_20m") or 0)
            if _burst and _burst < 100000 and len(rec["events"]) >= _burst:
                continue
            # 软租约：被占用的源额外增加排序权重，但不禁用
            busy = (1 if label in leased else 0) + int(load.get(label) or 0)
            # 2026-11-14：「试过不少、一集也没出」的源排到最后。
            # 原来只按"用量最少"排序，于是 ok=0 的坏源（因为用得少）
            # 反而总被优先选中，整个队伍陪它一起空转。
            # 实测 emby.chapanda.site day=67/ok=0 却被选中，
            # 而 39.96.113.82-mirror day=185/ok=10 反而轮不到。
            # day>=20 才算"试过"，避免把新源（day=0,ok=0）误伤。
            #
            # 2026-11-25（自省）：二值升为**档位**（见 `_source_yield_tier`），
            # 并把实测延迟作为同档内次序。原因见该函数 docstring：
            # 4.4% 与 99% 命中率被当等价，8 天白烧 ~745 次请求。
            tier = _source_yield_tier(rec)
            slow = 1 if float(rec.get("lat_ms") or 0) >= SRC_SLOW_MS else 0
            rows.append((tier, busy, slow, len(rec["events"]),
                         int(rec.get("day") or 0),
                         float(rec.get("last") or 0), label))
        rows.sort()
        labels_sorted = [x[-1] for x in rows]

        # 2026-11-15（E108）：**节流统计只算真正可用的源。**
        #
        # 实测 bug：eligible_sources(6 个源) 返回 []，但其中 4 个单独测都可用。
        # 根因是被封的源（yuntv download_fails=22、video.emos.best fails=10）
        # 已经在前面 continue 掉了，可下面的节流计算仍把它们的 events
        # 算进 recent_rate —— 它们的请求量拉高"近期速率"，触发
        # `recent_rate > allowed_rate * 2.5` → labels_sorted 清空 → 全部不放行。
        #
        # 一句话：**一个源被封，应该只损失它自己，不能拖累其他源。**
        _usable = [x[-1] for x in rows]        # 通过冷却/额度/突发过滤的源
        if not _usable:
            return []

        # ---- 全天节流器（2026-11-14，第二版）----
        # 第一版用「累计用量 vs 累计应花」，超速就停 —— 设计错了：
        # 停到半夜等于把剩下的额度白白浪费（实测 4 小时用了 854/2700，
        # 若一直停到 24 点，68% 的额度永远花不出去）。
        # 目标不是"花得均匀"，是**把额度全部花在有片的时候**。
        #
        # 正确做法是节流器：拿「剩余额度 ÷ 剩余小时」算出允许的速率，
        # 再和近期实际速率比：
        #   实际 ≤ 允许×1.2  → 全部源（正常探索）
        #   实际 ≤ 允许×2.0  → 只放 1 个源（降速）
        #   实际 >  允许×2.0 → 本轮不放行（严重超速才停，且下一轮就是 5 分钟后）
        # 这样既不会提前烧光，也不会把额度剩下来。
        used = sum(int(_source_record(d, l, now).get("day") or 0) for l in labels)
        caps = []
        for label in labels:
            rec = _source_record(d, label, now)
            caps.append(_eff_cap_of(rec, limits, now))
        total_cap = sum(caps) or 0
        if total_cap and labels_sorted:
            midnight = time.mktime(time.localtime()[:3] + (0, 0, 0, 0, 0, -1))
            elapsed_h = max(0.2, (now - midnight) / 3600.0)
            remain_h = max(0.2, 24.0 - elapsed_h)
            remain = max(0, total_cap - used)
            allow_rate = remain / remain_h          # 每小时允许花掉多少
            # 近期实际速率：用各源 events（BURST_WINDOW 内的请求时间戳）求和
            win = float(BURST_WINDOW or 1200)
            recent = []
            for l in labels:
                recent.extend(float(x) for x in (_source_record(d, l, now).get("events") or []))
            rate = len([x for x in recent if now - x <= win]) / (win / 3600.0)
            # 2026-11-14 第四版（定稿）：兜底 + 温和槽位跳过
            #
            # 前三版的教训：
            #   ① 超速就全停 → 额度锁死，68% 用不掉
            #   ② 按近期速率节流 → events 窗口滞后，停20分跑5分，振荡
            #   ③ 纯兜底 → lane 自带 sleep 给出 135 次/小时，但有效容量只有 1950，
            #      按此中午就烧光、晚上（日更剧上线）没额度
            #
            # 定稿：lane 的节奏是底盘，槽位跳过只用来把速率从 135 压到 ~70-80。
            #   n = clamp(round(used/target / 1.3), 2, 3)
            #   实测 used/target≈2.5 → n=2 → 每 2 个 5 分钟槽干一次 ≈ 67 次/小时，
            #   全天 ≈ 1600 次，既不会提前烧光，也不会剩一大截。
            # 跳过时仍只放 1 个源（单次成本最低）。
            # 2026-11-14（第七版，按**近期速率**限流）
            #
            # 前六版都在拿"累计用量 vs 累计应花"做判据。这个对比有个致命缺陷：
            # **前半夜的一次性扫描会永远拖着后半天的判断**。
            # 实测：06:42 已用 1503/1800，剩下的三百次要么十分钟内烧完、
            # 要么按旧公式一直被判"超速"而不放行 —— 两头都不对。
            #
            # 正确的问法是：**现在的速度，能不能把剩下的额度撑到午夜？**
            #   allowed_rate = 剩余额度 / 剩余小时
            #   recent_rate  = 最近 BURST_WINDOW 内的请求数 × (小时/窗口)
            #
            # 带死区（避免振荡）：
            #   recent <= allowed×1.2          → 正常，全部源
            #   allowed×1.2 < recent <= allowed×2.5 → 只给 1 个源
            #   recent > allowed×2.5          → 这一轮不给（lane 带抖动重试）
            #
            # **安全阀**：窗口内一次请求都没有 → 无条件放行。
            # 否则一旦公式算错，系统会彻底停摆而且不会自愈（今天已经踩过这个坑）。
            # 2026-11-14（第十二版）：**用各源余量之和，不用总和相减**。
            #
            # 实测：五个队列源 used=970、total_cap=970 → 判"used>=cap" → 全停。
            # 但分解开看：39.96 还剩 100 次（它 ok=32、cap 被奖励到 400），
            # 而 ct/chapanda/ibukifuko 各超了 20-55 次 —— **超额的源把余量吃掉了**。
            # "已用总和 >= 额度总和"是个假信号：它把"某个源还能干活"
            # 和"另一个源已经超了"混成一个数。
            # 每个源在上面的过滤里已经按自己的 eff_cap 卡过了，
            # 这里只需算"还有多少可用的余量"。
            _head = 0
            for _l in _usable:
                _r = _source_record(d, _l, now)
                _cap = _eff_cap_of(_r, limits, now)
                _head += max(0, _cap - int(_r.get("day") or 0))
            # 2026-11-20：**只算逐源正余量之和，不能再被 min(total_cap-used) 夹住。**
            # 超额源（cap=20 用了 53）在 total_cap-used 里贡献负数，把好源的余量吃掉：
            # 实测 6 源 used=445 / total_cap=447 → remain=2 → allowed_rate=0.13/h
            # → 两个还有余量的源（aaa 48、bangumi 115）也被全停，两条 lane 空转 5 小时。
            # 这正是第十二版注释里写的「只需算还有多少可用的余量」，但 min() 没删掉。
            remain = _head
            if _head <= 0:
                labels_sorted = []
            elif labels_sorted:
                remain_h = max(0.2, 24.0 - elapsed_h)
                allowed_rate = remain / remain_h
                win = float(BURST_WINDOW or 1200)
                recent = 0
                for l in _usable:
                    recent += len([x for x in (_source_record(d, l, now).get("events") or [])
                                   if now - float(x) <= win])
                if recent <= 0:
                    pass                                   # 安全阀：没动静就放行
                else:
                    recent_rate = recent * (3600.0 / win)
                    if allowed_rate > 0 and recent_rate > allowed_rate * 2.5:
                        labels_sorted = []
                    elif allowed_rate > 0 and recent_rate > allowed_rate * 1.2:
                        # 2026-11-14（第八版）：**放行时不再只给 1 个源**。
                        # 实测：放行那一轮只给 1 个源，而那个源的条目恰好全在冷却中
                        # → `no_job`，白花 7 次请求。节流的目的是**控制量**，
                        # 不是"只让一个源去碰运气"。每次放行都该能找到活，
                        # 否则剩下的额度会被一次次空放消耗完。
                        # 频率仍由窗口决定（一轮决策 = 一次机会），不会因此超速。
                        labels_sorted = labels_sorted[:3]

        if labels_sorted:
            rr = int(d.get("rr") or 0)
            # 2026-11-25（自省）：轮转**只能在同档位内**做，原来是整串 `k = rr % 3`。
            # 实测：3 个源时 `rr%3` 每三次就有两次把 0 产出的源转到第一位 ——
            # 排序里刚修好的分档被轮转原样抵消（yuntv 8 天 780 次请求就是这么来的）。
            # 正确做法：只把**首个档位组**轮转，后面的档位保持全部落在其后。
            head_tier = _source_yield_tier(_source_record(d, labels_sorted[0], now))
            same = 0
            for _lab in labels_sorted:
                if _source_yield_tier(_source_record(d, _lab, now)) != head_tier:
                    break
                same += 1
            head = labels_sorted[:max(1, same)]
            k = rr % len(head)
            labels_sorted = head[k:] + head[:k] + labels_sorted[len(head):]
            d["rr"] = rr + 1
            _write_budget_guarded(d)
        return labels_sorted


def charge_guard(guard):
    """Persist request counts/failures from one fresh src_guard.Guard session."""
    now = time.time()
    with _budget_lock():
        d = _load_budget_unlocked()
        limits = d["limits"]
        for label, st in getattr(guard, "states", {}).items():
            # 2026-11-14：预算按**源站真正应答过**的次数记，不按尝试次数。
            # 实测 2300 次尝试里 323 次是连接失败（status=0，DNS/超时/拒绝），
            # 源站应用层根本没收到请求，却照样扣掉当天配额 —— 白烧 14%。
            # 多镜像源更糟：一次逻辑 GET 会依次试多个 host，每个都 +1。
            # answered 只在拿到 HTTP 响应时 +1（404/500 也算应答，服务器确实处理了）。
            count = int(getattr(st, "answered", None)
                        if getattr(st, "answered", None) is not None
                        else getattr(st, "count", 0) or 0)
            fails_now = int(getattr(st, "fails", 0) or 0)
            oks_now = int(getattr(st, "ok", 0) or 0)
            if not count and not fails_now:
                continue
            rec = _source_record(d, label, now)
            if count:
                rec["day"] = int(rec.get("day") or 0) + count
                rec["events"].extend([now] * count)
                rec["last"] = now
            if fails_now:
                rec["fails"] = int(rec.get("fails") or 0) + fails_now
                if rec["fails"] >= 2:
                    rec["cooldown_until"] = max(
                        float(rec.get("cooldown_until") or 0),
                        now + int(limits["fail_cooldown_sec"]))
            elif oks_now:
                rec["fails"] = 0
                rec["cooldown_until"] = 0
            # 2026-11-25（自省）：每源实测延迟落一份 EMA，供 `eligible_sources`
            # 在同命中率档位内把慢源后置。`lat_ms` 不在 `_DAILY_REC_KEYS` 里，
            # 所以跨日保留 —— 延迟是源站属性，不是"今天的状态"，每天清零会让
            # 慢源在 00:00 之后重新排到最前面（与 E144 把 disabled 日切掉同一类错）。
            try:
                _ms = float(getattr(st, "avg_ms", 0) or 0)
            except (TypeError, ValueError):
                _ms = 0.0
            if _ms > 0:
                _old = float(rec.get("lat_ms") or 0)
                rec["lat_ms"] = round(_ms if _old <= 0 else _old * 0.7 + _ms * 0.3, 1)
            cool = float(getattr(st, "cool_until", 0) or 0)
            if cool > now:
                rec["cooldown_until"] = max(
                    float(rec.get("cooldown_until") or 0),
                    cool)
        _write_budget_guarded(d)


def record_source_failure(label, reason=""):
    if not label:
        return
    now = time.time()
    with _budget_lock():
        d = _load_budget_unlocked()
        rec = _source_record(d, str(label), now)
        # A completed media download is the only proof that a source can
        # actually deliver video bytes.  Metadata probes must not clear this
        # counter; otherwise every failed download is forgotten immediately
        # and the same broken source is selected again on the next run.
        rec["download_fails"] = int(rec.get("download_fails") or 0) + 1
        rec["last"] = now
        rec["last_download_error"] = str(reason or "")[:200]
        if rec["download_fails"] >= 2:
            rec["download_cooldown_until"] = max(
                float(rec.get("download_cooldown_until") or 0),
                now + int(d["limits"]["fail_cooldown_sec"]))
        _write_budget_guarded(d)


# ---------- 按集失败退避（E074） ----------
# 2026-11-14：`record_source_failure` 只冷却「源」（10 分钟），不记「集」。
# 于是出现死循环：《超市特工 S01E08》在源站是移动云盘 504（下不动），
# 但目标 Emby 缺这一集 → fill 模式每次都搜到它 → 每次失败 → 源冷却 10 分钟
# → 恢复后又搜到同一集 → 再失败。lane 被一集反复占住，Episode 计数 30 分钟不涨。
#
# 修法：按 (video_id) 记失败次数，达到阈值后冷却一整天。
# 只影响这一集，不影响该源的其他集，也不影响其他源。
EP_FAIL = os.path.join(HERE, ".gha_ep_fail.json")
EP_FAIL_TRIP = int(os.environ.get("EP_FAIL_TRIP") or 3)      # 连续失败几次后放长冷却
EP_FAIL_COOL_DAYS = float(os.environ.get("EP_FAIL_COOL_DAYS") or 1.0)   # 触发后的基准冷却
EP_FAIL_COOL_MAX_DAYS = float(os.environ.get("EP_FAIL_COOL_MAX_DAYS") or 30.0)
EP_FAIL_PRUNE_DAYS = float(os.environ.get("EP_FAIL_PRUNE_DAYS") or 90.0)
# 2026-11-19（自省）：冷却必须按**原因类别**分开。实测 `.gha_ep_fail.json` 81 条里
# 48 条原因是 `upload_failed` —— 那是**我们自己**的 TG 上传失败（重试就能好），
# 却被套上了「源站缺片」那套 1→2→4→8→30 天的指数退避：一次上传抖动就把这一集
# 判停 8~30 天，报表上还显示成「源站长期缺片」，于是改进清单一直指向「换源/标 DEAD」
# ——方向完全是错的。我方失败要短退避（防热循环即可），源站侧才用长退避。
EP_FAIL_OURS_BASE_MIN = float(os.environ.get("EP_FAIL_OURS_BASE_MIN") or 15.0)
EP_FAIL_OURS_MAX_H = float(os.environ.get("EP_FAIL_OURS_MAX_H") or 6.0)
# 我方失败重试到第 8 次仍不好 → 已经不是「抖动」而是坏件/永久拒绝，交回长退避。
# 没有这一档就变成另一种浪费：一集永远传不上去，却每 6 小时重下一次（≈4GB/天/集）。
EP_FAIL_OURS_HARD = int(os.environ.get("EP_FAIL_OURS_HARD") or 8)
# 2026-11-21（自省）：**「源站慢」不是「源站缺片」。** 实测 20 条在冷却的集里
# 10 条是 `source_too_slow`（下载 0.4~0.8MB/s 被速度闸掐断，文件明明在），
# 却被 `_EP_SRC_KEYS` 的 "source" 前缀命中 → 走 1/2/4/8/16/30 天长阶梯：
# 一集只因为**当时那个源慢**就被冻结最多 5.7 天（实测 fails=6 → 8251 分钟）。
# 缺片（`source_download_failed` / `no_valid_source`）才该长退避；
# 慢源该罚的是**源**（`record_source_failure` 已在做：download_fails 降额 +
# 10 分钟冷却），集本身换个源就能下。所以单列一档 `slow`：短阶梯 + 6h 封顶；
# 连续 EP_FAIL_SLOW_HARD 次都是慢，才算这份文件在所有源上都拿不到，交回长阶梯。
EP_FAIL_SLOW_BASE_MIN = float(os.environ.get("EP_FAIL_SLOW_BASE_MIN") or 60.0)
EP_FAIL_SLOW_HARD = int(os.environ.get("EP_FAIL_SLOW_HARD") or 6)
_EP_SLOW_KEYS = ("too_slow", "slow_download", "download_slow")
_EP_SRC_KEYS = ("source", "quality", "bad_source", "no_valid_source")
# 2026-11-21：sources_unavailable = 源站暂时不可用（429/超时/证书/连接），
# 不是「源站没有这一集」。必须走分钟级短退避，否则一次 429 就把整集冻 6 小时，
# 队列看起来「没活」，实际是我们在自己锁自己。
_EP_OURS_KEYS = ("upload", "manifest", "resume_scan", "claim", "slot", "session", "tg_",
                 "emby", "sources_unavailable")


def ep_fail_class(reason):
    """把失败原因分成三类，决定退避时长：

    - `src`  （源站侧、**能证明文件不在**）：长阶梯 1→2→4→8→16→30 天，少撞省配额。
    - `slow` （源站慢：文件在，只是这个源当下给不动）：短阶梯 1h→2h→4h（6h 封顶），
              换个源就能下；连续 EP_FAIL_SLOW_HARD 次才转回长阶梯。
    - `ours` （我方临时故障：上传/租约/会话/runner 退出）：15min 起翻倍，6h 封顶，
              连续 EP_FAIL_OURS_HARD 次仍不好才转回长阶梯。

    `slow` 必须排在 `src` 前面判：`source_too_slow` 同时含 "source" 与 "too_slow"。
    未知原因一律按 `src` 处理：宁可多等，也不要在原因不明时进入分钟级热循环。
    """
    r = str(reason or "").strip().lower()
    # 「源站暂时不可用」必须最先判：它同时含 "source"，会被 _EP_SRC_KEYS 抢先
    # 归到长阶梯（实测就是这样，改完仍是 src）。显式前置。
    if r.startswith("sources_unavailable"):
        return "ours"
    if any(k in r for k in _EP_SLOW_KEYS):
        return "slow"
    if r.startswith(_EP_SRC_KEYS) or any(k in r for k in _EP_SRC_KEYS):
        return "src"
    if r.startswith("runner") or any(k in r for k in _EP_OURS_KEYS):
        return "ours"
    return "src"


def ep_cool_seconds(n, reason=""):
    """第 n 次失败该冷却多少**秒**（按原因类别分档）。

    src  ：沿用 ep_cool_days 的指数阶梯（1/2/4/8/16/30 天）——源站真缺片，少撞省配额。
    slow ：60min 起、每次翻倍、封顶 6h —— 源站在、只是慢，换个源就能下；
           连续 EP_FAIL_SLOW_HARD 次仍下不动，说明所有源都拿不到这份文件，转回长阶梯。
    ours ：15min 起、每次翻倍、封顶 6h —— 我方失败重试就能好，长期封禁是纯损失；
           但连续 EP_FAIL_OURS_HARD 次仍失败说明是坏件/永久拒绝，转回长阶梯。
    """
    n = int(n or 1)
    cls = ep_fail_class(reason)
    if cls == "slow":
        # 连续 N 次都是「慢」→ 已经不是某个源慢，而是这份文件在哪儿都拿不到。
        # 交回长阶梯时从它的**基准档**起算（1 天），避免 4h 直接跳 8 天。
        if n >= EP_FAIL_SLOW_HARD:
            return ep_cool_days(EP_FAIL_TRIP + (n - EP_FAIL_SLOW_HARD)) * 86400
        k = max(0, n - EP_FAIL_TRIP)
        return min(EP_FAIL_SLOW_BASE_MIN * 60 * (2 ** k), EP_FAIL_OURS_MAX_H * 3600)
    if cls == "src" or n >= EP_FAIL_OURS_HARD:
        return ep_cool_days(n) * 86400
    k = max(0, n - EP_FAIL_TRIP)
    return min(EP_FAIL_OURS_BASE_MIN * 60 * (2 ** k), EP_FAIL_OURS_MAX_H * 3600)


def ep_cool_days(n):
    """第 n 次失败冷却多少天：3→1d，4→2d，5→4d，6→8d，7→16d，≥8→30d（封顶）。

    2026-11-18（自省）：原来是**恒定 1 天**。死集（源站元数据在、文件不在）于是
    每天被领一次、失败一次、再冷却一天，永远循环 —— 实测 21 集长期挂在这条链上，
    每集每天白烧一次请求，且永远等不到「源站补片」那天。
    指数退避把它压到 30 天一次；真有片了也能在下一个窗口领到（不是永久拉黑）。
    """
    k = max(0, int(n) - EP_FAIL_TRIP)
    return min(EP_FAIL_COOL_DAYS * (2 ** k), EP_FAIL_COOL_MAX_DAYS)


def _ep_fail_load():
    """读按集失败表，**顺手补齐缺失的 next_check**。

    2026-11-18：`next_check` 是这次加指数退避才有的字段，历史条目没有 →
    判活处 `now < rec.get("next_check") or 0` 会把它们当成「从没冷却过」，
    于是 52 条老死集在改动后立刻全部回锅重试一遍。这里按
    `last + ep_cool_days(fails)` 就地补，只在内存里补，写回由调用方决定。
    """
    try:
        d = json.load(open(EP_FAIL, encoding="utf-8"))
    except Exception:                                      # noqa: BLE001
        return {}
    for v in d.values():
        if isinstance(v, dict) and "next_check" not in v:
            last = float(v.get("last") or 0)
            if last:
                n = int(v.get("fails_cls") or v.get("fails") or 1)
                sec = ep_cool_seconds(n, v.get("reason") or "")
                v["cool_sec"] = sec
                v["cool_days"] = round(sec / 86400.0, 4)
                v["next_check"] = last + sec
    return d


# ---------- TG session 独占租约（E075） ----------
# 2026-11-14：上传报 `AuthKeyDuplicatedError: The authorization key (session file)
# was used under two different IP addresses simultaneously`。
#
# 病根不是"slot 分配算法不聪明"，而是**约束搞错了**：
#   claim 里存一个 tg_session_slot 就以为占住了 session，
#   但 slot 只是标签 —— 两台机器各自领到一个 claim，标签相同，
#   就真的同时用同一个 TG 账号从两个 IP 上传，TG 直接作废授权。
#
# 正确模型：session 是**跨机器的排他资源**，必须用租约显式独占，
# 用完（或超时）才还。N 个 session 最多支撑 N 台机器同时上传，
# 这是 TG 侧的硬约束，靠"算最小占用"绕不过去。
SLOT_DIR = os.path.join(HERE, ".gha_slots")
SLOT_TTL = 3 * 3600          # 上传最长 2h + 余量；超时视为进程已死，可被回收
# 2026-11-23：-- 但 TTL 只是兜底，不能当成"用完就自动放"的机制。
# 现在每个 run 结束都会 release_slot_by_run()，TTL 保留 3h 仅防进程被 kill -9
# 时把号永久锁死（实测：只租不放 → 3 次后全 no_session）。


def _slot_path(n):
    return os.path.join(SLOT_DIR, "slot%d.lock" % int(n))


# 2026-11-15（E106）：本机 runner 的 run_id 归类，**不能写死 slot 号**。
#
# 原来是 ("-slot1-", "-slot2-", "-slot4-")：本机加第三个 runner 实例
# （RUNNER_SLOT=5）后，它发起的 run 不被认作本机 → 永远不会被回收孤儿，
# 反过来还可能被别的路径误判。加机器等于要改代码，违背可扩展性。
#
# 改为：本机发起 = run_id 里的 slot 号在**本机 RUNNER_SLOT 白名单**里。
# 白名单来自环境变量 RUNNER_LOCAL_SLOTS（run_one.sh 导出），
# 没设置时退化为「含 -slot 且不含云盒标记」的宽判定。
def _local_slot_tokens():
    raw = (os.environ.get("RUNNER_LOCAL_SLOTS") or "").strip()
    if raw:
        toks = []
        for x in raw.replace(":", ",").split(","):
            x = x.strip()
            if x.isdigit():
                toks.append("-slot%s-" % x)
        if toks:
            return tuple(toks)
    # 兜底：配置里的 host=oracle 的 lane 对应的 slot，按本机 runner 实际实例数推
    toks = []
    for n in range(1, 13):
        toks.append("-slot%d-" % n)
    return tuple(toks)


_LOCAL_SLOT_TOKENS = _local_slot_tokens()


def _is_local_run(run_id):
    """这个 run_id 看起来是不是本机发起的（含 -slotN- 标记）。

    注意：这只是**弱信号**。云盒通过 ssh 回控制面执行 gha_next3，
    拿到的 run_id 同样带 slot 前缀 —— 所以不能单靠它决定回收。
    """
    rid = str(run_id or "")
    if not rid:
        return True
    return any(tok in rid for tok in _LOCAL_SLOT_TOKENS)


def _orphan_evidence(run_id):
    """心跳是权威判据（2026-11-24 E206）。

    这里曾经只认「进程查不到 + claim 不存在」两条同时成立。但 claim 行是
    旧时代的存活代理：run 死了、心跳停了，claim 还挂在库里 →
    `_orphan_evidence` 永远 False → 租约只能等 TTL(3h) 到期 →
    四个号被死租约钉死、全线 no_session（今天 10:20 又复现一次）。

    现在：只要该 run 有心跳文件且已过期（>180s），就直接认定孤儿。
    没有心跳文件时才退回旧规则（兼容没有心跳的历史 run）。
    """
    try:
        hb = os.path.join(HB_DIR, "%s.hb" % run_id)
        if os.path.exists(hb):
            return not _hb_fresh(run_id)
    except Exception:                                       # noqa: BLE001
        pass
    """有没有**足够证据**判定这条租约是孤儿（可以安全回收）。

    2026-11-15（E103/E106）两次踩坑后定下的规则：
      · E101 版：TTL 未到就 pgrep，查不到就回收 → 云盒租约被抢，
        同一 TG 账号双 IP 并发 → AuthKeyDuplicated（11-14 01:23 事故）。
      · E103 版：靠 run_id 格式区分本机/云盒 → 测试证明太脆，
        run_id 不带 slot 前缀时连真孤儿都回收不掉。

    现在要求**两条证据同时成立**才回收：
      ① ps 里确实找不到这个 run 的进程（跨机器时 ps 看不到云盒进程，
         但下一条会挡住误判）
      ② 该 run 名下的 claim 已经不存在 —— claim 才是「它在干活」的
         权威证据，云盒的 claim 一定在共享目录里，本机看得见。
    两条都满足 = 进程没了、claim 也释放了 = 真孤儿。
    """
    rid = str(run_id or "")
    if not rid:
        return True
    if _run_is_alive(rid):
        return False                     # 进程还在，活着
    try:
        import glob as _glob
        import json as _json
        base = os.path.dirname(os.path.abspath(__file__))
        for fp in _glob.glob(os.path.join(base, ".gha_claims", "*.json")):
            try:
                with open(fp, encoding="utf-8") as fh:
                    if str((_json.load(fh) or {}).get("run_id") or "") == rid:
                        return False     # claim 还在 → 云盒/别的机器在干活
            except Exception:            # noqa: BLE001
                continue
    except Exception:                    # noqa: BLE001
        return False                     # 查不动就保守当它活着
    return True


HB_DIR = "/opt/embytg-runner/run"


def _hb_fresh(run_id, max_age=180):
    """心跳文件判活：run_one.sh 每 30 秒写一次，最可靠（2026-11-23）。

    实测问题：上传进程的 argv 是 `gha_upload.py --job <json>`，里面**没有**
    run_id —— 于是按 argv 匹配永远判死，孤儿判定形同虚设：租约要么永久
    占着（no_session 刷屏），要么被误判后提前释放（同一集被两台机器重复下）。
    心跳文件是唯一和「这一轮到底活着没有」严格对应的信号。
    """
    try:
        pth = os.path.join(HB_DIR, "%s.hb" % run_id)
        if not os.path.exists(pth):
            return False
        return (time.time() - os.path.getmtime(pth)) <= max_age
    except Exception:                                       # noqa: BLE001
        return False


def _run_is_alive(run_id):
    """run_id 对应的上传进程还在不在本机。

    2026-11-14（E102）：以前用 `pgrep -af <run_id>`，那是**子串匹配** ——
    会命中命令行里恰好含这个字符串的任何进程：调用者自己的
    `python3 -m unittest ...`、包着它的 bash `-c '... runA ...'`，甚至
    本函数继承的 argv。实测 runA 根本不存在，pgrep 每行都命中，
    于是孤儿租约永远回收不掉、四个 lane 里有两个一直 no_session 空转
    （E101 那版修复等于没生效）。

    判据改为：拿 `ps -eo pid,comm,args`，只看**内核记账的可执行名**（comm）
    是 python*/boxwake 的进程，且某个 argv 元素**完整等于** run_id 或
    `<run_id>.py`。查不到进程 → 认定已死；查不动（异常）→ 保守认为还活着。
    """
    rid = str(run_id or "")
    if not rid:
        return True
    # 心跳优先（2026-11-23）：上传进程 argv 里根本没有 run_id，argv 匹配必判死。
    if os.path.exists(os.path.join(HB_DIR, "%s.hb" % rid)):
        return _hb_fresh(rid)
    # 2026-11-20：`<名字>-<pid>` 形式的 run_id（emos-3416254 / tgsrc-123）直接按 pid 判活。
    # 这类进程的 argv 里没有 run_id 字样（它是 emos_import.py --series ...），
    # 只靠下面的 argv 匹配永远查不到 → 被判孤儿 → 别的 lane 抢走同一个 TG 账号。
    # 实测：emos 领到 slot1 后 slot1.lock 凭空消失，同一账号上出现两条上传通道。
    _head, _sep, _pid = rid.rpartition("-")
    if _sep and _pid.isdigit() and _head.replace("_", "").isalnum():
        try:
            os.kill(int(_pid), 0)
            return True
        except ProcessLookupError:
            return False                               # pid 真没了 = 孤儿
        except Exception:                              # noqa: BLE001
            return True                                # 权限/其他：保守当活着
    try:
        import subprocess as _sp
        out = _sp.run(["ps", "-eo", "pid=,comm=,args="],
                      capture_output=True, text=True, timeout=10).stdout
    except Exception:                                  # noqa: BLE001
        return True                                    # 查不了就当它活着
    for line in (out or "").splitlines():
        t = line.split()
        if len(t) < 3:
            continue
        comm = t[1]
        if not (comm.startswith("python") or comm.startswith("boxwake")):
            continue                                   # bash/ssh/pgrep 等一律不算
        for arg in t[2:]:
            base = os.path.basename(arg)
            if base == rid or base == rid + ".py" or arg == rid:
                return True
    return False


RESERVE_SUFFIX = ".reserve"


def _slot_reserve_path(n):
    return _slot_path(n) + RESERVE_SUFFIX


def slot_reserve(n, holder, ttl=300):
    """给第 n 个 slot 立「预约旗」：在旗有效期内，别人不再领这一 slot。

    为什么需要（2026-11-15）：tgpipe 的零流量转存要和上传 lane 共用账号池，
    而 lane 是「收尾立刻领下一集」，释放到再次 claim 之间只有亚秒级间隙。
    实测 slot3 连续 60 秒同一 run_id、空闲率 0%，靠轮询**永远抢不到**
    （连续 30 分钟 0 条，扫描一行输出都没有）。
    改成预约制：tgpipe 先立旗 → lane 交完当前这集就不再领这个号 →
    tgpipe 自然拿到 → 用完撤旗。旗带 TTL，tgpipe 崩了也不会永久占号。
    """
    try:
        os.makedirs(SLOT_DIR, exist_ok=True)
        with open(_slot_reserve_path(n), "w", encoding="utf-8") as f:
            json.dump({"holder": str(holder), "ts": int(time.time()), "ttl": int(ttl)}, f)
        return True
    except Exception:                                  # noqa: BLE001
        return False


def slot_unreserve(n, holder=None):
    """撤旗。传 holder 时只撤自己的旗（防误撤别人的）。"""
    pth = _slot_reserve_path(n)
    try:
        if not os.path.exists(pth):
            return True
        if holder is not None:
            try:
                with open(pth, encoding="utf-8") as f:
                    d = json.load(f)
            except Exception:                          # noqa: BLE001
                d = {}
            if str(d.get("holder") or "") != str(holder):
                return False
        os.remove(pth)
        return True
    except Exception:                                  # noqa: BLE001
        return False


def _reserve_active(n):
    """旗是否仍在有效期内。没有/损坏 → 不挡（宁可漏挡，不可死锁）。"""
    try:
        with open(_slot_reserve_path(n), encoding="utf-8") as f:
            d = json.load(f)
    except Exception:                                  # noqa: BLE001
        return False
    try:
        return time.time() < float(d.get("ts") or 0) + float(d.get("ttl") or 0)
    except Exception:                                  # noqa: BLE001
        return False


def release_slot(n, run_id=None):
    """释放第 n 个 session 租约（2026-11-23）。

    原来**只租不放**：只有 TTL 到期（3 小时）或孤儿判定才会回收。
    实测后果：每次上传都占一个号，3 次之后三个号全被锁住 →
    runner 连续报 `no_session`（今天近 2 小时 37 次，是最大错误源）。
    """
    pth = _slot_path(n)
    try:
        if not os.path.exists(pth):
            return False
        with open(pth, encoding="utf-8") as f:
            d = json.load(f) or {}
        if run_id and str(d.get("run_id")) != str(run_id):
            return False
        os.unlink(pth)
        return True
    except Exception:                                       # noqa: BLE001
        return False


def abandon_run(run_id):
    """一个 run 结束时，把它持有的**所有**租约一次性放掉（2026-11-23）。

    今天暴露的真实故障：session 租约和条目 claim 都只租不放。
      · 3 个号被锁满 → 所有 lane 报 no_session（2 小时 37 次）
      · 条目 claim 残留 → 同一集被长期占着，别的机器领不到
    租约是「过程独占」，不是「结果记录」；进程退出时必须归还，
    否则 TTL（3h/4h）会把它变成长期占用。
    """
    freed_slots = release_slot_by_run(run_id)
    freed_items = 0
    try:
        hit = find_claim(str(run_id))
        path, job = (hit if isinstance(hit, tuple) else (None, hit))
        if job:
            key = job.get("key") or item_key(job)
            worker = job.get("worker") or worker_id(run_id)
            if release_item(key, worker):
                freed_items += 1
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:                                       # noqa: BLE001
        pass
    return {"slots": freed_slots, "items": freed_items}


def release_slot_by_run(run_id):
    """按 run_id 释放任意 slot 的租约（不知道落在哪个号时用）。"""
    n = 0
    try:
        for name in os.listdir(SLOT_DIR):
            if not name.startswith("slot") or not name.endswith(".lock"):
                continue
            pth = os.path.join(SLOT_DIR, name)
            try:
                with open(pth, encoding="utf-8") as f:
                    d = json.load(f) or {}
            except Exception:                               # noqa: BLE001
                continue
            if str(d.get("run_id")) == str(run_id):
                try:
                    os.unlink(pth)
                    n += 1
                except Exception:                           # noqa: BLE001
                    pass
    except Exception:                                       # noqa: BLE001
        pass
    return n


def slot_lease(n, run_id, ttl=SLOT_TTL):
    """尝试独占第 n 个 TG session。成功 True，被别人占着 False。

    原子性靠 O_EXCL 建文件（跨机器安全，所有执行方都在同一台控制面上）。
    回收有两种触发：TTL 到期（持有者崩了）与「TTL 没到但进程没了」（孤儿）。
    ← 孤儿判定见 `_run_is_alive()`；**TTL 不等于占用**这条是 E101 的核心。
    """
    os.makedirs(SLOT_DIR, exist_ok=True)
    # 预约优先（见 slot_reserve）：旗有效期内，非 tgpipe 的调用方直接让位。
    # tgpipe 自己的 run_id 形如 `tgpipe-<pid>`，不受此限。
    # 2026-11-20：**立旗的人自己必须能领到**。原来只放行 tgpipe，
    # 别的持有人（如 ysdq-auto）立旗后会被自己的旗挡住 → 等满窗口一场空。
    if not str(run_id).startswith("tgpipe") and _reserve_active(n):
        try:
            with open(_slot_reserve_path(n), encoding="utf-8") as _f:
                _holder = str((json.load(_f) or {}).get("holder") or "")
        except Exception:                                  # noqa: BLE001
            _holder = ""
        if _holder != str(run_id):
            return False
    pth = _slot_path(n)
    payload = json.dumps({"run_id": str(run_id), "ts": int(time.time()),
                          "ttl": int(ttl)}, ensure_ascii=False)
    for _ in range(3):
        try:
            fd = os.open(pth, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
            return True
        except FileExistsError:
            pass
        except Exception:                              # noqa: BLE001
            return False

        # 已存在：判断能不能回收
        try:
            with open(pth, encoding="utf-8") as f:
                cur = json.load(f)
        except Exception:                              # noqa: BLE001
            cur = None                                 # 损坏 → 可回收
        reclaim = False
        if cur is None:
            reclaim = True
        else:
            holder = str(cur.get("run_id") or "")
            exp = float(cur.get("ts") or 0) + float(cur.get("ttl") or 0)
            if time.time() >= exp:
                reclaim = True                         # TTL 到期
            elif _orphan_evidence(holder):
                # 2026-11-15（E103）：**撞车根因修复。**
                #
                # E101 的孤儿回收只在「本机能看见进程」时才成立。
                # 云盒 box2/new4 通过 ssh 回控制面领活，它的进程**本机根本
                # 看不到**，于是被误判成孤儿 → 本机另一个 lane 抢走同一 slot
                # → 同一 TG 账号双 IP 并发 → AuthKeyDuplicated（11-14 01:23
                # 本机 20 份 upload.log 全是这个错，全部发生在 slot1）。
                #
                # 判据：run_id 的命名里带本机 slot 前缀（如 `...-slot1-new2-`）
                # 才说明这是**本机**发起的 run；云盒的 run_id 带 slot3/slot4
                # 之类云盒自己的编号，或根本查不到进程 —— 一律当它活着，
                # 只信 TTL。宁可等 TTL 到期（3h），也不能抢同一个 TG 账号。
                reclaim = True                         # 本机孤儿，可回收
            else:
                return False                           # 真的有人在用
        if reclaim:
            try:
                os.remove(pth)
            except FileNotFoundError:
                pass                                   # 别人刚回收掉，重试抢锁
            except Exception:                          # noqa: BLE001
                return False
            continue
    return False


def slot_release(n, run_id=None):
    """归还第 n 个 session。只删属于自己（run_id 匹配）或无主的租约。"""
    pth = _slot_path(n)
    try:
        if run_id is not None:
            with open(pth, encoding="utf-8") as f:
                cur = json.load(f)
            if str(cur.get("run_id")) != str(run_id):
                return False                          # 不是我的，别动
        os.remove(pth)
        return True
    except FileNotFoundError:
        return False                                  # 已经没人持有了
    except Exception:                                 # noqa: BLE001
        return False


def slot_holder(n):
    """返回第 n 个 session 的租约内容；空闲返回 None。"""
    try:
        cur = json.load(open(_slot_path(n), encoding="utf-8"))
        if time.time() < float(cur.get("ts") or 0) + float(cur.get("ttl") or 0):
            return cur
    except Exception:                                 # noqa: BLE001
        pass
    return None


def record_episode_failure(video_id, reason=""):
    """记一次「这一集」的失败。返回 (本类别连续失败次数, 是否进入长冷却)。

    2026-11-19：**按原因类别分别计数**。同一个 video_id 先在源站侧失败 5 次
    （8 天冷却），后来源站补片了、下载成功但上传抖动一次 —— 如果沿用累计次数，
    这一次抖动会直接续上 30 天封禁。类别变了就重新计数，这是唯一不会把
    「我方故障」算进「源站缺片」账上的做法。
    """
    vid = str(video_id or "").strip()
    if not vid:
        return 0, False
    d = _ep_fail_load()
    now = time.time()
    rec = d.get(vid) or {}
    cls = ep_fail_class(reason)
    prev_cls = rec.get("cls") or (ep_fail_class(rec.get("reason")) if rec.get("reason") else cls)
    prev_n = int(rec.get("fails_cls") or rec.get("fails") or 0)
    n_cls = prev_n + 1 if prev_cls == cls else 1
    rec = {"fails": int(rec.get("fails") or 0) + 1, "fails_cls": n_cls, "cls": cls,
           "last": now, "reason": str(reason or "")[:120]}
    tripped = n_cls >= EP_FAIL_TRIP
    if tripped:
        sec = ep_cool_seconds(n_cls, reason)
        rec["cool_sec"] = sec
        rec["cool_days"] = round(sec / 86400.0, 4)
        rec["next_check"] = now + sec
    # 防无界增长：冷却早已到期且 90 天没再失败的条目直接丢（下次失败会重新记）
    # 20 万集规模下，5% 失败率 = 1 万条；不裁剪的话这份 JSON 每次 next3 都要整份解析。
    _stale = [k for k, v in d.items()
              if isinstance(v, dict)
              and now - float(v.get("last") or 0) > EP_FAIL_PRUNE_DAYS * 86400
              and now > float(v.get("next_check") or 0)]
    for k in _stale:
        d.pop(k, None)
    d[vid] = rec
    tmp = EP_FAIL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, EP_FAIL)
    return n_cls, tripped


def episode_blocked_set():
    """返回当前处于长冷却期的 video_id 集合。"""
    now = time.time()
    return {str(k) for k, v in _ep_fail_load().items()
            if isinstance(v, dict) and now < float(v.get("next_check") or 0)}


def clear_episode_failure(video_id):
    """这一集成功了 —— 清掉它的失败记录。"""
    vid = str(video_id or "").strip()
    d = _ep_fail_load()
    if vid in d:
        d.pop(vid, None)
        tmp = EP_FAIL + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
        os.chmod(tmp, 0o600)
        os.replace(tmp, EP_FAIL)


def record_source_success(label):
    if not label:
        return
    with _budget_lock():
        d = _load_budget_unlocked()
        rec = _source_record(d, str(label), time.time())
        rec["fails"] = 0
        rec["cooldown_until"] = 0
        rec["download_fails"] = 0
        rec["download_cooldown_until"] = 0
        # 成功出片 +1：这是"这个源到底有没有用"的唯一客观依据。
        rec["ok"] = int(rec.get("ok") or 0) + 1
        _write_budget_guarded(d)

