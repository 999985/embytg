#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""status_now.py — 单一状态入口（一条命令回答「现在什么情况」）

为什么有它（2026-11-10 用户原话「入库是我喊你做的，但是后面就不知道发生什么了」）：
当时真相散在 6 处 —— AgentCommunication.log / OPS_JOURNAL / tg.db / Emby API /
backup/LATEST / auto_finalize.log，每次都要人肉重建，且没人写「完成」闭环。
本脚本**只读聚合**这 6 处，输出固定格式，任何人/任何 agent 用同一条命令看到同一事实。

用法:  /usr/bin/python3 pipeline/status_now.py          # 人读
       /usr/bin/python3 pipeline/status_now.py --json   # 机器读
不改任何文件、不触发扫描/刷新/重启、不联网写。
"""
import glob
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

ROOT = "/opt/embytg"
PIPE = os.path.join(ROOT, "pipeline")
BK = os.path.join(ROOT, "backup")
TGDB = os.path.join(PIPE, "tg.db")
RECON_STATE = os.path.join(PIPE, "logs", "reconcile_state.json")
LOG = os.path.join(ROOT, "AgentCommunication.log")
EMBY_LOG = "/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt"


def sh(cmd, timeout=20):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=timeout).stdout.strip()
    except Exception:
        return ""


def cfg():
    try:
        return json.load(open(os.path.join(ROOT, "config.json"), encoding="utf-8"))
    except Exception:
        return {}


def emby(path, token, timeout=10):
    try:
        req = urllib.request.Request("http://127.0.0.1:8096" + path,
                                     headers={"X-Emby-Token": token})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except Exception:
        return {}


def loop_pids():
    """权威判定：/proc 精确匹配，避免 grep/awk 把自身 cmdline 算进去（踩过 2 次）。"""
    out = []
    for p in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            a = [x.decode("utf8", "ignore") for x in open(p, "rb").read().split(b"\0") if x]
        except Exception:
            continue
        if not a:
            continue
        if (any(x.endswith("auto_finalize.py") for x in a) and "--loop" in a
                and not any(x in ("-c", "sh", "bash") for x in a[:2])):
            out.append(int(p.split("/")[2]))
    return out


def active_agents():
    """正在跑的业务进程（排除常驻 loop / 自身 shell）。"""
    keys = ("tgpipe", "nfo_", "rename", "apply_rename", "naming_health",
            "todb_", "ingest_sop", "pg_dump", "gen_nfo", "_scan_now",
            "_refresh", "relabel", "auto_finalize")
    out = []
    for p in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            a = [x.decode("utf8", "ignore") for x in open(p, "rb").read().split(b"\0") if x]
        except Exception:
            continue
        if not a or "grep" in a[0] or ("/bin/sh" in a[0] and "-c" in a[:2]):
            continue
        s = " ".join(a)
        if any(k in s for k in keys):
            out.append((int(p.split("/")[2]), s[:130]))
    return out


def lane_snapshot():
    """三线队列才是 backlog 真源；tg.db 的 pending/cursor 只是历史管线游标。"""
    lanes = {}
    errors = []
    for lane, fn in (("new", ".gha_queue_new.json"),
                     ("fill", ".gha_queue_fill.json"),
                     ("upgrade", ".gha_queue_upgrade.json")):
        p = os.path.join(PIPE, fn)
        info = {"items": 0, "episodes": 0,
                "done": os.path.isfile(os.path.join(PIPE, ".gha_lane_done_%s" % lane)),
                "queue_mtime": None}
        try:
            data = json.load(open(p, encoding="utf-8"))
            items = data.get("items") or []
            info["items"] = len(items)
            if lane == "fill":
                info["episodes"] = sum(len(x.get("missing") or []) for x in items)
            elif lane == "upgrade":
                info["episodes"] = len(items)
            else:
                info["episodes"] = len(items)
            info["queue_mtime"] = time.strftime("%F %T", time.localtime(os.path.getmtime(p)))
        except Exception as e:                              # noqa: BLE001
            info["err"] = str(e)[:80]
            errors.append("%s: %s" % (lane, info["err"]))
        lanes[lane] = info
    claims = [os.path.basename(p) for p in
              glob.glob(os.path.join(PIPE, ".gha_claims/*.json"))]
    backlog = None
    if not errors:
        backlog = sum(v["episodes"] for k, v in lanes.items() if not v["done"])
    return {"lanes": lanes, "claims": claims, "backlog": backlog,
            "err": "; ".join(errors) or None}


def queue_progress(con):
    """tg_pending 队列进度 → (total, cursor, unapplied)。

    2026-11-16 E142：旧口径数 `payload.done`（pending_undone）是**类别错误**。
    tgpipe `apply --from-queue` 单批模式只推进 meta.cursor、**从不回写 payload.done**，
    所以 rowid 1..20 已登记、cursor=20 时它仍报「35 条未 apply」（实际剩 15），
    status_now 由此长期把「下一轮就消费」的队列判成【需收口】。
    正确口径：cursor 之前的条目即已消费（done 字段只服务常驻 loop 模式），
    未 apply = rowid > cursor。save_json 每次都是 DELETE+INSERT，rowid 恒为 1..N，
    与 cursor 的「第几条」同一坐标；历史裁剪导致 cursor > N 时自然得 0。
    """
    total = con.execute("select count(*) from pending").fetchone()[0]
    row = con.execute("select v from meta where k='cursor'").fetchone()
    cur_i = 0
    if row and row[0] is not None:
        raw = row[0]
        try:
            cur_i = int(json.loads(raw)) if isinstance(raw, str) else int(raw)
        except (TypeError, ValueError):
            cur_i = 0
    cur_i = max(0, cur_i)
    unapplied = con.execute("select count(*) from pending where rowid > ?",
                            (cur_i,)).fetchone()[0]
    return total, cur_i, unapplied


def item_ledger_gaps():
    """条目级五账差异（reconcile.py 每 15min 写进 tg.db.reconcile_gaps，带 age_min）。

    2026-11-16 自醒的根因：本脚本的 [对账] 只读 `lib_reconcile.py` 的**库级计数**
    （gaps = libs_with_gap），而 E035 已经证明「每库总数相等」也会错位（少 1 集、
    多 1 集，或者干脆整部剧不在 Emby）。当天实测的铁证：

        lib_reconcile  gaps=[]  converged=True
        reconcile.py   gaps=[unregistered 神烦警探 S01E01]   ← 单一状态入口看不见

    于是【稳定】可以和「teldrive 有文件、tg.db 台账里没有」同时成立 —— 正是
    「用户搜不到某剧、存在了很久没人发现」的那条裂缝。现在把 reconcile.py 的
    条目级结果并进来，并且**只用差异年龄**区分「在途」与「孤儿」：

      · 上传刚完成 → tg.db 写台账有 1~2 分钟延迟（实测 09:03 出现、09:04 消失），
        这类 age_min < grace 的差异属在途，不计阻塞；
      · 超过 grace（默认 30min，`EMBYTG_GAP_GRACE_MIN` 可覆盖）还没自愈的，
        就是 teldrive/台账/Emby 之间的真裂缝，必须阻塞并点名。

    对账器本身太久没跑（>`STALE_MIN` 90min）也算未知 —— 感知死了不能报「齐」。
    """
    grace = float(os.environ.get("EMBYTG_GAP_GRACE_MIN", "30"))
    stale_min = float(os.environ.get("EMBYTG_GAP_STALE_MIN", "90"))
    db = os.environ.get("EMBYTG_TGDB", TGDB)
    state = os.environ.get("EMBYTG_RECONCILE_STATE", RECON_STATE)
    out = {"grace_min": grace}
    try:
        out["run_age_min"] = round((time.time() - os.path.getmtime(state)) / 60.0, 1)
    except Exception:
        out["run_age_min"] = None                  # 读不到 = 未知 → 按 no_data 处理
    try:
        con = sqlite3.connect("file:%s?mode=ro" % db, uri=True, timeout=10)
        cols = [r[1] for r in con.execute("PRAGMA table_info(reconcile_gaps)")]
        if not cols:
            con.close()
            out["err"] = "reconcile_gaps 表不存在（对账器未跑过）"
            return out
        has_age = "age_min" in cols
        cur = con.execute("select kind, lib, series, season, ep,"
                          " %s from reconcile_gaps" % ("coalesce(age_min,0)" if has_age else "0"))
        rows = [{"kind": r[0], "lib": r[1] or "", "series": r[2] or "",
                 "season": r[3], "ep": r[4], "age": float(r[5] or 0)} for r in cur]
        con.close()
    except Exception as e:                          # noqa: BLE001
        out["err"] = str(e)[:110]
        return out
    kinds = {}
    for r in rows:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    aged = [r for r in rows if r["age"] >= grace]
    out.update({
        "gaps": len(rows),
        "aged": len(aged),
        "kinds": kinds,
        "age_max_min": round(max([r["age"] for r in rows]), 1) if rows else 0.0,
        "stale": (out["run_age_min"] is None) or (out["run_age_min"] > stale_min),
        "no_data": (not has_age) and bool(rows),     # 旧表无年龄列 = 无法判定在途/孤儿
        "sample": ["%s %s/%s S%sE%s %.0fmin"
                   % (r["kind"], r["lib"], r["series"][:16], r["season"], r["ep"], r["age"])
                   for r in sorted(rows, key=lambda x: -x["age"])[:3]],
    })
    return out


def collect():
    c = cfg()
    tok = c.get("emby_token", "")
    d = {"ts": time.strftime("%F %T %Z")}

    # 1) 现场
    d["mount"] = "OK" if os.path.ismount(os.path.join(ROOT, "mnt/embytg")) else "DEAD"
    d["emby_http"] = sh("curl -s -o /dev/null -w '%{http_code}' --max-time 5 "
                        "http://127.0.0.1:8096/System/Info -H 'X-Emby-Token: " + tok + "'")
    counts = {}
    for ty in ("Movie", "Series", "Episode"):
        r = emby("/Items?Recursive=true&IncludeItemTypes=%s&Limit=1" % ty, tok, 15)
        counts[ty] = r.get("TotalRecordCount", "ERR")
    d["counts"] = counts
    tasks = emby("/ScheduledTasks", tok)
    if isinstance(tasks, list):
        t = next((x for x in tasks if x.get("Name") == "Scan media library"), {})
        d["scan"] = {"state": t.get("State"), "progress": round(t.get("CurrentProgressPercentage") or 0, 1),
                     "triggers": t.get("Trigers") or t.get("Triggers")}
    else:
        d["scan"] = {"state": "ERR"}

    # 2) 收敛（真判据，不认 Scan Idle / 全局数字）
    try:
        r = subprocess.run(["/usr/bin/python3", "lib_reconcile.py", "--json"],
                           cwd=PIPE, capture_output=True, text=True, timeout=180)
        rec = json.loads(r.stdout)
        d["reconcile"] = {"gaps": rec.get("libs_with_gap"), "td_files": rec.get("td_files_total"),
                          "episode": rec.get("episode_total")}
        d["converged"] = (d["scan"]["state"] != "Running" and not rec.get("libs_with_gap"))
    except Exception as e:
        # E016（2026-11-10 自醒）：对账失败 = 「未知」，绝不等于「无 gap」。
        # 以前只写 {"err":...}，verdict 里 gaps 取到 None 后 `or []` 变成 []，
        # 于是打印「gaps=None td=None ep=None converged=False」却仍下结论【稳定】+「对账齐」。
        # 现在显式把三件套置 None，下游一律按「未过」处理（fail-closed）。
        d["reconcile"] = {"err": str(e)[:120], "gaps": None,
                          "td_files": None, "episode": None}
        d["converged"] = False

    # 2b) 条目级五账差异（2026-11-16）：库级计数看不出「少一集多一集」，
    # 单一状态入口必须同时看条目级，否则【稳定】会和真孤儿并存。
    d["ledger"] = item_ledger_gaps()

    # 3) NFO 缺口（用现成报告，不重跑 200s 体检）
    # 2026-11-13：这里原本硬编码 docs/NFO_GAPS_20261110.md，而
    # gen_nfo_gap_report.py 是按当天日期写 docs/NFO_GAPS_<date>.md。
    # 结果读取方永远读 11-10 那份，报告一天天变旧（观测到 STALE 76h），
    # 缺口数字其实是四天前的。改为按日期倒序取最新一份，任何一份都没有才算缺报告。
    _nfo_dir = os.path.join(PIPE, "docs")
    _cands = sorted(glob.glob(os.path.join(_nfo_dir, "NFO_GAPS_*.md")))
    gap = _cands[-1] if _cands else None
    d["nfo_gap"] = None
    if gap and os.path.isfile(gap):
        m = re.search(r"合计\s*(\d+)\s*条[^，,]*[，,]\s*涉及\s*(\d+)\s*部", open(gap, encoding="utf-8").read())
        if m:
            mt = os.path.getmtime(gap)
            age_h = (time.time() - mt) / 3600.0
            d["nfo_gap"] = {"missing": int(m.group(1)), "series": int(m.group(2)),
                            "report_mtime": time.strftime("%F %T", time.localtime(mt)),
                            "age_h": round(age_h, 1), "stale": age_h > 6.0}

    # 4) 入库状态（tg.db 只读）
    try:
        con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True)
        con.execute("PRAGMA query_only=ON")
        total_q, cur_q, unapplied = queue_progress(con)
        d["ingest"] = {
            "pending": total_q,
            "cursor": cur_q,
            # E142：未 apply = rowid > cursor（见 queue_progress docstring）。
            "pending_unapplied": unapplied,
            # 兼容旧口径，仅供对照：payload.done 只由常驻 loop 模式回写，单批模式恒 0。
            "pending_undone": con.execute(
                "select count(*) from pending "
                "where coalesce(json_extract(payload,'$.done'),0)=0").fetchone()[0],
            "state_key": con.execute("select count(*) from state_key").fetchone()[0],
            "integrity": con.execute("pragma integrity_check").fetchone()[0],
        }
        con.close()
    except Exception as e:
        d["ingest"] = {"err": str(e)[:120]}
    # 近 30 分钟新增媒体/NFO（判断 SOP 各步是否跑过）
    q = ("docker exec -i teldrive-postgres psql -U teldrive -d teldrive -At -c "
         "\"select count(*) from files where status='active' and created_at > now() - interval '30 minutes' and lower(name) ~ '\\.(mp4|mkv|avi|ts|mov)$'\"")
    d["recent_video_30m"] = sh(q, 30) or "?"
    q2 = q.replace("'\\.(mp4|mkv|avi|ts|mov)$'", "'%.nfo'").replace("~", "like")
    d["recent_nfo_30m"] = sh(q2, 30) or "?"

    # 4b) 三线 backlog（lane queue 才是真源，别拿 tg.db pending 当队列）
    d["lanes"] = lane_snapshot()

    # 5) 备份 / 状态机
    try:
        L = json.load(open(os.path.join(BK, "LATEST.json"), encoding="utf-8"))
        d["latest"] = {"state": L.get("state"), "created": L.get("created"),
                       "archive": L.get("archive_name"), "files": L.get("files_included"),
                       "missing": L.get("files_missing")}
        p = L.get("archive")
        if p and os.path.isfile(p):
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for b in iter(lambda: f.read(1 << 20), b""):
                    h.update(b)
            d["latest"]["sha256_ok"] = (h.hexdigest() == L.get("sha256"))
    except Exception as e:
        d["latest"] = {"err": str(e)[:120]}
    d["loop_pids"] = loop_pids()
    try:
        S = json.load(open(os.path.join(BK, "finalize_state.json"), encoding="utf-8"))
        d["finalize_stage"] = S.get("stage")
    except Exception:
        d["finalize_stage"] = "?"
    d["active_agents"] = active_agents()

    # 6) 协作日志闭环：最后一条「开始/进行中」有没有对应「完成/中止」
    d["open_tasks"] = []
    if os.path.isfile(LOG):
        txt = open(LOG, encoding="utf-8").read().splitlines()
        heads = [(i, l) for i, l in enumerate(txt) if re.match(r"^## \[\d{4}-", l)]
        for i, l in heads:
            m = re.match(r"^## \[([^\]]+)\] ([^|]+)\|\s*([^|]+)\|", l)
            if not m:
                continue
            name, status = m.group(2).strip(), m.group(3).strip()
            if status in ("开始", "进行中"):
                task_text = l.split("|", 3)[-1].strip()
                if task_text.startswith("广播"):
                    continue
                # 找同名之后有没有完成/中止
                closed = any(
                    (m2 := re.match(r"^## \[([^\]]+)\] ([^|]+)\|\s*([^|]+)\|", l2))
                    and m2.group(2).strip() == name and m2.group(3).strip() in ("完成", "中止")
                    and j > i
                    for j, l2 in heads)
                if not closed:
                    d["open_tasks"].append({"time": m.group(1), "agent": name,
                                            "status": status, "task": task_text[:90]})
        cst = timezone(timedelta(hours=8))
        now = datetime.now(cst)
        for t in d["open_tasks"]:
            try:
                raw = t["time"].strip().split("+", 1)[0].strip()
                ts = datetime.fromisoformat(raw)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=cst)
                t["age_h"] = round((now - ts).total_seconds() / 3600.0, 1)
            except Exception:                                  # noqa: BLE001
                t["age_h"] = 999.0
        d["open_tasks_recent"] = [t for t in d["open_tasks"] if t["age_h"] <= 3.0]
        d["open_tasks_historical"] = [t for t in d["open_tasks"] if t["age_h"] > 3.0]
        d["log_last"] = heads[-1][1][:130] if heads else "?"
        d["log_entries"] = len(heads)
    return d


def verdict(d):
    """一句话结论 + 需要注意的点。别把中间态说成稳定态，也别把 0.1% 尾部 gap
    当成「需收口」——运维任务 12:52：Scan Idle 后的小 gap 是新片/改名落盘的尾部条目，
    由 12h 周期扫描 + prewarm 自愈，不是人工收口项。"""
    out = []
    scan = d.get("scan", {}).get("state")
    ing = d.get("ingest", {})
    pending, cursor = ing.get("pending"), ing.get("cursor")
    rec = d.get("reconcile") or {}
    # E016：gaps 必须是「明确的空列表」才算对账齐；None/抛错 = 未知 → 阻塞。
    rec_unknown = bool(rec.get("err")) or rec.get("gaps") is None
    gaps = rec.get("gaps") or []
    _l = d.get("ledger")
    led = _l if isinstance(_l, dict) else {}
    lsnap = d.get("lanes") or {}
    lq = lsnap.get("lanes") or {}
    backlog = lsnap.get("backlog")
    claims = lsnap.get("claims") or []
    blocking = False
    if scan == "Running":
        out.append("Emby 全库扫描 Running %s%%（ffprobe 在推进=没卡死；期间禁刷新/重启/再扫描）"
                   % d["scan"].get("progress"))
        blocking = True
    # E142：判据换成「rowid > cursor 的条数」。旧版数 payload.done，而 tgpipe 单批模式
    # 只推 cursor 不回写 done → 把「剩 15 条、下一轮正常消费」报成「35 条未 apply」。
    # 单轮消费上限 20（embytg-tgpipe.service --max-items 20）：≤20 属在途，>20 才是积压。
    unapplied = ing.get("pending_unapplied")
    undone = ing.get("pending_undone")
    if isinstance(unapplied, int) and unapplied > 20:
        out.append("入库队列积压 %d 条（cursor=%s/%s，超过单轮消费上限 20，scan 增速 > apply）"
                   % (unapplied, cursor, pending))
        blocking = True
    elif isinstance(unapplied, int) and unapplied > 0:
        out.append("入库队列余 %d 条待 apply（cursor=%s/%s，属 tgpipe 下一轮正常消费，非阻塞）"
                   % (unapplied, cursor, pending))
    if not d.get("loop_pids"):
        out.append("auto_finalize --loop 0 实例（收敛监控/自动打包停摆，需恢复）")
        blocking = True
    if d.get("open_tasks_recent"):
        # 协作日志是交接记录，不是运行态闸门；历史未闭环不能把上片链路误判为阻塞。
        out.append("协作日志近 3h 有 %d 条未闭环的『开始/进行中』（仅日志卫生，不计入运行阻塞）"
                   % len(d["open_tasks_recent"]))
    if rec_unknown:
        out.append("对账未知（lib_reconcile 失败：%s）→ 不能当「对账齐」，L3 闸门按未过处理"
                   % (rec.get("err") or "gaps=None"))
        blocking = True
    if gaps:
        if scan == "Running":
            out.append("对账 gaps=%s（扫描未完成=中间态）" % gaps)
        else:
            # 2026-11-13：缺口里有「正在下载中」的集（claim 已写、teldrive 已登记、
            # Emby 还没建条目）。这属于正常推进，不是故障。用活动 claim 数兜底，
            # 与 improve_guard 的 gap_le_claims 保持同一口径。
            _n_claim = len(glob.glob(os.path.join(PIPE, ".gha_claims", "*.json")))
            _gapn = (d["reconcile"].get("td_files") or 0) - (d["reconcile"].get("episode") or 0)
            if _n_claim and 0 < _gapn <= _n_claim:
                out.append("对账 gaps=%s（缺口 %d 集 ≤ 活动 claim %d 个，"
                           "属正在下载/待索引，非阻塞）" % (gaps, _gapn, _n_claim))
            else:
                out.append("对账 gaps=%s（Scan 已 Idle，属新片/改名尾部约 %d 集，"
                           "由 12h 周期扫描 + prewarm 自愈，非阻塞）"
                           % (gaps, _gapn))
    if led.get("err"):
        out.append("条目级对账未知（%s）→ 不能当「对账齐」，L3 闸门按未过处理"
                   % led["err"])
        blocking = True
    elif led.get("no_data"):
        out.append("条目级差异没有年龄信息（reconcile_gaps 无 age_min 列 / 对账器未升级）"
                   "→ 无法区分在途与孤儿，按未知处理")
        blocking = True
    elif led.get("stale"):
        out.append("条目级对账 %s 分钟没更新（embytg-reconcile.timer 停了？）→ 感知盲区，"
                   "五账差异无法确认" % led.get("run_age_min"))
        blocking = True
    if isinstance(led.get("aged"), int) and led["aged"] > 0:
        out.append("条目级对账 %d 条差异超过 %.0f 分钟未自愈（%s；例：%s）→ "
                   "teldrive/台账/Emby 之间的真裂缝，用 reconcile.py --repair 或人工处理"
                   % (led["aged"], led.get("grace_min", 30),
                      ",".join("%s×%d" % (k, v) for k, v in (led.get("kinds") or {}).items()),
                      "; ".join(led.get("sample") or [])[:130]))
        blocking = True
    elif isinstance(led.get("gaps"), int) and led["gaps"] > 0:
        out.append("条目级对账 %d 条差异在 %.0f 分钟容忍窗内（上传完成→写台账/索引的延迟，"
                   "在途非阻塞）" % (led["gaps"], led.get("grace_min", 30)))
    if backlog is None:
        out.append("三线队列读取失败（%s）→ 不能判定「队列已空」" % (lsnap.get("err") or "unknown"))
        blocking = True
    elif backlog > 0:
        parts = []
        for lane in ("new", "fill", "upgrade"):
            v = lq.get(lane) or {}
            if v.get("done"):
                parts.append("%s=DONE" % lane)
            elif lane == "fill":
                parts.append("fill=%d部/%d集" % (v.get("items", 0), v.get("episodes", 0)))
            elif lane == "upgrade":
                parts.append("upgrade=%d集" % v.get("episodes", 0))
            else:
                parts.append("new=%d部" % v.get("items", 0))
        if claims:
            out.append("三线待办：%s；当前有活动 claim=%s（正常推进中）"
                       % (" ".join(parts), ",".join(claims)))
        else:
            out.append("三线待办：%s；当前无活动 claim（等待 timer/源站候选/预算冷却，"
                       "不等于队列空）" % " ".join(parts))
    if not out:
        out.append("无阻塞项：Scan 非 Running、入库队列无积压（≤单轮消费上限）、三线 backlog=0、"
                   "loop 在跑、对账齐、日志全闭环")
    # cursor 在 tg.db.meta 是 TEXT，读出来是 '0' 字符串；pending 是 int → 必须都转 int 比
    try:
        cur_i = int(cursor) if cursor is not None else -1
    except (TypeError, ValueError):
        cur_i = -1
    # E021：队列「干净」= 没有未 apply 的条目（undone==0）。旧版 `pending == cursor` 是
    # 类别错误（行数 vs 单调游标）：11-13 实测 134 行全 done、cursor=144 → 被判【需收口】。
    # 老库没有 done 字段时再退回「pending ≤ cursor」（比相等安全：裁剪只会让行数变小）。
    # E142：队列「干净」= 没有超过单轮消费上限的在途条目（≤20 条属下一轮的正常在途，
    # 不是需要人收口的积压）。旧 collect 没有 pending_unapplied 时退回 E021 的 undone 口径。
    if isinstance(unapplied, int):
        queue_ok = (unapplied <= 20)
    elif isinstance(undone, int):
        queue_ok = (undone == 0)
    else:
        queue_ok = (isinstance(pending, int) and cur_i >= 0 and pending <= cur_i)
    stable = (not blocking and queue_ok)
    return ("稳定" if stable else "进行中/需收口"), out


def main():
    # 2026-11-15 自省（E037）：`collect()` 只返回 dict，**解包成 tuple 会让 --json 直接崩**，
    # 而崩掉时调用方常写的 `> f 2>&1` 会产出「空文件 + 非 0 rc」被吞进管道，
    # 上一轮就是靠这一步暴露出来的。现在改成 fail-loud：结构不对立刻抛，不许静默。
    d = collect()
    if not isinstance(d, dict):
        raise SystemExit("status_now.collect() 必须返回 dict，实际 %r" % type(d))
    state, notes = verdict(d)
    if "--json" in sys.argv:
        d["verdict"] = {"state": state, "notes": notes}
        print(json.dumps(d, ensure_ascii=False, indent=2))
        return
    print("=" * 72)
    print("EmbyTG 单一状态视图 | %s | 结论: 【%s】" % (d["ts"], state))
    print("=" * 72)
    print("[现场] mount=%s Emby=%s M/S/E=%s/%s/%s Scan=%s %s%%"
          % (d["mount"], d["emby_http"], d["counts"].get("Movie"), d["counts"].get("Series"),
             d["counts"].get("Episode"), d["scan"].get("state"), d["scan"].get("progress")))
    _led0 = d.get("ledger")
    _led = _led0 if isinstance(_led0, dict) else {}
    if _led.get("err"):
        _ltxt = "条目级 UNKNOWN(%s)" % _led["err"]
    else:
        _ltxt = ("条目级 %s条(超%.0fmin未愈 %s, 最大age %.0fmin, 对账 %.0fmin前)"
                 % (_led.get("gaps"), _led.get("grace_min", 30), _led.get("aged"),
                    _led.get("age_max_min") or 0, _led.get("run_age_min") or 0))
    print("[对账] 库级 gaps=%s td=%s ep=%s converged=%s | %s%s"
          % (d["reconcile"].get("gaps"), d["reconcile"].get("td_files"),
             d["reconcile"].get("episode"), d["converged"], _ltxt,
             ("  ERR=%s" % d["reconcile"]["err"]) if d["reconcile"].get("err") else ""))
    ng = d.get("nfo_gap") or {}
    if ng.get("stale"):
        print("[刮削] NFO缺口=STALE(旧报告 %s，%.1fh 前；不计入当前结论)"
              % (ng.get("report_mtime"), ng.get("age_h") or 0))
    else:
        print("[刮削] NFO缺口=%s" % ng)
    print("[入库] tg.db pending=%s(未apply=%s) cursor=%s state_key=%s integrity=%s | 近30分钟 视频+%s NFO+%s"
          % (d["ingest"].get("pending"),
             d["ingest"].get("pending_unapplied", d["ingest"].get("pending_undone")),
             d["ingest"].get("cursor"),
             d["ingest"].get("state_key"), d["ingest"].get("integrity"),
             d["recent_video_30m"], d["recent_nfo_30m"]))
    ln = d.get("lanes") or {}
    lq = ln.get("lanes") or {}

    def fmt_lane(lane):
        v = lq.get(lane) or {}
        if v.get("done"):
            return "%s=DONE" % lane
        if lane == "fill":
            return "fill=%d部/%d集" % (v.get("items", 0), v.get("episodes", 0))
        if lane == "upgrade":
            return "upgrade=%d集" % v.get("episodes", 0)
        return "new=%d部" % v.get("items", 0)

    claim_names = ln.get("claims") or []
    print("[队列] %s | 待办=%s | claim=%d%s"
          % (" ".join(fmt_lane(x) for x in ("new", "fill", "upgrade")),
             ln.get("backlog") if ln.get("backlog") is not None else "UNKNOWN",
             len(claim_names), (" " + ",".join(claim_names)) if claim_names else ""))
    print("[备份] LATEST=%s state=%s created=%s files=%s sha_ok=%s | 包数=%s"
          % (d["latest"].get("archive"), d["latest"].get("state"), d["latest"].get("created"),
             d["latest"].get("files"), d["latest"].get("sha256_ok"),
             len([x for x in os.listdir(BK) if x.startswith("embytg_backup_") and x.endswith(".tar.gz")])))
    print("[状态机] loop_pids=%s stage=%s 活跃业务进程=%d"
          % (d["loop_pids"], d["finalize_stage"], len(d["active_agents"])))
    for pid, s in d["active_agents"]:
        print("    PID %s %s" % (pid, s))
    print("[协作日志] 条目=%d 未闭环=%d（近3h %d / 历史 %d） 最后=%s"
          % (d.get("log_entries", 0), len(d["open_tasks"]),
             len(d.get("open_tasks_recent", [])), len(d.get("open_tasks_historical", [])),
             d.get("log_last")))
    for t in d.get("open_tasks_recent", []):
        print("    ! [%s] %s | %s | %s" % (t["time"], t["agent"], t["status"], t["task"]))
    print("[需注意]")
    for n in notes:
        print("    -", n)
    print("=" * 72)


if __name__ == "__main__":
    main()
