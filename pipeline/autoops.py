#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""autoops.py —— 无人值守运维闭环：感知 → 分析 → 决策 → 执行 → 留痕

用户 2026-11-10 定：「我要无人值守，你有义务：自动入库、刮削自动纠错、监控与自愈、
服务器磁盘满维护、优化服务器性能，感知→分析→决策→执行，全流程留痕与审计。」

设计三原则
----------
1. **只做可逆动作**：媒体文件永不删除，只移到 `_trash_*`；索引问题只 refresh，绝不 `DELETE /Items`。
2. **有闸门**：单周期动作数、同类动作冷却、日流量 300GB、扫描互斥；每步先审计后执行。
3. **留痕可复盘**：`logs/autoops_audit.jsonl` 每行一条（finding/decision/action/result）；
   `.autoops_state.json` 记连续失败与冷却，避免同一个坑无限重试。

用法
----
    python3 autoops.py --dry            # 只看会做什么
    python3 autoops.py --cycle          # 跑一轮（感知→执行）
    python3 autoops.py --loop 1800      # 常驻：每 30 分钟一轮
    python3 autoops.py --status         # 上一轮摘要
    python3 autoops.py --audit 20       # 最近 20 条审计
"""
import argparse
import fcntl
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
MNT = "/opt/embytg/mnt/embytg"
CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
EMBY = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
TOKEN = CFG["emby_token"]
AUDIT = os.path.join(HERE, "logs", "autoops_audit.jsonl")
STATE = os.path.join(HERE, ".autoops_state.json")
LOCK = "/tmp/autoops.lock"

MAX_ACTIONS_PER_CYCLE = 5
COOLDOWN = {"autoingest_run": 3600, "mount_restart": 1800, "refresh_lib": 600, "purge_trash": 21600,
            "nfo_fill": 1800, "refresh_series": 1800, "restart_container": 1800, "alert": 3600,
            "gha_watch": 1800}
DISK_WARN_GB, DISK_CRIT_GB = 20.0, 12.0

# 2026-11-15（E125）：已知的「同 Path 重复 Episode」基线条数。
# 2026-11-15 已清零：那条（偶像选拔 S01E03）是直接改 library.db 精确删掉的
# —— API 的 DELETE 会连文件一起删（E115），而 refresh 治不了重复。
# 超过基线（>0）就说明 TargetedScan 并发建条又发生了，值得看一眼（P2，不动手）。
DUP_EPISODE_BASELINE = 0
try:                                    # 阈值自适应（adapt_thresholds.py 生成，缺失用默认）
    import adapt_thresholds as _AT
    _th = _AT.load()
    DISK_WARN_GB, DISK_CRIT_GB = _th["disk_warn_gb"], _th["disk_crit_gb"]
except Exception:                                                  # noqa: BLE001
    pass
TRASH_KEEP_DAYS = 2
INGEST_PER_CYCLE_GB = 30.0
# 调度层自检：4h 看护 job 自己死了没人知道，由 systemd 层的 autoops 反查。
# 2026-11-13 交接：qwenpaw 已停用，改查 Hermes cron（/root/.hermes/cron/jobs.json）。
CRON_JOB = "abd003ea5889"     # 运维守护-四小时看护（Hermes cron job id）
CRON_LATE_MIN = 30       # next_run_at 已过 30min 仍未触发 = 调度停摆
# 2026-11-14：警戒线必须与排程周期匹配。CRON_JOB 是 `45 */6 * * *`（周期 360min），
# 原来写死 5*60=300 → 每轮都判「已 326 分钟没有执行」误报 P1 并发邮件。
# 2026-11-18（E177）：改 8h 排程后 420 又不够用 → 死线改成由 jobs.json 的 schedule 现算
# （cron_stale_limit = 周期 + 120 缓冲）。这个常数只是「周期解析失败」时的兜底。
CRON_STALE_MIN = 420
# 2026-11-14：任务**正在跑**不算悬挂。`45 */6` 的巡检每轮会跑十几分钟到半小时，
# autoops 每 30min 探一次，若不做「在跑」豁免就会把正常执行判成
# 「last_status=error 执行超过 120 分钟未结束（疑似卡死）」→ 误报 P1 + 发邮件
# （实测 06:49 邮件：当轮 06:45 才开始跑）。判据改成：
#   在跑且 started_at 距今 < CRON_RUNNING_MAX_MIN → 正常，不告警；
#   在跑且超过该上限 → 才是真卡死。
CRON_RUNNING_MAX_MIN = 240
HERMES_CRON_JOBS = "/root/.hermes/cron/jobs.json"
SCAN_STALL_MIN = 45
# 2026-11-14（E086）：多久没有新片入库算「停摆」。取 45 分钟——
# 正常产能 20~30 条/h，45 分钟零产出已经明显异常，又不会被单集慢下载误报。
STALL_OUTPUT_MIN = int(os.environ.get("STALL_OUTPUT_MIN", "45"))


def _last_success_age_min():
    """最近一次成功入库距今多少分钟。

    2026-11-14（E086）：判据用 **tg.db.resource_state.landed_at**，
    不要读 runner.log。
    实测教训：new5 云盒完成的集不会写本机 runner.log（日志在云盒上），
    于是「最近成功」一直停在 13:04 而实际 15:29 还在进片 ——
    按日志判会误报停滞。
    resource_state 是五账之一，由 rsscan 每 30 分钟从挂载重建，
    云盒/本机/转存三条路都在里面，是唯一可靠的「真的进了新片」证据。
    """
    import time as _t
    try:
        con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                              uri=True, timeout=20)
        try:
            row = con.execute(
                "select max(landed_at) from resource_state "
                "where kind='episode' and landed_at is not null").fetchone()
        finally:
            con.close()
        ts = float((row or [0])[0] or 0)
        if ts > 1e12:            # 毫秒容错
            ts /= 1000.0
        if ts <= 0:
            return None
        return (_t.time() - ts) / 60.0
    except Exception:                                      # noqa: BLE001
        return None


def _kill_orphan_claims():
    """清掉「claim 在、但本机没有任何 runner/上传进程」的僵尸。

    只动本机 lane（run_id 含 slot1 或 lane 属于本机），云盒的 claim
    由云盒进程消费，本机看不见进程，不能误杀。
    """
    import glob as _glob
    import subprocess as _sp
    try:
        out = _sp.run(["pgrep", "-af",
                       "run_one.sh|gha_download.py|gha_upload.py|pc_upload.py"],
                      capture_output=True, text=True, timeout=15).stdout
    except Exception:                                      # noqa: BLE001
        return []
    killed = []
    for fp in _glob.glob(os.path.join(HERE, ".gha_claims", "*.json")):
        try:
            d = json.load(open(fp, encoding="utf-8"))
        except Exception:                                  # noqa: BLE001
            continue
        rid = d.get("run_id") or ""
        lane = d.get("lane") or ""
        if "slot1" not in rid and lane not in ("new2", "fill", "upgrade"):
            continue                       # 云盒 lane，不归本机管
        if rid and rid not in out:
            try:
                os.remove(fp)
                killed.append(lane)
            except Exception:                              # noqa: BLE001
                pass
        elif not rid:
            try:
                os.remove(fp)
                killed.append(lane)
            except Exception:                              # noqa: BLE001
                pass
    return killed


def _active_claim_count():
    import glob as _glob
    n = 0
    for _ in _glob.glob(os.path.join(HERE, ".gha_claims", "*.json")):
        n += 1
    return n      # 库扫描百分比连续不变超过此值 = ffprobe/rclone 读挂死拖住索引（E020）


def _ingest_progress_age_min():
    """在飞任务最后一次写进度的分钟数（None = 没有任何在飞任务）。

    2026-11-15 23:50（E137）：**「45 分钟没新片」不等于「卡住」。**
    实测一次：4 条 lane 全在传大文件（fill 那集 2160p 7.19GB、session 实测
    0.32MB/s，单集 6 小时），45 分钟窗口里自然没有一集收尾 → output_stalled
    触发 → `reap_and_restart` 无条件 `systemctl restart embytg-runner`，
    把正在传的 2.11GB 上传杀掉，下一轮从头重传（同一集 22:52 与 23:08 各传一次）。

    判据（与 claim_reap 同口径）：**有带 run_id 的活进程且在上写进度 = 没卡**。
    进度证据取 work/*/{upload,download}.log 的最新 mtime —— 上传每 33.6MB 一段、
    下载按百分比刷新，正常传输时这两个文件一直在动；真卡住时它们是静止的。
    """
    import glob as _glob
    base = os.environ.get("EMBYTG_RUNNER_WORK", "/opt/embytg-runner/work")
    newest = 0.0
    for pat in ("upload.log", "download.log"):
        for p in _glob.glob(os.path.join(base, "*", pat)):
            try:
                mt = os.path.getmtime(p)
            except OSError:
                continue
            if mt > newest:
                newest = mt
    if not newest:
        return None
    return (time.time() - newest) / 60.0


def _budget_state():
    """读预算闸门最近一次的决策（budget_gate.write_state 落盘）。"""
    p = os.environ.get("TG_BUDGET_STATE") or os.path.join(
        HERE, "logs", ".budget_gate_state.json")
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except Exception:                                      # noqa: BLE001
        return None


def _budget_pause_state():
    """闸门此刻是否在「按设计暂停」。返回 (reason, 剩余暂停分钟) 或 None。

    2026-11-16 11:41（E153）：日预算见底后 run_one.sh 按设计
    `IDLE_BUDGET_PAUSE` 睡 30 分钟 —— 这段时间**一集也不该落地**。旧判据只认
    「45 分钟没新片」，于是每 30 分钟 reap+restart 一次，11:41 还升级成
    `output_stalled_unfixed`。判据必须读闸门的结论：rc!=0 且还在它自己要求的
    等待窗口内 = 停是有意的；窗口过期后仍无产出，才是真故障。
    """
    d = _budget_state()
    if not d:
        return None
    try:
        ts = float(d.get("ts") or 0)
        rc = int(d.get("rc") or 0)
        wait = float(d.get("wait_sec") or 0)
    except Exception:                                      # noqa: BLE001
        return None
    if not ts or rc == 0:
        return None
    age = time.time() - ts
    if age > wait + 300:      # +5min 余量：runner 该回来重新问闸门了
        return None
    return (str(d.get("reason") or "budget"), max(0.0, (wait - age) / 60.0))


def _median_episode_gb():
    """近 300 集的中位体积（GB），把「预算 pace」折成「多久该出一集」。"""
    try:
        con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                              uri=True, timeout=15)
        try:
            rows = sorted(float(r[0]) for r in con.execute(
                "select size from resource_state where kind='episode' and size>0 "
                "order by landed_at desc limit 300"))
        finally:
            con.close()
        if not rows:
            return None
        return rows[len(rows) // 2] / 1e9
    except Exception:                                      # noqa: BLE001
        return None


def _observed_ep_rate_gb_h(hours=3.0):
    """近 N 小时的实际落地速率（GB/h）。判「多久该出一集」用它，而不是闸门的
    理论放行量 —— 用户口径改成「不做小时节流、只看日/月额度」后，闸门的
    allow_gb_h 在额度充足时等于剩余额度（几百 GB/h），拿它折算毫无意义。"""
    try:
        con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                              uri=True, timeout=15)
        try:
            row = con.execute(
                "select coalesce(sum(size),0), count(*) from resource_state "
                "where kind in ('episode','movie') and landed_at>=?",
                (int(time.time() - hours * 3600),)).fetchone()
        finally:
            con.close()
        gb = float((row or [0])[0] or 0) / 1e9
        n = int((row or [0, 0])[1] or 0)
        if n <= 0:
            return None
        return gb / hours
    except Exception:                                      # noqa: BLE001
        return None


def _stall_threshold_min():
    """停摆阈值跟「实际产能」缩放，别拿满速时代的 45 分钟卡限速时代。

    2026-11-16（E153）：闸门限速时速度会掉到 2~3 GB/h，单集中位 1.44GB ≈ 35
    分钟一集；再加上 landed_at 由 rsscan 每 30 分钟从挂载重建（账上时间点本来
    就批量、滞后），45 分钟窗口结构性误报 —— 今日实测量到的相邻落地间隔最大
    48 分钟。判据改成 max(45, 2×预期间隔 + 20)，上限 180 分钟：
    只有「明显慢于当前实际产能」才叫停摆。
    """
    ep = _median_episode_gb()
    rate = _observed_ep_rate_gb_h()
    if not ep or not rate:
        return STALL_OUTPUT_MIN      # 没数据可折算：保留老判据，别把真停滞放过
    expect = ep / rate * 60.0        # 按当前实际速率，一集大约多少分钟
    return int(min(180, max(STALL_OUTPUT_MIN, expect * 2 + 20)))
REFRESH_MAX_TRIES = 2    # 同一部剧 6h 内 refresh 次数上限，超了改告警不再空转
REFRESH_TRIES_WINDOW = 21600


def hermes_cron_jobs():
    """读 Hermes cron 任务表（/root/.hermes/cron/jobs.json）。读不到返回 []。
    2026-11-13 交接：qwenpaw 停用后，调度层状态以此为准。"""
    try:
        with open(HERMES_CRON_JOBS, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:                                   # noqa: BLE001
        audit("sense_error", where="hermes_cron", err=str(e)[:120])
        return []
    return data if isinstance(data, list) else (data or {}).get("jobs", [])


HERMES_CRON_DB = "/root/.hermes/cron/executions.db"


def hermes_cron_last_exec(job_id):
    """从 Hermes executions.db 取该 job 最近一次执行（只读）；读不到返回 {}。
    jobs.json 只保存调度计划（last_status/last_run_at 恒为 None），
    真正的执行结果在 executions.db，不查它则「任务一直报错」这条判据永远是死的。

    2026-11-13 补：同一 job 并发多条 execution 时，被 claim 机制丢弃的那条会写
    status=failed + error='Fire claim ownership lost; stale result was discarded.'，
    而同一时刻另一条同 job execution 是 completed。若直接取 started_at 最新一条，
    这条**调度器内部竞态**会被当成「任务一直报错」→ 误报 P1 cron_stale
    （实测 09:49 因人工 `hermes cron run` 与定时触发重叠而误报）。
    故查询时排除该竞态签名，只认真正的执行结果。"""
    RACE_ERR = "%claim ownership lost%"
    try:
        con = sqlite3.connect("file:%s?mode=ro" % HERMES_CRON_DB, uri=True, timeout=3)
        try:
            row = con.execute(
                "select status, started_at, finished_at, error from executions "
                "where job_id = ? and (error is null or error not like ?) "
                "order by started_at desc limit 1", (job_id, RACE_ERR)).fetchone()
        finally:
            con.close()
    except Exception as e:                                   # noqa: BLE001
        audit("sense_error", where="hermes_exec_db", err=str(e)[:120])
        return {}
    if not row:
        return {}
    return {"status": row[0], "started_at": row[1], "finished_at": row[2], "error": row[3]}


def sense_cron_all():
    """全量调度任务体检（不只盯看护 job）：next_run 已过期 >180min 或最近执行 error。
    显式 enabled=false 视为人工决定，不告警（避免替人做决定的噪音）。"""
    bad, now = [], time.time()
    for j in hermes_cron_jobs():
        jid = j.get("id")
        if not jid or jid == CRON_JOB or j.get("enabled") is False:
            continue
        nxt = _parse_ts(j.get("next_run_at"))
        if nxt and (now - nxt) / 60.0 > 180:
            bad.append({"id": jid, "name": j.get("name"),
                        "why": "next_run 已过期 %.0f 分钟" % ((now - nxt) / 60.0)})
            continue
        ex = hermes_cron_last_exec(jid)
        if ex.get("status") in ("error", "failed", "timeout"):
            bad.append({"id": jid, "name": j.get("name"),
                        "why": "最近执行 %s：%s" % (ex.get("status"), str(ex.get("error"))[:120])})
    return bad


def send_alert(level, code, msg, extra=None):
    """统一告警出口（local jsonl + P0 广播 + 可选 TG）；失败不影响主流程"""
    try:
        import importlib
        A = importlib.import_module("alert")
        A.alert(level, code, msg, extra)
    except Exception as e:                                          # noqa: BLE001
        audit("alert_fail", code=code, err=str(e)[:80])


# ---------------- 基础设施 ----------------
def audit(kind, **kw):
    rec = {"ts": time.strftime("%F %T"), "kind": kind}
    rec.update(kw)
    os.makedirs(os.path.dirname(AUDIT), exist_ok=True)
    with open(AUDIT, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def load_state():
    if os.path.exists(STATE):
        try:
            return json.load(open(STATE, encoding="utf-8"))
        except Exception:                                          # noqa: BLE001
            pass
    return {"cycle": 0, "last": {}, "fails": {}}


def save_state(s):
    json.dump(s, open(STATE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def cooled(st, key):
    return time.time() - st["last"].get(key, 0) >= COOLDOWN.get(key, 300)


def mark(st, key):
    st["last"][key] = time.time()


def run(cmd, timeout=900):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, ((r.stdout or "") + (r.stderr or "")).strip()
    except Exception as e:                                          # noqa: BLE001
        return 1, str(e)


def emby(path, method="GET", body=None, timeout=120):
    req = urllib.request.Request(EMBY + path,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 method=method, headers={"X-Emby-Token": TOKEN, "Content-Type": "application/json"})
    raw = urllib.request.urlopen(req, timeout=timeout).read().decode()
    return json.loads(raw) if raw.strip().startswith(("{", "[")) else raw


# ---------------- 感知 ----------------
def _parse_ts(v):
    """ISO / epoch 时间戳 → epoch；解析失败返回 None。

    2026-11-14 根因修复：Hermes executions.db 的 started_at 形如
    `2026-11-14T06:45:45.074091+08:00`。旧实现 `[:19]` 把 `+08:00` 截掉再用服务器
    本地时区(time.mktime)解析 —— 一旦进程 TZ 不是 +08:00（systemd 默认 UTC）就整体偏 8h，
    「执行了 357 分钟」其实是 6 分钟，于是把**正在正常跑**的巡检判成卡死并发出 P1 邮件。
    现在：带时区的交给 datetime.fromisoformat（认偏移），纯 epoch 数字直接返回，
    都失败才回落到旧的 strptime 路径。
    """
    if v is None or v == "":
        return None
    s = str(v).strip()
    try:
        return float(s)                          # epoch 秒
    except ValueError:
        pass
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.astimezone()                 # 无时区 = 本地时间，按本地解释
        return dt.timestamp()
    except Exception:                            # noqa: BLE001
        pass
    try:
        return time.mktime(time.strptime(s.replace("T", " ")[:19], "%Y-%m-%d %H:%M:%S"))
    except Exception:                                            # noqa: BLE001
        return None


_CRON_DOW_NAMES = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}


def _cron_field(spec, lo, hi):
    """单个 cron 字段 → 允许值集合（`*` / 列表 / 区间 / 步长 / 星期名）；解析不了返回 None。"""
    vals = set()
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            return None
        step = 1
        if "/" in part:
            part, _, st = part.partition("/")
            try:
                step = int(st)
            except ValueError:
                return None
            if step <= 0:
                return None
        if part in ("*", "?"):
            a, b = lo, hi
        elif "-" in part:
            a, _, b = part.partition("-")
            try:
                a, b = int(a), int(b)
            except ValueError:
                return None
        else:
            low = part.lower()
            try:
                a = _CRON_DOW_NAMES[low] if low in _CRON_DOW_NAMES else int(part)
            except ValueError:
                return None
            b = hi if step > 1 else a
        if a < lo or b > hi or a > b:
            return None
        vals.update(range(a, b + 1, step))
    return vals or None


def cron_period_min(expr):
    """cron 表达式的排程周期（分钟，相邻两次 fire 间隔的中位数）；解析失败返回 None。

    2026-11-18（E177）：09:30 的排程复审把「五路生产巡检」从 `45 */6`（360min）
    改成 `45 0,8,16`（480min），而 CRON_STALE_MIN 里写死的 420 没人跟着改 →
    13:45 起每轮都判「已 449 分钟没有执行」的假 P1。死线必须由排程自己算，
    不能再靠人肉同步常数（用户要求「加/改一个就自动生效」）。
    做法：按标准 cron 语义枚举未来几次 fire 时刻，取间隔中位数。
    """
    from datetime import datetime, timedelta
    parts = str(expr or "").split()
    if len(parts) != 5:
        return None
    minutes = _cron_field(parts[0], 0, 59)
    hours = _cron_field(parts[1], 0, 23)
    doms = _cron_field(parts[2], 1, 31)
    mons = _cron_field(parts[3], 1, 12)
    dows = _cron_field(parts[4], 0, 7)
    if not (minutes and hours and doms and mons and dows):
        return None
    dows = {d % 7 for d in dows}                     # 7 = 周日，与 0 同义
    dom_star = parts[2].strip() in ("*", "?")
    dow_star = parts[4].strip() in ("*", "?")
    base = datetime.now().replace(second=0, microsecond=0)
    day = base.date()
    fires = []
    for _ in range(400):                             # 上限：覆盖年度排程
        day_ok = None
        if day.month in mons:
            if dom_star and dow_star:
                day_ok = True
            elif dom_star:
                day_ok = (day.weekday() + 1) % 7 in dows
            elif dow_star:
                day_ok = day.day in doms
            else:                                     # dom 与 dow 都限定时按标准 cron 取并集
                day_ok = day.day in doms or (day.weekday() + 1) % 7 in dows
        if day_ok:
            for h in sorted(hours):
                for mi in sorted(minutes):
                    t = datetime(day.year, day.month, day.day, h, mi)
                    if t >= base:
                        fires.append(t)
            if len(fires) >= 4:
                break
        day += timedelta(days=1)
    if len(fires) < 3:
        return None
    gaps = sorted((fires[i + 1] - fires[i]).total_seconds() / 60.0
                  for i in range(len(fires) - 1))
    return round(gaps[len(gaps) // 2], 1)


def schedule_period_min(sched):
    """jobs.json 的 schedule 结构 → 周期分钟（cron 或 interval 都支持）。"""
    if not isinstance(sched, dict):
        return None
    if str(sched.get("kind") or "").lower() == "interval":
        try:
            m = float(sched.get("minutes") or 0)
        except (TypeError, ValueError):
            return None
        return m or None
    return cron_period_min(sched.get("expr"))


def cron_stale_limit(period_min):
    """看护 job 多久没跑才算「调度层死了」：排程周期 + 120 分钟缓冲（下限 240）。

    last_run_at 记的是**完成**时刻，而下一轮 fire 在周期末尾，所以空窗 =
    周期 + 上一轮执行时长（实测 7~28 分钟）；再留一次派发延迟的余量。
    缓冲 90 分钟时 8h 排程只剩 17 分钟余量（06:58 完成 → 16:45 fire = 587 > 570），
    派发晚几分钟就会在 fire 前误报；取 120 才有可用的余量。
    """
    if not period_min or period_min <= 0:
        return CRON_STALE_MIN
    return int(max(240, period_min + 120))


def sense_cron():
    """反查 Hermes 调度层状态；失败只留痕不抛（autoops 本身不能因此挂掉）。
    Hermes jobs.json 字段：enabled / last_status / last_error / last_run_at / next_run_at。
    next_age_min > 0 表示 next_run_at 已过期（调度停摆）。"""
    jobs = hermes_cron_jobs()
    st = next((j for j in jobs if j.get("id") == CRON_JOB), None)
    if st is None:
        return {"probe_err": "hermes_job_missing", "enabled": None, "status": None,
                "err": None, "in_flight": False, "run_age_min": None,
                "last_run_age_min": None, "next_age_min": None, "period_min": None}
    now = time.time()
    last = _parse_ts(st.get("last_run_at"))
    nxt = _parse_ts(st.get("next_run_at"))
    status, err = st.get("last_status"), st.get("last_error")
    # 2026-11-13：jobs.json 不写执行结果，改从 executions.db 补 last_status / 最后执行时间。
    ex = hermes_cron_last_exec(CRON_JOB)
    running = False
    running_min = None
    if ex:
        if last is None:
            last = _parse_ts(ex.get("started_at"))
        status = status or ex.get("status")
        err = err or ex.get("error")
        if ex.get("status") == "running":
            # 在跑 ≠ 卡死：只有「跑太久」才算异常（见 CRON_RUNNING_MAX_MIN）。
            # 必须用本条 execution 的 started_at：jobs.json 的 last_run_at 只在**上一次完成**时写，
            # 任务在跑时它还是 6 小时前的值，拿它算「跑了多久」会把 7 分钟算成 358 分钟。
            started = _parse_ts(ex.get("started_at"))
            if started is not None:
                last = started
            running_min = (now - last) / 60.0 if last else 0.0
            running = running_min < CRON_RUNNING_MAX_MIN
            if not running:
                status = "error"
                err = "执行超过 %d 分钟未结束（疑似卡死）" % CRON_RUNNING_MAX_MIN
            else:
                status, err = "running", None
        elif ex.get("status") in ("failed", "timeout"):
            status = "error"
    return {"status": status, "err": err,
            "enabled": bool(st.get("enabled")),
            "running": running,
            "running_min": round(running_min, 1) if running_min is not None else None,
            "in_flight": bool(running),
            "run_age_min": None,
            "last_run_age_min": round((now - last) / 60.0, 1) if last else None,
            "next_age_min": round((now - nxt) / 60.0, 1) if nxt else None,
            # 死线随排程走：排程改了周期，判据自动跟着变（见 cron_stale_limit）。
            "period_min": schedule_period_min(st.get("schedule"))}


def sense():
    f = {}
    du = shutil.disk_usage("/")
    f["disk_free_gb"] = round(du.free / 1024 ** 3, 1)
    f["disk_used_pct"] = round(du.used * 100.0 / du.total, 1)

    # 挂载
    try:
        os.stat(os.path.join(MNT, "TV"))
        f["mount_ok"] = True
    except Exception:                                              # noqa: BLE001
        f["mount_ok"] = False

    rc, out = run("docker ps --filter health=unhealthy --format '{{.Names}}'")
    f["unhealthy"] = [x for x in out.split() if x]
    f["restarts"] = {}
    for unit in ("embytg-autoops", "embytg-finalize", "embytg-mount"):
        rc, out = run("systemctl show %s -p NRestarts --value" % unit, timeout=20)
        try:
            f["restarts"][unit] = int((out or "0").strip())
        except ValueError:
            f["restarts"][unit] = 0
    rc, out = run("docker ps -q | wc -l")
    f["containers"] = int(out.strip() or 0)

    # tg.db
    try:
        con = sqlite3.connect(os.path.join(HERE, "tg.db"))
        f["db_ok"] = con.execute("pragma quick_check").fetchone()[0] == "ok"
        f["pending"] = con.execute("select count(*) from pending").fetchone()[0]
    except Exception:                                              # noqa: BLE001
        f["db_ok"], f["pending"] = False, -1

    # Emby
    try:
        f["episodes"] = emby("/Items?" + urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                                                 "Limit": "1", "EnableTotalRecordCount": "true"})).get("TotalRecordCount")
        f["series"] = emby("/Items?" + urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                                                               "Limit": "1", "EnableTotalRecordCount": "true"})).get("TotalRecordCount")
        tasks = emby("/ScheduledTasks")
        scan = [t for t in tasks if "Scan media" in (t.get("Name") or "")][0]
        _scan_audit(scan)
        f["scan_minutes"] = 0.0
        f["scan_stuck"] = False
        if scan.get("State") == "Running":
            from datetime import datetime as _dt, timezone as _tz
            try:
                st_iso = (scan.get("LastExecutionResult") or {}).get("StartTimeUtc")
                if st_iso:
                    st_dt = _dt.fromisoformat(st_iso.replace("Z", "+00:00"))
                    f["scan_minutes"] = round((_dt.now(_tz.utc) - st_dt).total_seconds() / 60, 1)
                    f["scan_stuck"] = f["scan_minutes"] > SCAN_LIMIT_MIN
            except Exception:                                      # noqa: BLE001
                pass
        f["scan_state"] = scan.get("State")
        f["scan_pct"] = round(scan.get("CurrentProgressPercentage") or 0, 1)
        f["sessions"] = len([s for s in emby("/Sessions") if s.get("NowPlayingItem")])
    except Exception as e:                                          # noqa: BLE001
        f["scan_state"] = "unknown"
        f["sessions"] = -1
        audit("sense_error", where="emby", err=str(e)[:120])

    # 磁盘侧 vs Emby（按库）
    try:
        import lib_reconcile as LR
        libs = LR.tv_lib_names()
        td = LR.teldrive_by_lib(libs)
        em, total = LR.emby_by_lib()
        # 2026-11-15（E125）：**比较前先扣掉「同 Path 重复条目」。**
        #
        # 重复条目（TargetedScan 并发建条，E115 留证）会让 Emby 计数永远比磁盘多 N，
        # 而 N 是刷不掉的：refresh 治不了重复，DELETE 又会把真实视频一起删。
        # 原来直接比较 → 每轮都判 index_diff → 升级成 P1 邮件（实测 17:26 发了一封）。
        #
        # 正确口径：磁盘文件数 vs (Emby 集数 - 重复条目数)。
        # 这样真缺口（emby < disk）照样报，重复条目不再误报。
        dups = {}
        try:
            for k in libs:
                n_dup = LR.dup_episode_count(k)
                if n_dup:
                    dups[k] = len(n_dup)
        except Exception:                                      # noqa: BLE001
            dups = {}
        f["lib_dups"] = dups
        f["lib_diff"] = {k: (td.get(k, (0, 0))[0], em.get(k, 0) - dups.get(k, 0))
                         for k in libs
                         if td.get(k, (0, 0))[0] != em.get(k, 0) - dups.get(k, 0)}
        f["td_files"] = sum(v[0] for v in td.values())
        f["emby_ep"] = total
    except Exception as e:                                          # noqa: BLE001
        f["lib_diff"] = {}
        audit("sense_error", where="reconcile", err=str(e)[:120])

    # 缺口 & 预算
    try:
        import ingest_quota as Q
        c = Q.cfg()
        d_gb, _, m_gb, _ = Q.usage()
        # 2026-11-16（E153 附）：日上限的唯一真源是闸门 —— 它读
        # /opt/embytg-runner/runner.env 的 TG_BUDGET_DAILY_GB（当前 500），
        # 而 .ingest_budget.json 还是 300。两边不一致时，「预算余 -25GB」和
        # 「闸门全速跑」会同时存在，看日志的人（和下一轮的 agent）会判错。
        _gs = _budget_state()
        _fresh = bool(_gs) and time.time() - float((_gs or {}).get("ts") or 0) < 1800
        _daily = float((_gs or {}).get("daily_gb") or c["daily_gb"]) if _fresh else c["daily_gb"]
        f["quota"] = {"today": round(d_gb, 1), "month": round(m_gb, 1),
                      "daily_gb": round(_daily, 1),
                      "left": round(min(_daily - d_gb, c["safety_gb"] - m_gb), 1)}
    except Exception:                                              # noqa: BLE001
        f["quota"] = {"today": 0, "month": 0, "left": 0}

    # 垃圾目录体积
    tr = 0
    for d in os.listdir(HERE):
        if d.startswith(("_trash_", "_nfo_quarantine_")):
            p = os.path.join(HERE, d)
            try:
                mtime = os.path.getmtime(p)
                sz = sum(os.path.getsize(os.path.join(dp, fn)) for dp, _, fs in os.walk(p) for fn in fs
                         if os.path.exists(os.path.join(dp, fn)))
                tr += sz
                f.setdefault("trash", []).append({"dir": d, "gb": round(sz / 1e9, 2),
                                                  "age_days": round((time.time() - mtime) / 86400, 1)})
            except Exception:                                      # noqa: BLE001
                pass
    f["trash_gb"] = round(tr / 1e9, 2)
    f["cron"] = sense_cron()
    f["cron_all"] = sense_cron_all()
    return f


SCAN_HIST = os.path.join(HERE, ".scan_history.jsonl")
SCAN_LIMIT_MIN = 60          # 全库扫描跑超过 60 分钟 = 视为卡死（FUSE 上 ffprobe 慢）


def _scan_audit(scan):
    """记录每次扫描的 开始/结束/时长/触发键，并落 .scan_history.jsonl（回答"谁在扫、多久一次"）"""
    try:
        res = scan.get("LastExecutionResult") or {}
        key = "%s|%s" % (res.get("StartTimeUtc"), res.get("Status"))
        st = load_state()
        if st.get("scan_last_key") != key and res.get("StartTimeUtc"):
            rec = {"ts": time.strftime("%F %T"), "start_utc": res.get("StartTimeUtc"),
                   "end_utc": res.get("EndTimeUtc"), "status": res.get("Status"),
                   "task_key": res.get("Key"), "state": scan.get("State"),
                   "progress": round(scan.get("CurrentProgressPercentage") or 0, 1)}
            with open(SCAN_HIST, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            st["scan_last_key"] = key
            save_state(st)
    except Exception:                                              # noqa: BLE001
        pass


# ---------------- 分析 ----------------
def scan_stalled(f, st):
    """库扫描停在同一个百分比超过 SCAN_STALL_MIN 分钟 → 真的卡了。
    E020（2026-11-12）：个别 4K 文件在 teldrive 后端读不回来，容器内 ffprobe 进 D 态永不返回，
    Emby 的扫描队列被串行堵死 → 新集不入库（现象：磁盘 12 集 / Emby 11 集，refresh 也无用）。
    判据用「百分比多久没变」，不看单次百分比值（正常扫描也会停在某个整数很久）。"""
    if f.get("scan_state") != "Running":
        return None
    pct = round(float(f.get("scan_pct") or 0), 2)
    now = time.time()
    hist = st.setdefault("scan_hist", [])
    if not hist or abs(float(hist[-1].get("pct", -1)) - pct) > 0.01:
        hist.append({"pct": pct, "ts": now, "last": now})      # ts = 这个百分比第一次出现的时刻
    else:
        hist[-1]["last"] = now
    st["scan_hist"] = hist[-6:]
    stall_s = now - float(st["scan_hist"][-1].get("ts", now))
    if stall_s >= SCAN_STALL_MIN * 60:
        return pct, stall_s
    return None


def analyze(f, st=None):
    """返回 [(severity, code, msg, suggested_action)]"""
    out = []
    if not f.get("mount_ok"):
        out.append(("P0", "mount_down", "rclone 挂载不可用", "restart_mount"))
    if f["disk_free_gb"] < DISK_CRIT_GB:
        out.append(("P0", "disk_crit", "磁盘仅剩 %.1f GB" % f["disk_free_gb"], "purge_trash+alert"))
    elif f["disk_free_gb"] < DISK_WARN_GB:
        out.append(("P1", "disk_warn", "磁盘剩 %.1f GB" % f["disk_free_gb"], "purge_trash"))
    if not f.get("db_ok"):
        out.append(("P0", "db_bad", "tg.db quick_check 失败", "alert"))
    if f.get("unhealthy"):
        out.append(("P1", "container_bad", "unhealthy: %s" % ",".join(f["unhealthy"]), "restart_container"))
    if f.get("lib_diff"):
        out.append(("P1", "index_diff", "库索引差: %s" % json.dumps(f["lib_diff"], ensure_ascii=False),
                    "refresh_series"))
    # 2026-11-15（E125 续）：**重复条目本身不告警，但「变多」要告警。**
    # 已知基线 = 1 条（偶像选拔 S01E03，E115 成因，清它有丢片风险所以留着）。
    # 一旦超过基线，说明 TargetedScan 的并发建条又发生了 —— 那才是新问题，
    # 值得看一眼（P2 级别，不半夜发 P1）。
    _dups = f.get("lib_dups") or {}
    _dup_total = sum(int(v or 0) for v in _dups.values())
    if _dup_total > DUP_EPISODE_BASELINE:
        out.append(("P2", "dup_episode_growth",
                    "重复 Episode 条目增至 %d 条（基线 %d）：%s"
                    % (_dup_total, DUP_EPISODE_BASELINE,
                       json.dumps(_dups, ensure_ascii=False)),
                    "note_only"))
    if f["disk_free_gb"] < DISK_CRIT_GB:
        out.append(("P0", "disk_crit", "磁盘仅剩 %.1f GB" % f["disk_free_gb"], "teldrive_cache"))
    elif f["disk_free_gb"] < DISK_WARN_GB:
        out.append(("P1", "disk_warn", "磁盘剩 %.1f GB" % f["disk_free_gb"], "deep_clean"))
    if f.get("scan_stuck"):
        out.append(("P1", "scan_stuck", "全库扫描已跑 %s 分钟（>%d）" % (f.get("scan_minutes"), SCAN_LIMIT_MIN), "stop_scan"))
    # 2026-11-14（E086）：**产出停滞必须主动告警，不能等人发现**。
    # 当天实测：两个 claim 卡在 post_ingest 占满 session 槽，
    # 上传停了 2 小时 50 分钟，直到用户问「频道很久没新的了」才暴露。
    # 判据：最近一次成功入库距今超过 STALL_OUTPUT_MIN 分钟，
    # 且当前确实有 claim 占着槽（证明不是"本来就没活"）。
    try:
        _stale = _last_success_age_min()
        _claims = _active_claim_count()
        _prog = _ingest_progress_age_min()
        # 2026-11-15（E137）：在飞任务仍在写进度（<10 分钟内有新日志）= 只是慢，不是卡。
        # 只判「距上次成功多久」会把「4 条 lane 都在传 2160p 大文件」误判成停滞，
        # 进而 restart 杀掉正在上传的字节（实测同一集被重传两次）。
        _busy = _prog is not None and _prog < 10
        # 2026-11-16（E153）：**预算闸门按设计暂停时，没有产出不是故障。**
        # 日预算见底 → run_one.sh 主动 IDLE_BUDGET_PAUSE 睡 30 分钟；旧判据照
        # 45 分钟判「停滞」→ 每 30 分钟 reap+restart，11:41 还升级成告警。
        # 阈值也随闸门 pace 缩放（_stall_threshold_min），别拿满速时代的 45 分钟卡匀速。
        _thr = _stall_threshold_min()
        _bp = _budget_pause_state()
        if _stale is not None and _stale > _thr and _bp:
            audit("note", code="budget_pause",
                  msg="已 %.0f 分钟无新片，但预算闸门在暂停（%s，约 %.0f 分钟后重试），属设计内"
                      % (_stale, _bp[0], _bp[1]))
        elif _stale is not None and _stale > _thr and _claims > 0 and not _busy:
            out.append(("P1", "output_stalled",
                        "已 %.0f 分钟无新片入库（阈值 %.0f 分钟），且有 %d 个 claim 占着 slot（在飞进度 %.0f 分钟未动，疑似卡单）"
                        % (_stale, _thr, _claims, _prog if _prog is not None else -1), "reap_and_restart"))
        elif _stale is not None and _stale > _thr and _claims > 0:
            audit("note", code="output_slow",
                  msg="已 %.0f 分钟无新片，但 %d 个 claim 仍在写进度（%.1f 分钟前），判定为在传大文件而非卡单"
                      % (_stale, _claims, _prog))
    except Exception:                                      # noqa: BLE001
        pass
    try:
        r = json.loads(run("python3 %s/scan_watch.py --json" % HERE, timeout=60)[1] or "{}")
        if r.get("stuck"):
            out.append(("P1", "scan_stuck", "全库扫描已跑 %s 分钟（>%d）" % (r.get("minutes"), SCAN_LIMIT_MIN), "alert"))
    except Exception:                                              # noqa: BLE001
        pass
    rs = f.get("restarts") or {}
    for unit, n in rs.items():
        if n and n >= 3:
            out.append(("P1", "restart_storm", "%s 已重启 %d 次" % (unit, n), "alert"))
    if f.get("trash_gb", 0) > 2:
        out.append(("P2", "trash_big", "回收目录 %.1f GB" % f["trash_gb"], "purge_trash"))
    c = f.get("cron") or {}
    if c and not c.get("probe_err"):
        na, lra = c.get("next_age_min"), c.get("last_run_age_min")
        _lim = cron_stale_limit(c.get("period_min"))
        if c.get("enabled") is False:
            out.append(("P1", "cron_stale", "看护 job 被禁用（Hermes cron enabled=false）", "alert"))
        elif c.get("running"):
            pass                       # 正在执行 = 正常，不告警（见 CRON_RUNNING_MAX_MIN）
        elif c.get("status") == "error":
            out.append(("P1", "cron_stale", "看护 job last_status=error err=%s" % c.get("err"), "alert"))
        elif na is not None and na > CRON_LATE_MIN:
            out.append(("P1", "cron_stale", "看护 job 迟到 %.0f 分钟未触发" % na, "alert"))
        elif lra is not None and lra > _lim:
            out.append(("P1", "cron_stale",
                        "看护 job 已 %.0f 分钟没有执行（>%d = 周期 %s 分钟 + 120 缓冲）"
                        % (lra, _lim, c.get("period_min") or "未知"), "alert"))
    for bad in (f.get("cron_all") or [])[:3]:
        out.append(("P1", "cron_job_stale",
                    "调度任务「%s」异常：%s" % (bad.get("name") or bad.get("id"), bad.get("why")), "alert"))
    # E020：原为 `if scan_state == Running and pct < 5 and scan_state == Running: pass`（死代码），
    # 扫描被挂死的 ffprobe 堵住时全静默 → 索引冻结无人知。改成真判据。
    if st is not None:
        stall = scan_stalled(f, st)
        if stall:
            pct, stall_s = stall
            out.append(("P1", "scan_stuck",
                        "库扫描卡在 %.1f%% 已 %.0f 分钟（索引冻结，新片不入库）" % (pct, stall_s / 60),
                        "restart_mount"))
    return out


# ---------------- 决策/执行 ----------------
def act(findings, f, st, dry):
    taken = 0
    for sev, code, msg, action in findings:
        if taken >= MAX_ACTIONS_PER_CYCLE:
            audit("skip", code=code, why="每轮动作上限")
            break
        if action == "note_only":
            # 只留痕，不执行任何动作（E125：重复条目增长属于「值得知道，不必动手」）
            audit("note", code=code, sev=sev, msg=msg)
            continue
        key = action
        if not cooled(st, key):
            audit("skip", code=code, why="冷却中")
            continue
        if dry:
            audit("dry", code=code, sev=sev, msg=msg, action=action)
            print("  [dry] %-12s %s → %s" % (code, msg, action))
            continue

        if action == "purge_trash":
            n, freed = purge_trash()
            audit("action", code=code, action=action, removed=n, freed_gb=freed)
            print("  ✔ 清理回收目录 %d 个，释放 %.1f GB" % (n, freed))
        elif action == "refresh_series":
            fixed, skipped = refresh_mismatched(f, st)
            audit("action", code=code, action=action, fixed=fixed, skipped=skipped)
            print("  ✔ 定向 refresh %d 部%s" % (len(fixed), ("，放弃 %d 部（已刷满次数）" % len(skipped)) if skipped else ""))
            if skipped and not fixed:
                send_alert("P1", "refresh_ineffective",
                           "库索引差 refresh %d 次无效（索引/挂载层卡住）：%s"
                           % (REFRESH_MAX_TRIES, json.dumps(skipped, ensure_ascii=False)[:300]))
        elif action == "restart_mount" and (f.get("sessions") or 0) <= 0:
            rc, out = run("systemctl restart embytg-mount", timeout=180)
            audit("action", code=code, action=action, rc=rc, out=out[-300:])
            print("  ✔ 重启挂载 rc=%d" % rc)
        elif action == "restart_container":
            for name in f["unhealthy"]:
                rc, out = run("docker restart %s" % name, timeout=180)
                audit("action", code=code, action=action, container=name, rc=rc)
                print("  ✔ 重启容器 %s rc=%d" % (name, rc))
        elif action == "deep_clean":
            fr = deep_clean()
            audit("action", code=code, action=action, **fr)
            print("  ✔ 深度清理：%s" % fr)
        elif action == "reap_and_restart":
            # 2026-11-16（E153）：闸门暂停期间 restart 没有任何意义 —— 起来还是
            # 同一个 BUDGET_WAIT（实测 11:38、12:08、12:11 三次重启都在睡 1800s）。
            # findings 已经拦过一层，这里再确认一次，防「判据生成后这几十秒里
            # 预算刚好见底」的竞态。
            _bp = _budget_pause_state()
            if _bp:
                audit("skip", code=code, why="预算闸门暂停中(%s)，无需 reap+restart" % _bp[0])
                print("  ⤼ 预算闸门暂停中（%s），跳过 reap+restart" % _bp[0])
                continue
            # 2026-11-14（E087）：**先自愈，修不好才通知用户**。
            # 用户的诉求是「45 分钟没产出你要知道」，不是「你要发邮件告诉我」。
            # 停摆的实际原因是僵尸 claim 占满 session 槽（日志里表现为
            # post_ingest FAIL 后 claim 不释放），属于可自动处置的范围：
            #   ① 跑 claim_reap --apply 回收僵尸
            #   ② 若仍有 claim 但无对应进程 → 直接清掉
            #   ③ 重启 runner 让它重新领片
            #   ④ 处理完再确认一次，真的还不动才 escalation
            rc, out = run("%s %s/claim_reap.py --apply" % (sys.executable, HERE),
                          timeout=180)
            audit("action", code=code, action=action, step="reap", rc=rc,
                  out=(out or "")[-200:])
            print("  ✔ claim_reap rc=%d：%s" % (rc, (out or "").strip().splitlines()[-1][:80]
                                                 if out else ""))
            _killed = _kill_orphan_claims()
            if _killed:
                audit("action", code=code, action=action, step="kill_orphans",
                      killed=_killed)
                print("  ✔ 清理无进程 claim %d 个：%s" % (len(_killed), _killed))
            # 2026-11-15（E137）：**restart 前必须再确认一次真的没人在传。**
            # 判据来自快照，快照可能已过期；而 restart 会连带杀掉同一进程组的
            # gha_upload，正在传的几 GB 白费（实测同一集 22:52 与 23:08 各传一次）。
            _prog2 = _ingest_progress_age_min()
            if _prog2 is not None and _prog2 < 10:
                audit("action", code=code, action=action, step="restart_skipped",
                      progress_age_min=round(_prog2, 1))
                print("  ⤼ 有在飞任务仍在写进度（%.1f 分钟前），跳过 restart" % _prog2)
                continue
            rc2, _ = run("systemctl restart embytg-runner", timeout=120)
            audit("action", code=code, action=action, step="restart", rc=rc2)
            print("  ✔ 重启 runner rc=%d" % rc2)
            # 给新任务 3 分钟起步，再看是否恢复
            import time as _t
            _t.sleep(180)
            _age2 = _last_success_age_min()
            if _age2 is not None and _age2 > STALL_OUTPUT_MIN and not _budget_pause_state():
                send_alert("P1", "output_stalled_unfixed",
                           "自动处置后仍 %.0f 分钟无新片入库（已 reap+重启过），需人工看看"
                           % _age2)
                print("  ✘ 自愈失败，已升级告警")
            else:
                print("  ✔ 已恢复（最近成功 %.0f 分钟前）" % (_age2 or 0))
        elif action == "stop_scan":
            # 只停任务，绝不重启 Emby（重启会 dispose TaskManager、留下 D 状态 ffprobe）
            try:
                tasks = emby("/ScheduledTasks")
                t = [x for x in tasks if "Scan media" in (x.get("Name") or "")][0]
                urllib.request.urlopen(urllib.request.Request(
                    EMBY + "/ScheduledTasks/Running/%s" % t["Id"], method="DELETE",
                    headers={"X-Emby-Token": TOKEN}), timeout=60).read()
                audit("action", code=code, action="stop_scan", task=t["Id"])
                send_alert("P1", "scan_stopped", "全库扫描卡住 %.0f 分钟，已停止任务（未重启 Emby）"
                           % (f.get("scan_minutes") or 0))
                print("  ✔ 已停止卡住的扫描任务")
            except Exception as e:                                 # noqa: BLE001
                audit("action_error", code=code, err=str(e)[:100])
                send_alert("P0", "scan_stop_failed", "扫描卡住且停止失败：%s" % str(e)[:80])
        elif action == "teldrive_cache":
            if (f.get("sessions") or 0) > 0:
                audit("skip", code=code, why="有正在播放的会话，不动 teldrive 缓存")
                print("  ! 磁盘紧张但有播放，暂不动 teldrive 缓存")
            else:
                fr = clear_teldrive_cache()
                audit("action", code=code, action=action, **fr)
                print("  ✔ 清 teldrive 缓存：%s" % fr)
        elif action == "alert":
            send_alert(sev, code, msg)
            audit("alert", code=code, sev=sev, msg=msg)
        else:
            audit("noop", code=code, action=action)
        mark(st, key)
        taken += 1
    return taken


def purge_trash():
    """只清 _trash_*/_nfo_quarantine_* 里超过保留期的文件；不碰任何媒体目录"""
    removed = freed = 0
    now = time.time()
    for d in list(os.listdir(HERE)):
        if not d.startswith(("_trash_", "_nfo_quarantine_")):
            continue
        p = os.path.join(HERE, d)
        if (now - os.path.getmtime(p)) / 86400 < TRASH_KEEP_DAYS:
            continue
        for fn in os.listdir(p):
            fp = os.path.join(p, fn)
            try:
                sz = os.path.getsize(fp) if os.path.isfile(fp) else 0
                if os.path.isfile(fp):
                    os.remove(fp)
                else:
                    shutil.rmtree(fp, ignore_errors=True)
                removed += 1
                freed += sz
            except Exception:                                      # noqa: BLE001
                pass
        try:
            if not os.listdir(p):
                os.rmdir(p)
        except OSError:
            pass
    # /tmp 对账副本
    for fn in os.listdir("/tmp"):
        if fn.startswith("reconcile_"):
            try:
                os.remove(os.path.join("/tmp", fn))
            except Exception:                                      # noqa: BLE001
                pass
    return removed, round(freed / 1e9, 2)


def deep_clean():
    """二级清理：旧备份包只留 2 个、回收站清空、journal 压 200M、docker 无用镜像"""
    freed = {"backups": 0, "trash_x": 0, "ok": True}
    # 备份包
    import glob
    packs = sorted(glob.glob("/opt/embytg/backup/*.tar.gz"), key=os.path.getmtime, reverse=True)
    for p in packs[2:]:
        try:
            freed["backups"] += os.path.getsize(p)
            os.remove(p)
        except Exception:                                          # noqa: BLE001
            pass
    # 回收站
    n, gb = purge_trash()
    freed["trash_x"] = gb
    # journal / docker
    run("journalctl --vacuum-size=200M", timeout=120)
    run("docker image prune -f", timeout=180)
    return freed


def clear_teldrive_cache():
    """清 teldrive 的 varc 磁盘缓存（仅在无人播放时调用）"""
    d = "/opt/embytg/cache/varc"
    before = shutil.disk_usage("/").free
    run("cd /opt/embytg && docker compose --env-file .env stop teldrive", timeout=180)
    for sub in ("data", "meta"):
        p = os.path.join(d, sub)
        if os.path.isdir(p):
            for x in os.listdir(p):
                try:
                    shutil.rmtree(os.path.join(p, x), ignore_errors=True)
                except Exception:                                  # noqa: BLE001
                    pass
    run("cd /opt/embytg && docker compose --env-file .env start teldrive", timeout=180)
    time.sleep(15)
    after = shutil.disk_usage("/").free
    return {"freed_gb": round((after - before) / 1024 ** 3, 1), "free_gb": round(after / 1024 ** 3, 1)}


# Emby 容器内看到的挂载前缀 → 宿主实际路径前缀（stat 判据必须在宿主上做）
MNT_MAP = ("/mnt/embytg", MNT)


def _host_path(p):
    """Emby 里的 Path（容器视角）→ 宿主路径；不是该前缀就原样返回。"""
    if p.startswith(MNT_MAP[0] + "/"):
        return MNT_MAP[1] + p[len(MNT_MAP[0]):]
    return p


def _disk_lag_explained(emby_paths, disk_files, isfile=None):
    """Emby 比磁盘多出来的条目，是否只是 readdir 列集缓存滞后。

    2026-11-15（E133）：teldrive FUSE 的**目录列集有 4h 缓存**，而 TargetedScan
    建条是实时的 → 一部正在上片的剧，每半小时巡检都会看到 `emby = disk + 1`，
    其实文件早就在挂载上、索引完全正确。旧判据把它当「索引/挂载层卡住」：
    空刷两次 refresh（刷不动）→ 升级 P1。偶像选拔 17:26 那条就是这么误报的
    （disk=12/emby=13，13 分钟后磁盘自己变成 13，期间一直在正常上片）。

    判别方法：Emby 多出来的那些 Path 逐个 stat（stat 不走列集缓存）。
    全都能 stat 到 → 列集滞后，判「无需动手」；有 stat 不到的 → 才是真幽灵条目。
    """
    seen = set(disk_files or ())
    _isfile = isfile or os.path.isfile
    extra = [_host_path(p) for p in (emby_paths or []) if p and _host_path(p) not in seen]
    return bool(extra) and all(_isfile(p) for p in extra)


def _episode_paths_of(series_id):
    """该 Series 下全部 Episode 的 Path（失败返回 []）。"""
    try:
        q = urllib.parse.urlencode({"ParentId": series_id, "Recursive": "true",
                                    "IncludeItemTypes": "Episode",
                                    "Fields": "Path", "Limit": "2000"})
        return [(it.get("Path") or "") for it in emby("/Items?" + q).get("Items", [])]
    except Exception:                                          # noqa: BLE001
        return []


def _dup_episodes_of(series_id, total_hint=None, paths=None):
    """该 Series 下「同 Path 重复的 Episode 条数」（多出来的那几条）。

    2026-11-15（E125）：见 refresh_mismatched 里的说明。
    只对集数不符的剧调用（每轮最多 3 部），不增加热路径开销。
    """
    ps = paths if paths is not None else _episode_paths_of(series_id)
    seen, dup = set(), 0
    for pth in ps:
        if not pth:
            continue
        if pth in seen:
            dup += 1
        else:
            seen.add(pth)
    return dup


def _scan_counters(r):
    """只留非零计数，写进 audit/告警，省得下次只能看到「mode=scanpath」。"""
    return {k: r.get(k) for k in ("Created", "Refreshed", "PathNotFound",
                                  "ParentNotFound", "Failed", "skipped") if r.get(k)}


def _scanpath_verdict(r):
    """TargetedScan 返回值三分类（E211）。

    2026-11-26 19:21 多喜一家人 S01E18：post_ingest 19:20:01 送扫时文件还没落到
    挂载视图（PathNotFound），autoops 19:21:51 重试却被 emby_scan 的「同路径 150s
    去重窗」挡下（返回只有 skipped）—— 旧代码把这种情况也当「ScanPath 未建成」，
    第一次就升级 P1，文案还写「refresh 2 次无效」（tries=0）。

    判据分开：
      ok      —— 建了/刷了条目
      pending —— 只被去重窗挡下：另一条 lane 刚送扫同一路径，异步生效中，不是失败
      failed  —— 插件真回了 PathNotFound / ParentNotFound / Failed
    """
    if (r.get("Created") or 0) + (r.get("Refreshed") or 0) > 0:
        return "ok"
    if (r.get("PathNotFound") or 0) + (r.get("ParentNotFound") or 0) + (r.get("Failed") or 0) > 0:
        return "failed"
    if r.get("skipped"):
        return "pending"
    return "failed"


def refresh_mismatched(f, st=None):
    """对 lib_diff 里的库，找出具体哪几部剧集数不符 → 定向 refresh（最多 3 部）
    返回 (fixed, skipped)：同一部剧 REFRESH_TRIES_WINDOW 内已刷 REFRESH_MAX_TRIES 次仍不符，
    就不再刷（刷不动说明是索引/挂载层问题，见 E020），转成告警，避免每 30 分钟空转。"""
    try:
        import lib_reconcile as LR
    except Exception:                                              # noqa: BLE001
        return [], []
    # 2026-11-26（E211）：原来是 `(st or {})` —— 传进来空 state（{}）时 setdefault
    # 作用在临时 dict 上，refresh_tries 永远写不回，同一部剧会每个周期重刷永不计数。
    tries = (st if st is not None else {}).setdefault("refresh_tries", {})
    now = time.time()
    fixed = []
    skipped = []
    for lib, (disk, em) in (f.get("lib_diff") or {}).items():
        root = os.path.join(MNT, "TV", lib)
        if not os.path.isdir(root):
            continue
        q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                                    "Fields": "Path", "Limit": "5000"})
        series = [s for s in emby("/Items?" + q).get("Items", []) if "/TV/%s/" % lib in (s.get("Path") or "")]
        for s in series:
            nm = os.path.basename(s["Path"])
            p = os.path.join(root, nm)
            if not os.path.isdir(p):
                continue
            disk_files = {os.path.join(dp, fn) for dp, _, fs in os.walk(p)
                          for fn in fs if fn.lower().endswith((".mp4", ".mkv"))}
            n = len(disk_files)
            q2 = urllib.parse.urlencode({"ParentId": s["Id"], "Recursive": "true",
                                         "IncludeItemTypes": "Episode",
                                         "Fields": "Path", "Limit": "2000"})
            _eps = emby("/Items?" + q2)
            _paths = [(it.get("Path") or "") for it in (_eps.get("Items") or [])]
            _tot = _eps.get("TotalRecordCount")
            e = _tot if _tot is not None else len(_paths)
            if n != e:
                # 2026-11-15（E125）：**Emby 比磁盘多，可能只是同 Path 重复条目。**
                #
                # 实测：偶像选拔 S01E03 有两条同 Path 条目（167446/167447，同一秒创建，
                # TargetedScan 并发建条，E115 留证）→ disk 12 / emby 13 永远不等，
                # 每轮 refresh 无效，两次后升级成 P1 邮件。
                # 而这类重复**不能 DELETE 清**（E115：DELETE 会把真实视频连同 .nfo 一起删）。
                #
                # 判据：多出来的量能由重复条目完全解释 → 这是已知重复，不是索引卡住，
                # 跳过（refresh 治不了它）且不告警；解释不了才按原逻辑处理。
                if e > n:
                    dup = _dup_episodes_of(s["Id"], e, paths=_paths)
                    if dup and (e - n) <= dup:
                        audit("skip", code="index_diff",
                              why="已知重复条目 %d 条（同 Path，E115 成因）" % dup,
                              series=nm)
                        tries.pop(nm, None)
                        continue
                    # 2026-11-15（E133）：**列集缓存滞后 ≠ 索引卡住。**
                    # 上片中的剧 disk 会稳定比 emby 少 1（FUSE readdir 4h 缓存，
                    # TargetedScan 实时建条）。多出来的 Path 全部 stat 得到 → 不动手。
                    if _disk_lag_explained(_paths, disk_files):
                        audit("skip", code="index_diff",
                              why="readdir 列集滞后 %d 集（文件均已 stat 到，索引正确）" % (e - n),
                              series=nm)
                        tries.pop(nm, None)
                        continue
                hist = [t for t in tries.get(nm, []) if now - t < REFRESH_TRIES_WINDOW]
                if len(hist) >= REFRESH_MAX_TRIES:
                    tries[nm] = hist
                    skipped.append({"lib": lib, "series": nm, "disk": n, "emby": e, "tries": len(hist)})
                    continue
                # E115（2026-11-22 夜）：**插件可用时不许发 Recursive=true** —— 它会重新枚举
                # 季目录插一条，与并发的 TargetedScan 在同秒各建一条（同 Path 双条目）。
                # 正确做法：把「磁盘有、Emby 没有」的精确文件路径交给 TargetedScan（自带
                # 同路径去重窗），只有插件不可用时才退回 Recursive=true 当建条目退路。
                _miss = [_host_path(p) for p in sorted(disk_files)
                         if _host_path(p) not in {_host_path(x) for x in _paths}]
                _used_scanpath = False
                try:
                    import emby_scan as _ES
                    if _ES.have_scanpath() and _miss:
                        _c = [MNT_MAP[0] + p[len(MNT):] if p.startswith(MNT) else p
                              for p in _miss][:50]
                        _r = _ES.scan_paths(_c)
                        _used_scanpath = True
                        _v = _scanpath_verdict(_r)
                        if _v == "pending":
                            # E211：另一条 lane 刚送扫过同一路径（150s 去重窗），
                            # 异步建条目中 —— 不是失败，不计次、不告警。
                            audit("skip", code="index_diff", series=nm,
                                  why="ScanPath 去重窗内（另一 lane 刚送扫，异步生效中）")
                            continue
                        if _v == "failed":
                            # E178/E211：刚上传的文件在插件侧会短暂不可见（PathNotFound）；
                            # 插件对失败路径已不占用去重窗，这里清一次父目录缓存并重试一次。
                            # PathNotFound 表示没建成任何条目，重试不会造成重复建条。
                            try:
                                import reconcile as _RC
                                _vis, _healed = _RC.heal_mount_cache(_c)
                                if _healed:
                                    audit("heal", code="index_diff", series=nm, healed=_healed)
                                _retry = _vis or _c
                                if _retry:
                                    _r = _ES.scan_paths(_retry)
                                    _v = _scanpath_verdict(_r)
                            except Exception:              # noqa: BLE001
                                pass
                        if _v != "ok":
                            tries[nm] = hist + [now]
                            if len(tries[nm]) >= REFRESH_MAX_TRIES:
                                skipped.append({"lib": lib, "series": nm, "disk": n,
                                                "emby": e, "tries": len(tries[nm]),
                                                "why": "ScanPath 未建成 %s %s"
                                                       % (_r.get("mode"), _scan_counters(_r))})
                            continue
                except Exception:                          # noqa: BLE001
                    _used_scanpath = False
                if not _used_scanpath:
                    emby("/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default" % s["Id"], "POST")
                fixed.append({"lib": lib, "series": nm, "disk": n, "emby": e})
                tries[nm] = hist + [now]
                if len(fixed) >= 3:
                    return fixed, skipped
    return fixed, skipped


# ---------------- 每日自省 / 自动入库 ----------------
def gha_watch(st, f, dry):
    """GHA 数据面看护：.gha_paused（平台封号暂停）或集数连续 3h 不增长 → P1 告警。
    没有这条时，本机 autoingest 空跑 rc=0 会把「入库停摆」伪装成正常。"""
    if not cooled(st, "gha_watch"):
        return
    if dry:
        print("  [dry] GHA 看护：检查 .gha_paused / 集数增长")
        return
    mark(st, "gha_watch")
    ep = f.get("emby_ep")
    why = None
    pause = os.path.join(HERE, ".gha_paused")
    if os.path.exists(pause):
        try:
            note = open(pause, encoding="utf-8").read().strip().splitlines()[0][:100]
        except Exception:                                          # noqa: BLE001
            note = "（无说明）"
        why = "GHA 已暂停（.gha_paused）：%s" % note
    elif ep is not None:
        if st.get("gha_ep") == ep:
            hrs = (time.time() - st.get("gha_ep_ts", time.time())) / 3600.0
            if hrs >= 3:
                why = "集数 %.0f 小时无增长（仍 %s），GHA 疑似停摆" % (hrs, ep)
        else:
            st["gha_ep"], st["gha_ep_ts"] = ep, time.time()
    # 2026-11-12：GHA 看护整体退役。保留函数只为兼容旧状态/旧调用，
    # 但**永远不外发告警**（send_alert 会走 alert.py 的 SMTP 发到用户邮箱）。
    if why:
        audit("gha_watch_retired", code="suppressed", sev="info", msg=why,
              why="GHA 已退役+上片归韩国阿里云；该判据为过期噪音，按用户指令消除")
        print("  [retired] GHA 看护已停用，仅本地留痕：%s" % why)
    else:
        audit("gha_ok", ep=ep)


def daily_tasks(st, dry):
    """每天只做一次的活：自审（self_audit）+ 重排补片队列（autoingest --plan）"""
    today = time.strftime("%Y-%m-%d")
    if st.get("daily_date") == today:
        return
    if time.localtime().tm_hour < 6:
        return
    if dry:
        audit("daily_dry", day=today)
        print("  [dry] 今日自省：self_audit + autoingest --plan")
        return
    rc, out = run("%s %s/self_audit.py" % (sys.executable, HERE), timeout=600)
    audit("self_audit", rc=rc, tail=out.strip().splitlines()[-6:] if out.strip() else [])
    print("  ✔ 自审完成（rc=%d）" % rc)
    rc2, out2 = run("%s %s/autoingest.py --plan" % (sys.executable, HERE), timeout=1800)
    audit("autoingest_plan", rc=rc2, tail=out2.strip().splitlines()[-3:] if out2.strip() else [])
    print("  ✔ 补片队列重排（rc=%d）" % rc2)
    st["daily_date"] = today


def auto_ingest(st, f, dry):
    """无人值守入库：每小时最多一次，每轮 ≤30GB，且必须还在预算内"""
    h = time.localtime().tm_hour
    if h < 6 or h > 23:
        return
    # 2026-11-12 消除噪音（用户「现在根本不让gha上片，请消除噪音，以后不要被误会」）：
    # 本机入库被 .oracle_media_ingest.disabled 停用后，原路由转 gha_watch() 盯「集数3h不涨」
    # 并发 P1 邮件说「GHA 疑似停摆」。但 GHA 已于 11-11 18:15 永久退役、
    # 11-11 19:07 用户明确上片只交韩国阿里云 —— 该监控对象/文案/判据全部过时，
    # 「集数暂时不涨」是正常状态不是故障。这里整体退役该路由：只本地留痕，绝不发外发告警。
    # 韩国 runner 的健康由 runner_report.py(只报告不邮件) + 韩国侧 healthcheck.sh 负责，不归本脚本。
    if os.path.exists(os.path.join(HERE, ".oracle_media_ingest.disabled")):
        audit("ingest_paused", code="no_op", why="本机入库已停用；上片归韩国阿里云 runner；GHA 已退休，不再做 GHA 看护/停摆告警")
        return
    if not cooled(st, "autoingest_run"):
        return
    if (f.get("quota", {}).get("left") or 0) < 2:
        audit("skip", code="ingest", why="预算不足")
        return
    if dry:
        audit("dry", code="ingest", action="autoingest --run")
        print("  [dry] 自动入库：autoingest --run --max-gb 30 --max-files 3")
        return
    rc, out = run("%s %s/autoingest.py --run --max-gb 30 --max-files 3" % (sys.executable, HERE), timeout=3600)
    audit("autoingest_run", rc=rc, tail=out.strip().splitlines()[-4:] if out.strip() else [])
    print("  %s 自动入库 rc=%d：%s" % ("✔" if rc == 0 else "✘", rc,
                                        out.strip().splitlines()[-1][:80] if out.strip() else ""))
    mark(st, "autoingest_run")


# ---------------- 主循环 ----------------
def cycle(dry=False):
    os.makedirs(os.path.dirname(AUDIT), exist_ok=True)
    lf = open(LOCK, "w")
    try:
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("另一轮 autoops 正在跑，跳过")
        return 0
    st = load_state()
    st["cycle"] = st.get("cycle", 0) + 1
    t0 = time.time()
    print("== autoops cycle #%d %s ==" % (st["cycle"], "(dry)" if dry else ""))
    f = sense()
    print("  感知: 磁盘 %.1fGB(%.0f%%) 挂载=%s 容器=%s unhealthy=%s 扫描=%s %.0f%% 集=%s 缺口库=%s 预算余=%.1fGB 回收=%.1fGB"
          % (f["disk_free_gb"], f["disk_used_pct"], f["mount_ok"], f["containers"], f["unhealthy"] or "-",
             f.get("scan_state"), f.get("scan_pct") or 0, f.get("emby_ep"), f.get("lib_diff"), f["quota"].get("left", 0),
             f.get("trash_gb", 0)))
    findings = analyze(f, st)
    print("  分析: %d 条" % len(findings))
    n = act(findings, f, st, dry)
    try:
        auto_ingest(st, f, dry)
    except Exception as e:                                          # noqa: BLE001
        audit("ingest_error", err=str(e)[:160])
    try:
        daily_tasks(st, dry)
    except Exception as e:                                          # noqa: BLE001
        audit("daily_error", err=str(e)[:160])
    # E017（2026-11-10 13:5x）：这里原为 st["last"] = {...}，把 cooled()/mark() 用的
    # 动作冷却时间戳整段覆盖 → 冷却全程失效，purge_trash/refresh_series 每轮空跑。
    # 摘要改存 st["summary"]，"last" 只留给动作时间戳。
    st["summary"] = {"ts": time.strftime("%F %T"), "findings": [c for _, c, _, _ in findings],
                     "actions": n, "disk": f["disk_free_gb"]}
    save_state(st)
    audit("cycle", n=st["cycle"], dry=dry, findings=len(findings), actions=n,
          dur=round(time.time() - t0, 1), snapshot={k: f[k] for k in ("disk_free_gb", "mount_ok", "scan_state",
                                                                      "emby_ep", "trash_gb")})
    print("  完成 %.0fs（动作 %d）" % (time.time() - t0, n))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycle", action="store_true")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--loop", type=int, help="常驻，每 N 秒一轮")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--audit", type=int, default=0)
    a = ap.parse_args()

    if a.status:
        st = load_state()
        print("cycle=%s last=%s" % (st.get("cycle"), json.dumps(st.get("last"), ensure_ascii=False)))
        return
    if a.audit:
        lines = open(AUDIT, encoding="utf-8").read().strip().splitlines()[-a.audit:]
        for l in lines:
            r = json.loads(l)
            print("%s %-8s %s" % (r["ts"], r["kind"], {k: v for k, v in r.items() if k not in ("ts", "kind")}))
        return
    if a.loop:
        # 2026-11-12 12:10：autoops.py 被改过但常驻进程仍在跑旧代码 → cycle #97-101 继续外发
        # 已退役的 runner_stall P1（重启后 cycle #102 立即正常）。这里做「源码更新自动 reload」：
        # 每轮结束比对 mtime，py_compile 通过就 execv 自重启加载新代码，失败只留痕不挣扎。
        src = os.path.abspath(__file__)
        src_mtime = os.path.getmtime(src)
        while True:
            try:
                cycle(dry=a.dry)
            except Exception as e:                                 # noqa: BLE001
                audit("loop_error", err=str(e)[:200])
                print("cycle 异常：%s" % e)
            try:
                mt = os.path.getmtime(src)
                if mt > src_mtime + 1:
                    rc = subprocess.call([sys.executable, "-m", "py_compile", src],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if rc == 0:
                        audit("self_reload", old=src_mtime, new=mt,
                              why="autoops.py 磁盘已更新，execv 自重启加载新代码")
                        print("  ↻ autoops.py 已更新（%.0f→%.0f），重启加载新代码" % (src_mtime, mt), flush=True)
                        argv = [sys.executable, "-u", src, "--loop", str(a.loop)] + (["--dry"] if a.dry else [])
                        os.execv(sys.executable, argv)
                    else:
                        audit("self_reload_blocked", code="py_compile", rc=rc)
                        src_mtime = mt                             # 避免每轮重复编译
            except Exception as e:                                 # noqa: BLE001
                audit("self_reload_error", err=str(e)[:120])
            time.sleep(a.loop)
    if a.cycle or a.dry:
        return cycle(dry=a.dry or not a.cycle)
    ap.print_help()


if __name__ == "__main__":
    sys.exit(main() or 0)
