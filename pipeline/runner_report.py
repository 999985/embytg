#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""runner_report.py —— 远程上片日报 / 健康检查（五路 runner 数据面）

背景：GitHub Actions 与韩国阿里云已退役；当前数据面固定为 PC `new`、Oracle
`new2`、MCP `main` `new3`、`box2` `new4`、`box3` `new5`。
甲骨文控制面负责 claim / 登记 / NFO / Refresh / 台账。文件名里的 `gha_` 是历史命名。
原来的 `ingest_review.py` 读 `logs/autoingest.log`（本机拉片日志），现在会是空的，
所以**日报要读五路共享状态文件**：

  .gha_claims/            领片记录（每次 gha_next 一个）
  .gha_new_done.json      new lane 完成登记
  .gha_queue_{new,fill,upgrade}.json   三条队列
  .gha_src_budget.json    每源日配额（daily/burst/冷却）
  .gha_cursor_*           各 lane 游标

输出：健康判定（PASS/WARN/FAIL）+ 今日/近 3h 完成数 + 队列余量 + 卡单检测 + 源配额。

用法：
    python3 runner_report.py             # 文本
    python3 runner_report.py --json
"""
import argparse
import collections
import datetime
import glob
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import gate_history

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
EMBY = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
TOKEN = CFG["emby_token"]
HIST = os.path.join(HERE, ".gha_report_history.json")
STALE_CLAIM_MIN = 720         # 可被 .thresholds.json 覆盖（adapt_thresholds.py）
try:
    import adapt_thresholds as _AT2
    STALE_CLAIM_MIN = int(_AT2.load()["claim_stale_min"])
except Exception:                                                  # noqa: BLE001
    pass
# 五路 runner 共享 Oracle 控制面；同一个物理 slot 号可在不同云电脑复用。
# 慢源单集可跑数小时（实测 4h+），系统层超时 13–27h；>12h 才算卡单。
SLOW_PROGRESS_MIN = 15        # 远端 download.log 15min 内有写入 = 慢但在跑，不判卡单
NO_FINISH_HOURS = 3           # 3 小时没有任何完成 → 告警
THROUGHPUT_RATIO = 0.30       # 近 3h 产能低于「过去 24h 均值」的 30% → 产能塌陷（2026-11-13）
ORPHAN_CLAIM_MIN = 90         # 同 lane 有更新 claim 且旧记录超过 90min → 疑似孤儿候选


def emby_episodes():
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                "Limit": "1", "EnableTotalRecordCount": "true"})
    req = urllib.request.Request(EMBY + "/Items?" + q, headers={"X-Emby-Token": TOKEN})
    return json.loads(urllib.request.urlopen(req, timeout=60).read().decode()).get("TotalRecordCount")


def load(p, default):
    try:
        return json.load(open(os.path.join(HERE, p), encoding="utf-8"))
    except Exception:                                              # noqa: BLE001
        return default


def claim_live(run_id):
    """这个 claim 在本机还有活的下载/上传进程吗？

    2026-11-15：**年龄不是卡单证据，活进程才是「没卡」的证据。**
    实测 辐射 E2 在 TG 限速 0.3 MB/s 下单集要传 1 小时+，claim 年龄 160 分钟，
    报表把它报成「卡单 1」，而 gha_upload 正在传第 190/214 段 —— 照着这条去
    gha_cancel 会把两个小时的上传直接砍掉。同一天 claim_reap 也因为
    pgrep 漏了 gha_upload 差点砍掉它，两处判据必须同口径。
    """
    rid = str(run_id or "")
    if not rid:
        return False
    try:
        out = subprocess.run(
            ["pgrep", "-af", "run_one.sh|gha_download|gha_upload|pc_upload"],
            capture_output=True, text=True, timeout=15).stdout
    except Exception:                                      # noqa: BLE001
        return False
    return rid in out


def claim_meta(path):
    """读一条 claim 的 run_id / ts / series；坏了就当空 dict，不阻断日报。"""
    try:
        j = json.load(open(path, encoding="utf-8"))
    except Exception:                                              # noqa: BLE001
        return {}
    return j if isinstance(j, dict) else {}


def lane_of(run_id):
    """run_id 形如 20261112_213559-slot3-new5-da8003 → runner lane。"""
    parts = str(run_id or "").split("-")
    for part in parts:
        if part in {"new", "new2", "new3", "new4", "new5", "fill", "upgrade", "upgrade2"}:
            return part
    return ""


def duplicate_today():
    """今日 autoingest.log 的 duplicate 事件数 + 重复最多的剧。

    2026-11-13：队列里已入库的条目被反复扫（装甲骑兵 VOTOMS E1 一天扫 14 次），
    5 分钟一轮的 runner 把槽位耗在重复确认上，完成量从 20/h 掉到 1/h，
    而旧判定只看「近 3h 有完成」→ 报 PASS。空转必须单独暴露。

    同日改进：只统计**最近 3 小时**的 duplicate，并给出「完成/重复」比值。
    原因：全天累计数字会把修复前的历史（今日一度 1107 条）一直带进日报，
    让「已经修好了」看起来还像没修。判据应该是当下是否还在空转。
    """
    n = 0
    n_3h = 0
    _batches3h = set()
    cnt = collections.Counter()
    p = os.path.join(HERE, "logs", "autoingest.log")
    day = time.strftime("%Y-%m-%d")
    cutoff = time.time() - 3 * 3600
    # 2026-11-13：修复分界。判重逻辑改过几轮（66a1011 / a081137 等），
    # 每改一次，旧窗口里的历史 duplicate 都会继续把日报压成「还在空转」——
    # 观测到修复生效后 19 点只剩 2 条，日报仍报 432 条（17 点那批 228 条还在窗口内）。
    # 这里读一个可选分界文件：存在时，统计起点取「3h 前」与「分界时刻」中较晚者，
    # 让日报反映「当前代码是否还在空转」，而不是被修好的历史数据拖住。
    # 文件内容为一行 epoch 秒；写它的时机 = 确认修复已全链路生效那一刻。
    _fix_epoch = 0.0
    _fp = os.path.join(HERE, "docs", "DUP_FIX_EPOCH")
    try:
        if os.path.exists(_fp):
            _fix_epoch = float((open(_fp, encoding="utf-8").read() or "0").strip() or 0)
    except Exception:                                          # noqa: BLE001
        _fix_epoch = 0.0
    if _fix_epoch > cutoff:
        cutoff = _fix_epoch
    # 2026-11-13：**在途剧的重复扫描不算空转**。一部剧有 claim 在跑时，源站每轮都会把
    # 整部剧重新列一遍（连载剧尤其明显），这些 duplicate 是预期行为，把它算进"空转"
    # 会让判据长期假 FAIL（实测 3h 76 条里有 70 条来自正在下载的两部剧）。
    # 判据应该是：**没有 claim 的剧还在被反复扫** = 真的在烧配额。
    _busy_series = set()
    try:
        for _f in glob.glob(os.path.join(HERE, ".gha_claims", "*.json")):
            _d = json.load(open(_f, encoding="utf-8"))
            _s = (_d.get("series") or "").strip()
            if _s and (_d.get("status") or "") == "running":
                _busy_series.add(_s)
    except Exception:                                          # noqa: BLE001
        pass
    # 2026-11-13 再收紧一层：**在冷却期的剧也不算空转**。
    # 实测娘道 3h 内 47 条 duplicate，全在 21:49 写入 series_complete 冷却（7 天）之前——
    # 也就是"已经修好但历史还在窗口里"。判据要问的是：既没在跑、也没在冷却的剧，
    # 还在被反复扫 = 真的在烧配额。冷却映射走 .gha_new_done.json(video_id) + 队列(video_id→series)。
    _cool_series = set()
    try:
        _done = json.load(open(os.path.join(HERE, ".gha_new_done.json"), encoding="utf-8"))
        _now = time.time()
        _cool_ids = {str(k) for k, v in _done.items()
                     if float((v or {}).get("next_check") or 0) > _now}
        if _cool_ids:
            _q = json.load(open(os.path.join(HERE, ".gha_queue_new.json"), encoding="utf-8"))
            _items = _q.get("items") if isinstance(_q, dict) else _q
            for _i in (_items or []):
                if str(_i.get("video_id")) in _cool_ids and _i.get("series"):
                    _cool_series.add(_i["series"].strip())
    except Exception:                                          # noqa: BLE001
        pass
    if os.path.exists(p):
        for line in open(p, encoding="utf-8", errors="ignore"):
            if day not in line[:24]:
                continue
            try:
                r = json.loads(line)
            except Exception:                                      # noqa: BLE001
                continue
            if r.get("event") == "duplicate":
                # 2026-11-13 修：一条 duplicate 日志是**一集**，不是"一次扫描"。
                # 一部 10 集的剧重扫一遍会产生 9 行 duplicate，按行数统计会把
                # "扫了 3 次"读成"浪费了 30 次"，判据必然假 FAIL。
                # 正确的量纲是**扫描批次**：同一秒 + 同一剧 = 一次扫描。
                n += 1
                cnt[r.get("series") or "?"] += 1
                _batch = ((r.get("ts") or "")[:19], r.get("series") or "?")
                try:
                    ts = time.mktime(time.strptime(r.get("ts", "")[:19], "%Y-%m-%d %H:%M:%S"))
                except Exception:                                  # noqa: BLE001
                    ts = 0
                if (ts >= cutoff and (r.get("series") or "") not in _busy_series
                        and (r.get("series") or "") not in _cool_series):
                    _batches3h.add(_batch)
    # n_3h 用批次口径（"重扫了几次"），不再是"重复了几集"
    return n, cnt.most_common(3), len(_batches3h)


def new_done_cold_count():
    """待扫 new 队列里有多少条已被 new_done 冷却挡住（7 天内不会再扫）。

    冷却项不是待办：报「待办 1926」会把已收完/无源的剧算成积压，
    导致产能评估虚高、重复扫描的锅甩给 runner。
    """
    try:
        q = json.load(open(os.path.join(HERE, ".gha_queue_new.json"), encoding="utf-8"))
        items = q.get("items") if isinstance(q, dict) else q
    except Exception:                                          # noqa: BLE001
        return 0, 0
    try:
        done = json.load(open(os.path.join(HERE, ".gha_new_done.json"), encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        done = {}
    now = time.time()
    cold = 0
    for it in items or []:
        rec = done.get(str(it.get("video_id"))) or {}
        try:
            if now < float(rec.get("next_check") or 0):
                cold += 1
        except Exception:                                      # noqa: BLE001
            continue
    return cold, len(items or [])


def gate_blocked_window(window_sec=None):
    """近 N 小时预算闸门「按设计停」占了多少秒（E160）。

    为什么要这个数：`max_3h_eps` 是**预算口径**的允许量（allow_gb_h×3÷单集），
    预算一充裕它就涨到物理不可能（2026-11-17 00:46 实测 429.6 集/3h，
    而历史最好一天 243 集）。用它当产能下限 = 每天凌晨必假红。
    同理，窗口里若跨了前晚的硬停（22:36 daily_exhausted → 00:00 重置），
    这 3 小时里有一半时间根本不许领片，拿它跟 24h 均值比也是错判。

    解析器只有一个：`gate_history.blocked_window()`（stall_watch 也用它，E161）。
    读不到日志 → 返回 None（判据回落到原有逻辑，绝不假装 0）。
    """
    window_sec = float(window_sec if window_sec is not None else NO_FINISH_HOURS * 3600)
    return gate_history.blocked_window(window_sec)


def throughput_verdict(done_3h, rate_24h_per_h, bb, blocked=None):
    """产能判据：预算视角优先（见 byte_budget 的说明）。

    没有预算视图时才回落到「近 3h vs 过去 24h 均值」的老判据。

    闸门正在拒（rc=75/3）时直接 PASS —— 理由不是「宽容」，是**物理不可能**：
    这四个 lane 连 claim 都不许领，产出必然是 0；而 `max_3h_eps` 在「不节流」
    口径下等于「剩余总额 ÷ 单集」，闸门拒的恰恰是「这一小时再放就超」，
    两者互相矛盾。2026-11-16 22:30 实测：剩 5.52GB → max_3h_eps 9.2 →
    门槛 5.5 集，而闸门一集都不放 → 每晚预算用完后 daily_ops 必红。

    两个下限取**较小**的那个（E160）：预算允许 429 集/3h 不代表 lane 能跑 429，
    历史 24h 均值代表可持续产能。要求「同时跑赢预算和历史」会把充裕预算
    变成不可能任务；正确的门是「不要低于两者中更宽松的那条」。
    """
    if isinstance(bb, dict) and (bb.get("gate_holding") or bb.get("exhausted")):
        return True
    # 窗口里跨了硬停、且刚放开不到 90 分钟：恢复期不给产能定罪（E160）。
    # 下一轮窗口不再含硬停时判据自行恢复，最多跳过一次。
    if isinstance(blocked, dict) and blocked.get("secs") \
            and blocked["secs"] >= 0.2 * NO_FINISH_HOURS * 3600 \
            and blocked.get("last_end") is not None and blocked["last_end"] < 5400:
        return True
    needs = []
    if rate_24h_per_h * NO_FINISH_HOURS >= 6:
        needs.append(THROUGHPUT_RATIO * rate_24h_per_h * NO_FINISH_HOURS)
    if isinstance(bb, dict) and bb.get("max_3h_eps") is not None \
            and rate_24h_per_h * NO_FINISH_HOURS >= 6:
        needs.append(0.6 * float(bb["max_3h_eps"]))
    if needs and done_3h < min(needs):
        return False
    return True


def _gate_eval(lane="new"):
    """直接问 runner 侧闸门「此刻放不放行」（E156）。

    为什么必须问它而不是自己算：2026-11-16 起 `budget_gate.py` 的日额度是
    **自动跟随月度余额**的（`month_left / 剩余天数`，实测当日 456GB），
    而本报告沿用的是 `ingest_quota.cfg().daily_gb` 写死的 300GB。
    于是同一时刻闸门还剩 28GB（照常放行、四个 lane 都在上传），
    报告却算出 -128GB 并写「预算已用完、runner 按闸门暂停领片」——
    假“按设计停”，而且它会把 `done_3h == 0` 的真停摆吞掉（那个分支在 elif 后面）。
    判据只能有一个来源：闸门本身。
    """
    try:
        import importlib.util
        p = os.environ.get("TG_BUDGET_GATE_PY") or "/opt/embytg-runner/budget_gate.py"
        spec = importlib.util.spec_from_file_location("runner_budget_gate", p)
        if spec is None or spec.loader is None:
            return None
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)                            # 只读库，不写 state
        d = m.evaluate(lane)
        return d if isinstance(d, dict) and d.get("daily_gb") else None
    except Exception:                                          # noqa: BLE001
        return None


def byte_budget():
    """当日字节预算视图 —— 唯一口径是 runner 侧 budget_gate.evaluate()（E148/E156）。

    为什么判定必须读它：装上预算闸门以后，产能不再由带宽/队列决定，而是由
    「剩余预算 ÷ 剩余小时」决定。此时拿过去 24h 均值判「产能塌陷」就是判据错 ——
    实测 2026-11-16 09:10 已用 255.9/300GB，剩 44GB/14.8h = 3.0GB/h，
    物理上就只能出 1~2 集/h；真正的故障是「前 5h 41GB/h、之后 19h 空转」，
    而那正是闸门要消灭的形状。

    口径漂移史（别再各算一套）：固定 300 → 自动月均（E156）。
    """
    gate = _gate_eval()
    if gate:
        daily = float(gate["daily_gb"])
        # 用闸门自己的计量：它算的是 resource_state 的 kind/size 口径，
        # 和它据以放行的数字必须是同一个，否则「报告说超了、闸门说没超」。
        d_gb = float(gate.get("used_today_gb") or 0.0)
        remaining = float(gate.get("remaining_gb") or 0.0)
        hours_left = float(gate.get("hours_left") or 0.5)
        pace = float(gate.get("pace_gb_h") or 0.0)
        allow = float(gate.get("allow_gb_h") or 0.0)
        reason = str(gate.get("reason") or "")
        # 判定只认闸门的拒绝理由，不认我们自己减出来的负数
        exhausted = reason in ("daily_exhausted", "month_exhausted")
        throttled = reason in ("pacing", "reserve_new") or (0.0 < remaining < daily * 0.10)
        avg_gb = 1.8
        try:
            _c = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                                 uri=True, timeout=20)
            row = _c.execute(
                "select coalesce(sum(size),0), count(*) from resource_state "
                "where kind in ('episode','movie') and "
                "date(landed_at,'unixepoch','localtime')=?", (today_iso(),)).fetchone()
            _c.close()
            if row and row[1] and row[0]:
                avg_gb = max(0.2, float(row[0]) / row[1] / 1e9)
        except Exception:                                      # noqa: BLE001
            pass
        return {
            "source": "budget_gate",
            "gate_reason": reason,
            "gate_holding": bool(int(gate.get("rc") or 0) != 0),
            "used_today_gb": round(d_gb, 1),
            "daily_gb": round(daily, 1),
            "remaining_gb": round(remaining, 1),
            "month_left_gb": round(float(gate.get("month_left_gb") or 0.0), 1),
            "hours_left": round(hours_left, 1),
            "pace_gb_h": round(pace, 2),
            "allow_gb_h": round(allow, 2),
            "avg_ep_gb": round(avg_gb, 2),
            "max_3h_eps": round(allow * NO_FINISH_HOURS / avg_gb, 1),
            "exhausted": exhausted,
            "throttled": throttled,
        }
    try:
        import ingest_quota as Q
        c = Q.cfg()
        daily = float(c.get("daily_gb") or 300.0)
        d_gb, _d_n, m_gb, _m_n = Q.usage()
    except Exception:                                          # noqa: BLE001
        return None
    now = time.time()
    mid = datetime.datetime.combine(datetime.date.today() + datetime.timedelta(days=1),
                                    datetime.time.min)
    hours_left = max(0.5, (time.mktime(mid.timetuple()) - now) / 3600.0)
    remaining = daily - d_gb
    pace = max(0.0, remaining) / hours_left
    avg_gb = 1.8
    try:
        _c = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                             uri=True, timeout=20)
        row = _c.execute(
            "select coalesce(sum(size),0), count(*) from resource_state "
            "where kind in ('episode','movie') and "
            "date(landed_at,'unixepoch','localtime')=?", (today_iso(),)).fetchone()
        _c.close()
        if row and row[1] and row[0]:
            avg_gb = max(0.2, float(row[0]) / row[1] / 1e9)
    except Exception:                                          # noqa: BLE001
        pass
    allow = max(pace, float(os.environ.get("TG_BUDGET_MIN_RATE_GB_H", "2.0")))
    even = daily / 24.0
    return {
        "source": "ingest_quota(fallback)",
        "used_today_gb": round(d_gb, 1),
        "daily_gb": round(daily, 1),
        "remaining_gb": round(remaining, 1),
        "hours_left": round(hours_left, 1),
        "pace_gb_h": round(pace, 2),
        "allow_gb_h": round(allow, 2),
        "avg_ep_gb": round(avg_gb, 2),
        "max_3h_eps": round(allow * NO_FINISH_HOURS / avg_gb, 1),
        "exhausted": remaining <= 1.0,
        "throttled": pace < even * 0.8,
    }


def today_iso():
    return datetime.date.today().isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    rep = {"ts": datetime.datetime.now().strftime("%F %T")}  # type: dict[str, object]

    # 1) 完成量
    # 2026-11-14 修：`done_today` 原来数的是 `.gha_new_done.json` 里**剧级完成/冷却**的记录，
    # 不是集数。实测 11-13 真实落地 287 集，而它报 249 —— 两个不同量纲混在一个名字下，
    # 任何"产能是涨是跌"的判断都会错。现在统一到 tg.db.resource_state.landed_at
    # （对账器 reconcile 用的同一本账），剧级完成另存 series_done_today。
    done = load(".gha_new_done.json", {})
    ts_list = sorted(v.get("ts", 0) for v in done.values() if isinstance(v, dict) and v.get("ts"))
    today = datetime.date.today()
    rep["series_done_today"] = sum(
        1 for t in ts_list if datetime.datetime.fromtimestamp(t).date() == today)
    rep["series_done_total"] = len(ts_list)
    _ep_ts = []
    ep_list = []
    try:
        _c = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                             uri=True, timeout=30)
        _ep_ts = [r[0] for r in _c.execute(
            # 不要按扩展名过滤：resource_state 本来就只装视频，而库里 .mkv 占多数，
            # 写死 '%.mp4' 会把 mkv 全漏掉（实测王冠 S01E01 就是 mkv，导致今日计 0）。
            "SELECT landed_at FROM resource_state WHERE landed_at > 0")]
        _c.close()
    except Exception:                                       # noqa: BLE001
        pass
    if _ep_ts:
        ep_list = sorted(_ep_ts)
        rep["done_total"] = len(ep_list)
        rep["done_today"] = sum(
            1 for t in ep_list
            if datetime.datetime.fromtimestamp(t).date() == today)
    else:
        rep["done_total"] = len(ts_list)
        rep["done_today"] = rep["series_done_today"]
    # 2026-11-15 修（量纲）：done_3h / rate_* 原来数的是 `.gha_new_done.json` 的**剧级**
    # 记录（每部剧一轮一条），而 done_today/done_total 已是 **集级** landed_at。
    # 两个量纲混在一起 → 产能「塌陷」误报（实测近 3h 落盘 25 集，它报 1 条）。
    # 产能判据只认集级台账：ep_list（= resource_state.landed_at），剧级量只作参考存 series_done_*。
    _rate_src = ep_list if _ep_ts else ts_list
    rep["done_3h"] = sum(1 for t in _rate_src if time.time() - t < NO_FINISH_HOURS * 3600)
    rep["last_finish"] = (datetime.datetime.fromtimestamp(_rate_src[-1]).strftime("%F %T") if _rate_src else None)

    # 1.1) 产能趋势：近 3h 完成数 vs 过去 24h（剔除近 3h）每小时均值
    now = time.time()
    base = [t for t in _rate_src if 3 * 3600 <= now - t < 24 * 3600]
    rep["rate_24h_per_h"] = round(len(base) / 21.0, 2)
    rep["rate_3h_per_h"] = round(rep["done_3h"] / float(NO_FINISH_HOURS), 2)
    _bb = byte_budget()
    rep["byte_budget"] = _bb
    rep["gate_blocked_3h"] = gate_blocked_window()
    rep["throughput_ok"] = throughput_verdict(rep["done_3h"], rep["rate_24h_per_h"],
                                              _bb, rep["gate_blocked_3h"])

    # 2) 队列
    q = {}
    for lane in ("new", "fill", "upgrade"):
        d = load(".gha_queue_%s.json" % lane, {})
        items = d.get("items") if isinstance(d, dict) else d
        q[lane] = len(items or [])
    rep["queues"] = q

    # 3) 卡单：有 claim 但超过 40 分钟没完成
    claims = []
    cd = os.path.join(HERE, ".gha_claims")
    if os.path.isdir(cd):
        for f in sorted(glob.glob(os.path.join(cd, "*.json")), key=os.path.getmtime)[-20:]:
            j = claim_meta(f)
            claims.append({"file": os.path.basename(f),
                           "age_min": round((time.time() - os.path.getmtime(f)) / 60, 1),
                           "run_id": j.get("run_id") or "",
                           "lane": j.get("lane") or lane_of(j.get("run_id")),
                           "series": j.get("series") or "",
                           "status": j.get("status") or "",
                           "error": str(j.get("error") or "")[:60],
                           "ts": float(j.get("ts") or 0)})
    # 3.0) 孤儿 claim：同一 runner lane 上已有更新 claim，说明旧任务早该释放。
    # 不能按物理 slot 分组：不同云电脑会重复使用 slot3。
    newest = {}
    for c in claims:
        if c["lane"] and c["ts"] > newest.get(c["lane"], 0):
            newest[c["lane"]] = c["ts"]
    rep["orphan_claims"] = [c for c in claims
                            if c["lane"] and c["age_min"] > ORPHAN_CLAIM_MIN
                            and c["ts"] < newest.get(c["lane"], 0)]
    # Oracle cannot directly inspect every runner's work directory. Age alone is
    # not proof of a stale job, so report candidates for remote job/manifest review.
    # 2026-11-15：有活进程的 claim 不算卡单（慢上传 ≠ 卡死）。
    for c in claims:
        c["live"] = claim_live(c.get("run_id"))
    old = [c for c in claims if c["age_min"] > STALE_CLAIM_MIN and not c["live"]]
    rep["live_claims"] = [c for c in claims if c["live"]]
    rep["stale_claims"] = old
    # 3.1) 失败 claim：status=failed 的文件没人释放就一直占着 lane。
    # 2026-11-13：gha_cancel 用的是 find_claim() 默认参（只认 running），
    # 导致 new4 的加油吧威基基 S1E6 以 [Errno 32] Broken pipe 失败后卡 8.5 小时，
    # 那个 lane 一直显示有 claim 却不出片。失败态必须单独暴露出来。
    # 2026-11-14：failed 要持续一段时间才算真失败。
    # run_one.sh 的 finish 是"失败重试"模型：attempt 1 失败会把 claim 标成 failed，
    # 几秒后 attempt 2 往往成功并自己清掉。这个窗口里 daily_ops 撞上就会报
    # "失败 claim 未释放"，实测 new5/噬血狂襲 就是这么误报的（查到时报 1 条，
    # 两分钟后再看文件已经消失）。真失败的特征是**没人管、一直在**，
    # 所以加 10 分钟宽限期：retry 在秒级完成，10 分钟还挂着才是真卡住。
    FAILED_GRACE_MIN = 10
    rep["failed_claims"] = [c for c in claims
                            if c["status"] == "failed"
                            and c.get("age_min", 0) > FAILED_GRACE_MIN]
    rep["slow_claims"] = []

    # 4) 源配额
    b = load(".gha_src_budget.json", {})
    lim = (b.get("limits") or {}).get("daily", 0)
    used = {k: (v.get("day") or 0) for k, v in (b.get("sources") or {}).items()}
    rep["budget"] = {"daily_limit": lim, "used": used,
                     "saturated": [k for k, v in used.items() if lim and v >= lim]}

    # 5) Emby 增长（与上次快照比）
    ep = emby_episodes()
    hist = load(".gha_report_history.json", {})
    prev = None
    for d in sorted(hist):
        if d < today.isoformat():
            prev = (d, hist[d].get("episodes"))
    rep["emby_episodes"] = ep
    rep["emby_delta"] = (ep - prev[1]) if prev and prev[1] else None
    hist[today.isoformat()] = {"episodes": ep, "ts": rep["ts"]}
    json.dump(hist, open(HIST, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    # 6) 判定
    dup_n, dup_top, dup_3h = duplicate_today()
    rep["dup_today"] = dup_n
    rep["dup_3h"] = dup_3h
    rep["dup_top"] = [{"series": s, "n": n} for s, n in dup_top]
    problems = []
    notes = []
    _bb0 = rep.get("byte_budget")
    _bb = _bb0 if isinstance(_bb0, dict) else {}
    _exhausted = bool(_bb.get("exhausted"))
    if _exhausted:
        # 预算见底的 0 产出是闸门在正常工作，不是故障：明日 00:00 自动重置。
        notes.append("今日字节预算已用完（%.0f/%.0fGB），runner 按闸门暂停领片，"
                     "明日 00:00 自动恢复" % (_bb.get("used_today_gb") or 0,
                                             _bb.get("daily_gb") or 0))
    elif rep["done_3h"] == 0 and not _bb.get("gate_holding"):
        problems.append("近 %d 小时没有新集登记（检查五路 runner 队列和源站预算）" % NO_FINISH_HOURS)
    if _bb.get("throttled"):
        notes.append("配额节流中（预算闸门）：剩 %.0fGB / %.1fh → %.1fGB/h，"
                     "物理上限约 %.1f 集/%dh（洗版 lane 已让路给新增集）"
                     % (_bb.get("remaining_gb") or 0, _bb.get("hours_left") or 0,
                        _bb.get("pace_gb_h") or 0, _bb.get("max_3h_eps") or 0,
                        NO_FINISH_HOURS))
    if _bb.get("gate_holding"):
        notes.append("预算闸门在拒（reason=%s，余 %.1f/%.1fGB）→ 近 %dh 产出不作为产能判据"
                     % (_bb.get("gate_reason") or "-", _bb.get("remaining_gb") or 0,
                        _bb.get("daily_gb") or 0, NO_FINISH_HOURS))
        # 但「预算还有一大半却一集不放」是另一回事：那是闸门口径/闸门本身卡住，
        # 必须报，否则闸门自己停摆也能一路绿。
        if not _exhausted and float(_bb.get("daily_gb") or 0) > 0 and \
                float(_bb.get("remaining_gb") or 0) > 0.30 * float(_bb.get("daily_gb") or 1):
            problems.append("闸门在拒但预算仍余 %.0f/%.0fGB（pace %.1fGB/h）："
                            "不是预算不足，是闸门口径或闸门本身卡住"
                            % (_bb.get("remaining_gb") or 0, _bb.get("daily_gb") or 0,
                               _bb.get("pace_gb_h") or 0))
    _gb = rep.get("gate_blocked_3h")
    _gb = _gb if isinstance(_gb, dict) else {}
    if _gb.get("secs", 0) >= 0.2 * NO_FINISH_HOURS * 3600 \
            and _gb.get("last_end") is not None and _gb["last_end"] < 5400:
        notes.append("近 %dh 里预算闸门按设计停了 %.0f 分钟（%s），刚放开 %.0f 分钟 → "
                     "产能判据本轮不适用，下一轮自动恢复"
                     % (NO_FINISH_HOURS, _gb["secs"] / 60.0,
                        ",".join(_gb.get("reasons") or []) or "-",
                        _gb["last_end"] / 60.0))
    if not rep["throughput_ok"]:
        problems.append("产能塌陷：近 %d h %d 条，过去 24h 均值 %.1f 条/h（仅 %.0f%%）"
                        % (NO_FINISH_HOURS, rep["done_3h"], rep["rate_24h_per_h"],
                           100.0 * rep["done_3h"] / max(1e-9, rep["rate_24h_per_h"] * NO_FINISH_HOURS)))
    # 判据改成「近 3h 的 duplicate」，并同时参考完成数。
    # 全天累计会把修复前的历史一直带进来（今日一度 1107 条，修好后仍是 1107），
    # 让「已经好了」持续报警；真正该问的是「现在还在空转吗」。
    if dup_3h > max(30, 3 * rep["done_3h"]):
        problems.append("队列空转：近 3h %d 条 duplicate 扫描 vs 完成 %d 条（今日累计 %d，最多 %s）"
                        % (dup_3h, rep["done_3h"], dup_n,
                           ", ".join("%s×%d" % (s, n) for s, n in dup_top) or "-"))
    if rep["orphan_claims"]:
        problems.append("%d 个疑似孤儿 claim（同一 lane 有更新任务，需核对对应远端 job/manifest）：%s"
                        % (len(rep["orphan_claims"]),
                           ", ".join("%s/%s(%s,%.0fmin)" % (c["file"], c["lane"],
                                                              c["series"] or "-", c["age_min"])
                                     for c in rep["orphan_claims"][:3])))
    q = dict(q) if not isinstance(q, dict) else q
    cold, total = new_done_cold_count()
    rep["queue_new_cold"] = cold
    rep["queue_new_scannable"] = max(0, total - cold)
    print("  [queue] new 队列 %d 条，其中 %d 条在 new_done 冷却中（7 天内不再扫），实际待扫 %d 条"
          % (total, cold, max(0, total - cold)))
    if rep["stale_claims"]:
        problems.append("%d 个 claim 超过 %d 分钟待核对（年龄不是卡单证据；先查对应远端 job/manifest，不自动清理）"
                        % (len(rep["stale_claims"]), STALE_CLAIM_MIN))
    if rep.get("failed_claims"):
        problems.append("失败 claim 超过 %d 分钟仍未释放（占着 lane 不出片，需 gha_cancel --run-id 释放）：%s"
                        % (FAILED_GRACE_MIN,
                           ", ".join("%s/%s(%s)" % (c["lane"], c["series"][:10],
                                                    c["error"][:24] or "failed")
                                     for c in rep["failed_claims"][:3])))
    if q.get("fill", 0) == 0 and rep["done_today"]:
        problems.append("fill lane 队列已空（需要补队列，否则 fill 停摆）")
    if rep["budget"]["saturated"]:
        # 2026-11-13：配额打满本身不是故障 —— 每源 300/天是硬顶，用满说明在满负荷跑。
        # 真正的问题是「配额满了产能还没起来」，那已经被上面的 throughput_ok 抓住了。
        # 这里降级为提示：写进 notes，不进 problems，避免每天固定几条假 FAIL。
        sat = [str(x) for x in (rep["budget"]["saturated"] or [])]
        notes.append("源日配额已用满（正常满负荷，次日 00:00 CST 重置）：%s"
                     % ",".join(sat[:3]))
    if rep["emby_delta"] is not None and rep["emby_delta"] < 0:
        problems.append("Emby 集数下降 %d（可能误删/索引异常）" % abs(rep["emby_delta"]))
    rep["verdict"] = "PASS" if not problems else ("FAIL" if rep["done_3h"] == 0 else "WARN")
    rep["problems"] = problems
    rep["notes"] = notes

    if a.json:
        print(json.dumps(rep, ensure_ascii=False, indent=1))
        return 0 if rep["verdict"] == "PASS" else (1 if rep["verdict"] == "WARN" else 2)

    print("== 远程上片日报（五路 runner）（%s）—— %s ==" % (rep["ts"], rep["verdict"]))
    print("  完成：今日 %d 条 / 近 3h %d 条 / 累计 %d 条（最近一次 %s）"
          % (rep["done_today"], rep["done_3h"], rep["done_total"], rep["last_finish"] or "无"))
    print("  队列：new %d / fill %d / upgrade %d" % (q["new"], q["fill"], q["upgrade"]))
    print("  产能：近 %dh %.1f 条/h，过去 24h %.1f 条/h；duplicate 近3h %d / 今日累计 %d%s"
          % (NO_FINISH_HOURS, rep["rate_3h_per_h"], rep["rate_24h_per_h"], dup_3h, dup_n,
             ("（今日最多 %s）" % ", ".join("%s×%d" % (s, n) for s, n in dup_top)) if dup_top else ""))
    print("  Emby 集数 %d（较上次快照 %s）" % (ep, ("%+d" % rep["emby_delta"]) if rep["emby_delta"] is not None else "n/a"))
    print("  源配额：上限 %s/天；已用 %s" % (rep["budget"]["daily_limit"],
                                             ", ".join("%s=%d" % kv for kv in list(used.items())[:4]) or "-"))
    if rep["stale_claims"]:
        print("  待核对 claim（不可仅凭年龄清理）：%s" % ", ".join("%s/%s(%.0fmin)" % (c["file"], c["lane"] or "unknown", c["age_min"]) for c in rep["stale_claims"][:5]))
    if rep.get("slow_claims"):
        print("  慢但在跑：%s" % ", ".join("%s(%.0fmin，%.0fmin前有进度)"
                                          % (c["file"], c["age_min"], c["progress_min"]) for c in rep["slow_claims"][:5]))
    if problems:
        print("  问题：")
        for p in problems:
            print("    ✘", p)
    else:
        print("  ✅ 正常：有完成、无卡单、队列有余量、集数未下降")
    for n in notes:
        print("  ℹ", n)
    return 0 if rep["verdict"] == "PASS" else (1 if rep["verdict"] == "WARN" else 2)


if __name__ == "__main__":
    sys.exit(main())
