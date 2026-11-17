#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""budget_gate.py —— 下载前的「日字节预算 + 匀速闸门」（E148/E151）

为什么要有它
------------
TG 账号和源服都有日流量上限，但 runner 在此之前**没有任何字节闸门**：谁快谁
全速。2026-11-16 的表现是前 5 小时烧掉 204GB（41GB/h），之后 19 小时落在
throttled 区间空转（单段 134MB 传不完 → worker 900s 超时 → UPLOAD failed
→ claim 释放回队首）。「上传超时」是症状，「前 5 小时烧光、后 19 小时空转」
才是病根（docs/ERRORS.md E148）。

怎么算
------
不写死速率，按「剩余预算 ÷ 剩余小时」算出此刻该走多快：
    pace = (daily_gb - used_today) / hours_left
四个 lane 共用同一个 pace（用最近 60 分钟的实际落地字节比，而不是各算各的，
否则 4 个 lane 各自放行 → 实际 4 倍速）。预算紧张时只放行 new（净增集数），
fill/upgrade（洗版）让路：洗版不许抢新增集的配额。

判据是「已经落地多少」，不是「已经领了多少」：landed_at 只在文件落进 teldrive
后才有，所以下载中的文件还没进账 —— 这是已知偏差，用 slack(1.25) 吸收。

退出码
------
0   放行
75  需要等待（stdout JSON 里有 wait_sec）
3   预算硬停（今日预算或月度安全线已到）

env 覆盖（全部可配，不写死）
---------------------------
TG_BUDGET_GATE=0            完全关闭闸门
TG_BUDGET_DB                tg.db 路径
TG_BUDGET_CFG               .ingest_budget.json 路径
TG_BUDGET_DAILY_GB          日上限（默认取 cfg）
TG_BUDGET_SLACK             允许超 pace 的倍数（默认 1.25）
TG_BUDGET_MIN_STOP_GB       剩余低于此值直接停（默认 1.0）
TG_BUDGET_MIN_RATE_GB_H     new lane 的速率下限（默认 2.0 GB/h）
TG_BUDGET_NEW_RESERVE_FRAC  剩余预算低于日上限的这个比例时，非 new lane 停（默认 0.35）
TG_BUDGET_WINDOW_SEC        滑动窗口（默认 3600）
"""
import argparse
import datetime
import json
import os
import re
import random
import sqlite3
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
PIPE = "/opt/embytg/pipeline"
DEFAULT_CFG = {"daily_gb": 300.0, "monthly_gb": 10000.0, "safety_gb": 9000.0,
               "month_start": "2026-11-10"}
NEW_LANES = {"new", "new2", "new3", "new4", "new5"}
COUNTED_KINDS = ("episode", "movie")


def env_f(name, default):
    try:
        return float(os.environ.get(name, default))
    except Exception:                                              # noqa: BLE001
        return float(default)


def cfg():
    d = dict(DEFAULT_CFG)
    p = os.environ.get("TG_BUDGET_CFG") or os.path.join(PIPE, ".ingest_budget.json")
    try:
        with open(p, encoding="utf-8") as f:
            d.update(json.load(f))
    except Exception:                                              # noqa: BLE001
        pass
    d["daily_gb"] = env_f("TG_BUDGET_DAILY_GB", d.get("daily_gb") or 300.0)
    return d


def db_path():
    return os.environ.get("TG_BUDGET_DB") or os.path.join(PIPE, "tg.db")


def _sum_between(con, t_from, t_to=None):
    q = ("select coalesce(sum(size),0), count(*) from resource_state "
         "where landed_at>=? and kind in (%s)" % ",".join("?" * len(COUNTED_KINDS)))
    args = [int(t_from)] + list(COUNTED_KINDS)
    if t_to is not None:
        q += " and landed_at<?"
        args.append(int(t_to))
    row = con.execute(q, args).fetchone()
    return float(row[0] or 0) / 1e9, int(row[1] or 0)


IFACE = os.environ.get("TG_BUDGET_IFACE", "enp0s6")


def _egress_gb_since(day_from):
    """网卡出站（GB），从 day_from（date）起算。

    为什么按网卡而不按台账：Oracle 计费的是**网卡出站**，台账只记成功落地的
    字节。零流量转存（tgpipe）落地但不产生出站 —— 实测 2026-11-10 起台账
    2439GB、网卡出站 1683GB，用台账当额度会把免费额度提前判成用光；
    反过来若重传很多，台账又会低估真实出站。按计费口径走才不误判。
    """
    try:
        out = subprocess.run(["vnstat", "-i", IFACE, "-d"],
                             capture_output=True, text=True, timeout=20).stdout
    except Exception:                                            # noqa: BLE001
        return None
    total, seen = 0.0, False
    pat = re.compile(r"\s+(\d{4}-\d{2}-\d{2})\s+([\d.]+)\s+GiB\s*\|\s*([\d.]+)\s+GiB")
    for line in out.splitlines():
        m = pat.match(line)
        if not m or m.group(1) < day_from.isoformat():
            continue
        total += float(m.group(3)) * 1.073741824          # GiB → GB
        seen = True
    return total if seen else None


def _inflight_gb(now):
    """在飞字节：已领 claim、还没落地的下载量（上界）。

    为什么要算：landed_at 只在文件落进 teldrive 后才有 —— 4 个 lane 各抱着
    一个 4GB 的活时，闸门看到的窗口是 0，于是放行 → 四集同时落地，
    下个小时的额度被一口气吃掉（突发，不是超预算）。把在飞的 sizeof(claim)
    当上界先扣掉，闸门才会「按节奏放一个、等它落地再放下一个」。
    """
    cd = os.environ.get("TG_BUDGET_CLAIMS_DIR") or os.path.join(PIPE, ".gha_claims")
    if not os.path.isdir(cd):
        return 0.0
    total = 0
    try:
        for fn in os.listdir(cd):
            if not fn.endswith(".json"):
                continue
            try:
                with open(os.path.join(cd, fn), encoding="utf-8") as f:
                    j = json.load(f)
            except Exception:                                      # noqa: BLE001
                continue
            ts = float(j.get("ts") or 0)
            # 只看还在跑的；超过 4 小时还没落地的 claim 由 claim_reap 处理，
            # 不能一直压在闸门头上（否则一个僵尸 claim 能把整机停 6 小时）
            if ts and now - ts > 4 * 3600:
                continue
            if str(j.get("status") or "").lower() not in ("running", "claimed", ""):
                continue
            total += int(j.get("size") or 0)
    except Exception:                                              # noqa: BLE001
        return 0.0
    return float(total) / 1e9


def evaluate(lane, now=None):
    """返回决策 dict。纯函数式：只读库和配置，不睡眠。"""
    now = float(now if now is not None else time.time())
    c = cfg()
    daily = float(c["daily_gb"])
    safety = float(c.get("safety_gb") or 9000.0)
    min_stop = env_f("TG_BUDGET_MIN_STOP_GB", 1.0)
    slack = env_f("TG_BUDGET_SLACK", 1.25)
    min_rate = env_f("TG_BUDGET_MIN_RATE_GB_H", 2.0)
    reserve_frac = env_f("TG_BUDGET_NEW_RESERVE_FRAC", 0.35)
    window = env_f("TG_BUDGET_WINDOW_SEC", 3600)

    day0 = datetime.datetime.combine(datetime.date.fromtimestamp(now),
                                     datetime.time.min)
    t_day0 = int(time.mktime(day0.timetuple()))
    t_next = t_day0 + 86400
    hours_left = max(0.5, (t_next - now) / 3600.0)

    # Oracle 的 10TB 是**自然月**额度（1 号 00:00 重置）。原来读配置里的
    # month_start=2026-11-10 —— 钉死的锚点，跨月不前进，攒到 9000GB 就永久停摆。
    # 现在取当月 1 号；TG_BUDGET_MONTH_START 可显式覆盖。
    _ov = os.environ.get("TG_BUDGET_MONTH_START")
    try:
        _ms = (datetime.datetime.strptime(_ov, "%Y-%m-%d") if _ov else day0.replace(day=1))
        month_start_date = _ms.date()
        t_month = int(time.mktime(_ms.timetuple()))
    except Exception:                                              # noqa: BLE001
        month_start_date = day0.date().replace(day=1)
        t_month = t_day0

    out = {"ok": False, "lane": lane, "ts": int(now)}
    try:
        con = sqlite3.connect("file:%s?mode=ro" % db_path(), uri=True, timeout=10)
        used_today, _ = _sum_between(con, t_day0)
        used_window, _ = _sum_between(con, now - window)
        used_month, _ = _sum_between(con, t_month)
        con.close()
        if os.environ.get("TG_BUDGET_METRIC", "egress") == "egress":
            _eg = _egress_gb_since(month_start_date)
            if _eg is not None:
                used_month = _eg
    except Exception as e:                                         # noqa: BLE001
        # 读不到账 = 不知道预算 → 不拦（拦了就是全停），但要吼出来
        out.update({"rc": 0, "ok": True, "reason": "budget_unknown:%s" % str(e)[:60]})
        print(json.dumps(out, ensure_ascii=False))
        return out

    remaining = daily - used_today
    month_left = safety - used_month
    inflight = _inflight_gb(now) if os.environ.get("TG_BUDGET_COUNT_INFLIGHT", "1") != "0" else 0.0
    used_eff = used_window + inflight
    # 2026-11-16（懒人模式）：**日额度自动跟随月度余额，不再需要人工调参。**
    # 原来日上限是个写死的数（300 / 500），月里前面用得快、后面就白浪费，
    # 或者反过来上午就打光。现在按「月剩余 ÷ 剩余天数」自动定当日额度，
    # 上下限 50~2000GB 兜底；TG_BUDGET_AUTO=0 可回到固定值。
    if os.environ.get("TG_BUDGET_AUTO", "1") == "1":
        try:
            # 注意：hours_left 是「今天还剩几小时」，不是「本月还剩几天」。
            # 用 hours_left/24 会把月余额摊到不足一天里，算出 2000GB 这种离谱值
            # （实测 month_left 6877GB / 0.335 天 → 被上限截断）。
            _today = datetime.date.today()
            _next_month = (_today.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
            _days_left = max(1.0, float((_next_month - _today).days))
            _auto_daily = max(50.0, min(1200.0, float(month_left) / _days_left))
            if abs(_auto_daily - float(daily)) > 1.0:
                print("# 日额度自动：month_left=%.0fGB / %.1f天 → %.0fGB"
                      % (month_left, _days_left, _auto_daily), file=sys.stderr, flush=True)
            daily = _auto_daily
            remaining = daily - used_today
        except Exception:                              # noqa: BLE001
            pass

    pace = max(0.0, remaining) / hours_left
    is_new = lane in NEW_LANES

    # 2026-11-16（用户口径）：**不要小时节流，只看日/月总额度。**
    # 原来的 allow = max(pace*slack, min_rate) 把出站压到 ~2GB/h，
    # 而实际一集 1.5GB —— 相当于每小时只放一集多，最后表现成
    # 「BUDGET_WAIT reason=pacing」反复等待，产能被自己卡死。
    # 现在 pacing 默认关闭（TG_BUDGET_PACING=1 可恢复）：
    # 只要日额度（TG_BUDGET_DAILY_GB）和月额度还有剩，就全速跑；
    # 撞到额度自然落到下面的 daily_exhausted / month_exhausted。
    _pacing = os.environ.get("TG_BUDGET_PACING", "0") == "1"
    if not _pacing:
        allow = max(remaining, 0.0)
    else:
        allow = max(pace * slack, 0.0)
        if is_new:
            allow = max(allow, min_rate)
        else:
            # 洗版让路：预算进入尾部（剩余不足日上限 reserve_frac）时停止非 new lane
            if remaining < daily * reserve_frac:
                allow = 0.0
            else:
                allow = max(allow * 0.5, 0.5)

    out.update({
        "used_today_gb": round(used_today, 2),
        "used_window_gb": round(used_window, 2),
        "inflight_gb": round(inflight, 2),
        "used_effective_gb": round(used_eff, 2),
        "used_month_gb": round(used_month, 1),
        "daily_gb": round(daily, 1),
        "remaining_gb": round(remaining, 2),
        "month_left_gb": round(month_left, 1),
        "hours_left": round(hours_left, 2),
        "pace_gb_h": round(pace, 2),
        "allow_gb_h": round(allow, 2),
        "window_sec": int(window),
    })

    if remaining <= min_stop:
        out.update({"rc": 3, "ok": False, "reason": "daily_exhausted",
                    "wait_sec": 1800})
    elif month_left <= min_stop:
        out.update({"rc": 3, "ok": False, "reason": "month_exhausted",
                    "wait_sec": 3600})
    elif allow <= 0.0:
        out.update({"rc": 75, "ok": False, "reason": "reserve_new",
                    "wait_sec": 900 + random.randint(0, 120)})
    elif used_eff >= allow:
        # 超速：等一个窗口的 1/4，让滑动窗口衰减；至少 300s
        wait = max(300, int(window / 4) + random.randint(0, 90))
        out.update({"rc": 75, "ok": False, "reason": "pacing", "wait_sec": wait})
    else:
        out.update({"rc": 0, "ok": True, "reason": "ok", "wait_sec": 0})
    return out


def state_path():
    """闸门状态文件路径（env 可覆盖，测试用）。"""
    return os.environ.get("TG_BUDGET_STATE") or os.path.join(
        PIPE, "logs", ".budget_gate_state.json")


def write_state(d):
    """把这次的决策落盘，给 autoops 判「没产出是不是设计内的」。

    为什么要有（E153，2026-11-16 11:41）
    ------------------------------------
    日预算见底后 run_one.sh 按设计 `IDLE_BUDGET_PAUSE` 睡 30 分钟，这段时间
    一集也不会落地。但 autoops 的 `output_stalled` 只认「45 分钟没新片」→
    11:38 与 12:11 各来一次 reap+restart，11:41 还升级成
    `output_stalled_unfixed` 告警。重启只会重新落进同一个 BUDGET_WAIT，
    纯属空转；判据必须知道闸门此刻在「按预算停」。

    写失败不能影响放行/拦截决策，所以这里吞异常。
    """
    try:
        p = state_path()
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
        os.replace(tmp, p)
    except Exception:                                              # noqa: BLE001
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lane", default=os.environ.get("RUNNER_LANE", "") or "new")
    ap.add_argument("--json", action="store_true", help="只打 JSON（默认也是）")
    a = ap.parse_args()
    if os.environ.get("TG_BUDGET_GATE", "1") == "0":
        d = {"ok": True, "rc": 0, "reason": "gate_disabled",
             "wait_sec": 0, "ts": int(time.time())}
        write_state(d)
        print(json.dumps(d, ensure_ascii=False))
        return 0
    d = evaluate(a.lane)
    write_state(d)
    print(json.dumps(d, ensure_ascii=False))
    return int(d.get("rc", 0))


if __name__ == "__main__":
    sys.exit(main())
