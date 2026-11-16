#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""stall_watch.py —— 停摆守望：配额还有、却长时间零出片时，自己诊断并告警。

为什么要有它
------------
2026-11-14 实测：五条 lane 在配额只用了 43% 的情况下**静默停摆 40 分钟**。
期间没有任何报错、没有告警、所有监控都是绿的 —— 因为"没出片"本身不产生事件。
排查花了很久，最后挖出七层缺陷（见 docs/ERRORS.md E057-E059）。

这类故障的特征：**它的症状是"什么都没发生"**。
所以必须有东西定期问一句："本该有片子进来的这段时间，为什么没有？"

判据（不是"有没有在跑"，而是"该不该有产出"）
--------------------------------------------
1. 距最近一次落地超过 STALL_MIN 分钟
2. 且**配额还有余量**（否则空闲是正常的）
3. 且当前不在人为暂停（.gha_paused_new 之类）

诊断内容（直接写进告警，省掉下一次重查）
----------------------------------------
- 各源状态：day/cap/ok/burst/cooldown，以及被哪个闸门挡住
- 最近几次决策的 skip_reasons 直方图
- 今日出片数、已用配额

用法
----
    python3 stall_watch.py            # 只读，打印诊断
    python3 stall_watch.py --alert    # 判定停摆时调用 alert.py
"""
import argparse
import glob
import json
import os
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DB = os.path.join(HERE, "tg.db")
BUDGET = os.path.join(HERE, ".gha_src_budget.json")
LOG = os.path.join(HERE, "logs", "stall_watch.log")
WORK_DIRS = "/opt/embytg-runner/work/*/job.json"

# 字节预算闸门（embytg-runner/budget_gate.py）的决策落盘。E153 当时只把它接进了
# autoops.output_stalled，stall_watch 这条**独立的**判据没接 —— 于是预算见底、
# 闸门明明在按设计停，它照样按「配额有余 + 90 分钟没片」拉 P1（2026-11-16 22:02 实测）。
GATE_STATE = os.path.join(HERE, "logs", ".budget_gate_state.json")
GATE_HOLD_MARK = os.path.join(HERE, "logs", ".stall_gate_hold.json")
GATE_STALE_SEC = 45 * 60        # 闸门状态超过这么久没更新 → 不采信（run_one 每轮都写）
GATE_MAX_HOLD_SEC = 6 * 3600    # 连续按住超这个时长且预算还有余 → 不是设计内，照报
GATE_HOLD_REASONS = ("pacing", "reserve_new", "daily_exhausted", "month_exhausted")

STALL_MIN = 60          # 连续多少分钟零落地算"可疑"
ALERT_MIN = 90          # 超过这个才真的告警（60 分钟可能只是配额安排）

PAUSE_MARKERS = (".gha_paused_new", ".gha_paused", ".gha_stop")


def _log(msg):
    line = "%s %s" % (time.strftime("%F %T"), msg)
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(LOG), exist_ok=True)
        # 2026-11-16：**stdout 已经指向这份日志时不要再写一遍。**
        # embytg-stallwatch.service 用 StandardOutput=append:…/stall_watch.log，
        # 而这里又 open(LOG,'a') 写一次 —— 实测每条日志都出现 2 遍（00:50 那次：
        # 同样的「判定：正常」两行），把 log 刷成一半是噪音，看起来像跑了两轮。
        try:
            if os.stat(LOG).st_ino == os.fstat(sys.stdout.fileno()).st_ino:
                return
        except (OSError, ValueError):
            pass
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:                                          # noqa: BLE001
        pass


def last_landing():
    try:
        c = sqlite3.connect("file:%s?mode=ro" % DB, uri=True)
        r = c.execute("select max(landed_at) from resource_state").fetchone()
        c.close()
        return float((r and r[0]) or 0)
    except Exception:                                          # noqa: BLE001
        return 0.0


def budget_state():
    """返回 (used, cap_total, per_source_lines)。"""
    try:
        d = json.load(open(BUDGET, encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        return 0, 0, []
    lim = (d.get("limits") or {}).get("daily", 300)
    burst = (d.get("limits") or {}).get("burst_20m", 60)
    now = time.time()
    used = cap = 0
    lines = []
    for lab, v in sorted((d.get("sources") or {}).items()):
        day = int(v.get("day") or 0)
        ok = int(v.get("ok") or 0)
        ev = len(v.get("events") or [])
        c = int(lim * _penalty(int(v.get("download_fails") or 0)))
        used += day
        cap += c
        why = []
        if float(v.get("cooldown_until") or 0) > now:
            why.append("cool")
        if float(v.get("download_cooldown_until") or 0) > now:
            why.append("dl-cool")
        if day >= c:
            why.append("cap %d/%d" % (day, c))
        if ev >= burst:
            why.append("burst %d/%d" % (ev, burst))
        lines.append("  %-26s day=%-4d ok=%-3d %s"
                     % (lab, day, ok, " | ".join(why) or "可用"))
    return used, cap, lines


def _penalty(f):
    if f <= 0:
        return 1.0
    if f <= 2:
        return 0.75
    if f <= 5:
        return 0.5
    if f <= 9:
        return 0.3
    return 0.35


def recent_skips(limit=4):
    """最近几次决策的 reason 与 skip_reasons。"""
    files = sorted(glob.glob(WORK_DIRS), key=os.path.getmtime, reverse=True)[:limit]
    out = []
    for p in files:
        try:
            d = json.load(open(p, encoding="utf-8"))
        except Exception:                                      # noqa: BLE001
            continue
        out.append({
            "t": time.strftime("%H:%M", time.localtime(os.path.getmtime(p))),
            "reason": d.get("reason"),
            "attempts": d.get("attempts"),
            "skip": d.get("skip_reasons"),
            "detail": d.get("detail"),
        })
    return out


def _usable_remain(remain):
    """剩余额度里，真正**能花出去**的部分。

    只算"还有余量 + 队列里有未冷却条目"的源。
    否则会出现今天那种误报：额度还有 270，
    但能花的源全没条目了。
    """
    try:
        q = json.load(open(os.path.join(HERE, ".gha_queue_new.json"), encoding="utf-8"))
        items = q.get("items") if isinstance(q, dict) else q
        done = json.load(open(os.path.join(HERE, ".gha_new_done.json"), encoding="utf-8"))
        now = time.time()
        hot = {str(k) for k, v in done.items()
               if isinstance(v, dict) and now < float(v.get("next_check") or 0)}
        have = {str(i.get("source_label")) for i in (items or [])
                if i.get("source_label") and str(i.get("video_id")) not in hot}
    except Exception:                                          # noqa: BLE001
        return remain
    if not have:
        return 0
    try:
        d = json.load(open(BUDGET, encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        return remain
    lim = (d.get("limits") or {}).get("daily", 300)
    now = time.time()
    tot = 0
    for lab, v in (d.get("sources") or {}).items():
        if lab not in have:
            continue
        if float(v.get("cooldown_until") or 0) > now:
            continue
        if float(v.get("download_cooldown_until") or 0) > now:
            continue
        # 复用主逻辑的公式，不自己写一份 ——
        # 刚刚就因为自己写了一份（没带产出加成），两边算出来的可用额度不同。
        try:
            import gha_state as _GS
            cap = min(400, int(lim * _GS._source_yield_penalty(v, now)
                               * _GS._source_yield_bonus(v, now)))
        except Exception:                                      # noqa: BLE001
            cap = min(400, int(lim * _penalty(int(v.get("download_fails") or 0))))
        tot += max(0, cap - int(v.get("day") or 0))
    return min(remain, tot)


def paused():
    return [m for m in PAUSE_MARKERS if os.path.exists(os.path.join(HERE, m))]


def gate_hold(now=None, path=None, mark=None):
    """字节预算闸门此刻是不是「按设计停」。

    为什么必须问它（E158，2026-11-16 22:02）
    ----------------------------------------
    日字节预算见底后，`run_one.sh` 在**领 claim 之前**就问 budget_gate，
    拿到 rc=75 就 BUDGET_WAIT 睡 15 分钟再问 —— 这期间一条 lane 都不会启动，
    一集也不可能落地。这是设计，不是停摆。

    但 stall_watch 的判据只看源站配额（request/day）和暂停标记：
    22:02 那天源站配额余 754 次（>30）、没有暂停标记、90 分钟零落地 →
    判「疑似停摆」拉 P1，而真相是 `used_today 449GB / daily 455GB`，
    闸门已经连按三小时。一天里 E153 修过同一个坑的另一半
    （autoops.output_stalled），这条独立判据被漏掉了。

    返回 dict 或 None：
        None                     闸门没写过状态（老版本 / 未启用）→ 不看它
        {"holding": False, ...}  闸门放行，或状态太旧不敢采信
        {"holding": True, ...}   闸门在按设计停，零出片属正常

    `hold_sec` 是「连续按住多久」：连续 6 小时以上还按着、而预算明明有余，
    那就不是设计内了（闸门自己卡住 / 口径算错），交给上层照报。
    """
    now = float(now if now is not None else time.time())
    p = path or os.environ.get("STALL_GATE_STATE") or GATE_STATE
    mp = mark or os.environ.get("STALL_GATE_MARK") or GATE_HOLD_MARK
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:                                          # noqa: BLE001
        return None
    try:
        ts = float(d.get("ts") or 0)
        rc = int(d.get("rc") or 0)
    except (TypeError, ValueError):
        return None
    reason = str(d.get("reason") or "")
    age = now - ts
    holding = (rc != 0 and reason in GATE_HOLD_REASONS and age <= GATE_STALE_SEC)

    # 连续按住时长：只在「这一刻在按」时累加，放行/状态过期就清零
    since = 0.0
    if holding:
        try:
            with open(mp, encoding="utf-8") as f:
                since = float((json.load(f) or {}).get("since") or 0)
        except Exception:                                      # noqa: BLE001
            since = 0.0
        if not since or since > now:
            since = ts or now
        try:
            os.makedirs(os.path.dirname(mp), exist_ok=True)
            with open(mp, "w", encoding="utf-8") as f:
                json.dump({"since": since}, f)
        except Exception:                                      # noqa: BLE001
            pass
    else:
        try:
            os.unlink(mp)
        except OSError:
            pass

    return {
        "holding": holding,
        "reason": reason,
        "rc": rc,
        "age_sec": round(age),
        "remaining_gb": d.get("remaining_gb"),
        "daily_gb": d.get("daily_gb"),
        "hold_sec": int(now - since) if holding and since else 0,
    }


def gate_hold_blocks(gate):
    """闸门按设计停 → 这次「零出片」不算停摆。

    唯一的例外：`pacing`/`reserve_new` 这种「本该是短暂的」节流连按超过
    GATE_MAX_HOLD_SEC 还不放（预算明明有余）—— 那不是节流，是卡死。

    `daily_exhausted`/`month_exhausted` 是硬停：预算到顶就该一直停到
    00:00 / 下月 1 日，停 10 小时也是设计内。这两条**不能**受 6 小时上限约束，
    否则「13:00 把日额度用完」的日子会在 19:00 收到一条假 P1（下一轮又变正常）。
    """
    if not gate or not gate.get("holding"):
        return False
    if gate.get("reason") in ("daily_exhausted", "month_exhausted"):
        return True
    return gate.get("hold_sec", 0) < GATE_MAX_HOLD_SEC


def lane_auth_dead():
    """E116：某 lane 的授权被永久吊销时，全局「有产出」会掩盖它。

    2026-11-15 实测：new4 全部 290 个 work 都 AuthKeyDuplicated，今日产出 0，
    但其它 lane 一直在出片 → `stall_watch` 判「正常」，这个 lane 白烧了整天才有人发现。
    判据：本机 .gha_lane_auth_dead/* 里以 permanent 开头的文件 = 需要人工重登的 lane。
    """
    d = os.path.join(HERE, ".gha_lane_auth_dead")
    # 2026-11-16：**已停用的 lane 不再报。**
    # 云电脑（main/box2/box3 → new3/new4/new5）2026-11-15 起全部下线，
    # scale.json 里已置 enabled=false。它们残留的永久吊销记录还躺在
    # .gha_lane_auth_dead/ 里，如果照旧报警，等于每轮都在喊「去重登一台不存在的机器」——
    # 告警指错了人（用户会去控制台白折腾）。判据只认「仍启用的 lane」。
    enabled = _enabled_lanes()
    out = []
    try:
        for name in sorted(os.listdir(d)):
            if enabled is not None and name not in enabled:
                continue
            p = os.path.join(d, name)
            try:
                head = open(p, encoding="utf-8").read(64)
            except OSError:
                continue
            if head.startswith("permanent"):
                out.append(name)
    except FileNotFoundError:
        pass
    return out


def _enabled_lanes():
    """config/scale.json 里 enabled=true 的 lane 集合；读不到就返回 None（保守：全都算启用）。"""
    try:
        with open(os.path.join(HERE, "config", "scale.json"), encoding="utf-8") as fh:
            items = (json.load(fh) or {}).get("lanes", {}).get("items") or []
        names = {x["lane"] for x in items
                 if isinstance(x, dict) and x.get("lane") and x.get("enabled", True)}
        return names or None
    except Exception:                                      # noqa: BLE001
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--alert", action="store_true", help="判定停摆时调用 alert.py")
    ap.add_argument("--stall-min", type=int, default=STALL_MIN)
    a = ap.parse_args()

    now = time.time()
    last = last_landing()
    idle_min = (now - last) / 60.0 if last else 1e9
    used, cap, lines = budget_state()
    remain = max(0, cap - used)
    pauses = paused()

    _log("最近落地 %s，已 %s；配额 %d/%d（余 %d）%s" % (
        time.strftime("%F %T", time.localtime(last)) if last else "无记录",
        ("%.0f 分钟" % idle_min) if idle_min < 1e8 else "从未",
        used, cap, remain,
        ("；暂停标记 " + ",".join(pauses)) if pauses else ""))

    # 2026-11-14：**用"真正能花的额度"判断，不是总剩余**。
    # 实测：总剩 270 次，但有条目的源全部打满或冷却，
    # 剩下的额度全在"目录已经收完、队列里没有条目"的源上 ——
    # 那就不是停摆，是今天的产能真的用完了，不该拉警。
    dead = lane_auth_dead()
    if dead:
        _log("  ⚠ 授权被永久吊销的 lane：%s（需人工重登；不重登重试一万次也不会成功）"
             % ",".join(dead))

    # E158：零出片可能是字节闸门在按预算停 —— 必须问闸门，不能只看源站配额
    gate = gate_hold(now)
    blocked = gate_hold_blocks(gate)
    if gate and gate.get("holding"):
        _log("  字节闸门：reason=%s 按住 %d 分钟（余 %s/%s GB，状态 %ds 前）"
             % (gate["reason"], gate.get("hold_sec", 0) // 60,
                gate.get("remaining_gb"), gate.get("daily_gb"), gate.get("age_sec", 0)))
    elif gate:
        _log("  字节闸门：放行（reason=%s，状态 %ds 前）"
             % (gate.get("reason"), gate.get("age_sec", 0)))

    usable = _usable_remain(remain)
    stalled = (idle_min >= a.stall_min and usable > 30 and not pauses and not blocked)
    if not stalled:
        _log("  判定：正常（%s）" % (
            "预算闸门按设计停：reason=%s，余 %s/%s GB" % (
                gate.get("reason"), gate.get("remaining_gb"), gate.get("daily_gb"))
            if blocked else
            "有暂停标记" if pauses else
            "听任空闲：剩余 %d 次额度里只有 %d 次能花"
            % (remain, usable) if usable <= 30 else
            "近期有产出"))
        return 0

    _log("  ⚠ 判定：疑似停摆 —— 还有 %d 次配额、%d 分钟零出片%s" % (
        remain, idle_min,
        "；闸门已连按 %.1f 小时（超上限，按卡死处理）" % (gate["hold_sec"] / 3600.0)
        if gate and gate.get("holding") else ""))
    for ln in lines:
        _log(ln)
    for s in recent_skips():
        _log("  决策 %s reason=%s attempts=%s skip=%s %s"
             % (s["t"], s["reason"], s["attempts"], s["skip"], s["detail"] or ""))

    if a.alert and idle_min >= ALERT_MIN:
        msg = ("EmbyTG 疑似停摆：%.0f 分钟零出片，配额仍余 %d/%d。\n"
               "%s各源：\n%s\n最近决策：%s"
               % (idle_min, remain, cap,
                  ("字节闸门：reason=%s，连按 %.1f 小时，余 %s/%s GB\n"
                   % (gate.get("reason"), gate.get("hold_sec", 0) / 3600.0,
                      gate.get("remaining_gb"), gate.get("daily_gb")))
                  if gate and gate.get("holding") else "",
                  "\n".join(lines),
                  json.dumps(recent_skips(3), ensure_ascii=False)))
        try:
            import subprocess
            subprocess.run([sys.executable, os.path.join(HERE, "alert.py"),
                            "--level", "P1", "--code", "stall",
                            "--msg", msg], check=False, timeout=120)
            _log("  已发出告警")
        except Exception as e:                                 # noqa: BLE001
            _log("  告警发送失败：%s" % e)
    return 0


if __name__ == "__main__":
    sys.exit(main())
