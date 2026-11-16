#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TG 账号池 —— 按实测吞吐自动加权、自动淘汰。

为什么需要池子（2026-11-14 事故）：
    账号被写死成「TG_SESSION_STRING + TG_SESSION_STRING_2」，加第 3/4 个号
    要么被挡（pc_upload 只认 2 个），要么均匀分片（权重硬编码 1,1,1,1）。
    实测当天 TG 把上传压到 0.41 MB/s，本机出口却有 100 MB/s ——
    说明「哪个号快、哪个号被限」是动态变化的，写死权重等于把最快的号
    和已经被限速的号当成一样快，分片全被慢号拖住，最后 worker 超时。

池子的三条规则：
    1. 权重来自**实测**：每个号记 EWMA 吞吐（MB/s），不写死。
    2. 淘汰是**临时**的：连续失败或吞吐低于阈值的号进 cooldown，到期自动
       回池（只降权不进冷宫，防止一次抖动永久废掉一个号）。
    3. 池子空了不硬传：全网都慢时返回最小可行分配，而不是把分片全塞给
       一个已经超时的号。

落盘：config/tg_pool.json（可读、可手改、可纳入备份）。
"""
import json
import os
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
POOL = os.path.join(HERE, "config", "tg_pool.json")

# 慢于这个速度视为「已被限速」，权重压到下限但仍保留在池里
SLOW_MBPS = 1.5
# 连续失败这么多次进 cooldown
FAIL_TRIP = 2
# cooldown 时长（秒）；到期自动回池，权重从 MIN_WEIGHT 爬起
COOLDOWN_SEC = 1800
# 权重上下限：下限保证慢号还能分到一点活（便于恢复后重新测速）
MIN_WEIGHT = 0.05
MAX_WEIGHT = 4.0
# EWMA 平滑系数：新样本占 30%
ALPHA = 0.3


def _load():
    try:
        with open(POOL, encoding="utf-8") as fh:
            d = json.load(fh)
        return d if isinstance(d, dict) else {}
    except Exception:                                      # noqa: BLE001
        return {}


def _save(d):
    """原子写 + 进程内互斥。

    2026-11-14（E093）：**并发写会互相踩死，而且异常被上层吞掉，池化静默失效。**
    实测：8 个上传线程同时 TP.report() → 抢同一个 POOL.tmp →
    FileNotFoundError: tg_pool.json.tmp -> tg_pool.json
    （A 线程写 tmp，B 线程 os.replace 把它搬走，A 再 replace 就找不到）
    结果：上传正常出片，池子永远停在初始值，等于没做池化。

    修法：① 线程锁串行化；② tmp 文件名带 pid+线程 id，彻底避让。
    """
    with _get_lock():
        _save_unlocked(d)


def _save_unlocked(d):
    """调用方必须已持有 _get_lock()。"""
    os.makedirs(os.path.dirname(POOL), exist_ok=True)
    tmp = "%s.%d.%d.tmp" % (POOL, os.getpid(), threading.get_ident())
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(d, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, POOL)


_LOCK = None


def _get_lock():
    """惰性建锁。用 RLock：_report_locked 已持锁时内部还会调 _save，普通 Lock 会自锁死。"""
    global _LOCK
    if _LOCK is None:
        _LOCK = threading.RLock()
    return _LOCK


def _now():
    return time.time()


def discover_sessions():
    """按 N 排序返回可用 session 字符串（slot 序号即 1 起的下标）。"""
    items = []
    for k, v in os.environ.items():
        if not k.startswith("TG_SESSION_STRING"):
            continue
        v = (v or "").strip()
        if not v:
            continue
        sfx = k[len("TG_SESSION_STRING"):]
        if sfx.startswith("_") and sfx[1:].isdigit():
            idx = int(sfx[1:])
        elif not sfx:
            idx = 1
        else:
            continue
        items.append((idx, v))
    out, seen = [], set()
    for idx, v in sorted(items):
        if v in seen:
            continue
        seen.add(v)
        out.append({"slot": idx, "session": v})
    return out


def _entry(d, slot):
    nodes = d.setdefault("nodes", {})
    e = nodes.setdefault(str(slot), {})
    e.setdefault("mbps", 0.0)          # EWMA 吞吐
    e.setdefault("fails", 0)           # 连续失败
    e.setdefault("ok", 0)              # 累计成功区段
    e.setdefault("cooldown_until", 0.0)
    e.setdefault("last_mbps", 0.0)
    return e


def weights(now=None):
    """返回 {slot: weight}，已排除 cooldown 中的号。"""
    now = now or _now()
    d = _load()
    ss = discover_sessions()
    out = {}
    for s in ss:
        e = _entry(d, s["slot"])
        if float(e.get("cooldown_until") or 0) > now:
            continue                                   # 冷却中，不分活
        mb = float(e.get("mbps") or 0)
        if mb <= 0:
            w = 1.0                                     # 新号给中性权重
        else:
            w = max(MIN_WEIGHT, min(MAX_WEIGHT, mb / max(SLOW_MBPS, 0.01)))
        out[s["slot"]] = w
    if not out:                                        # 全在冷却 → 强行全回池
        for s in ss:
            out[s["slot"]] = MIN_WEIGHT
    return out


def report(slot, ok, mbps=None, err=None):
    """记一次结果。ok=True 记吞吐；ok=False 累计失败，到阈值进冷却。

    2026-11-14（E093）：**读-改-写必须整段加锁。**
    原来 _load() 在锁外，8 个线程各自读到同一份旧快照，
    各改自己的一格再整份覆盖回去 → 后写的把先写的抹掉。
    实测 8 次并发只留下 2 条记录，池子学到的数据是残缺的。
    """
    with _get_lock():
        return _report_locked(slot, ok, mbps, err)


def _report_locked(slot, ok, mbps=None, err=None):
    d = _load()
    e = _entry(d, slot)
    now = _now()
    if ok:
        e["fails"] = 0
        e["ok"] = int(e.get("ok") or 0) + 1
        if mbps and mbps > 0:
            old = float(e.get("mbps") or 0)
            e["mbps"] = round(mbps if old <= 0 else (ALPHA * mbps + (1 - ALPHA) * old), 3)
            e["last_mbps"] = round(float(mbps), 3)
        e["cooldown_until"] = 0.0
    else:
        e["fails"] = int(e.get("fails") or 0) + 1
        e["last_err"] = (err or "")[:120]
        if e["fails"] >= FAIL_TRIP:
            e["cooldown_until"] = now + COOLDOWN_SEC
            e["fails"] = 0                             # 进冷却后计数归零，留回血机会
    e["updated"] = int(now)
    _save_unlocked(d)
    return e


def snapshot():
    """给人看的池子状态。"""
    d = _load()
    now = _now()
    rows = []
    for s in discover_sessions():
        e = _entry(d, s["slot"])
        cd = float(e.get("cooldown_until") or 0)
        rows.append({
            "slot": s["slot"],
            "mbps": round(float(e.get("mbps") or 0), 2),
            # 2026-11-16（E147）：`last_mbps` 必须一起给出来。
            # `pc_upload._adapt_speed_plan` 用它判断「这条通道近期真的快过吗」——
            # 没有它，试探轮永远只能靠 PC_FORCE_PROBE 手工开，等于死代码。
            "last_mbps": round(float(e.get("last_mbps") or 0), 2),
            "ok": int(e.get("ok") or 0),
            "fails": int(e.get("fails") or 0),
            "cooling": cd > now,
            "cool_left_min": round((cd - now) / 60.0, 1) if cd > now else 0,
            "last_err": e.get("last_err") or "",
        })
    return rows


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="清空池的统计")
    ap.add_argument("--set", nargs=2, metavar=("SLOT", "MBPS"), help="手工设某号吞吐")
    a = ap.parse_args()
    if a.reset:
        _save({})
        print("池已清空")
    elif a.set:
        report(int(a.set[0]), True, float(a.set[1]))
        print("已设置 slot%s = %s MB/s" % (a.set[0], a.set[1]))
    print("%-6s %-8s %-6s %-6s %-8s %s" % ("slot", "MB/s", "成功", "连败", "状态", "最近错误"))
    for r in snapshot():
        st = "冷却 %.0fmin" % r["cool_left_min"] if r["cooling"] else "可用"
        print("%-6s %-8.2f %-6d %-6d %-8s %s" % (
            r["slot"], r["mbps"], r["ok"], r["fails"], st, r["last_err"][:40]))
