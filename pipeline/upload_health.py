#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""upload_health.py —— 上传结果台账 + 断流闸门（E182）。

为什么要有它
------------
2026-11-19 03:42–05:41 实测：**连续 2 小时零落地**，而四条 lane 一直在领片。
每一条 claim 的路径都一样：从源站下完一整集（烧的是硬顶配额）→ 上传到 TG
时 `worker 超时（1800s）`、`ZERO_PROGRESS 0 个分片落地` → 释放 claim →
下一条 lane 立刻领下一集，重复。六个 claim、约 12GB 源站流量，产出 0。

`stall_watch` 那 94/115 分钟两条 P1 只写了「配额仍余 2590/3075、各源可用」——
它看得见「没有产出」，看不见「产出死在哪一段」。诊断要从头再查一遍 runner 日志。

判据为什么放在「结果」而不是「速度」
------------------------------------
速度是噪声：同一个账号 03:44 是 0.12MB/s、05:41 是 6MB/s。
拿速度预测「这一单传不传得完」的尝试（E139/E147/E167/E174）全都在和噪声搏斗。
**结果**是确定的：一个分片都没落地就是没落地。所以闸门只看
`零进展失败（rc=3/6）` 的连续次数，不预测速度、也不改并发/分片（那是用户的
「跑满速，撞到额度就休息」决定，不能顺手推翻）。

规则
----
1. 连续 3 次零进展失败、且来自 **≥2 条不同 lane**、且发生在 90 分钟内 → 开闸（hold）。
   （单条 lane 连败可能只是它自己的会话坏了，池子冷却会处理，不该拖停全局。）
2. 开闸后 10 分钟起、每次再失败翻倍（10/20/30 分钟封顶）。
   任一上传成功 → 立刻关闸，连续计数归零。
3. 闸门到期不是全放：**每 3 分钟只放一条 lane 去探一次**（flock 令牌），
   探针失败就继续按住 —— 否则四条 lane 同时领片，就是 03:44 那次的重演。
4. `fill` 没有豁免：它同样会从源站下载。闸门只挡「领新活」，
   已经在 `_keep` 里的字节由下一轮 fill 复用，不产生新配额开销。

用法
----
    python3 upload_health.py --record ok   --bytes 123 --claim RUN --lane new2
    python3 upload_health.py --record fail --rc 6  --bytes 123 --claim RUN --lane new2 --err "..."
    python3 upload_health.py --gate --slot 2      # rc=0 放行 / rc=75 按住（stdout 是 JSON）
    python3 upload_health.py --status             # 人看的摘要
    python3 upload_health.py --reset              # 人工解除（改完 TG 侧之后）

环境变量
--------
    TG_UPLOAD_BREAKER=0        关闭闸门（记账照旧）
    UPLOAD_HEALTH_LEDGER=...   台账路径（测试用）
    UPLOAD_HEALTH_STATE=...    闸门状态路径（测试用）
"""
import argparse
import fcntl
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LEDGER = os.environ.get("UPLOAD_HEALTH_LEDGER") or os.path.join(HERE, "logs", "upload_health.jsonl")
STATE = os.environ.get("UPLOAD_HEALTH_STATE") or os.path.join(HERE, "logs", ".upload_breaker.json")
LOCK = STATE + ".lock"

ZERO_RCS = (3, 6)          # 3=QuotaRest（按秒休息）/ 6=零进展（一个分片都没落地）
TRIP_FAILS = 3             # 连续多少次零进展失败开闸
TRIP_WINDOW_SEC = 90 * 60  # 这些失败必须发生在多长的窗口内
TRIP_MIN_LANES = 2         # 至少来自几条不同 lane
HOLD_BASE_SEC = 600        # 首次按住 10 分钟
HOLD_MAX_SEC = 1800
PROBE_GAP_SEC = 180        # 到期后每 3 分钟放一条 lane 探一次
RECENT_SEC = 6 * 3600      # 只保留/只看最近 6 小时的记录
MAX_LINES = 4000


# ------------------------------------------------------------------ 基础 IO

def _now(now=None):
    return float(now if now is not None else time.time())


def _enabled():
    return os.environ.get("TG_UPLOAD_BREAKER", "1") != "0"


def _ensure_dir(path):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _read_records(path=None, cutoff=None):
    path = path or LEDGER
    out = []
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(r, dict):
                    continue
                try:
                    r["ts"] = float(r.get("ts") or 0)
                except (TypeError, ValueError):
                    continue
                if cutoff is not None and r["ts"] < cutoff:
                    continue
                out.append(r)
    except FileNotFoundError:
        pass
    out.sort(key=lambda r: r["ts"])
    return out


def _load_state(path=None):
    path = path or STATE
    try:
        with open(path, encoding="utf-8") as fh:
            d = json.load(fh)
        return d if isinstance(d, dict) else {}
    except Exception:                                          # noqa: BLE001
        return {}


def _save_state(d, path=None):
    path = path or STATE
    _ensure_dir(path)
    tmp = "%s.%d.tmp" % (path, os.getpid())
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(d, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def _with_lock(fn, path=None):
    """state 的读-改-写整段加锁（多 lane 并发写，E093 的教训）。"""
    lk = (path or STATE) + ".lock"
    _ensure_dir(lk)
    with open(lk, "a") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            return fn()
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


# ------------------------------------------------------------------ 判据（纯函数，可单测）

def trip_decision(records, now=None):
    """根据台账判断此刻该不该开闸/按住多久。

    返回 dict：
        consecutive  最近一次成功之后的零进展失败数
        lanes        这些失败涉及的 lane 集合（排序后）
        first_ts     这批失败里最早的时间
        hold_sec     若开闸，按住多久（0=不开）
        reason       not_tripped / tripped / window_expired / single_lane
    """
    now = _now(now)
    recs = [r for r in records if r.get("ts", 0) >= now - RECENT_SEC]
    last_ok = 0.0
    for r in recs:
        if r.get("result") == "ok":
            last_ok = max(last_ok, r["ts"])
    fails = [r for r in recs if r.get("result") == "fail" and r["ts"] > last_ok]
    zero = [r for r in fails if int(r.get("rc") or 0) in ZERO_RCS]
    lanes = sorted({str(r.get("lane") or "?") for r in zero})
    out = {"consecutive": len(zero), "lanes": lanes,
           "first_ts": zero[0]["ts"] if zero else 0.0,
           "last_ts": zero[-1]["ts"] if zero else 0.0,
           "hold_sec": 0, "reason": "not_tripped"}
    if len(zero) < TRIP_FAILS:
        return out
    if zero[-1]["ts"] - zero[0]["ts"] > TRIP_WINDOW_SEC:
        out["reason"] = "window_expired"
        return out
    if len(lanes) < TRIP_MIN_LANES:
        out["reason"] = "single_lane"
        return out
    n = len(zero) - TRIP_FAILS
    out["hold_sec"] = min(HOLD_MAX_SEC, HOLD_BASE_SEC * (2 ** min(n, 4)))
    out["reason"] = "tripped"
    return out


def gate_decision(records, state, slot=0, now=None):
    """闸门时刻表。返回 {allow, reason, wait_sec, consecutive, ...}。

    probe 令牌放在 state 里（读-改-写已在调用方加锁），保证「到期后只有一条 lane
    能领活」，其余 lane 按 slot 错开等待。
    """
    now = _now(now)
    d = trip_decision(records, now)
    out = {"allow": True, "reason": "ok", "wait_sec": 0,
           "consecutive": d["consecutive"], "lanes": d["lanes"],
           "open_until": 0, "slot": slot}
    if d["reason"] != "tripped":
        if state.get("open_until"):
            state["open_until"] = 0
        out["reason"] = d["reason"]
        return out

    # 开闸时刻 = 最后一次零进展失败 + hold。状态里记 open_until，跨进程一致。
    open_until = float(state.get("open_until") or 0)
    want_until = d["last_ts"] + d["hold_sec"]
    if want_until > open_until:
        open_until = want_until
        state["open_until"] = open_until
    out["open_until"] = open_until

    if now < open_until:
        out.update(allow=False, reason="holding",
                   wait_sec=int(open_until - now) + 45 * max(0, int(slot) - 1))
        return out

    # 到期：每 PROBE_GAP_SEC 只放一条 lane 做探针
    last_probe = float(state.get("probe_ts") or 0)
    if now - last_probe < PROBE_GAP_SEC:
        out.update(allow=False, reason="probe_gap",
                   wait_sec=int(PROBE_GAP_SEC - (now - last_probe)) + 30 * max(0, int(slot) - 1))
        return out
    state["probe_ts"] = now
    out.update(allow=True, reason="probe")
    return out


# ------------------------------------------------------------------ 落盘入口

def record(result, rc=0, nbytes=0, claim="", lane="", err="", now=None, path=None):
    """记一条上传结果，并顺手维护闸门状态。返回写入的记录。"""
    now = _now(now)
    rec = {"ts": round(now, 3), "result": result, "rc": int(rc or 0),
           "bytes": int(nbytes or 0), "claim": str(claim or "")[:80],
           "lane": str(lane or "")[:24], "err": str(err or "")[:200]}
    path = path or LEDGER
    _ensure_dir(path)
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as e:                                       # noqa: BLE001
        print("upload_health: 台账写入失败: %s" % e, file=sys.stderr)
    _trim(path, now)

    def _update():
        st = _load_state()
        if result == "ok":
            st.update(open_until=0, probe_ts=0, consecutive=0)
            _save_state(st)
            return
        d = trip_decision(_read_records(path), now)
        st["consecutive"] = d["consecutive"]
        st["lanes"] = d["lanes"]
        st["last_reason"] = d["reason"]
        if d["reason"] == "tripped":
            st["open_until"] = max(float(st.get("open_until") or 0), d["last_ts"] + d["hold_sec"])
        _save_state(st)
    if path == LEDGER:
        _with_lock(_update)
    else:
        _update()
    return rec


def _trim(path, now):
    """台账只留最近 RECENT_SEC，防止无限长。"""
    try:
        if os.path.getsize(path) < 3_000_000:
            return
        recs = _read_records(path, cutoff=now - RECENT_SEC)
        recs = recs[-MAX_LINES:]
        tmp = "%s.%d.tmp" % (path, os.getpid())
        with open(tmp, "w", encoding="utf-8") as fh:
            for r in recs:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        os.replace(tmp, path)
    except OSError:
        pass


def gate(slot=0, now=None):
    """给 run_one.sh 用的判据。返回 (allow, info)。"""
    now = _now(now)
    if not _enabled():
        return True, {"allow": True, "reason": "disabled", "wait_sec": 0}
    recs = _read_records(cutoff=now - RECENT_SEC)

    def _decide():
        st = _load_state()
        info = gate_decision(recs, st, slot=slot, now=now)
        _save_state(st)
        return info
    info = _with_lock(_decide)
    return bool(info.get("allow")), info


def summary(now=None):
    """给 stall_watch / 巡检用的摘要。"""
    now = _now(now)
    recs = _read_records(cutoff=now - RECENT_SEC)
    d = trip_decision(recs, now)
    st = _load_state()
    last = None
    for r in reversed(recs):
        last = r
        break
    return {
        "consecutive": d["consecutive"],
        "lanes": d["lanes"],
        "reason": d["reason"],
        "open_until": float(st.get("open_until") or 0),
        "holding": float(st.get("open_until") or 0) > now,
        "last": last,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--record", choices=("ok", "fail"))
    ap.add_argument("--rc", type=int, default=0)
    ap.add_argument("--bytes", type=int, default=0, dest="nbytes")
    ap.add_argument("--claim", default="")
    ap.add_argument("--lane", default="")
    ap.add_argument("--err", default="")
    ap.add_argument("--gate", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--slot", type=int, default=0)
    a = ap.parse_args()

    if a.record:
        rec = record(a.record, a.rc, a.nbytes, a.claim, a.lane, a.err)
        print(json.dumps(rec, ensure_ascii=False))
        return 0
    if a.reset:
        _with_lock(lambda: _save_state({}))
        print("breaker reset")
        return 0
    if a.gate:
        allow, info = gate(a.slot)
        print(json.dumps(info, ensure_ascii=False))
        return 0 if allow else 75
    if a.status:
        s = summary()
        print(json.dumps(s, ensure_ascii=False, indent=1))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
