#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Persistent lane rotation for the Korea runner.

Policy:
  * new is permanent and is never marked done.
  * fill/upgrade are one-shot backlogs; they stop permanently once their
    queue has actually been emptied.
  * one job at a time, with new preferred for half of the successful turns.
"""
import argparse
import json
import os
import sys
import time

BASE = os.getenv("EMBYTG_RUNNER_BASE", "/opt/embytg-runner")
STATE = os.getenv("EMBYTG_LANE_STATE", os.path.join(BASE, "lane_state.json"))
LANES = ("new", "fill", "upgrade")
# 2026-11-19：fill 从 1/4 提到 2/5。
# 依据：近 6h fill 18 次成功 0 失败（源站确实有这些缺集），
# 而队列里躺着 404 季 / 7387 集已知缺口；new 是「发现新剧」，fill 是「补完已有剧」，
# 目标要最全，补完已有剧的确定性远高于发现新剧。
# 2026-11-20：PC 一直在等活。根因不是 PC 没配，而是 claim 是「每 lane 一个文件」
# （.gha_claims/<lane>.json）—— 本机 lane 先抢到，PC 就只能干等。
# 改成：本机主力做 fill/upgrade，new 排最后，把 new 的 claim 窗口尽量让给 PC；
# PC 不在线时本机仍会轮到 new（1/4 班次），P0 不会彻底停。
SEQUENCE = tuple(os.getenv("EMBYTG_LANE_SEQUENCE", "fill,upgrade,fill,new").split(","))

# 2026-11-25（E2xx）：**份额化轮转**。
#
# 事故形状：11-23 把 _order 改成「按欠活量排序」后，new 的库存是 3000 部、
# fill 读的是一个错误台账（.nfo_gap.json 是「占位集名」台账，最后一版 11-19；
# .gap_report.json 根本不存在），于是 new 的 pressure 永远最大 —— 四条 lane
# 全部把 new 排第一，谁先领到谁干 new，fill/upgrade 24 小时 0 条产出
# （lane_state: fill 45h、upgrade 61h 没有 last_success），3192 集补集队列空转。
#
# 排序只能决定「同一次尝试里先试谁」，决定不了份额：只要有活，第一条永远成功，
# 后面的永远轮不到。份额必须显式写在调度里。
# 口径（用户 11-23：重点是 new 和 fill，upgrade 降权）：new 55% / fill 40% / upgrade 5%。
TARGET_WHEEL = ("new",) * 11 + ("fill",) * 8 + ("upgrade",) * 1

PIPELINE = os.getenv("EMBYTG_PIPELINE", "/opt/embytg/pipeline")
QUEUE_FILE = {
    "new": ".gha_queue_new.json",
    "fill": ".gha_queue_fill.json",
    "upgrade": ".gha_queue_upgrade.json",
}


def _default():
    return {
        "version": 1,
        "rotation": 0,
        "updated_at": int(time.time()),
        "lanes": {
            lane: {
                "done": False,
                "last_result": "",
                "last_attempt": 0,
                "last_success": 0,
                "successes": 0,
                "backoff_until": 0,
            }
            for lane in LANES
        },
    }


def _load():
    try:
        with open(STATE, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:                                      # noqa: BLE001
        return _default()
    if not isinstance(data, dict):
        return _default()
    out = _default()
    out["rotation"] = int(data.get("rotation") or 0)
    for lane in LANES:
        row = (data.get("lanes") or {}).get(lane) or {}
        if lane != "new":
            out["lanes"][lane]["done"] = bool(row.get("done"))
        for key in ("last_result", "last_attempt", "last_success", "successes",
                    "backoff_until"):
            if key in row:
                out["lanes"][lane][key] = row[key]
    return out


def _save(data):
    data["version"] = 1
    data["updated_at"] = int(time.time())
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    tmp = STATE + ".tmp.%d" % os.getpid()
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, STATE)


def _queue_len(lane):
    """真实库存（条）：-1 = 文件读不到（未知，不当成 0）。"""
    path = os.path.join(PIPELINE, QUEUE_FILE.get(lane, ""))
    try:
        with open(path, encoding="utf-8") as fh:
            d = json.load(fh)
        items = d.get("items") if isinstance(d, dict) else d
        return len(items or [])
    except Exception:                                      # noqa: BLE001
        return -1


def _pressure():
    """每条 lane 有多「欠」活 —— 唯一来源是 build_three_queues 产出的三个队列。

    2026-11-25：原来 fill 读 .gap_report.json / .nfo_gap.json、upgrade 读
    .lowq_library.json —— 三个都是别的脚本的副产品，两个 11-19 就停更、
    一个从来没生成过。调度依据的台账必须就是领活用的台账，
    否则「欠活量」是幻觉：new 永远第一，fill/upgrade 排到天荒地老。

    权重：upgrade 要重下一整集（单位流量产出最低），压力折半；
    new 每条相当于 0.3 集落库（一部剧会长出很多集），按 0.3 折算。
    """
    p = {"new": 1, "fill": 0, "upgrade": 0}
    n = _queue_len("new")
    if n > 0:
        p["new"] = max(1, int(n * 0.3))
    f = _queue_len("fill")
    if f > 0:
        p["fill"] = f
    u = _queue_len("upgrade")
    if u > 0:
        p["upgrade"] = int(u * 0.5)
    return {k: max(0, v) for k, v in p.items()}


def _order(data):
    """返回本次尝试的 lane 顺序：**第一条是份额指定的主力 lane**。

    排序决定不了份额（第一条永远成功、后面永远轮不到），所以主力 lane 由
    rotation 计数器在 TARGET_WHEEL 上走；其余 lane 按欠活量兜底，
    保证主力 lane 冷却/空队列时不会空转。
    """
    active = [lane for lane in LANES if not data["lanes"][lane]["done"]]
    if not active:
        return ["new"]
    now = int(time.time())
    ready = [lane for lane in active
             if int(data["lanes"][lane].get("backoff_until") or 0) <= now]
    if not ready:
        return []
    pres = _pressure()
    _W = {"new": 1.0, "fill": 1.0, "upgrade": 0.05}
    wheel = TARGET_WHEEL
    primary = wheel[int(data.get("rotation") or 0) % len(wheel)]
    # 主力 lane 必须真的能领：未完成、未冷却、队列非空（-1=读不到，仍算可用）
    if primary not in ready or _queue_len(primary) == 0:
        primary = None
    rest = sorted([lane for lane in ready if lane != primary],
                  key=lambda l: pres.get(l, 0) * _W.get(l, 1.0),
                  reverse=True)
    return ([primary] if primary else []) + rest


def _select_claim(path):
    try:
        with open(path, encoding="utf-8") as fh:
            rows = json.load(fh)
    except Exception:                                      # noqa: BLE001
        rows = []
    now = time.time()
    failed = []
    running = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        lane = str(row.get("lane") or "")
        run_id = str(row.get("run_id") or "")
        if not lane or not run_id:
            continue
        status = row.get("status")
        ts = float(row.get("ts") or 0)
        if status == "failed":
            failed.append((ts, lane, run_id))
        elif status == "running" and now - ts < 24 * 3600:
            running.append((ts, lane, run_id))
    rows = failed or running
    if not rows:
        print("none\t-\t-")
        return
    ts, lane, run_id = max(rows)
    print("%s\t%s\t%s" % ("failed" if failed else "running", lane, run_id))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("next")
    sub.add_parser("advance")
    sub.add_parser("status")
    mark = sub.add_parser("mark-done")
    mark.add_argument("--lane", required=True, choices=("fill", "upgrade"))
    mark.add_argument("--reason", default="empty_queue")
    note = sub.add_parser("note")
    note.add_argument("--lane", required=True, choices=LANES)
    note.add_argument("--result", required=True)
    claim = sub.add_parser("select-claim")
    claim.add_argument("--file", required=True)
    a = ap.parse_args()

    if a.cmd == "select-claim":
        _select_claim(a.file)
        return 0

    data = _load()
    if a.cmd == "next":
        order = _order(data)
        if not order:
            active = [lane for lane in LANES if not data["lanes"][lane]["done"]]
            soonest = min(int(data["lanes"][lane].get("backoff_until") or 0) for lane in active) if active else 0
            delay = max(5, min(120, soonest - int(time.time())))
            time.sleep(delay)
            order = _order(_load())
        print(" ".join(order))
        return 0
    if a.cmd == "advance":
        # 2026-11-25：按 TARGET_WHEEL 长度取模，不是按 SEQUENCE（4）——
        # 按 4 取模时 rotation 永远落在 0..3，而 new 占 wheel 前 11 格，
        # 于是主力 lane 恒为 new，份额化等于没做（实测 20 次全 new）。
        data["rotation"] = (int(data.get("rotation") or 0) + 1) % len(TARGET_WHEEL)
        _save(data)
        return 0
    if a.cmd == "status":
        print(json.dumps(data, ensure_ascii=False, indent=1))
        return 0
    if a.cmd == "mark-done":
        data["lanes"][a.lane]["done"] = True
        data["lanes"][a.lane]["last_result"] = a.reason
        data["lanes"][a.lane]["last_attempt"] = int(time.time())
        _save(data)
        print("done\t%s\t%s" % (a.lane, a.reason))
        return 0
    if a.cmd == "note":
        row = data["lanes"][a.lane]
        row["last_result"] = a.result
        row["last_attempt"] = int(time.time())
        if a.result in ("no_job", "empty_queue", "no_queue"):
            cooldown = {"new": 600, "fill": 900, "upgrade": 600}.get(a.lane, 600)
            row["backoff_until"] = int(time.time()) + cooldown
        elif a.result == "done":
            row["last_success"] = int(time.time())
            row["successes"] = int(row.get("successes") or 0) + 1
            row["backoff_until"] = 0
        else:
            row["backoff_until"] = 0
        _save(data)
        return 0
    raise SystemExit(2)


if __name__ == "__main__":
    sys.exit(main())
