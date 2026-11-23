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


def _pressure():
    """每条 lane 有多「欠」活。缺文件就退回 1，保证永远有 lane 可选。

    2026-11-21：用户指出「按固定顺序轮转」等于把 worker 钉死在 lane 上——
    某条 lane 没活就空转，某条堆几千集也只有一个 worker 啃。
    改成按欠活量排序：谁欠得多谁先干。
    """
    pipe = os.getenv("EMBYTG_PIPELINE", "/opt/embytg/pipeline")
    p = {}
    # fill 的真实来源：.nfo_gap.json（缺口台账）；.gap_report.json 是另一脚本产物，可能不存在
    p["fill"] = 0
    for cand, getter in (((".gap_report.json"), lambda g: g.get("missing_total") or g.get("missing")),
                         ((".nfo_gap.json"), lambda g: len(g.get("episodes") or g.get("items") or g) if isinstance(g, (dict, list)) else 0)):
        try:
            g = json.load(open(os.path.join(pipe, cand), encoding="utf-8"))
            n = getter(g) if isinstance(g, dict) else len(g)
            if isinstance(n, list):
                n = len(n)
            p["fill"] = max(p["fill"], int(n or 0))
        except Exception:                                  # noqa: BLE001
            continue
    try:
        lo = json.load(open(os.path.join(pipe, ".lowq_library.json"), encoding="utf-8"))
        items = lo if isinstance(lo, list) else (lo.get("items") or lo.get("episodes") or [])
        p["upgrade"] = int(len(items) * 0.5)      # 每集要重传，成本高，权重折半
    except Exception:                                      # noqa: BLE001
        p["upgrade"] = 0
    # 2026-11-22：new 的真实库存 = .gha_queue_new.json（源站目录比对后的新剧）。
    # 原来读 tg_state.json 的 queue，那是旧的手工清单，永远几百条，
    # 于是 new 的压力算出来只有个位数，调度几乎不排 new。
    # 实测 catalog_sync 一跑，源站有 7556 部不在库的剧 → 队列 9922 条。
    p["new"] = 1
    for fname, scale in ((".gha_queue_new.json", 0.3),):
        try:
            d = json.load(open(os.path.join(pipe, fname), encoding="utf-8"))
            items = d.get("items") if isinstance(d, dict) else d
            n = len(items or [])
            if n:
                p["new"] = max(1, int(n * scale))
        except Exception:                                  # noqa: BLE001
            continue
    try:
        st = json.load(open(os.path.join(pipe, "tg_state.json"), encoding="utf-8"))
        q = st.get("queue") or st.get("pending") or []
        if isinstance(q, list) and q:
            p["new"] = max(p["new"], int(len(q) * 0.3))
    except Exception:                                      # noqa: BLE001
        pass
    return {k: max(0, v) for k, v in p.items()}


def _order(data):
    active = [lane for lane in LANES if not data["lanes"][lane]["done"]]
    if not active:
        return ["new"]
    now = int(time.time())
    ready = [lane for lane in active
             if int(data["lanes"][lane].get("backoff_until") or 0) <= now]
    # A lane that just returned no_job is not useful to scan again immediately.
    # Keep all lanes visible, but put cooling lanes last; when every lane is
    # cooling, try the one whose cooldown expires first.
    if not ready:
        return []
    # 按欠活量排序（原来按 SEQUENCE 轮转，绑死了 worker）
    pres = _pressure()
    ready_sorted = sorted(ready, key=lambda l: pres.get(l, 0), reverse=True)
    return ready_sorted


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
        data["rotation"] = (int(data.get("rotation") or 0) + 1) % len(SEQUENCE)
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
