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
SEQUENCE = tuple(os.getenv("EMBYTG_LANE_SEQUENCE", "fill,new,fill,new,upgrade").split(","))


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
    preferred = SEQUENCE[int(data.get("rotation") or 0) % len(SEQUENCE)]
    if preferred not in ready:
        preferred = ready[0]
    return [preferred] + [lane for lane in ready if lane != preferred]


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
