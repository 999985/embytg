#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prune a successfully finished job from the one-shot fill/upgrade queue.

Only a real, fully verified finish calls this module.  A queue becomes empty
only after every fill episode / upgrade episode has actually succeeded, so a
temporary source failure can never be mistaken for "lane complete".
"""
import contextlib
import fcntl
import json
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
QUEUE_FILES = {
    "fill": os.path.join(HERE, ".gha_queue_fill.json"),
    "upgrade": os.path.join(HERE, ".gha_queue_upgrade.json"),
}
DONE_FILES = {
    lane: os.path.join(HERE, ".gha_lane_done_%s" % lane)
    for lane in QUEUE_FILES
}
LOCK = os.path.join(HERE, ".gha_lane_queue.lock")


def _queue_lane(lane):
    lane = str(lane)
    if lane == "upgrade2":
        return "upgrade"
    if lane in ("new2", "new3", "new4", "new5"):
        return "new"
    return lane


@contextlib.contextmanager
def _lock():
    fd = os.open(LOCK, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def is_done(lane):
    lane = _queue_lane(lane)
    return lane in DONE_FILES and os.path.exists(DONE_FILES[lane])


def mark_done(lane, reason, run_id=""):
    lane = _queue_lane(lane)
    if lane not in DONE_FILES:
        return False
    path = DONE_FILES[lane]
    if os.path.exists(path):
        return True
    tmp = path + ".tmp.%d" % os.getpid()
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"lane": lane, "reason": reason, "run_id": run_id,
                   "ts": int(time.time())}, fh, ensure_ascii=False)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
    return True


def _write_queue(path, data):
    tmp = path + ".tmp.%d" % os.getpid()
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def prune_job(job):
    lane = _queue_lane(job.get("queue_lane") or job.get("lane") or "")
    path = QUEUE_FILES.get(lane)
    if not path or not os.path.exists(path):
        return None
    tmdb = str(job.get("tmdb") or "")
    season = int(job.get("season") or 0)
    ep = int(job.get("ep") or 0)
    if not tmdb or not season or not ep:
        return None

    with _lock():
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        items = data.get("items") or []
        before = len(items)
        kept = []
        changed = False
        for item in items:
            if str(item.get("tmdb") or "") != tmdb:
                kept.append(item)
                continue
            if int(item.get("season") or 0) != season:
                kept.append(item)
                continue
            if lane == "upgrade":
                if int(item.get("ep") or 0) == ep:
                    changed = True
                    continue
                kept.append(item)
                continue
            missing = [int(x) for x in (item.get("missing") or [])]
            pruned = [x for x in missing if x != ep]
            if pruned != missing:
                changed = True
            missing = pruned
            if missing:
                kept.append({**item, "missing": missing})
        remaining = len(kept)
        if changed or remaining != before:
            data["items"] = kept
            data["pruned_at"] = time.strftime("%F %T")
            _write_queue(path, data)
        if remaining == 0:
            mark_done(lane, "queue_empty_after_finish",
                      str(job.get("run_id") or ""))
        return {"lane": lane, "before": before, "remaining": remaining}
