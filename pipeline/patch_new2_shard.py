#!/usr/bin/env python3
"""Add a second new-lane shard (new2) mirroring upgrade2's stable partition."""
import py_compile
import shutil
import time

ROOT = "/opt/embytg/pipeline"
TS = time.strftime("%Y%m%d_%H%M%S")


def patch(path, pairs):
    shutil.copy2(path, path + ".bak.new2." + TS)
    src = open(path, encoding="utf-8").read()
    for old, new in pairs:
        if new in src and old not in src:
            continue
        if src.count(old) != 1:
            raise SystemExit("%s: expected 1 occurrence of %r, got %d"
                             % (path, old[:70], src.count(old)))
        src = src.replace(old, new)
    open(path, "w", encoding="utf-8").write(src)
    py_compile.compile(path, doraise=True)
    print("patched", path)


patch(ROOT + "/gha_next3.py", [
    ('LANES = ("new", "fill", "upgrade", "upgrade2")',
     'LANES = ("new", "new2", "fill", "upgrade", "upgrade2")'),
    ('    "new": os.path.join(HERE, ".gha_cursor_new"),',
     '    "new": os.path.join(HERE, ".gha_cursor_new"),\n'
     '    "new2": os.path.join(HERE, ".gha_cursor_new2"),'),
    ('    "new": os.path.join(HERE, ".gha_queue_new.json"),',
     '    "new": os.path.join(HERE, ".gha_queue_new.json"),\n'
     '    "new2": os.path.join(HERE, ".gha_queue_new.json"),'),
    ('def invalid_candidate(episode, size, min_bytes, max_bytes):',
     'def new_shard(item):\n'
     '    """Stable two-way partition of the new queue (new / new2)."""\n'
     '    raw = "%s|%s|%s|%s" % (\n'
     '        item.get("tmdb"), item.get("source_series_id"),\n'
     '        item.get("series") or "", item.get("year") or "")\n'
     '    return int(hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8], 16) % 2\n'
     '\n\n'
     'def invalid_candidate(episode, size, min_bytes, max_bytes):'),
    ('    if a.lane == "new" and not items:',
     '    if a.lane in ("new", "new2") and not items:'),
    ('    if a.lane == "fill":\n        items = A.dedupe_items(items)',
     '    if a.lane == "fill":\n        items = A.dedupe_items(items)\n'
     '    if a.lane in ("new", "new2"):\n'
     '        shard = 1 if a.lane == "new2" else 0\n'
     '        full_items = len(items)\n'
     '        items = [x for x in items if new_shard(x) == shard]\n'
     '        if not items:\n'
     '            emit({"ok": False, "reason": "no_job", "lane": a.lane,\n'
     '                  "detail": "new_shard_empty", "queue_remaining": full_items})\n'
     '            return 0'),
    ('        "queue_lane": "upgrade" if lane == "upgrade2" else lane,',
     '        "queue_lane": ("upgrade" if lane == "upgrade2" else\n'
     '                       ("new" if lane == "new2" else lane)),'),
])

patch(ROOT + "/lane_queue.py", [
    ('def _queue_lane(lane):\n'
     '    return "upgrade" if str(lane) == "upgrade2" else str(lane)',
     'def _queue_lane(lane):\n'
     '    lane = str(lane)\n'
     '    if lane == "upgrade2":\n'
     '        return "upgrade"\n'
     '    if lane == "new2":\n'
     '        return "new"\n'
     '    return lane'),
])

patch(ROOT + "/gha_finish.py", [
    ('    elif job.get("lane") == "new" and Q_RANK.get(actual, 0) < 2:',
     '    elif str(job.get("lane") or "").startswith("new") and '
     'Q_RANK.get(actual, 0) < 2:'),
])

print("done")
