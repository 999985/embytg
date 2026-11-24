#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""notify_replay.py —— 重放 tgpipe 落下的 notify_pending.jsonl（入库通知失败队列）。

## 为什么需要它（2026-11-24 日终发现）

`tgpipe.emby_media_updated()` 送扫失败时只把路径 append 进 `notify_pending.jsonl`，
**没有任何消费者**：一次瞬时失败（插件超时 / 父目录还没被 Emby 看见 / ScanPaths 抛错）
= 该片永远不进 Emby，而且五账只数 Episode，电影漏建**完全不可见**。
实测 2026-11-24 15:19–23:21 积了 15 条，其中 4 部电影在 Emby 查不到（重建后才补上）。

## 判据（不信任接口回执，只认 Emby 里真的有这条 Path）

对每条 pending：
  1. 磁盘上文件还在 → 送 `emby_scan.scan_paths`
  2. 用 `/Items?Path=` 回查，`TotalRecordCount > 0` 才算解决
  3. 解决 → 从队列删除；未解决 → attempts+1 保留（下次再试，指数退避不必要，15min 一轮足够）
  4. `attempts >= --alert-at`（默认 6 ≈ 1.5h）→ alert P1 `notify_stuck`，之后每 4 轮再报一次

## 用法

```
python3 notify_replay.py            # 重放一轮（cron 每 15min）
python3 notify_replay.py --dry      # 只看队列
python3 notify_replay.py --max 200  # 单轮上限
```
退出码：0 = 队列已清空或本轮全解决；1 = 仍有未解决条目（供 daily_ops / 巡检判红）。
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import embytg_conf as C                                          # noqa: E402

QUEUE = os.path.join(HERE, "notify_pending.jsonl")
LOCK = QUEUE + ".lock"
E = (C.cfg("emby_url", "http://127.0.0.1:8096") or "").rstrip("/")
T = C.cfg("emby_api_key") or C.cfg("emby_token") or ""
DISK_PREFIX = "/mnt/embytg"
HOST_PREFIX = "/opt/embytg/mnt/embytg"


def _log(msg):
    print("[%s] %s" % (time.strftime("%F %T"), msg), flush=True)


def host_path(p):
    """容器内 /mnt/embytg/... → 宿主机 /opt/embytg/mnt/embytg/..."""
    if p.startswith(DISK_PREFIX + "/"):
        return HOST_PREFIX + p[len(DISK_PREFIX):]
    return p


def in_emby(path):
    url = ("%s/Items?Recursive=true&IncludeItemTypes=Episode,Movie&Limit=1&Path=%s&api_key=%s"
           % (E, urllib.parse.quote(path), T))
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return int(json.load(r).get("TotalRecordCount", 0)) > 0
    except Exception as e:                                       # noqa: BLE001
        _log("! 回查失败 %s: %s" % (path[:60], type(e).__name__))
        return False


def load_queue():
    entries = {}
    if not os.path.exists(QUEUE):
        return entries
    for line in open(QUEUE, encoding="utf-8", errors="ignore"):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:                                        # noqa: BLE001
            continue
        p = rec.get("path")
        if not p:
            continue
        cur = entries.get(p)
        if cur is None:
            entries[p] = {"path": p, "ts": rec.get("ts", int(time.time())),
                          "attempts": int(rec.get("attempts", 0)),
                          "last": rec.get("last", rec.get("ts", 0))}
        else:
            cur["attempts"] = max(cur["attempts"], int(rec.get("attempts", 0)))
            cur["ts"] = min(cur["ts"], rec.get("ts", cur["ts"]))
            cur["last"] = max(cur["last"], rec.get("last", 0))
    return entries


def save_queue(entries):
    tmp = QUEUE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for rec in sorted(entries.values(), key=lambda r: r["ts"]):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    os.replace(tmp, QUEUE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--max", type=int, default=200)
    ap.add_argument("--alert-at", type=int, default=6)
    a = ap.parse_args()

    if not os.path.ismount(HOST_PREFIX):
        _log("mount 不可用，跳过本轮（不消费队列）")
        return 0

    entries = load_queue()
    if not entries:
        _log("队列空")
        return 0

    todo = list(entries.values())[:a.max]
    _log("队列 %d 条，本轮处理 %d" % (len(entries), len(todo)))
    if a.dry:
        for r in todo:
            print("  attempts=%d %s" % (r["attempts"], r["path"]))
        return 0

    # 1) 文件还在的才送扫（删掉的直接出队并留痕）
    gone, alive = [], []
    for r in todo:
        if os.path.exists(host_path(r["path"])):
            alive.append(r)
        else:
            gone.append(r)
    if gone:
        _log("磁盘已无文件，出队 %d 条（不送扫）" % len(gone))
        for r in gone:
            entries.pop(r["path"], None)

    if alive:
        import emby_scan
        paths = [r["path"] for r in alive]
        try:
            res = emby_scan.scan_paths(paths)
            _log("送扫 %d 条 → Created=%s Refreshed=%s PathNotFound=%s ParentNotFound=%s Failed=%s"
                 % (len(paths), res.get("Created"), res.get("Refreshed"),
                    res.get("PathNotFound"), res.get("ParentNotFound"), res.get("Failed")))
        except Exception as e:                                   # noqa: BLE001
            _log("! scan_paths 异常 %s: %s" % (type(e).__name__, str(e)[:120]))

        # 2) 回查到 Emby 才算解决
        ok = miss = 0
        for r in alive:
            if in_emby(r["path"]):
                entries.pop(r["path"], None)
                ok += 1
            else:
                r["attempts"] += 1
                r["last"] = int(time.time())
                miss += 1
        _log("复核：解决 %d / 未解决 %d" % (ok, miss))

    save_queue(entries)

    stuck = [r for r in entries.values() if r["attempts"] >= a.alert_at]
    if stuck:
        try:
            from alert import alert
            alert("P1", "notify_stuck",
                  "入库通知重放仍有 %d 条未进 Emby（>=%d 次尝试，约 %.1fh）"
                  % (len(stuck), a.alert_at, (time.time() - min(r["ts"] for r in stuck)) / 3600),
                  {"sample": [r["path"] for r in stuck[:5]]})
        except Exception as e:                                   # noqa: BLE001
            _log("! alert 失败: %s" % e)
        return 1
    return 1 if entries else 0


if __name__ == "__main__":
    sys.exit(main())
