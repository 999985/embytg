#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""触发 Emby 自带「Scan media library」任务（可靠的全库重扫）。

⚠️ 这是**全库扫描**。按 E008 它的用途只剩「初次导入 / 灾后重建 / 幽灵条目清理」。
日常入库必须走 `POST /Library/Media/Updated` 精确路径（`tgpipe.emby_media_updated()`），
不要拿这个脚本当入库手段。

2026-11-13 加的两道闸（对齐 aetrix-emby-portal 的线上事故结论）：
> 四个媒体库在同一分钟点「扫描」，四个任务合法地同时跑起来 —— 单个库的互斥只防
> 「同一个库重复扫描」，不防「不同的库同时打同一个远程挂载」。同一个端点被反复列举，
> 远端延迟被放大、SQLite 被四个写入方争抢，CPU 打满而条目数长时间是 0。
> **最小根因修复是「按远程挂载串行化」，不是提高线程数。**

本机 17 个库共用**同一个** rclone 挂载，所以：

闸 1｜文件锁：同一台机器上同一时刻只允许一个全库扫描发起者。第二个调用者会等待
      （默认 3600s）而不是也去点开始；超时则明确报错退出，不静默叠加。
闸 2｜已在跑就不重复点：Emby 自己的 ScheduledTask 状态是 Running 时直接返回，
      并打印当前进度，让调用方知道「在跑」而不是「失败了」。

用法：
  python3 _scan_now.py            # 发起（或等待）一次全库扫描
  python3 _scan_now.py --status   # 只看状态，不发起
  python3 _scan_now.py --timeout 600
"""
import argparse
import fcntl
import json
import os
import sys
import time
import urllib.request

import embytg_conf as C

E = C.cfg("emby_url", "http://127.0.0.1:8096").rstrip("/")
T = C.cfg("emby_api_key") or C.cfg("emby_token") or ""
HERE = os.path.dirname(os.path.abspath(__file__))
LOCK = os.path.join(HERE, ".emby_scan.lock")


def get(u):
    return json.load(urllib.request.urlopen(
        urllib.request.Request(u, headers={"X-Emby-Token": T}), timeout=60))


def post(u):
    r = urllib.request.Request(u, data=b"", method="POST",
                               headers={"X-Emby-Token": T})
    with urllib.request.urlopen(r, timeout=60) as x:
        return x.status


def find_task():
    for t in get(E + "/ScheduledTasks"):
        if t.get("Key") == "RefreshLibrary" or "Scan media library" in (t.get("Name") or ""):
            return t
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--timeout", type=int, default=3600,
                    help="等锁的最长时间（秒）；超时即退出，不叠加第二个扫描")
    a = ap.parse_args()

    for _ in range(20):
        try:
            get(E + "/System/Info")
            break
        except Exception:                                   # noqa: BLE001
            time.sleep(5)

    if a.status:
        t = find_task()
        if not t:
            print("没找到 Scan media library 任务")
            return 1
        print(json.dumps({"id": t.get("Id"), "name": t.get("Name"),
                          "state": t.get("State"),
                          "progress": t.get("CurrentProgressPercentage")},
                         ensure_ascii=False))
        return 0

    lock_fd = os.open(LOCK, os.O_CREAT | os.O_RDWR, 0o600)
    deadline = time.time() + a.timeout
    waited = False
    while True:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if not waited:
                print("  另一个全库扫描发起者持有锁，等待（最多 %ds）…" % a.timeout)
                waited = True
            if time.time() > deadline:
                print("  ✗ 等锁超时：已有扫描在跑，本进程退出（不叠加第二个）")
                os.close(lock_fd)
                return 3
            time.sleep(5)

    try:
        t = find_task()
        if not t:
            print("没找到 Scan media library 任务")
            return 1
        print("task %s %s state=%s" % (t.get("Id"), t.get("Name"), t.get("State")))
        if t.get("State") == "Running":
            print("已在运行（%.1f%%）—— 不重复发起"
                  % (t.get("CurrentProgressPercentage") or 0))
            return 0
        print("start ->", post(E + "/ScheduledTasks/Running/%s" % t["Id"]))
        for _ in range(12):
            time.sleep(5)
            t2 = find_task() or {}
            if t2.get("State") == "Running":
                print("  已开始，进度 %.1f%%" % (t2.get("CurrentProgressPercentage") or 0))
                return 0
        print("  ⚠ 发起后 60s 内未观察到 Running，检查 Emby 日志")
        return 2
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


if __name__ == "__main__":
    sys.exit(main())
