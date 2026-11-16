#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""prewarm_daemon.py — 「ffprobe 多线程」在本项目的安全落地形态（运维任务 2026-11-10）

【用户指令】「ffprobe 我看网上评论说可以启动多线程，我们有启动多线程吗？如果有请以后都打开」

【为什么不是去改 Emby】（AGENTS §6：别人方案必须结合本机后端判断）
- 网上「多线程」有三种含义：
  ① ffprobe 内部 -threads 0（codec 解码线程）——本机每条 argv 都已经带，本来就是开的，且探测瓶颈是 I/O 不是 CPU。
  ② Emby 库扫描并行线程 —— 官方 Luke「there's no setting for it」(topic/26846)、
     功能请求帖 topic/113927 TL;DR「Library parallel scanning is not the same as FFMPEG multithreading」；
     本机 4.10.2.0 实测 /System/Configuration / ScheduledTask / openapi.json 全部无相关字段（0 次）。
  ③ 外部并发预热 —— **这个我们能开，就是本脚本**。
- Emby 官方没有并行扫描开关；Jellyfin 才有。硬改 Emby 只能上 StrmAssistant PRO（付费/闭源，
  E010 已一票否决：4.10.2.0 需 PRO、arm64 已知崩溃、且并发读会打爆 teldrive→TG 后端）。
- E001 血泪：full vfs-cache 下并发 ffprobe 打爆 teldrive → FUSE 挂起 → Emby 硬编码 60s 超时
  → 失败不落库 → 下轮重扫死循环。2026-11-10 12:28 实测仍有单个 ffprobe.real 卡
  D(folio_wait_bit_common) 9+ 分钟、kill -9 都收不掉。**并发度必须受控**。

【本脚本做什么】
把「多线程」放在 Emby 之外：并发跑 ffprobe 填 E011 shim 缓存，让 Emby 后续扫描
变成缓存命中（87ms/条）而不是真探（2-3s/条）。这是唯一在本环境被实测验证过的提速路径
（prewarm_ffprobe.sh 实测 38 集 refresh 130s→30s，4 倍）。

【五道闸门（缺一不跑，防止重演 E001）】
  G1 Scan media library 非 Running（Emby 在扫时不抢 FUSE）
  G2 无 apply / --force-pack / 其它 prewarm 在跑
  G3 只处理缓存未命中的冷文件，且本次最多 MAX_FILES 条
  G4 并发固定 CONCURRENCY（默认 4，非 8：TG 后端高延迟，E010 建议 2-4）
  G5 单文件硬超时 TIMEOUT 秒 + 记录超时文件到 skip 清单（下次跳过，不反复卡）

用法:
  /usr/bin/python3 prewarm_daemon.py --once          # 跑一轮（可挂 cron）
  /usr/bin/python3 prewarm_daemon.py --scan          # 只报告冷文件数，不探测
  /usr/bin/python3 prewarm_daemon.py --once --max 40 --concurrency 4
不改 Emby / 不写库 / 不重启 / 不触发扫描 / 不联网拉媒体字节（ffprobe 走 rclone writes 冷路径）。
"""
import argparse
import glob
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = "/opt/embytg"
MNT = os.path.join(ROOT, "mnt/embytg")
CACHE = "/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/cache"
EMBY_LOG = "/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt"
STATE = "/opt/embytg/pipeline/.prewarm_state.json"
REAL = "/bin/ffprobe.real"

# 与 Emby MediaProbeManager 逐字一致（key 才能命中，prewarm_ffprobe.sh 已实证）
ARGV = ["-threads", "0", "-v", "info", "-print_format", "json",
        "-show_streams", "-show_chapters", "-show_format", "-show_data"]
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".ts", ".mov")


def log(s):
    print("[%s] %s" % (time.strftime("%F %T"), s), flush=True)


def cfg():
    return json.load(open(os.path.join(ROOT, "config.json"), encoding="utf-8"))


def emby_scan_state(tok):
    try:
        r = urllib.request.Request("http://127.0.0.1:8096/ScheduledTasks",
                                   headers={"X-Emby-Token": tok})
        with urllib.request.urlopen(r, timeout=10) as f:
            tasks = json.loads(f.read())
        t = next((x for x in tasks if x.get("Name") == "Scan media library"), {})
        return t.get("State", "?")
    except Exception:
        return "?"


def busy_agents():
    """只认真正的 python/docker 进程，排除 shell -c / timeout 包装 / 自身进程树。

    踩坑 12:38：`timeout 600 /usr/bin/python3 prewarm_daemon.py --once` 的父 shell
    cmdline 里含 `prewarm_daemon.py --once` 字面量，被旧版算成「另一个 prewarm」→ G2 自锁。
    """
    keys = ("tgpipe.py apply", "auto_finalize.py --force-pack", "pg_dump",
            "prewarm_ffprobe.sh")
    n = 0
    mypid = os.getpid()
    for p in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            pid = int(p.split("/")[2])
            a = [x.decode("utf8", "ignore") for x in open(p, "rb").read().split(b"\0") if x]
        except Exception:
            continue
        if not a or pid == mypid:
            continue
        # 只接受 argv[0] 是 python/docker 的实体进程；shell -c / timeout / grep 一律不算
        exe = os.path.basename(a[0])
        if not (exe.startswith("python") or exe == "docker" or exe.startswith("pg_dump")):
            continue
        s = " ".join(a)
        if any(k in s for k in keys):
            n += 1
    return n


def cache_key(container_path, size):
    # 与 p0_3_ffprobe_shim.sh 完全一致：sha256(path | size | 归一化 argv)
    argv = "|" + "|".join(ARGV)
    return hashlib.sha256(("%s|%s|%s" % (container_path, size, argv)).encode()).hexdigest()


def cold_files(limit):
    skips = set()
    if os.path.isfile(STATE):
        try:
            skips = set(json.load(open(STATE, encoding="utf-8")).get("skip", []))
        except Exception:
            pass
    out = []
    for dirpath, _dirs, files in os.walk(MNT):
        for fn in files:
            if not fn.lower().endswith(VIDEO_EXT):
                continue
            host = os.path.join(dirpath, fn)
            rel = os.path.relpath(host, MNT)
            cpath = "/mnt/embytg/" + rel.replace(os.sep, "/")
            try:
                size = os.stat(host).st_size
            except OSError:
                continue
            key = cache_key(cpath, size)
            if os.path.isfile(os.path.join(CACHE, key + ".json")):
                continue
            if rel in skips:
                continue
            out.append((host, cpath))
            if len(out) >= limit:
                return out
    return out


def probe(host, cpath, timeout):
    # 必须调 /bin/ffprobe（= shim 包装层）才会写缓存；调 REAL 真身虽 rc=0 但不落缓存。
    # 12:39 实测踩过：20/20 探测成功但缓存 +0，全被误记进 skip。
    cmd = ["docker", "exec", "emby", "/bin/ffprobe", "-i", "file:" + cpath] + ARGV
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0 and '"streams"' in (r.stdout or "") and '"format"' in (r.stdout or "")
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--max", type=int, default=40)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=45)
    a = ap.parse_args()
    from concurrent.futures import ThreadPoolExecutor, as_completed

    c = cfg()
    tok = c["emby_token"]

    if a.scan:
        files = cold_files(10 ** 9)
        log("SCAN: 冷文件(缓存未命中)=%d，缓存现有=%d" %
            (len(files), len(glob.glob(os.path.join(CACHE, "*.json")))))
        for h, _ in files[:10]:
            log("  " + os.path.relpath(h, MNT))
        return

    # G1/G2
    st = emby_scan_state(tok)
    if st == "Running":
        log("G1 FAIL: Scan media library Running，本轮跳过（不抢 FUSE）")
        return
    busy = busy_agents()
    if busy:
        log("G2 FAIL: 有 %d 个 apply/force-pack/prewarm 在跑，本轮跳过" % busy)
        return
    log("G1/G2 PASS: scan=%s busy=%d" % (st, busy))

    files = cold_files(a.max)      # G3
    if not files:
        log("G3: 无冷文件，全部已缓存。无需预热。")
        return
    log("G3 PASS: 冷文件 %d 条（本轮上限 %d）；G4 并发=%d；G5 单文件超时=%ds"
        % (len(files), a.max, a.concurrency, a.timeout))

    before = len(glob.glob(os.path.join(CACHE, "*.json")))
    ok = timeout_list = 0
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs = {ex.submit(probe, h, cp, a.timeout): (h, cp) for h, cp in files}
        for f in as_completed(futs):
            h, cp = futs[f]
            try:
                good = f.result()
            except Exception:
                good = False
            if good:
                ok += 1
            else:
                timeout_list += 1
    after = len(glob.glob(os.path.join(CACHE, "*.json")))

    # G5：超时/失败的文件记入 skip，避免反复卡（本机实测存在 D 状态卡死文件）
    state = {"skip": [], "last_run": time.strftime("%F %T"), "ok": ok,
             "timeout": timeout_list, "cache_before": before, "cache_after": after}
    if os.path.isfile(STATE):
        try:
            state["skip"] = json.load(open(STATE, encoding="utf-8")).get("skip", [])
        except Exception:
            pass
    for h, cp in files:
        rel = os.path.relpath(h, MNT)
        if not os.path.isfile(os.path.join(CACHE, cache_key(cp, os.stat(h).st_size) + ".json")):
            if rel not in state["skip"]:
                state["skip"].append(rel)
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    log("DONE: 成功=%d 超时/失败=%d 缓存 %d→%d (+%d)，skip 清单累计 %d 条"
        % (ok, timeout_list, before, after, after - before, len(state["skip"])))


if __name__ == "__main__":
    main()
