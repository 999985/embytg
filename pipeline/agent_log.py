#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""agent_log.py —— 统一时间源的协作日志写入器（AgentCommunication.log）

为什么需要它（2026-11-10 用户指出「日期和时间每个 agent 好像有偏差」）：
  LLM 并不知道真实时间，**手写时间戳必然漂移**。本次实测：日志里两条写着
  09:50 / 09:58，而对应 commit 的真实时间是 09:38 —— 比现实"超前"了 12~20 分钟。
  根因不是时区配错，是**时间靠猜**。

统一规则（写进 AGENTS.md §7.5）：
  1. 时间戳**只能由服务器产生**，禁止手写；
  2. 时区固定 `Asia/Shanghai`（宿主机 `timedatectl` = CST +0800，NTP 已同步）；
  3. 需要时间的场合一律调本脚本，或 `date '+%F %T %Z'`；
  4. 容器时区：`emby` = CST；`teldrive-postgres` 2026-11-10 起也已统一为 CST
     （compose 加 `TZ` + `ALTER DATABASE teldrive SET timezone`），`teldrive` 镜像无 `date`。

用法：
  python3 agent_log.py who                       # 列出已用过的 agent 名字
  python3 agent_log.py start  --name 千问-岚 --task "跑 todb_resolve" \\
        --files "pipeline/todb_resolve.py" --resources "TODB API" --eta "10:30" \\
        --broadcast "别动 .todb_title_cache.json"
  python3 agent_log.py done   --name 千问-岚 --task "跑 todb_resolve" --result "补 30 条，naming_health 五项 PASS"
  python3 agent_log.py abort  --name 千问-岚 --task "..." --result "TODB 429，已回滚"
  python3 agent_log.py note   --name 千问-岚 --broadcast "扫描 Running，别发 RefreshItem"
  python3 agent_log.py verify                    # 校验：未来时间 / 乱序 / 缺字段

默认日志路径 `/opt/embytg/AgentCommunication.log`，可用 AGENT_LOG 覆盖。
"""
import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timezone, timedelta

LOG = os.environ.get("AGENT_LOG", "/opt/embytg/AgentCommunication.log")
CST = timezone(timedelta(hours=8))
HEAD_RE = re.compile(r"^## \[(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\] ([^|]+)\| ([^|]+)\| (.*)$")
STATUSES = ("开始", "进行中", "广播", "完成", "中止")
# 2026-11-16（E144 同轮）：判「未闭环」的年龄门槛。
# 上一版把**任何**没有后续 完成/中止 的 开始/进行中 都算悬空 → 实测每轮必 FAIL：
# `embytg-pipeline` 是机械写入者，每 6 分钟写一条「进行中 | 韩国双槽上片」，
# 下一次校验几乎必然撞在「刚开、还没写完」的窗口里（实测 06:16 就是这么挂的）。
# **一个永远为 FAIL 的判据等于没有判据**（E143 的同一个教训：告警疲劳）。
# 现在：超过 STALE_OPEN_MIN 还没结才算悬空；门槛内记为「进行中（正常）」。
# 150 分钟与 `claim_reap` 的僵尸判据同源（单集最长 2h + 余量）。
STALE_OPEN_MIN = int(os.environ.get("AGENT_LOG_STALE_MIN", "150"))


def now_str():
    """唯一时间源：服务器本地时间（Asia/Shanghai）"""
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M")


def zone_note():
    try:
        return subprocess.run(["date", "+%Z %z"], capture_output=True, text=True,
                              timeout=5).stdout.strip()
    except Exception:
        return "?"


def append(block):
    # 2026-11-15 自省（E037-4）：时间戳必须在**持锁写入的这一刻**生成。
    # 旧路径 `build()` 先 now_str()、再 open/write，两步之间别的进程可能插进来 ——
    # 于是 L15769 那条「18:57 进行中」落在了 L15756「19:02 完成」后面，
    # 每一次这样的交错都会被 verify 记成「时间戳错」，而它其实只是竞态。
    # 现在：占 flock → 重算时间 → 落盘 → 释放。时间与写入顺序从此严格单调。
    import fcntl
    with open(LOG, "a+", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            ts = now_str()
            block = re.sub(r"^## \[\d{4}-\d{2}-\d{2} \d{2}:\d{2}\]", "## [%s]" % ts,
                           block, count=1)
            block = re.sub(r"`\d{4}-\d{2}-\d{2} \d{2}:\d{2}`", "`%s`" % ts, block)
            f.write("\n" + block.rstrip("\n") + "\n")
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    print(block)


def build(name, status, task, files=None, resources=None, eta=None,
          broadcast=None, result=None):
    ts = now_str()
    rows = ["## [%s] %s | %s | %s" % (ts, name, status, task)]
    rows.append("- **时间源**：服务器 `%s`（%s，NTP 同步；**禁止手写**）" % (ts, zone_note()))
    if files:
        rows.append("- **正在动**：%s" % files)
    if resources:
        rows.append("- **占用资源**：%s" % resources)
    if eta:
        rows.append("- **预计结束**：%s" % eta)
    if broadcast:
        rows.append("- **广播（别人必须知道）**：%s" % broadcast)
    if result:
        rows.append("- **结果**：%s" % result)
    return "\n".join(rows)


def who():
    if not os.path.exists(LOG):
        print("(日志不存在)"); return
    names, last = {}, {}
    for ln in open(LOG, encoding="utf-8"):
        m = HEAD_RE.match(ln.rstrip("\n"))
        if m:
            ts, nm, st, _t = m.groups()
            nm = nm.strip()
            names[nm] = names.get(nm, 0) + 1
            last[nm] = ts
    if not names:
        print("(还没有带时间戳的条目)"); return
    for nm in sorted(names):
        print("  %-10s %2d 条，最后 %s" % (nm, names[nm], last[nm]))


def verify():
    """校验：未来时间 / 乱序 / 状态词非法 / **未闭环任务(R2)** / **同名多实例(R1)**。

    AGENTS.md §7.6（2026-11-10）：用户「入库是我喊你做的，但是后面就不知道发生什么了」。
    根因之一是长任务没有「完成/中止」闭环 + 两个会话用同一 agent 名字。
    故 PASS 的前提增加：0 条未闭环，且同名并发「开始」要显式告警。
    """
    if not os.path.exists(LOG):
        print("日志不存在"); return 1
    now = datetime.now(CST)
    prev = None
    max_seen = datetime(1970, 1, 1, tzinfo=CST)   # 2026-11-15：判「真乱序」的基准（显式初值，不用 min）
    hard = []                 # 近 24h 的问题：算 FAIL
    hist = []                 # 24h 之前的历史问题：只提示，不算 FAIL（日志只追加，改不了）
    hist_note = {}            # 2026-11-15：历史乱序只计数，不再进 hist 逐条刷屏
    cutoff = now - timedelta(hours=24)
    heads = 0
    entries = []          # (行号, ts, name, status, task)
    for i, ln in enumerate(open(LOG, encoding="utf-8"), 1):
        m = HEAD_RE.match(ln.rstrip("\n"))
        if not m:
            continue
        heads += 1
        ts, nm, st, task = m.groups()
        nm, st = nm.strip(), st.strip()
        entries.append((i, ts, nm, st, task.strip()))
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M").replace(tzinfo=CST)
        except ValueError:
            hard.append("L%d 时间格式错: %s" % (i, ts)); continue
        bag = hard if dt >= cutoff else hist
        # 2026-11-15 自省（E037-2）：区分两种「乱序」，别让并发竞态冒充时间戳写坏。
        # ① 竞态乱序：该行时间 ≥ 它前一条（那是别人后追加的）→ 时序本身合理，
        #    只是分两个进程 append 造成的写入交错。真问题为零，历史上 10 条全是这种。
        # ② 真乱序：该行时间比它**之前出现过的最新时间**还早 → 时间戳真的写坏了，算 FAIL。
        # 旧口径只比相邻两行，两种混在一起报，selfcheck 天天被噪音埋掉（告警疲劳）。
        if prev and dt < prev[0] - timedelta(minutes=1):
            if dt >= max_seen:
                hist_note["race"] = hist_note.get("race", 0) + 1
            else:
                bag.append("L%d **乱序** %s 早于此前最新 %s（时间戳疑似写坏）"
                           % (i, ts, max_seen.strftime("%Y-%m-%d %H:%M")))
        max_seen = max(max_seen, dt)
        if dt > now + timedelta(minutes=2):
            bag.append("L%d **未来时间** %s（现在 %s）— %s"
                       % (i, ts, now.strftime("%Y-%m-%d %H:%M"), nm))
        if st not in STATUSES:
            bag.append("L%d 状态词非法「%s」（只允许 %s）" % (i, st, "/".join(STATUSES)))
        prev = (dt, ts)

    # R2 未闭环：§7.3「一个 agent 同时只允许一条进行中」→ 同名 agent 之后
    # 任意一条 完成/中止 都把该 agent 之前所有 开始/进行中 结掉（广播类 note 常换任务名，
    # 按任务名精确匹配会把「广播→done(广播(...))」误判成悬空，实测踩过）。
    probs = hard
    open_tasks = []      # 超过 STALE_OPEN_MIN 仍没结 → 真悬空（算 FAIL）
    inflight = []        # 还在门槛内 → 正常进行中（不算 FAIL）
    for idx, (ln, ts, nm, st, task) in enumerate(entries):
        if st not in ("开始", "进行中"):
            continue
        # 2026-11-16 18:55（E157）：**纯广播不该占「进行中」这一格**。
        # `note` 以前写「进行中|广播」，于是每写一条广播就给自己留一条永远没有
        # 「完成」的悬空条目 —— 实测 06:20 的广播条让 verify 每轮报
        # 「未闭环 1 条」，而它根本不涉及任何资源（AGENTS §7.2 只要求
        # 占用资源的任务闭环）。现在 `note` 写独立状态「广播」；历史上已经写成
        # `进行中|广播` 的条目按广播对待，不再计入悬空。
        if st == "进行中" and task.strip() == "广播":
            continue
        closed = any(e[2] == nm and e[3] in ("完成", "中止") and j > idx
                     for j, e in enumerate(entries))
        if closed:
            continue
        try:
            age = (now - datetime.strptime(ts, "%Y-%m-%d %H:%M").replace(tzinfo=CST)
                   ).total_seconds() / 60.0
        except ValueError:
            age = 10 ** 9                      # 时间戳坏掉的，按悬空处理
        (inflight if age <= STALE_OPEN_MIN else open_tasks).append((ts, nm, st, task))

    # R1 同名多实例：同一名字出现 >=2 条未结的「开始」（不同任务）
    by_name = {}
    for ts, nm, st, task in (open_tasks + inflight):
        by_name.setdefault(nm, []).append((ts, task))
    dup = {n: v for n, v in by_name.items() if len(v) >= 2}

    print("== 时间一致性校验 ==")
    print("   条目 %d 条；当前时间 %s（%s）" % (heads, now.strftime("%Y-%m-%d %H:%M"), zone_note()))
    print("== 任务闭环校验 (AGENTS §7.6 R1/R2) ==")
    if open_tasks:
        print("   ! **未闭环 %d 条**（超过 %d 分钟仍没结，PASS 的前提是 0 条）:"
              % (len(open_tasks), STALE_OPEN_MIN))
        for ts, nm, st, task in open_tasks:
            print("      [%s] %s | %s | %s" % (ts, nm, st, task[:80]))
        hard.append("未闭环 %d 条（%s）" % (len(open_tasks), "、".join(sorted({t[1] for t in open_tasks}))))
    else:
        print("   ✓ 所有 开始/进行中 均已结掉")
    if inflight:
        print("   · 进行中 %d 条（%d 分钟内，属正常在跑，不算 FAIL）:"
              % (len(inflight), STALE_OPEN_MIN))
        for ts, nm, st, task in inflight[:5]:
            print("      [%s] %s | %s | %s" % (ts, nm, st, task[:60]))
    if dup:
        for n, v in dup.items():
            print("   ! **同名多实例嫌疑** %s 有 %d 条未结开始（R1：名字必须含会话后缀）" % (n, len(v)))
            for ts, t in v:
                print("      [%s] %s" % (ts, t[:70]))
            hard.append("同名多实例 %s" % n)
    if hist_note.get("race"):
        print("   · 并发追加竞态 %d 处（多 agent 同时写日志，时序合理，不计 FAIL）"
              % hist_note["race"])
    if hist:
        print("   · 历史问题 %d 条（24h 前，只追加日志无法回溯，不计 FAIL）：" % len(hist))
        for h in hist[:8]:
            print("      " + h)
        if len(hist) > 8:
            print("      …另有 %d 条" % (len(hist) - 8))
    if hard:
        print("   ✗ 近 24h 问题 %d 条：" % len(hard))
        for h in hard[:12]:
            print("      " + h)
    if probs:
        print("   ✗ 结论: FAIL（%d 项）" % len(probs))
        return 1
    print("   ✓ 无未来时间、无乱序、状态词合法、0 未闭环、同名无冲突")
    return 0


def show_tz():
    """打印各处时间，明确谁和谁差 8 小时"""
    host = subprocess.run(["date", "+%F %T %Z %z"], capture_output=True, text=True).stdout.strip()
    utc = subprocess.run(["date", "-u", "+%F %T %Z"], capture_output=True, text=True).stdout.strip()
    print("== 时间源对照（统一口径：Asia/Shanghai）==")
    print("   宿主机（权威）     %s" % host)
    print("   脚本用（本文件）   %s" % datetime.now(CST).strftime("%F %T %Z"))
    print("   UTC               %s" % utc)
    for c in ("emby", "teldrive-postgres"):
        try:
            out = subprocess.run(["docker", "exec", c, "date", "+%F %T %Z %z"],
                                 capture_output=True, text=True, timeout=8).stdout.strip()
            print("   容器 %-17s %s" % (c, out or "（无 date 命令）"))
        except Exception:
            print("   容器 %-17s 取不到" % c)
    print("   结论：日志一律用 CST；teldrive-postgres 已于 2026-11-10 统一为 CST（历史 UTC 数据是 timestamptz，不影响比较）")
    return 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for c in ("start", "done", "abort", "note"):
        p = sub.add_parser(c)
        p.add_argument("--name", required=(c != "note"))
        p.add_argument("--task", default="")
        p.add_argument("--files")
        p.add_argument("--resources", default="无")
        p.add_argument("--eta")
        p.add_argument("--broadcast")
        p.add_argument("--result")
    sub.add_parser("who")
    sub.add_parser("verify")
    sub.add_parser("tz")
    a = ap.parse_args()
    if a.cmd == "who":
        return who()
    if a.cmd == "verify":
        return verify()
    if a.cmd == "tz":
        return show_tz()
    task = a.task or {"note": "广播", "done": "（补结果）", "abort": "（中止）"}.get(a.cmd, "")
    status = {"start": "开始", "done": "完成", "abort": "中止", "note": "广播"}[a.cmd]
    block = build(a.name, status, task, a.files, a.resources, a.eta, a.broadcast, a.result)
    append(block)


if __name__ == "__main__":
    sys.exit(main() or 0)
