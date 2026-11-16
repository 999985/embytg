#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ingest_quota.py —— 入库流量预算闸门（用户 2026-11-10：一天两百多 G，一个月只有 1T）

为什么要它
----------
用户原话：「入库不要无限入库，一天两百多 g 就行了」（2026-11-10 更正：月流量是 10T，不是 1T）。
实测历史：11-08 首次全库落地 **21.6 TB**，11-09 一天 **1273 GB** —— 按这个速度 1 TB/月的套餐
两天就烧穿。所以入库（从源服下载 / TG 拉取 / 直链转存）必须有**双闸门**：

| 闸门 | 值 | 说明 |
|---|---|---|
| 日上限 | **300 GB** | 用户 2026-11-10 上调 |
| 月上限 | **10000 GB** | 用户 2026-11-10 更正：月流量是 10T |
| 月度安全线 | **9000 GB** | 留 10% 给播放/校验/重下 |

数据源
------
`tg.db` 的 `resource_state.size + landed_at`（落地即记账），外加 `.ingest_budget.json`
（`month_start` 之前的存量不算本月 —— 首次全库 21.6TB 不能算进预算）。

用法
----
    python3 ingest_quota.py --status                 # 今日/本月用量、剩余、还能下几天
    python3 ingest_quota.py --check --need 5.3       # 下 5.3GB 前问一句（退出码 0=可以, 3=超限）
    python3 ingest_quota.py --check --need 15.7 --explain
    python3 ingest_quota.py --top 5                  # 今日最大的 5 个落地文件（谁在吃流量）

约定
----
任何下载/转存脚本（`src_guard` 拉流、`moviewatch ingest`、`tgpipe apply`）在开始**单个文件**前
调 `--check --need <GB>`；返回非 0 就跳过并记日志，不要"先下再说"。
"""
import argparse
import datetime
import json
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "tg.db")
CFG = os.path.join(HERE, ".ingest_budget.json")

DEFAULTS = {"daily_gb": 300.0, "monthly_gb": 10000.0, "safety_gb": 9000.0,
            "month_start": "2026-11-10", "note": "用户：一天两百多G，一月1T"}


def cfg():
    d = dict(DEFAULTS)
    if os.path.exists(CFG):
        try:
            d.update(json.load(open(CFG, encoding="utf-8")))
        except Exception:                                          # noqa: BLE001
            pass
    return d


MANUAL = os.path.join(HERE, ".ingest_manual.jsonl")


def manual_usage():
    """源服手工拉取的流量（tg.db 记账覆盖不到，单独记）"""
    today = datetime.date.today().isoformat()
    d = m = 0.0
    if os.path.exists(MANUAL):
        for line in open(MANUAL, encoding="utf-8"):
            try:
                r = json.loads(line)
            except Exception:                                      # noqa: BLE001
                continue
            ts = r.get("ts", "")[:10]
            if ts == today:
                d += r.get("gb", 0)
            if ts >= cfg()["month_start"]:
                m += r.get("gb", 0)
    return d, m


ORACLE_FLAG = os.path.join(HERE, ".oracle_media_ingest.disabled")


def enabled():
    """入库总开关（上片只归韩国 runner，本机只做控制面，不再拉片）。
    两个来源任一关闭即停：`.oracle_media_ingest.disabled` 文件（autoingest 也读它）或配置 ingest_enabled=false。"""
    if os.path.exists(ORACLE_FLAG):
        return False
    return bool(cfg().get("ingest_enabled", True))


def usage():
    """返回 (今日GB, 今日文件数, 本月GB(自 month_start), 本月文件数)"""
    c = cfg()
    con = sqlite3.connect(DB)
    today = datetime.date.today().isoformat()
    d_gb, d_n = con.execute(
        "select coalesce(sum(size),0)/1e9, count(*) from resource_state "
        "where date(landed_at,'unixepoch','localtime')=?", (today,)).fetchone()
    m_gb, m_n = con.execute(
        "select coalesce(sum(size),0)/1e9, count(*) from resource_state "
        "where date(landed_at,'unixepoch','localtime')>=?", (c["month_start"],)).fetchone()
    md, mm = manual_usage()
    return d_gb + md, d_n, m_gb + mm, m_n


def status(explain=False):
    c = cfg()
    d_gb, d_n, m_gb, m_n = usage()
    today = datetime.date.today()
    days_in_month = (datetime.date(today.year + (today.month == 12), (today.month % 12) + 1, 1)
                     - datetime.date(today.year, today.month, 1)).days
    left_days = days_in_month - today.day + 1
    m_left = max(0.0, c["safety_gb"] - m_gb)
    d_left = max(0.0, c["daily_gb"] - d_gb)
    per_day = m_left / left_days if left_days else 0
    print("== 入库流量预算 ==")
    if not enabled():
        print("  ⛔ 入库已停用（ingest_enabled=false）：上片由韩国 runner 负责，本机不再拉片")
    print("  今日 %s  %7.1f / %.0f GB  (剩 %.1f GB, %d 个文件)" % (today, d_gb, c["daily_gb"], d_left, d_n))
    print("  本月(自 %s) %7.1f / %.0f GB  (安全线 %.0f, 剩 %.1f GB, %d 个文件)"
          % (c["month_start"], m_gb, c["monthly_gb"], c["safety_gb"], m_left, m_n))
    print("  本月剩 %d 天 → 均摊每天还能下 %.1f GB" % (left_days, per_day))
    print("  今天最多可再下: %.1f GB" % min(d_left, m_left))
    if explain:
        print("  依据: 用户 2026-11-10 「入库不要无限入库，一天两百多g就行了，因为我一个月只有1t流量」")
        print("  记账: tg.db resource_state(size, landed_at)；month_start 之前的存量不计入（首次全库 21.6TB）")
    return d_left, m_left


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--need", type=float, default=0.0, help="本次要下的 GB")
    ap.add_argument("--explain", action="store_true")
    ap.add_argument("--top", type=int, default=0)
    ap.add_argument("--estimate", action="store_true", help="直接扫挂载按 mtime 估算今日落地量（记账滞后时用）")
    ap.add_argument("--lib", help="配合 --estimate，只扫某个库")
    a = ap.parse_args()

    if a.estimate:
        import time
        MNT = "/opt/embytg/mnt/embytg"
        roots = [os.path.join(MNT, a.lib)] if a.lib else [os.path.join(MNT, x) for x in ("TV", "Movies")]
        t0 = time.time() if "time" in dir() else None
        import time as _t
        t0 = _t.time()
        zero = _t.mktime(_t.localtime()[:3] + (0, 0, 0, 0, 0, -1))
        tot = 0.0
        cnt = 0
        for r in roots:
            for dp, _, fns in os.walk(r):
                for f in fns:
                    if not f.lower().endswith((".mp4", ".mkv", ".ts")):
                        continue
                    fp = os.path.join(dp, f)
                    try:
                        stt = os.stat(fp)
                    except OSError:
                        continue
                    if stt.st_mtime >= zero:
                        tot += stt.st_size
                        cnt += 1
        c = cfg()
        print("== 挂载侧估算（mtime 今日，%s）==" % (a.lib or "TV+Movies"))
        print("  %d 个文件  %.1f GB  （耗时 %.0fs）" % (cnt, tot / 1e9, _t.time() - t0))
        print("  记账口径（tg.db）今日: %.1f GB —— 若明显小于估算，说明 resource_state 滞后" % usage()[0])
        print("  注意：rclone 挂载的 mtime 多数不是落地时间（重命名才会刷新），估算只作参考，以 resource_state 记账为准")
        print("  日上限 %.0f GB → %s" % (c["daily_gb"], "已超" if tot / 1e9 > c["daily_gb"] else "未超"))
        return

    if a.top:
        con = sqlite3.connect(DB)
        rows = con.execute(
            "select path, round(size/1e9,2), datetime(landed_at,'unixepoch','localtime') "
            "from resource_state where date(landed_at,'unixepoch','localtime')=date('now','localtime') "
            "order by size desc limit ?", (a.top,)).fetchall()
        print("今日最大的 %d 个落地文件：" % len(rows))
        for p, g, t in rows:
            print("  %7.2f GB  %s  %s" % (g, t, os.path.basename(p)[:60]))
        return

    d_left, m_left = status(a.explain)
    if a.check:
        allow = min(d_left, m_left)
        need = a.need or 0.0
        if need <= allow:
            print("\n✅ 可以下 %.2f GB（余量 %.1f GB）" % (need, allow))
            sys.exit(0)
        print("\n⛔ 超限：本次需要 %.2f GB，但只剩 %.1f GB" % (need, allow))
        print("   处理：等明天（日上限重置）、或压缩目标（换 1080p）、或调 .ingest_budget.json 的 daily_gb")
        sys.exit(3)


if __name__ == "__main__":
    main()
