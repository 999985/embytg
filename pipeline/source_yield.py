#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""source_yield.py —— 每源产能归因（哪个源在白烧配额）

## 为什么需要它

每个源每天 300 次请求是硬上限，8 个源全打满 = 2400 次/天，而实际出片约 250 集。
**命中率只有约 10%**。问题在于：以前预算文件里只记了「打了多少枪」（`day`）和
「失败几次」（`fails`），**从不记「打中多少」**——于是"哪个源在白烧配额"只能靠感觉。

2026-11-13 起 `gha_state.record_source_success()` 会给 `sources[label]["ok"]` +1。
本脚本把三件事摆在一起看：

    day（请求次数） / ok（成功出片） / fails（失败） → yield = ok/day

判据（不是拍脑袋的阈值）：
- `yield < 2%` 且 `day >= 100`  → **零产出源**，300 次配额只换不到 6 集，建议降权/停用
- `yield < 5%` 且 `day >= 100`  → 低效源，检查是不是 `source_too_slow` / 画质不符
- 其余 → 正常

注意：`ok` 是**当日**计数，与 `day` 同生命周期（预算文件按日重置）。
所以每天应看当天数据，不要拿跨日累计比。

## 用法

```
python3 source_yield.py               # 人类可读
python3 source_yield.py --json
python3 source_yield.py --min-day 50  # 只看消耗超过 50 次的源
```
"""
import argparse
import json
import os
import sys
import time

import gha_state as GS

HERE = os.path.dirname(os.path.abspath(__file__))


def load():
    try:
        d = json.load(open(GS.BUDGET_FILE, encoding="utf-8"))
    except Exception:                                       # noqa: BLE001
        return {}, ""
    return (d.get("sources") or {}), (d.get("date") or "")


def rows(min_day=0):
    src, date = load()
    out = []
    for label, rec in src.items():
        day = int(rec.get("day") or 0)
        if day < min_day:
            continue
        ok = int(rec.get("ok") or 0)
        fails = int(rec.get("fails") or 0)
        dfails = int(rec.get("download_fails") or 0)
        yield_pct = (ok * 100.0 / day) if day else 0.0
        if day >= 100 and yield_pct < 2:
            verdict = "zero"          # 白烧配额
        elif day >= 100 and yield_pct < 5:
            verdict = "low"           # 低效
        elif day == 0:
            verdict = "idle"
        else:
            verdict = "ok"
        out.append({"label": label, "day": day, "ok": ok,
                    "fails": fails, "download_fails": dfails,
                    "yield_pct": round(yield_pct, 1), "verdict": verdict,
                    "cooldown": int(rec.get("download_cooldown_until") or 0) > time.time()})
    out.sort(key=lambda r: (r["yield_pct"], -r["day"]))
    return out, date


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--min-day", type=int, default=0)
    a = ap.parse_args()

    rs, date = rows(a.min_day)
    if a.json:
        print(json.dumps({"date": date, "sources": rs}, ensure_ascii=False, indent=1))
        return 0

    tot_day = sum(r["day"] for r in rs)
    tot_ok = sum(r["ok"] for r in rs)
    print("== 每源产能归因（预算日期 %s）==" % (date or "?"))
    print("   合计 请求 %d 次 → 出片 %d 集 → 命中率 %.1f%%"
          % (tot_day, tot_ok, (tot_ok * 100.0 / tot_day) if tot_day else 0))
    print()
    print("   %-24s %5s %4s %4s %5s %6s  %s" % ("源", "请求", "出片", "失败", "下载败", "命中%", "判定"))
    for r in rs:
        tag = {"zero": "⛔ 白烧配额", "low": "⚠ 低效", "ok": "✓", "idle": "· 未使用"}[r["verdict"]]
        print("   %-24s %5d %4d %4d %5d %6.1f  %s%s"
              % (r["label"][:24], r["day"], r["ok"], r["fails"],
                 r["download_fails"], r["yield_pct"], tag,
                 "（冷却中）" if r["cooldown"] else ""))
    zero = [r for r in rs if r["verdict"] == "zero"]
    if zero:
        wasted = sum(r["day"] - r["ok"] for r in zero)
        print()
        print("   ⛔ %d 个零产出源共烧掉 %d 次请求（约 %.0f%% 总配额）"
              % (len(zero), wasted, wasted * 100.0 / tot_day if tot_day else 0))
        print("      建议：在 .gha_src_budget.json 里把它们的 limit 降到 30，或直接移出源列表。")
    if tot_ok == 0 and tot_day > 0:
        print("   注：ok 计数器 2026-11-13 才启用，当日全 0 属正常，明日起有效。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
