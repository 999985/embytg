#!/usr/bin/env python3
"""src_budget_rebalance.py —— 按实测命中率重新分配各源日额度。

## 为什么要有这个（2026-11-17）

额度是平均分的（每源 300/天），但产出极度不均。今日 18:00 实测：

| 源 | 今日请求 | 成功 | 命中率 | 实得额度 |
|---|---|---|---|---|
| aaa.yusen6.ccwu.cc | 133 | 55 | 41% | 400（顶格） |
| emby.bangumi.ca | 173 | 38 | 22% | 300 |
| 39.96.113.82-mirror | 112 | 12 | 11% | 400 |
| emby.chapanda.site | 116 | 5 | 4% | 160 |
| ct.cdn.bgp.yt | 78 | 0 | 0% | 160 |
| ibukifuko.tech | 82 | 0 | 0% | 10 |

最赚钱的源被顶在 400，而 0 产出的源照样拿 160 次。`ct.cdn.bgp.yt` 的
`daily_cap=0`（"不限"）语义让它绕过惩罚拿到 160 —— 这是 E104 那条
「固定 cap 不得绕过惩罚」没覆盖到的分支：`0` 走的是 `400*pen`。

## 规则

命中率 = ok / day（day < MIN_SAMPLE 不判，保留原样，避免清早样本噪声）：

    hit >= 0.30          → 800   （主力，给它吃饱）
    0.15 <= hit < 0.30   → 500
    0.05 <= hit < 0.15   → 200
    hit <  0.05 且 day>=30 → 20  （零产出，只留探活次数）

`daily_cap` 是礼貌上限，频率约束（burst_20m=60 / 间隔 1.6s / 单源并发 1）
在 src_guard 里，跟这个无关，不会被这里放宽。
"""
import json
import os
import sys
import time

PIPE = os.path.dirname(os.path.abspath(__file__))
BUDGET = os.path.join(PIPE, ".gha_src_budget.json")
MIN_SAMPLE = 30          # 少于这么多次请求不判命中率

# 2026-11-23（用户口径）：**源站配额不设上限，只做不频繁的请求。**
# 这里原来按命中率把 daily_cap 压到 800/500/200/20，每 2 小时跑一次 ——
# 等于我手工把上限放开后，它又会悄悄改回去（今天实测 daily_cap 又变回 20）。
# 频率约束（间隔 1.6s / 单源并发 1 / src_guard 的 burst）才是真正该管的，
# 而那是 src_guard 的职责。本脚本从此只做两件事：报告命中率、把额度置 0（不限）。
def tier_of(hit, day):                                     # noqa: ARG001
    return 0


def main(apply=True):
    with open(BUDGET, encoding="utf-8") as f:
        d = json.load(f)
    srcs = d.get("sources") or {}
    rows, changed = [], 0
    for name, rec in srcs.items():
        if not isinstance(rec, dict) or rec.get("disabled"):
            continue
        day = int(rec.get("day") or 0)
        ok = int(rec.get("ok") or 0)
        hit = (ok / day) if day else 0.0
        new_cap = tier_of(hit, day)
        if new_cap is None:
            rows.append((name, day, ok, hit, rec.get("daily_cap"), None, ""))
            continue
        if rec.get("daily_cap") != new_cap:
            changed += 1
            note = "2026-11-23 命中率 %d/%d=%.0f%% → 不限（只保留频率约束）" % (ok, day, hit * 100)
            if apply:
                rec["daily_cap"] = new_cap
                rec["cap_note"] = note
            rows.append((name, day, ok, hit, rec.get("daily_cap"), new_cap, note))
        else:
            rows.append((name, day, ok, hit, rec.get("daily_cap"), new_cap, ""))

    rows.sort(key=lambda r: -(r[3] or 0))
    print("  源                        今日  成功  命中率   现cap  →新cap")
    for name, day, ok, hit, cap, new_cap, note in rows:
        arrow = "" if new_cap is None or new_cap == cap else "→%s" % new_cap
        print("   %-24s %4d %5d %6.0f%% %6s %7s" % (name, day, ok, hit * 100, cap, arrow))

    if apply and changed:
        tmp = BUDGET + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
        os.replace(tmp, BUDGET)
        print("  已写入 %d 个源的额度（%s）" % (changed, time.strftime("%H:%M")))
    elif not changed:
        print("  无需调整")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--dry" not in sys.argv))
