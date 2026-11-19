#!/usr/bin/env python3
"""reprioritize_sources.py —— 按真实产出重排源优先级（根因：优先级是人手写死的，会腐烂）。

问题：`config/source_priority.json` 里的数字是 2026-11-14 手写的实测速度。源站会变
（yuntv 2026-11-20 起全端点 403、video.emos.best 401 停摆），但数字不会自己变，
于是「死源压在活源前面」——每条 lane 先撞一次墙再熔断，白耗时间。

判据只用可验证的数据：
  1. `.gha_src_budget.json` 的 `ok`（真实成功落地次数）降序；
  2. 有产出但次数相同的，按 `_measured` 里记录的速度；
  3. `ok == 0` 的一律沉底（5 分），只有显式登记在 `_verified` 且当轮无产出的
     新源（如手工实测 44 MB/s 的 emos）允许置顶 110。
所以「加一个源就自动生效」：新源第一次有 ok 记录后自动排到该排的位置。

用法：python3 reprioritize_sources.py [--dry]
"""
import argparse
import json
import os
import sys

CFG_DIR = "/opt/embytg/pipeline/config"
PRIO = os.path.join(CFG_DIR, "source_priority.json")
BUDGET = "/opt/embytg/pipeline/.gha_src_budget.json"
DEAD_SCORE = 5
VERIFIED_TOP = 110
TOP_BASE = 100


def load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                          # noqa: BLE001
        return default


def speed_of(measured, key):
    v = (measured or {}).get(key) or ""
    try:
        return float(str(v).split()[0])
    except Exception:                                          # noqa: BLE001
        return 0.0


def rank(prio, budget_sources):
    """按真实产出算出应有的优先级，**不读写文件**。

    2026-11-20：判据必须是「有产出的源永远高于没产出的源」，而
    `config/source_priority.json` 是人写的、会腐烂（yuntv 全端点 403 后
    仍压着在出片的源，每条 lane 先撞墙再熔断，回归用例
    `test_producing_source_outranks_dead` 直接 FAIL）。
    所以把规则做成函数，消费方（build_new_queue_from_sources）每轮直接调用，
    不再依赖「有人记得跑 reprioritize_sources.py」。

    `ok` 是**当日**计数（预算文件按日重置），所以：
      - 今天一次成功都没有（刚过 00:00）→ 返回 {}，由调用方沿用静态值，
        不能把所有源一起沉底；
      - 有成功记录 → 按 ok 降序排，ok==0 一律进死源档；
      - 只在预算文件里出现过的新源也参与排序（加一个源自动生效）。
    """
    prio = prio or {}
    budget_sources = budget_sources or {}
    measured = prio.get("_measured") or {}
    verified = set(prio.get("_verified_top") or [])
    labels = {k for k in prio if not str(k).startswith("_")}
    labels |= {k for k in budget_sources if not str(k).startswith("_")}
    labels = sorted(labels)
    ok = {k: int(((budget_sources or {}).get(k) or {}).get("ok") or 0)
          for k in labels}
    if not any(ok.values()):
        return {}

    producing = sorted([k for k in labels if ok[k] > 0],
                       key=lambda k: (-ok[k], -speed_of(measured, k), k))
    dead = sorted([k for k in labels if ok[k] == 0],
                  key=lambda k: (-speed_of(measured, k), k))

    out = {}
    for i, k in enumerate(producing):
        # 下限 DEAD_SCORE+1：产出源多到 100-i*5 掉到死源档时，
        # 「产出源高于死源」这个不变式会被算术破坏。
        out[k] = max(DEAD_SCORE + 1, TOP_BASE - i * 5)
    for k in dead:
        out[k] = VERIFIED_TOP if k in verified else DEAD_SCORE
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    prio = load(PRIO, {})
    budget = load(BUDGET, {}).get("sources") or {}

    labels = [k for k in prio if not k.startswith("_")]
    ok = {k: int((budget.get(k) or {}).get("ok") or 0) for k in labels}

    new = rank(prio, budget)
    if not new:
        print("  今日暂无任何成功产出记录，保留静态优先级（不把全部源沉底）")
        return 0

    changed = {k: (prio.get(k), new[k]) for k in new if prio.get(k) != new[k]}
    if not changed:
        print("  优先级已与真实产出一致，无需改动")
        return 0
    print("  调整 %d 个源（旧→新）：" % len(changed))
    for k, (o, n) in sorted(changed.items(), key=lambda x: -x[1][1]):
        print("   %-26s %-4s → %-4s  (ok=%d)" % (k, o, n, ok.get(k, 0)))
    if a.dry:
        return 0
    prio.update(new)
    prio["_note"] = ("按真实产出自动重排（reprioritize_sources.py）。"
                     "有产出的按 ok 降序，无产出一律 5 分，"
                     "_verified_top 里的新源置顶 110。")
    with open(PRIO, "w", encoding="utf-8") as f:
        json.dump(prio, f, ensure_ascii=False, indent=1)
    print("  已写入 %s" % PRIO)
    return 0


if __name__ == "__main__":
    sys.exit(main())
