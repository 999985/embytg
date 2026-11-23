#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""check_caps.py —— 防止「量化上限」再次悄悄回归。

背景：2026-11-22 一天内三次因为「保护性上限」把产能饿死：
  日上限 300/400、burst 60、单会话 12 次、--max-attempts 1
每次都是代码里一个看似合理的数字，把整个系统锁成「没活」。

这个脚本扫描源码，发现这些上限重新生效就直接非零退出。
接进 daily_ops / tests 之后，回归会在当天被抓到，不靠人记。

用法：python3 check_caps.py [--json]
"""
import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def read(name):
    p = os.path.join(HERE, name)
    try:
        return open(p, encoding="utf-8").read()
    except Exception:                                       # noqa: BLE001
        return ""


def check_src_guard():
    src = read("src_guard.py")
    m = re.search(r"MAX_PER_HOST\s*=\s*(.+)", src)
    problems = []
    if not m:
        problems.append("src_guard.MAX_PER_HOST 定义丢失（无法判断上限）")
    else:
        expr = m.group(1)
        if "environ" not in expr:                           # 必须可被环境覆盖
            lit = re.search(r"\d+", expr)
            if lit and int(lit.group()) < 100:
                problems.append("MAX_PER_HOST 为固定值 %s（<100），"
                                "枚举源站目录会被掐死" % lit.group())
    return problems


def check_eligible_sources():
    src = read("gha_state.py")
    problems = []
    # 日上限必须只在显式配置时生效
    if "_explicit_cap" not in src:
        problems.append("eligible_sources 没有 _explicit_cap 分支 —— "
                        "日上限可能又变成无条件生效")
    if "burst_20m" in src and "_burst < 100000" not in src:
        problems.append("burst_20m 没有放宽分支 —— 20 分钟条数上限可能又生效")
    # 慢源必须走短退避
    if "sources_unavailable" not in src:
        problems.append("ep_fail_class 缺少 sources_unavailable 的短退避处理")
    return problems


def check_publish_attempts():
    src = read("gha_queue.py")
    problems = []
    m = re.search(r'"--max-attempts",\s*(.+)', src)
    if not m:
        problems.append("publish-auto 找不到 --max-attempts 参数")
    else:
        expr = m.group(1)
        if "environ" in expr:
            return problems                                  # 可配置，视为通过
        lit = re.search(r'"(\d+)"', expr)
        if lit and int(lit.group(1)) < 4:
            problems.append("publish-auto 的 --max-attempts=%s（<4）："
                            "队列很大但每次只试几部，会误报 no_job" % lit.group(1))
    return problems


def check_budget_file():
    problems = []
    p = os.path.join(HERE, ".gha_src_budget.json")
    try:
        d = json.load(open(p, encoding="utf-8"))
    except Exception:                                       # noqa: BLE001
        return problems
    burst = int((d.get("limits") or {}).get("burst_20m") or 0)
    if burst and burst < 100000:
        problems.append("预算文件里 burst_20m=%d 仍在生效（应为 0 = 不限）" % burst)
    return problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    checks = {
        "src_guard.MAX_PER_HOST": check_src_guard(),
        "gha_state.eligible_sources": check_eligible_sources(),
        "gha_queue.publish-auto": check_publish_attempts(),
        ".gha_src_budget.json": check_budget_file(),
    }
    bad = {k: v for k, v in checks.items() if v}
    if a.json:
        print(json.dumps({"ok": not bad, "problems": bad}, ensure_ascii=False, indent=1))
    else:
        for k, v in checks.items():
            print("  %-28s %s" % (k, "OK" if not v else "问题 %d 条" % len(v)))
            for x in v:
                print("      ✘ %s" % x)
        print("  小结：%s" % ("PASS" if not bad else "FAIL"))
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
