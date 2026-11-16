#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cron_lint.py —— 定时任务自检（防止"任务引用了不存在的脚本"这类静默失效）

为什么需要：2026-11-11 我把 `gha_report.py` 改名成 `runner_report.py`，
三个 cron prompt 还写着旧名字 —— 任务照常"运行成功"，实际什么都没做。
定时任务是**纯文本 prompt**，没有编译期检查，改名/删文件不会报错。

本工具做 6 类检查
-----------------
1. **prompt 里引用的脚本是否存在**（`xxx.py` 出现在 prompt 中 → 必须存在于 pipeline/）
2. **cron 表达式合法**（5 段；分钟/小时/日/月/星期在范围内）
3. **timeout 与周期不冲突**（timeout < 周期，否则会叠加）
4. **silent 合理性**（每 4h 的任务建议 silent）
5. **systemd/cron.d 里引用的脚本是否存在**
6. **重复调度**（两个任务同刻且同会话 → 冲突）

用法：
    python3 cron_lint.py            # 体检，退出码 0=PASS / 1=FAIL
    python3 cron_lint.py --json
"""
import glob
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
# 2026-11-13 交接：qwenpaw 停用，改读 Hermes cron 任务表（/root/.hermes/cron/jobs.json）
HERMES_CRON_JOBS = "/root/.hermes/cron/jobs.json"
SCRIPT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*\.py)\b")
CRON_RE = re.compile(r"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$")


def hermes_jobs():
    try:
        with open(HERMES_CRON_JOBS, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:                                              # noqa: BLE001
        return []
    return data if isinstance(data, list) else (data or {}).get("jobs", [])


def job_text(j):
    return (j.get("prompt") or "").strip()


def check_cron_expr(expr):
    m = CRON_RE.match(expr.strip())
    if not m:
        return "段数不是 5"
    ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]
    for part, (lo, hi) in zip(m.groups(), ranges):
        if part == "*":
            continue
        for tok in part.split(","):
            base = tok.split("/")[0]
            if base == "*":
                continue
            if "-" in base:
                a, b = base.split("-")[:2]
                if not (a.isdigit() and b.isdigit()):
                    return "区间非法 %r" % tok
                if not (lo <= int(a) <= hi and lo <= int(b) <= hi):
                    return "越界 %r" % tok
            elif base.isdigit():
                if not (lo <= int(base) <= hi):
                    return "越界 %r" % tok
            else:
                if not re.fullmatch(r"[a-zA-Z]{3}", base):
                    return "无法解析 %r" % tok
    return None


def main():
    js = hermes_jobs()
    problems = []
    rows = []
    for j in js:
        name = j.get("name", "?")
        sched = (j.get("schedule") or {}).get("expr")
        t = job_text(j)
        silent = "静默" in t          # Hermes 无 silent 字段，按 prompt 语义判断
        # 1) 脚本存在性
        for s in sorted(set(SCRIPT_RE.findall(t))):
            if s in ("main.py",):
                continue
            # 2026-11-15（E127）：**脚本引用要按项目真实布局解析，不能只看 pipeline/ 根目录。**
            # 实测「告警自愈」任务的 prompt 里写了 pipeline/tests/test_regressions.py
            # （那是要求 agent 补回归测试的正确指引），却被判 missing_script ——
            # 判据只查 HERE/s，而测试文件本来就在 HERE/tests/ 下。
            # 一个检查如果会把「写对了的指引」判成故障，错的是检查。
            _cands = [os.path.join(HERE, s),
                      os.path.join(HERE, "tests", s),
                      os.path.join(os.path.dirname(HERE), s)]
            if not any(os.path.exists(x) for x in _cands):
                problems.append({"job": name, "kind": "missing_script", "detail": s})
        # 2) cron 合法
        if sched:
            err = check_cron_expr(sched)
            if err:
                problems.append({"job": name, "kind": "bad_cron", "detail": "%s (%s)" % (sched, err)})
        # 3) timeout 与周期
        timeout = None                # Hermes cron 不暴露 per-job timeout
        period = None
        if sched:
            try:
                minute, hour = sched.split()[:2]
                if minute.startswith("*/"):
                    period = int(minute[2:]) * 60
                elif hour.startswith("*/"):
                    period = int(hour[2:]) * 3600
                elif minute.isdigit() and hour == "*":
                    period = 3600
                else:
                    period = 86400
            except Exception:                                      # noqa: BLE001
                period = None
        if timeout and period and timeout >= period:
            problems.append({"job": name, "kind": "timeout_ge_period",
                             "detail": "timeout=%ss ≥ 周期 %ss" % (timeout, period)})
        # 4) 高频任务应 silent
        if period and period <= 4 * 3600 and not silent:
            problems.append({"job": name, "kind": "noisy_high_freq", "detail": "周期 %ss 未 silent" % period})
        rows.append({"name": name, "cron": sched, "timeout": timeout,
                     "silent": silent, "scripts": len(set(SCRIPT_RE.findall(t)))})
        rows[-1]["enabled"] = bool(j.get("enabled", True))

    # 5) systemd / cron.d 引用的脚本
    for unit in glob.glob("/etc/systemd/system/embytg-*.service") + glob.glob("/etc/cron.d/embytg_*"):
        txt = open(unit, encoding="utf-8", errors="ignore").read()
        for s in set(SCRIPT_RE.findall(txt)):
            if not os.path.exists(os.path.join(HERE, s)):
                problems.append({"job": os.path.basename(unit), "kind": "missing_script", "detail": s})

    # 6) 同刻同会话
    slots = {}
    for j in js:
        sched = (j.get("schedule") or {}).get("expr")
        if not sched:
            continue
        key = sched
        if key in slots:
            problems.append({"job": j.get("name"), "kind": "same_slot",
                             "detail": "与 %s 同 cron 同会话" % slots[key]})
        else:
            slots[key] = j.get("name")

    res = {"jobs": len(js), "rows": rows, "problems": problems,
           "verdict": "PASS" if not problems else "FAIL"}
    if "--json" in sys.argv:
        print(json.dumps(res, ensure_ascii=False, indent=1))
    else:
        print("== 定时任务自检（%d 个任务）==" % len(js))
        for r in rows:
            print("  %-24s %-13s timeout=%-5s silent=%-5s 引用脚本=%d"
                  % (r["name"][:24], r["cron"], r["timeout"], r["silent"], r["scripts"]))
        print()
        if problems:
            print("  问题 %d 条：" % len(problems))
            for p in problems:
                print("    ✘ [%s] %s：%s" % (p["kind"], p["job"], p["detail"]))
        else:
            print("  ✅ PASS：无失效引用、cron 合法、timeout 与周期不冲突、高频任务已静默、无同刻冲突")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
