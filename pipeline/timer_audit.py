#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""timer_audit.py — 每天巡检「定时任务 + runner」日志，出事故必须留痕。

用户口径（2026-11-23）：
  「记得日志一定要记录出事情 其次就是每天的定时任务也要检查日志」

此前的问题：daily_ops 检查的是业务指标（五账、磁盘、队列），**从不看 journal**。
于是 `embytg-ghsync` 连续几天推送失败、`embytg-mount` 偶发掉挂载、
runner 每分钟刷 no_session —— 全都躺在日志里没人看。事故不是没发生，
是没被记录成事故。

本脚本做三件事：
  1. 扫近 24h 所有 embytg-* 单元的失败（Failed with result / 非零退出）
  2. 统计 runner loop.log 的退出码与原因分布（rc=1 / no_session / no_job …）
  3. 有失败 → 写 alerts 日志 + 走 alert.py 出口；全绿 → 只留一行摘要

用法：
  python3 timer_audit.py            # 人读
  python3 timer_audit.py --json     # 机读
  python3 timer_audit.py --quiet    # 只在有失败时输出（给 timer 用）
"""
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER_LOG = "/opt/embytg-runner/logs/loop.log"
STATE = os.path.join(HERE, ".timer_audit.json")
FAIL_RE = re.compile(r"Failed with result '([^']+)'")
REASON_RE = re.compile(r"reason=([a-z_]+)")


def _sh(cmd, timeout=60):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                           timeout=timeout)
        return r.stdout or ""
    except Exception:                                       # noqa: BLE001
        return ""


def service_failures(hours=24):
    """每个 embytg 单元在窗口内的失败次数与最后一条原因。"""
    units = [u.strip() for u in _sh(
        "systemctl list-units 'embytg-*' --all --no-legend --no-pager "
        "| awk '{print $1}'").splitlines() if u.strip()]
    out = []
    for u in units:
        txt = _sh("journalctl -u %s --since '-%dh' --no-pager -o cat" % (u, hours),
                  timeout=30)
        fails = FAIL_RE.findall(txt)
        if not fails:
            continue
        last = ""
        for line in reversed(txt.splitlines()):
            if "Failed with result" in line or "error" in line.lower():
                last = line.strip()[:160]
                break
        out.append({"unit": u, "fails": len(fails), "last": last})
    out.sort(key=lambda x: -x["fails"])
    return out


def runner_stats():
    """loop.log 里最近 24h 的退出码与原因分布。"""
    if not os.path.exists(RUNNER_LOG):
        return {"exists": False}
    cutoff = time.time() - 24 * 3600
    rc0 = rc1 = 0
    reasons = {}
    try:
        with open(RUNNER_LOG, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line[:2] != "20":
                    continue
                try:
                    ts = time.mktime(time.strptime(line[:19], "%Y-%m-%d %H:%M:%S"))
                except Exception:                            # noqa: BLE001
                    continue
                if ts < cutoff:
                    continue
                if "EXIT" in line and "rc=0" in line:
                    rc0 += 1
                elif "EXIT" in line and "rc=1" in line:
                    rc1 += 1
                m = REASON_RE.search(line)
                if m:
                    reasons[m.group(1)] = reasons.get(m.group(1), 0) + 1
    except Exception:                                       # noqa: BLE001
        return {"exists": True, "error": "read_failed"}
    return {"exists": True, "rc0": rc0, "rc1": rc1,
            "reasons": dict(sorted(reasons.items(), key=lambda x: -x[1])[:8])}


def main():
    as_json = "--json" in sys.argv
    quiet = "--quiet" in sys.argv
    svc = service_failures()
    run = runner_stats()

    # 阈值：单元失败 >0 或 runner rc=1 占比 >20% 视为需要处理
    total_fail = sum(x["fails"] for x in svc)
    rc1 = int(run.get("rc1") or 0)
    rc0 = int(run.get("rc0") or 0)
    bad_ratio = (rc1 / max(1, rc0 + rc1)) if (rc0 + rc1) >= 20 else 0.0
    problems = bool(total_fail) or bad_ratio > 0.2

    report = {"ts": int(time.time()), "services": svc, "runner": run,
              "problems": problems}

    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=1))
    elif not quiet or problems:
        print("== 近 24h 单元失败 ==")
        if not svc:
            print("   全部正常（0 个单元失败）")
        for x in svc[:8]:
            print("   %-32s %3d 次 | %s" % (x["unit"], x["fails"], x["last"][:70]))
        print("== runner 退出与原因 ==")
        print("   rc=0 %d | rc=1 %d | 失败占比 %.0f%%"
              % (rc0, rc1, bad_ratio * 100))
        _rs = run.get("reasons")
        if not isinstance(_rs, dict):
            _rs = {}
        for k, v in _rs.items():
            print("   %-24s %d" % (k, v))

    try:
        with open(STATE, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=1)
    except Exception:                                       # noqa: BLE001
        pass

    if problems:
        try:
            sys.path.insert(0, HERE)
            import alert as A
            top = ", ".join("%s×%d" % (x["unit"].replace(".service", ""), x["fails"])
                            for x in svc[:4]) or "runner"
            A.alert("warn", "timer_audit",
                    "定时任务日志有失败：%s（rc=1 %d/%d）" % (top, rc1, rc0 + rc1))
        except Exception:                                   # noqa: BLE001
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
