#!/usr/bin/env python3
"""预算闸门「按设计停」的历史区间 —— 唯一事实来源（E160/E161）。

为什么要有这个模块：runner 在领 claim 之前问 `budget_gate`，被拒就写一行
`BUDGET_WAIT ... reason=... wait=Ns` 然后睡 N 秒。任何「近 N 小时产出够不够」
的判据，只要窗口里跨了这种停，就必须把这段时间从分母里扣掉 ——
否则日切（00:00 额度重置）或预算打满后的恢复期，每天都会误报一次。
2026-11-17 00:24 的假 P1「109 分钟零出片」就是这么来的。

同一份事实只能有一个解析器：runner_report（产能判据）与 stall_watch（停摆判据）
都从这里取，不许再各写一套（E158 的教训）。
"""
import collections
import datetime
import os
import re
import time

HARD_STOP_REASONS = ("daily_exhausted", "month_exhausted")
DEFAULT_LOG = "/opt/embytg-runner/logs/runner.log"

_BW_RE = re.compile(
    r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) \[[^\]]+\] BUDGET_WAIT .*?"
    r"reason=([a-z_]+).*?wait=(\d+)s")


def log_path(path=None):
    return path or os.environ.get("TG_RUNNER_LOG") or DEFAULT_LOG


def _spans(window_sec, now=None, path=None, reasons=None, tail_lines=6000):
    """窗口内所有 BUDGET_WAIT 区间（已按并集合并）。读不到日志返回 None。"""
    now = float(now if now is not None else time.time())
    start = now - float(window_sec)
    try:
        with open(log_path(path), encoding="utf-8", errors="ignore") as f:
            lines = collections.deque(f, maxlen=tail_lines)
    except Exception:                                          # noqa: BLE001
        return None
    spans, hits = [], set()
    for ln in lines:
        m = _BW_RE.match(ln.strip())
        if not m:
            continue
        if reasons and m.group(2) not in reasons:
            continue
        try:
            t = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
        except ValueError:
            continue
        if t + int(m.group(3)) < start:                        # 整段都在窗口外
            continue
        a, b = max(t, start), min(t + int(m.group(3)), now)
        if b > a:
            spans.append((a, b))
            hits.add(m.group(2))
    if not spans:
        return {"secs": 0.0, "last_end": None, "reasons": []}
    spans.sort()
    merged = [list(spans[0])]
    for a, b in spans[1:]:
        if a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return {"secs": sum(b - a for a, b in merged),
            "last_end": round(now - merged[-1][1], 1),
            "reasons": sorted(hits)}


def blocked_window(window_sec, now=None, path=None, reasons=None):
    """近 window_sec 秒内闸门停了多少秒 + 最后一段结束距现在多久。"""
    return _spans(window_sec, now=now, path=path, reasons=reasons)


def blocked_between(start, end, path=None, reasons=None):
    """[start, end] 区间内闸门停了多少秒（用于「有效空闲」= 空闲 − 被挡）。"""
    start, end = float(start), float(end)
    if end <= start:
        return 0.0
    d = _spans(end - start, now=end, path=path, reasons=reasons)
    return float(d["secs"]) if d else 0.0
