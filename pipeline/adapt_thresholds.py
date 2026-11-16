#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""adapt_thresholds.py —— 阈值自适应（有护栏，只调"数据支持得动"的那几个）

用户授权后实现。原则：**能自动的自动，不能证明的保持固定并写明**。

自动调整（有数据 + 有护栏）
--------------------------
| 阈值 | 依据 | 上下限 |
|---|---|---|
| `disk_warn_gb` | 近 7 天磁盘可用量的线性斜率（GB/天）× 2 天缓冲 | 15–40 GB |
| `disk_crit_gb` | 同上 × 1 天缓冲 | 8–20 GB |

保持固定（写明原因，别假装自适应）
---------------------------------
- `claim_stale_min = 150`：由 embytg-runner.service 的 TimeoutStartSec=7200 推导（2h+30min 余量）；
  没有可靠的领取时间，**不瞎调**；可在 `.thresholds.json` 手工覆盖
- 告警去重窗口（P1 6h / P2 24h）：改动会直接影响你收信频率，保持固定
- `autoops` 每轮动作上限 5：防事故扩散，不随数据变

用法
----
    python3 adapt_thresholds.py            # 采样 + 重算（建议每天一次）
    python3 adapt_thresholds.py --show     # 看当前阈值与依据
调用点：06:10 巡检（daily_ops 之后）；消费方：autoops（磁盘）、runner_report（卡单）
"""
import argparse
import json
import os
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(HERE, ".disk_history.jsonl")
TH = os.path.join(HERE, ".thresholds.json")
# claim_stale_min 的推导（2026-11-13 从 720 改成 150）：
#   /etc/systemd/system/embytg-runner.service 的 TimeoutStartSec=7200（2 小时），
#   systemd 到点会直接 kill 整个 control-group。**所以任何 running 状态的 claim
#   超过 2 小时，在物理上必然已经没有进程在跑它** —— 这是可证明的上界，不是经验值。
#   留 30 分钟余量 → 150。原来的 720（12 小时）是因为当时以为"慢源单集可跑 4 小时"，
#   但服务根本不允许跑那么久；实测过一个 209 分钟的僵尸 claim 因此一直没被报出来，
#   那条 lane 空转了 3.6 小时（五分之一产能）。
DEFAULTS = {"disk_warn_gb": 20.0, "disk_crit_gb": 12.0, "claim_stale_min": 150}
CLAMP = {"disk_warn_gb": (15.0, 40.0), "disk_crit_gb": (8.0, 20.0)}


def sample():
    du = shutil.disk_usage("/")
    rec = {"ts": time.strftime("%F %T"), "free_gb": round(du.free / 1024 ** 3, 2)}
    with open(HIST, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


def slope_per_day(days=7):
    """用最近 N 天的采样拟合线性斜率（GB/天，负数=可用空间在减少）"""
    if not os.path.exists(HIST):
        return None, 0
    rows = []
    cutoff = time.time() - days * 86400
    for line in open(HIST, encoding="utf-8"):
        try:
            r = json.loads(line)
            ts = time.mktime(time.strptime(r["ts"], "%F %T"))
            if ts >= cutoff:
                rows.append((ts, r["free_gb"]))
        except Exception:                                          # noqa: BLE001
            pass
    if len(rows) < 4:
        return None, len(rows)
    # 最小二乘
    n = len(rows)
    mx = sum(t for t, _ in rows) / n
    my = sum(v for _, v in rows) / n
    num = sum((t - mx) * (v - my) for t, v in rows)
    den = sum((t - mx) ** 2 for t, _ in rows) or 1
    per_sec = num / den
    return per_sec * 86400, n


def compute():
    s = sample()
    sl, n = slope_per_day()
    out = dict(DEFAULTS)
    out["updated"] = s["ts"]
    out["free_gb"] = s["free_gb"]
    out["samples_7d"] = n
    if sl is None:
        out["basis"] = "样本不足（<4 次），沿用默认"
    else:
        consume = max(0.0, -sl)          # 每天消耗 GB
        warn = 2 * consume + 10
        crit = 1 * consume + 6
        for k, v in (("disk_warn_gb", warn), ("disk_crit_gb", crit)):
            lo, hi = CLAMP[k]
            out[k] = round(min(hi, max(lo, v)), 1)
        out["slope_gb_per_day"] = round(sl, 2)
        out["basis"] = "7 天斜率 %.2f GB/天（消耗 %.1f）→ warn=2x+10, crit=1x+6（含钳位）" % (sl, consume)
    out["claim_stale_min"] = DEFAULTS["claim_stale_min"]
    out["note"] = "claim_stale_min 固定：缺少可靠的 claim 领取时间，无法自适应（可手工覆盖）"
    json.dump(out, open(TH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return out


def load():
    if os.path.exists(TH):
        try:
            d = json.load(open(TH, encoding="utf-8"))
            out = dict(DEFAULTS)
            out.update({k: v for k, v in d.items() if k in DEFAULTS})
            return out
        except Exception:                                          # noqa: BLE001
            pass
    return dict(DEFAULTS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true")
    a = ap.parse_args()
    if a.show:
        print(json.dumps(load(), ensure_ascii=False, indent=1))
        return 0
    out = compute()
    print("== 阈值自适应 ==")
    print("  磁盘可用 %.1f GB｜7 天样本 %d 次" % (out["free_gb"], out["samples_7d"]))
    print("  %s" % out["basis"])
    print("  → disk_warn=%.1f GB  disk_crit=%.1f GB  claim_stale=%d min"
          % (out["disk_warn_gb"], out["disk_crit_gb"], out["claim_stale_min"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
