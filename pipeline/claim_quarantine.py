#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""claim_quarantine.py —— 把「收尾超限」的 claim 移出活跃集（run_one.sh RESUME_GIVE_UP 调用）。

## 为什么单独成脚本（2026-11-21 事故）

`run_one.sh` 的 RESUME_GIVE_UP 分支原来执行的是：

    mv /opt/embytg/pipeline/.gha_claims/$CLAIM_LANE.json  $QDIR/$CLAIM_RUN.json

但 2026-11-20 起 claim 已按 run 分文件：`<lane>.<run_id>.json`。源文件永远不存在，
`mv` 的失败又被 `|| true` 吞掉 —— 于是「隔离」是空操作：

  · claim 留在活跃集（status=failed），slot1 每 15 秒重新选中它；
  · `.resume_cycles` 从 5 一路涨到 260+，每轮都发一条 P1（channels 被 dedupe，
    但 alerts.jsonl 十几分钟涨了 250+ 行）；
  · new2 lane 永远不干新活。

判据：按 **run_id** 在 `.gha_claims/*.json`（含旧的 `<lane>.json`）里找，
找到就搬到 `.gha_claims_quarantine/<run_id>.json`，搬不动就非零退出让调用方知道。

用法：
    python3 claim_quarantine.py --run-id 20261120_184827-slot1-new2-fab2a9
"""
import argparse
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CLAIMS = os.path.join(HERE, ".gha_claims")
QUAR = os.path.join(HERE, ".gha_claims_quarantine")


def quarantine(run_id, claims=CLAIMS, quar=QUAR):
    """把 run_id 匹配的 claim 文件移进隔离目录，返回被移动的目标路径列表。"""
    run_id = str(run_id or "").strip()
    if not run_id:
        return []
    os.makedirs(quar, exist_ok=True)
    moved = []
    for path in sorted(glob.glob(os.path.join(claims, "*.json"))):
        try:
            with open(path, encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, ValueError):
            continue
        if not isinstance(d, dict) or str(d.get("run_id") or "") != run_id:
            continue
        dst = os.path.join(quar, run_id + ".json")
        os.replace(path, dst)
        moved.append(dst)
    return moved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--claim-dir", default=CLAIMS)
    ap.add_argument("--quar-dir", default=QUAR)
    a = ap.parse_args()
    try:
        moved = quarantine(a.run_id, a.claim_dir, a.quar_dir)
    except OSError as e:
        print("quarantine failed: %s" % e, flush=True)
        return 2
    if not moved:
        # 已经不在活跃集（被别的路径释放/收尾成功）也算正常：给调用方一个可判断的退出码。
        print("no active claim for %s" % a.run_id, flush=True)
        return 1
    print("quarantined %d file(s): %s" % (len(moved), ", ".join(moved)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
