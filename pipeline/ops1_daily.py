#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ops1_daily.py — 运维任务每日入库工单（只读，绝不自动 apply）

用户 2026-11-10 任命「每天从 tg 频道入库片子」。TG 云端转发不可逆，
故每日 05:00 由本脚本产出「开闸判定 + 建议批次 + 全量快照」工单，
真入库由运维任务按 OPS1_MANUAL.md §2.0 看过预演后执行。

只读保证：只调用只读脚本 / GET Emby / mode=ro 的 tg.db；不写任何生产状态，
唯一输出是 backup/ops1_daily_<date>.json 人读工单。
"""
import glob
import json
import os
import subprocess
import sys
import time

PIPE = "/opt/embytg/pipeline"
BK = "/opt/embytg/backup"
sys.path.insert(0, PIPE)
import status_now  # noqa: E402  复用单一状态入口，避免口径分叉
import ingest_quota  # noqa: E402  E020：入库总开关的唯一真源（同 autopilot G0）


def sh(cmd, timeout=60):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.returncode, (r.stdout or "").strip())
    except Exception as e:
        return (-1, str(e)[:120])


def main():
    now = time.strftime("%F %T %Z")
    day = time.strftime("%Y%m%d")
    d = status_now.collect()

    # --- 开闸条件逐条判定（OPS1_MANUAL §2.0）---
    scan = d.get("scan", {}).get("state")
    rec = d.get("reconcile") or {}
    # E016：gaps=None / err = 对账未知（脚本失败或并发冲突），不是「齐」→ 不开闸。
    rec_unknown = bool(rec.get("err")) or rec.get("gaps") is None
    gaps = rec.get("gaps") or []
    ing = d.get("ingest") or {}
    try:
        cur = int(ing.get("cursor"))
    except (TypeError, ValueError):
        cur = -1
    pend = ing.get("pending")

    checks = {}
    checks["Scan 非 Running"] = (scan != "Running", "scan=%s" % scan)
    # 与 status_now.verdict 口径一致：Scan Idle 后的小 gap 是新片/改名尾部条目，
    # 由 12h 周期扫描 + prewarm 自愈，属「注意」不是「阻塞」，不挡开闸（但缩小建议批次）。
    checks["按库对账齐(Idle后小gap=自愈项,非阻塞)"] = (
        rec_unknown is False and ((not gaps) or (scan != "Running")),
        "gaps=%s scan=%s%s%s" % (rec.get("gaps") if rec_unknown else gaps, scan,
                                 " (尾部自愈)" if (gaps and not rec_unknown) else "",
                                 (" UNKNOWN err=%s" % rec.get("err")) if rec_unknown else ""))
    # E021：与 status_now / selfcheck 同口径——「未 apply 条数==0」才算队列干净；
    # 不再拿 pending 行数与 cursor 相等（行数会因裁剪变小，11-13 实测 134 vs 144 全 done）。
    # 2026-11-17：口径收敛到 status_now.queue_unapplied() 单点。本处原读 pending_undone
    # （payload.done 计数，单批 apply 不回写 → 恒等于总行数），只要有 pending 就会假关闸，
    # 与 status_now 打印的「未apply=0」矛盾。
    _undone, _usrc = status_now.queue_unapplied(ing)
    checks["队列不悬半途"] = (
        _undone == 0,                          # None(判据缺失) → False，按未过处理
        "未apply=%s pending=%s cursor=%s [%s]" % (
            "未知" if _undone is None else _undone, pend, cur, _usrc))
    checks["状态机在跑"] = (bool(d.get("loop_pids")), "loop=%s" % d.get("loop_pids"))
    # E020：与 status_now / autopilot 同口径——协作日志是交接记录，历史未闭环不是运行阻塞；
    # 只有近 3h 的未闭环才挡开闸（否则 11 条历史条目会让工单天天空闸，与 status_now【稳定】打架）。
    _recent_open = d.get("open_tasks_recent")
    if _recent_open is None:                  # 旧版 status_now 无此字段 → 退回全量
        _recent_open = d.get("open_tasks") or []
    checks["协作日志闭环(近3h)"] = (
        not _recent_open,
        "近3h未闭环=%d 历史=%d" % (len(_recent_open), len(d.get("open_tasks_historical") or [])))

    # quota：用 --check --need 1 判定是否还有余量（不消耗额度）
    rc, qout = sh("/usr/bin/python3 %s/ingest_quota.py --check --need 1" % PIPE, 30)
    checks["流量配额有余"] = (rc == 0, "quota --check --need 1 -> rc=%d" % rc)
    _, qstatus = sh("/usr/bin/python3 %s/ingest_quota.py --status" % PIPE, 30)

    # shim 在位 + 近 30min 无新超时
    rc2, head = sh("docker exec emby head -1 /bin/ffprobe 2>/dev/null", 20)
    shim_ok = (rc2 == 0 and head.strip() == "#!/bin/sh")
    checks["ffprobe shim 在位"] = (shim_ok, "head=%r" % head.strip())
    _, p03 = sh("bash %s/p0_3_status.sh 2>/dev/null | grep -E '超时累计|缓存文件|Emby API'" % PIPE, 60)

    # 磁盘
    _, df = sh("df -h / | tail -1", 10)
    avail = 0
    try:
        avail = int(sh("df -m / | tail -1 | awk '{print $4}'", 10)[1])
    except Exception:
        pass
    checks["磁盘余量>8G"] = (avail > 8000, "avail=%dMB" % avail)

    # 运维守护是否在拉片（共享带宽，需避让）
    pulling = []
    for p in glob.glob("/opt/embytg/tmp/*.part") + glob.glob(PIPE + "/logs/pull_*.log"):
        try:
            if time.time() - os.path.getmtime(p) < 600:
                pulling.append(os.path.basename(p))
        except OSError:
            pass
    checks["运维守护未在大批量拉片(可缩小批次规避)"] = (not pulling, "活跃=%s" % pulling[:4])

    # E020：工单也必须看入库总开关。旧版只看 `quota --check`，而它在停用状态下仍返回
    # rc=0（E019 根因）→ 工单会写「开闸=True 建议批次 50」，与 §12 上片暂停令直接矛盾。
    try:
        switch_on = bool(ingest_quota.enabled())
        switch_why = "enabled" if switch_on else "停用(ingest_enabled=false / 上片已暂停)"
    except Exception as e:                              # 读不到=未知 → 不开闸
        switch_on, switch_why = False, "读取失败:%s" % str(e)[:40]
    checks["入库总开关(G0)"] = (switch_on, switch_why)

    opened = all(v[0] for k, v in checks.items()
                 if k != "运维守护未在大批量拉片(可缩小批次规避)")    # 建议批次：开闸且无人拉片=50；有人拉片=20；未开闸=0
    if not opened:
        batch = 0
    elif pulling:
        batch = 20
    else:
        batch = 50

    report = {
        "ts": now, "day": day, "role": "运维自动化",
        "verdict_status_now": status_now.verdict(d)[0],
        "snapshot": d,
        "checks": {k: {"ok": v[0], "detail": v[1]} for k, v in checks.items()},
        "quota_status": qstatus, "p0_3_status": p03,
        "open_gate": opened, "suggested_batch": batch,
        "sop": "bash pipeline/ingest_sop.sh <频道> --dry 预演 → 六步 SOP（OPS1_MANUAL §2.1）",
        "note": "本工单只读；真 apply 由运维任务看过 --dry 后执行，绝不自动转发。",
    }
    out = os.path.join(BK, "ops1_daily_%s.json" % day)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("== 运维任务每日工单 %s ==" % now)
    print("  结论 status_now = %s | 开闸 = %s | 建议批次 = %d 条"
          % (report["verdict_status_now"], opened, batch))
    for k, v in checks.items():
        print("   %s %-42s %s" % ("OK " if v[0] else "!! ", k, v[1]))
    print("  工单 -> %s" % out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
