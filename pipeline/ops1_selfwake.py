#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ops1_selfwake.py — AI 每日自醒的「上下文简报」（只读，给被定时唤起的 agent 看）

用户 2026-11-10 原话：「你需要想办法自醒，我需要借助 ai 来无人值守，全靠脚本可能靠不住，
所以你每天应该还有一个任务，就是优化自己手里的脚本，提高自己的效率，让自己越来越强大」。

【为什么需要它】定时唤起的是一个**全新会话**，没有本次对话记忆。若只发「优化脚本」四个字，
它会瞎改。本脚本把「过去 24h 发生了什么 / 哪些脚本在跑 / 哪里失败过 / 手册待办 / 上次自醒改了什么」
聚合成一页 Markdown，作为自醒 prompt 的上下文锚点 —— 让 AI 用推理能力审查并改造自己，
而不是再写一个 cron 脚本假装自省。

【只读保证】只读 logs / json / md / git status；不写生产状态，唯一输出是
backup/ops1_selfwake_<date>.md（简报）+ stdout（可直接贴进自醒 prompt）。

用法:
  /usr/bin/python3 ops1_selfwake.py            # 生成简报
  /usr/bin/python3 ops1_selfwake.py --print   # 只打印不落盘
"""
import glob
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

ROOT = "/opt/embytg"
PIPE = os.path.join(ROOT, "pipeline")
BK = os.path.join(ROOT, "backup")
LOGS = os.path.join(PIPE, "logs")

SCRIPTS = ["ops1_autopilot.py", "ops1_daily.py", "ops1_selfcheck.py",
           "status_now.py", "prewarm_daemon.py", "agent_log.py",
           "ingest_sop.sh", "tgpipe.py", "nfo_gen.py", "lib_reconcile.py",
           "auto_finalize.py"]

CRONS = ["/etc/cron.d/embytg_ops1_autopilot", "/etc/cron.d/embytg_ops1_daily",
         "/etc/cron.d/embytg_prewarm", "/etc/cron.d/embytg_tgdb_daily",
         "/etc/cron.d/p03_ffprobe_shim"]


def sh(cmd, timeout=30):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.stdout or "").strip()
    except Exception:
        return ""


def read(p, n=100000):
    try:
        return open(p, encoding="utf-8", errors="ignore").read()[:n]
    except Exception:
        return ""


def last_jsonl(path, since_h=24):
    """取审计日志里近 since_h 小时的记录。"""
    cutoff = datetime.now() - timedelta(hours=since_h)
    out = []
    try:
        for l in open(path, encoding="utf-8", errors="ignore"):
            try:
                d = json.loads(l)
                ts = d.get("ts", "")[:19]
                t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                if t >= cutoff:
                    out.append(d)
            except Exception:
                pass
    except Exception:
        pass
    return out


def main():
    today = time.strftime("%Y%m%d")
    now = time.strftime("%F %T %Z")

    # 1) 过去 24h 的 autopilot 审计摘要
    audits = last_jsonl(os.path.join(LOGS, "ops1_audit.jsonl"), 24)
    cycles = len([a for a in audits if a.get("phase") == "留痕"])
    decisions = [a for a in audits if a.get("phase") == "分析"]
    execs = [a for a in audits if a.get("phase") == "执行"]
    fails = [a for a in execs if any(k in str(a.get("result", "")) for k in
             ("失败", "FAIL", "熔断", "TIMEOUT", "PARTIAL", "rc=-", "rc=1"))]
    # E019：L3 的动作**归属在 action 字段**，不在 decision。真灌那一步的 decision 是
    # 「自动入库判定通过 → 执行六步SOP」（不含 "L3"）→ 旧过滤条件永远匹配不到，
    # 把 2026-11-10 14:46:11 那次 result=「L3 真入库 OK」的**真灌**统计成 0 次。
    # 后果比漏报更糟：简报天天告我「L3 真入库 0 次」，我就拿这个假事实去质疑闸门
    # （昨天 E017/E018 都是这么被激发的）。现在按 action 归属，并区分 真灌/预演/跳过。
    l3_exec = [a for a in execs if str(a.get("action", "")) == "L3"]
    l3_runs = [a for a in l3_exec if "真入库" in str(a.get("result", ""))]
    l3_dry = [a for a in l3_exec
              if a not in l3_runs
              and ("预演" in str(a.get("result", "")) or "DRY" in str(a.get("result", "")))]
    l3_skip = [a for a in l3_exec if a not in l3_runs and a not in l3_dry]
    # E177：只统计 L3 会让「L1/L2 也被冻住」完全看不见 —— 2026-11-11~18 的六天里，
    # 简报每天只说「L3 真入库 0 次」，而 L2 的 96 轮 [DRY] 与 L1 的假「disk_guard 完成」
    # 一次都没出现在简报上，我因此审了五天都没发现自愈停摆。三层各自的真/DRY 必须同屏。
    def _real_dry(action, alert_prefix=None):
        rows = [a for a in execs if str(a.get("action", "")) == action]
        if alert_prefix:
            rows = [a for a in rows if not str(a.get("result", "")).startswith(alert_prefix)]
        dry = [a for a in rows if str(a.get("result", "")).lstrip().startswith("[DRY]")
               or "DRY（未执行" in str(a.get("result", ""))]
        return len(rows) - len(dry), len(dry)

    l1_real, l1_dry = _real_dry("L1", alert_prefix="只告警")
    l2_real, l2_dry = _real_dry("L2")
    skipped_gates = []
    for a in decisions:
        m = re.search(r"'未过=\[(.*?)\]'|\"未过=\[(.*?)\]\"", str(a.get("decision", "")))
        if m:
            skipped_gates.append((a.get("ts", "")[:16], (m.group(1) or m.group(2) or "")[:80]))

    # 2) 脚本健康：py_compile 全过？最近改动？行数？
    # .sh 用 sh -n，不能混进 py_compile（13:28 实测误报 SyntaxError）
    py_scripts = [s for s in SCRIPTS if s.endswith(".py")]
    sh_scripts = [s for s in SCRIPTS if s.endswith(".sh")]
    pyc = sh("cd %s && /usr/bin/python3 -m py_compile %s 2>&1 && echo ALL_OK"
             % (PIPE, " ".join(py_scripts)), 60)
    shc = sh("cd %s && %s && echo SH_OK"
             % (PIPE, " && ".join("sh -n " + s for s in sh_scripts)), 30)
    if "SH_OK" not in shc:
        pyc = (pyc + " | sh -n FAIL: " + shc[:120]).strip()
    changed = []
    for s in SCRIPTS:
        p = os.path.join(PIPE, s)
        if not os.path.isfile(p):
            continue
        age_h = (time.time() - os.path.getmtime(p)) / 3600
        if age_h < 24:
            changed.append("%s(%.1fh前, %d行)" % (s, age_h, len(read(p).splitlines())))
    lines = {s: len(read(os.path.join(PIPE, s)).splitlines()) for s in SCRIPTS
             if os.path.isfile(os.path.join(PIPE, s))}

    # 3) cron 清单（哪些在自跑）
    cron_lines = []
    for c in CRONS:
        for l in read(c).splitlines():
            if re.match(r"^\s*[\d*/]", l):
                cron_lines.append("%s: %s" % (os.path.basename(c), l.strip()[:110]))

    # 4) 手册/文档：上次自醒改了什么、待办
    # 2026-11-14：手册 2026-11-13 已归档到 docs/archive/。旧路径 read() 恒返回空串，
    # 结果简报 §5「手册变更记录」天天是空的 —— 跨会话连续性（OPS1_SELFWAKE §0 第④层）
    # 就此静默断掉，比报错更危险。取真身，找不到就明说，不装正常。
    _manual_p = os.path.join(ROOT, "docs/archive/OPS1_MANUAL.md")
    if not os.path.isfile(_manual_p):
        _manual_p = os.path.join(ROOT, "docs/OPS1_MANUAL.md")   # 万一哪天又迁回
    manual = read(_manual_p)
    # E019：原写死 "| 2026-11-10" → 只有 11-10 那天的变更记录会出现在简报里，
    # 之后我每天追加的 §6 行**永远不会被下一轮自醒看到**（跨会话记忆断在这里）。
    v_rows = [l.strip() for l in manual.splitlines() if l.startswith("| 2026-")]
    # E019 的同类问题（2026-11-13 补）：这份报告同样是按日期落盘的
    # docs/NFO_GAPS_<date>.md，写死 20261110 就永远读四天前的缺口。
    # 与 status_now.py 保持同一取法：按文件名倒序取最新一份。
    import glob as _glob
    _nfo = sorted(_glob.glob(os.path.join(PIPE, "docs/NFO_GAPS_*.md")))
    gaps_md = read(_nfo[-1]) if _nfo else ""
    m_gap = re.search(r"合计\s*(\d+)\s*条", gaps_md)

    # 5) 最近一次 selfcheck / status_now 结论
    last_sc = [a for a in audits if a.get("phase") == "留痕"][-1:]  # 占位
    sc = sh("cd %s && /usr/bin/python3 ops1_selfcheck.py --json" % PIPE, 200)
    try:
        sc_verdict = json.loads(sc[sc.find("{"):]).get("verdict")
        sc_fails = [r["key"] for r in json.loads(sc[sc.find("{"):])["results"] if not r["ok"]]
    except Exception:
        sc_verdict, sc_fails = "?", []

    # 6) git 未提交改动（避免自醒时覆盖别人/自己的半成品）
    gs = sh("cd %s && git status --short" % ROOT, 20).splitlines()

    # 7) 磁盘/配额
    disk = sh("df -h / | tail -1", 10)
    quota = sh("cd %s && /usr/bin/python3 ingest_quota.py --status 2>&1 | head -5" % PIPE, 30)

    # ---------- 组装简报 ----------
    L = []
    L.append("# AI 每日自醒简报 | %s | 运维自动化" % now)
    L.append("")
    L.append("> 本简报由 `ops1_selfwake.py` 只读生成，是**被定时唤起的全新会话**的上下文锚点。")
    L.append("> 你的任务不是「执行脚本」，而是**用推理能力审查并改造自己手里的脚本**：")
    L.append("> 读下面事实 → 判断哪个环节低效/会失效/口径矛盾 → 亲手改代码或手册 → 验证 → 写变更记录 → 广播。")
    L.append("")
    L.append("## 1. 过去 24h 闭环实况")
    L.append("- autopilot 轮次 **%d**，决策 %d 条，执行 %d 条，疑似失败/熔断 **%d** 条"
             % (cycles, len(decisions), len(execs), len(fails)))
    L.append("- **L3 真入库 %d 次**（最近 %s）/ 预演 %d 次 / 其它跳过 %d 次"
             "（按 `action==L3` 归属统计；E019 前这一行恒为 0）"
             % (len(l3_runs), (l3_runs[-1].get("ts", "")[:16] if l3_runs else "无"),
                len(l3_dry), len(l3_skip)))
    for r in l3_runs[-3:]:
        L.append("  - `[%s] %s`" % (r.get("ts", "")[:16], str(r.get("result", ""))[:120]))
    L.append("- **L1/L2 执行面**（E177：dry 不许冒充产出）: L1 真动作 **%d** / DRY %d；"
             "L2 真跑 **%d** / DRY %d%s"
             % (l1_real, l1_dry, l2_real, l2_dry,
                "  ⚠ **L2 整段 DRY = 刮削停摆**" if l2_dry and not l2_real else ""))
    if fails:
        for f in fails[:5]:
            L.append("  - ⚠ `[%s] %s → %s`" % (f.get("ts", "")[:16],
                     str(f.get("decision", ""))[:60], str(f.get("result", ""))[:90]))
    else:
        L.append("  - 无失败记录")
    if skipped_gates:
        L.append("- **被闸门挡住的历史**（重点审查：是保护正确，还是闸门过严导致该干的没干？）:")
        for t, g in skipped_gates[-5:]:
            L.append("  - `%s` 未过: %s" % (t, g))
    L.append("")
    L.append("## 2. 我手里的脚本（今天的优化对象）")
    L.append("- `py_compile`: **%s**" % ("ALL_OK" if "ALL_OK" in pyc else "FAIL: " + pyc[:200]))
    L.append("- 近 24h 被改过: %s" % (", ".join(changed) if changed else "无（说明今天还没优化过任何脚本）"))
    L.append("- 行数（找过度膨胀/该拆分的）: %s" %
             ", ".join("%s=%d" % (k, v) for k, v in sorted(lines.items(), key=lambda x: -x[1])[:8]))
    L.append("")
    L.append("## 3. 正在自跑的定时任务")
    for c in cron_lines:
        L.append("- `%s`" % c)
    L.append("")
    L.append("## 4. 当前健康 / 待办")
    L.append("- `ops1_selfcheck`: **%s**%s" % (sc_verdict, (" FAIL项=" + str(sc_fails)) if sc_fails else ""))
    L.append("- NFO 缺口: %s 条（KB §6 不猜标题的多数属已知可接受）" % (m_gap.group(1) if m_gap else "?"))
    L.append("- 磁盘: `%s`" % disk)
    L.append("- 配额: %s" % quota.replace("\n", " / ")[:220])
    L.append("- git 未提交改动 %d 项（**改前先看，别 `git add -A`**）:" % len(gs))
    for g in gs[:12]:
        L.append("  - `%s`" % g[:110])
    L.append("")
    L.append("## 5. 手册变更记录（自醒的连续性）")
    for r in v_rows[-4:]:
        L.append("- %s" % r[:200])
    L.append("")
    L.append("## 5.5 动手前先判象限（AGENTS §7.8，适用所有 agent，优先级最高）")
    L.append("> 非职责+非重大事故=**只广播不动手**；重大事故(不论谁的职责)=**先处理并广播**；拿不准按重大事故。")
    L.append("> Q1 重要+紧急=马上处理+广播 / Q2 重要不紧急=纳入职责计划 / Q3 不重要但紧急=只广播点名负责人 / Q4=不做不刷屏。")
    L.append("> 重大事故 6 条判据见 §7.8.3；本轮若发现别人的未闭环/报错，属 Q3，只广播不代上手。")
    L.append("")
    L.append("## 6. 今日自醒必须回答的问题（写完答案再动手）")
    L.append("1. 过去 24h 里，哪个决策是**错的或低效的**？根因在脚本逻辑还是我的判断？")
    L.append("2. 有没有「脚本一直在跑但其实没产出」的假闭环？（看 L3 真入库次数 + dry 占比）")
    L.append("3. 哪个脚本口径与另一个脚本矛盾？（历史上真出过：status_now【稳定】vs ops1_daily【不开闸】）")
    L.append("4. 磁盘/性能/失败模式里，有没有能提前一步自愈而现在只能告警的？")
    L.append("5. 手册哪句话已经和代码不符？**当轮修**（§5 持续改进）。")
    L.append("6. 今天要改的**一个**最高价值脚本是什么？改完 `py_compile` + 实跑 + 写 §6 变更记录 + 广播。")
    L.append("")
    L.append("## 7. 硬边界（自醒时也不许跨）")
    L.append("- 不猜标题 / 不在 Scan Running 时触发刷新或重启 / 不无冷快照改 tg.db /")
    L.append("  不碰运维守护源服链路与近 6h 在写的 .part / 不 `git add -A` / 不手写时间戳。")
    L.append("- 改生产脚本前留 `.bak`；改完 `py_compile` + 至少一次实跑；重大动作写 `AgentCommunication.log`。")
    L.append("- 若本轮判断「无需改动」，也要在 OPS_JOURNAL 写一条「审查通过、无变更」并说明依据，")
    L.append("  **不能静默跳过自醒**（否则和纯脚本没区别）。")
    L.append("")
    L.append("---")
    L.append("审计原文: `pipeline/logs/ops1_audit.jsonl`；手册: `docs/archive/OPS1_MANUAL.md`（已归档）；")
    L.append("状态: `status_now.py`；反思: `ops1_selfcheck.py`。")

    brief = "\n".join(L) + "\n"
    if "--print" not in sys.argv:
        out = os.path.join(BK, "ops1_selfwake_%s.md" % today)
        with open(out, "w", encoding="utf-8") as f:
            f.write(brief)
        print("简报 -> %s (%d 行)" % (out, len(L)))
        print("=" * 60)
    print(brief)
    return 0


if __name__ == "__main__":
    sys.exit(main())
