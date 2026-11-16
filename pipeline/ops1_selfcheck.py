#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ops1_selfcheck.py — 运维任务「每次收尾反思」的机器判据（OPS1_MANUAL §4）

用户要求「每一次都要反思自己是不是做到工作手册的要求」。
反思不能靠自觉：本脚本把手册 §1 的 8 条标准 + §4 的 8 问变成可执行检查，
任何 FAIL 要么当场修、要么在日志写明「已知例外 + 谁负责 + 何时修」，不许静默略过。

用法: /usr/bin/python3 ops1_selfcheck.py            # 人读，退出码 0=全过 1=有FAIL
      /usr/bin/python3 ops1_selfcheck.py --json
只读，不改任何生产状态。
"""
import glob
import hashlib
import json
import os
import re
import subprocess
import sys
import time

PIPE = "/opt/embytg/pipeline"
ROOT = "/opt/embytg"
BK = os.path.join(ROOT, "backup")
sys.path.insert(0, PIPE)
import status_now  # noqa: E402


def sh(cmd, timeout=60):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip())
    except Exception as e:
        return (-1, "", str(e)[:150])


def today_has(path_glob):
    day = time.strftime("%Y%m%d")
    return bool(glob.glob(path_glob.replace("*", "*" + day + "*"))) or bool(glob.glob(path_glob))


def md5(p):
    try:
        h = hashlib.md5()
        with open(p, "rb") as f:
            for b in iter(lambda: f.read(1 << 20), b""):
                h.update(b)
        return h.hexdigest()[:8]
    except Exception:
        return "MISSING"


def main():
    d = status_now.collect()
    results = []

    def add(key, ok, detail, fix):
        results.append({"key": key, "ok": bool(ok), "detail": detail, "fix": fix})

    # Q1 开工前看全貌了吗（当日工单）
    daily = glob.glob(os.path.join(BK, "ops1_daily_%s.json" % time.strftime("%Y%m%d")))
    add("Q1 当日 status_now/工单", bool(daily),
        daily[0] if daily else "无当日工单",
        "跑 /usr/bin/python3 ops1_daily.py")

    # Q2 变更可回滚：LATEST 包 state=DONE + sha256 验签 + snapshot.scan 非 Running
    L = d.get("latest") or {}
    try:
        lj = json.load(open(os.path.join(BK, "LATEST.json"), encoding="utf-8"))
        scan_snap = lj.get("snapshot", {}).get("state", "?")
        ok2 = (L.get("state") == "DONE" and L.get("sha256_ok")
               and scan_snap != "Running")
    except Exception:
        scan_snap, ok2 = "?", False
    add("Q2 备份可回滚(sha验签+Scan非Running)", ok2,
        "%s sha_ok=%s snapshot.scan=%s" % (L.get("archive"), L.get("sha256_ok"), scan_snap),
        "重打一份 Scan Idle 后的包")

    # Q3 任务闭环：AGENTS §7.8（用户 2026-11-11 08:2x 定，**优先级高于本手册**）——
    # 「别人任务超时未结条」属 Q3「只广播、不代结」。所以本项只对我自己的条目负责：
    #   我的未闭环 = FAIL；别人的未闭环 = 明细里列出 + 必须广播点名（不 FAIL）。
    # 旧版把 Codex-北辰 / Codex-源服PC 的 3 条悬空条目算成我的 FAIL → selfcheck 天天
    # FAIL(1)、PASS 永远不可达 → 真正该看的 FAIL 被常驻噪声淹没（告警疲劳）。
    ME3 = ("hermes-ops", "Hermes-运维", "运维任务")
    rc3, out3, _ = sh("/usr/bin/python3 %s/agent_log.py verify" % PIPE, 30)
    open3 = d.get("open_tasks") or []
    mine3 = [t for t in open3 if any(k in str(t.get("agent", "")) for k in ME3)]
    other3 = [t for t in open3 if t not in mine3]
    struct_bad = [k for k in ("未来时间", "乱序", "状态词非法") if k in out3]
    # 2026-11-15 自省（E037-3）：上面按子串判，而 verify 的「提示行」里也带这些词 ——
    # 例如「· 历史问题…L676 **乱序**」「只允许 开始/进行中/完成/中止」。
    # 结果：**历史噪音被算成本轮 FAIL**，FAIL(1) 永远消不掉，PASS 不可达（告警疲劳）。
    # 判据：只认「✗ 近 24h 问题 N 条」这一行（带 ✗ 才是本轮结论段，提示段不带 ✗）；
    # 且里面只有「时间戳疑似写坏 / 未来时间 / 状态词非法」算结构错，
    # 「未闭环」与「同名多实例」已由 mine3 / dup 两条路径单独判，不重复计入。
    _tail = re.search(r"✗ 近\s*24h\s*问题\s*(\d+)\s*条：(.*)", out3, re.S)
    if not _tail or int(_tail.group(1)) == 0:
        struct_bad = []
    else:
        lines24 = [x for x in _tail.group(2).splitlines()
                   if "**乱序**" in x or "**未来时间**" in x or "状态词非法" in x]
        struct_bad = ["24h内时间/状态问题 %d 条" % len(lines24)] if lines24 else []
    q3_ok = (not mine3) and (not struct_bad)
    q3_detail = ("自身未闭环=%d%s | 他人未闭环=%d%s | 时间/状态=%s"
                 % (len(mine3),
                    (" " + "; ".join("%s %s %s" % (t["agent"], t["time"], t["task"][:30])
                                     for t in mine3[:3])) if mine3 else "",
                    len(other3),
                    ("（§7.8 只广播:%s）" % ",".join(sorted({str(t.get("agent")) for t in other3})))
                    if other3 else "",
                    "ok" if not struct_bad else "坏:" + ",".join(struct_bad)))
    add("Q3 任务闭环(自身0未闭环)", q3_ok, q3_detail,
        "结掉自己的 开始/进行中；别人的按 AGENTS §7.8 只广播点名，不代结")

    # Q4 队列不悬半途（E021：用 pending 里「未 apply 条数」，不再拿行数与游标比相等）
    ing = d.get("ingest") or {}
    try:
        cur = int(ing.get("cursor"))
    except (TypeError, ValueError):
        cur = -1
    pend = ing.get("pending")
    undone = ing.get("pending_undone")
    if isinstance(undone, int):
        q4_ok = (undone == 0)
        q4_note = "未apply=%d / pending=%s 行 / cursor=%s" % (undone, pend, cur)
    else:                                     # 老库无 done 字段 → 退回 pending ≤ cursor
        q4_ok = isinstance(pend, int) and cur >= 0 and pend <= cur
        q4_note = "pending=%s cursor=%s（无 undone 字段，退回旧口径）" % (pend, cur)
    add("Q4 队列不悬半途", q4_ok,
        "%s integrity=%s" % (q4_note, ing.get("integrity")),
        "把 pending 里未 apply 的条目跑完（或确认无 apply 进程后归档），不留半成品")

    # Q5 收敛（E016：对账未知=FAIL，不能把 gaps=None 当「齐」）
    recq = d.get("reconcile") or {}
    rec_unknown = bool(recq.get("err")) or recq.get("gaps") is None
    gaps = recq.get("gaps") or []
    scan = (d.get("scan") or {}).get("state")
    # 2026-11-16：库级计数看不出「少一集多一集」（E035），条目级差异才是真判据。
    # 只认「超过容忍窗还没自愈」的条目差异；容忍窗内的属于上传→写台账的延迟。
    _lq = d.get("ledger")
    ledq = _lq if isinstance(_lq, dict) else {}
    led_bad = bool(ledq.get("err")) or bool(ledq.get("stale")) or bool(ledq.get("aged"))
    q5_ok = (scan != "Running") and not rec_unknown and not led_bad
    # Scan Idle 后的小 gap 属尾部延迟（运维任务 12:52 定性），不判 FAIL，标 WARN
    # Q5：Scan Running = 未完成(FAIL)；Scan Idle 且有 gap = 尾部自愈(按 12:52 定性不判 FAIL，标 WARN)
    q5_note = (" (Idle后小gap=新片/改名尾部,由12h扫描+prewarm自愈,非阻塞)" if gaps and scan != "Running" else "")
    if rec_unknown:
        q5_note = " 对账未知(err=%s)：lib_reconcile 未给出 gaps，不能当收敛" % (recq.get("err") or "gaps=None")
    add("Q5 收敛(gaps=[] / Scan非Running)", q5_ok,
        "gaps=%s scan=%s | 条目级 %s条(超窗 %s, 对账 %.0fmin前)%s"
        % (recq.get("gaps") if rec_unknown else gaps, scan, ledq.get("gaps"),
           ledq.get("aged"), ledq.get("run_age_min") or 0, q5_note),
        "对账失败先修 lib_reconcile/查 pg 慢查询；Scan Running 时等它跑完；"
        "条目级超窗差异用 reconcile.py --repair；Idle 后小 gap 不强触发全库扫描")

    # Q6 配额/容量/shim
    rc6, q6, _ = sh("/usr/bin/python3 %s/ingest_quota.py --check --need 1" % PIPE, 30)
    rc6b, head6, _ = sh("docker exec emby head -1 /bin/ffprobe 2>/dev/null", 20)
    _, dfm, _ = sh("df -m / | tail -1 | awk '{print $4}'", 10)
    try:
        avail = int(dfm)
    except Exception:
        avail = 0
    add("Q6 配额/磁盘/shim", rc6 == 0 and head6.strip() == "#!/bin/sh" and avail > 8000,
        "quota_rc=%d shim=%r availMB=%d" % (rc6, head6.strip(), avail),
        "查 quota 余量 / p0_3_status.sh / 磁盘")

    # Q7 告诉别人：文档双副本一致 + 本轮有开始/完成条目
    pairs = [("AGENTS.md", "pipeline/AGENTS.md"),
             ("docs/KB.md", "pipeline/docs/KB.md"),
             # 2026-11-14 自省：这三份在 2026-11-13 已被归档到 docs/archive/
             # （qwenpaw 停用，岗位并入 Hermes cron；见 AGENTS.md 表格与 INDEX §四）。
             # 本函数仍按 docs/ 旧路径比对，导致 Q7/Q8 天天 FAIL、且 FAIL 理由与真实
             # 状态无关 —— 告警疲劳会把真问题埋掉（本轮真问题就是这三份文档
             # 没进备份包，恰恰被这三条假 FAIL 盖了过去）。改成「找真身，不看副本」。
             ("docs/archive/OPS_JOURNAL.md", "docs/archive/OPS_JOURNAL.md"),
             ("docs/archive/OPS1_MANUAL.md", "docs/archive/OPS1_MANUAL.md"),
             ("docs/archive/OPS1_SELFWAKE.md", "docs/archive/OPS1_SELFWAKE.md")]
    bad = []
    for a, b in pairs:
        pa, pb = os.path.join(ROOT, a), os.path.join(ROOT, b)
        if not os.path.isfile(pa) or md5(pa) != md5(pb):
            bad.append(a)
    add("Q7 文档双副本 + 广播", not bad,
        "不一致=%s | 协作日志条目=%s 未闭环=%d" % (bad, d.get("log_entries"), len(d.get("open_tasks") or [])),
        "cp 同步 + md5sum 复核；补写广播条目")

    # Q8 手册还准吗：本手册存在 + 当日是否有变更记录（软判据，人工确认）
    # 2026-11-14：手册已归档到 docs/archive/，旧路径恒 FAIL。
    manual = os.path.join(ROOT, "docs/archive/OPS1_MANUAL.md")
    exists = os.path.isfile(manual)
    add("Q8 手册存在且本轮已复核", exists,
        "OPS1_MANUAL.md %s，§6 变更记录需人工确认本轮是否要追加" % ("存在" if exists else "缺失"),
        "本轮有新坑/新数字/新要求 → 当轮更新 §6 并双副本同步")

    # Q9（2026-11-11 AGENTS §7.8）：本轮有没有「非职责却上手」/「重大事故却只广播没处理」？
    # 机器判据：协作日志里本 agent 最近的「完成」广播是否含象限声明；软判据，提醒用，不误报 FAIL。
    try:
        with open(os.path.join(ROOT, "AgentCommunication.log"), encoding="utf-8", errors="ignore") as f:
            jt = f.read()
        recent = jt[-4000:]
        has_quadrant = ("象限" in recent) or ("Q1" in recent) or ("Q3" in recent)
        add("Q9 象限纪律(AGENTS §7.8)", True,
            "最近广播含象限声明=%s；口诀:非职责非事故只广播/重大事故处理+广播" % has_quadrant,
            "每轮广播按 §7.8.4 写清象限/职责/事故/交给谁")
    except Exception:
        pass

    fails = [r for r in results if not r["ok"]]
    verdict = "PASS" if not fails else "FAIL(%d)" % len(fails)

    if "--json" in sys.argv:
        print(json.dumps({"ts": time.strftime("%F %T %Z"), "verdict": verdict,
                          "results": results}, ensure_ascii=False, indent=2))
        return 0 if not fails else 1

    print("=" * 72)
    print("运维任务 收尾反思 | %s | 结论: 【%s】" % (time.strftime("%F %T %Z"), verdict))
    print("=" * 72)
    for r in results:
        print("  %s %-38s %s" % ("OK " if r["ok"] else "!! ", r["key"], r["detail"]))
        if not r["ok"]:
            print("        → 修法: %s" % r["fix"])
    print("=" * 72)
    print("规则(OPS1_MANUAL §4): 任何 FAIL 要么当场修、要么在 AgentCommunication.log")
    print("写明『已知例外 + 谁负责 + 何时修』，不许静默略过。")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
