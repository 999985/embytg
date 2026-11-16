#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""self_audit.py —— 每日自审：把自己的日志变成「改进清单」

用户 2026-11-10：「你需要想办法自醒……全靠脚本可能靠不住，你每天应该还有一个任务，
就是优化自己手里的脚本，提高自己的效率，让自己越来越强大。」

它读自己的运行痕迹，回答三个问题：
  1. **哪里在漏**：成功率、失败原因分布（HTTP 404 / 无此剧 / 超时 / 预算拦截）
  2. **哪里在磨**：每轮耗时、单集下载速度、重复劳动（同一集反复失败）
  3. **该先修什么**：按「影响 × 频次 ÷ 成本」出 Top-5 清单，落到 `.self_backlog.json`

输出：
  - `pipeline/logs/self_review_<date>.md`（当天自审报告）
  - `pipeline/.self_backlog.json`（带优先级的待办，autoops 每轮读一次提示）

只读日志，不改任何生产状态。
"""
import argparse
import collections
import json
import os
import re
import statistics
import subprocess
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs")
OUT_MD = os.path.join(LOGS, "self_review_%s.md" % time.strftime("%Y%m%d"))
BACKLOG = os.path.join(HERE, ".self_backlog.json")


def tail_jsonl(name, n=5000):
    p = os.path.join(LOGS, name)
    out = []
    if not os.path.exists(p):
        return out
    for line in open(p, encoding="utf-8", errors="ignore").read().strip().splitlines()[-n:]:
        try:
            out.append(json.loads(line))
        except Exception:                                          # noqa: BLE001
            pass
    return out


def classify(tail):
    if not tail:
        return "未知"
    # 新：source_pull 的机器标记优先（2026-11-11）。此前 no_source 的 tail
    # 里带有多源搜索的 "HTTP Error 404"，被误判成 404，导致改进清单指向错方向。
    if "RESULT=no_source_series" in tail:
        return "源没有这部剧（整部不在任何源 → 需 TG 补源）"
    if "RESULT=no_source_episode" in tail:
        return "无源可提供（源有剧但缺这一集 → 需 TG 补源）"
    if "RESULT=direct_link_fail" in tail:
        return "源直链 404（元数据在、文件不在）"
    if "RESULT=budget_blocked" in tail:
        return "被预算闸门拦下"
    if "没有源提供这一集" in tail or "没有源提供" in tail:
        return "无源可提供（源有剧但缺这一集 → 需 TG 补源）"
    if "无此剧" in tail:
        return "源没有这部剧（整部不在任何源 → 需 TG 补源）"
    if "所有源都失败（直链不可用）" in tail:
        return "源直链 404（元数据在、文件不在）"
    if "HTTP Error 404" in tail:
        return "源直链 404（元数据在、文件不在）"
    if "HTTP Error 401" in tail or "HTTP Error 403" in tail:
        return "鉴权失效/被封"
    if "无 E" in tail:
        return "源该季缺这一集"
    if "超限" in tail:
        return "被预算闸门拦下"
    if "搜索失败" in tail:
        return "源搜索接口异常"
    if "timeout" in tail.lower() or "timed out" in tail.lower():
        return "超时"
    return "其它"


def review(days=7):
    """7 天回归复查：指标趋势 + 这段时间改过哪些脚本（验证"进化"是否真的有效）"""
    import glob
    tr = []
    for f in sorted(glob.glob(os.path.join(LOGS, "self_review_*.md")))[-days:]:
        try:
            txt = open(f, encoding="utf-8").read()
            m = re.search(r"成功率 \*\*([\d.]+)%", txt)
            d = os.path.basename(f).replace("self_review_", "").replace(".md", "")
            if m:
                tr.append((d, float(m.group(1))))
        except Exception:                                          # noqa: BLE001
            pass
    print("== 改进有效性复查（近 %d 天）==" % days)
    if len(tr) >= 2:
        first, last = tr[0], tr[-1]
        print("  成功率：%s %.1f%% → %s %.1f%%（%+.1f）"
              % (first[0], first[1], last[0], last[1], last[1] - first[1]))
        print("  逐日：" + "  ".join("%s=%.0f%%" % (d[4:], v) for d, v in tr))
    else:
        print("  快照不足（至少两天的 self_review 才能比）")
    try:
        out = subprocess.run(["git", "-C", "/opt/embytg", "log",
                              "--since=%d.days" % days, "--pretty=%h %ad %s",
                              "--date=format:%m-%d %H:%M", "--", "pipeline/*.py"],
                             capture_output=True, text=True, timeout=60).stdout.strip()
        lines = out.splitlines()
        print("  期间脚本改动 %d 次（最近 6 条）：" % len(lines))
        for l in lines[:6]:
            print("    " + l[:96])
    except Exception as e:                                         # noqa: BLE001
        print("  git 查询失败：%s" % str(e)[:50])
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-days", type=int, default=7)
    ap.add_argument("--review", action="store_true", help="只看 7 天改进有效性")
    a = ap.parse_args()
    if a.review:
        return review(a.window_days)
    cutoff = time.time() - a.window_days * 86400
    rep = {"ts": time.strftime("%F %T"), "window_days": a.window_days}

    # ---- 1) 入库成功率 ----
    # E035（2026-11-12 运维守护自省）：autoingest.log 里除了「尝试」还混着
    # duplicate / skip_series / register 等事件，它们没有 ok 字段。此前一律
    # 计入 tries → 63 条 duplicate + 2 条 skip_series 被记成「失败/未知」，
    # 成功率被压到 13.8%（真实 24/106=22.6%），改进清单也跟着指向错方向。
    # 现在：只有带 ok 字段的记录算尝试；其余按 event 单列，不进分母。
    ing_all = [r for r in tail_jsonl("autoingest.log") if ts2epoch(r.get("ts", "")) >= cutoff]
    ing = [r for r in ing_all if "ok" in r]
    skipped = [r for r in ing_all if "ok" not in r]
    ok = [r for r in ing if r.get("ok")]
    fail = [r for r in ing if not r.get("ok")]
    gb = sum(r.get("gb") or 0 for r in ok)
    rep["ingest"] = {"tries": len(ing), "ok": len(ok), "fail": len(fail),
                     "gb": round(gb, 1),
                     "success_rate": round(len(ok) * 100.0 / len(ing), 1) if ing else None,
                     "non_attempt": len(skipped)}
    rep["non_attempt_events"] = collections.Counter(
        (r.get("event") or "?") + ("/" + str(r.get("reason")) if r.get("reason") else "")
        for r in skipped).most_common(6)
    causes = collections.Counter(classify(r.get("tail")) for r in fail)
    rep["fail_causes"] = causes.most_common(8)

    # ---- 2) 源服健康 ----
    reqs = [r for r in tail_jsonl("source_requests.log", 20000) if ts2epoch(r.get("ts", "")) >= cutoff]
    per_host = collections.defaultdict(lambda: [0, 0, 0.0, 0])
    for r in reqs:
        h = r.get("label") or r.get("host")
        st = r.get("status") or 0
        per_host[h][0] += 1
        if 200 <= st < 300:
            per_host[h][1] += 1
        if st == 404:
            per_host[h][3] += 1        # 缺片：源没这部剧，不是源故障
        per_host[h][2] += (r.get("dur_ms") or 0) / 1000.0
    # rate   = 含 404 的原始成功率；rate_ok = 剔除 404 后的"源可用率"（判故障用）
    rep["sources"] = {h: {"n": v[0], "ok": v[1], "miss": v[3],
                          "rate": round(v[1] * 100.0 / v[0], 1) if v[0] else 0,
                          "rate_ok": round(v[1] * 100.0 / max(1, v[0] - v[3]), 1) if v[0] else 0,
                          "avg_ms": round(v[2] * 1000 / v[0]) if v[0] else 0}
                      for h, v in per_host.items()}

    # ---- 3) 巡检/自愈 ----
    cycles = [r for r in tail_jsonl("autoops_audit.jsonl", 20000)
              if r.get("kind") == "cycle" and ts2epoch(r.get("ts", "")) >= cutoff]
    actions = [r for r in tail_jsonl("autoops_audit.jsonl", 20000)
               if r.get("kind") == "action" and ts2epoch(r.get("ts", "")) >= cutoff]
    rep["ops"] = {"cycles": len(cycles), "actions": len(actions),
                  "avg_sec": round(statistics.mean([c.get("dur", 0) for c in cycles]), 1) if cycles else 0,
                  "findings": collections.Counter(
                      c for r in cycles
                      for c in (r.get("findings") if isinstance(r.get("findings"), list) else [])).most_common(6),
                  "findings_total": sum(r.get("findings") for r in cycles
                                        if isinstance(r.get("findings"), int))}

    # ---- 4) 失败记忆 ----
    fm = {}
    if os.path.exists(os.path.join(HERE, ".autoingest_failed.json")):
        try:
            fm = json.load(open(os.path.join(HERE, ".autoingest_failed.json"), encoding="utf-8"))
        except Exception:                                          # noqa: BLE001
            pass
    rep["dead_items"] = len(fm)

    # ---- 5) 改进清单（影响 × 频次 ÷ 成本） ----
    bl = []
    if rep["ingest"]["success_rate"] is not None and rep["ingest"]["success_rate"] < 80:
        top = causes.most_common(1)
        bl.append({"p": 1, "item": "入库成功率 %.0f%%（低）" % rep["ingest"]["success_rate"],
                   "why": "首要失败原因：%s ×%d" % (top[0][0], top[0][1]) if top else "原因未知",
                   "fix": "按失败原因改 source_pull：404→多源回退+失败记忆；401→换 token；无此剧→换源站",
                   "cost": "小"})
    if rep["dead_items"] >= 5:
        bl.append({"p": 1, "item": "%d 个「死集」反复失败" % rep["dead_items"],
                   "why": "源上元数据在、文件不在，重试只会浪费请求",
                   "fix": "延长失败记忆窗口到 30 天，并在缺口报告里标 DEAD，等换源",
                   "cost": "小"})
    slow = [h for h, v in rep["sources"].items() if v["avg_ms"] > 3000]
    if slow:
        bl.append({"p": 2, "item": "源响应慢：%s" % ",".join(slow[:3]),
                   "why": "平均 > 3s，拖慢整条入库链",
                   "fix": "在 src_guard 里按 avg 延迟加权排序（慢源后置）",
                   "cost": "小"})
    lowsrc = [h for h, v in rep["sources"].items()
              if v["n"] - v.get("miss", 0) >= 5 and v.get("rate_ok", v["rate"]) < 50]
    if lowsrc:
        bl.append({"p": 2, "item": "源成功率低：%s" % ",".join(lowsrc[:3]),
                   "why": "剔除 404 缺片后仍低于 50%（鉴权/限流/路径）",
                   "fix": "逐个确认 401 换 token；连续失败加长熔断",
                   "cost": "中"})
    if not bl:
        bl.append({"p": 3, "item": "本轮无高优先级问题",
                   "why": "成功率与源健康都在阈值内",
                   "fix": "继续扩展覆盖面（TODB 覆盖率盘点），或压缩单集平均耗时",
                   "cost": "中"})
    bl.sort(key=lambda x: x["p"])
    rep["backlog"] = bl
    json.dump(rep, open(BACKLOG, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    # ---- 输出 ----
    lines = ["# 自审报告 %s（近 %d 天）" % (rep["ts"], a.window_days), ""]
    i = rep["ingest"]
    lines.append("## 入库")
    lines.append("- 尝试 %s 次，成功 %s，失败 %s，落地 %.1f GB，成功率 **%s%%**"
                 % (i["tries"], i["ok"], i["fail"], i["gb"], i["success_rate"]))
    if i.get("non_attempt"):
        lines.append("- 另 %d 条非尝试事件（不计入成功率）：%s"
                     % (i["non_attempt"], "；".join("%s ×%d" % (k, v) for k, v in rep["non_attempt_events"])))
    if rep["fail_causes"]:
        lines.append("- 失败原因：" + "；".join("%s ×%d" % (k, v) for k, v in rep["fail_causes"]))
    lines.append("")
    lines.append("## 源服")
    for h, v in sorted(rep["sources"].items(), key=lambda x: -x[1]["n"])[:8]:
        lines.append("- %-24s %4d 次  成功率 %5.1f%%（剔除404 %5.1f%%，缺片 %d）  平均 %d ms"
                     % (h, v["n"], v["rate"], v.get("rate_ok", v["rate"]), v.get("miss", 0), v["avg_ms"]))
    lines.append("")
    lines.append("## 巡检/自愈")
    o = rep["ops"]
    lines.append("- %d 轮，%d 个动作，平均 %.1fs/轮" % (o["cycles"], o["actions"], o["avg_sec"]))
    if o["findings"]:
        lines.append("- 命中问题：" + "；".join("%s ×%d" % (k, v) for k, v in o["findings"]))
    lines.append("")
    lines.append("## 今日改进清单（自己给自己派活）")
    for idx, b in enumerate(bl, 1):
        lines.append("%d. **[P%d] %s**  \n   - 为什么：%s  \n   - 怎么改：%s（成本 %s）"
                     % (idx, b["p"], b["item"], b["why"], b["fix"], b["cost"]))
    open(OUT_MD, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print("\n→ 报告 %s\n→ 清单 %s" % (OUT_MD, BACKLOG))
    return 0


def ts2epoch(ts):
    try:
        return time.mktime(time.strptime(ts, "%Y-%m-%d %H:%M:%S"))
    except Exception:                                              # noqa: BLE001
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
