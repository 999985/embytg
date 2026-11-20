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
    """7 天回归复查：指标趋势 + 这段时间改过哪些脚本（验证"进化"是否真的有效）

    2026-11-18：指标从「成功率」换成「次/集」（quota_history 口径）。
    旧格式的快照（`成功率 **X%**`）已经不写了，这里不再解析 —— 混着两种口径
    画趋势会得出「成功率涨了」的假结论，而真实成本在别处。
    """
    import glob
    tr = []
    for f in sorted(glob.glob(os.path.join(LOGS, "self_review_*.md")))[-days:]:
        try:
            txt = open(f, encoding="utf-8").read()
            m = re.search(r"(\d+) 天共落地 \*\*(\d+) 集\*\*，烧 \d+ 次源请求，\*\*([\d.]+) 次/集\*\*", txt)
            d = os.path.basename(f).replace("self_review_", "").replace(".md", "")
            if m:
                tr.append((d, int(m.group(2)), float(m.group(3))))
        except Exception:                                          # noqa: BLE001
            pass
    print("== 改进有效性复查（近 %d 天）==" % days)
    if len(tr) >= 2:
        first, last = tr[0], tr[-1]
        print("  次/集：%s %.2f → %s %.2f（%+.2f，越低越好）"
              % (first[0], first[2], last[0], last[2], last[2] - first[2]))
        print("  逐日：" + "  ".join("%s=%.1f(%d集)" % (d[4:], v, n) for d, n, v in tr))
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

    # ---- 1) 入库产能（真源：logs/quota_history.jsonl） ----
    # 2026-11-18（自省）：原来这里读 autoingest.log 里「带 ok 字段的尝试」算成功率。
    # 但 autoingest.py --loop 已随 GHA 链路退役（ps 无进程、cron 无条目），
    # 实测 7 天窗口 tries=0 / success_rate=None —— 这一节每天都是空的，
    # 等于**监控指着退役链路**（AGENTS.md §7.8.6 的同一类错误：文档改了、判据没改）。
    #
    # 现在改读 quota_history.jsonl（daily_ops 每轮刷新，一天一行，tg.db 记账派生）：
    #   episodes = 当日落地集数，requests = 当日源请求数，
    #   per_ep   = 每集消耗请求数 —— 这才是 AGENTS.md §0.2 定的产能第一指标。
    prod_rows = []
    qh = os.path.join(LOGS, "quota_history.jsonl")
    if os.path.exists(qh):
        for line in open(qh, encoding="utf-8", errors="ignore"):
            try:
                r = json.loads(line)
            except Exception:                                      # noqa: BLE001
                continue
            if r.get("day") and ts2epoch(str(r["day"]) + " 00:00:00") >= cutoff - 86400:
                prod_rows.append(r)
    prod_rows.sort(key=lambda r: str(r.get("day")))
    full = [r for r in prod_rows if not r.get("partial")]
    ep_sum = sum(int(r.get("episodes") or 0) for r in prod_rows)
    req_sum = sum(int(r.get("requests") or 0) for r in prod_rows)
    rep["prod"] = {
        "days": len(prod_rows),
        "episodes": ep_sum,
        "requests": req_sum,
        "per_ep": round(req_sum / ep_sum, 2) if ep_sum else None,
        # 只拿「完整日」算基线：当天 partial 会把 per_ep 拉偏（请求已发、集还没落完）
        "full_days": len(full),
        "full_per_ep": (round(sum(int(r["requests"]) for r in full)
                              / max(1, sum(int(r["episodes"]) for r in full)), 2)
                        if full and sum(int(r["episodes"]) for r in full) else None),
        "today": (prod_rows[-1] if prod_rows else {}),
    }
    # autoingest.log 仍在写（duplicate / skip 事件由 gha_next3 打），只取事件计数
    ing_all = [r for r in tail_jsonl("autoingest.log") if ts2epoch(r.get("ts", "")) >= cutoff]
    skipped = [r for r in ing_all if "ok" not in r]
    dup_n = len([r for r in skipped if r.get("event") == "duplicate"])
    rep["ingest"] = {
        "episodes": ep_sum, "requests": req_sum, "per_ep": rep["prod"]["per_ep"],
        "duplicate": dup_n,
        # 每落地 1 集付出的 duplicate 扫描条数（§0.2 的 KPI，越低越好）
        "dup_per_ep": round(dup_n / ep_sum, 3) if ep_sum else None,
        "non_attempt": len(skipped),
    }
    rep["non_attempt_events"] = collections.Counter(
        (r.get("event") or "?") + ("/" + str(r.get("reason")) if r.get("reason") else "")
        for r in skipped).most_common(6)
    rep["fail_causes"] = []       # 失败原因已由 quota_history.queue.reasons 体现（见 §4）

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

    # ---- 4) 失败记忆（真源：.gha_ep_fail.json） ----
    # 2026-11-18（自省）：原来读 `.autoingest_failed.json` —— 那是 autoingest.py 的表，
    # 随 GHA 链路退役后**冻结在 2026-11-15**，永远 43 条。于是 P1「43 个死集反复失败」
    # 连续几天挂在改进清单上，指向一个没人再跑的文件，真问题反而看不见（§7.8.6 同一类）。
    #
    # 现在读 live 的按集退避表（gha_state.record_episode_failure 写、gha_next3 落 claim 前读）：
    #   total     = 表内条数
    #   cooling   = next_check 未到期（正在退避，本来就不该被领 —— 不领才是对的）
    #   today     = 今天又失败过（退避到期后又撞了一次 → 该换源/换画质，不是再等一天）
    #   by_reason = 原因分布：upload_failed 找 runner，source_* 找源站
    ep_fail = {}
    try:
        ep_fail = json.load(open(os.path.join(HERE, ".gha_ep_fail.json"), encoding="utf-8"))
    except Exception:                                              # noqa: BLE001
        ep_fail = {}
    _now = time.time()
    _today = time.strftime("%Y-%m-%d")
    _cooling = [k for k, v in ep_fail.items()
                if isinstance(v, dict) and _now < float(v.get("next_check") or 0)]
    # 2026-11-19：冷却条目必须按类别拆开报。原来一律写成「源站长期缺片」，
    # 而实测 48/81 条是 upload_failed —— 报表把人指向「换源/标 DEAD」，
    # 真问题（TG 上传通道）反而看不见。类别判定与 gha_state.ep_fail_class 同一实现。
    # 2026-11-21：再加一档 slow（源站慢、文件在）—— 它既不是缺片也不是我方故障，
    # 混进任何一边都会让清单指错方向（实测 20 条在冷却里 10 条是它）。
    import gha_state as _GS
    _cls_of = {k: _GS.ep_fail_class((ep_fail.get(k) or {}).get("reason")) for k in _cooling}
    _cool_src = [k for k in _cooling if _cls_of.get(k) == "src"]
    _cool_slow = [k for k in _cooling if _cls_of.get(k) == "slow"]
    _cool_ours = [k for k in _cooling
                  if k not in set(_cool_src) and k not in set(_cool_slow)]
    _today_fail = [k for k, v in ep_fail.items()
                   if isinstance(v, dict)
                   and time.strftime("%Y-%m-%d", time.localtime(float(v.get("last") or 0))) == _today]
    legacy_n = 0
    try:
        legacy_n = len(json.load(open(os.path.join(HERE, ".autoingest_failed.json"),
                                      encoding="utf-8")))
    except Exception:                                              # noqa: BLE001
        pass
    rep["ep_fail"] = {
        "total": len(ep_fail), "cooling": len(_cooling), "today": len(_today_fail),
        "cooling_src": len(_cool_src), "cooling_slow": len(_cool_slow),
        "cooling_ours": len(_cool_ours),
        "by_reason": collections.Counter(
            str(v.get("reason") or "?")[:40] for v in ep_fail.values()
            if isinstance(v, dict)).most_common(5),
        # 今日新增失败的原因要单独统计：全表分布会被历史条目带偏（改进清单要能指向当天的病）
        "by_reason_today": collections.Counter(
            str((ep_fail.get(k) or {}).get("reason") or "?")[:40] for k in _today_fail).most_common(5),
        "legacy_retired": legacy_n,     # 退役链路的旧表，只做留痕，不再派活
    }
    # 队列停泊原因（quota_history 当日行）：no_source / series_complete / fill_ep_present
    _q = (rep["prod"]["today"] or {}).get("queue") or {}
    rep["parked"] = {
        "total": _q.get("total"), "parked": _q.get("parked"),
        "top": sorted((_q.get("reasons") or {}).items(), key=lambda x: -x[1])[:5],
    }

    # ---- 5) 改进清单（影响 × 频次 ÷ 成本） ----
    bl = []
    _per_ep = rep["prod"]["per_ep"]
    if _per_ep is not None and _per_ep > 15:
        bl.append({"p": 1, "item": "每集消耗 %.1f 次源请求（>15，配额被烧穿）" % _per_ep,
                   "why": "%d 集烧了 %d 次请求；配额是硬顶，次/集 高就是白烧"
                          % (rep["prod"]["episodes"], rep["prod"]["requests"]),
                   "fix": "查 quota_history 的 search/episodes_req 占比：search 高→加列集缓存；"
                          "episodes_req 高→多源复用同一列集结果",
                   "cost": "中"})
    elif _per_ep is not None and _per_ep > 8:
        bl.append({"p": 2, "item": "每集消耗 %.1f 次源请求（目标 ≤5）" % _per_ep,
                   "why": "配额约 1950 次/天 → 次/集 决定日产能上限；当前 %d 集/7 天"
                          % rep["prod"]["episodes"],
                   "fix": "先量化 search/episodes_req 各自占比（quota_history 有字段），"
                          "再决定加列集缓存还是多源复用",
                   "cost": "中"})
    if rep["ep_fail"].get("cooling_src", 0) >= 20:
        bl.append({"p": 1, "item": "%d 集在**源站缺片**退避里" % rep["ep_fail"]["cooling_src"],
                   "why": "源站侧反复失败，已按指数退避冷却；冷却期满还会再撞，除非换源或标 DEAD",
                   "fix": "缺口报告里标 DEAD（60 天不再尝试）+ 对这批剧换源站；原因分布见 §4",
                   "cost": "小"})
    if rep["ep_fail"].get("cooling_slow", 0) >= 3:
        _n_slow = rep["ep_fail"]["cooling_slow"]
        bl.append({"p": 1 if _n_slow >= 10 else 2,
                   "item": "%d 集因**源站太慢**退避（文件在，不是缺片）" % _n_slow,
                   "why": "下载速度低于 TG_MIN_DOWNLOAD_MBPS 被速度闸掐断；罚的应该是源"
                          "（download_fails 降额 + 10min 冷却），集本身换个源就能下",
                   "fix": "已按 slow 档 1h→2h→4h（6h 封顶）重试；同一集连续慢 "
                          "EP_FAIL_SLOW_HARD 次才转长退避。反复出现就看 runner 日志 "
                          "SOURCE_TOO_SLOW 的源名分布（多半是夜间带宽或某个慢源被反复选中）",
                   "cost": "小"})
    if rep["ep_fail"].get("cooling_ours", 0) >= 10:
        bl.append({"p": 1, "item": "%d 集在**我方失败**退避里（上传/会话，不是源站缺片）"
                               % rep["ep_fail"]["cooling_ours"],
                   "why": "这些集源站有片，是上传/收尾环节失败被冷却；长期封禁等于白丢产能",
                   "fix": "看 runner 的 upload 日志与 TG 通道健康；这类退避已限 ≤6h",
                   "cost": "小"})
    if rep["ep_fail"]["today"] >= 3:
        bl.append({"p": 2, "item": "今日 %d 集新进失败记忆" % rep["ep_fail"]["today"],
                   "why": "原因：" + "；".join("%s ×%d" % (r, n)
                                              for r, n in rep["ep_fail"]["by_reason_today"][:3]),
                   "fix": "upload_failed→看 runner 日志/orphan_finish；source_*→换源",
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
    p = rep["prod"]
    lines.append("## 入库（真源 quota_history）")
    lines.append("- %d 天共落地 **%d 集**，烧 %d 次源请求，**%.2f 次/集**（目标 ≤5）"
                 % (p["days"], p["episodes"], p["requests"], p["per_ep"] or 0))
    if p.get("full_per_ep") is not None:
        lines.append("- 完整日基线 **%.2f 次/集**（%d 天；当天 partial 不计）"
                     % (p["full_per_ep"], p["full_days"]))
    t = p.get("today") or {}
    if t:
        lines.append("- 今天：%s 集 / %s 次请求 / %s 次每集%s"
                     % (t.get("episodes"), t.get("requests"), t.get("per_ep"),
                        "（未结束）" if t.get("partial") else ""))
    lines.append("- duplicate %d 条 → **%.2f 条/集**（§0.2 产能比，越低越好）"
                 % (i["duplicate"], i["dup_per_ep"] or 0))
    if i.get("non_attempt"):
        lines.append("- 非尝试事件 %d 条：%s"
                     % (i["non_attempt"], "；".join("%s ×%d" % (k, v) for k, v in rep["non_attempt_events"])))
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
    lines.append("## 失败记忆 / 队列停泊")
    e = rep["ep_fail"]
    lines.append("- 按集退避表 %d 条：冷却中 %d / 今日新增失败 %d"
                 "（旧表 `.autoingest_failed` %d 条属退役链路，只留痕不派活）"
                 % (e["total"], e["cooling"], e["today"], e["legacy_retired"]))
    if e["by_reason"]:
        lines.append("- 失败原因：" + "；".join("%s ×%d" % (r, n) for r, n in e["by_reason"]))
    pk = rep.get("parked") or {}
    if pk.get("parked") is not None:
        lines.append("- 队列 %s 条，停泊 %s 条；主因：%s"
                     % (pk.get("total"), pk.get("parked"),
                        "；".join("%s ×%d" % (k, v) for k, v in (pk.get("top") or []))))
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
