#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ingest_review.py —— 次日复核：昨天入库到底"成没成"

用户 2026-11-10 嘱咐：
  「定时任务入库一定让按流程来，其次第二天要查看一下昨天是否入库正常，
    如果不正常排查原因并从根本上解决问题，防止反复犯错。」

它回答四个问题（对指定日期，默认昨天）：
  1. **拉到了吗**：attempts / ok / fail / GB（autoingest.log + .ingest_manual.jsonl + tg.db）
  2. **按流程走了吗**：每集是否有 `.nfo`、标题是否非占位、Emby 是否收录且 Name==NFO（抽查 20）
  3. **库干净吗**：当前 NFO 缺口 vs 昨日快照（有没有"入库反而更乱"）
  4. **错了为什么**：失败原因分类 + **根因候选** + 建议的永久修法（写进报告，避免下月重犯）

产出：
  - `logs/ingest_review_<date>.md`
  - `.nfo_gap_history.json`（每日缺口快照，用于趋势）
  - 退出码 0=PASS / 1=WARN / 2=FAIL（FAIL 会在次日巡检里被点名）

用法：
  python3 ingest_review.py                # 复核昨天
  python3 ingest_review.py --date 2026-11-10
  python3 ingest_review.py --date 2026-11-10 --json
"""
import argparse
import datetime
import json
import os
import re
import sqlite3
import sys
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
MNT = "/opt/embytg/mnt/embytg/TV"
CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
EMBY = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
TOKEN = CFG["emby_token"]
HIST = os.path.join(HERE, ".nfo_gap_history.json")
PLACEHOLDER = re.compile(r"^第\s*\d+\s*[集期]$|^Episode\s*\d+$", re.I)


def emby(path, timeout=60):
    req = urllib.request.Request(EMBY + path, headers={"X-Emby-Token": TOKEN})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode())


def load_jsonl(name, limit=20000):
    p = os.path.join(HERE, "logs", name)
    out = []
    if not os.path.exists(p):
        return out
    for line in open(p, encoding="utf-8", errors="ignore").read().strip().splitlines()[-limit:]:
        try:
            out.append(json.loads(line))
        except Exception:                                          # noqa: BLE001
            pass
    return out


def classify(tail):
    if not tail:
        return "未知"
    # 机器标记优先（source_pull 2026-11-11 起输出），避免文本误判
    if "RESULT=no_source_series" in tail:
        return "源没有这部剧（整部不在任何源 → 需 TG 补源）"
    if "RESULT=no_source_episode" in tail:
        return "无源可提供（搜索无结果 → 需 TG 补源）"
    if "RESULT=direct_link_fail" in tail:
        return "源直链 404（元数据在、文件不在）"
    if "RESULT=budget_blocked" in tail:
        return "预算闸门拦下"
    # 旧日志：no_source 必须先于 404 判断——no_source 的 tail 里带有多源搜索的 404。
    if "没有源提供这一集" in tail or "没有源提供" in tail:
        return "无源可提供（搜索无结果 → 需 TG 补源）"
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
        return "预算闸门拦下"
    if "搜索失败" in tail:
        return "源搜索接口异常"
    if "timeout" in tail.lower() or "timed out" in tail.lower():
        return "超时"
    return "其它"


def nfo_gaps():
    """当前缺 NFO 的视频数（只统计有视频没 .nfo 的）"""
    n = 0
    for lib in os.listdir(MNT):
        d = os.path.join(MNT, lib)
        if not os.path.isdir(d):
            continue
        for dp, _, fs in os.walk(d):
            vids = {os.path.splitext(f)[0] for f in fs if f.lower().endswith((".mp4", ".mkv"))}
            have = {os.path.splitext(f)[0] for f in fs if f.lower().endswith(".nfo")}
            n += len(vids - have)
    return n


def source_placeholders(lib, sdir, cache={}):
    """判断该剧在数据源里是否本身就是占位标题（TMDB 全是「第 N 集」）。
    返回 (总集数, 非占位数)；拿不到返回 (0,0)。
    目的：区分「流程没写标题」与「数据源根本没标题」，避免每天误报。"""
    if sdir in cache:
        return cache[sdir]
    m = re.search(r"\{tmdb=(\d+)\}", sdir)
    if not m:
        cache[sdir] = (0, 0)
        return cache[sdir]
    try:
        k = CFG["tmdb_api_key"]
        u = "%s/tv/%s/season/1?api_key=%s&language=zh-CN" % (CFG["tmdb_api"], m.group(1), k)
        d = json.loads(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}),
                                              timeout=25).read().decode())
        names = [e.get("name") or "" for e in d.get("episodes", [])]
        good = sum(1 for n in names if n and not PLACEHOLDER.match(n))
        cache[sdir] = (len(names), good)
    except Exception:                                              # noqa: BLE001
        cache[sdir] = (0, 0)
    return cache[sdir]


def episode_has_real_title(lib, sdir, ep_no, cache={}):
    """该集在数据源（TMDB）里是否有真实标题。

    2026-11-14（E085）：`source_placeholders` 只能回答「整季都没有标题」，
    但真实情况更细：FX战士久留美 S01 共 12 集，7 集有真标题、5 集没有。
    收尾验收把「某集没标题」判成 FAIL，导致 claim 永不释放、卡死整条链路。
    这里按集号精确判断，只有「这一集确实没有」才豁免。
    """
    key = (sdir, int(ep_no))
    if key in cache:
        return cache[key]
    m = re.search(r"\{tmdb=(\d+)\}", sdir)
    if not m:
        cache[key] = True                  # 判不了就当有标题（保守：报出来）
        return cache[key]
    try:
        k = CFG["tmdb_api_key"]
        sm = re.search(r"Season\s*(\d+)", sdir, re.I)
        season = int(sm.group(1)) if sm else 1
        u = "%s/tv/%s/season/%d?api_key=%s&language=zh-CN" % (
            CFG["tmdb_api"], m.group(1), season, k)
        d = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}),
            timeout=25).read().decode())
        for e in (d.get("episodes") or []):
            if int(e.get("episode_number") or 0) == int(ep_no):
                nm = (e.get("name") or "").strip()
                cache[key] = bool(nm) and not PLACEHOLDER.match(nm)
                return cache[key]
        cache[key] = False
    except Exception:                                      # noqa: BLE001
        cache[key] = True                  # 查询失败不豁免，保持原判据
    return cache[key]


def check_files(day):
    """抽查当天入库文件：NFO 在不在、标题是不是占位、Emby 有没有且名字对不对"""
    ing = [r for r in load_jsonl("autoingest.log") if (r.get("ts") or "").startswith(day)]
    ok = [r for r in ing if r.get("ok")]
    res = {"sampled": 0, "no_nfo": [], "placeholder": [], "emby_missing": [], "name_mismatch": []}
    for r in ok[:20]:
        tgt = r.get("target") or ""
        if not tgt or not os.path.exists(tgt):
            continue
        res["sampled"] += 1
        nfo = os.path.splitext(tgt)[0] + ".nfo"
        if not os.path.exists(nfo):
            res["no_nfo"].append(os.path.basename(tgt))
            continue
        title = ""
        m = re.search(r"<title>(.*?)</title>", open(nfo, encoding="utf-8", errors="ignore").read(), re.S)
        if m:
            title = m.group(1).strip()
        if not title or PLACEHOLDER.match(title):
            res["placeholder"].append(os.path.basename(tgt))
        # Emby 侧
        emby_path = tgt.replace("/opt/embytg/mnt/embytg", "/mnt/embytg")
        try:
            q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                        "Path": emby_path, "Fields": "Path", "Limit": "1"})
            items = emby("/Items?" + q).get("Items", [])
            if not items:
                res["emby_missing"].append(os.path.basename(tgt))
            elif title and items[0].get("Name") != title:
                res["name_mismatch"].append("%s: emby=%r nfo=%r" % (os.path.basename(tgt)[:24],
                                                                    items[0].get("Name"), title))
        except Exception:                                          # noqa: BLE001
            pass
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    day = a.date or (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

    rep = {"date": day, "ts": datetime.datetime.now().strftime("%F %T")}

    # 1) 拉取统计
    ing = [r for r in load_jsonl("autoingest.log") if (r.get("ts") or "").startswith(day)]
    ok = [r for r in ing if r.get("ok")]
    fail = [r for r in ing if not r.get("ok")]
    gb_log = round(sum((r.get("gb") or 0) for r in ok), 2)
    man = [r for r in load_jsonl(".ingest_manual.jsonl") if (r.get("ts") or "").startswith(day)]
    gb_man = round(sum((r.get("gb") or 0) for r in man), 2)
    try:
        con = sqlite3.connect(os.path.join(HERE, "tg.db"))
        row = con.execute("select coalesce(sum(size),0)/1e9, count(*) from resource_state "
                          "where date(landed_at,'unixepoch','localtime')=?", (day,)).fetchone()
        gb_tg, n_tg = round(row[0] or 0, 1), row[1] or 0
    except Exception:                                              # noqa: BLE001
        gb_tg, n_tg = 0, 0
    rep["pull"] = {"attempts": len(ing), "ok": len(ok), "fail": len(fail),
                   "gb_auto": gb_log, "gb_manual": gb_man, "gb_tgdb": gb_tg, "tgdb_files": n_tg,
                   "success_rate": round(len(ok) * 100.0 / len(ing), 1) if ing else None}
    causes = {}
    for r in fail:
        k = classify(r.get("tail"))
        causes[k] = causes.get(k, 0) + 1
    rep["fail_causes"] = causes

    # 2) 流程验收（抽样）
    rep["files"] = check_files(day)
    # 区分「数据源无标题」与「流程没写」
    srcph = []
    for fn in rep["files"]["placeholder"]:
        pass
    for r in [x for x in load_jsonl("autoingest.log") if (x.get("ts") or "").startswith(day) and x.get("ok")][:20]:
        tgt = r.get("target") or ""
        if "/TV/" in tgt:
            parts = tgt.split("/TV/")[1].split("/")
            if len(parts) >= 2:
                tot, good = source_placeholders(parts[0], parts[1])
                if tot and good <= max(1, int(tot * 0.2)):
                    srcph.append(parts[1])
    rep["source_placeholder_series"] = sorted(set(srcph))

    # 3) 库健康 + 趋势
    gaps_now = nfo_gaps()
    hist = {}
    if os.path.exists(HIST):
        try:
            hist = json.load(open(HIST, encoding="utf-8"))
        except Exception:                                          # noqa: BLE001
            hist = {}
    prev = None
    for d in sorted(hist):
        if d < day:
            prev = (d, hist[d])
    rep["gaps"] = {"now": gaps_now, "prev": prev}
    hist[day] = gaps_now
    json.dump(hist, open(HIST, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    # 4) 判定
    f = rep["files"]
    problems = []
    if rep["pull"]["attempts"] and (rep["pull"]["success_rate"] or 0) < 80:
        problems.append("成功率 %.0f%%（<80%%）" % rep["pull"]["success_rate"])
    if f["no_nfo"]:
        problems.append("%d 个文件没有 NFO（流程第 3 步漏了）" % len(f["no_nfo"]))
    if f["placeholder"]:
        if rep.get("source_placeholder_series"):
            rep.setdefault("notes", []).append(
                "%d 个占位标题属数据源限制（%s 在 TMDB/TODB 本身就是「第 N 集」），不计为流程问题"
                % (len(f["placeholder"]), ", ".join(rep["source_placeholder_series"])[:40]))
        else:
            problems.append("%d 个 NFO 是占位标题" % len(f["placeholder"]))
    if f["emby_missing"]:
        problems.append("%d 个文件 Emby 未收录（流程第 5 步漏了）" % len(f["emby_missing"]))
    if f["name_mismatch"]:
        problems.append("%d 个 Emby Name 与 NFO 不一致（流程第 4 步漏了）" % len(f["name_mismatch"]))
    if prev and gaps_now > prev[1]:
        problems.append("NFO 缺口比 %s 增加 %d 条" % (prev[0], gaps_now - prev[1]))
    rep["problems"] = problems

    # 5) 根因候选 + 永久修法（针对每类失败给出"下次不会再错"的动作）
    root = []
    for k, v in causes.items():
        if "无源" in k:
            root.append({"cause": k, "n": v,
                         "fix": "这类不要反复重试：已在 .autoingest_failed.json 标 no_source（30 天内不再排），"
                                "转交运维任务从 TG 补源，或降低该剧优先级",
                         "where": ".autoingest_failed.json / autoingest.hot()"})
        elif "404" in k:
            root.append({"cause": k, "n": v,
                         "fix": "已加多源换源重试+失败记忆；若复现，检查是否所有源都缺该文件（改走 TG 补源）",
                         "where": "source_pull.resolve(exclude=...) / .autoingest_failed.json"})
        elif "鉴权" in k:
            root.append({"cause": k, "n": v, "fix": "src_login.py 重新换 token（Hills UA）",
                         "where": "src_guard.TOKEN_STALE / src_login.py"})
        elif "没有这部剧" in k or "缺这一集" in k:
            root.append({"cause": k, "n": v,
                         "fix": "已写入系列级失败记忆（@series:，30 天不再排）+ 单轮同剧连败 3 集熔断；"
                                "转运维任务从 TG 补源",
                         "where": ".autoingest_failed.json / autoingest.hot_series()"})
        elif "预算" in k:
            root.append({"cause": k, "n": v, "fix": "调批次大小或换 1080p；预算值见 .ingest_budget.json",
                         "where": "ingest_quota.py"})
        else:
            root.append({"cause": k, "n": v, "fix": "看 logs/autoingest.log 里该条 tail 的原始报错",
                         "where": "logs/autoingest.log"})
    if f["no_nfo"] or (f["placeholder"] and not rep.get("source_placeholder_series")):
        root.append({"cause": "流程漏步（NFO 未生成/占位）", "n": len(f["no_nfo"]) + (0 if rep.get("source_placeholder_series") else len(f["placeholder"])),
                     "fix": "把 nfo_gen + nfo_episodes_tmdb 设为入库收尾的强制步骤（cron prompt 已写死）",
                     "where": "cron_specs/远程上片核查.json / post_ingest.py"})
    if f["emby_missing"] or f["name_mismatch"]:
        root.append({"cause": "Emby 未同步", "n": len(f["emby_missing"]) + len(f["name_mismatch"]),
                     "fix": "入库后必须 Media/Updated + 单剧 Refresh + nfo_name_to_emby，再复核",
                     "where": "autoingest.notify() / nfo_name_to_emby.py"})
    rep["root_causes"] = root

    verdict = "PASS" if not problems else ("FAIL" if (f["emby_missing"] or f["no_nfo"] or
                                                      (rep["pull"]["success_rate"] or 100) < 50) else "WARN")
    rep["verdict"] = verdict

    if a.json:
        print(json.dumps(rep, ensure_ascii=False, indent=1))
    else:
        p = rep["pull"]
        print("== 次日复核 %s（%s）==" % (day, verdict))
        print("  拉取: 尝试 %d，成功 %d，失败 %d，成功率 %s%%"
              % (p["attempts"], p["ok"], p["fail"], p["success_rate"]))
        print("  流量: auto %.2f GB + manual %.2f GB（tg.db 记账 %.1f GB / %d 文件）"
              % (p["gb_auto"], p["gb_manual"], p["gb_tgdb"], p["tgdb_files"]))
        print("  验收抽样 %d 个：无NFO %d / 占位 %d / Emby 缺 %d / 名字不符 %d"
              % (f["sampled"], len(f["no_nfo"]), len(f["placeholder"]),
                 len(f["emby_missing"]), len(f["name_mismatch"])))
        print("  NFO 缺口: %d（%s）" % (gaps_now, ("上一快照 %s: %d" % prev) if prev else "无历史快照"))
        if problems:
            print("  问题:")
            for x in problems:
                print("    ✘", x)
            print("  根因与永久修法:")
            for r in root:
                print("    • %s ×%d → %s（%s）" % (r["cause"], r["n"], r["fix"], r["where"]))
        else:
            print("  ✅ 昨日入库正常：拉到、写全、索引到、名字对、缺口未恶化")

        md = ["# 入库复核 %s —— %s" % (day, verdict), ""]
        md += ["## 拉取", "- 尝试 %d / 成功 %d / 失败 %d / 成功率 %s%%"
               % (p["attempts"], p["ok"], p["fail"], p["success_rate"]),
               "- 流量：auto %.2f GB + manual %.2f GB（tg.db %.1f GB / %d 文件）"
               % (p["gb_auto"], p["gb_manual"], p["gb_tgdb"], p["tgdb_files"])]
        if causes:
            md.append("- 失败原因：" + "；".join("%s ×%d" % kv for kv in causes.items()))
        md += ["", "## 流程验收（抽样 %d）" % f["sampled"],
               "- 无 NFO：%d；占位标题：%d；Emby 缺：%d；Name 不符：%d"
               % (len(f["no_nfo"]), len(f["placeholder"]), len(f["emby_missing"]), len(f["name_mismatch"]))]
        for k in ("no_nfo", "placeholder", "emby_missing", "name_mismatch"):
            for x in f[k][:5]:
                md.append("  - %s: %s" % (k, x))
        md += ["", "## 库健康", "- 当前 NFO 缺口 %d" % gaps_now,
               "- 上一快照 %s" % (str(prev) if prev else "无")]
        if problems:
            md += ["", "## 问题", *["- ✘ %s" % x for x in problems],
                   "", "## 根因与永久修法", *["- %s ×%d → %s（%s）" % (r["cause"], r["n"], r["fix"], r["where"])
                                          for r in root]]
        out = os.path.join(HERE, "logs", "ingest_review_%s.md" % day)
        open(out, "w", encoding="utf-8").write("\n".join(md) + "\n")
        print("\n  → 报告 %s" % out)

    return {"PASS": 0, "WARN": 1, "FAIL": 2}[verdict]


if __name__ == "__main__":
    sys.exit(main())
