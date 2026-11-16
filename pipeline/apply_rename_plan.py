#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""apply_rename_plan.py —— 执行子智能体产出的「改名计划」（renumber 用）

背景：119 条缺 NFO 里约 113 条是**盘侧集号/季号错**，不是数据缺失。修法是改名
（季节目录 + 集号），但这是改生产状态的操作，必须可回滚、可复核。

约定
----
计划 JSON 结构（子智能体产出，见 /tmp/plan_A.json 等）::

    {"batch": "A", "series": [
      {"dir": "/opt/embytg/mnt/embytg/TV/综艺/xxx {tmdb=1}",
       "verdict": "mapped",
       "renames": [{"from": "Season 02/a S02E522 - 2160p.mp4",
                    "to":   "Season 02/a S02E12 - 标题 - 2160p.mp4",
                    "evidence": "TMDB S2E12 air=2024-05-22"}, ...]},
      ...]}

安全闸门（任何一条不满足就整条跳过，不执行）
--------------------------------------------
1. `from` 必须存在、必须是文件、扩展名必须与 `to` 一致（.mp4/.mkv/.ts…）
2. `to` 不能已存在（防覆盖）
3. `to` 的季集号必须能被正则解析（SxxExx）
4. `to` 的季集号不能与**同剧其它文件**冲突（重名检测覆盖全剧）
5. 剧目目录必须存在
6. 同一批次里 `to` 不得重复

用法
----
    python3 apply_rename_plan.py /tmp/plan_A.json            # 干跑，打印逐条
    python3 apply_rename_plan.py /tmp/plan_A.json --apply    # 执行（写 journal）
    python3 apply_rename_plan.py --rollback journal.json     # 按 journal 回滚
"""
import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nfo_sidecar import rename_with_sidecars          # noqa: E402  E022：视频+NFO+海报+字幕整体搬

SE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")
TV_ROOT = "/opt/embytg/mnt/embytg/TV"
VIDEO_EXT = {".mp4", ".mkv", ".ts", ".avi", ".mov", ".m4v", ".flv", ".wmv"}


def log(msg):
    print(msg, flush=True)


def collect_series_files(root):
    """全剧现有文件的 (S,E) → 路径，用于冲突检测"""
    out = {}
    for dp, _, fn in os.walk(root):
        for f in fn:
            if os.path.splitext(f)[1].lower() not in VIDEO_EXT:
                continue
            m = SE_RE.search(f)
            if m:
                out[(int(m.group(1)), int(m.group(2)))] = os.path.join(dp, f)
    return out


def build_actions(plan):
    """把计划展开成可执行动作，并做全部校验；返回 (ok_actions, skipped)"""
    actions, skipped, seen_targets = [], [], set()
    for s in plan.get("series", []):
        root = s.get("dir", "")
        if root and not os.path.isdir(root):          # 子智能体可能写相对 TV 根 的路径
            alt = os.path.join(TV_ROOT, root.lstrip("/"))
            if os.path.isdir(alt):
                root = alt
        if not root or not os.path.isdir(root):
            skipped.append({"dir": root, "why": "剧目录不存在", "file": ""})
            continue
        if s.get("verdict") != "mapped":
            skipped.append({"dir": root, "why": "计划判定 unresolvable：%s" % (s.get("reason") or "")[:60], "file": ""})
            continue
        unit = os.path.basename(root)
        existing = collect_series_files(root)
        for r in s.get("renames", []):
            src_rel, dst_rel = (r.get("from") or "").strip(), (r.get("to") or "").strip()
            src = os.path.join(root, src_rel)
            dst = os.path.join(root, dst_rel)
            why = ""
            if not src_rel or not dst_rel:
                why = "from/to 为空"
            elif not os.path.isfile(src):
                why = "源文件不存在"
            elif os.path.exists(dst):
                why = "目标已存在（防覆盖）"
            elif os.path.splitext(src)[1].lower() != os.path.splitext(dst)[1].lower():
                why = "扩展名不一致"
            elif os.path.basename(os.path.dirname(src)) == os.path.basename(os.path.dirname(dst)) and os.path.basename(src) == os.path.basename(dst):
                why = "源与目标相同"
            else:
                m = SE_RE.search(os.path.basename(dst))
                if not m:
                    why = "目标文件名无 SxxExx"
                elif (int(m.group(1)), int(m.group(2))) in existing and existing[(int(m.group(1)), int(m.group(2)))] != src:
                    why = "目标季集号已被占用 %s" % os.path.basename(existing[(int(m.group(1)), int(m.group(2)))])
                elif dst in seen_targets:
                    why = "本批次目标重复"
            if why:
                skipped.append({"dir": unit, "file": src_rel, "why": why})
                continue
            seen_targets.add(dst)
            actions.append({"series": unit, "root": root, "from": src, "to": dst, "evidence": r.get("evidence", "")})
    return actions, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("plan", nargs="?", help="计划 JSON")
    ap.add_argument("--apply", action="store_true", help="真正执行（默认干跑）")
    ap.add_argument("--rollback", help="按 journal 回滚")
    ap.add_argument("--limit", type=int, default=0, help="只执行前 N 条（试点用）")
    a = ap.parse_args()

    if a.rollback:
        j = json.load(open(a.rollback, encoding="utf-8"))
        n = 0
        for it in reversed(j.get("done", [])):
            if os.path.exists(it["to"]) and not os.path.exists(it["from"]):
                os.makedirs(os.path.dirname(it["from"]), exist_ok=True)
                rename_with_sidecars(it["to"], it["from"])
                n += 1
        log("回滚完成 %d 条（journal=%s）" % (n, a.rollback))
        return

    if not a.plan:
        ap.error("需要 plan 或 --rollback")
    plan = json.load(open(a.plan, encoding="utf-8"))
    actions, skipped = build_actions(plan)

    log("== 计划 %s（batch=%s）==" % (os.path.basename(a.plan), plan.get("batch", "?")))
    log("  可执行 %d 条 / 跳过 %d 条" % (len(actions), len(skipped)))
    if skipped:
        log("  --- 跳过明细（前 20）---")
        for s in skipped[:20]:
            log("    × %-30s %-46s %s" % (s["dir"][:30], (s["file"] or "-")[:46], s["why"]))

    groups = {}
    for i in actions:
        groups.setdefault(i["series"], []).append(i)
    log("  --- 按剧汇总 ---")
    for k, v in sorted(groups.items(), key=lambda x: -len(x[1])):
        log("    %-42s %3d 条" % (k[:42], len(v)))

    if a.limit:
        actions = actions[:a.limit]
        log("  !! --limit=%d，只执行前 %d 条" % (a.limit, len(actions)))

    log("  --- 逐条（前 15）---")
    for i in actions[:15]:
        log("    %s\n      → %s" % (os.path.relpath(i["from"], i["root"]), os.path.relpath(i["to"], i["root"])))

    if not a.apply:
        log("\n[干跑] 未改任何文件。确认后加 --apply。")
        return

    jpath = "/opt/embytg/pipeline/rename_journal_%s.json" % time.strftime("%Y%m%d_%H%M%S")
    journal = {"plan": a.plan, "batch": plan.get("batch"), "start": time.strftime("%F %T"), "done": []}
    ok = fail = 0
    for i in actions:
        try:
            os.makedirs(os.path.dirname(i["to"]), exist_ok=True)
            rename_with_sidecars(i["from"], i["to"])
            journal["done"].append({"from": i["from"], "to": i["to"], "series": i["series"]})
            ok += 1
        except Exception as e:                                    # noqa: BLE001
            log("    ERR %s: %s" % (i["from"], e))
            fail += 1
    json.dump(journal, open(jpath, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    log("\n执行完成：成功 %d / 失败 %d → journal %s" % (ok, fail, jpath))
    log("回滚：python3 apply_rename_plan.py --rollback %s" % jpath)


if __name__ == "__main__":
    main()
