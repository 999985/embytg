#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gen_nfo_gap_report.py —— 生成「仍缺 NFO」的可执行清单（含 TODB 候选与不匹配原因）

目的：把"200 条无 NFO"从一句结论变成可追溯、可复查的清单 —— 每条都写清
  ① 属于哪部剧、② 缺哪几集、③ TODB 有哪些候选、④ 为什么没映射上。
配合 `todb_resolve.py`（结构匹配）与 `docs/KB.md §5`。

用法：
  python3 gen_nfo_gap_report.py            # 写 docs/NFO_GAPS_<date>.md
  python3 gen_nfo_gap_report.py --print
"""
import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict

sys.path.insert(0, "/opt/embytg/pipeline")
import todb_titles as TT
from embytg_conf import cfg

TMDB_API = cfg("tmdb_api") or "https://api.themoviedb.org/3"
TMDB_TK = cfg("tmdb_token")
_ID = re.compile(r"\{tmdb=(\d+)\}")


def tmdb_seasons(name):
    """返回 [(season, count)]，取不到则 None"""
    import urllib.request
    m = _ID.search(name)
    if not m:
        return None
    try:
        r = urllib.request.Request("%s/tv/%s?language=zh-CN" % (TMDB_API, m.group(1)),
                                   headers={"Authorization": "Bearer " + TMDB_TK})
        d = json.loads(urllib.request.urlopen(r, timeout=20).read().decode())
        return [(x.get("season_number"), x.get("episode_count"))
                for x in (d.get("seasons") or []) if (x.get("season_number") or 0) > 0]
    except Exception:
        return None

MNT = "/opt/embytg/mnt/embytg/TV"
LIBS = ["国产剧", "日韩剧", "欧美剧", "港台剧", "国漫", "日漫", "综艺"]
RESOLVE = "/opt/embytg/pipeline/.todb_resolve_last.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--print", action="store_true")
    a = ap.parse_args()
    gaps = defaultdict(list)
    for lib in LIBS:
        D = os.path.join(MNT, lib)
        if not os.path.isdir(D):
            continue
        for s in sorted(os.listdir(D)):
            sp = os.path.join(D, s)
            if not os.path.isdir(sp):
                continue
            for root, _, fs in os.walk(sp):
                for f in fs:
                    if not f.lower().endswith(TT.VIDEO_EXT):
                        continue
                    p = os.path.join(root, f)
                    if os.path.exists(os.path.splitext(p)[0] + ".nfo"):
                        continue
                    gaps["%s/%s" % (lib, s)].append(
                        os.path.relpath(p, sp).replace(os.sep, "/"))
    # 合并 resolver 的候选信息
    cand = {}
    if os.path.exists(RESOLVE):
        try:
            for r in json.load(open(RESOLVE)).get("report", []):
                cand[r["series"]] = r
        except Exception:
            pass
    total = sum(len(v) for v in gaps.values())
    lines = []
    lines.append("# NFO 缺口清单（%s 自动生成）" % time.strftime("%F %T"))
    lines.append("")
    lines.append("> 生成脚本 `pipeline/gen_nfo_gap_report.py`。判定口径：磁盘视频没有同名 `.nfo`。")
    lines.append("> **每条都写清为什么不补**——不要重复排查，也不要猜标题（见 `KB.md §6`）。")
    lines.append("")
    lines.append("**合计 %d 条，涉及 %d 部剧。**" % (total, len(gaps)))
    lines.append("")
    lines.append("| # | 库/剧 | 缺 | 缺的集 | 磁盘季 | TMDB 季 | TODB 候选（集数/命中） | 结论 |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for i, (k, files) in enumerate(sorted(gaps.items(), key=lambda x: -len(x[1])), 1):
        lib, name = k.split("/", 1)
        eps = sorted({int(m.group(2)) for f in files
                      if (m := TT.SE_RE.search(f))} )
        rst = []
        for r in files:
            m = TT.SE_RE.search(r)
            if m:
                rst.append("S%sE%s" % (m.group(1), m.group(2)))
        reason = "TODB/TMDB 无对应集或无标题"
        ts_probe = tmdb_seasons(name)
        maxep = max([int(m.group(2)) for r in files if (m := TT.SE_RE.search(r))] or [0])
        cinfo = ""
        cs = []
        if name in cand:
            c = cand[name]
            cs = c.get("cands") or []
            cinfo = "、".join("%s(%s集/中%s)" % (x["vid"], x["eps"], x["hit"]) for x in cs[:4])
            if c.get("filled"):
                reason = "resolver 可补 %d 条（未执行）" % c["filled"]
            elif cs:
                reason = "候选均无法覆盖这些集"
        # 自动归因（比"数据源缺失"更准确）
        if ts_probe:
            tmap = dict(ts_probe)
            dseasons = sorted({int(m.group(1)) for r in files if (m := TT.SE_RE.search(r))})
            if dseasons and any(x not in tmap for x in dseasons):
                reason = "**盘侧季号与 TMDB 不符**（磁盘 %s / TMDB %s）→ 需重编号，不是数据缺失" % (
                    ",".join("S%d" % x for x in dseasons),
                    ",".join("S%d" % k for k in sorted(tmap)))
            elif maxep > max(tmap.values() or [0]):
                reason = "**集号为绝对/日期编号**（最大 E%d > TMDB 单季 %d）→ 属 E012 类，先 renumber" % (
                    maxep, max(tmap.values() or [0]))
            elif len(files) and all(len([r for r in files if TT.SE_RE.search(r) and int(TT.SE_RE.search(r).group(1)) == k]) > tmap.get(k, 0) for k in dseasons):
                reason = "**TMDB 该季集数少于磁盘** → 数据源不全"
            elif not cs:
                reason = "TMDB 有该季但 TODB 无候选 → 用 nfo_episodes_tmdb.py"
        disk_seasons = sorted({int(m.group(1)) for r in files
                               if (m := TT.SE_RE.search(r))})
        ts = tmdb_seasons(name)
        tstr = ",".join("S%s:%s" % (a_, b_) for a_, b_ in ts) if ts else "?"
        lines.append("| %d | %s/%s | %d | %s | %s | %s | %s | %s |"
                     % (i, lib, name[:26], len(files),
                        ",".join(rst[:8]) + ("…" if len(rst) > 8 else ""),
                        ",".join("S%d" % x for x in disk_seasons) or "-",
                        tstr, cinfo or "-", reason))
    lines.append("")
    lines.append("## 分类处理建议")
    lines.append("")
    lines.append("- **同季前后不一致/整季缺**：先跑 `todb_resolve.py --scan` 看候选表，能全覆盖再 `--apply --rewrite`。")
    lines.append("- **TODB 只有占位名（第 N 集）**：可用 `nfo_episodes_tmdb.py --apply --allow-placeholder`（仍优于文件名）。")
    lines.append("- **日期式集号（E522/E930）**：属 E012 未覆盖的残留，需先 `renumber_todb.py` 或人工确认季集。")
    lines.append("- **确实没有**：保持空 NFO，`naming_health.py` 会一直显示缺口数；等数据源补录。")
    out = "\n".join(lines) + "\n"
    if a.print:
        print(out)
        return
    p = "/opt/embytg/pipeline/docs/NFO_GAPS_%s.md" % time.strftime("%Y%m%d")
    open(p, "w", encoding="utf-8").write(out)
    print("已写 %s（%d 条 / %d 部剧）" % (p, total, len(gaps)))


if __name__ == "__main__":
    main()
