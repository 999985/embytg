#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nfo_episodes_fix.py —— 补齐 nfo_episodes.py 因「TODB 季结构与磁盘季不一致」漏掉的 episode NFO

背景：TODB（=TMDB）很多剧把全季塞进 Season 1（如《咒术回战》S1=59 集，实际 S1 24 + S2 23），
而磁盘按 S01/S02 分开且集号重新从 1 开始。nfo_episodes.py 只按 (season,episode) 精确匹配，
于是 S02 的集全部拿不到标题 → 不写 NFO → Emby Name 退回文件名。

本脚本只做「能证明对的映射」，分三类，逐条打印依据：
  A 直接命中     (s,e) 在 TODB 存在                                  —— 安全
  B 同季同集号   TODB 只有一个正季 M，(M,e) 存在                      —— 安全（86 那种 S02E12 接着编号）
  C 累计偏移     TODB 只有一个正季 M，offset = len(M) - 磁盘总集数     —— 仅当 offset>=0 且 (M,offset+e) 存在
  D 集号落在某季 TODB 集号区间内（斗破 S01E158 落在 S5 1-219）          —— 仅当唯一命中
其余一律跳过并计入 unfixable，绝不猜。

用法：
  python3 nfo_episodes_fix.py --scan [--lib 日漫]
  python3 nfo_episodes_fix.py --apply [--lib 日漫] [--workers 8]
"""
import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, "/opt/embytg/pipeline")
import todb_titles as TT

VC = "/opt/embytg/pipeline/.todb_vid_cache.json"
OUT = "/opt/embytg/pipeline/.nfo_fix_last.json"


def xml_esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def build_xml(season, ep, info):
    rows = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>",
            "  <title>%s</title>" % xml_esc(info["t"]),
            "  <season>%d</season>" % season, "  <episode>%d</episode>" % ep]
    if info.get("d"):
        rows.append("  <plot>%s</plot>" % xml_esc(info["d"]))
    if info.get("air"):
        rows.append("  <premiered>%s</premiered>" % info["air"])
        rows.append("  <aired>%s</aired>" % info["air"])
    rows.append("</episodedetails>")
    return "\n".join(rows) + "\n"


def disk_seasons(sp):
    out = {}
    for sub in sorted(os.listdir(sp)):
        sd = os.path.join(sp, sub)
        if not os.path.isdir(sd) or not re.match(r"Season\s*\d+$", sub, re.I):
            continue
        sn = int(re.search(r"\d+", sub).group())
        files, eps = [], []
        for f in sorted(os.listdir(sd)):
            if not f.lower().endswith(TT.VIDEO_EXT):
                continue
            m = TT.SE_RE.search(f)
            if not m:
                continue
            files.append(f)
            eps.append(int(m.group(2)))
        if files:
            out[sn] = list(zip(eps, files))
    return out


def resolve(eps, ds, s, e, stats):
    """返回 (todb_season, todb_ep, reason) 或 (None,None,why)"""
    if (s, e) in eps and eps[(s, e)].get("t"):
        return s, e, "A-直接命中"
    pos = sorted({ss for (ss, _ee) in eps if ss > 0})
    if len(pos) == 1:
        M = pos[0]
        n_todb = sum(1 for (ss, _) in eps if ss == M)
        disk_total = sum(len(v) for v in ds.values())
        this_season = len(ds.get(s, []))
        # 磁盘集号是否从 1 连续到 disk_total（跨季接着编号，如 86 的 S02E12..E23）
        seq = sorted(e2 for s2 in sorted(ds) for e2, _f in ds[s2])
        continuous = seq == list(range(1, disk_total + 1))
        ok = (M, e) in eps and eps[(M, e)].get("t")
        if ok and n_todb == this_season:
            return M, e, "B-同季同集号(集数相等)"
        if ok and n_todb == disk_total and continuous:
            return M, e, "B-同季同集号(跨季连续编号)"
        # 磁盘这一季从 1 重新编号 → 偏移 = 之前各季最大集号（能由磁盘自身证明）
        prev = [max(ee for ee, _f in ds[s2]) for s2 in sorted(ds) if s2 < s]
        if seq and min(seq for s2 in [s] for seq in [min(ee for ee, _f in ds[s2])]) == 1 and prev:
            off = max(prev)
            if (M, off + e) in eps and eps[(M, off + e)].get("t"):
                return M, off + e, "C-按前季最大集号偏移+%d" % off
        stats["unfix_merged"] = stats.get("unfix_merged", 0) + 1
        return None, None, "TODB合并季但无法证明映射"
    # 多季 TODB：不做「集号落某季」猜测（可能把 S01E28 误配到 S2E28，仅当磁盘编号跨季连续时才可证明）
    seq_all = sorted(e2 for s2 in sorted(ds) for e2, _f in ds[s2])
    continuous = seq_all == list(range(1, len(seq_all) + 1))
    if continuous and e <= len(seq_all):
        acc = 0
        for ss in sorted({ss for (ss, _ee) in eps if ss > 0}):
            n = sum(1 for (s3, _e3) in eps if s3 == ss)
            if acc < e <= acc + n:
                cand = (ss, e - acc)
                if cand in eps and eps[cand].get("t"):
                    return cand[0], cand[1], "D-跨季累计定位"
                break
            acc += n
    stats["unfix_multi"] = stats.get("unfix_multi", 0) + 1
    return None, None, "TODB多季且无法证明映射"


def plan(lib=None, tid=None):
    vc = json.load(open(VC))
    jobs, notes, detail = [], {"series": 0, "skip": 0}, {}
    for l, sp, sid in TT.iter_series(lib, tid):
        t, y = TT.series_title(sp)
        vid = vc.get("%s|%s" % (t, y))
        if not vid:
            notes["skip"] += 1
            continue
        try:
            eps = TT.todb_eps(vid)
        except Exception:
            notes["skip"] += 1
            continue
        if not eps:
            notes["skip"] += 1
            continue
        notes["series"] += 1
        ds = disk_seasons(sp)
        stats = {}
        per = []
        for s in sorted(ds):
            for e, f in ds[s]:
                p = os.path.join(sp, "Season %02d" % s, f)
                if os.path.exists(os.path.splitext(p)[0] + ".nfo"):
                    continue
                ts, te, why = resolve(eps, ds, s, e, stats)
                if ts is None:
                    per.append((s, e, f, None, why))
                else:
                    per.append((s, e, f, (ts, te), why))
                    jobs.append((p, ts, te, eps[(ts, te)]))
        if per:
            detail["%s %s" % (l, os.path.basename(sp))] = per
    return jobs, notes, detail


def do(job):
    path, s, e, info = job
    nfo = os.path.splitext(path)[0] + ".nfo"
    try:
        with open(nfo, "w", encoding="utf-8") as f:
            f.write(build_xml(s, e, info))
        return "write"
    except Exception as ex:
        return "fail:%s" % str(ex)[:60]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    t0 = time.time()
    jobs, notes, detail = plan(a.lib, a.series)
    reasons = {}
    for k, per in detail.items():
        for s, e, f, m, why in per:
            reasons[why] = reasons.get(why, 0) + 1
    print("# 可补 NFO %d 条 | 涉及剧 %d | 无TODB片 %d | 原因分布 %s"
          % (len(jobs), len(detail), notes["skip"], reasons), flush=True)
    shown = 0
    for k, per in list(detail.items()):
        m = [(s, e, f, mm, why) for s, e, f, mm, why in per if mm]
        if not m:
            continue
        print("  %s" % k, flush=True)
        for s, e, f, mm, why in m[:3]:
            print("     S%02dE%02d -> TODB S%dE%-3d [%s] %s" % (s, e, mm[0], mm[1], why, f[:40]), flush=True)
        shown += 1
        if shown >= 12:
            break
    if a.scan or not a.apply or not jobs:
        json.dump({"ts": time.strftime("%H:%M:%S"), "jobs": len(jobs), "reasons": reasons},
                  open(OUT, "w"), ensure_ascii=False, indent=1)
        return
    stats = {}
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        for f in as_completed([ex.submit(do, j) for j in jobs]):
            r = f.result(); k = r.split(":")[0]; stats[k] = stats.get(k, 0) + 1
    print("# 完成 %s / %.1fs" % (stats, time.time() - t0))
    json.dump({"ts": time.strftime("%H:%M:%S"), "jobs": len(jobs), "stats": stats, "reasons": reasons},
              open(OUT, "w"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
