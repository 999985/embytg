#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""audit_naming.py —— 以 TODB 为唯一基准审计磁盘命名

检查四类问题（根本原因分类）：
  no-se      文件名里没有 SxxExx（Emby IndexNumber=null，被当成额外条目）
  over       集号超出该季 TODB 真实集数（源里多出来的集 / 旧映射塞错季）
  dup        同季同集同画质多份文件（Emby 会显示成重复条目）
  miss       TODB 有、磁盘没有（信息项，不算错误）

用法:
  python3 audit_naming.py --audit [--lib 国产剧] [--series 103147]
  python3 audit_naming.py --fix-no-se --apply     # 按 TODB 标题精确匹配补回集号
  python3 audit_naming.py --fix-dup  --apply      # 同集同画质多余文件移到 /opt/embytg/dups
"""
import argparse
import json
import os
import re
import shutil
import sys
import time

import todb_titles as TT

VID_CACHE = "/opt/embytg/pipeline/.todb_vid_cache.json"
SE_RE = re.compile(r"S(\d{1,2})E(\d{1,4})", re.I)
VID_EXT = TT.VIDEO_EXT


def load_vid_cache():
    if os.path.exists(VID_CACHE):
        try:
            return json.load(open(VID_CACHE))
        except Exception:
            pass
    return {}


def vid_for(title, year, cache):
    k = "%s|%s" % (title, year)
    if k in cache:
        return cache[k]
    vid, _ = TT.resolve(title, year)
    cache[k] = vid
    json.dump(cache, open(VID_CACHE, "w"), ensure_ascii=False)
    return vid


def seasons_of(eps):
    """{season: {ep: info}}"""
    out = {}
    for (s, e), info in eps.items():
        out.setdefault(s, {})[e] = info
    return out


def walk_series(sp):
    out = {}
    for sub in sorted(os.listdir(sp)):
        sd = os.path.join(sp, sub)
        if not os.path.isdir(sd) or not re.match(r"Season\s*\d+$", sub, re.I):
            continue
        files = [f for f in sorted(os.listdir(sd)) if f.lower().endswith(VID_EXT)]
        if files:
            out[sub] = (sd, files)
    return out


def audit(lib=None, tid=None):
    vcache = load_vid_cache()
    rows = []
    for l, sp, sid in TT.iter_series(lib, tid):
        title, year = TT.series_title(sp)
        vid = vid_for(title, year, vcache)
        if not vid:
            rows.append({"lib": l, "series": title, "tmdb": sid, "vid": None, "problem": "todb-miss"})
            continue
        eps = TT.todb_eps(vid)
        se = seasons_of(eps)
        disk = walk_series(sp)
        no_se, over, dup = [], [], []
        for sub, (sd, files) in disk.items():
            sn = int(re.search(r"\d+", sub).group())
            want = se.get(sn) or {}
            bykey = {}
            for f in files:
                m = SE_RE.search(f)
                if not m:
                    no_se.append((sub, f))
                    continue
                s2, e2 = int(m.group(1)), int(m.group(2))
                if not want or e2 not in want:
                    over.append((sub, e2, f))
                qm = TT.QUAL_RE.search(os.path.splitext(f)[0])
                key = (s2, e2, qm.group(1) if qm else "SD")
                bykey.setdefault(key, []).append(f)
            for k, fs in bykey.items():
                if len(fs) > 1:
                    dup.append((sub, k, fs))
        if no_se or over or dup:
            rows.append({"lib": l, "series": title, "tmdb": sid, "vid": vid,
                         "no_se": no_se, "over": over, "dup": dup,
                         "seasons": {k: len(v) for k, v in se.items()}})
    return rows


def report(rows):
    tot = {"series": len(rows), "no_se": 0, "over": 0, "dup": 0}
    for r in rows:
        tot["no_se"] += len(r.get("no_se") or [])
        tot["over"] += len(r.get("over") or [])
        tot["dup"] += sum(len(x[2]) for x in (r.get("dup") or []))
    print("有问题的剧集 %d 部；无集号 %d 个文件；超纲集 %d 个文件；重复 %d 个文件"
          % (tot["series"], tot["no_se"], tot["over"], tot["dup"]))
    for r in rows[: int(os.environ.get("SHOW", "15"))]:
        if r.get("problem"):
            print("  [%s] %s tmdb=%s -> %s" % (r["lib"], r["series"], r["tmdb"], r["problem"]))
            continue
        print("  [%s] %s vid=%s no_se=%d over=%d dup=%d"
              % (r["lib"], r["series"], r["vid"], len(r["no_se"]), len(r["over"]),
                 sum(len(x[2]) for x in r["dup"])))
    return tot


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--json-out")
    a = ap.parse_args()
    t0 = time.time()
    rows = audit(a.lib, a.series)
    tot = report(rows)
    if a.series:
        for r in rows:
            for sub, f in r.get("no_se") or []:
                print("   no-se  %s | %s" % (sub, f[:80]))
            for sub, e2, f in r.get("over") or []:
                print("   over   %s E%s | %s" % (sub, e2, f[:80]))
            for sub, k, fs in r.get("dup") or []:
                print("   dup    %s %s | %s" % (sub, k, fs))
    if a.json_out:
        json.dump(rows, open(a.json_out, "w"), ensure_ascii=False, indent=1)
        print("明细 ->", a.json_out)
    print("耗时 %.1fs" % (time.time() - t0))
