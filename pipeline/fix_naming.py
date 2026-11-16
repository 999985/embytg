#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fix_naming.py —— 按 TODB 基准根治集号/命名问题

四类问题与处理：
  no-se    文件名无 SxxExx → 用 TODB 标题精确/唯一匹配补回 (季,集)，重命名为标准式
  multiver 同季同集多份文件（1080p + 2160p 等）→ 保留最高画质（同画质取最大体积），
           其余移到 /opt/embytg/dups/（这是 Emby 里「多出来几集」的主因：同集两条条目）
  over-a   集号 ≥100 且该季无此集 → 视为日期残留，用 renumber.map_date 重新映射
  over-b   集号 <100 但超出 TODB 该季集数，且 TODB 季结构可信 → 移入 Season 00 续号

可信判据：TODB 最大单季集数 < 0.8×磁盘总数（说明 TODB 是按季正确拆分的，
不是把全集塞进同一季）；否则该剧 TODB 数据不可信，over-b 只报告不动。

用法:
  python3 fix_naming.py --plan [--series 103147] [--only no-se,multiver,over-a,over-b]
  python3 fix_naming.py --apply ...
  python3 fix_naming.py --rollback fix_naming_<ts>.json
"""
from nfo_sidecar import rename_with_sidecars   # 改名同步搬 .nfo/海报（E016 防孤儿）
import argparse
import json
import os
import re
import shutil
import sys
import time

import audit_naming as AU
import renumber_todb as R
import todb_titles as TT

DUP_DIR = "/opt/embytg/mnt/embytg/TV/_dups"   # 放在 TG 挂载内、任何库目录之外：字节进 teldrive，Emby 扫不到
SE_RE = AU.SE_RE
QRANK = {"2160p": 4, "1440p": 3, "1080p": 2, "720p": 1, "SD": 0, "4K": 4}


def norm(s):
    return re.sub(r"[\s\-_]+", "", s or "").lower()


# 「 - 」后的尾段不一定是集标题：可能只是画质/编码标记。
# 旧代码把尾段无条件当标题再交给 build_name，于是再拼一次画质 →
# 「少年白马醉春风 S00E02 - 2160p - 2160p.mp4」（2026-11-16 实测全库 38 个文件、
# 全部落在 Season 00，且这些剧的 tvshow.nfo 也跟着缺）。
QUAL_TOKEN_RE = re.compile(
    r"^(?:\d{3,4}[pi]|4k|uhd|hdr|sdr|hevc|h\.?26[45]|avc|x26[45]|web-?dl|webrip|bluray|bdrip|"
    r"remux|hdtv|\d{2,3}fps|\d+bit|aac|ddp?|eac3|dts|flac|truehd|atmos|"
    r"国语|粤语|国粤|中字|简中|繁中|双语|内嵌|外挂|无水印)$", re.I)


def s0_ep_title(stem):
    """从文件名里取「真正的集标题」；取不到就返回空串（调用方走无标题分支）。"""
    if " - " not in stem:
        return ""
    seg = stem.rsplit(" - ", 1)[-1].strip()
    seg = TT.QUAL_RE.sub("", seg).strip(" -.，,;")
    seg = re.sub(r"^\d+[\.\、]\s*", "", seg).strip()
    if not seg or QUAL_TOKEN_RE.match(seg) or SE_RE.search(seg):
        return ""
    return TT.sanitize(seg)


def s0_filename(title, ep, stem, qual, ext):
    """over-b 移入 Season 00 时的目标文件名（唯一入口，方便回归测试）。"""
    t = s0_ep_title(stem)
    if t:
        return TT.build_name(title, 0, ep, t, qual, ext.lower())
    return "%s S%02dE%02d - %s.%s" % (TT.sanitize(title, 40), 0, ep, qual, ext.lstrip(".").lower())


def match_title(stem, eps):
    core = TT.QUAL_RE.sub("", stem).strip()
    core = re.sub(r"^\d+[\.\、]\s*", "", core).strip()
    n = norm(core)
    if len(n) < 4:
        return []
    exact = [(s, e, i["t"]) for (s, e), i in eps.items() if norm(i["t"]) == n]
    if exact:
        return exact
    part = [(s, e, i["t"]) for (s, e), i in eps.items() if n and (n in norm(i["t"]))]
    return part if len(part) == 1 else []


def plan_series(sp, vid, eps, st=None):
    title, year = TT.series_title(sp)
    se = AU.seasons_of(eps)

    # 季结构可信度（S0 是特辑，不参与判据）
    disk_total = 0
    for sub, (sd, files) in AU.walk_series(sp).items():
        disk_total += len(files)
    max_season = max([len(v) for k, v in se.items() if k > 0] or [0])
    trusted = disk_total > 0 and max_season < 0.8 * disk_total and len(se) >= 3

    s0_max = max(list(se.get(0) or {}) or [0])
    next_s0 = s0_max
    todo = []

    def s0_slot(stem, ext, qual, note):
        nonlocal next_s0
        next_s0 += 1
        newname = s0_filename(title, next_s0, stem, qual, ext)
        return {"kind": "over-b", "src": None, "dst": os.path.join(sp, "Season 00", newname),
                "note": "%s -> S0E%d" % (note, next_s0)}

    for sub, (sd, files) in AU.walk_series(sp).items():
        sn = int(re.search(r"\d+", sub).group())
        want = se.get(sn) or {}
        groups = {}
        for f in files:
            stem, ext = os.path.splitext(f)
            m = SE_RE.search(f)
            if not m:
                hit = match_title(stem, eps)
                qm = TT.QUAL_RE.search(stem)
                qual = qm.group(1) if qm else "SD"
                if len(hit) == 1:
                    s2, e2, t = hit[0]
                    newname = TT.build_name(title, s2, e2, TT.sanitize(t), qual, ext.lower())
                    todo.append({"kind": "no-se", "src": os.path.join(sd, f),
                                 "dst": os.path.join(sd, newname), "note": "title->S%dE%d" % (s2, e2)})
                else:
                    todo.append({"kind": "no-se", "src": os.path.join(sd, f), "dst": None,
                                 "note": "unmatched(%d)" % len(hit)})
                continue
            groups.setdefault(int(m.group(2)), []).append((f, stem, ext))

        for e2, items in sorted(groups.items()):
            extras = sorted(e for e in groups if e not in want and e < 100)
            wmax = max(list(want) or [0])
            contig = bool(extras) and extras == list(range(wmax + 1, wmax + 1 + len(extras)))
            allow_s0 = trusted and contig and len(extras) <= max(8, len(want))
            scored = []
            for f, stem, ext in items:
                qm = TT.QUAL_RE.search(stem)
                q = qm.group(1) if qm else "SD"
                scored.append((QRANK.get(q, 0), os.path.getsize(os.path.join(sd, f)), f, stem, ext, q))
            scored.sort(reverse=True)
            keep = scored[0]
            for it in scored[1:]:
                todo.append({"kind": "multiver", "src": os.path.join(sd, it[2]), "dst": None,
                             "note": "keep " + keep[2][:44]})
            f, stem, ext, q = keep[2], keep[3], keep[4], keep[5]
            if e2 in want:
                continue
            hit = None
            note = "S%dE%d" % (sn, e2)
            if e2 >= 100 and st:
                py = R.season_year(st, sn) or year
                hit, why = R.map_date(st, "%04d" % e2, py, sn)
                note += " date(%s)" % why
            if hit:
                s2, e2b, _air = hit
                info = eps.get((s2, e2b))
                if info and info["t"]:
                    newname = TT.build_name(title, s2, e2b, TT.sanitize(info["t"]), q, ext.lower())
                    todo.append({"kind": "over-a", "src": os.path.join(sd, f),
                                 "dst": os.path.join(sp, "Season %02d" % s2, newname),
                                 "note": note + " -> S%dE%d" % (s2, e2b)})
                    continue
            if allow_s0:
                it = s0_slot(stem, ext, q, note)
                it["src"] = os.path.join(sd, f)
                todo.append(it)
            else:
                todo.append({"kind": "over-x", "src": os.path.join(sd, f), "dst": None,
                             "note": note + (" TODB季结构不可信" if not trusted else " 非连续续号，跳过")})
    return todo


def build_plan(lib=None, tid=None, only=None):
    vcache = AU.load_vid_cache()
    plan = []
    for l, sp, sid in TT.iter_series(lib, tid):
        title, year = TT.series_title(sp)
        vid = AU.vid_for(title, year, vcache)
        if not vid:
            continue
        eps = TT.todb_eps(vid)
        if not eps:
            continue
        try:
            st = R.series_struct(sid)
        except Exception:
            st = None
        for it in plan_series(sp, vid, eps, st):
            if only and it["kind"] not in only:
                continue
            it.update({"lib": l, "series": title, "tmdb": sid, "vid": vid})
            plan.append(it)
    return plan


def apply(plan):
    ts = time.strftime("%Y%m%d_%H%M%S")
    jp = "/opt/embytg/pipeline/fix_naming_%s.json" % ts
    done, fail = [], []
    for it in plan:
        if not it.get("dst") and it["kind"] != "multiver":
            fail.append({"src": it["src"], "why": it.get("note") or "no-dst", "kind": it["kind"]})
            continue
        src = it["src"]
        dst = it.get("dst") or (it["src"] + ".dup")
        if not os.path.exists(src):
            fail.append({"src": src, "why": "missing", "kind": it["kind"]})
            continue
        if os.path.exists(dst):
            fail.append({"src": src, "why": "dst-exists", "kind": it["kind"]})
            continue
        try:
            if it["kind"] == "multiver":
                # 同目录内改名加 .dup 后缀：Emby 不识别该扩展名 → 条目消失；rclone 元数据操作，不搬字节
                rename_with_sidecars(src, dst)
            else:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                rename_with_sidecars(src, dst)
            done.append({"kind": it["kind"], "from": src, "to": dst})
        except Exception as e:
            fail.append({"src": src, "why": str(e)[:120], "kind": it["kind"]})
    json.dump({"ts": ts, "done": done, "fail": fail}, open(jp, "w"), ensure_ascii=False, indent=1)
    stats = {}
    for d in done:
        stats[d["kind"]] = stats.get(d["kind"], 0) + 1
    print("已修 %d 条 %s；未处理 %d 条 -> %s" % (len(done), stats, len(fail), jp))
    for f in fail[:12]:
        print("   ! %-8s %s | %s" % (f["kind"], os.path.basename(f["src"])[:50], f["why"]))
    return jp


def rollback(path):
    j = json.load(open(path))
    ok = bad = 0
    for d in j.get("done") or []:
        frm, to = d["from"], d["to"]
        if os.path.exists(to) and not os.path.exists(frm):
            rename_with_sidecars(to, frm)
            ok += 1
        else:
            bad += 1
    print("回滚 %d 条，跳过 %d 条" % (ok, bad))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--only")
    ap.add_argument("--rollback")
    a = ap.parse_args()
    if a.rollback:
        rollback(a.rollback)
        sys.exit(0)
    only = set(a.only.split(",")) if a.only else None
    plan = build_plan(a.lib, a.series, only)
    stats = {}
    for p in plan:
        stats[p["kind"]] = stats.get(p["kind"], 0) + 1
    print("计划 %d 条 %s" % (len(plan), stats))
    for p in plan[:14]:
        print("   %-9s %s | %s" % (p["kind"], os.path.basename(p["src"])[:48], (p.get("note") or "")[:42]))
    if a.apply:
        apply(plan)
