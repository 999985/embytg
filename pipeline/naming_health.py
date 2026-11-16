#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""naming_health.py —— 命名/刮削一键体检（只读）

回答四个问题，全部基于实测，不看 Emby 的扫描进度：
  1. 库结构对不对（应有 10 电影 + 7 剧集，每库 Locations 恰好 1 条且目录存在）
  2. 刮削完成度（磁盘视频 vs 同名 .nfo 覆盖率）
  3. Emby 索引对不对（每库 Episode 数 vs 磁盘视频数、幽灵条目数）
  4. 标题对不对（Episode.Name 是否等于 NFO 的 <title>）

依据：PROJECT.md §一（刮削=NFO 前置，Emby 只读本地文件）、ERRORS E019/E020。

用法：
  cd /opt/embytg/pipeline
  python3 naming_health.py                 # 全量
  python3 naming_health.py --lib 综艺      # 单库
  python3 naming_health.py --no-name       # 跳过 Name 比对（快，省 2 分钟）
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

c = json.load(open("/opt/embytg/config.json"))
T = c["emby_token"]
B = c.get("emby_url", "http://127.0.0.1:8096").rstrip("/")
MNT = "/opt/embytg/mnt/embytg"
CTR = "/mnt/embytg"
MOVIE_LIBS = ["剧情片", "喜剧片", "动作片", "惊悚片", "犯罪片", "科幻片",
              "动画片", "纪录片", "战争片", "其他片"]
TV_LIBS = ["国产剧", "日韩剧", "欧美剧", "港台剧", "国漫", "日漫", "综艺"]
VIDEO_EXT = (".mp4", ".mkv", ".ts", ".m4v", ".mov", ".avi", ".wmv")
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S)


def api(path, **q):
    u = B + path + ("?" + urllib.parse.urlencode(q) if q else "")
    req = urllib.request.Request(u, headers={"X-Emby-Token": T})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


def virtual_folders():
    return api("/Library/VirtualFolders")


def episodes(lib_id):
    out, start = [], 0
    while True:
        d = api("/Items", ParentId=lib_id, Recursive="true", IncludeItemTypes="Episode",
                Fields="Name,Path", Limit=500, StartIndex=start)
        it = d.get("Items") or []
        out += it
        if len(out) >= (d.get("TotalRecordCount") or 0) or not it:
            return out
        start += len(it)


def disk_stats(path):
    n = 0
    clips = []
    for root, _, fs in os.walk(path):
        for f in fs:
            if f.lower().endswith(VIDEO_EXT):
                n += 1
                clips.append(os.path.join(root, f))
    return n, clips


def audit_extras():
    """额外巡检：孤儿 NFO / 同 tmdb 多目录 / 备份包新鲜度（E016/E021）"""
    out = {"orphan_nfo": 0, "dup_tmdb": [], "backup": None}
    tm = {}
    for lib in TV_LIBS:
        D = os.path.join(MNT, "TV", lib)
        if not os.path.isdir(D):
            continue
        for s in os.listdir(D):
            p = os.path.join(D, s)
            if not os.path.isdir(p):
                continue
            m = re.search(r"\{tmdb=(\d+)\}", s)
            if m:
                tm.setdefault(m.group(1), []).append("%s/%s" % (lib, s))
    out["dup_tmdb"] = {k: v for k, v in tm.items() if len(v) > 1}
    for lib in TV_LIBS:
        D = os.path.join(MNT, "TV", lib)
        for root, _, fs in os.walk(D):
            names = set(fs)
            for f in fs:
                if not f.endswith(".nfo") or f == "tvshow.nfo":
                    continue
                base = f[:-4]
                if base.endswith(".dup"):
                    continue          # fix_naming 的 .dup 副本，配套 NFO 允许保留
                if not any(base + e in names for e in VIDEO_EXT):
                    out["orphan_nfo"] += 1
    try:
        lat = json.load(open("/opt/embytg/backup/LATEST.json"))
        ts = lat.get("created") or ""
        out["backup"] = (lat.get("state"), ts.strip())
    except Exception:
        out["backup"] = None
    return out


def series_title_audit(deep=False):
    """校验 Series 中文名：默认只看 Emby；deep 时再逐部读 NFO（慢，低峰用）。"""
    d = api("/Items", Recursive="true", IncludeItemTypes="Series",
            Fields="Path,Name", Limit=5000)
    checked, bad = 0, []
    for x in d.get("Items") or []:
        p = (x.get("Path") or "").rstrip("/")
        if not p:
            continue
        base = os.path.basename(p)
        folder = re.sub(r"\s*\{tmdb=\d+\}\s*$", "", base, flags=re.I)
        folder = re.sub(r"\s*\(\d{4}\)\s*$", "", folder).strip()
        if not re.search(r"[\u4e00-\u9fff]", folder):
            continue
        checked += 1
        name = (x.get("Name") or "").strip()
        if not re.search(r"[\u4e00-\u9fff]", name):
            bad.append("%s: Emby=%r，目录=%r" % (folder[:28], name, folder))
            continue
        if not deep:
            continue
        nfo_title = ""
        try:
            with open(os.path.join(p.replace(CTR, MNT), "tvshow.nfo"),
                      encoding="utf-8", errors="ignore") as f:
                txt = f.read()
            nm = TITLE_RE.search(txt)
            nfo_title = nm.group(1).strip() if nm else ""
        except OSError:
            pass
        if nfo_title and not re.search(r"[\u4e00-\u9fff]", nfo_title):
            bad.append("%s: NFO=%r，目录=%r" % (folder[:28], nfo_title, folder))
        elif not nfo_title:
            bad.append("%s: tvshow.nfo 缺标题" % folder[:28])
    return checked, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib")
    ap.add_argument("--no-name", action="store_true")
    ap.add_argument("--deep-title", action="store_true",
                    help="逐部读取 tvshow.nfo 校验标题真源（慢，低峰用）")
    a = ap.parse_args()
    t0 = time.time()
    vfs = virtual_folders()
    mvs = [x for x in vfs if x.get("CollectionType") == "movies"]
    tvs = [x for x in vfs if x.get("CollectionType") == "tvshows"]
    print("== 1) 库结构 ==")
    print("   电影库 %d（期望 %d）  剧集库 %d（期望 %d）  合计 %d"
          % (len(mvs), len(MOVIE_LIBS), len(tvs), len(TV_LIBS), len(vfs)))
    bad = 0
    for x in vfs:
        locs = x.get("Locations") or []
        probs = []
        if len(locs) != 1:
            probs.append("Locations=%d" % len(locs))
        for l in locs:
            if not os.path.isdir(l.replace(CTR, MNT)):
                probs.append("目录不存在:%s" % l)
        if probs:
            bad += 1
            print("   ✗ %-8s %s" % (x.get("Name"), " | ".join(probs)))
    extra = [x["Name"] for x in tvs if x["Name"] not in TV_LIBS] + \
            [x["Name"] for x in mvs if x["Name"] not in MOVIE_LIBS]
    if extra:
        print("   ! 不在预期清单的库: %s" % extra)
    print("   结构异常库: %d" % bad)

    print("\n== 2) 刮削完成度（NFO 覆盖率）+ 3) Emby 索引 ==")
    print("   %-6s %-7s %-9s %-9s %-7s %-7s %s"
          % ("库", "磁盘", "有NFO", "覆盖率", "Emby集", "幽灵", "Name==NFO"))
    tot = {"disk": 0, "nfo": 0, "emby": 0, "ghost": 0, "ok": 0, "nonfo": 0}
    targets = [(x["Name"], x["Id"]) for x in tvs]
    if a.lib:
        targets = [t for t in targets if t[0] == a.lib]
    for name, lid in targets:
        d, clips = disk_stats(os.path.join(MNT, "TV", name))
        nfo = sum(1 for p in clips if os.path.exists(os.path.splitext(p)[0] + ".nfo"))
        eps = episodes(lid)
        ghost = sum(1 for e in eps
                    if not os.path.exists((e.get("Path") or "").replace(CTR, MNT)))
        ok = nonfo = 0
        if not a.no_name:
            for e in eps:
                p = (e.get("Path") or "").replace(CTR, MNT)
                np = os.path.splitext(p)[0] + ".nfo"
                if not os.path.exists(np):
                    nonfo += 1
                    continue
                try:
                    m = TITLE_RE.search(open(np, encoding="utf-8", errors="ignore").read())
                except OSError:
                    m = None
                if m and (e.get("Name") or "").strip() == m.group(1).strip():
                    ok += 1
        print("   %-6s %-7d %-9d %-9s %-7d %-7d %s"
              % (name, d, nfo, "%.1f%%" % (100.0 * nfo / d if d else 0),
                 len(eps), ghost, ("%d/%d" % (ok, ok + nonfo)) if not a.no_name else "-"))
        tot["disk"] += d; tot["nfo"] += nfo; tot["emby"] += len(eps)
        tot["ghost"] += ghost; tot["ok"] += ok; tot["nonfo"] += nonfo
    print("   %-6s %-7d %-9d %-9s %-7d %-7d %s"
          % ("合计", tot["disk"], tot["nfo"],
             "%.1f%%" % (100.0 * tot["nfo"] / tot["disk"] if tot["disk"] else 0),
             tot["emby"], tot["ghost"],
             ("%d/%d" % (tot["ok"], tot["ok"] + tot["nonfo"])) if not a.no_name else "-"))

    ex = audit_extras()
    print("\n== 3.5) 额外巡检 ==")
    print("   孤儿 NFO（有 .nfo 没同名视频，E016）: %d" % ex["orphan_nfo"])
    print("   同一 tmdb id 多目录: %d 组%s" % (len(ex["dup_tmdb"]),
          "" if not ex["dup_tmdb"] else " -> " + str(list(ex["dup_tmdb"].items())[:3])))
    print("   备份包 LATEST: %s" % (ex["backup"] or "缺失","")if False else "   备份包 LATEST: %s" % (ex["backup"],))
    title_checked, title_bad = series_title_audit(a.deep_title)
    print("   Series 中文名: %d 部有中文目录，异常 %d 部" %
          (title_checked, len(title_bad)))
    for x in title_bad[:5]:
        print("      ✘ %s" % x[:120])

    print("\n== 4) 结论 ==")
    verdict = []
    verdict.append(("库结构", bad == 0 and not extra))
    verdict.append(("索引对齐", tot["emby"] == tot["disk"] and tot["ghost"] == 0))
    verdict.append(("无孤儿NFO", ex["orphan_nfo"] == 0))
    verdict.append(("无重复目录", not ex["dup_tmdb"]))
    verdict.append(("Series中文名", not title_bad))
    if not a.no_name:
        verdict.append(("标题一致", tot["ok"] + tot["nonfo"] == tot["emby"]))
    for k, v in verdict:
        print("   %-6s %s" % (k, "PASS" if v else "FAIL"))
    print("   缺 NFO %d 条（%.1f%% 已覆盖）；耗时 %.0fs"
          % (tot["disk"] - tot["nfo"], 100.0 * tot["nfo"] / tot["disk"] if tot["disk"] else 0,
             time.time() - t0))
    return 0 if all(v for _, v in verdict) else 1


if __name__ == "__main__":
    sys.exit(main())
