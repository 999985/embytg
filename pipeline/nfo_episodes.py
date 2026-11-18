#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nfo_episodes.py —— 为每一集生成 Episode 级 NFO，把 TODB `episode_title` 固化成持久元数据

背景（2026-11-09）：
  todb_meta.py 只能把 TODB 集标题一次性 POST 进 Emby 数据库；本库没有逐集 NFO，
  Emby 的 Episode Name 实际来自文件名。任何改名/重扫都会把 Name 打回
  「剧名 SxxEyy - 标题 - 2160p」。根治办法是写 <video>.nfo，让 BaseNfoParser 的
  <title> 成为权威来源。

生成内容（与媒体文件同名的 .nfo）：
  <episodedetails>
    <title>   TODB episode_title          -> Emby Episode Name
    <plot>    TODB episode_description     -> Emby Overview（有才写）
    <premiered>/<aired> TODB date_air       -> Emby PremiereDate（有才写）
    <season>/<episode>                      -> 与文件名 SxxEyy 对齐
  </episodedetails>

用法（服务器 cd /opt/embytg/pipeline）：
  python3 nfo_episodes.py --scan                 # 只统计，不写文件
  python3 nfo_episodes.py                        # 全量生成（幂等，已一致的跳过）
  python3 nfo_episodes.py --lib 国产剧 --workers 8
  python3 nfo_episodes.py --series 139162        # 单剧（tmdb id）
"""
import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import todb_titles as TT

VID_CACHE = "/opt/embytg/pipeline/.todb_vid_cache.json"
MISS_CACHE = "/opt/embytg/pipeline/.todb_vid_miss.json"
MISS_TTL = 7 * 86400          # TODB 没收录/候选不唯一 → 7 天内不重复查
OUT = "/opt/embytg/pipeline/nfo_episodes_last.json"


def load_vid_cache():
    try:
        return json.load(open(VID_CACHE))
    except Exception:
        return {}


def load_miss_cache():
    try:
        return json.load(open(MISS_CACHE))
    except Exception:
        return {}


def save_miss_cache(d):
    tmp = MISS_CACHE + ".tmp.%d" % os.getpid()
    json.dump(d, open(tmp, "w"), ensure_ascii=False)
    os.replace(tmp, MISS_CACHE)


def xml_esc(s):
    if not s:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


# 2026-11-16（E124）：TODB 有时只有占位集标题（"第 7 集"）。
# 这类标题不是真标题，写进 NFO 会被 post_ingest 判 placeholder → 收尾 FAIL；
# 更糟的是会把 nfo_episodes_tmdb 已经写好的真实集标题覆盖掉（实测「我的有罪之人」
# E01–E06 被从「暴露危机/双面人生/…」改回「第 N 集」）。占位一律不写。
PLACEHOLDER = re.compile(r"^第\s*\d+\s*[集期]$|^Episode\s*\d+$", re.I)


def build_xml(season, ep, info):
    rows = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>",
            "  <title>%s</title>" % xml_esc(info["t"]),
            "  <season>%d</season>" % season,
            "  <episode>%d</episode>" % ep]
    if info.get("d"):
        rows.append("  <plot>%s</plot>" % xml_esc(info["d"]))
    if info.get("air"):
        rows.append("  <premiered>%s</premiered>" % info["air"])
        rows.append("  <aired>%s</aired>" % info["air"])
    rows.append("</episodedetails>")
    return "\n".join(rows) + "\n"


def _invalidate_title_cache(nfo_path, title):
    """本脚本写入 NFO 后，作废 nfo_episodes_tmdb 的「已写过」记账。

    两张工具写同一个 .nfo 路径：nfo_episodes.py 写 TODB 权威标题，
    nfo_episodes_tmdb.py 用 .nfo_title_cache.db 判「不用重写」。
    不通知它，它会拿旧标题当现状跳过，用户看到的仍是旧标题（实测 2026-11-16）。
    """
    try:
        import sqlite3
        db = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".nfo_title_cache.db")
        con = sqlite3.connect(db, timeout=20)
        try:
            con.execute("delete from nfo_title where path=?", (nfo_path,))
            con.commit()
        finally:
            con.close()
    except Exception:
        pass


def up_to_date(path, info):
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            s = f.read()
    except OSError:
        return False
    return "<title>%s</title>" % xml_esc(info["t"]) in s


def do_one(job):
    path, season, ep, info = job
    nfo = os.path.splitext(path)[0] + ".nfo"
    if up_to_date(nfo, info):
        return "skip"
    try:
        with open(nfo, "w", encoding="utf-8") as f:
            f.write(build_xml(season, ep, info))
        _invalidate_title_cache(nfo, info["t"])
        return "write"
    except Exception as e:
        return "fail:%s" % str(e)[:80]


def plan(lib=None, tid=None, limit=0):
    vcache = load_vid_cache()
    jobs, notes = [], {"no_vid": 0, "no_title": 0, "no_se": 0, "series": 0, "no_eps": 0,
                       "resolved": 0}
    for l, sp, sid in TT.iter_series(lib, tid):
        title, year = TT.series_title(sp)
        # 目录名里的「(未知)」是年份没查到时的占位符，不是剧名的一部分；先剥掉再查缓存，
        # 否则 key「虎与龙 (未知)|None」永远命中不了 → 这部剧一辈子没有 NFO（2026-11-18 实测 32 部）。
        qtitle = TT.query_title(title)
        vid = vcache.get("%s|%s" % (title, year)) or vcache.get("%s|%s" % (qtitle, year))
        if not vid:
            # 缓存没有就现查一次 TODB；只有候选唯一才落缓存，避免把同名多条目选错固化（E021）。
            neg = {}
            try:
                neg = load_miss_cache()
            except Exception:
                neg = {}
            nkey = "%s|%s" % (qtitle, year)
            if time.time() - (neg.get(nkey) or 0) < MISS_TTL:
                notes["neg_cached"] = notes.get("neg_cached", 0) + 1
            else:
                cands = []
                try:
                    cands = TT.resolve_all(qtitle, year)
                except Exception:
                    cands = []
                if len(cands) == 1:
                    vid = cands[0]["vid"]
                    vcache[nkey] = vid
                    notes["resolved"] += 1
                    try:
                        tmp = VID_CACHE + ".tmp.%d" % os.getpid()
                        json.dump(vcache, open(tmp, "w"), ensure_ascii=False)
                        os.replace(tmp, VID_CACHE)
                    except Exception:
                        pass
                    if nkey in neg:
                        neg.pop(nkey, None)
                        try:
                            save_miss_cache(neg)
                        except Exception:
                            pass
                else:
                    # 负缓存：TODB 确实没有 / 候选不唯一 → 7 天内不再重复查。
                    # 不加这个，232 部无 vid 剧每跑一次 plan 就要 232 次 TODB 搜索（实测 6 分钟）。
                    try:
                        neg[nkey] = int(time.time())
                        save_miss_cache(neg)
                    except Exception:
                        pass
        if not vid:
            notes["no_vid"] += 1
            continue
        try:
            eps = TT.todb_eps(vid)
        except Exception:
            notes["no_vid"] += 1
            continue
        notes["series"] += 1
        n = 0
        for sub in sorted(os.listdir(sp)):
            sd = os.path.join(sp, sub)
            if not os.path.isdir(sd) or not re.match(r"Season\s*\d+$", sub, re.I):
                continue
            sn = int(re.search(r"\d+", sub).group())
            for f in sorted(os.listdir(sd)):
                if not f.lower().endswith(TT.VIDEO_EXT):
                    continue
                m = TT.SE_RE.search(f)
                if not m:
                    notes["no_se"] += 1
                    continue
                s2, e2 = int(m.group(1)), int(m.group(2))
                info = eps.get((s2, e2))
                if not info or not info.get("t"):
                    notes["no_title"] += 1
                    continue
                if PLACEHOLDER.match(str(info.get("t")).strip()):
                    notes["placeholder"] = notes.get("placeholder", 0) + 1
                    continue
                jobs.append((os.path.join(sd, f), s2, e2, info))
                n += 1
        if not n:
            notes["no_eps"] += 1
        if limit and len(jobs) >= limit:
            break
    return jobs, notes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    t0 = time.time()
    jobs, notes = plan(a.lib, a.series, a.limit)
    print("# 计划写 NFO %d 个 | 剧=%d 无TODB=%d 无标题=%d 无集号=%d 空剧=%d 占位跳过=%d"
          % (len(jobs), notes["series"], notes["no_vid"], notes["no_title"],
             notes["no_se"], notes["no_eps"], notes.get("placeholder", 0)), flush=True)
    for p, s, e, info in jobs[:8]:
        print("   S%02dE%02d %-28s | %s" % (s, e, os.path.basename(p)[:28], info["t"][:40]))
    if a.scan or not jobs:
        return
    stats = {}
    done = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(do_one, j) for j in jobs]
        for f in as_completed(futs):
            r = f.result()
            k = r.split(":")[0]
            stats[k] = stats.get(k, 0) + 1
            done += 1
            if done % 500 == 0:
                print("   .. %d/%d %s %.0fs" % (done, len(jobs), stats, time.time() - t0), flush=True)
    print("# 完成 %s / %.1fs" % (stats, time.time() - t0))
    try:
        json.dump({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "total": len(jobs),
                   "stats": stats, "notes": notes}, open(OUT, "w"), ensure_ascii=False, indent=1)
    except Exception:
        pass


if __name__ == "__main__":
    main()
