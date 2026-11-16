#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""renumber_todb.py —— 剧集命名标准化（TODB / TMDB 编号）

背景（用户 m00001）：综艺按「日期」编集，落盘成 `现在就出发 S04E1001 - 2160p.mp4`
（1001 = 10 月 01 日），集号非法，Emby 与 TODB 都识别不了。
本脚本以 TMDB 季/集结构为准（TODB 与 TMDB 逐条一致：实测 231620 五季
173/20/20/47/20 条、date_air 完全相同），把本地文件名重排成标准 SxxExx：

  * E>=100 且能按 MMDD 匹配到某一集的 date_air  -> 用该集的 season/episode
  * E<100 且该季确实有这一集                      -> 保持不动
  * 匹配不到                                      -> 报告，不猜（进 pending_manual）

改名 = 同目录内 mv（teldrive 元数据移动，不动字节）；写 renumber_<ts>.json 回滚映射。

用法（服务器 cd /opt/embytg/pipeline）：
  python3 renumber_todb.py --scan                     # 只报告，不改
  python3 renumber_todb.py --apply --series 231620     # 单部剧
  python3 renumber_todb.py --apply --lib 国产剧         # 一个库
  python3 renumber_todb.py --apply --all                # 全 TV 库
  python3 renumber_todb.py --rollback renumber_20261109_1530.json
"""
from nfo_sidecar import rename_with_sidecars   # 改名同步搬 .nfo/海报（E016 防孤儿）
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

MNT = "/opt/embytg/mnt/embytg"
CFG = "/opt/embytg/config.json"
CACHE = "/opt/embytg/pipeline/.renumber_tmdb_cache.json"
VIDEO_EXT = (".mp4", ".mkv", ".ts", ".m4v", ".mov", ".avi", ".wmv", ".flv", ".webm")
_conf = json.load(open(CFG))
TMDB_API = _conf.get("tmdb_api", "https://api.themoviedb.org/3")
TMDB_TK = _conf.get("tmdb_token", "")
EMBY = "http://127.0.0.1:8096"
TOKEN = _conf.get("emby_token", "")
UA = "Mozilla/5.0 Chrome/156.0 Safari/537.36"
HDR = {"Authorization": "Bearer " + TMDB_TK, "User-Agent": UA, "Accept": "application/json"}

SE_RE = re.compile(r"S(\d{1,2})E(\d{1,4})", re.I)
QUAL_RE = re.compile(r"-\s*(\d{3,4}p|SD|4K)\s*$", re.I)
TMDB_RE = re.compile(r"\{tmdb=(\d+)\}")

_cache = {}
if os.path.exists(CACHE):
    try:
        _cache = json.load(open(CACHE))
    except Exception:
        _cache = {}


def save_cache():
    tmp = CACHE + ".tmp"
    json.dump(_cache, open(tmp, "w"))
    os.replace(tmp, CACHE)


def tmdb(path, retries=3):
    for i in range(retries):
        try:
            r = urllib.request.Request(TMDB_API + path, headers=HDR)
            with urllib.request.urlopen(r, timeout=25) as f:
                return json.loads(f.read().decode("utf-8", "ignore"))
        except Exception:
            if i == retries - 1:
                return {}
            time.sleep(1.2 * (i + 1))


def series_struct(tid):
    """TMDB 全季集结构（带磁盘缓存）：{name, seasons:[{n, year, eps:[{e, air, name}]}]}"""
    key = str(tid)
    if key in _cache:
        return _cache[key]
    d = tmdb("/tv/%s?language=zh-CN" % tid)
    if not d:
        return None
    out = {"name": d.get("name") or d.get("original_name") or "",
           "first_air": d.get("first_air_date") or "", "seasons": []}
    for s in d.get("seasons") or []:
        n = int(s.get("season_number") or 0)
        sd = tmdb("/tv/%s/season/%d?language=zh-CN" % (tid, n))
        eps = []
        for e in sd.get("episodes") or []:
            eps.append({"e": int(e.get("episode_number") or 0),
                        "air": e.get("air_date") or "",
                        "name": e.get("name") or ""})
        out["seasons"].append({"n": n, "air": s.get("air_date") or "", "eps": eps})
    _cache[key] = out
    save_cache()
    return out


def season_year(st, n):
    for s in st["seasons"]:
        if s["n"] == n:
            for cand in (s["air"], (s["eps"][0]["air"] if s["eps"] else "")):
                if cand[:4].isdigit():
                    return int(cand[:4])
    if st["first_air"][:4].isdigit():
        return int(st["first_air"][:4])
    return None


def all_eps(st):
    for s in st["seasons"]:
        for e in s["eps"]:
            if e["air"]:
                yield s["n"], e["e"], e["air"]


def map_date(st, mmdd, prefer_year, prefer_season=None):
    """按 MMDD 找 TMDB 集。

    规则（v2，防跨年误配）：
      1) 只接受 **同年** 的候选（文件所在季的年份）；跨年一律不猜 → no-date-year。
         （v1 会把 2026-09-30 配到 2018 的 S01E06，实测出现过。）
      2) 本季有该日期 → 用本季（同日多集取最小集号 = 上集）。
      3) 本季没有 → 取「离本季最近」的季（先导片/加更常落 S00）。
    """
    cands = [(n, e, air) for n, e, air in all_eps(st)
             if air[5:7] + air[8:10] == mmdd]
    if not cands:
        return None, "no-date"
    if prefer_year:
        cands = [c for c in cands if c[2][:4] == str(prefer_year)]
        if not cands:
            return None, "no-date-year"
    if prefer_season is not None:
        same = [c for c in cands if c[0] == prefer_season]
        if same:
            cands = same
        else:
            cands.sort(key=lambda c: (abs(c[0] - prefer_season), c[0], c[1]))
            c = cands[0]
            return (c[0], c[1], c[2]), "xseason%d" % c[0]
    cands.sort(key=lambda c: (c[0], c[1]))
    return (cands[0][0], cands[0][1], cands[0][2]), ("ok" if len(cands) == 1 else "ambig%d" % len(cands))


def scan_series(dirpath, st):
    """返回 (plan, notes)。

    日期模式判据（防误伤长番）：一个季目录里 ≥80% 的文件集号 ≥100 才按日期解读。
    吞噬星空 S01E110 这种真集号（文件号 1..110 连续）不会被当成 1 月 10 日。"""
    base = os.path.basename(dirpath.rstrip("/"))
    title = TMDB_RE.sub("", base).strip()
    title = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()
    plan, notes = [], []
    for sub in sorted(os.listdir(dirpath)):
        sp = os.path.join(dirpath, sub)
        if not os.path.isdir(sp) or not re.match(r"Season\s*\d+$", sub, re.I):
            continue
        cur_season = int(re.search(r"(\d+)", sub).group(1))
        files = [f for f in sorted(os.listdir(sp)) if f.lower().endswith(VIDEO_EXT)]
        nums = []
        for f in files:
            m = SE_RE.search(f)
            if m:
                nums.append(int(m.group(2)))
        date_mode = bool(nums) and sum(1 for n in nums if n >= 100) >= 0.8 * len(nums)
        for f in files:
            src = os.path.join(sp, f)
            m = SE_RE.search(f)
            if not m:
                notes.append(("no-se", f))
                continue
            season, ep = int(m.group(1)), int(m.group(2))
            qm = QUAL_RE.search(os.path.splitext(f)[0])
            qual = qm.group(1) if qm else "SD"
            new_season, new_ep, why = season, ep, "keep"
            have = any(e["e"] == ep for s in st["seasons"] if s["n"] == season for e in s["eps"])
            if have:
                pass                                   # 本季真有这一集（长番 1000+ 集），不动
            elif ep >= 100 and date_mode:
                mmdd = "%04d" % ep
                hit, why = map_date(st, mmdd, season_year(st, cur_season), cur_season)
                if hit:
                    new_season, new_ep = hit[0], hit[1]
                else:
                    notes.append(("unmapped-date", f))
                    continue
            else:
                notes.append(("no-such-ep", f))
                continue
            dst_dir = os.path.join(dirpath, "Season %02d" % new_season)
            dst = os.path.join(dst_dir, "%s S%02dE%02d - %s%s" % (
                title, new_season, new_ep, qual, os.path.splitext(f)[1].lower()))
            plan.append({"src": src, "dst": dst, "season": new_season, "ep": new_ep,
                         "qual": qual, "old": (season, ep), "why": why})

    # 撞车检测：同一个目标 (季,集) 被「不同日期」的文件抢到 -> 谁都不改，报 collision。
    # 同一天的不同画质（1080p/2160p）映射到同一集是**期望行为**（Emby 多版本），不算撞车。
    groups = {}
    for p in plan:
        groups.setdefault((p["season"], p["ep"]), []).append(p)
    keep = []
    for k, items in groups.items():
        dates = {i["old"][1] for i in items}
        if len(dates) > 1:
            for i in items:
                notes.append(("collision S%02dE%02d" % k, os.path.basename(i["src"])))
            continue
        keep.extend(items)
    return keep, notes


def iter_series(lib=None, tid=None):
    tv = os.path.join(MNT, "TV")
    for l in sorted(os.listdir(tv)):
        lp = os.path.join(tv, l)
        if not os.path.isdir(lp) or (lib and l != lib):
            continue
        for s in sorted(os.listdir(lp)):
            sp = os.path.join(lp, s)
            if not os.path.isdir(sp):
                continue
            m = TMDB_RE.search(s)
            if not m:
                continue
            if tid and m.group(1) != str(tid):
                continue
            yield l, sp, int(m.group(1))


def run(apply=False, lib=None, tid=None, quiet=False):
    t0 = time.time()
    tot_plan, tot_notes, all_plan = 0, 0, []
    for l, sp, sid in iter_series(lib, tid):
        st = series_struct(sid)
        if not st:
            print("!! TMDB 拉取失败 %s" % sp)
            continue
        plan, notes = scan_series(sp, st)
        moves = [p for p in plan if os.path.basename(p["src"]) != os.path.basename(p["dst"])
                 or os.path.dirname(p["src"]) != os.path.dirname(p["dst"])]
        if not moves and not notes:
            continue
        print("== %-28s tmdb=%-8s 文件%-4d 需改名%-4d 未匹配%-3d  %.0fs" % (
            os.path.basename(sp)[:28], sid, len(plan), len(moves), len(notes), time.time() - t0))
        for p in moves[:400]:
            print("   %-46s -> S%02dE%02d" % (os.path.basename(p["src"])[:46], p["season"], p["ep"]))
        if len(moves) > 400:
            print("   ... 其余 %d 条" % (len(moves) - 400))
        for kind, f in notes[:10]:
            print("   ! %-14s %s" % (kind, f))
        tot_plan += len(moves)
        tot_notes += len(notes)
        all_plan.extend(moves)
    print("\n合计需改名 %d 条，未匹配 %d 条，耗时 %.1fs" % (tot_plan, tot_notes, time.time() - t0))
    if not apply or not all_plan:
        return all_plan
    return do_apply(all_plan)


def do_apply(plan):
    ts = time.strftime("%Y%m%d_%H%M%S")
    journal = "/opt/embytg/pipeline/renumber_%s.json" % ts
    done, fail = [], []
    seen = {}
    for p in plan:
        if not os.path.exists(p["src"]):
            fail.append((p["src"], "src-missing"))
            continue
        key = (p["dst"],)
        if key in seen:
            fail.append((p["src"], "dup-target %s" % os.path.basename(p["dst"])))
            continue
        if os.path.exists(p["dst"]):
            fail.append((p["src"], "dst-exists"))
            continue
        try:
            os.makedirs(os.path.dirname(p["dst"]), exist_ok=True)
            rename_with_sidecars(p["src"], p["dst"])
            seen[key] = p
            done.append({"from": p["src"], "to": p["dst"]})
        except Exception as e:
            fail.append((p["src"], str(e)[:120]))
    json.dump({"ts": ts, "done": done, "fail": fail}, open(journal, "w"), ensure_ascii=False, indent=1)
    print("\n改名完成 %d 条，失败 %d 条 -> %s" % (len(done), len(fail), journal))
    for f, why in fail[:20]:
        print("  ! %-60s %s" % (os.path.basename(f)[:60], why))
    return done


def rollback(path):
    j = json.load(open(path))
    ok = bad = 0
    for d in j.get("done") or []:
        if os.path.exists(d["to"]) and not os.path.exists(d["from"]):
            os.makedirs(os.path.dirname(d["from"]), exist_ok=True)
            rename_with_sidecars(d["to"], d["from"])
            ok += 1
        else:
            bad += 1
    print("回滚 %d 条，跳过 %d 条" % (ok, bad))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--rollback")
    a = ap.parse_args()
    if a.rollback:
        rollback(a.rollback)
        sys.exit(0)
    run(apply=a.apply, lib=a.lib, tid=a.series)
