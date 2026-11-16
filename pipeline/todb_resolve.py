#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""todb_resolve.py —— TODB「同名多条目」结构匹配解析器 + 按需补 episode NFO

为什么需要它（2026-11-10 用户纠正）：
  之前判「哑舍 / 我们的少年时代 TODB 没有标题」是**错的**。真实情况：
    · 哑舍          vid=3494 只有 12 集（动画版）、vid=3480 才有 30 集（磁盘要的那个）
    · 我们的少年时代  vid=19997 39 集但标题全是「第 N 集」占位；
                    vid=361273（标题「我们的少年时代2」）才有 36 集真标题
  病根有两条：
    1. `todb_titles.resolve()` **只取排序第一个候选**（按年份差、video_id），从不验证
       该 video_id 的季集结构能否覆盖磁盘 → 同名不同版本/续集必然选错；
    2. `.todb_vid_cache.json` 一旦写入就永久复用，**错误选择被固化**，后续所有脚本都跟着错。

本脚本的做法（结构匹配，而不是名字匹配）：
  1. `/video/list?title=` 拉**所有**候选（含标题变体、续集前缀）；
  2. 每个候选拉 `/video/{id}/season/all` 得到真实季集结构；
  3. 用磁盘实际的 (season, episode) 集合打分：**命中数最多者胜**；同分再看年份接近度；
  4. 只有所有候选都覆盖不到才判「TODB 真没有」；
  5. 输出每部剧的候选对比与选择依据，写 NFO 时逐条留证据（绝不覆盖已有 NFO）。

用法：
  python3 todb_resolve.py --scan                 # 只报告：每部剧候选打分
  python3 todb_resolve.py --apply                # 按最优候选补缺失 NFO
  python3 todb_resolve.py --scan --series 哑舍
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, "/opt/embytg/pipeline")
import todb_titles as TT

OUT = "/opt/embytg/pipeline/.todb_resolve_last.json"
CACHE = "/opt/embytg/pipeline/.todb_resolve_cache.json"


def api(path, **q):
    url = "https://theotherdb.org/api" + path
    if q:
        url += "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": TT.UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read().decode("utf-8", "ignore")
    try:
        return json.loads(raw)
    except Exception:
        return {}


import re as _re
PLACEHOLDER_TITLE = _re.compile(r"^第\s*\d+\s*[集期]$|^Episode\s*\d+$|^\d+$", _re.I)
_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tg.db")


def search(title):
    d = api("/video/list", title=title, page_size=20)
    return d.get("items") or []


def _structure_from_db(vid):
    """从 todb_episode 表读已缓存的分集结构；没有真实标题就返回 {}。"""
    try:
        import sqlite3
        con = sqlite3.connect("file:%s?mode=ro" % _DB,
                              uri=True, timeout=10)
        try:
            rows = con.execute(
                "select season_number, episode_number, title, date_air "
                "from todb_episode where video_id=? order by season_number,"
                " episode_number", (int(vid),)).fetchall()
        finally:
            con.close()
    except Exception:                                      # noqa: BLE001
        return {}
    if not rows:
        return {}
    out = {}
    real = 0
    for sn, en, t, air in rows:
        t = (t or "").strip()
        if t and not PLACEHOLDER_TITLE.match(t):
            real += 1
        out[(int(sn or 0), int(en or 0))] = {"t": t, "d": "", "air": (air or "")[:10]}
    # 全是占位标题 → 当没有，让它走接口重新抓
    return out if real else {}


def structure(vid):
    """{(season, ep): {t,d,air}}

    2026-11-15（E111）：**先查本地 todb_episode 表，没有再打接口。**

    实测：这个函数每次调用都实时打 TODB（season/all + 每季 episode/all），
    而一部 210 集的剧就是 2 次请求、一部多季剧更多。NFO 补齐循环里
    每部剧都会调一次，配额大量消耗在「已经抓过的数据」上。

    现在：表里该剧有真实标题就直接用；没有才走接口，并顺手回填表
    （由 todb_pull_episodes.py 批量做，这里只做兜底单剧回填）。
    """
    local = _structure_from_db(vid)
    if local:
        return local
    seasons = api("/video/%s/season/all" % vid)
    rows = seasons if isinstance(seasons, list) else (seasons.get("data") or [])
    out = {}
    for s in rows:
        sn = int(s.get("season_number") or 0)
        eps = api("/video/%s/season/%d/episode/all" % (vid, sn))
        lst = eps if isinstance(eps, list) else (eps.get("data") or [])
        for e in lst:
            n = e.get("episode_number")
            if n is None:
                continue
            out[(sn, int(n))] = {"t": (e.get("episode_title") or "").strip(),
                                 "d": (e.get("episode_description") or "").strip(),
                                 "air": (e.get("date_air") or "")[:10]}
    return out


def disk_pairs(sp):
    """[(season, ep, filepath)]"""
    out = []
    for sub in sorted(os.listdir(sp)):
        sd = os.path.join(sp, sub)
        if not os.path.isdir(sd) or not re.match(r"Season\s*\d+$", sub, re.I):
            continue
        sn = int(re.search(r"\d+", sub).group())
        for f in sorted(os.listdir(sd)):
            if not f.lower().endswith(TT.VIDEO_EXT):
                continue
            m = TT.SE_RE.search(f)
            if m:
                out.append((sn, int(m.group(2)), os.path.join(sd, f)))
    return out


def _norm(t):
    return re.sub(r"[\s\-_·:：!！?？·]+", "", (t or "")).lower()


def candidates(title, year):
    """同名候选（**严格**）：标题归一化后必须完全相等；年份已知时要求 |差|<=1

    为什么不用「前缀匹配」：`小丑回魂` vs `小丑回魂：欢迎来到德里镇`、`哑舍` vs `哑舍2025版`、
    `我们的少年时代` vs `我们的少年时代2` 都是**不同作品**，按前缀收进来必然写错标题。
    """
    seen, out = set(), []
    base = TT.SEASON_WORD.sub("", title).strip()
    for t in [title, base]:
        if not t:
            continue
        for it in search(t):
            if (it.get("video_type") or "tv") != "tv":
                continue
            vid = int(it.get("video_id"))
            if vid in seen:
                continue
            nm = it.get("video_title") or ""
            if _norm(nm) != _norm(title) and _norm(nm) != _norm(base):
                continue                      # 标题必须完全一致
            cy = str(it.get("date_air") or "")[:4]
            if year and cy.isdigit() and abs(int(cy) - int(year)) > 1:
                continue                      # 年份差 >1 视为不同作品
            seen.add(vid)
            out.append({"vid": vid, "title": nm, "year": cy or None,
                        "air": str(it.get("date_air") or "")[:10]})
    return out


def season_counts(st):
    """{season: [eps...]}"""
    out = {}
    for (sn, e) in st:
        out.setdefault(sn, []).append(e)
    return {k: sorted(v) for k, v in out.items()}


def year_blocks(st):
    """{year: [ep...]}，仅当只有一个正季时用于「TMDB 合并季」的年份分块"""
    sc = season_counts(st)
    pos = [k for k in sc if k > 0]
    if len(pos) != 1:
        return None, {}
    blocks = {}
    for (sn, e), v in st.items():
        y = (v.get("air") or "")[:4]
        if y:
            blocks.setdefault(y, []).append(e)
    if len(blocks) < 2:
        return None, {}
    return pos[0], {y: sorted(v) for y, v in blocks.items()}


def build_map(st, pairs):
    """为磁盘 (s,e) 找 TODB 对应项，返回 {(s,e): (ts,te,why)}；只接受可证明映射"""
    sc = season_counts(st)
    pos = sorted(k for k in sc if k > 0)
    disk_by_season = {}
    for s_, e_, _p in pairs:
        disk_by_season.setdefault(s_, []).append(e_)
    disk_by_season = {k: sorted(set(v)) for k, v in disk_by_season.items()}
    out = {}
    merged_sn, blocks = (pos[0], None) if False else (None, None)
    if len(pos) == 1:
        merged_sn = pos[0]
        _sn, blocks = year_blocks(st)
    for s_, e_, _p in pairs:
        if (s_, e_) in st and st[(s_, e_)]["t"]:
            out[(s_, e_)] = (s_, e_, "A")
            continue
        if len(pos) == 1:
            M = pos[0]
            if len(sc[M]) == len(disk_by_season.get(s_, [])) and (M, e_) in st and st[(M, e_)]["t"]:
                out[(s_, e_)] = (M, e_, "B")
                continue
            # 该季从 1 重新编号 → 偏移 = 前季最大集号
            prev = [max(disk_by_season[k]) for k in sorted(disk_by_season) if k < s_]
            if prev and min(disk_by_season[s_]) == 1:
                off = max(prev)
                if (M, off + e_) in st and st[(M, off + e_)]["t"]:
                    out[(s_, e_)] = (M, off + e_, "C+%d" % off)
                    continue
            # 年份分块：磁盘季号>=2 且该季文件数 == 某后续年份块集数
            if blocks and s_ >= 2:
                cands = [(y, v) for y, v in sorted(blocks.items())][1:]
                for y, v in cands:
                    if len(v) == len(disk_by_season.get(s_, [])) and (M, v[0] + e_ - 1) in st:
                        if (st[(M, v[0] + e_ - 1)].get("air") or "")[:4] == y:
                            out[(s_, e_)] = (M, v[0] + e_ - 1, "D@%s" % y)
                            break
                if (s_, e_) in out:
                    continue
        # F 集号绝对/超纲：磁盘季不在 TODB，或集号超过本季上限 → 找"唯一能覆盖该集号"的季
        #   依据（用户 2026-11-10 指出斗破苍穹正常）：TODB 1021 的 S5 有 219 集（编号 1..219），
        #   磁盘 `S01E158..E177` 实为年番的第 158..177 集 → 唯一命中 S5。
        #   若能覆盖的季不止一个（如 少年歌行 E27 同时落在 S2:33 与 S3:27）→ 不猜，跳过。
        cur_max = max(sc.get(s_, [0]))
        if s_ not in sc or e_ > cur_max:
            cands = [sn for sn in pos if 1 <= e_ <= max(sc[sn])]
            if len(cands) == 1 and (cands[0], e_) in st and st[(cands[0], e_)]["t"]:
                out[(s_, e_)] = (cands[0], e_, "F-绝对集号@S%d" % cands[0])
                continue

        # 多季 TODB：按累计顺序定位
        seq = []
        for sn in sorted(pos):
            seq += [(sn, e) for e in sc[sn]]
        allp = sorted(set((a, b) for a, b, _ in pairs))
        if len(seq) == len(allp):
            try:
                idx = allp.index((s_, e_))
                tsn, te = seq[idx]
                if st[(tsn, te)]["t"]:
                    out[(s_, e_)] = (tsn, te, "E-seq")
            except ValueError:
                pass
    return out


def pick(title, year, pairs):
    """用「可映射命中数」打分，命中最多者胜。返回 (best_row, best_map, rows)"""
    rows, maps = [], {}
    for c in candidates(title, year):
        st = structure(c["vid"])
        m = build_map(st, pairs)
        rows.append({**c, "hit": len(m), "eps": len(st),
                     "titled": sum(1 for v in st.values() if v["t"]),
                     "year_pen": abs(int(c["year"]) - int(year)) if (c["year"] and year) else 99})
        maps[c["vid"]] = m
    if not rows:
        return None, {}, []
    order = sorted(range(len(rows)), key=lambda i: (-rows[i]["hit"], rows[i]["year_pen"], rows[i]["vid"]))
    bi = order[0]
    return rows[bi], maps[rows[bi]["vid"]], rows


def xml_esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def write_nfo(path, s, e, info, overwrite=False):
    nfo = os.path.splitext(path)[0] + ".nfo"
    if os.path.exists(nfo) and not overwrite:
        return "skip_exists"
    rows = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>",
            "  <title>%s</title>" % xml_esc(info["t"]),
            "  <season>%d</season>" % s, "  <episode>%d</episode>" % e]
    if info.get("d"):
        rows.append("  <plot>%s</plot>" % xml_esc(info["d"]))
    if info.get("air"):
        rows.append("  <premiered>%s</premiered>" % info["air"])
        rows.append("  <aired>%s</aired>" % info["air"])
    rows.append("</episodedetails>")
    try:
        with open(nfo, "w", encoding="utf-8") as f:
            f.write("\n".join(rows) + "\n")
        return "write"
    except Exception as ex:
        return "fail:%s" % str(ex)[:50]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--series")
    ap.add_argument("--lib")
    ap.add_argument("--rewrite", action="store_true",
                    help="同一季只用一个 TODB 条目：覆盖全部集时整季重写（旧 NFO 移到隔离目录）")
    a = ap.parse_args()
    vc = json.load(open(TT.__dict__.get("CACHE", "/opt/embytg/pipeline/.todb_title_cache.json")))
    report, stats = [], {}
    t0 = time.time()
    for l, sp, sid in TT.iter_series(a.lib, a.series):
        pairs = disk_pairs(sp)
        if not pairs:
            continue
        missing = [(s, e, p) for s, e, p in pairs if not os.path.exists(os.path.splitext(p)[0] + ".nfo")]
        miss = pairs if a.rewrite else missing
        if not missing and not a.rewrite:
            continue
        title, year = TT.series_title(sp)
        best, bmap, rows = pick(title, year, pairs)
        if not best:
            report.append({"lib": l, "series": os.path.basename(sp), "why": "无候选"})
            print("  ✗ %-6s %-30s 无 TODB 候选" % (l, os.path.basename(sp)[:30]), flush=True)
            continue
        st = structure(best["vid"])
        cover = sum(1 for s_, e_, _p in pairs
                    if (s_, e_) in bmap and st.get((bmap[(s_, e_)][0], bmap[(s_, e_)][1]), {}).get("t"))
        if a.rewrite and cover < len(pairs):
            print("  - %-6s %-28s 最佳仅覆盖 %d/%d，跳过整季重写"
                  % (l, os.path.basename(sp)[:28], cover, len(pairs)), flush=True)
            miss = missing
        filled = 0
        for s, e, p in miss:
            mp = bmap.get((s, e))
            if not mp:
                continue
            info = st.get((mp[0], mp[1]))
            if not info or not info["t"]:
                continue
            if a.apply:
                r = write_nfo(p, mp[0], mp[1], info, overwrite=a.rewrite)
                stats[r.split(":")[0]] = stats.get(r.split(":")[0], 0) + 1
                if r == "write":
                    filled += 1
            else:
                filled += 1
        report.append({"lib": l, "series": os.path.basename(sp), "pick": best,
                       "cands": rows, "miss": len(miss), "filled": filled})
        print("  %-6s %-30s 缺=%-3d 选 vid=%-7s「%s」%s → 可补 %-3d | 候选: %s"
              % (l, os.path.basename(sp)[:30], len(miss), best["vid"], best["title"][:16],
                 best["year"] or "----", filled,
                 ", ".join("%s(%s集/中%d)" % (r["vid"], r["eps"], r["hit"]) for r in rows[:4])),
              flush=True)
    json.dump({"ts": time.strftime("%F %T"), "report": report, "stats": stats},
              open(OUT, "w"), ensure_ascii=False, indent=1)
    tot = sum(r.get("filled", 0) for r in report)
    print("\n# 涉及剧 %d 部，可补/已补 %d 条，耗时 %.0fs → %s"
          % (len(report), tot, time.time() - t0, OUT))


if __name__ == "__main__":
    main()
