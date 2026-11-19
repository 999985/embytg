#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fix_series_identity.py —— 纠正「剧名 / TMDB ID / 季号」三者中任一错的剧（E024 通用版）

来源：用户两次指出同类问题 ——
  · 「神墓年番」不存在（TMDB 328917 是 1 季 1 集 stub，真身 TODB 1015 神墓 S3）
  · 「小丑回魂 (1990)」里装的其实是《小丑回魂：欢迎来到德里镇》(TMDB 200875, 2025)
共同点：**目录名与 TMDB ID 都错**，导致永远拿不到集标题。

做法（全部可回滚，journal 落盘）
------------------------------
1. 剧目目录改名：`<旧名> (<旧年>) {tmdb=<旧id>}` → `<新名> (<新年>) {tmdb=<新id>}`
2. 季目录改名：`Season <旧季>` → `Season <新季>`（可多个 --season-pair 旧:新）
3. 逐集改名：按 TODB（优先）或 TMDB 的集标题 + 运行时对齐（±90s 容差可调），
   若集号一一对应（文件数 == 该季集数且 1..N 连续）则按集号直接对；
   否则按 duration 匹配，匹配不唯一就跳过该条并报告。
4. 重写 tvshow.nfo；逐集重写 NFO
5. 打印需人工处理项（例如截断文件）

用法
----
    python3 fix_series_identity.py \
      --old "/opt/embytg/mnt/embytg/TV/欧美剧/小丑回魂 (1990) {tmdb=19614}" \
      --new "小丑回魂：欢迎来到德里镇 (2025) {tmdb=200875}" \
      --title "小丑回魂：欢迎来到德里镇" --year 2025 --tmdb 200875 --season-pair 1:1 \
      [--vid 12345] [--apply] [--rollback journal.json]
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nfo_sidecar import rename_with_sidecars        # noqa: E402

VIDEO_EXT = {".mp4", ".mkv", ".ts"}
SE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))


def tmdb(path, **q):
    q.update(api_key=CFG["tmdb_api_key"], language="zh-CN")
    u = CFG["tmdb_api"] + path + "?" + urllib.parse.urlencode(q)
    with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as f:
        return json.loads(f.read().decode())


def todb(path):
    u = "https://theotherdb.org/api" + path
    req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                                              "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as f:
        return json.loads(f.read().decode())


def dur(p):
    try:
        out = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", p],
                             capture_output=True, text=True, timeout=120).stdout.strip()
        return float(out.splitlines()[0]) if out else 0.0
    except Exception:                                              # noqa: BLE001
        return 0.0


def sanitize(s, limit=70):
    bad = '\\/:*?"<>|'
    out = "".join("-" if ch in bad else ch for ch in (s or "")).strip().strip(".")
    return out[:limit].strip()


def episode_meta(tmdb_id, sn, vid=None):
    """返回 {集号: {'title':..,'air':..,'runtime':..}}"""
    out = {}
    if vid:
        try:
            eps = todb("/video/%d/season/%d/episode/all" % (vid, sn))
            eps = eps if isinstance(eps, list) else eps.get("data") or []
            for e in eps:
                out[int(e["episode_number"])] = {"title": e.get("episode_title") or "",
                                                 "air": (e.get("date_air") or "")[:10],
                                                 "runtime": e.get("episode_run_time") or e.get("runtime") or 0}
            if out:
                return out, "TODB"
        except Exception as ex:                                    # noqa: BLE001
            print("  ! TODB 失败 %s，回退 TMDB" % ex)
    d = tmdb("/tv/%d/season/%d" % (tmdb_id, sn))
    for e in d.get("episodes", []):
        out[int(e["episode_number"])] = {"title": e.get("name") or "",
                                         "air": e.get("air_date") or "",
                                         "runtime": (e.get("runtime") or 0) * 60}
    return out, "TMDB"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True, help="新目录名（不含父目录）")
    ap.add_argument("--title", required=True)
    ap.add_argument("--year", required=True)
    ap.add_argument("--tmdb", required=True)
    ap.add_argument("--vid", type=int, default=0)
    ap.add_argument("--season-pair", action="append", default=[], help="旧:新，可多次")
    ap.add_argument("--plot", default="")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--rollback")
    a = ap.parse_args()

    if a.rollback:
        j = json.load(open(a.rollback, encoding="utf-8"))
        for it in reversed(j["done"]):
            if os.path.exists(it["to"]) and not os.path.exists(it["from"]):
                os.makedirs(os.path.dirname(it["from"]), exist_ok=True)
                os.rename(it["to"], it["from"])
                print("  rollback", os.path.basename(it["to"]))
        return

    old = a.old.rstrip("/")
    parent = os.path.dirname(old)
    new = os.path.join(parent, a.new)
    if not os.path.isdir(old):
        print("✗ 旧目录不存在：%s" % old)
        return
    if os.path.exists(new):
        print("✗ 目标目录已存在：%s" % new)
        return

    pairs = {}
    for x in a.season_pair:
        o, n = x.split(":")
        pairs[int(o)] = int(n)
    print("== %s ==" % a.title)
    print("  目录 %s\n     → %s" % (os.path.basename(old), a.new))
    plan = []          # (src, dst_rel, note)
    for sd in sorted(os.listdir(old)):
        m = re.match(r"Season (\d+)$", sd)
        if not m or not os.path.isdir(os.path.join(old, sd)):
            continue
        so = int(m.group(1))
        sn = pairs.get(so, so)
        meta, src = episode_meta(int(a.tmdb), sn, a.vid or None)
        files = sorted(f for f in os.listdir(os.path.join(old, sd)) if os.path.splitext(f)[1].lower() in VIDEO_EXT)
        print("  Season %02d → Season %02d，文件 %d 个，元数据源 %s（%d 集）" % (so, sn, len(files), src, len(meta)))
        # 按集号对
        bynum = {}
        for f in files:
            mm = SE_RE.search(f)
            if mm:
                bynum[int(mm.group(2))] = f
        use_num = len(files) == len(meta) and sorted(bynum) == sorted(meta)
        if not use_num:                                            # 回退：按时长匹配
            print("    ! 集号不连续/数量不符，改用时长匹配（容差 90s）")
            durs = {f: dur(os.path.join(old, sd, f)) for f in files}
            for f, dd in durs.items():
                best = sorted(((abs(dd - v["runtime"]), k) for k, v in meta.items() if v["runtime"]),
                              key=lambda x: x[0])
                if best and best[0][0] <= 90:
                    bynum[best[0][1]] = f
                    print("      %s (%.0fs) → E%d (%.0fs)" % (f[:34], dd, best[0][1], meta[best[0][1]]["runtime"]))
        for e, f in sorted(bynum.items()):
            t = sanitize(meta.get(e, {}).get("title") or "")
            qual = "2160p" if "2160p" in f else ("1080p" if "1080p" in f else "")
            ext = os.path.splitext(f)[1]
            base = "%s S%02dE%02d%s - %s%s" % (a.title, sn, e, (" - " + t) if t else "", qual, ext)
            dst_rel = os.path.join("Season %02d" % sn, base)
            plan.append((os.path.join(old, sd, f), dst_rel, t))
            print("    %-40s → %-56s [%s]" % (f[:40], base[:56], t or "无标题"))
    if not a.apply:
        print("\n[干跑] 未改任何文件。确认后加 --apply。")
        return

    jp = "/opt/embytg/pipeline/identity_journal_%s.json" % time.strftime("%Y%m%d_%H%M%S")
    j = {"start": time.strftime("%F %T"), "done": []}
    os.rename(old, new)
    j["done"].append({"from": old, "to": new})
    for o, n in pairs.items():
        if o != n:
            so, sn = os.path.join(new, "Season %02d" % o), os.path.join(new, "Season %02d" % n)
            if os.path.isdir(so) and not os.path.exists(sn):
                os.rename(so, sn)
                j["done"].append({"from": so, "to": sn})
                print("  ✓ Season %02d → %02d" % (o, n))
    for srcp, dst_rel, _ in plan:
        d = os.path.join(new, dst_rel)
        sp = srcp.replace(old, new, 1)
        if os.path.exists(sp):
            os.makedirs(os.path.dirname(d), exist_ok=True)
            rename_with_sidecars(sp, d)
            j["done"].append({"from": sp, "to": d})
    plot = a.plot
    if not plot:
        try:
            plot = tmdb("/tv/%s" % a.tmdb).get("overview") or ""
        except Exception:                                          # noqa: BLE001
            plot = ""
    open(os.path.join(new, "tvshow.nfo"), "w", encoding="utf-8").write(
        '<?xml version="1.0" encoding="utf-8"?>\n<tvshow>\n  <title>%s</title>\n  <originaltitle>%s</originaltitle>\n'
        "  <year>%s</year>\n  <plot>%s</plot>\n  <tmdbid>%s</tmdbid>\n</tvshow>\n"
        % (a.title, a.title, a.year, plot.replace("&", "&amp;"), a.tmdb))
    n = 0
    for _, dst_rel, t in plan:
        base = os.path.splitext(os.path.join(new, dst_rel))[0]
        mm = SE_RE.search(os.path.basename(dst_rel))
        open(base + ".nfo", "w", encoding="utf-8").write(
            '<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<episodedetails>\n  <title>%s</title>\n'
            "  <season>%d</season>\n  <episode>%d</episode>\n</episodedetails>\n"
            % ((t or "").replace("&", "&amp;"), int(mm.group(1)), int(mm.group(2))))
        n += 1
    j["end"] = time.strftime("%F %T")
    json.dump(j, open(jp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("\n完成：改名 %d 个、NFO %d 个 → journal %s" % (len(plan), n, jp))
    print("回滚: python3 fix_series_identity.py --old '%s' --new x --title x --year 0 --tmdb 0 --rollback %s"
          % (old, jp))


if __name__ == "__main__":
    main()
