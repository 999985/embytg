#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""relabel_series.py —— 只改「季号 / 年份 / tmdbid」的轻量换标（不改剧身份内容）

用途（E024 衍生）：
  · 监狱学园：12 集放在 `Season 02`，TMDB/TODB 只有 S1×12 → `--season-from 2 --season-to 1`
  · 新闻女王2：`(0000)`、25 集在 `Season 01`，TMDB season_number=2 → `--season-from 1 --season-to 2 --year 2025`
  · 哑舍：`{tmdb=273190}`（12 集版）→ `{tmdb=270856}`（30 集版）→ `--tmdb 270856 --year 2025`

做四件事（顺序固定，任一步失败即停，journal 可回滚）：
  1. `Season NN` 目录改名
  2. 季内每个视频把 `SxxEyy` 的 xx 换成新季号（标题/画质原样保留），`.nfo/.jpg/.srt` 同步搬
  3. 逐集 `.nfo` 的 `<season>/<episode>` 用 `fix_nfo_titles.py` 口径回填
  4. `tvshow.nfo` 的 `<title>/<year>/<tmdbid>` 更新；若改 tmdbid 同时改目录名

用法::

    python3 relabel_series.py --dir "..." --season-from 2 --season-to 1
    python3 relabel_series.py --dir "..." --tmdb 270856 --year 2025
    python3 relabel_series.py --dir "..." --season-from 1 --season-to 2 --year 2025 --apply
"""
import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nfo_sidecar import rename_with_sidecars        # noqa: E402

SE = re.compile(r"([Ss])(\d{1,2})([Ee])(\d{1,3})")
VIDEO_EXT = {".mp4", ".mkv", ".ts"}
CFG = "/opt/embytg/config.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--season-from", type=int)
    ap.add_argument("--season-to", type=int)
    ap.add_argument("--year")
    ap.add_argument("--tmdb")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    d = a.dir.rstrip("/")
    if not os.path.isdir(d):
        print("✗ 目录不存在: %s" % d)
        return
    print("== %s ==" % os.path.basename(d))
    moves = []
    if a.season_from and a.season_to and a.season_from != a.season_to:
        sd = os.path.join(d, "Season %02d" % a.season_from)
        if not os.path.isdir(sd):
            print("✗ 没有 Season %02d" % a.season_from)
            return
        print("  Season %02d → Season %02d" % (a.season_from, a.season_to))
        for f in sorted(os.listdir(sd)):
            if os.path.splitext(f)[1].lower() not in VIDEO_EXT:
                continue
            m = SE.search(f)
            if not m:
                print("    ! 无 SxxExx: %s" % f)
                continue
            newf = f[:m.start(2)] + "%02d" % a.season_to + f[m.end(2):]
            moves.append((os.path.join(sd, f), os.path.join(d, "Season %02d" % a.season_to, newf)))
            print("    %-52s → %s" % (f[:52], newf[:52]))
    if a.year or a.tmdb:
        print("  tvshow.nfo: year=%s tmdbid=%s" % (a.year or "(不改)", a.tmdb or "(不改)"))
    if a.tmdb:
        m = re.search(r"\{tmdb=\d+\}", d)
        if m:
            nd = d[:m.start()] + "{tmdb=%s}" % a.tmdb + d[m.end():]
            print("  目录: %s\n     → %s" % (os.path.basename(d), os.path.basename(nd)))
            d2 = nd
        else:
            d2 = d
    else:
        d2 = d

    if not a.apply:
        print("\n[干跑] 未改任何文件。确认后加 --apply。")
        return

    jp = "/opt/embytg/pipeline/relabel_journal_%s.json" % time.strftime("%Y%m%d_%H%M%S")
    j = {"start": time.strftime("%F %T"), "done": []}
    if a.season_from and a.season_to and a.season_from != a.season_to:
        old_sd = os.path.join(d, "Season %02d" % a.season_from)
        new_sd = os.path.join(d, "Season %02d" % a.season_to)
        os.makedirs(new_sd, exist_ok=True)
        for src, dst in moves:
            if os.path.exists(src) and not os.path.exists(dst):
                rename_with_sidecars(src, dst)
                j["done"].append({"from": src, "to": dst})
        # 季目录空了就删（只删空目录）
        try:
            if not os.listdir(old_sd):
                os.rmdir(old_sd)
                print("  ✓ 空季目录 Season %02d 已移除" % a.season_from)
        except OSError:
            pass
        print("  ✓ 改名 %d 个" % len(j["done"]))
        # 回填 NFO 季号
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import fix_nfo_titles as F
        n = 0
        for dp, _, fns in os.walk(d):
            for fn in fns:
                if os.path.splitext(fn)[1].lower() not in F.VIDEO_EXT:
                    continue
                got = F.parse_name(fn)
                if not got:
                    continue
                s, e, title = got
                nfo = os.path.splitext(os.path.join(dp, fn))[0] + ".nfo"
                open(nfo, "w", encoding="utf-8").write(F.upd_nfo(nfo, s, e, title))
                n += 1
        print("  ✓ 回填 NFO %d 个" % n)
    if a.tmdb and d2 != d:
        os.rename(d, d2)
        j["done"].append({"from": d, "to": d2})
        print("  ✓ 目录改名")
    if a.year or a.tmdb:
        t = os.path.join(d2, "tvshow.nfo")
        xml = open(t, encoding="utf-8").read() if os.path.exists(t) else "<tvshow>\n</tvshow>\n"
        if a.year:
            if "<year>" in xml:
                xml = re.sub(r"<year>.*?</year>", "<year>%s</year>" % a.year, xml, flags=re.S)
            else:
                xml = xml.replace("</tvshow>", "  <year>%s</year>\n</tvshow>" % a.year)
        if a.tmdb:
            if "<tmdbid>" in xml:
                xml = re.sub(r"<tmdbid>.*?</tmdbid>", "<tmdbid>%s</tmdbid>" % a.tmdb, xml, flags=re.S)
            else:
                xml = xml.replace("</tvshow>", "  <tmdbid>%s</tmdbid>\n</tvshow>" % a.tmdb)
        open(t, "w", encoding="utf-8").write(xml)
        print("  ✓ tvshow.nfo 更新")
    j["end"] = time.strftime("%F %T")
    json.dump(j, open(jp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("\n完成 → %s" % jp)


if __name__ == "__main__":
    main()
