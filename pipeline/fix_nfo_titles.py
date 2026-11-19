#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fix_nfo_titles.py —— 让逐集 .nfo 的 <title>/<season>/<episode> 与文件名一致

用在 renumber 之后：文件名已经带上正确季集号与标题（apply_rename_plan 改的），
但随文件搬过来的旧 .nfo 里还写着老季集号/占位标题。这里按**文件名**回填，
不求助任何外部 API（E007 的"文件名即事实"口径），因此快且不会写错源。

用法::

    python3 fix_nfo_titles.py --series "天赐的声音" --dry
    python3 fix_nfo_titles.py --series "天赐的声音" --apply
    python3 fix_nfo_titles.py --lib 综艺 --apply        # 整库扫
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ROOTS = "/opt/embytg/mnt/embytg/TV"
SE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
VIDEO_EXT = {".mp4", ".mkv", ".ts", ".avi", ".mov", ".m4v"}
QUAL_RE = re.compile(r"(2160p|1080p|720p|480p|SD|4K|UHD)", re.I)


def parse_name(fn):
    """'剧名 S03E05 - 标题 - 2160p.mp4' → (s, e, title)；无标题返回 None"""
    m = SE_RE.search(fn)
    if not m:
        return None
    s, e = int(m.group(1)), int(m.group(2))
    rest = fn[m.end():]
    rest = os.path.splitext(rest)[0]
    parts = [p.strip() for p in rest.split(" - ") if p.strip()]
    parts = [p for p in parts if not QUAL_RE.fullmatch(p)]
    if not parts:
        return None
    title = parts[0].strip()
    if re.fullmatch(r"第\s*\d+\s*[集期]", title):        # 占位标题不入 NFO
        return None
    return s, e, title


def upd_nfo(nfo, s, e, title):
    xml = ""
    if os.path.exists(nfo):
        try:
            xml = open(nfo, encoding="utf-8").read()
        except Exception:                                          # noqa: BLE001
            xml = ""
    if not xml.strip():
        xml = ('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<episodedetails>\n'
               "  <title></title>\n  <season>0</season>\n  <episode>0</episode>\n</episodedetails>\n")
    esc = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    if "<title>" in xml:
        xml = re.sub(r"<title>.*?</title>", "<title>%s</title>" % esc, xml, count=1, flags=re.S)
    else:
        xml = xml.replace("<episodedetails>", "<episodedetails>\n  <title>%s</title>" % esc, 1)
    xml = re.sub(r"<season>.*?</season>", "<season>%d</season>" % s, xml, count=1, flags=re.S)
    xml = re.sub(r"<episode>.*?</episode>", "<episode>%d</episode>" % e, xml, count=1, flags=re.S)
    return xml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", action="append", default=[], help="剧名关键字，可多次")
    ap.add_argument("--lib")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()
    if not a.apply:
        a.dry = True

    roots = [os.path.join(ROOTS, a.lib)] if a.lib else [os.path.join(ROOTS, x) for x in os.listdir(ROOTS)
                                                       if os.path.isdir(os.path.join(ROOTS, x)) and not x.startswith(".")]
    todo, changed, skipped = [], 0, 0
    for root in roots:
        for d in sorted(os.listdir(root)):
            p = os.path.join(root, d)
            if not os.path.isdir(p) or d.startswith("."):
                continue
            if a.series and not any(k in d for k in a.series):
                continue
            for dp, _, fns in os.walk(p):
                for fn in fns:
                    if os.path.splitext(fn)[1].lower() not in VIDEO_EXT:
                        continue
                    got = parse_name(fn)
                    if not got:
                        skipped += 1
                        continue
                    s, e, title = got
                    nfo = os.path.splitext(os.path.join(dp, fn))[0] + ".nfo"
                    old = ""
                    if os.path.exists(nfo):
                        m = re.search(r"<title>(.*?)</title>", open(nfo, encoding="utf-8", errors="ignore").read(), re.S)
                        old = (m.group(1) if m else "").strip()
                    if old == title:
                        continue
                    todo.append((nfo, s, e, title, old))
    print("== 待回填 %d 个 NFO（跳过无标题文件名 %d）==" % (len(todo), skipped))
    for nfo, s, e, title, old in todo[:12]:
        print("   S%02dE%02d  %-28s ← 「%s」" % (s, e, title[:28], old[:16] or "空"))
    if a.dry:
        print("\n[干跑] 未改任何文件。确认后加 --apply。")
        return
    for nfo, s, e, title, _ in todo:
        try:
            open(nfo, "w", encoding="utf-8").write(upd_nfo(nfo, s, e, title))
            changed += 1
        except Exception as ex:                                     # noqa: BLE001
            print("  ERR %s: %s" % (nfo, ex))
    print("\n已回填 %d 个 NFO" % changed)


if __name__ == "__main__":
    main()
