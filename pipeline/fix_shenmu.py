#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fix_shenmu.py —— 修正「神墓年番」错标题/错 TMDB ID/错季号（用户 2026-11-10 指出）

问题
----
- 目录名 `神墓年番 (2025) {tmdb=328917}`：**「神墓年番」不是真实剧名**，TMDB 328917 是一个
  `1 季 1 集` 的 stub（只有 airdate，没有集数据）→ 该剧永远拿不到集标题，属"标题有问题"。
- 真实剧条：TODB `1015` = **神墓**（2022-07-21），`S1 16 / S2 27 / S3 52`；TMDB `206343` = 神墓。
- 磁盘 10 个文件 `神墓年番 S01E01..E10`（2160p，2025-08-08 起播）= **神墓 S3 E1..E10**。

改法（全部可回滚）
------------------
1. 剧目目录 `神墓年番 (2025) {tmdb=328917}` → `神墓 (2022) {tmdb=206343}`
2. `Season 01` → `Season 03`
3. 每集 `S01Exx - 2160p.mp4` → `S03Exx - <TODB 标题> - 2160p.mp4`（.nfo/.jpg 同步）
4. 重写 `tvshow.nfo`（标题/年份/tmdbid/剧情）
5. 用 TODB S3 重写逐集 NFO

用法::

    python3 fix_shenmu.py            # 干跑
    python3 fix_shenmu.py --apply
    python3 fix_shenmu.py --rollback /opt/embytg/pipeline/shenmu_journal_xxx.json
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nfo_sidecar import rename_with_sidecars      # noqa: E402

OLD = "/opt/embytg/mnt/embytg/TV/国产剧/神墓年番 (2025) {tmdb=328917}"
NEW = "/opt/embytg/mnt/embytg/TV/国产剧/神墓 (2022) {tmdb=206343}"
VID = 1015
SEASON = 3
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"


def api(p):
    r = urllib.request.Request("https://theotherdb.org/api" + p, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(r, timeout=30) as f:
        return json.loads(f.read().decode())


def sanitize(s, limit=70):
    bad = '\\/:*?"<>|'
    out = "".join("-" if ch in bad else ch for ch in (s or "")).strip().strip(".")
    return out[:limit].strip()


def series_meta():
    d = api("/video/%d" % VID)
    eps = api("/video/%d/season/%d/episode/all" % (VID, SEASON))
    eps = eps if isinstance(eps, list) else eps.get("data") or []
    return d, {e["episode_number"]: e for e in eps}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--rollback")
    a = ap.parse_args()

    if a.rollback:
        j = json.load(open(a.rollback, encoding="utf-8"))
        for it in reversed(j["done"]):
            if os.path.exists(it["to"]) and not os.path.exists(it["from"]):
                os.makedirs(os.path.dirname(it["from"]), exist_ok=True)
                os.rename(it["to"], it["from"])
                print("  rollback", os.path.basename(it["to"]), "→", os.path.basename(it["from"]))
        return

    if not os.path.isdir(OLD):
        print("✗ 旧目录不存在：%s" % OLD)
        print("  若已改过，检查 %s 是否存在" % NEW)
        return
    if os.path.exists(NEW):
        print("✗ 目标目录已存在：%s" % NEW)
        return

    meta, eps = series_meta()
    files = sorted(f for f in os.listdir(os.path.join(OLD, "Season 01")) if f.lower().endswith(".mp4"))
    print("== 神墓 修正计划 ==")
    print("  目录: %s" % OLD)
    print("     → %s" % NEW)
    print("  季: Season 01 → Season 03（TODB S3 共 %d 集）" % len(eps))
    print("  文件 %d 个：" % len(files))
    plan = []
    for f in files:
        try:
            e = int(f.split("E")[1][:2])
        except Exception:                                             # noqa: BLE001
            print("    ! 无法解析集号：%s" % f)
            continue
        t = sanitize(eps.get(e, {}).get("episode_title") or "")
        dst = "神墓 S%02dE%02d%s - 2160p.mp4" % (SEASON, e, (" - " + t) if t else "")
        plan.append((f, dst, e, t))
        print("    %-34s → %-56s [%s]" % (f, dst, t or "无标题"))
    if not a.apply:
        print("\n[干跑] 未改任何文件。确认后加 --apply。")
        return

    jpath = "/opt/embytg/pipeline/shenmu_journal_%s.json" % time.strftime("%Y%m%d_%H%M%S")
    journal = {"start": time.strftime("%F %T"), "done": []}
    # 1) 剧目目录改名（带 tvshow.nfo/poster 一起走，同目录内 rename 即可）
    os.rename(OLD, NEW)
    journal["done"].append({"from": OLD, "to": NEW})
    print("  ✓ 目录改名")
    # 2) 季目录改名
    os.rename(os.path.join(NEW, "Season 01"), os.path.join(NEW, "Season 03"))
    journal["done"].append({"from": os.path.join(NEW, "Season 01"), "to": os.path.join(NEW, "Season 03")})
    print("  ✓ 季目录 Season 01 → Season 03")
    # 3) 逐集改名（.nfo/.jpg 同步）
    for src_name, dst_name, _, _ in plan:
        src = os.path.join(NEW, "Season 03", src_name)
        dst = os.path.join(NEW, "Season 03", dst_name)
        if os.path.exists(src):
            rename_with_sidecars(src, dst)
            journal["done"].append({"from": src, "to": dst})
    print("  ✓ 逐集改名 %d 个" % len(plan))
    # 4) 重写 tvshow.nfo
    p = meta.get("video_description") or ""
    tv = """<?xml version="1.0" encoding="utf-8"?>
<tvshow>
  <title>神墓</title>
  <originaltitle>神墓</originaltitle>
  <year>2022</year>
  <premiered>2022-07-21</premiered>
  <plot>%s</plot>
  <tmdbid>206343</tmdbid>
  <country>CN</country>
</tvshow>
""" % p
    open(os.path.join(NEW, "tvshow.nfo"), "w", encoding="utf-8").write(tv)
    print("  ✓ tvshow.nfo 重写（神墓 / 2022 / tmdb=206343）")
    # 5) 逐集 NFO
    n = 0
    for _, dst_name, e, _ in plan:
        ep = eps.get(e) or {}
        base = os.path.splitext(os.path.join(NEW, "Season 03", dst_name))[0]
        xml = """<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<episodedetails>
  <title>%s</title>
  <season>%d</season>
  <episode>%d</episode>
  <premiered>%s</premiered>
  <aired>%s</aired>
  <plot>%s</plot>
</episodedetails>
""" % ((ep.get("episode_title") or "").replace("&", "&amp;"), SEASON, e,
       (ep.get("date_air") or "")[:10], (ep.get("date_air") or "")[:10],
       (ep.get("episode_description") or ep.get("video_description") or "").replace("&", "&amp;"))
        open(base + ".nfo", "w", encoding="utf-8").write(xml)
        n += 1
    print("  ✓ 逐集 NFO 写入 %d 个" % n)
    journal["end"] = time.strftime("%F %T")
    json.dump(journal, open(jpath, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("\n完成 → journal %s" % jpath)
    print("回滚: python3 fix_shenmu.py --rollback %s" % jpath)
    subprocess.call(["ls", "-1", os.path.join(NEW, "Season 03")])


if __name__ == "__main__":
    main()
