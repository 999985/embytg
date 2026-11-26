#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fix_quality_labels.py —— 把「实测分辨率与文件名画质标签不符」的电影改到正确标签。

## 为什么需要（2026-11-26 E219）

20:12–20:14 从 TG 频道转存的一批电影，入队时消息里没有 video 宽高属性，
`tgpipe.parse_quality()` 只能退回「信源标签」，于是 1920x804/816/800 的宽银幕
1080p 片子全部按源站标签落成 `- 720p.mp4`。入队后 ffprobe 探到 1920 宽，
但**没有任何环节把文件名改回来** —— `daily_ops` 4.5 因此 FAIL 14 条。

后果不只是标签难看：`upgrade` 会认为库里只有 720p，去洗版同一部电影的 1080p，
白烧配额（用户明确讨厌重复下载）。

## 做法（顺序固定，任一步失败即停）

1. 挑出 `resource_state` 里 `kind='movie'`、`width>0`、且标签 != `quality_from_dims()` 的行
2. 通过挂载 `os.rename` 改名（**不是 SQL 改 teldrive 行** —— 那会打断挂载映射，
   见 E215；FUSE rename 走 teldrive 自己的 move，元数据 0.5s、字节不动）
   同名 sidecar（`.nfo` / `-poster.jpg`）一起改
3. `tg.db.resource_state` 的 `path/rel` 同步（只改这两列）
4. `emby_scan.scan_paths(新路径)` 建新条目
5. 旧条目由 `clean_ghost_movies.py --apply` 三重确认后删（旧 Path 已不存在）

## 用法

    python3 fix_quality_labels.py            # 干跑，列出将执行的改名
    python3 fix_quality_labels.py --apply    # 执行
    python3 fix_quality_labels.py --apply --limit 1   # 先做一条验证
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import emby_scan                                               # noqa: E402

MOUNT = "/opt/embytg/mnt/embytg"
TGDB = os.path.join(HERE, "tg.db")
SIDE_EXT = (".nfo", "-poster.jpg", ".srt")


def real_quality(w, h):
    """库内命名口径（与 daily_ops 4.5 / 15.4 完全一致）。

    注意不能用 gha_finish.quality_from_dims —— 它多一个 1440p 档，而库内/文件名
    只用 2160p|1080p|720p|480p 四档；混用会把 2560 宽的 1080p 全改成 1440p，
    反而制造新的不一致。
    """
    w, h = w or 0, h or 0
    if w >= 3000 or h >= 1800:
        return "2160p"
    if w >= 1700 or h >= 1000:
        return "1080p"
    if w >= 1200 or h >= 700:
        return "720p"
    return "SD"


def tagged_quality(name):
    for q in ("2160p", "1080p", "720p", "480p"):
        if q in name:
            return q
    return None


def plan_rows(max_age_h=24.0):
    con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=20)
    q = "select path, rel, width, height, landed_at from resource_state " \
        "where kind='movie' and width>0 and height>0"
    if max_age_h:
        q += " and landed_at>=%d" % int(time.time() - max_age_h * 3600)
    rows = con.execute(q).fetchall()
    con.close()
    out = []
    for path, rel, w, h, _la in rows:
        nm = os.path.basename(rel)
        tag = tagged_quality(nm)
        if not tag:
            continue
        real = real_quality(w, h)
        if tag == real:
            continue
        new_nm = nm.replace("- %s" % tag, "- %s" % real, 1)
        if new_nm == nm:
            continue
        out.append({"path": path, "rel": rel, "old": nm, "new": new_nm,
                    "tag": tag, "real": real, "w": w, "h": h})
    return out


def _sidecars(src):
    """同一 stem 的 sidecar：a.mp4 → a.nfo / a-poster.jpg / a.srt"""
    stem = os.path.splitext(src)[0]
    base = os.path.splitext(os.path.basename(src))[0]
    found = []
    d = os.path.dirname(src)
    for suf in SIDE_EXT:
        p = os.path.join(d, base + suf)
        if os.path.exists(p):
            found.append((p, suf))
    return stem, found


def rename_one(item, apply=False):
    src = item["path"]
    dst = os.path.join(os.path.dirname(src), item["new"])
    if not os.path.exists(src):
        return ("missing", "挂载上看不到旧文件")
    if os.path.exists(dst):
        return ("exists", "新文件名已存在，拒绝覆盖")
    if not apply:
        return ("dry", "%s → %s" % (item["old"], item["new"]))
    _, sides = _sidecars(src)
    os.rename(src, dst)
    moved = []
    stem_old = os.path.splitext(src)[0]
    stem_new = os.path.splitext(dst)[0]
    for p, suf in sides:
        np = stem_new + suf
        try:
            os.rename(p, np)
            moved.append(suf)
        except Exception as e:                                # noqa: BLE001
            return ("sidecar_failed", "%s: %s（视频已改名）" % (suf, str(e)[:80]))
    # 台账同步：只改 path/rel，其余列不动
    new_rel = os.path.join(os.path.dirname(item["rel"]), item["new"]).replace("\\", "/")
    con = sqlite3.connect(TGDB, timeout=20)
    con.execute("update resource_state set path=?, rel=? where path=?",
                (dst, new_rel, src))
    con.commit()
    con.close()
    return ("ok", "改好（sidecar %s）" % (",".join(moved) or "无"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--all", action="store_true", help="不限 24h 窗口（会动存量）")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    items = plan_rows(0 if a.all else 24.0)
    if a.limit:
        items = items[:a.limit]
    if a.json:
        print(json.dumps(items, ensure_ascii=False, indent=1))
        return 0
    print("== 画质标签修正 %s ==" % ("执行" if a.apply else "干跑"))
    if not items:
        print("  无需修正")
        return 0
    done, news = [], []
    for it in items:
        rc, msg = rename_one(it, a.apply)
        print("  [%s] %s" % (rc, msg))
        if rc in ("ok", "dry"):
            # Emby 要的是容器内路径（/mnt/embytg/...），台账存的是宿主路径
            news.append(os.path.join(os.path.dirname(it["path"]), it["new"])
                        .replace(MOUNT, "/mnt/embytg"))
    print("  小计：%d 条%s" % (len(news), "已改" if a.apply else "待改"))
    if a.apply and news:
        st = emby_scan.scan_paths(news)
        print("  scan_paths：%s" % json.dumps({k: v for k, v in st.items() if k != "ids"},
                                             ensure_ascii=False))
        print("  下一步：python3 clean_ghost_movies.py --apply（清旧路径条目）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
