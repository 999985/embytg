#!/usr/bin/env python3
"""dedupe_episodes.py —— 同一集有多份文件时，留最好的，其余直接删。

用户 2026-11-19：「不用移出到某个地方 直接删了 保持整洁度和完美度」。

选留规则与 best_pick 一致：分辨率档 > 编解码(HEVC/AV1 优先) > 码率。
删除走 drop_old.py 的五处清理（TG 消息 / teldrive / 台账 / state_key / Emby）。

用法：
    python3 dedupe_episodes.py --dry-run            # 只报告
    python3 dedupe_episodes.py --limit 20 --apply   # 先处理 20 组验证
    python3 dedupe_episodes.py --apply              # 全量
"""
import argparse
import collections
import os
import re
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DB = os.path.join(HERE, "tg.db")
sys.path.insert(0, HERE)
import best_pick as BP                                        # noqa: E402


def load():
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=25)
    rows = con.execute("select rel, size, dur_sec, width, height, vcodec from resource_state "
                       "where kind='episode' and rel like 'TV/%' and rel like '%.mp4' "
                       "or kind='episode' and rel like 'TV/%' and rel like '%.mkv'").fetchall()
    con.close()
    groups = collections.defaultdict(list)
    for rel, size, dur, w, h, vc in rows:
        m = re.search(r"^(.*)/Season (\d+)/.*S\d+E(\d+)", rel)
        if not m:
            continue
        groups[(m.group(1), int(m.group(2)), int(m.group(3)))].append(
            {"rel": rel, "size": size or 0, "duration": dur or 0,
             "width": w or 0, "height": h or 0, "vcodec": vc or ""})
    return groups


def label_of(it):
    nm = os.path.basename(it["rel"]).lower()
    if "2160" in nm or (it["width"] or 0) >= 3000 or (it["height"] or 0) >= 1800:
        return "2160p"
    if "1080" in nm or (it["width"] or 0) >= 1700:
        return "1080p"
    if "720" in nm or (it["width"] or 0) >= 1200:
        return "720p"
    return "sd"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    groups = load()
    dups = {k: v for k, v in groups.items() if len(v) > 1}
    total_kill = sum(len(v) - 1 for v in dups.values())
    print("  同集多文件 %d 组，可删 %d 个多余文件" % (len(dups), total_kill))

    done = 0
    for key in sorted(dups, key=lambda k: -len(dups[k])):
        if a.limit and done >= a.limit:
            break
        items = dups[key]
        view = [{"label": label_of(i), "size": i["size"], "duration": i["duration"],
                 "vcodec": i["vcodec"]} for i in items]
        ranked = BP.rank_candidates(view)
        if not ranked:
            print("  %s E%d：全部候选都不达标，跳过（不冒险删）" % (key[0].split("/")[-1][:24], key[2]))
            continue
        win = ranked[0]
        keep_rel = next(i["rel"] for i, v in zip(items, view)
                        if v["size"] == win["size"] and v["label"] == win["label"])
        kill = [i["rel"] for i in items if i["rel"] != keep_rel]
        print("  %-28s E%-3d 留 %s，删 %d 个（%s）"
              % (key[0].split("/")[-1][:28], key[2],
                 os.path.basename(keep_rel)[-18:], len(kill),
                 ", ".join("%s/%.0fMB" % (os.path.basename(k)[-14:], next(i["size"] for i in items if i["rel"] == k) / 1e6)
                           for k in kill[:3])))
        if not a.apply:
            done += 1
            continue
        for k in kill:
            r = subprocess.run([sys.executable, os.path.join(HERE, "drop_old.py"),
                                "--path", k, "--force"], capture_output=True, text=True, timeout=300)
            tail = [l for l in (r.stdout or "").splitlines() if "已删除" in l or "拒绝" in l]
            if tail:
                print("     ", tail[-1].strip())
        done += 1
    print("  处理 %d 组%s" % (done, "" if a.apply else "（dry-run）"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
