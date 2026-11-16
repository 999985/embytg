#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fix_shaonian_shidai.py —— 处理「我们的少年时代」两目录重复（4K 在错系列、1080p 在正系列）

实测事实（2026-11-10）：
  A = TV/国产剧/我们的少年时代 (2017) {tmdb=119538}/Season 02/  36 个 4K（3840x1604，~1.7G）
  B = TV/国产剧/我们的少年时代2 (2026) {tmdb=331830}/Season 01/  32 个 1080p（1920x802，~0.58G）
  · 抽 8 对 ffprobe：**时长完全一致**（1829.0 / 1777.4 / …）→ 同一批内容，仅画质不同
  · TMDB 119538 = 我们的少年时代(2017) S1 有 39 集；TMDB 331830 = 我们的少年时代2(2026) S1 有 36 集
  · A 侧 36 个文件 == 331830 的 36 集 → **A 的 Season 02 是错放**，内容属于 B 的系列

做法（E015「同集只留最高画质」的目录级应用）：
  1. 按 ffprobe 时长把 A 的 36 个文件对到 B 的 32 个（±2s），剩 4 个按序号接在后面；
  2. 标题取自 TODB vid=361273（我们的少年时代2，36 集有真标题）；
  3. A 的 4K 文件搬进 B/Season 01，命名 `我们的少年时代2 S01E<nn> - <标题> - 2160p.mp4`；
  4. B 原有的 1080p 文件改名为 `<原文件名>.dup`（Emby 不识扩展名 → 条目消失；NFO 变 `.dup.nfo`）；
  5. 为搬入的 4K 文件写 NFO（title/plot/air 来自 TODB）；
  6. A 的 Season 02 清空后删空目录，A 系列目录（只剩 tvshow.nfo/poster）移到 `pipeline/_trash_<ts>/`；
  7. 全程 journal：`pipeline/fix_shaonian_<ts>.json`，附 `--rollback`。

用法：
  python3 fix_shaonian_shidai.py --dry
  python3 fix_shaonian_shidai.py --apply
  python3 fix_shaonian_shidai.py --rollback pipeline/fix_shaonian_<ts>.json
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time

sys.path.insert(0, "/opt/embytg/pipeline")
import todb_titles as TT
from nfo_sidecar import rename_with_sidecars

TV = "/opt/embytg/mnt/embytg/TV/国产剧"
A_DIR = os.path.join(TV, "我们的少年时代 (2017) {tmdb=119538}")
B_DIR = os.path.join(TV, "我们的少年时代2 (2026) {tmdb=331830}")
TODB_VID = 361273          # 我们的少年时代2
TITLE = "我们的少年时代2"


def probe_dur(path):
    c = path.replace("/opt/embytg/mnt/embytg", "/mnt/embytg")
    out = subprocess.run(["docker", "exec", "emby", "/bin/ffprobe.real", "-v", "error",
                          "-show_entries", "format=duration", "-of", "csv=p=0", "file:" + c],
                         capture_output=True, text=True, timeout=120)
    try:
        return round(float(out.stdout.strip()), 1)
    except Exception:
        return None


def probe_res(path):
    c = path.replace("/opt/embytg/mnt/embytg", "/mnt/embytg")
    out = subprocess.run(["docker", "exec", "emby", "/bin/ffprobe.real", "-v", "error",
                          "-select_streams", "v:0", "-show_entries", "stream=width,height",
                          "-of", "csv=p=0", "file:" + c], capture_output=True, text=True, timeout=120)
    return out.stdout.strip()


def collect(d, season):
    sd = os.path.join(d, season)
    if not os.path.isdir(sd):
        return []
    out = []
    for f in sorted(os.listdir(sd)):
        if f.lower().endswith(TT.VIDEO_EXT):
            out.append((os.path.join(sd, f), f))
    return out


def build_plan():
    """**按顺序**配对（A 的 S02E01..E36 ↔ B 的 S01E01..E36），时长只作校验。

    为什么不用"按时长匹配"：时长会撞（本次实测 A[31] 的时长与 B 的 E15 相同 ±2s），
    按时长贪心会把内容对错集。两侧文件名本身都是顺序编号，按序配对才可靠。
    """
    A = collect(A_DIR, "Season 02")
    B = collect(B_DIR, "Season 01")
    eps = TT.todb_eps(TODB_VID)
    # 按集号排序，确保顺序
    def key(item):
        m = TT.SE_RE.search(item[1])
        return int(m.group(2)) if m else 0
    A.sort(key=key)
    B.sort(key=key)
    plan, warn = [], []
    for i, (p, f) in enumerate(A):
        m = TT.SE_RE.search(f)
        ep = int(m.group(2)) if m else (i + 1)   # 用 A 自己的集号（1..36 连续），不靠位置推
        info = eps.get((1, ep)) or {}
        t = (info.get("t") or "").strip() or ("第%d集" % ep)
        dst = os.path.join(B_DIR, "Season 01",
                           TT.build_name(TITLE, 1, ep, TT.sanitize(t), "2160p",
                                         os.path.splitext(f)[1].lower()))
        da = probe_dur(p)
        db = probe_dur(B[i][0]) if i < len(B) else None
        if da is not None and db is not None and abs(da - db) > 2:
            warn.append((f, da, B[i][1], db))
        plan.append({"src": p, "srcname": f, "dur": da, "ep": ep,
                     "title": t, "dst": dst, "info": info})
    dups = [(p, f, key((p, f))) for p, f in B]
    return plan, warn, dups


def write_nfo(path, ep, info):
    nfo = os.path.splitext(path)[0] + ".nfo"
    rows = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>",
            "  <title>%s</title>" % (info.get("t") or ""),
            "  <season>1</season>", "  <episode>%d</episode>" % ep]
    if info.get("d"):
        rows.append("  <plot>%s</plot>" % info["d"].replace("&", "&amp;").replace("<", "&lt;"))
    if info.get("air"):
        rows.append("  <premiered>%s</premiered>" % info["air"])
        rows.append("  <aired>%s</aired>" % info["air"])
    rows.append("</episodedetails>")
    open(nfo, "w", encoding="utf-8").write("\n".join(rows) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--rollback")
    a = ap.parse_args()
    if a.rollback:
        j = json.load(open(a.rollback))
        ok = bad = 0
        for d in reversed(j.get("done", [])):
            try:
                if d["kind"] == "move":
                    if os.path.exists(d["to"]) and not os.path.exists(d["from"]):
                        rename_with_sidecars(d["to"], d["from"]); ok += 1
                    else:
                        bad += 1
                elif d["kind"] == "dup":
                    if os.path.exists(d["to"]) and not os.path.exists(d["from"]):
                        rename_with_sidecars(d["to"], d["from"]); ok += 1
                    else:
                        bad += 1
            except Exception as e:
                bad += 1
        print("回滚 ok=%d bad=%d（NFO 由 sidecar 一起搬回）" % (ok, bad))
        return
    plan, warn, dups = build_plan()
    print("A 侧 %d 个（4K），B 侧 %d 个（1080p）；按时长校验异常 %d 条"
          % (len(plan), len(dups), len(warn)))
    for w in warn[:5]:
        print("  ⚠ 时长不一致:", w[0][:40], w[1], "vs", w[2][:40], w[3])
    for x in plan[:6]:
        print("  A %-46s %6.1fs → E%-3d %s" % (x["srcname"][:46], x["dur"], x["ep"], os.path.basename(x["dst"])[:52]))
    print("  …")
    print("  将把 B 的 %d 个 1080p 改为 .dup" % len(dups))
    if a.dry or not a.apply:
        return
    ts = time.strftime("%Y%m%d_%H%M%S")
    jp = "/opt/embytg/pipeline/fix_shaonian_%s.json" % ts
    j = {"ts": ts, "done": [], "fail": []}
    # 1) 先把 B 的 1080p 标 .dup
    for p, f, e in dups:
        try:
            rename_with_sidecars(p, p + ".dup")
            j["done"].append({"kind": "dup", "from": p, "to": p + ".dup", "ep": e})
        except Exception as ex:
            j["fail"].append({"src": p, "why": str(ex)[:80]})
    # 2) 搬 A 的 4K 进 B
    for x in plan:
        if os.path.exists(x["dst"]):
            j["fail"].append({"src": x["src"], "why": "dst-exists"}); continue
        try:
            rename_with_sidecars(x["src"], x["dst"])
            if x["info"].get("t"):
                write_nfo(x["dst"], x["ep"], x["info"])
            j["done"].append({"kind": "move", "from": x["src"], "to": x["dst"], "ep": x["ep"]})
        except Exception as ex:
            j["fail"].append({"src": x["src"], "why": str(ex)[:80]})
    # 3) A 的 Season 02 清空后删空目录；整剧目录移到 trash
    trash = "/opt/embytg/pipeline/_trash_%s" % ts
    s2 = os.path.join(A_DIR, "Season 02")
    try:
        if os.path.isdir(s2) and not [f for f in os.listdir(s2) if f.lower().endswith(TT.VIDEO_EXT)]:
            shutil.rmtree(s2)
            os.makedirs(trash, exist_ok=True)
            if os.path.isdir(A_DIR):
                shutil.move(A_DIR, os.path.join(trash, os.path.basename(A_DIR)))
            j["done"].append({"kind": "trash", "from": A_DIR, "to": os.path.join(trash, os.path.basename(A_DIR))})
    except Exception as ex:
        j["fail"].append({"src": A_DIR, "why": str(ex)[:80]})
    json.dump(j, open(jp, "w"), ensure_ascii=False, indent=1)
    from collections import Counter
    print("完成 %d 条 %s；失败 %d → %s"
          % (len(j["done"]), dict(Counter(d["kind"] for d in j["done"])), len(j["fail"]), jp))


if __name__ == "__main__":
    main()
