#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""merge_parts_fix.py —— 把「被源站拆成两个独立 mp4」的片子合并成一个再入库。

背景（2026-11-24）：
    《斗破苍穹剧场版：双帝之战》在源频道被切成 seg1(3.55GB) + seg2(3.82GB)。
    我按项目既有做法把它们登记成同一个 teldrive 文件的两个 part —— 结果
    播放不出来、时长只有 68 分钟。

    原因是「分片」有两种，语义完全不同：
      · 字节切分（我们上传大文件用的）→ 按 part 顺序拼接就是原文件，teldrive 拼回来能播
      · 独立封装（源站切的两个 mp4）→ 各自有 moov 头，字节拼接是非法的 mp4
    所以这部片必须先用 ffmpeg 无损合并，再按字节切分上传。

用法：
    python3 merge_parts_fix.py --file-id <teldrive文件id> --name "..." --apply
"""
import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/opt/embytg-runner")

import gha_state as GS          # noqa: E402
import tgpipe as TP             # noqa: E402
import tgsrc_copy as TC         # noqa: E402
import emos_import as EI        # noqa: E402
import emby_scan                # noqa: E402
from telethon import TelegramClient          # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

WORK = "/opt/embytg-runner/media.merge"
RUNNER_ENV = "/opt/embytg-runner/runner.env"


def env():
    d = {}
    for line in open(RUNNER_ENV, encoding="utf-8"):
        m = re.match(r"^([A-Z0-9_]+)=(.*)$", line.strip())
        if m:
            d[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return d


def parts_of(file_id):
    rows = TP.sql("select message_id, plain_size from teldrive.file_parts "
                  "where file_id='%s' order by part_no" % file_id)
    out = []
    for line in (rows or "").splitlines():
        if "|" in line:
            a, b = line.split("|", 1)
            out.append((int(a), int(b)))
    return out


async def fetch(e, slot, msgs, workdir):
    sess = e.get("TG_SESSION_STRING" if slot == 1 else "TG_SESSION_STRING_%d" % slot) or e["TG_SESSION_STRING"]
    c = TelegramClient(StringSession(sess), int(e["TG_API_ID"]), e["TG_API_HASH"])
    await c.connect()
    os.makedirs(workdir, exist_ok=True)
    out = []
    try:
        for i, mid in enumerate(msgs, 1):
            dest = os.path.join(workdir, "p%d.mp4" % i)
            m = await c.get_messages(-1004413117262, ids=mid)
            if not m or not m.document:
                raise RuntimeError("消息 %s 不存在" % mid)
            print("  下载 part%d msg=%s %.2fGB ..." % (i, mid, m.document.size / 1e9), flush=True)
            await c.download_media(m, file=dest)
            out.append(dest)
            print("  完成 %.2fGB" % (os.path.getsize(dest) / 1e9), flush=True)
    finally:
        await c.disconnect()
    return out


def merge(files, out):
    with open(os.path.join(WORK, "list.txt"), "w", encoding="utf-8") as fh:
        for f in files:
            fh.write("file '%s'\n" % f)
    cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i",
           os.path.join(WORK, "list.txt"), "-c", "copy", "-movflags", "+faststart", out]
    print("  合并中（-c copy，不转码）...", flush=True)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError("ffmpeg 失败: %s" % (r.stderr or "")[-300:])
    dur = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                          "-of", "default=nw=1:nk=1", out], capture_output=True, text=True)
    return os.path.getsize(out), float((dur.stdout or "0").strip() or 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file-id", required=True)
    ap.add_argument("--name", required=True, help="teldrive 里的文件名（含 tmdb 锚点）")
    ap.add_argument("--lib", default="Movies/动画片")
    ap.add_argument("--tmdb", default="")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    e = env()
    pts = parts_of(a.file_id)
    print("  现有 parts: %s" % [(m, round(s / 1e9, 2)) for m, s in pts], flush=True)
    if len(pts) < 2:
        print("  只有一个 part，不需要合并")
        return 0
    if not a.apply:
        print("  DRY：会用 slot 下载 %d 段 → ffmpeg 合并 → 重新上传达标切分" % len(pts))
        return 0

    rid = "merge-%d" % os.getpid()
    slot = 0
    for n in (1, 2, 3, 4):
        if GS.slot_lease(n, rid, ttl=10800):
            slot = n
            break
    if not slot:
        raise SystemExit("没有空闲 slot")
    try:
        files = asyncio.run(fetch(e, slot, [m for m, _ in pts], WORK))
        merged = os.path.join(WORK, "merged.mp4")
        size, dur = merge(files, merged)
        print("  合并完成: %.2f GB / %.1f 分钟" % (size / 1e9, dur / 60), flush=True)
        if dur < 90 * 60:
            raise RuntimeError("合并后时长只有 %.0f 分钟，明显不对，中止" % (dur / 60))

        # 重新上传（按字节切分，teldrive 拼接即合法 mp4）
        new_parts = TC.upload(e, slot, merged)
        print("  重新上传完成: %d 段" % len(new_parts), flush=True)

        # 清旧登记 → 建新登记
        TP.sql("delete from teldrive.file_parts where file_id='%s'" % a.file_id)
        TP.sql("delete from teldrive.files where id='%s'" % a.file_id)
        parent = None
        for seg in a.lib.split("/"):
            parent = TP.ensure_folder(seg, parent)
        fid, total = TP.register(parent, a.name, [(p["message_id"], p["plain_size"]) for p in new_parts]
                                 if isinstance(new_parts, list) and new_parts and isinstance(new_parts[0], dict)
                                 else new_parts)
        print("  新登记: %s  %.2f GB" % (fid[:8], total / 1e9), flush=True)

        rel = "%s/%s" % (a.lib, a.name)
        import sqlite3
        con = sqlite3.connect(os.path.join(HERE, "tg.db"), timeout=30)
        con.execute("update resource_state set size=?, rel=?, path=? where rel like ?",
                    (total, rel, "/opt/embytg/mnt/embytg/" + rel, "%" + a.name.split(" (")[0] + "%"))
        con.commit()
        con.close()
        print("  scan:", emby_scan.scan_paths(["/mnt/embytg/" + rel]), flush=True)
        for f in files + [merged]:
            try:
                os.remove(f)
            except OSError:
                pass
        return 0
    finally:
        GS.slot_release(slot, rid)


if __name__ == "__main__":
    sys.exit(main())
