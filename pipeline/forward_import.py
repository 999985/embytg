#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""forward_import.py —— 频道里已有的片，**零上行**入库。

原理：Telegram 的转发是**服务端复制**，文件字节不经本机网络。所以只要源频道
里已经有这部片，转发到存储频道 + 登记即可，完全不消耗上传带宽。

（tgsrc_copy.py 是「下载再上传」，那要吃掉一整集的流量；这里走转发。）

用法：
    python3 forward_import.py --channel yfqh06 --msg 2165 \
        --dir "Movies/动漫电影/斗破苍穹剧场版：双帝之战 (2026)" \
        --name "斗破苍穹剧场版：双帝之战 (2026) - 2160p.mp4" \
        --tmdb 1374700 --lib "Movies/动漫电影" --year 2026 --apply

不加 --apply 只做检查（转发前先看清楚）。
"""
import argparse
import asyncio
import json
import os
import re
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/opt/embytg-runner")

import tgpipe as TP                    # noqa: E402  (teldrive 登记)
import emos_import as EI               # noqa: E402  (tg.db 登记 / NFO / 扫描)
import gha_state as GS                 # noqa: E402
from telethon import TelegramClient    # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

RUNNER_ENV = "/opt/embytg-runner/runner.env"
STORE_CH = -1004413117262   # 存储频道（必须带 -100 前缀，否则被当成用户）
MNT = "/opt/embytg/mnt/embytg"


def load_env():
    env = {}
    for line in open(RUNNER_ENV, encoding="utf-8"):
        m = re.match(r"^([A-Z0-9_]+)=(.*)$", line.strip())
        if m:
            env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return env


def pick_slot():
    rid = "fwd-%d" % os.getpid()
    for n in (1, 2, 3, 4):
        if GS.slot_lease(n, rid, ttl=1800):
            return n, rid
    raise SystemExit("没有空闲 slot")


async def do_forward(env, slot, channel, msg, name):
    sess = env.get("TG_SESSION_STRING" if slot == 1 else "TG_SESSION_STRING_%d" % slot) \
        or env["TG_SESSION_STRING"]
    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    try:
        src = await c.get_entity(channel)
        new = await c.forward_messages(STORE_CH, msg, from_peer=src)
        if isinstance(new, list):
            new = new[0]
        f = new.file
        return {"message_id": new.id, "size": (f.size if f else 0),
                "name": (f.name if f else name)}
    finally:
        await c.disconnect()


def write_movie_nfo(dir_path, name, tmdb, title, year):
    base = os.path.splitext(name)[0]
    p = os.path.join(dir_path, base + ".nfo")
    xml = ("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<movie>\n"
           "  <title>%s</title>\n  <year>%s</year>\n"
           "  <uniqueid type=\"tmdb\" default=\"true\">%s</uniqueid>\n"
           "</movie>\n" % (title, year or "", tmdb or ""))
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(xml)
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--msg", type=int, required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--tmdb", default="")
    ap.add_argument("--lib", default="Movies")
    ap.add_argument("--title", default="")
    ap.add_argument("--year", default="")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    env = load_env()
    slot, rid = pick_slot()
    try:
        print("  用 slot%d 转发 @%s msg=%d → 存储频道" % (slot, a.channel, a.msg),
              flush=True)
        if not a.apply:
            print("  DRY：目标 %s/%s" % (a.dir, a.name))
            return 0
        r = asyncio.run(do_forward(env, slot, a.channel, a.msg, a.name))
        print("  转发完成: 新 msg=%s  %.2f GB" % (r["message_id"], r["size"] / 1e9))

        parent = None
        for seg in a.dir.strip("/").split("/"):
            parent = TP.ensure_folder(seg, parent)
        fid, total = TP.register(parent, a.name, [(r["message_id"], r["size"])])
        print("  teldrive 已登记: %s  %.2f GB" % (fid[:8], total / 1e9))

        if a.tmdb:
            ok = EI.register_tgdb(a.dir, fid, a.name,
                                  [{"part_no": 1, "message_id": r["message_id"],
                                    "plain_size": r["size"], "stored_size": r["size"],
                                    "channel_id": STORE_CH}],
                                  int(a.tmdb), a.lib)
            print("  tg.db 登记:", "OK" if ok else "失败")

        dest_dir = os.path.join(MNT, a.dir)
        os.makedirs(dest_dir, exist_ok=True)
        nfo = write_movie_nfo(dest_dir, a.name, a.tmdb,
                              a.title or a.name.rsplit(" - ", 1)[0], a.year)
        print("  NFO:", nfo)
        EI.scan_one("/mnt/embytg/%s/%s" % (a.dir.strip("/"), a.name))
        print("  已触发 Emby 扫描")
        return 0
    finally:
        GS.slot_release(slot, rid)


if __name__ == "__main__":
    sys.exit(main())
