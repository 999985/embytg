#!/usr/bin/env python3
"""probe_bot.py —— 实时抓机器人应答（含消息编辑），看清交互到底怎么走。

用法:
    probe_bot.py @YSDQBOT "关键词" [选择序号] [等待秒数]

必须持 slot 租约（同 session 跨进程并发会 AuthKeyDuplicated 永久废号）。
"""
import asyncio
import os
import sys

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.custom import Button


def load_env():
    env = {}
    for p in ("/opt/embytg-runner/runner.env", "/opt/embytg/pipeline/.env"):
        if not os.path.exists(p):
            continue
        for line in open(p, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return env


def describe(m, tag):
    txt = (m.raw_text or "").strip().replace("\n", " | ")
    btns = [b.text for row in (m.buttons or []) for b in row if isinstance(b, Button)]
    kind = ""
    if m.document:
        kind = " [文件 %.2fGB %s]" % ((m.document.size or 0) / 1e9, m.document.mime_type or "")
    elif m.photo:
        kind = " [图片]"
    print("  %-8s id=%s%s %s" % (tag, m.id, kind, txt[:300]))
    if btns:
        print("           按钮(%d): %s" % (len(btns), " / ".join(btns[:12])))
    if m.document:
        names = [getattr(a, "file_name", None) for a in (m.document.attributes or [])]
        print("           文件名: %s" % [n for n in names if n][:1])


async def main():
    env = load_env()
    sess = env.get("TG_SESSION_STRING_2") or env["TG_SESSION_STRING"]
    target = sys.argv[1]
    query = sys.argv[2] if len(sys.argv) > 2 else ""
    pick = sys.argv[3] if len(sys.argv) > 3 else ""
    wait = float(sys.argv[4]) if len(sys.argv) > 4 else 12.0

    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(target)
    print("  目标: %s (id=%s)" % (getattr(ent, "title", None) or target, ent.id))

    @c.on(events.NewMessage(chats=ent))
    async def on_new(e):
        if not e.out:
            describe(e.message, "NEW")

    @c.on(events.MessageEdited(chats=ent))
    async def on_edit(e):
        if not e.out:
            describe(e.message, "EDIT")

    await c.send_message(ent, "/start")
    await asyncio.sleep(3)
    if query:
        await c.send_message(ent, query)
        await asyncio.sleep(6)
    if pick:
        await c.send_message(ent, pick)
        await asyncio.sleep(wait * 2)
    else:
        await asyncio.sleep(wait)
    await c.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
