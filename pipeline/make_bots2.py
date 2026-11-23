#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""make_bots2.py —— 补建/回收下载机器人（修复版）。

第一版两个 bug（2026-11-24）：
  ① ChatAdminRights 没有 read_messages 参数 → 加管理员必抛异常，
     但 bot 其实已经建好了，token 丢在异常里。
  ② 第二个 bot 走 /newbot 时 BotFather 状态没复位，回的是帮助文本。

这一版：
  · 加管理员只传合法字段
  · 建 bot 前先 /cancel + 排空回复
  · **token 一律从 /mybots → 该 bot → API Token 取**，不依赖创建时的消息
    （这样即使创建流程中断，也能把已有 bot 的 token 捞回来）
  · 已存在就跳过创建，直接取 token

用法：
    python3 make_bots2.py --want 15,17,19,21 --apply
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, "/opt/embytg-runner")
sys.path.insert(0, HERE)

from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402
from telethon.tl.functions.channels import EditAdminRequest, InviteToChannelRequest  # noqa: E402
from telethon.tl.types import ChatAdminRights               # noqa: E402

ENV = "/opt/embytg-runner/runner.env"
TOKENS = "/opt/embytg/.bot_tokens.json"
STORE_CH = -1004413117262
API = "http://127.0.0.1:8080"
TOKEN_RE = re.compile(r"(\d{8,12}:[A-Za-z0-9_-]{35})")


def env():
    d = {}
    for line in open(ENV, encoding="utf-8"):
        m = re.match(r"^([A-Z0-9_]+)=(.*)$", line.strip())
        if m:
            d[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return d


def sess(e, slot):
    k = "TG_SESSION_STRING" if slot == 1 else "TG_SESSION_STRING_%d" % slot
    return e.get(k) or e["TG_SESSION_STRING"]


async def drain(conv, seconds=2.0):
    end = time.time() + seconds
    while time.time() < end:
        try:
            await conv.get_response(timeout=0.6)
        except Exception:                                   # noqa: BLE001
            break


async def list_bots(conv):
    await conv.send_message("/mybots")
    r = await conv.get_response(timeout=30)
    return [b.text for row in (r.buttons or []) for b in row]


async def fetch_token(conv, username):
    """/mybots → 点该 bot → API Token → 取 token。"""
    names = await list_bots(conv)
    target = None
    for n in names:
        if n and n.lower().lstrip("@") == username.lower():
            target = n
            break
    if not target:
        return None
    await conv.send_message(target)
    r = await conv.get_response(timeout=30)
    labels = [b.text for row in (r.buttons or []) for b in row]
    btn = next((b for b in labels if b and "api token" in b.lower()), None)
    if not btn:
        return None
    await conv.send_message(btn)
    r = await conv.get_response(timeout=30)
    m = TOKEN_RE.search(r.message or "")
    return m.group(1) if m else None


async def create_bot(conv, display, username):
    await conv.send_message("/cancel")
    await drain(conv, 1.5)
    await conv.send_message("/newbot")
    r = await conv.get_response(timeout=30)
    if "username" in (r.message or "").lower() and "bot" in (r.message or "").lower() and "choose" in (r.message or "").lower():
        pass                                            # 直接要用户名
    else:
        await conv.send_message(display)
        r = await conv.get_response(timeout=30)
        if "username" not in (r.message or "").lower():
            raise RuntimeError("未进入用户名步骤: %s" % (r.message or "")[:80])
    await conv.send_message(username)
    r = await conv.get_response(timeout=30)
    txt = r.message or ""
    if "sorry" in txt.lower() or "taken" in txt.lower():
        raise RuntimeError("用户名被占: %s" % txt[:60])
    m = TOKEN_RE.search(txt)
    return m.group(1) if m else None


async def ensure_admin(client, username):
    ent = await client.get_entity(username)
    try:
        await client(InviteToChannelRequest(STORE_CH, [ent]))
    except Exception:                                       # noqa: BLE001
        pass
    await client(EditAdminRequest(
        STORE_CH, ent,
        ChatAdminRights(post_messages=True, edit_messages=True, delete_messages=True),
        rank="dl"))


def register(tokens):
    tok = (os.popen("bash /opt/embytg/api.sh 2>/dev/null").read().strip().splitlines() or [""])[-1]
    h = {"Authorization": "Bearer " + tok, "Content-Type": "application/json"}

    def post(path, body):
        req = urllib.request.Request(API + path, data=json.dumps(body).encode(),
                                     method="POST", headers=h)
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status
    try:
        return post("/v1/bots", {"tokens": tokens}), post("/v1/bots/provision", {})
    except Exception as e:                                  # noqa: BLE001
        return "err", str(e)[:100]


def save(rows):
    data = json.load(open(TOKENS, encoding="utf-8")) if os.path.exists(TOKENS) else []
    have = {(x.get("username") or "").lower() for x in data}
    added = 0
    for r in rows:
        if r["username"].lower() in have:
            continue
        data.append({"username": r["username"], "display_name": r["display_name"],
                     "token": r["token"], "created_at": int(time.time())})
        added += 1
    tmp = TOKENS + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, TOKENS)
    return len(data), added


async def main(a):
    e = env()
    want = [int(x) for x in a.want.split(",") if x.strip()]
    got = []
    for slot in (1, 2, 3, 4):
        c = TelegramClient(StringSession(sess(e, slot)), int(e["TG_API_ID"]), e["TG_API_HASH"])
        await c.connect()
        try:
            async with c.conversation("@BotFather", timeout=120) as conv:
                for idx in want:
                    user = "embytg_dl_%d_bot" % idx
                    disp = "embytg dl %d" % idx
                    # 该号上的 bot：14→slot1, 16→slot2, 18→slot3, 20→slot4（偶数），
                    # 目标奇数 15/17/19/21 也按同号建：15→slot1 等
                    owner = 1 + ((idx - 14) // 2)
                    if owner != slot:
                        continue
                    print("  [slot%d] %s" % (slot, user), flush=True)
                    tok = None
                    try:
                        tok = await fetch_token(conv, user)
                    except Exception:                       # noqa: BLE001
                        tok = None
                    if not tok:
                        if a.dry:
                            print("    需新建（dry）"); continue
                        try:
                            tok = await create_bot(conv, disp, user)
                        except Exception as ex:              # noqa: BLE001
                            print("    ✘ 建失败: %s" % str(ex)[:80]); continue
                        tok = tok or await fetch_token(conv, user)
                    if not tok:
                        print("    ✘ 拿不到 token"); continue
                    try:
                        await ensure_admin(c, user)
                    except Exception as ex:                  # noqa: BLE001
                        print("    ! 加管理员失败（token 已拿到）: %s" % str(ex)[:60])
                    got.append({"username": user, "display_name": disp, "token": tok})
                    print("    ✔ token 已取到", flush=True)
                    await asyncio.sleep(2)
        finally:
            await c.disconnect()
    if a.dry:
        print("  DRY：%d 个待处理" % len(want)); return 0
    if not got:
        print("  没有新增"); return 1
    st = register([g["token"] for g in got])
    print("  /v1/bots:", st[0], "/provision:", st[1])
    total, added = save(got)
    print("  .bot_tokens.json: %d 条（新增 %d）" % (total, added))
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--want", default="15,17,19,21")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args)))
