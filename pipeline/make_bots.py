#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""make_bots.py —— 给每个本机账号批量申请下载机器人并加入池子。

用户要求（2026-11-24）：每个号再申请两个机器人，加到池子里，token 存项目里。

流程（每个 bot）：
    ① 对应账号开 BotFather 会话 → /newbot → 名字 → 用户名
    ② 拿到 token
    ③ 把 bot 加进存储频道为管理员（读+发+编辑+删除）
    ④ 汇总 token → POST /v1/bots → /v1/bots/provision
    ⑤ 写 .bot_tokens.json（600）

用法：
    python3 make_bots.py --per-account 2 --start-index 14 --dry
    python3 make_bots.py --per-account 2 --start-index 14 --apply
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
sys.path.insert(0, HERE)
sys.path.insert(0, "/opt/embytg-runner")

from telethon import TelegramClient                       # noqa: E402
from telethon.sessions import StringSession               # noqa: E402
from telethon.tl.functions.channels import EditAdminRequest, InviteToChannelRequest  # noqa: E402
from telethon.tl.types import ChatAdminRights              # noqa: E402

ENV = "/opt/embytg-runner/runner.env"
TOKENS = "/opt/embytg/.bot_tokens.json"
STORE_CH = -1004413117262
API = "http://127.0.0.1:8080"


def load_env():
    d = {}
    for line in open(ENV, encoding="utf-8"):
        m = re.match(r"^([A-Z0-9_]+)=(.*)$", line.strip())
        if m:
            d[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return d


def session_of(e, slot):
    key = "TG_SESSION_STRING" if slot == 1 else "TG_SESSION_STRING_%d" % slot
    return e.get(key) or e["TG_SESSION_STRING"]


def api_token():
    try:
        return (os.popen("bash /opt/embytg/api.sh 2>/dev/null").read().strip().splitlines() or [""])[-1]
    except Exception:                                       # noqa: BLE001
        return ""


async def make_one(client, display, username):
    """走 BotFather 建一个 bot，返回 token 或 None。"""
    async with client.conversation("@BotFather", timeout=90) as conv:
        await conv.send_message("/cancel")
        try:
            await conv.get_response(timeout=8)
        except Exception:                                   # noqa: BLE001
            pass
        await conv.send_message("/newbot")
        r = await conv.get_response()
        if "cancel" in (r.message or "").lower():
            raise RuntimeError("BotFather 状态异常: %s" % (r.message or "")[:60])
        await conv.send_message(display)
        r = await conv.get_response()
        if "username" not in (r.message or "").lower():
            raise RuntimeError("没等到用户名提示: %s" % (r.message or "")[:80])
        await conv.send_message(username)
        r = await conv.get_response()
        m = re.search(r"(\d{8,12}:[A-Za-z0-9_-]{35})", r.message or "")
        if not m:
            raise RuntimeError("没拿到 token: %s" % (r.message or "")[:120])
        return m.group(1)


async def add_admin(client, bot_username):
    ent = await client.get_entity(bot_username)
    try:
        await client(InviteToChannelRequest(STORE_CH, [ent]))
    except Exception:                                       # noqa: BLE001
        pass                                                # 已是成员
    await client(EditAdminRequest(
        STORE_CH, ent,
        ChatAdminRights(post_messages=True, edit_messages=True,
                        delete_messages=True, read_messages=True),
        rank="dl"))


def register_tokens(tokens):
    tok = api_token()
    def call(path, body):
        h = {"Authorization": "Bearer " + tok, "Content-Type": "application/json"}
        req = urllib.request.Request(API + path, data=json.dumps(body).encode(),
                                     method="POST", headers=h)
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status
    try:
        s1 = call("/v1/bots", {"tokens": tokens})
        s2 = call("/v1/bots/provision", {})
        return s1, s2
    except Exception as e:                                  # noqa: BLE001
        return "err", str(e)[:120]


def save_tokens(rows):
    data = []
    if os.path.exists(TOKENS):
        data = json.load(open(TOKENS, encoding="utf-8"))
    have = {(x.get("username") or "").lower() for x in data}
    for r in rows:
        if r["username"].lower() in have:
            continue
        data.append({"username": r["username"], "display_name": r["display_name"],
                     "token": r["token"], "created_at": int(time.time())})
    tmp = TOKENS + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, TOKENS)
    return len(data)


async def run(args):
    e = load_env()
    slots = [1, 2, 3, 4]
    made = []
    idx = args.start_index
    for slot in slots:
        sess = session_of(e, slot)
        c = TelegramClient(StringSession(sess), int(e["TG_API_ID"]), e["TG_API_HASH"])
        await c.connect()
        try:
            for k in range(args.per_account):
                user = "embytg_dl_%d_bot" % idx
                disp = "embytg dl %d" % idx
                print("  [slot%d] 建 %s (%s)" % (slot, user, disp), flush=True)
                if args.dry:
                    idx += 1
                    continue
                try:
                    tok = await make_one(c, disp, user)
                    await add_admin(c, user)
                    made.append({"username": user, "display_name": disp,
                                 "token": tok, "slot": slot})
                    print("    ✔ %s" % user, flush=True)
                except Exception as ex:                     # noqa: BLE001
                    print("    ✘ %s: %s" % (user, str(ex)[:100]), flush=True)
                idx += 1
                await asyncio.sleep(2)
        finally:
            await c.disconnect()
    if args.dry:
        print("  DRY：会建 %d 个 bot（每号 %d 个）" % (args.per_account * len(slots), args.per_account))
        return 0
    if not made:
        print("  一个都没建成")
        return 1
    print("  建成 %d 个，开始登记 teldrive" % len(made))
    st = register_tokens([m["token"] for m in made])
    print("  /v1/bots:", st[0], "/provision:", st[1])
    n = save_tokens(made)
    print("  .bot_tokens.json 现在 %d 条" % n)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-account", type=int, default=2)
    ap.add_argument("--start-index", type=int, default=14)
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if not (a.dry or a.apply):
        print("加 --dry 或 --apply")
        return 2
    return asyncio.run(run(a))


if __name__ == "__main__":
    sys.exit(main())
