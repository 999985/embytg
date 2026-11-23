#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""make_bots3.py —— 建/收下载机器人（轮询版，用 venv 的 telethon 1.45）。

前两版失败原因：
  ① 系统 python 的 telethon 1.44 解析不了 BotFather 的新按钮类型
     （TypeNotFoundError: b2b15770）—— 必须用 runner venv 的 1.45。
  ② conversation() 在多次操作后抛 InvalidStateError —— 改用「发消息 + 轮询最新回复」。

用法：
    /opt/embytg-runner/venv/bin/python make_bots3.py --want 15,17,19,21 --apply
    /opt/embytg-runner/venv/bin/python make_bots3.py --recover 14,16,18,20 --apply
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
import urllib.request

sys.path.insert(0, "/opt/embytg-runner")
from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402
from telethon.tl.functions.channels import EditAdminRequest, InviteToChannelRequest  # noqa: E402
from telethon.tl.types import ChatAdminRights               # noqa: E402

ENV = "/opt/embytg-runner/runner.env"
TOKENS = "/opt/embytg/.bot_tokens.json"
STORE_CH = -1004413117262
API = "http://127.0.0.1:8080"
RE = re.compile(r"(\d{8,12}:[A-Za-z0-9_-]{35})")


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


async def last_msg(c, wait=2.0):
    await asyncio.sleep(wait)
    msgs = await c.get_messages("@BotFather", limit=1)
    return msgs[0] if msgs else None


async def send(c, text, wait=2.0):
    await c.send_message("@BotFather", text)
    return await last_msg(c, wait)


async def create_bot(c, display, username):
    await send(c, "/cancel", 1.2)
    r = await send(c, "/newbot", 2.0)
    body = (r.message if r else "") or ""
    if "name" not in body.lower():
        r = await send(c, "/newbot", 2.5)
        body = (r.message if r else "") or ""
    if "name" not in body.lower():
        raise RuntimeError("未进入命名步骤: %s" % body[:70])
    r = await send(c, display, 2.0)
    body = (r.message if r else "") or ""
    if "username" not in body.lower():
        raise RuntimeError("未进入用户名步骤: %s" % body[:70])
    r = await send(c, username, 3.0)
    body = (r.message if r else "") or ""
    if "sorry" in body.lower() or "already" in body.lower():
        raise RuntimeError("用户名不可用: %s" % body[:70])
    m = RE.search(body)
    return m.group(1) if m else None


async def recover_token(c, username):
    """/mybots → 选 bot → API Token。"""
    r = await send(c, "/mybots", 2.5)
    if not r or not r.buttons:
        return None
    for row in r.buttons:
        for b in row:
            if (b.text or "").lower().lstrip("@") == username.lower():
                await b.click()
                r2 = await last_msg(c, 2.0)
                if not r2 or not r2.buttons:
                    return None
                for row2 in r2.buttons:
                    for b2 in row2:
                        if "api token" in (b2.text or "").lower():
                            await b2.click()
                            r3 = await last_msg(c, 2.5)
                            txt = (r3.message if r3 else "") or ""
                            m = RE.search(txt)
                            return m.group(1) if m else None
    return None


async def ensure_admin(c, username):
    ent = await c.get_entity(username)
    try:
        await c(InviteToChannelRequest(STORE_CH, [ent]))
    except Exception:                                       # noqa: BLE001
        pass
    await c(EditAdminRequest(
        STORE_CH, ent,
        ChatAdminRights(post_messages=True, edit_messages=True, delete_messages=True),
        rank="dl"))


def register(tokens):
    tok = (os.popen("bash /opt/embytg/api.sh 2>/dev/null").read().strip().splitlines() or [""])[-1]
    h = {"Authorization": "Bearer " + tok, "Content-Type": "application/json"}

    def post(path):
        req = urllib.request.Request(API + path, data=b"{}", method="POST", headers=h)
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status
    s1 = None
    try:
        req = urllib.request.Request(API + "/v1/bots",
                                     data=json.dumps({"tokens": tokens}).encode(),
                                     method="POST", headers=h)
        with urllib.request.urlopen(req, timeout=60) as r:
            s1 = r.status
        s2 = post("/v1/bots/provision")
        return s1, s2
    except Exception as e:                                  # noqa: BLE001
        return s1, str(e)[:100]


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
    # 编号 → 账号：14/15 → slot1，16/17 → slot2，18/19 → slot3，20/21 → slot4
    want = [int(x) for x in a.want.split(",") if x.strip()]
    got = []
    for idx in want:
        slot = 1 + (idx - 14) // 2
        c = TelegramClient(StringSession(sess(e, slot)), int(e["TG_API_ID"]), e["TG_API_HASH"])
        await c.connect()
        user = "embytg_dl_%d_bot" % idx
        disp = "embytg dl %d" % idx
        try:
            print("  [slot%d] %s" % (slot, user), flush=True)
            tok = await recover_token(c, user)
            if tok:
                print("    ↺ 已存在，token 回收成功", flush=True)
            elif a.recover:
                print("    ✘ 回收失败", flush=True)
                continue
            else:
                tok = await create_bot(c, disp, user)
                if not tok:
                    tok = await recover_token(c, user)
                print("    ✔ 新建", flush=True) if tok else print("    ✘ 建失败", flush=True)
            if not tok:
                continue
            try:
                await ensure_admin(c, user)
            except Exception as ex:                          # noqa: BLE001
                print("    ! 管理员失败: %s" % str(ex)[:60], flush=True)
            got.append({"username": user, "display_name": disp, "token": tok})
        except Exception as ex:                              # noqa: BLE001
            print("    ✘ %s" % str(ex)[:90], flush=True)
        finally:
            await c.disconnect()
        await asyncio.sleep(2)
    if a.dry:
        print("  DRY 完成"); return 0
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
    ap.add_argument("--recover", action="store_true")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args)))
