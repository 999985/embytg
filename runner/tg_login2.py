#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""分两步登录 TG：send-code / sign-in。

用法：
  tg_login2.py send   "+8613..."   → 发验证码，把 phone_code_hash 存到 /tmp/tg_login_state.json
  tg_login2.py sign   "12345"      → 提交验证码；若需 2FA 会提示
  tg_login2.py pass   "密码"        → 提交两步验证密码
"""
import asyncio
import json
import os
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession

ENV = "/opt/embytg-runner/runner.env"
STATE = "/tmp/tg_login_state.json"
OUT = "/tmp/tg_session_new.txt"


def load_env():
    d = {}
    for line in open(ENV, encoding="utf-8"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            d[k.strip()] = v.strip().strip('"').strip("'")
    return d


def load_state():
    try:
        return json.load(open(STATE, encoding="utf-8"))
    except Exception:
        return {}


def save_state(d):
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)
    os.chmod(STATE, 0o600)


async def send_code(phone):
    env = load_env()
    c = TelegramClient(StringSession(), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    if await c.is_user_authorized():
        me = await c.get_me()
        print(f"ALREADY_AUTH id={me.id} name={me.first_name}")
        sess = c.session.save()
        open(OUT, "w", encoding="utf-8").write(sess)
        os.chmod(OUT, 0o600)
        await c.disconnect()
        return 0
    s = await c.send_code_request(phone)
    save_state({
        "phone": phone,
        "hash": s.phone_code_hash,
        "session": c.session.save(),
        "type": type(s.type).__name__,
    })
    print(f"CODE_SENT type={type(s.type).__name__}")
    print(f"next_timeout={getattr(s, 'next_type', None) and type(s.next_type).__name__}")
    await c.disconnect()
    return 0


async def sign_in(code):
    env = load_env()
    st = load_state()
    if not st:
        print("NO_STATE: 先跑 send")
        return 2
    c = TelegramClient(StringSession(st["session"]),
                       int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    try:
        await c.sign_in(phone=st["phone"], code=code,
                        phone_code_hash=st["hash"])
    except Exception as e:                                 # noqa: BLE001
        name = type(e).__name__
        if "SessionPasswordNeeded" in name:
            save_state({**st, "session": c.session.save()})
            print("NEED_2FA")
            await c.disconnect()
            return 3
        print(f"SIGNIN_FAIL {name}: {str(e)[:200]}")
        save_state({**st, "session": c.session.save()})
        await c.disconnect()
        return 4
    me = await c.get_me()
    sess = c.session.save()
    open(OUT, "w", encoding="utf-8").write(sess)
    os.chmod(OUT, 0o600)
    print(f"OK id={me.id} name={me.first_name} user=@{me.username}")
    print(f"SESSION_LEN={len(sess)}")
    await c.disconnect()
    return 0


async def send_pass(pw):
    env = load_env()
    st = load_state()
    if not st:
        print("NO_STATE")
        return 2
    c = TelegramClient(StringSession(st["session"]),
                       int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    try:
        await c.sign_in(password=pw)
    except Exception as e:                                 # noqa: BLE001
        print(f"PASS_FAIL {type(e).__name__}: {str(e)[:200]}")
        await c.disconnect()
        return 4
    me = await c.get_me()
    sess = c.session.save()
    open(OUT, "w", encoding="utf-8").write(sess)
    os.chmod(OUT, 0o600)
    print(f"OK id={me.id} name={me.first_name} user=@{me.username}")
    print(f"SESSION_LEN={len(sess)}")
    await c.disconnect()
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    cmd, arg = sys.argv[1], sys.argv[2]
    fn = {"send": send_code, "sign": sign_in, "pass": send_pass}.get(cmd)
    if not fn:
        print("cmd 只能是 send / sign / pass")
        sys.exit(1)
    sys.exit(asyncio.run(fn(arg)) or 0)
