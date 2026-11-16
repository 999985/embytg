#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交互式登录 TG，生成新的 StringSession。

2026-11-14：旧 session 因 AuthKeyDuplicatedError 被 TG 永久作废，
必须重新登录。本脚本在终端交互：手机号 → 验证码 →（如需要）两步密码。

结果写到 /tmp/tg_session_new.txt（0600），不打印到 stdout 以外的日志。
"""
import asyncio
import os
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession

ENV = "/opt/embytg-runner/runner.env"
OUT = "/tmp/tg_session_new.txt"


def load_env():
    d = {}
    for line in open(ENV, encoding="utf-8"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            d[k.strip()] = v.strip().strip('"').strip("'")
    return d


async def main():
    env = load_env()
    api_id = int(env["TG_API_ID"])
    api_hash = env["TG_API_HASH"]

    print("=" * 60)
    print("Telegram 登录 —— 生成新的 session")
    print("=" * 60)
    print(f"  API_ID  : {api_id}")
    print(f"  输出    : {OUT}")
    print()
    print("请输入手机号（含国家码，例 +8613800138000）")
    phone = input("手机号: ").strip()
    if not phone:
        print("没输入手机号，退出")
        return 1

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        sent = await client.send_code_request(phone)
        print()
        print("验证码已发到你的 TG（或短信）。")
        print(f"  code_type = {type(sent.type).__name__}")
        print()
        code = input("验证码: ").strip()

        try:
            await client.sign_in(phone=phone, code=code,
                                 phone_code_hash=sent.phone_code_hash)
        except Exception as e:                             # noqa: BLE001
            name = type(e).__name__
            if "SessionPasswordNeeded" in name:
                print()
                print("该账号开了两步验证（2FA），请输入密码：")
                import getpass
                pw = getpass.getpass("2FA 密码: ")
                await client.sign_in(password=pw)
            else:
                print(f"登录失败: {name}: {e}")
                return 2

    me = await client.get_me()
    sess = client.session.save()
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(sess)
    os.chmod(OUT, 0o600)

    print()
    print("=" * 60)
    print(f"✔ 登录成功：{me.first_name or ''} (@{me.username or '-'}) id={me.id}")
    print(f"✔ session 已写入 {OUT}（600，{len(sess)} 字符）")
    print("=" * 60)
    await client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
