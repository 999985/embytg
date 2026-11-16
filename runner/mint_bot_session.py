#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
from urllib.parse import urlparse

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.types import InputChannel


def load_env(path):
    values = {}
    with open(path, encoding="utf-8-sig") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
    return values


def parse_proxy(raw):
    if not raw:
        return None
    parsed = urlparse(raw)
    result = dict(
        proxy_type="http" if parsed.scheme.startswith("http") else "socks5",
        addr=parsed.hostname,
        port=parsed.port,
        rdns=True,
    )
    if parsed.username:
        result["username"] = parsed.username
        result["password"] = parsed.password or ""
    return result


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--bots", default="/opt/embytg/.bot_tokens.json")
    parser.add_argument("--config", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--proxy", default="")
    args = parser.parse_args()

    config = load_env(args.config)
    with open(args.bots, encoding="utf-8") as handle:
        bots = json.load(handle)
    bot = next(
        (item for item in bots if item.get("username") == args.username),
        None,
    )
    if not bot:
        raise SystemExit("bot not found: %s" % args.username)

    channels = [
        int(value)
        for value in config.get("TG_STORE_CH", "").replace(",", " ").split()
        if value
    ]
    client = TelegramClient(
        StringSession(),
        int(config["TG_API_ID"]),
        config["TG_API_HASH"],
        proxy=parse_proxy(args.proxy),
        connection_retries=3,
        timeout=90,
    )
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.sign_in(bot_token=bot["token"])
        me = await client.get_me()
        verified = []
        for channel_id in channels:
            result = await client(
                GetChannelsRequest([InputChannel(channel_id, 0)])
            )
            verified.append(str(result.chats[0].id))
        session = client.session.save()
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        temp = args.out + ".tmp"
        with open(temp, "w", encoding="utf-8") as handle:
            handle.write(session + "\n")
        os.chmod(temp, 0o600)
        os.replace(temp, args.out)
        print(
            "BOT_SESSION_OK username=%s uid=%s channels=%s"
            % (args.username, me.id, ",".join(verified))
        )
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
