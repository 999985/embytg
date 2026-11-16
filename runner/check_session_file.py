#!/usr/bin/env python3
import argparse
import asyncio

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


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--session-file", required=True)
    args = parser.parse_args()

    config = load_env(args.config)
    with open(args.session_file, encoding="utf-8-sig") as handle:
        session = handle.read().strip()
    client = TelegramClient(
        StringSession(session),
        int(config["TG_API_ID"]),
        config["TG_API_HASH"],
        connection_retries=3,
        timeout=90,
    )
    await client.connect()
    try:
        me = await client.get_me()
        channels = [
            int(value)
            for value in config.get("TG_STORE_CH", "").replace(",", " ").split()
            if value
        ]
        verified = []
        for channel_id in channels:
            result = await client(
                GetChannelsRequest([InputChannel(channel_id, 0)])
            )
            verified.append(str(result.chats[0].id))
        print(
            "SESSION_OK uid=%s username=%s channels=%s"
            % (me.id, me.username or "", ",".join(verified))
        )
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
