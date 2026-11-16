#!/usr/bin/env python3
import argparse
import asyncio
import os
import time

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import functions


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
    parser.add_argument("--mib", type=int, default=32)
    args = parser.parse_args()

    config = load_env(args.config)
    with open(args.session_file, encoding="utf-8") as handle:
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
        total = args.mib * 1024 * 1024
        part_size = 512 * 1024
        count = (total + part_size - 1) // part_size
        file_id = int.from_bytes(os.urandom(8), "big", signed=True)
        started = time.time()
        for index in range(count):
            length = min(part_size, total - index * part_size)
            await client(
                functions.upload.SaveBigFilePartRequest(
                    file_id,
                    index,
                    count,
                    b"\0" * length,
                )
            )
        elapsed = time.time() - started
        print(
            "BOT_UPLOAD_OK username=%s uid=%s bytes=%d seconds=%.3f MBps=%.2f"
            % (me.username, me.id, total, elapsed, total / elapsed / 1e6)
        )
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
