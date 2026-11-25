import asyncio, re, os, sys
from telethon import TelegramClient
from telethon.sessions import StringSession

env = {}
for line in open("/opt/embytg-runner/runner.env", encoding="utf-8"):
    m = re.match(r'^([A-Z0-9_]+)=(.*)$', line.strip())
    if m:
        env[m.group(1)] = m.group(2).strip().strip('"').strip("'")

OUT = "/opt/embytg-runner/media.busan/釜山行2半岛.1080p.mp4"
os.makedirs(os.path.dirname(OUT), exist_ok=True)


async def main():
    c = TelegramClient(StringSession(env["TG_SESSION_STRING"]), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity("yfqh06")

    last = [0]

    async def prog(done, total):
        if total and done - last[0] > 200 * 1024 * 1024:
            last[0] = done
            print("  %.1f%% %.2f/%.2f GB" % (done * 100 / total, done / 1e9, total / 1e9), flush=True)

    await c.download_media(await c.get_messages(ent, ids=296), file=OUT, progress_callback=prog)
    print("  DONE %d bytes" % os.path.getsize(OUT), flush=True)
    await c.disconnect()

asyncio.run(main())
