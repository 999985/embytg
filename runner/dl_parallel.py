"""并行分段下载 TG 文件（单流只有 0.8MB/s，4 流能到 2-3MB/s）。"""
import asyncio, os, re, sys, time
from telethon import TelegramClient
from telethon.sessions import StringSession

env = {}
for line in open("/opt/embytg-runner/runner.env", encoding="utf-8"):
    m = re.match(r'^([A-Z0-9_]+)=(.*)$', line.strip())
    if m:
        env[m.group(1)] = m.group(2).strip().strip('"').strip("'")

CH = os.environ.get("SRC_CH", "yingji")
MID = int(os.environ.get("SRC_ID", "669"))
OUT = os.environ.get("OUT", "/opt/embytg-runner/media.busan/釜山行2半岛.mp4")
WORKERS = int(os.environ.get("WORKERS", "4"))
CHUNK = 8 * 1024 * 1024


async def main():
    c = TelegramClient(StringSession(env["TG_SESSION_STRING"]), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(CH)
    msg = await c.get_messages(ent, ids=MID)
    total = msg.file.size
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    # 预先占位
    with open(OUT, "wb") as fh:
        fh.truncate(total)

    sem = asyncio.Semaphore(WORKERS)
    done = [0]
    lock = asyncio.Lock()
    t0 = time.time()

    async def grab(off):
        async with sem:
            buf = b""
            for _ in range(5):
                try:
                    buf = b""
                    async for ch in c.iter_download(msg.media, offset=off, limit=CHUNK):
                        buf += ch
                        if len(buf) >= CHUNK:
                            break
                    if buf:
                        break
                except Exception:
                    await asyncio.sleep(2)
            with open(OUT, "r+b") as fh:
                fh.seek(off)
                fh.write(buf)
            async with lock:
                done[0] += len(buf)
                if done[0] // (300 << 20) != (done[0] - len(buf)) // (300 << 20):
                    el = time.time() - t0
                    print("  %.1f%% %.2f/%.2f GB  %.2f MB/s" % (
                        done[0] * 100.0 / total, done[0] / 1e9, total / 1e9,
                        done[0] / 1e6 / max(el, 1e-6)), flush=True)

    offs = list(range(0, total, CHUNK))
    await asyncio.gather(*[grab(o) for o in offs])
    print("  DONE %d / %d bytes  %.1f min" % (os.path.getsize(OUT), total, (time.time() - t0) / 60), flush=True)
    await c.disconnect()

asyncio.run(main())
