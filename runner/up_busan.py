"""并行分片上传：4 个独立 session 各传一段，最后按偏移顺序登记成 parts。

为什么不用 pc_upload 的 jobs>1：多个 worker 共用同一个 session 时
get_entity 会并发冲突，worker 直接 IndexError（今天实测 jobs=2/4 全崩），
而 jobs=1 只有 0.15-0.3MB/s。改成「一个 session 一个段」就绕开了。
"""
import asyncio
import json
import os
import re
import subprocess
import sys

HERE = "/opt/embytg-runner"
SRC = os.path.join(HERE, "media.busan/釜山行2：半岛 (2020) {tmdb=581392} - 720p.mp4")
NAME = "釜山行2：半岛 (2020) {tmdb=581392} - 720p.mp4"
PIECES = int(os.environ.get("PIECES", "6"))

env = {}
for line in open(os.path.join(HERE, "runner.env"), encoding="utf-8"):
    m = re.match(r'^([A-Z0-9_]+)=(.*)$', line.strip())
    if m:
        env[m.group(1)] = m.group(2).strip().strip('"').strip("'")

SESSIONS = [env.get("TG_SESSION_STRING"), env.get("TG_SESSION_STRING_2"),
            env.get("TG_SESSION_STRING_3"), env.get("TG_SESSION_STRING_4")]
SESSIONS = [s for s in SESSIONS if s]


async def main():
    total = os.path.getsize(SRC)
    step = (total + PIECES - 1) // PIECES
    jobs = []
    for i in range(PIECES):
        off = i * step
        ln = min(step, total - off)
        e = dict(env)
        e["TG_SESSION_STRING"] = SESSIONS[i % len(SESSIONS)]
        e["TG_CHUNK_MIB"] = "1800"
        e["TG_CHUNK_DEADLINE"] = os.environ.get("TG_CHUNK_DEADLINE", "5400")
        jobs.append((i, off, ln, e))

    async def run(i, off, ln, envv):
        cmd = [sys.executable, "-u", os.path.join(HERE, "pc_worker.py"),
               "--src", SRC, "--offset", str(off), "--length", str(ln), "--name", NAME]
        p = await asyncio.create_subprocess_exec(*cmd, cwd=HERE, env=envv,
                                                 stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
        out, err = await p.communicate()
        txt = (out or b"").decode("utf-8", "replace")
        mid = None
        for line in txt.splitlines():
            if line.startswith("MSG="):
                mid = int(line[4:].strip())
        print("  piece%d off=%d len=%.0fMB → msg=%s rc=%d %s" % (
            i, off, ln / 1e6, mid, p.returncode,
            (err or b"").decode("utf-8", "replace")[-120:] if not mid else ""), flush=True)
        return (off, ln, mid)

    res = await asyncio.gather(*[run(*j) for j in jobs])
    res.sort()
    parts = [(mid, ln) for off, ln, mid in res if mid]
    json.dump({"name": NAME, "parts": parts, "total": total},
              open(os.path.join(HERE, "media.busan/parts.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("  完成 %d/%d 段，总 %d 字节" % (len(parts), PIECES, total), flush=True)


asyncio.run(main())
