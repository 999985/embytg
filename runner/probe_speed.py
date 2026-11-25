"""测四把 session 的当前上传吞吐：各传 3MB，谁快用谁。"""
import asyncio
import os
import re
import sys

HERE = "/opt/embytg-runner"
SRC = os.path.join(HERE, "media.busan/釜山行2：半岛 (2020) {tmdb=581392} - 720p.mp4")

env = {}
for line in open(os.path.join(HERE, "runner.env"), encoding="utf-8"):
    m = re.match(r'^([A-Z0-9_]+)=(.*)$', line.strip())
    if m:
        env[m.group(1)] = m.group(2).strip().strip('"').strip("'")

S = [env.get("TG_SESSION_STRING"), env.get("TG_SESSION_STRING_2"),
     env.get("TG_SESSION_STRING_3"), env.get("TG_SESSION_STRING_4")]
S = [s for s in S if s]
PROBE = 3 * 1024 * 1024


async def one(i, sess):
    e = dict(env)
    e["TG_SESSION_STRING"] = sess
    e["TG_CHUNK_MIB"] = "1800"
    e["TG_CHUNK_DEADLINE"] = "600"
    cmd = [sys.executable, "-u", os.path.join(HERE, "pc_worker.py"),
           "--src", SRC, "--offset", str(i * PROBE), "--length", str(PROBE),
           "--name", "_probe_slot%d.mp4" % (i + 1)]
    t0 = asyncio.get_event_loop().time()
    p = await asyncio.create_subprocess_exec(*cmd, cwd=HERE, env=e,
                                             stdout=asyncio.subprocess.PIPE,
                                             stderr=asyncio.subprocess.PIPE)
    out, err = await p.communicate()
    dt = asyncio.get_event_loop().time() - t0
    txt = (out or b"").decode("utf-8", "replace")
    ok = "MSG=" in txt
    print("  slot%d: %s %.2f MB/s (%.1fs) %s" % (
        i + 1, "OK" if ok else "FAIL", PROBE / 1e6 / max(dt, 1e-6), dt,
        "" if ok else (err or b"").decode("utf-8", "replace").strip().splitlines()[-1][:60]),
        flush=True)
    return (i + 1, PROBE / 1e6 / max(dt, 1e-6), ok)


async def main():
    res = await asyncio.gather(*[one(i, s) for i, s in enumerate(S)])
    good = sorted([r for r in res if r[2]], key=lambda x: -x[1])
    print("  排名:", [(f"slot{r[0]}", round(r[1], 2)) for r in good], flush=True)


asyncio.run(main())
