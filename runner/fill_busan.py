"""补齐釜山行2 缺失的两段（0-511MB、511-1022MB），只用最快的 session。

存活分片（已验证落地）：
  off 1022793316 len 511MB → msg 124372
  off 1534189974 len 511MB → msg 124070
缺：
  [0, 511396658) 和 [511396658, 1022793316)
"""
import asyncio
import json
import os
import re
import sys

HERE = "/opt/embytg-runner"
SRC = os.path.join(HERE, "media.busan/釜山行2：半岛 (2020) {tmdb=581392} - 720p.mp4")
NAME = "釜山行2：半岛 (2020) {tmdb=581392} - 720p.mp4"
STEP = 128 * 1024 * 1024
SLOT = int(os.environ.get("SLOT", "3"))

env = {}
for line in open(os.path.join(HERE, "runner.env"), encoding="utf-8"):
    m = re.match(r'^([A-Z0-9_]+)=(.*)$', line.strip())
    if m:
        env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
SESS = [env.get("TG_SESSION_STRING"), env.get("TG_SESSION_STRING_2"),
        env.get("TG_SESSION_STRING_3"), env.get("TG_SESSION_STRING_4")][SLOT - 1]

MISSING = [(0, 511396658), (511396658, 511396658)]


async def main():
    total = os.path.getsize(SRC)
    todo = []
    for start, end in ((0, 511396658), (511396658, 1022793316)):
        off = start
        while off < end:
            ln = min(STEP, end - off)
            todo.append((off, ln))
            off += ln
    print("  待补 %d 片，共 %.2fGB，用 slot%d" % (len(todo), sum(l for _, l in todo) / 1e9, SLOT), flush=True)

    done = []
    for i, (off, ln) in enumerate(todo):
        e = dict(env)
        e["TG_SESSION_STRING"] = SESS
        e["TG_CHUNK_MIB"] = "1800"
        e["TG_CHUNK_DEADLINE"] = "3600"
        cmd = [sys.executable, "-u", os.path.join(HERE, "pc_worker.py"),
               "--src", SRC, "--offset", str(off), "--length", str(ln), "--name", NAME]
        p = await asyncio.create_subprocess_exec(*cmd, cwd=HERE, env=e,
                                                 stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
        out, err = await p.communicate()
        txt = (out or b"").decode("utf-8", "replace")
        mid = None
        for line in txt.splitlines():
            if line.startswith("MSG="):
                mid = int(line[4:].strip())
        print("  [%d/%d] off=%d %.0fMB → %s" % (i + 1, len(todo), off, ln / 1e6, mid or "失败"), flush=True)
        if mid:
            done.append((off, ln, mid))
        else:
            print("     err:", (err or b"").decode("utf-8", "replace").strip().splitlines()[-1][:90], flush=True)
    json.dump([{"offset": o, "size": l, "msg": m} for o, l, m in done],
              open(os.path.join(HERE, "media.busan/fill.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("  补齐完成 %d/%d" % (len(done), len(todo)), flush=True)


asyncio.run(main())
