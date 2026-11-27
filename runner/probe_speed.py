"""测四把 session 的当前上传吞吐：各传 3MB，谁快用谁。"""
import asyncio
import os
import re
import sys

HERE = "/opt/embytg-runner"

# E224（2026-11-27）：源文件写死成 media.busan/釜山行2 → 被清理后探针直接
# FileNotFoundError，四个会话全 FAIL，等于「测速工具自己先死了」。
# 改成自动挑一个够大的现存文件：优先 _keep（失败留存，稳定不参与新任务），
# 其次 media.slot*。不写死任何一部片名。
PROBE = 3 * 1024 * 1024


def _pick_src(need):
    cands = []
    for d in ["_keep", "media.slot1", "media.slot2", "media.slot3",
              "media.slot4", "media.slot5", "media.busan"]:
        p = os.path.join(HERE, d)
        if not os.path.isdir(p):
            continue
        for f in os.listdir(p):
            q = os.path.join(p, f)
            if not os.path.isfile(q) or f.endswith((".json", ".path")):
                continue
            try:
                sz = os.path.getsize(q)
            except OSError:
                continue
            if sz >= need:
                cands.append((0 if d == "_keep" else 1, sz, q))
    if not cands:
        return None
    cands.sort()
    return cands[0][2]

env = {}
for line in open(os.path.join(HERE, "runner.env"), encoding="utf-8"):
    m = re.match(r'^([A-Z0-9_]+)=(.*)$', line.strip())
    if m:
        env[m.group(1)] = m.group(2).strip().strip('"').strip("'")

S = [env.get("TG_SESSION_STRING"), env.get("TG_SESSION_STRING_2"),
     env.get("TG_SESSION_STRING_3"), env.get("TG_SESSION_STRING_4")]
S = [s for s in S if s]
SRC = _pick_src(PROBE * (len(S) or 1))
if not SRC:
    print("没有可用于测速的源文件（需要 >= %d MB）：先跑一轮上传或清理 _keep。"
          % (PROBE * (len(S) or 1) // 1024 // 1024), flush=True)
    sys.exit(2)
print("  源=%s 会话数=%d" % (os.path.basename(SRC), len(S)), flush=True)


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
