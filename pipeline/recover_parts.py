"""给「有文件行、没有分片」的条目重建 file_parts（从存储频道的消息倒推）。

成因：迁移/改名时先删了旧行（含 parts），随后登记新行失败或漏插 —— 字节还在
频道里，只是台账没有指向它们的 parts。表现为 Emby Size=0、挂载读不动。

做法：在存储频道里按文件名找候选消息，按 message_id 升序取一段连续区间，
其 size 之和等于文件声明大小，然后按顺序写回 file_parts（part_no 1..n）。

用法：
    python3 recover_parts.py --name "xxx.mkv" --size 18632192911 --apply
"""
import argparse
import asyncio
import re
import subprocess
import sys

sys.path.insert(0, "/opt/embytg/pipeline")
import tgpipe as TP                                       # noqa: E402

PSQL = ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive", "-t", "-A", "-F", "|", "-c"]
CH = -1004413117262


def env():
    d = {}
    for line in open("/opt/embytg-runner/runner.env", encoding="utf-8"):
        m = re.match(r'^([A-Z0-9_]+)=(.*)$', line.strip())
        if m:
            d[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return d


async def find(name, total):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    e = env()
    c = TelegramClient(StringSession(e["TG_SESSION_STRING"]), int(e["TG_API_ID"]), e["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(CH)
    cands = []
    async for m in c.iter_messages(ent, limit=4000):
        if m.file and (m.file.name or "") == name:
            cands.append((m.id, m.file.size))
    await c.disconnect()
    cands.sort()
    # 从最早的开始找连续区间，使 size 之和等于 total
    for i in range(len(cands)):
        s = 0
        for j in range(i, len(cands)):
            s += cands[j][1]
            if s == total:
                return cands[i:j + 1]
            if s > total:
                break
    return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--size", type=int, required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    rows = [l.split("|") for l in subprocess.run(
        PSQL + ["select id, parent_id, size::bigint from files where name='%s';" % a.name.replace("'", "''")],
        capture_output=True, text=True).stdout.strip().splitlines() if "|" in l]
    if not rows:
        print("  找不到该文件行")
        return
    fid, pid, size = rows[0]
    print("  文件行 %s 声明 %.2fGB" % (fid[:8], int(size) / 1e9))
    parts = asyncio.run(find(a.name, int(size)))
    print("  频道里找到 %d 个分段，合计 %.2fGB" % (len(parts), sum(s for _, s in parts) / 1e9))
    if not parts:
        print("  没能凑出完整序列，需人工确认")
        return
    if not a.apply:
        print("  dry run：", [p[0] for p in parts][:8])
        return
    subprocess.run(PSQL + ["delete from file_parts where file_id='%s';" % fid], capture_output=True)
    for i, (mid, sz) in enumerate(parts, 1):
        sql = ("insert into file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size) "
               "values ('%s',%d,4413117262,%d,%d,%d);" % (fid, i, mid, sz, sz))
        subprocess.run(PSQL + [sql], capture_output=True)
    r = subprocess.run(PSQL + ["select count(*) from file_parts where file_id='%s';" % fid], capture_output=True, text=True)
    print("  已写回分片:", (r.stdout or "").strip())


if __name__ == "__main__":
    main()
