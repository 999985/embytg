#!/usr/bin/env python3
"""tgsrc_copy.py —— 从「禁止转发」的频道把片源搬到自己的存储频道。

## 为什么需要它（2026-11-18 实测）


    noforwards=True     但成员可以 get_messages
    也能 iter_download  实测两个频道各拉 512KB 成功（2.4GB / 3.9GB 的文件）

也就是说 **禁的是「转发」这个动作，不是「读取字节」**。所以办法是：
下载 → 用 pc_upload 重新上传到存储频道 → 按 teldrive 的 file_parts 登记。

代价：这条路**不为零流量**。转发（tgpipe）是服务器端引用，不产生出站；
复制要完整下载（入站，免费）+ 完整上传（出站，计 Oracle 10TB 额度）。

## 用法

        --dir "电影/测试" --name "测试.mp4" --dry     # 只看，不动
        --dir "电影/测试" --name "测试.mp4"           # 真搬
"""
import argparse
import asyncio
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgpipe as TP                                        # noqa: E402

RUNNER = "/opt/embytg-runner"
ENV = os.path.join(RUNNER, "runner.env")


def load_env(path=ENV):
    d = {}
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            d[k] = v
    return d


def session_for(env, slot):
    return env["TG_SESSION_STRING"] if slot == 1 else env["TG_SESSION_STRING_%d" % slot]


async def fetch_one(env, slot, channel, msg_id, dest):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    client = TelegramClient(StringSession(session_for(env, slot)),
                            int(env["TG_API_ID"]), env["TG_API_HASH"])
    await client.connect()
    try:
        ent = await client.get_entity(channel)
        m = await client.get_messages(ent, ids=msg_id)
        if not m or not m.media:
            raise SystemExit("消息 %s 没有媒体" % msg_id)
        doc = m.document
        size = getattr(doc, "size", 0) or 0
        print("  源文件 %s  %.2f GB" % (msg_id, size / 1e9), flush=True)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        done = 0
        t0 = time.time()
        with open(dest, "wb") as f:
            async for chunk in client.iter_download(m, request_size=1024 * 1024):
                f.write(chunk)
                done += len(chunk)
                if done % (256 * 1024 * 1024) < 1024 * 1024:
                    el = max(time.time() - t0, 0.01)
                    print("    %.2f/%.2f GB  %.1f MB/s" % (done / 1e9, size / 1e9, done / el / 1e6),
                          flush=True)
        el = time.time() - t0
        print("  下载完成 %.2f GB  %.1f MB/s" % (done / 1e9, done / el / 1e6), flush=True)
        return dest
    finally:
        await client.disconnect()


def upload(env, slot, path):
    cmd = [os.path.join(RUNNER, "venv/bin/python"), os.path.join(RUNNER, "pc_upload.py"),
           path, "--session-slot", str(slot), "--store-ch", env["TG_STORE_CH"].split(",")[0]]
    env2 = dict(os.environ)
    env2.update({k: v for k, v in env.items()})
    print("  上传: %s" % " ".join(cmd[1:]), flush=True)
    # 2026-11-19：原来没有 timeout、且 capture_output 把进度全吞进内存 ——
    # 2.09GB 那单卡了 40 分钟，日志里既没进度也没报错，无从判断是慢还是死。
    # 现在按体积给预算（下限 20 分钟，按 8MB/s 估），超时就杀并打印尾部。
    try:
        _sz = os.path.getsize(path)
    except OSError:
        _sz = 0
    _budget = max(1200, int(_sz / (8 * 1e6)) + 600)
    try:
        r = subprocess.run(cmd, env=env2, capture_output=True, text=True, timeout=_budget)
    except subprocess.TimeoutExpired as _te:
        _tail = ((_te.stdout or b"") if isinstance(_te.stdout, bytes) else (_te.stdout or ""))
        if isinstance(_tail, bytes):
            _tail = _tail.decode("utf-8", "ignore")
        print("  上传超时（%ds，%.2fGB）：%s" % (_budget, _sz / 1e9, str(_tail)[-300:]), flush=True)
        raise SystemExit("上传超时")
    out = (r.stdout or "") + (r.stderr or "")
    mani = ""
    for line in out.splitlines():
        if line.startswith("MANIFEST="):
            mani = line.split("=", 1)[1].strip()
    if r.returncode != 0 or not mani:
        print(out[-800:], flush=True)
        raise SystemExit("上传失败 rc=%s" % r.returncode)
    j = json.load(open(mani, encoding="utf-8"))
    # parts 是 **list**（每项 part_no/message_id/channel_id/plain_size），按 part_no 排序
    raw = j["parts"]
    if isinstance(raw, dict):
        raw = list(raw.values())
    raw = sorted(raw, key=lambda x: int(x.get("part_no") or 0))
    return [(int(p["message_id"]), int(p["plain_size"])) for p in raw]


def register(dir_path, name, parts):
    parent = None
    for seg in dir_path.strip("/").split("/"):
        parent = TP.ensure_folder(seg, parent)
    fid, total = TP.register(parent, name, parts)
    print("  已登记 teldrive: %s  %s  %.2f GB" % (fid[:8], name, total / 1e9), flush=True)
    return fid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--msg", type=int, required=True)
    ap.add_argument("--dir", required=True, help="teldrive 目标目录，如 电影/测试")
    ap.add_argument("--name", required=True)
    ap.add_argument("--slot", type=int, default=0)
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    env = load_env()
    slot = a.slot
    if not slot:
        for n in (1, 2, 3, 4):
            if GS.slot_lease(n, "tgsrc-%d" % os.getpid(), ttl=7200):
                slot = n
                break
        if not slot:
            raise SystemExit("没有空闲 slot")
    else:
        if not GS.slot_lease(slot, "tgsrc-%d" % os.getpid(), ttl=7200):
            raise SystemExit("slot %d 被占用" % slot)
    rid = "tgsrc-%d" % os.getpid()
    try:
        if a.dry:
            print("  DRY: 会用 slot%d 从 @%s 拉 msg=%d，目标 %s/%s"
                  % (slot, a.channel, a.msg, a.dir, a.name))
            return 0
        dest = os.path.join(RUNNER, "media.slot%d" % slot, a.name)
        fetch_one_sync = asyncio.run(fetch_one(env, slot, a.channel, a.msg, dest))
        parts = upload(env, slot, fetch_one_sync)
        register(a.dir, a.name, parts)
        try:
            os.remove(fetch_one_sync)
        except OSError:
            pass
        return 0
    finally:
        GS.slot_release(slot, rid)


if __name__ == "__main__":
    sys.exit(main())
