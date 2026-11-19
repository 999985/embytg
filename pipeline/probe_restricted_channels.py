#!/usr/bin/env python3
"""probe_restricted_channels.py —— 探测「禁止转发」频道到底禁到哪一步。


  1. noforwards（保护内容）→ 转发被拒，但**下载通常仍然允许**；
  2. 频道禁止保存/复制 → 同上，API 侧一般不断；
  3. 需要加入才能读（私密频道）→ 连 get_messages 都拿不到。

本脚本只做只读探测：解析实体、读 noforwards 标志、取一条媒体消息、
用 iter_download 只拉前 512KB（不落盘、不转存），然后立刻退出。
必须持 slot 租约运行，避免和上传 lane 抢同一个 TG session。
"""
import asyncio
import os
import sys

sys.path.insert(0, "/opt/embytg/pipeline")

import gha_state as GS                                    # noqa: E402
from telethon import TelegramClient                       # noqa: E402
from telethon.sessions import StringSession               # noqa: E402

ENV = "/opt/embytg-runner/runner.env"


def load_env(path=ENV):
    d = {}
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            d[k] = v
    return d


def session_for(env, slot):
    if slot == 1:
        return env["TG_SESSION_STRING"]
    return env["TG_SESSION_STRING_%d" % slot]


async def probe(client, name):
    out = {"name": name}
    try:
        ent = await client.get_entity(name)
    except Exception as e:                                # noqa: BLE001
        out["error"] = "get_entity: %s" % type(e).__name__
        return out
    out["id"] = getattr(ent, "id", None)
    out["title"] = getattr(ent, "title", None)
    out["noforwards"] = bool(getattr(ent, "noforwards", False))
    out["broadcast"] = bool(getattr(ent, "broadcast", False))
    try:
        msgs = await client.get_messages(ent, limit=30)
    except Exception as e:                                # noqa: BLE001
        out["error"] = "get_messages: %s" % type(e).__name__
        return out
    media = [m for m in msgs if getattr(m, "media", None)]
    out["recent"] = len(msgs)
    out["with_media"] = len(media)
    if not media:
        return out
    # 优先挑带 document（视频/文件）的消息；照片没法验证「能不能下载片源」
    m, doc = None, None
    for cand in media:
        d = getattr(cand, "document", None)
        if d is not None:
            m, doc = cand, d
            break
    if doc is None:
        out["media_kind"] = "只有图片，没有文件消息"
        return out
    out["msg_id"] = m.id
    out["size_mb"] = round((getattr(doc, "size", 0) or 0) / 1e6, 1)
    out["mime"] = getattr(doc, "mime_type", "")
    # 只拉 512KB 验证「能不能取字节」，不落盘
    got = 0
    try:
        async for chunk in client.iter_download(m, offset=0, request_size=256 * 1024, limit=2):
            got += len(chunk)
    except Exception as e:                                # noqa: BLE001
        out["download_error"] = "%s: %s" % (type(e).__name__, str(e)[:60])
        return out
    out["downloaded_kb"] = round(got / 1024, 0)
    out["download_ok"] = got > 0
    return out


async def main():
    slot = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    if not slot:
        print("用法: probe_restricted_channels.py <slot 1-4>")
        return 2
    rid = "probe-restricted-%d" % os.getpid()
    ok = GS.slot_lease(slot, rid, ttl=180)
    if not ok:
        print("slot %d 被占用，放弃（不许抢 session）" % slot)
        return 3
    env = load_env()
    client = TelegramClient(StringSession(session_for(env, slot)),
                            int(env["TG_API_ID"]), env["TG_API_HASH"])
    try:
        await client.connect()
        for name in TARGETS:
            r = await probe(client, name)
            print("  " + "  ".join("%s=%s" % (k, v) for k, v in r.items() if v is not None))
    finally:
        await client.disconnect()
        GS.slot_release(slot, rid)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
