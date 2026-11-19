#!/usr/bin/env python3
"""fill_by_ids.py —— 按**消息 ID** 从频道取指定集换进库（不依赖 hashtag 搜索）。

为什么需要：Telethon 对中文频道的 search 极不可靠 ——
`search="#谷雨"` 只命中 26 条、`search="谷雨"` 只命中 2 条，
但该剧 30 集在频道里是齐的（用户直接给出 t.me/yingshi9999/<id> 链接）。
所以改成拿到 ID 就直接按 ID 取，标题/集数从 caption 里解析并校验。

用法：
    python3 fill_by_ids.py --ch yingshi9999 --series 谷雨 \
        --dir "TV/国产剧/谷雨 (2026) {tmdb=279155}/Season 01" \
        --ids "1:2087,2:2097,3:2098" --apply --replace
"""
import argparse
import asyncio
import os
import re
import sys

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgsrc_copy as TC                                    # noqa: E402
import ysdq_import as Y                                    # noqa: E402

from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402


def real_qual(doc):
    """doc 既可能是 message（有 .document），也可能直接是 Document —— 两种都兼容。"""
    d = getattr(doc, "document", doc)
    for a in (getattr(d, "attributes", None) or []):
        if hasattr(a, "w"):
            w, h = int(getattr(a, "w", 0) or 0), int(getattr(a, "h", 0) or 0)
            if w >= 3000 or h >= 1800:
                return "2160p"
            if w >= 1700 or h >= 1000:
                return "1080p"
            return "720p"
    return "1080p"


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ch", required=True)
    ap.add_argument("--series", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--ids", required=True, help="集:消息ID,用逗号分隔，如 7:2129,8:2130")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--replace", action="store_true")
    a = ap.parse_args()

    pairs = []
    for part in a.ids.replace(" ", "").split(","):
        if ":" in part:
            ep, mid = part.split(":", 1)
            if ep.isdigit() and mid.isdigit():
                pairs.append((int(ep), int(mid)))

    env = Y.load_env()
    slot = 0
    import time as _t
    _deadline = _t.time() + int(os.environ.get("FILL_WAIT_SLOT", "900") or 0)
    while True:                                   # 号与上传 lane 共用，等上传 job 让出来
        for n in (1, 2, 3, 4):
            if GS.slot_lease(n, "fill-ids", ttl=1800):
                slot = n
                break
        if slot or _t.time() >= _deadline:
            break
        await asyncio.sleep(60)
    if not slot:
        print("  等不到空闲号")
        return 3
    c = TelegramClient(StringSession(env.get("TG_SESSION_STRING_%d" % slot) or env["TG_SESSION_STRING"]),
                       int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    try:
        ch = await c.get_entity(a.ch)
        for ep_want, mid in pairs:
            m = await c.get_messages(ch, ids=mid)
            if not m or not m.document:
                print("  E%-3d msg=%s 不是文件，跳过" % (ep_want, mid))
                continue
            cap = (m.raw_text or "")
            mm = re.search(r"第\s*(\d+)\s*集", cap)
            ep = int(mm.group(1)) if mm else ep_want
            if ep != ep_want:
                print("  E%-3d msg=%s 实际是第 %d 集，按实际处理" % (ep_want, mid, ep))
            q = real_qual(m.document)
            dur = 0
            for at in (m.document.attributes or []):
                if hasattr(at, "duration"):
                    dur = int(at.duration or 0)
                    break
            mb = (m.document.size * 8 / dur / 1e6) if dur else 0
            name = "%s S01E%02d - %s.%s" % (a.series, ep, q,
                                            "mkv" if (m.document.mime_type or "").endswith("matroska") else "mp4")
            print("   E%-3d msg=%-6s %6.0fMB %.1fMbps → %s" % (ep, mid, m.document.size / 1e6, mb, name))
            if not a.apply:
                continue
            fwd = await c.forward_messages(Y.STORE_CH, m.id, from_peer=ch)
            fmid = fwd[0].id if isinstance(fwd, list) else fwd.id
            try:
                fid = TC.register(a.dir, name, [(fmid, m.document.size)])
                print("     已登记 %s" % fid[:8])
            except Exception as e:                             # noqa: BLE001
                if "duplicate key" not in str(e):
                    raise
                print("     同名已存在（需要 --replace 且新文件更大才会换）")
                continue
            d = os.path.join(Y.MNT, a.dir)
            try:
                os.makedirs(d, exist_ok=True)
                with open(os.path.join(d, os.path.splitext(name)[0] + ".nfo"), "w",
                          encoding="utf-8") as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n'
                            '<episodedetails>\n  <title>第 %d 集</title>\n  <season>1</season>\n'
                            '  <episode>%d</episode>\n</episodedetails>\n' % (ep, ep))
            except Exception:                                  # noqa: BLE001
                pass
            try:
                import emby_scan
                r = emby_scan.scan_paths(["/mnt/embytg/%s/%s" % (a.dir, name)])
                print("     送扫 Created=%s" % r.get("Created"))
            except Exception as e:                             # noqa: BLE001
                print("     送扫失败 %s" % str(e)[:40])
    finally:
        await c.disconnect()
        GS.slot_release(slot, "fill-ids")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
