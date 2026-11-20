#!/usr/bin/env python3
"""upgrade_from_channels.py —— 跨频道修低码率集（按标题搜索，不用 hashtag）。

为什么不用 hashtag：中文频道里 `search="#剧名"` 命中率极低（谷雨只命中 26/30），
而 `search="剧名"` 在多个频道里都能搜到（鉴定 3/4/5 条、遮云 1/1/2 条）。

流程：多频道按标题搜 → caption 解析集号 → 每集取最大的一份 →
       与 .lowq_library.json 里的现有体积比，>1.05 倍才换 →
       forward 到存储频道（零上行）→ 登记同名 → 写 NFO → 送扫。

用法：
    python3 upgrade_from_channels.py --series 鉴定 --dry-run
    python3 upgrade_from_channels.py --series 鉴定 --apply
"""
import argparse
import asyncio
import json
import os
import re
import sys

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgsrc_copy as TC                                    # noqa: E402
import tg_common as Y                                    # noqa: E402

from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402

CHANNELS = ["yingshi9999", "yfqh06", "yingji", "DYPD_3", "tengxun4k"]
LOW = "/opt/embytg/pipeline/.lowq_library.json"


def load_low(series):
    try:
        low = json.load(open(LOW, encoding="utf-8"))
    except Exception:                                       # noqa: BLE001
        return {}
    return {int(x["ep"]): x for x in low if x.get("series") == series}


def series_dir(series):
    import sqlite3
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    rows = con.execute("select rel from resource_state where kind='episode' and rel like ?",
                       ("TV/%/%" + series + " (%",)).fetchall()
    con.close()
    if not rows:
        return ""
    rel = rows[0][0]
    return "/".join(rel.split("/")[:-1])


def real_qual(doc):
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
    ap.add_argument("--series", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-ratio", type=float, default=1.05)
    a = ap.parse_args()

    want = load_low(a.series)
    d = series_dir(a.series)
    if not want or not d:
        print("  没有低码率清单或找不到目录（series=%s dir=%s）" % (a.series, d[:50]))
        return 2
    print("  目标 %d 集（低于下限），目录 %s" % (len(want), d[-46:]))

    env = Y.load_env()
    slot = 0
    for n in (1, 2, 3, 4):
        if GS.slot_lease(n, "up-ch", ttl=1800):
            slot = n
            break
    if not slot:
        print("  没有空闲号")
        return 3
    c = TelegramClient(StringSession(env.get("TG_SESSION_STRING_%d" % slot) or env["TG_SESSION_STRING"]),
                       int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    best = {}
    try:
        for ch in CHANNELS:
            try:
                e = await c.get_entity(ch)
            except Exception:                               # noqa: BLE001
                continue
            n = 0
            async for m in c.iter_messages(e, search=a.series, limit=80):
                if not m.document:
                    continue
                cap = m.raw_text or ""
                mm = re.search(r"第\s*(\d+)\s*集", cap) or re.search(r"S\d{1,2}E(\d{1,3})", cap)
                if not mm:
                    continue
                ep = int(mm.group(1))
                if ep not in want:
                    continue
                n += 1
                cur = best.get(ep)
                if not cur or m.document.size > cur["size"]:
                    best[ep] = {"msg": m.id, "size": m.document.size, "ch": ch,
                                "qual": real_qual(m.document)}
            print("  %-12s 命中目标集 %d" % (ch, n))
        print("  合计可换 %d 集" % len(best))
        plan = []
        for ep, v in sorted(best.items()):
            cur_mb = want[ep]["mb"]
            if v["size"] > cur_mb * 1e6 * a.min_ratio:
                plan.append((ep, v, cur_mb))
                print("   E%-3d %s %6.0fMB → %6.0fMB" % (ep, v["ch"], cur_mb, v["size"] / 1e6))
        if not a.apply:
            print("  （dry-run，未执行；加 --apply）")
            return 0
        for ep, v, cur_mb in plan:
            e = await c.get_entity(v["ch"])
            fwd = await c.forward_messages(Y.STORE_CH, v["msg"], from_peer=e)
            mid = fwd[0].id if isinstance(fwd, list) else fwd.id
            name = "%s S01E%02d - %s.%s" % (a.series, ep, v["qual"], "mkv")
            try:
                TC.register(d, name, [(mid, v["size"])])
                print("   E%-3d 已登记 %s %.0fMB" % (ep, name[-20:], v["size"] / 1e6))
            except Exception as ex:                         # noqa: BLE001
                print("   E%-3d 登记失败 %s" % (ep, str(ex)[:40]))
    finally:
        await c.disconnect()
        GS.slot_release(slot, "up-ch")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
