#!/usr/bin/env python3
"""repair_lowq.py —— 批量修补「假高清/低码率」剧集：从频道里找更好的版本换掉。

数据源：`.lowq_library.json`（由 daily_ops/一次性扫描生成，
字段 series/ep/mbps/mb/name，来自 Emby 的真实 RunTimeTicks 计算）。

做法：对每一部剧，用 `#剧名` 在指定频道里搜；命中集里挑体积最大的那份，
比库里现有的大 5% 以上才换（走 upgrade_episodes 的 forward → 登记 → 送扫 链路）。

每集一条消息的额度限制依旧生效（pace_send：1 条/分钟/号）——
所以这里只做「按剧批量排队」，真正的发送节奏由 ysdq_auto 的限速统一管。

用法：
    python3 repair_lowq.py --top 5 --apply
    python3 repair_lowq.py --series 纸牌屋 --apply
"""
import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgsrc_copy as TC                                    # noqa: E402
import tg_common as Y                                    # noqa: E402

from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402

TAG = "repair_lowq.py"
LOW = "/opt/embytg/pipeline/.lowq_library.json"


def load_low():
    try:
        with open(LOW, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                          # noqa: BLE001
        return []


def series_dirs():
    """从台账拿每部剧的目录（含 tmdb 锚点），用于登记。"""
    import sqlite3
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    rows = con.execute("select rel from resource_state where kind='episode' "
                       "and rel like 'TV/%'").fetchall()
    con.close()
    d = {}
    for (rel,) in rows:
        # TV/<库>/<剧名 (年) {tmdb=…}>/Season XX/<文件> —— 多季剧每季一个目录
        parts = rel.split("/")
        if len(parts) >= 5:
            key = (parts[2].split(" (")[0], int("".join(ch for ch in parts[3] if ch.isdigit()) or 1))
            d.setdefault(key, "/".join(parts[:-1]))
    return d


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", default="yingshi9999")
    ap.add_argument("--series", default="")
    ap.add_argument("--top", type=int, default=0, help="按缺口集数取前 N 部剧")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    low = load_low()
    if not low:
        print("  没有 .lowq_library.json，先跑一次扫描")
        return 2
    from collections import Counter
    cnt = Counter(x["series"] for x in low)
    todo = [a.series] if a.series else [k for k, _ in cnt.most_common(a.top or 1000)]
    dirs = series_dirs()
    print("  待修 %d 部剧，共 %d 集" % (len(todo), sum(cnt[t] for t in todo if t in cnt)))

    env = Y.load_env()
    slot = 0
    for n in (1, 2, 3, 4):
        if GS.slot_lease(n, TAG, ttl=7200):
            slot = n
            break
    if not slot:
        print("  没有空闲号")
        return 3
    c = TelegramClient(StringSession(env.get("TG_SESSION_STRING_%d" % slot) or env["TG_SESSION_STRING"]),
                       int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    try:
        ch = await c.get_entity(a.channel)
        for name in todo:
            wants = [x for x in low if x["series"] == name]
            if not wants:
                continue
            print("  [%s] 目标 %d 集" % (name, len(wants)))
            if not a.apply:
                continue
            best = {}
            async for m in c.iter_messages(ch, search="#" + name, limit=300):
                if not m.document:
                    continue
                import re as _re
                mm = _re.search(r"第\s*(\d+)\s*集", m.raw_text or "")
                if not mm:
                    continue
                ep = int(mm.group(1))
                if not any(w["ep"] == ep for w in wants):
                    continue
                if not best.get(ep) or m.document.size > best[ep]["size"]:
                    best[ep] = {"msg": m.id, "size": m.document.size,
                                "cur": next(w for w in wants if w["ep"] == ep)}
            print("     频道命中 %d 集" % len(best))
            for ep, v in sorted(best.items()):
                cur = v["cur"]
                if v["size"] <= cur["mb"] * 1e6 * 1.05:
                    continue
                import re as _re2
                sm = _re2.search(r"S(\d+)E\d+", cur["name"] or "")
                season = int(sm.group(1)) if sm else 1
                d = dirs.get((name, season))
                if not d:
                    print("     E%-3d 找不到 S%02d 目录，跳过" % (ep, season))
                    continue
                q = "2160p" if "2160" in (cur["name"] or "") else "1080p"
                fname = "%s S%02dE%02d - %s.%s" % (name, season, ep, q,
                                                   "mkv" if "mkv" in cur["name"] else "mp4")
                fwd = await c.forward_messages(Y.STORE_CH, v["msg"], from_peer=ch)
                mid = fwd[0].id if isinstance(fwd, list) else fwd.id
                try:
                    TC.register(d, fname, [(mid, v["size"])])
                    print("     E%-3d +%s %.0fMB" % (ep, fname[-18:], v["size"] / 1e6))
                except Exception as e:                         # noqa: BLE001
                    print("     E%-3d 登记失败 %s" % (ep, str(e)[:40]))
    finally:
        await c.disconnect()
        GS.slot_release(slot, TAG)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
