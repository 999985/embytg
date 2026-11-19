#!/usr/bin/env python3
"""ysdq_crawl.py —— 按机器人**分类**把所有电影爬一遍，跟库内比对（只爬不转存）。

## 为什么走分类

`/fl` 回一条带内联按钮的消息，每个按钮是一个分类：
    KeyboardInlineButton('【惊悚】', callback data=b'category:<hash>')
点它 → 机器人把这条消息原地编辑成该分类第 1 页，再带「下一页」按钮。
翻页和切分类**都是 callback，不是发消息** —— 对账号几乎零消息量，
所以可以放心把 50+ 页爬完（这比按关键词搜安全得多）。

## 输出

`pipeline/.ysdq_pool.json`：
    {"ts": …, "cats": {"惊悚": {"pages": 53, "items": [{"payload": "get_x", "text": "…"}]}}}

爬完再 `--report` 跟 `resource_state` 里的电影比对，给出每个分类缺多少部。

## 用法

    python3 ysdq_crawl.py --dry                 # 只看分类清单与页数
    python3 ysdq_crawl.py --max-pages 60        # 全量爬（默认 60 页/分类）
    python3 ysdq_crawl.py --report              # 爬完做库内比对
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import ysdq_auto as A                                      # noqa: E402
import ysdq_import as Y                                    # noqa: E402

from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest  # noqa: E402

TAG = "ysdq-crawl"
POOL = "/opt/embytg/pipeline/.ysdq_pool.json"
PAGE_DELAY = 2.2


def load_pool():
    try:
        with open(POOL, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                          # noqa: BLE001
        return {"ts": 0, "cats": {}}


def save_pool(d):
    tmp = POOL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)
    os.replace(tmp, POOL)


def page_info(msg):
    """读「当前第 N 页 / 共 M 页」，返回 (N, M)。"""
    t = (msg.raw_text or "")
    m = re.search(r"当前第\s*(\d+)\s*页\s*/\s*共\s*(\d+)\s*页", t)
    if m:
        return int(m.group(1)), int(m.group(2))
    return 0, 0


def cat_buttons(msg):
    out = []
    for row in (getattr(msg, "buttons", None) or []):
        for b in row:
            if getattr(b, "data", None) and b.data.decode(errors="ignore").startswith("category:"):
                out.append((b.text.strip("【】"), b.data))
    return out


async def click(c, ent, msg_id, data):
    await c(GetBotCallbackAnswerRequest(peer=ent, msg_id=msg_id, data=data))
    await asyncio.sleep(PAGE_DELAY)
    return await c.get_messages(ent, ids=msg_id)


async def crawl(max_pages, only=""):
    env = Y.load_env()
    slot = 0
    for n in (2, 3, 4, 1):
        if GS.slot_lease(n, TAG, ttl=7200):
            slot = n
            break
    if not slot:
        print("  没有空闲 slot")
        return 2
    print("  使用 slot%d" % slot)
    c = TelegramClient(StringSession(env.get("TG_SESSION_STRING_%d" % slot) or env["TG_SESSION_STRING"]),
                       int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(Y.BOT)
    try:
        if not await A.pace_send(c, ent, slot, "/fl"):
            print("  发送受限，退出")
            return 3
        msg = None
        for _ in range(10):
            await asyncio.sleep(2)
            for m in await c.get_messages(ent, limit=3):
                if not m.out and cat_buttons(m):
                    msg = m
                    break
            if msg:
                break
        if not msg:
            print("  没拿到分类按钮")
            return 4
        cats = cat_buttons(msg)
        print("  分类 %d 个：%s" % (len(cats), ", ".join(t for t, _ in cats)))
        pool = load_pool()
        for name, data in cats:
            if only and only not in name:
                continue
            # 点分类后机器人**新发一条**结果消息（「【惊悚】资源 | 共 1054 条 / 53 页」），
            # 不是原地编辑 /fl 那条；翻页按钮 data 是 `category_result:<hash>`。
            before = (await c.get_messages(ent, limit=1))[0].id
            await c(GetBotCallbackAnswerRequest(peer=ent, msg_id=msg.id, data=data))
            await asyncio.sleep(3.0)
            cur = None
            for _ in range(10):
                for m in await c.get_messages(ent, limit=4):
                    if not m.out and m.id > before and A.links_of(m):
                        cur = m
                        break
                if cur:
                    break
                await asyncio.sleep(2)
            if not cur:
                print("  [%s] 没有结果消息" % name)
                continue
            items, pages = [], 1
            cur_page, total = page_info(cur)
            limit = min(max_pages, total or max_pages)
            print("  [%s] 共 %s 页，本轮翻 %d 页" % (name, total or "?", limit))
            stale = 0
            while True:
                items += [{"payload": p, "text": t} for p, t in A.links_of(cur).items()]
                if pages >= limit:
                    break
                nxt = A.next_button_data(cur)
                if not nxt:
                    break
                await c(GetBotCallbackAnswerRequest(peer=ent, msg_id=cur.id, data=nxt))
                # 等页码真的 +1，最多等 12s；连续两次没动就认为机器人不给了
                moved = False
                for _ in range(6):
                    await asyncio.sleep(2)
                    nxtmsg = await c.get_messages(ent, ids=cur.id)
                    if not nxtmsg:
                        break
                    np, _tp = page_info(nxtmsg)
                    if np > cur_page:
                        cur, cur_page, moved = nxtmsg, np, True
                        break
                if not moved:
                    stale += 1
                    if stale >= 2:
                        print("  [%s] 第 %d 页后不再前进，停止" % (name, cur_page))
                        break
                else:
                    stale = 0
                pages += 1
            # 去重
            uniq = {it["payload"]: it for it in items}
            pool["cats"][name] = {"pages": pages, "items": list(uniq.values())}
            pool["ts"] = int(time.time())
            save_pool(pool)
            print("  [%s] %d 页 / %d 部（累计池 %d）"
                  % (name, pages, len(uniq), sum(len(v["items"]) for v in pool["cats"].values())))
    finally:
        await c.disconnect()
        GS.slot_release(slot, TAG)
    return 0


def report():
    pool = load_pool()
    if not pool["cats"]:
        print("  还没有爬取结果")
        return 2
    have = A.library_titles()
    total = miss = 0
    rows = []
    for name, v in pool["cats"].items():
        items = v["items"]
        m = 0
        for it in items:
            t, y, _q, _f = A.parse_header(it.get("text") or "")
            if not t:
                continue
            if A.in_library(t, y, "1080p"):
                continue
            m += 1
        total += len(items)
        miss += m
        rows.append((name, v["pages"], len(items), m))
    rows.sort(key=lambda r: -r[3])
    print("  分类         页数  抓到   库里缺")
    for r in rows:
        print("  %-10s %4d %6d %7d" % r)
    print("  合计 %d 部，库里缺 %d 部（已有 %d 部）" % (total, miss, len(have)))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=60)
    ap.add_argument("--only", default="", help="只爬名字包含该词的分类")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--report", action="store_true")
    a = ap.parse_args()
    if a.report:
        return report()
    if a.dry:
        print("  dry：将爬所有分类，每类最多 %d 页，结果写 %s" % (a.max_pages, POOL))
        return 0
    return asyncio.run(crawl(a.max_pages, a.only))


if __name__ == "__main__":
    sys.exit(main())
