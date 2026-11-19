#!/usr/bin/env python3
"""ysdq_batch.py —— 四个号并行从 @YSDQBOT 转存电影（零上行，纯并发提速）。

为什么能并行：转存是 forward，不占上行带宽，瓶颈只有「机器人返回速度」和「账号数」。
每个号一条独立 session，**必须先抢到该 slot 的租约**，否则会和上传 lane 抢同一个 AuthKey
（E137：AuthKeyDuplicated 永久废号）。

用法：
    python3 ysdq_batch.py --list titles.txt --slots 4 --limit 20
    python3 ysdq_batch.py --title "碟中谍7" --title "沙丘2" --slots 2
    python3 ysdq_batch.py --list titles.txt --dry-run         # 只搜不入库

titles.txt 每行一个片名（可带年份："沙丘2 2024"）。
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

TAG = "ysdq-batch"


def existing_movies():
    """库里已有的 (片名, 年份)，用于跳过。"""
    import sqlite3
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    rows = con.execute("select rel from resource_state where rel like 'Movies/%'").fetchall()
    con.close()
    have = set()
    for (rel,) in rows:
        base = os.path.basename(rel or "")
        m = re.match(r"^(.+?)\s*\((\d{4})\)", base)
        if m:
            have.add((m.group(1).strip(), int(m.group(2))))
        else:
            have.add((os.path.splitext(base)[0].strip(), 0))
    return have


async def worker(slot, queue, env, stats, dry):
    sess = env.get("TG_SESSION_STRING_%d" % slot) or (env.get("TG_SESSION_STRING") if slot == 1 else None)
    if not sess:
        print("  [slot%d] 没有 session，退出" % slot)
        return
    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    if not await c.is_user_authorized():
        print("  [slot%d] session 未授权" % slot)
        await c.disconnect()
        GS.slot_release(slot, TAG)
        return
    ent = await c.get_entity(Y.BOT)
    while True:
        try:
            title, year = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            res = await Y.bot_search(c, title)
            if not res:
                print("  [slot%d] %s → 没搜到" % (slot, title))
                stats["miss"] += 1
                continue
            pick = Y.best_match(res, title, year)
            if not pick:
                cand = " / ".join("%s(%s)" % (r["title"][:14], r["year"]) for r in res[:4])
                print("  [slot%d] %s → 无可靠匹配，候选: %s" % (slot, title, cand))
                stats["miss"] += 1
                continue
            if dry:
                print("  [slot%d] %s → %s %s %s" % (slot, title, pick["title"], pick["year"], pick["payload"]))
                continue
            doc, desc = await Y.fetch_item(c, pick["payload"])
            if not doc:
                print("  [slot%d] %s → 机器人没回文件" % (slot, title))
                stats["fail"] += 1
                continue
            gb = (doc.document.size or 0) / 1e9
            dm = re.search(r"分类：([^\n|]+)", desc or "")
            genre = dm.group(1) if dm else ""
            t = Y.safe_name(pick["title"])
            y = pick["year"] or 0
            q = pick["qual"]
            lib = Y.pick_dir(genre)
            folder = "%s/%s (%s)" % (lib, t, y) if y else "%s/%s" % (lib, t)
            name = "%s (%s) - %s.mkv" % (t, y, q) if y else "%s - %s.mkv" % (t, q)
            fwd = await c.forward_messages(Y.STORE_CH, doc.id, from_peer=ent)
            mid = fwd[0].id if isinstance(fwd, list) else fwd.id
            TC.register(folder, name, [(mid, int(doc.document.size or 0))])
            nd = os.path.join(Y.MNT, folder)
            try:
                os.makedirs(nd, exist_ok=True)
                plot = re.search(r"介绍：(.+)$", desc or "", re.S)
                with open(os.path.join(nd, os.path.splitext(name)[0] + ".nfo"), "w",
                          encoding="utf-8") as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<movie>\n'
                            '  <title>%s</title>\n  <year>%s</year>\n  <plot>%s</plot>\n'
                            '</movie>\n' % (t, y, plot.group(1).strip()[:1500] if plot else ""))
            except Exception as ex:
                print("  [slot%d] NFO 失败: %s" % (slot, str(ex)[:40]))
            try:
                import emby_scan
                r = await asyncio.get_event_loop().run_in_executor(
                    None, emby_scan.scan_paths, ["/mnt/embytg/%s/%s" % (folder, name)])
                print("  [slot%d] %s %.2fGB → 入库 Created=%s" % (slot, name[:34], gb, r.get("Created")))
            except Exception as ex:
                print("  [slot%d] 送扫失败: %s" % (slot, str(ex)[:40]))
            stats["ok"] += 1
        except Exception as ex:
            print("  [slot%d] %s → 异常 %s" % (slot, title, str(ex)[:70]))
            stats["fail"] += 1
    await c.disconnect()
    GS.slot_release(slot, TAG)


async def run(titles, slots, dry):
    env = Y.load_env()
    have = existing_movies()
    q = asyncio.Queue()
    skipped = []
    for t in titles:
        m = re.match(r"^(.*?)\s+(\d{4})$", t.strip())
        name, year = (m.group(1), int(m.group(2))) if m else (t.strip(), 0)
        if not name:
            continue
        dup = any((name == h[0] and (not year or year == h[1] or h[1] == 0)) for h in have)
        if dup:
            skipped.append(name)
            continue
        q.put_nowait((name, year))
    print("  待处理 %d 条，已跳过 %d 条（库里已有）" % (q.qsize(), len(skipped)))
    if skipped[:5]:
        print("   跳过示例: %s" % " / ".join(skipped[:5]))

    free = []
    for n in range(1, slots + 1):
        if GS.slot_lease(n, TAG, ttl=3600):
            free.append(n)
    print("  拿到租约的号: %s" % (free or "无"))
    if not free:
        print("  四个号都被上传占用，稍后再跑")
        return 3
    stats = {"ok": 0, "fail": 0, "miss": 0}
    await asyncio.gather(*[worker(n, q, env, stats, dry) for n in free])
    print("  完成 成功=%d 失败=%d 没搜到=%d" % (stats["ok"], stats["fail"], stats["miss"]))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", action="append", default=[])
    ap.add_argument("--list", default="")
    ap.add_argument("--slots", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    titles = list(a.title)
    if a.list:
        titles += [x.strip() for x in open(a.list, encoding="utf-8") if x.strip()
                   and not x.startswith("#")]
    if a.limit:
        titles = titles[: a.limit]
    if not titles:
        print("  没有片名"); return 2
    return asyncio.run(run(titles, a.slots, a.dry_run))


if __name__ == "__main__":
    sys.exit(main())
