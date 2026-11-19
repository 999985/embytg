#!/usr/bin/env python3
"""ysdq_auto.py —— 机器人自动补片：拿「近期热门」→ 逐个转存入库（四号并行）。

上一版按 /rm 的**标题文本**解析，实测不可靠：热榜条目本身被机器人截断成
「凌晨我坐上了旺角开往…」，标题和年份都拿不全。所以改成**只用深链 payload**
（`get_3593` 这种是完整可靠的），标题/年份/画质在 `/start get_xxx` 回来的那条
文件消息里才是全的 —— 从那里解析，再交给 TMDB 补 tmdb_id。

链路：
  /rm 热榜 → 取 payload 列表
    → 每个 payload: /start get_X → 拿文件 + 完整简介（首行是「片名 画质 年份」）
    → 剔抢先版 → TMDB 补 (年份, id) → 转发到存储频道（零上行）
    → 登记 teldrive（片名 (年) {tmdb=ID} - 画质.mkv）→ 写 NFO → ScanPaths

用法：
    python3 ysdq_auto.py --dry-run --limit 10
    python3 ysdq_auto.py --limit 10
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

TAG = "ysdq-auto"


VER_WORDS = ("原声版", "原声", "普通话", "国语", "粤语", "蓝光", "4K高清", "高清", "抢先版", "枪版", "加长版", "导演剪辑版")


def norm_title(t):
    s = (t or "").strip()
    for w in VER_WORDS:
        s = s.replace(w, " ")
    s = re.sub(r"\s+", " ", s).strip(" -·")
    return s


def parse_header(desc):
    """从文件消息的简介首行取 (片名, 年份, 画质)。"""
    first = (desc or "").strip().splitlines()[0] if (desc or "").strip() else ""
    m = re.match(r"^(.*?)\s*(4K高清|高清|蓝光|抢先版|TC版|TS版)?\s*(\d{4})?年?\s*$", first)
    title = (m.group(1) if m else first).strip()
    year = int(m.group(3)) if (m and m.group(3)) else 0
    qual = "2160p" if ("4K" in first or "2160" in first) else "1080p"
    return title, year, qual, first


async def hot_payloads(slot=2, wait=18.0):
    env = Y.load_env()
    sess = env.get("TG_SESSION_STRING_%d" % slot) or env.get("TG_SESSION_STRING")
    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(Y.BOT)
    before = (await c.get_messages(ent, limit=1))[0].id
    await c.send_message(ent, "/rm")
    out, seen = [], set()
    deadline = asyncio.get_event_loop().time() + wait
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(2.5)
        for m in await c.get_messages(ent, limit=6):
            if m.out or m.id <= before or m.id in seen:
                continue
            seen.add(m.id)
            for e in (m.entities or []):
                if type(e).__name__ != "MessageEntityTextUrl":
                    continue
                u = e.url or ""
                if "start=" in u:
                    out.append(u.split("start=")[-1])
        if out:
            break
    await c.disconnect()
    return out


def library_titles():
    """库里已有电影 (片名, 年份)，用于跳过重复转存。"""
    import sqlite3
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    rows = con.execute("select rel from resource_state where kind='movie'").fetchall()
    con.close()
    out = set()
    for (rel,) in rows:
        base = re.sub(r"\s*\{tmdb=\d+\}", "", os.path.basename(rel or ""))
        base = re.sub(r"\s*-\s*(2160p|1080p|720p|SD).*$", "", base)
        m = re.match(r"^(.+?)\s*\((\d{4})\)", base)
        if m:
            out.add((norm_title(m.group(1)), int(m.group(2))))
        else:
            out.add((norm_title(os.path.splitext(base)[0]), 0))
    return out


def already(have, title, year):
    t = norm_title(title)
    for hn, hy in have:
        if year and hy and abs(hy - year) > 1:
            continue
        if not hn:
            continue
        if hn == t or (len(t) >= 2 and t in hn) or (len(hn) >= 2 and hn in t):
            return True
    return False


async def worker(slot, payloads, results, dry):
    env = Y.load_env()
    sess = env.get("TG_SESSION_STRING_%d" % slot) or (env.get("TG_SESSION_STRING") if slot == 1 else None)
    if not sess:
        return
    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(Y.BOT)
    await c.send_message(ent, "/start")
    await asyncio.sleep(2)
    while True:
        try:
            payload = payloads.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            doc, desc = await Y.fetch_item(c, payload, wait=25.0)
            if not doc:
                print("  [slot%d] %s → 机器人没回文件" % (slot, payload))
                results["miss"] += 1
                continue
            title, year, qual, first = parse_header(desc)
            if Y.is_cam(first):
                print("  [slot%d] %s → 抢先版跳过" % (slot, first[:24]))
                results["miss"] += 1
                continue
            y2, tid = Y.tmdb_lookup(norm_title(title))
            year = year or y2
            if already(library_titles(), title, year):
                print("  [slot%d] %s (%s) → 库里已有，跳过" % (slot, title[:22], year))
                results["dup"] += 1
                continue
            if not year:
                print("  [slot%d] %s → 无年份不入库" % (slot, title[:20]))
                results["miss"] += 1
                continue
            tid = tid or 0
            anchor = " {tmdb=%s}" % tid if tid else ""
            genre = ""
            gm = re.search(r"分类：([^\n|]+)", desc or "")
            if gm:
                genre = gm.group(1)
            t = Y.safe_name(norm_title(title))
            lib = Y.pick_dir(genre)
            folder = "%s/%s (%s)%s" % (lib, t, year, anchor)
            name = "%s (%s)%s - %s.mkv" % (t, year, anchor, qual)
            if dry:
                print("  [dry][slot%d] %s" % (slot, "%s/%s %.2fGB" % (folder, name,
                      (doc.document.size or 0) / 1e9)))
                results["ok"] += 1
                continue
            fwd = await c.forward_messages(Y.STORE_CH, doc.id, from_peer=ent)
            mid = fwd[0].id if isinstance(fwd, list) else fwd.id
            TC.register(folder, name, [(mid, int(doc.document.size or 0))])
            d = os.path.join(Y.MNT, folder)
            try:
                os.makedirs(d, exist_ok=True)
                pm = re.search(r"介绍：(.+)$", desc or "", re.S)
                with open(os.path.join(d, os.path.splitext(name)[0] + ".nfo"), "w",
                          encoding="utf-8") as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<movie>\n'
                            '  <title>%s</title>\n  <year>%s</year>\n  <plot>%s</plot>\n'
                            '  <genre>%s</genre>\n</movie>\n'
                            % (t, year, pm.group(1).strip()[:1500] if pm else "", genre))
            except Exception as ex:                                  # noqa: BLE001
                print("  [slot%d] NFO 失败: %s" % (slot, str(ex)[:40]))
            try:
                import emby_scan
                r = await asyncio.get_event_loop().run_in_executor(
                    None, emby_scan.scan_paths, ["/mnt/embytg/%s/%s" % (folder, name)])
                print("  [slot%d] %s %.2fGB → 入库 Created=%s" % (
                    slot, name[:40], (doc.document.size or 0) / 1e9, r.get("Created")))
            except Exception as ex:                                  # noqa: BLE001
                print("  [slot%d] 送扫失败: %s" % (slot, str(ex)[:40]))
            results["ok"] += 1
        except Exception as ex:                                      # noqa: BLE001
            print("  [slot%d] %s 异常 %s" % (slot, payload, str(ex)[:60]))
            results["fail"] += 1
    await c.disconnect()
    GS.slot_release(slot, TAG)


async def run(limit, slots, dry):
    pl = await hot_payloads()
    print("  热榜拿到 %d 个深链" % len(pl))
    if not pl:
        return 2
    have = B_existing()
    q = asyncio.Queue()
    seen = set()
    for p in pl:
        if p in seen:
            continue
        seen.add(p)
        q.put_nowait(p)
        if q.qsize() >= limit:
            break
    free = [n for n in range(1, slots + 1) if GS.slot_lease(n, TAG, ttl=3600)]
    print("  拿到租约的号: %s；本轮 %d 条" % (free or "无", q.qsize()))
    if not free:
        return 3
    results = {"ok": 0, "fail": 0, "miss": 0, "dup": 0}
    await asyncio.gather(*[worker(n, q, results, dry) for n in free])
    print("  完成 成功=%d 失败=%d 跳过=%d 库内已有=%d（库存电影 %d 部）"
          % (results["ok"], results["fail"], results["miss"], results["dup"], have))
    return 0


def B_existing():
    import sqlite3
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    n = con.execute("select count(*) from resource_state where kind='movie'").fetchone()[0]
    con.close()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--slots", type=int, default=4)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    return asyncio.run(run(a.limit, a.slots, a.dry_run))


if __name__ == "__main__":
    sys.exit(main())
