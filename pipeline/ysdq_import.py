#!/usr/bin/env python3
"""ysdq_import.py —— 从 @YSDQBOT 零流量转存电影入库。

## 机制（2026-11-20 实测）

机器人交互不是「发数字选片」，是**深链**：
    https://t.me/YSDQBOT?start=get_3593
对应动作：向机器人发 `/start get_3593` → 它回一条带 video 文件的消息（含分类/评分/简介）。
该消息 `protect_content=False`，可以 `forward_messages` 到自己的频道 —— **不消耗上行带宽**，
实测 2.97GB 秒到。所以整条链路是「转发 → 登记 teldrive（名字我们说了算）→ NFO → ScanPath」。

## 用法

    python3 ysdq_import.py --search "庆余年"                 # 只列结果
    python3 ysdq_import.py --search "色，戒" --pick 1 --apply # 真转存第 1 条
    python3 ysdq_import.py --payload get_3593 --apply         # 已知深链直接转
"""
import argparse
import asyncio
import os
import re
import sys

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgsrc_copy as TC                                    # noqa: E402

BOT = "@YSDQBOT"
STORE_CH = -1004413117262
MNT = "/opt/embytg/mnt/embytg"

# 机器人给的「分类」→ 库目录
GENRE_DIR = [
    ("动画", "Movies/动画片"), ("喜剧", "Movies/喜剧片"), ("爱情", "Movies/剧情片"),
    ("动作", "Movies/动作片"), ("惊悚", "Movies/惊悚片"), ("恐怖", "Movies/惊悚片"),
    ("犯罪", "Movies/犯罪片"), ("科幻", "Movies/科幻片"), ("战争", "Movies/战争片"),
    ("纪录", "Movies/纪录片"), ("剧情", "Movies/剧情片"),
]
DEFAULT_DIR = "Movies/其他片"


def load_env():
    env = {}
    for p in ("/opt/embytg-runner/runner.env", "/opt/embytg/pipeline/.env"):
        if not os.path.exists(p):
            continue
        for line in open(p, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return env


def parse_results(msg):
    """把一条搜索结果消息解析成 [{idx,text,title,year,qual,payload}]。"""
    out = []
    for ent in (msg.entities or []):
        if type(ent).__name__ != "MessageEntityTextUrl":
            continue
        text = (msg.raw_text or "")[ent.offset:ent.offset + ent.length].strip()
        m = re.match(r"^(\d+)\.\s*(.+)$", text)
        if not m:
            continue
        body = m.group(2)
        ym = re.search(r"(\d{4})年\s*$", body)
        year = int(ym.group(1)) if ym else 0
        head = body[:ym.start()].strip() if ym else body
        qual = "2160p" if ("4K" in head or "2160" in head) else ("1080p" if "蓝光" in head else "1080p")
        title = re.sub(r"\s*(4K高清|高清|蓝光|抢先版|原声|普通话|国语|粤语)\s*", " ", head).strip()
        title = re.sub(r"\s+", " ", title)
        payload = ent.url.split("start=")[-1] if "start=" in (ent.url or "") else ""
        out.append({"idx": int(m.group(1)), "text": body, "title": title, "year": year,
                    "qual": qual, "raw_q": head, "payload": payload})
    if out:
        return out
    # 机器人实际给的实体文本不含序号（"色，戒 高清 2007年"），序号在链接外面 ——
    # 此时按实体顺序编号，避免整批解析成 0 条。
    for i, ent in enumerate([e for e in (msg.entities or [])
                             if type(e).__name__ == "MessageEntityTextUrl"], start=1):
        body = ((msg.raw_text or "")[ent.offset:ent.offset + ent.length] or "").strip()
        ym = re.search(r"(\d{4})年\s*$", body)
        year = int(ym.group(1)) if ym else 0
        head = body[:ym.start()].strip() if ym else body
        qual = "2160p" if ("4K" in head or "2160" in head) else "1080p"
        title = re.sub(r"\s*(4K高清|高清|蓝光|抢先版|原声|普通话|国语|粤语)\s*", " ", head).strip()
        title = re.sub(r"\s+", " ", title)
        payload = ent.url.split("start=")[-1] if "start=" in (ent.url or "") else ""
        if payload:
            out.append({"idx": i, "text": body, "title": title, "year": year, "qual": qual,
                        "raw_q": head, "payload": payload})
    return out


def best_match(results, query, year=0):
    """在机器人返回的模糊结果里挑真正对得上的那一条；对不上就返回 None。

    机器人是关键词模糊匹配（搜「周处除三害」会回「三民窒」），直接取第 1 条会入错片。
    规则：片名相等/包含 → 直接采信；否则用相似度，且年份必须吻合（查询带年份时）。
    """
    import difflib
    if not results:
        return None
    q = query.strip()
    cands = [r for r in results if (not year or r["year"] == year)] or list(results)
    best, score = None, 0.0
    for r in cands:
        t = (r["title"] or "").strip()
        if not t:
            continue
        if t == q:
            return r
        if q and (q in t or t in q):
            s = 0.95
        else:
            s = difflib.SequenceMatcher(None, q, t).ratio()
        if year and r["year"] == year:
            s += 0.05
        if s > score:
            best, score = r, s
    return best if score >= 0.6 else None

async def bot_search(c, keyword, wait=16.0):
    """轮询最近消息（比事件handler稳：机器人有时改同一消息而不是发新消息）。"""
    ent = await c.get_entity(BOT)
    # 必须先 /start：不重置会话，机器人对同一用户的连续搜索会静默不回
    await c.send_message(ent, "/start")
    await asyncio.sleep(3.5)
    last = await c.get_messages(ent, limit=1)
    before = last[0].id if last else 0
    await c.send_message(ent, keyword)
    deadline = asyncio.get_event_loop().time() + wait
    res, seen = [], set()
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(2)
        for m in await c.get_messages(ent, limit=6):
            if m.out or m.id <= before or m.id in seen:
                continue
            seen.add(m.id)
            res += parse_results(m)
        if res:
            # 再等一轮，把分页/补充消息收全
            await asyncio.sleep(3)
            for m in await c.get_messages(ent, limit=6):
                if not m.out and m.id > before and m.id not in seen:
                    seen.add(m.id)
                    res += parse_results(m)
            break
    return res


async def fetch_item(c, payload, wait=18.0):
    """发 /start <payload>，返回带文件的消息；同时返回简介文本。"""
    from telethon import events
    box = {}

    @c.on(events.NewMessage(chats=await c.get_entity(BOT)))
    async def on(e):                                      # noqa: ANN001
        if e.out:
            return
        if e.message.document and "doc" not in box:
            box["doc"] = e.message
        elif not box.get("txt") and e.message.raw_text:
            box["txt"] = e.message.raw_text

    await c.send_message(BOT, "/start " + payload)
    deadline = asyncio.get_event_loop().time() + wait
    while asyncio.get_event_loop().time() < deadline and "doc" not in box:
        await asyncio.sleep(0.5)
    c.remove_event_handler(on)
    return box.get("doc"), box.get("txt") or ""


def pick_dir(desc):
    for kw, d in GENRE_DIR:
        if kw in desc:
            return d
    return DEFAULT_DIR


def safe_name(s):
    return re.sub(r'[\\/:*?"<>|]', " ", s).strip()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--search", default="")
    ap.add_argument("--payload", default="")
    ap.add_argument("--pick", type=int, default=1)
    ap.add_argument("--dir", default="", help="覆盖库目录，如 Movies/动作片")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    from telethon import TelegramClient
    from telethon.sessions import StringSession
    env = load_env()
    sess = env.get("TG_SESSION_STRING_2") or env["TG_SESSION_STRING"]
    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()

    item = None
    if a.payload:
        item = {"payload": a.payload, "title": a.payload, "year": 0, "qual": "1080p"}
    else:
        res = await bot_search(c, a.search)
        if not res:
            print("  没搜到"); await c.disconnect(); return 2
        print("  共 %d 条，前 8 条：" % len(res))
        for r in res[:8]:
            print("   %-3s %-44s %s %s" % (r["idx"], r["text"][:44], r["year"], r["payload"]))
        item = next((r for r in res if r["idx"] == a.pick), res[0])
        print("  选中: %s" % item["text"])

    if not a.apply:
        print("  [dry] 将转存 payload=%s" % item["payload"])
        await c.disconnect()
        return 0

    doc, desc = await fetch_item(c, item["payload"])
    if not doc:
        print("  机器人没回文件"); await c.disconnect(); return 3
    size = (doc.document.size or 0) / 1e9
    dm = re.search(r"分类：([^\n|]+)", desc or "")
    genre = dm.group(1) if dm else ""
    print("  文件 %.2fGB 分类=%s" % (size, genre))

    title = safe_name(item.get("title") or "未命名")
    year = item.get("year") or 0
    qual = item.get("qual") or "1080p"
    lib = a.dir or pick_dir(genre)
    folder = "%s/%s (%s)" % (lib, title, year) if year else "%s/%s" % (lib, title)
    name = "%s (%s) - %s.mkv" % (title, year, qual) if year else "%s - %s.mkv" % (title, qual)

    fwd = await c.forward_messages(STORE_CH, doc.id, from_peer=await c.get_entity(BOT))
    mid = fwd[0].id if isinstance(fwd, list) else fwd.id
    print("  已转存 msg=%s（零上行）" % mid)

    fid = TC.register(folder, name, [(mid, int(doc.document.size or 0))])
    print("  teldrive: %s → %s/%s" % (fid[:8], folder, name))

    # NFO（电影）
    d = os.path.join(MNT, folder)
    try:
        os.makedirs(d, exist_ok=True)
        plot = ""
        pm = re.search(r"介绍：(.+)$", desc or "", re.S)
        if pm:
            plot = pm.group(1).strip()[:1500]
        with open(os.path.join(d, os.path.splitext(name)[0] + ".nfo"), "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<movie>\n'
                    '  <title>%s</title>\n  <year>%s</year>\n  <plot>%s</plot>\n  <genre>%s</genre>\n'
                    '</movie>\n' % (title, year, plot, genre))
        print("  NFO 已写")
    except Exception as ex:
        print("  NFO 失败（不影响入库）: %s" % str(ex)[:60])

    try:
        import emby_scan
        r = emby_scan.scan_paths(["/mnt/embytg/%s/%s" % (folder, name)])
        print("  送扫: Created=%s Refreshed=%s" % (r.get("Created"), r.get("Refreshed")))
    except Exception as ex:
        print("  送扫失败: %s" % str(ex)[:60])
    await c.disconnect()
    return 0


if __name__ == "__main__":
    rc = asyncio.run(main())
    if rc == 0 and "--apply" in sys.argv:
        try:
            GS.slot_release(2, "ysdq-import")
        except Exception:
            pass
    sys.exit(rc)
