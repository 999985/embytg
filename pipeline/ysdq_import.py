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
import json
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


CAM_WORDS = ("抢先版", "枪版", "抢鲜版", "TC版", "TS版", "HDTS", "HDTC", "枪鲜", "预告片", "花絮")


def is_cam(text):
    """抢先版/枪版一律不收：画质和音质都不可用，进了库等于污染画质策略。"""
    t = text or ""
    return any(w in t for w in CAM_WORDS)


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
                    "qual": qual, "raw_q": head, "payload": payload,
                    "cam": is_cam(body)})
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
                        "raw_q": head, "payload": payload, "cam": is_cam(body)})
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
    clean = [r for r in results if not r.get("cam")]
    if not clean:
        return None                      # 全是抢先版/枪版 → 当没搜到
    cands = [r for r in clean if (not year or r["year"] == year)] or clean
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

async def bot_search(c, keyword, wait=35.0, start=False):
    """发**一次**关键词，然后耐心轮询。

    机器人只是回得慢（实测 10-30s），不是丢了：重发关键词只会让它重复劳动，
    反而更慢、还刷屏。所以这里绝不重发，只把等待窗口拉长。
    """
    ent = await c.get_entity(BOT)
    if start:
        # 会话重置只做一次（每个号连上时做），不跟着每次搜索重复发
        await c.send_message(ent, "/start")
        await asyncio.sleep(3.0)
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
        # 简介是**文件消息的 caption**，不是单独一条文本消息 ——
        # 原来只在「没文件的消息」里取 txt，于是真正的标题/年份/分类全丢，
        # 表现为「无年份不入库」。这里改成任何非空文本都收（优先带附件的）。
        if e.message.raw_text and (not box.get("txt") or e.message.document):
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


def tmdb_lookup(title, kind="movie"):
    """按片名查 TMDB，返回 (年份, tmdb_id)；查不到返回 (0, 0)。

    为什么必须做：
      1) 目录/文件名不带年份时，Emby 会按名字模糊匹配到别的年份的片
         （实测「王牌 (2025)」被认成「王牌保镖 (2017)」）；
      2) 不带 `{tmdb=ID}` 锚点时，Emby 全靠猜。库里历史文件全是
         `片名 (年) {tmdb=ID} - 画质.ext`，所以新导入必须沿用同一格式。
    拿不到就返回 0 —— 调用方据此拒绝入库，宁缺毋错。
    """
    import urllib.parse
    import urllib.request
    try:
        from embytg_conf import cfg
        api = cfg("tmdb_api").rstrip("/")
        tk = cfg("tmdb_token")
    except Exception:                                          # noqa: BLE001
        return 0, 0
    q = urllib.parse.urlencode({"query": title, "language": "zh-CN", "include_adult": "false"})
    req = urllib.request.Request("%s/search/%s?%s" % (api, kind, q),
                                 headers={"Authorization": "Bearer %s" % tk,
                                          "accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
    except Exception:                                          # noqa: BLE001
        return 0, 0
    best = (0, 0)
    for it in (d.get("results") or [])[:8]:
        nm = (it.get("title") or it.get("name") or "").strip()
        if nm != title.strip():
            continue
        rd = it.get("release_date") or it.get("first_air_date") or ""
        y = int(rd[:4]) if rd[:4].isdigit() else 0
        if y and (y > best[0]):
            best = (y, int(it.get("id") or 0))
    return best


def tmdb_year(title, kind="movie"):
    return tmdb_lookup(title, kind)[0]


def safe_name(s):
    return re.sub(r'[\\/:*?"<>|]', " ", s).strip()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--search", default="")
    ap.add_argument("--payload", default="")
    ap.add_argument("--pick", type=int, default=0,
                    help="指定第几条；不填=按片名自动匹配，匹配不上就不入")
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
        res = await bot_search(c, a.search, start=True)
        if not res:
            print("  没搜到"); await c.disconnect(); return 2
        clean = [r for r in res if not r.get("cam")]
        print("  共 %d 条（剔除抢先版/枪版 %d 条），前 8 条：" % (len(clean), len(res) - len(clean)))
        for r in clean[:8]:
            print("   %-3s %-44s %s %s" % (r["idx"], r["text"][:44], r["year"], r["payload"]))
        if not clean:
            print("  全部是抢先版/枪版，不收"); await c.disconnect(); return 2
        if a.pick:
            item = next((r for r in clean if r["idx"] == a.pick), clean[0])
        else:
            item = best_match(clean, a.search, 0)          # 不指定就按片名匹配，宁缺毋错
            if not item:
                print("  没有能对上「%s」的结果，不入库" % a.search)
                await c.disconnect(); return 2
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
    _y, _tid = tmdb_lookup(title)
    if not year and _y:
        year = _y
        print("  年份由 TMDB 补全: %s (%s) id=%s" % (title, year, _tid))
    if not year:
        print("  拿不到年份，不入库（无年份必被 Emby 认错片）：%s" % title)
        await c.disconnect()
        return 4
    anchor = " {tmdb=%s}" % (_tid or "") if _tid else ""
    qual = item.get("qual") or "1080p"
    lib = a.dir or pick_dir(genre)
    folder = "%s/%s (%s)%s" % (lib, title, year, anchor)
    name = "%s (%s)%s - %s.mkv" % (title, year, anchor, qual)

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
