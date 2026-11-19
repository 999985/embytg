#!/usr/bin/env python3
"""ysdq_auto.py —— 机器人补片：**内联按钮翻页**（正路）+ 逐条转存（零上行）。

## 机器人交互的真实结构（2026-11-20 实测，别再猜）

1. 搜关键词 → 一条结果消息，**原地编辑**成 15 条/页，带内联按钮：
      KeyboardInlineButton(text='下一页', type=InlineButtonTypeCallback,
                           data=b'search:<hash>:2')
2. 翻页必须**点按钮/回 callback**，发文本「下一页」没用（那只是又搜了「下一页」）。
   实测 callback 第 2/3/4/5 页各 15 条全新链接，共 46 页 / 677 条。
3. 每条结果 = 深链 `?start=get_<id>`；`/start get_<id>` 才拿到文件消息（标题在 caption）。
4. `/fl` 只有一句「请选择类型」，无列表无按钮 —— 此路不通。
5. `/rm` 热榜 15 条，当「今天补什么」的兜底。

## 设计要点

* 页码断点存 `.ysdq_seen.json`：{keyword: {hash, pages:[...], payloads:[...]}}。
  一趟跑不完 46 页，下次从断点继续；已抓过的 payload 不再重复抓。
* 三段去重：① 归一化片名+年份 vs 库内电影（4K 可覆盖 1080p，反向跳过）；
  ② teldrive 同名文件已存在；③ seen 里的 payload。
* 注册撞 unique 约束 = 文件已在 teldrive → 按「重复」跳过，不算失败。
* 限速：翻页 2.5s、每条详情 0.8s，单号串行。

## 用法

    python3 ysdq_auto.py --keyword 4K --pages 2 --per-page 3 --dry-run
    python3 ysdq_auto.py --keyword 4K --pages 10 --per-page 2 --slots 4
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
import tgsrc_copy as TC                                    # noqa: E402
import ysdq_import as Y                                    # noqa: E402

from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest  # noqa: E402

TAG = "ysdq-auto"
_slot_of = {"n": 1}
SEEN = "/opt/embytg/pipeline/.ysdq_seen.json"
PAGE_DELAY = 2.5
ITEM_DELAY = 0.8
_LIB = {}


def load_seen():
    try:
        with open(SEEN, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                          # noqa: BLE001
        return {}


def save_seen(d):
    tmp = SEEN + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    os.replace(tmp, SEEN)


def library_titles():
    """库内电影 [(归一化片名, 年份, 画质)]，进程内只算一次。"""
    if _LIB.get("v"):
        return _LIB["v"]
    import sqlite3
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    rows = con.execute("select rel from resource_state where kind='movie'").fetchall()
    con.close()
    out = []
    for (rel,) in rows:
        rel = rel or ""
        base = re.sub(r"\s*\{tmdb=\d+\}", "", os.path.basename(rel))
        base = re.sub(r"\s*-\s*(2160p|1080p|720p|SD).*$", "", base)
        m = re.match(r"^(.+?)\s*\((\d{4})\)", base)
        name = Y.norm_key(m.group(1)) if m else Y.norm_key(os.path.splitext(base)[0])
        year = int(m.group(2)) if m else 0
        out.append((name, year, "2160p" if "2160p" in rel else "1080p"))
    _LIB["v"] = out
    return out


def in_library(title, year, qual):
    """同名同年份已在库 → 除非这次是 4K 而库里只有 1080p，否则跳过。"""
    k = Y.norm_key(title)
    hit = []
    for hn, hy, q in library_titles():
        if year and hy and abs(hy - year) > 1:
            continue
        if not hn:
            continue
        if hn == k or (len(k) >= 2 and k in hn) or (len(hn) >= 2 and hn in k):
            hit.append(q)
    if not hit:
        return False
    if qual == "2160p" and "2160p" not in hit:
        return False
    return True


def links_of(msg):
    out = {}
    for e in (msg.entities or []):
        if type(e).__name__ != "MessageEntityTextUrl":
            continue
        u = e.url or ""
        if "start=" in u:
            out[u.split("start=")[-1]] = (msg.raw_text or "")[e.offset:e.offset + e.length]
    return out


PACE = "/opt/embytg/pipeline/.ysdq_pace.json"
MIN_GAP = 60          # 同一账号两次发消息的最小间隔（秒）
# 用户口径（2026-11-19）：限流是「发太快」触发的，不是小时总量。
# 1 分钟一条稳定发，小时量可以很高；所以间隔是主约束，小时上限只做兜底。
HOUR_CAP = 60         # 与 60s 间隔一致：1 条/分钟/号 → 4 号 = 240 条/小时


def _pace_load():
    try:
        with open(PACE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                          # noqa: BLE001
        return {}


def _pace_save(d):
    tmp = PACE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f)
    os.replace(tmp, PACE)


STARTED = "/opt/embytg/pipeline/.ysdq_started.json"


def _load_started():
    try:
        with open(STARTED, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                          # noqa: BLE001
        return {}


def _save_started(d):
    tmp = STARTED + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f)
    os.replace(tmp, STARTED)


async def pace_send(c, ent, slot, text):
    """限速发消息：同号 25s 间隔 + 尊重 FloodWait 冷却。

    2026-11-20 实测教训：单号一小时内发了十几次搜索，Telegram 直接
    FloodWaitError 1996 秒，systemd 里表现为服务 exit 1，而且接下来 33 分钟
    这个号对任何机器人都发不出消息。
    """
    from telethon.errors.rpcerrorlist import FloodWaitError
    d = _pace_load()
    now = time.time()
    cur = d.get(str(slot)) or {}
    nxt = float(cur.get("next_at") or 0)
    if nxt > now:
        print("  [slot%d] 冷却中，还需 %.0f 秒，跳过发送" % (slot, nxt - now))
        return False
    hist = [t for t in (cur.get("hist") or []) if now - float(t) < 3600]
    if len(hist) >= HOUR_CAP:
        print("  [slot%d] 本小时已发 %d 条（上限 %d），本轮收手" % (slot, len(hist), HOUR_CAP))
        return False
    last_at = float(cur.get("last_at") or 0)
    if now - last_at < MIN_GAP:
        await asyncio.sleep(MIN_GAP - (now - last_at))
    try:
        await c.send_message(ent, text)
    except FloodWaitError as e:
        d[str(slot)] = {"next_at": time.time() + int(e.seconds) + 5, "last_at": time.time(),
                        "hist": hist + [time.time()]}
        _pace_save(d)
        print("  [slot%d] FloodWait %d 秒，本轮就此收手（下次定时再跑）" % (slot, e.seconds))
        return False
    d[str(slot)] = {"last_at": time.time(), "next_at": 0, "hist": hist + [time.time()]}
    _pace_save(d)
    return True


async def open_search(c, ent, keyword, slot=1):
    """发关键词，拿到结果消息与翻页 hash。"""
    before = (await c.get_messages(ent, limit=1))[0].id
    if not await pace_send(c, ent, slot, keyword):
        return None, ""
    for _ in range(12):
        await asyncio.sleep(2.0)
        for m in await c.get_messages(ent, limit=3):
            if m.out or m.id <= before or not m.buttons:
                continue
            h = ""
            for row in m.buttons:
                for b in row:
                    if b.text == "下一页" and b.data and b.data.decode().startswith("search:"):
                        h = b.data.decode().split(":")[1]
            if h:
                return m, h
    return None, ""


def next_button_data(msg):
    """从**当前**结果消息里读「下一页」的 callback data。

    不能自己拼 `search:<hash>:N` —— 实测拼出来回过去报
    `DataInvalidError: Encrypted data invalid`（data 是随页变化且服务端校验的）。
    """
    for row in (getattr(msg, "buttons", None) or []):
        for b in row:
            if b.text == "下一页" and b.data:
                return b.data
    return None


async def click_next(c, ent, msg):
    """点当前消息的「下一页」，返回编辑后的消息；没有下一页返回 None。"""
    data = next_button_data(msg)
    if not data:
        return None
    await c(GetBotCallbackAnswerRequest(peer=ent, msg_id=msg.id, data=data))
    await asyncio.sleep(PAGE_DELAY)
    return await c.get_messages(ent, ids=msg.id)


def real_qual(doc, fallback="1080p"):
    """用文件自带的视频属性判真实画质 —— 机器人文字标签不可信。

    实测：标「4K高清」的 6 部里，实际是 1920x1080 / 2560x1080 / 1920x800。
    DocumentAttributeVideo 里带 w/h，按宽度优先（宽>=3000 才 2160p）判定，
    与 tgpipe.parse_quality 同一口径。
    """
    try:
        for a in (getattr(doc, "document", None).attributes or []):
            if hasattr(a, "w") and hasattr(a, "h"):
                w, h = int(getattr(a, "w", 0) or 0), int(getattr(a, "h", 0) or 0)
                if w >= 3000 or h >= 1800:
                    return "2160p"
                if w >= 1700 or h >= 1000:
                    return "1080p"
                if w >= 1200 or h >= 700:
                    return "720p"
                return "SD"
    except Exception:                                          # noqa: BLE001
        pass
    return fallback


def clean_title(t):
    """落库用的干净片名：去版本词、去零宽字符、收空格。"""
    s = (t or "")
    s = s.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "").replace("\ufeff", "")
    for w in ("4K高清", "高清", "蓝光", "原声版", "原声", "普通话", "国语", "粤语",
              "抢先版", "枪版", "TC版", "TS版"):
        s = s.replace(w, " ")
    return re.sub(r"\s+", " ", s).strip(" -·|")


def parse_header(desc):
    first = (desc or "").strip().splitlines()[0] if (desc or "").strip() else ""
    m = re.match(r"^(.*?)\s*(4K高清|高清|蓝光|抢先版|TC版|TS版)?\s*(\d{4})?年?\s*$", first)
    title = (m.group(1) if m else first).strip()
    # 画质词已经从 qual 拿到了，别留在片名里（否则出现「邻人可疑 4K高清 下集」）
    title = re.sub(r"\s*(4K高清|高清|蓝光|抢先版|TC版|TS版)\s*", " ", title).strip()
    year = int(m.group(3)) if (m and m.group(3)) else 0
    qual = "2160p" if ("4K" in first or "2160" in first) else "1080p"
    return title, year, qual, first


def register_safe(folder, name, parts):
    try:
        return TC.register(folder, name, parts)
    except Exception as e:                                     # noqa: BLE001
        if "duplicate key" in str(e) or "unique constraint" in str(e):
            return None
        raise


async def scan_pages(slot, keyword, pages, q, results):
    """生产者：**一个号**打开一次搜索，逐页把深链塞进共享队列。

    只占 1 个号，且每页只发 1 次消息（翻页走 callback），消息量最小。
    """
    env = Y.load_env()
    sess = env.get("TG_SESSION_STRING_%d" % slot) or (env.get("TG_SESSION_STRING") if slot == 1 else None)
    if not sess:
        return
    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(Y.BOT)
    try:
        msg, h = await open_search(c, ent, keyword, slot)
        if not msg:
            print("  [scan slot%d] %s → 没拿到结果页" % (slot, keyword))
            results["miss"] += 1
            return
        seen = load_seen().setdefault(keyword, {"hash": h, "pages": [], "payloads": []})
        seen["hash"] = h
        known = set(seen["payloads"])
        cur_page = 1
        for p in pages:
            while cur_page < p:
                nxt = await click_next(c, ent, msg)
                if not nxt:
                    print("  [scan slot%d] 第 %d 页没有「下一页」了，停止" % (slot, cur_page))
                    break
                msg = nxt
                cur_page += 1
            if cur_page < p:
                break
            links = links_of(msg)
            fresh = [x for x in links if x not in known]
            for x in fresh:
                await q.put(x)
            if p not in seen["pages"]:
                seen["pages"].append(p)
            save_seen(load_seen() | {keyword: seen})
            print("  [scan slot%d] %s p%d → %d 条（新 %d）入队"
                  % (slot, keyword, p, len(links), len(fresh)))
    finally:
        await c.disconnect()


async def item_worker(slot, q, results, dry, cap):
    """消费者：四个号从**同一个队列**取活，天然负载均衡。"""
    env = Y.load_env()
    sess = env.get("TG_SESSION_STRING_%d" % slot) or (env.get("TG_SESSION_STRING") if slot == 1 else None)
    if not sess:
        return
    c = TelegramClient(StringSession(sess), int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    ent = await c.get_entity(Y.BOT)
    _slot_of["n"] = slot
    st = _load_started()
    if not st.get(str(slot)):
        if not await pace_send(c, ent, slot, "/start"):
            await c.disconnect()
            GS.slot_release(slot, TAG)
            return
        st[str(slot)] = int(time.time())
        _save_started(st)
    while True:
        if results["ok"] >= cap:
            break
        try:
            payload = q.get_nowait()
        except asyncio.QueueEmpty:
            # 扫描器可能还在翻页：等一会儿再取，不要立刻退出（否则整轮空跑）
            if results.get("scan_done"):
                break
            await asyncio.sleep(1.5)
            continue
        try:
            try:
                doc, desc = await Y.fetch_item(
                    c, payload, wait=25.0,
                    send=lambda txt: pace_send(c, ent, slot, txt))
            except Exception as _ex:                           # noqa: BLE001
                if "FloodWait" in type(_ex).__name__ or "A wait of" in str(_ex):
                    _secs = 0
                    try:
                        import re as _re
                        _secs = int(_re.search(r"wait of (\d+) seconds", str(_ex)).group(1))
                    except Exception:                          # noqa: BLE001
                        _secs = 3600
                    _d = _pace_load()
                    _d[str(slot)] = {"last_at": time.time(), "next_at": time.time() + _secs + 10,
                                     "hist": (_d.get(str(slot), {}).get("hist") or [])}
                    _pace_save(_d)
                    print("  [slot%d] FloodWait %d 秒 → 本号退出本轮" % (slot, _secs))
                    break
                raise
            await asyncio.sleep(ITEM_DELAY)
            if not doc:
                # 发送被限速/冷却挡下时不要继续空转，直接退出本轮该号
                _p = _pace_load().get(str(slot)) or {}
                if float(_p.get("next_at") or 0) > time.time():
                    print("  [slot%d] 发送受限，本号退出本轮" % slot)
                    break
                results["miss"] += 1
                continue
            title, year, qual_label, first = parse_header(desc)
            qual = real_qual(doc, qual_label)          # 以文件属性为准，不信文字标签
            if results.get("only_4k") and qual != "2160p":
                results["miss"] += 1
                continue                               # 只要 2160p，实际不是就跳过
            if Y.is_cam(first):
                results["cam"] += 1
                continue
            y2, tid = Y.tmdb_lookup(title)
            year = year or y2
            if not year:
                results["miss"] += 1
                continue
            if in_library(title, year, qual):
                results["dup"] += 1
                continue
            anchor = " {tmdb=%s}" % tid if tid else ""
            t = Y.safe_name(clean_title(title))
            gm = re.search(r"分类：([^\n|]+)", desc or "")
            lib = Y.pick_dir(gm.group(1) if gm else "")
            folder = "%s/%s (%s)%s" % (lib, t, year, anchor)
            name = "%s (%s)%s - %s.mkv" % (t, year, anchor, qual)
            # 画质闸门：分辨率达标但码率过低的不入（谷雨 2160p/400MB 事故）
            _dur = 0
            try:
                for _a in (doc.document.attributes or []):
                    if hasattr(_a, "duration"):
                        _dur = int(getattr(_a, "duration", 0) or 0)
                        break
            except Exception:                                  # noqa: BLE001
                pass
            try:
                import quality_gate as QG
                _bad, _m = QG.too_low(int(doc.document.size or 0), _dur, qual)
                if _bad:
                    QG.log_reject("ysdq", name, int(doc.document.size or 0), _dur, qual, _m)
                    print("  [slot%d] %s → 码率 %.1fMbps 过低，跳过" % (slot, name[:40], _m))
                    results["lowq"] = results.get("lowq", 0) + 1
                    continue
            except Exception:                                  # noqa: BLE001
                pass
            gb = (doc.document.size or 0) / 1e9
            if dry:
                print("  [dry][slot%d] %s  %.2fGB" % (slot, name[:54], gb))
                results["ok"] += 1
                continue
            fwd = await c.forward_messages(Y.STORE_CH, doc.id, from_peer=ent)
            mid = fwd[0].id if isinstance(fwd, list) else fwd.id
            fid = register_safe(folder, name, [(mid, int(doc.document.size or 0))])
            if not fid:
                print("  [slot%d] %s → teldrive 已存在，跳过" % (slot, name[:44]))
                results["dup"] += 1
                continue
            d = os.path.join(Y.MNT, folder)
            try:
                os.makedirs(d, exist_ok=True)
                pm = re.search(r"介绍：(.+)$", desc or "", re.S)
                with open(os.path.join(d, os.path.splitext(name)[0] + ".nfo"), "w",
                          encoding="utf-8") as f:
                    f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n'
                            '<movie>\n  <title>%s</title>\n  <year>%s</year>\n'
                            '  <plot>%s</plot>\n</movie>\n'
                            % (t, year, pm.group(1).strip()[:1500] if pm else ""))
            except Exception as ex:                            # noqa: BLE001
                print("  [slot%d] NFO 失败: %s" % (slot, str(ex)[:40]))
            try:
                import emby_scan
                r = await asyncio.get_event_loop().run_in_executor(
                    None, emby_scan.scan_paths, ["/mnt/embytg/%s/%s" % (folder, name)])
                print("  [slot%d] %s %.2fGB → 入库 Created=%s"
                      % (slot, name[:46], gb, r.get("Created")))
            except Exception as ex:                            # noqa: BLE001
                print("  [slot%d] 送扫失败: %s" % (slot, str(ex)[:40]))
            results["ok"] += 1
            results["by_slot"][slot] = results["by_slot"].get(slot, 0) + 1
        except Exception as ex:                                # noqa: BLE001
            print("  [slot%d] %s 异常 %s" % (slot, payload, str(ex)[:70]))
            results["fail"] += 1
    await c.disconnect()
    GS.slot_release(slot, TAG)


POOL = "/opt/embytg/pipeline/.ysdq_pool.json"


def pool_payloads(only_4k=False):
    """从分类爬取池里挑出库里没有的片（payload 列表）。

    池是 ysdq_crawl.py 按 28 个分类爬的（实测 8010 条 / 3499 部唯一）。
    这里只做「缺哪些」的判断，真正取片仍走 worker（会再核一次库）。
    """
    try:
        with open(POOL, encoding="utf-8") as f:
            pool = json.load(f)
    except Exception:                                          # noqa: BLE001
        return []
    out = []
    for cat, v in (pool.get("cats") or {}).items():
        for it in v.get("items") or []:
            t, y, q, first = parse_header(it.get("text") or "")
            if not t:
                continue
            if only_4k and "4K" not in (first or ""):
                continue
            if in_library(t, y, real_hint(first)):
                continue
            out.append(it["payload"])
    return out


def real_hint(first):
    return "2160p" if ("4K" in (first or "") or "2160" in (first or "")) else "1080p"


async def run(keyword, pages, slots, dry, cap, payloads=None, only_4k=False):
    # 号是上传 lane 与机器人补片**共用**的排他资源：上传 job 期间拿不到。
    # --wait-slot 给一个等待窗口，等上传 job 结束再抢（否则一到点就空跑一轮）。
    import os as _os
    _wait = int(_os.environ.get("YSDQ_WAIT_SLOT", "0") or 0)
    _deadline = time.time() + _wait
    leased = []
    while True:
        leased = [n for n in range(1, slots + 1) if GS.slot_lease(n, TAG, ttl=3600)]
        if leased or time.time() >= _deadline:
            break
        await asyncio.sleep(60)
    # 冷却中的号（FloodWait）不能当扫描器，否则整轮空转
    pace = _pace_load()
    now = time.time()
    free = [n for n in leased if float((pace.get(str(n)) or {}).get("next_at") or 0) <= now]
    for n in leased:
        if n not in free:
            GS.slot_release(n, TAG)
    print("  可用号 %s（冷落 %s）；关键词 %s 翻页 %s；本轮上限 %d 部"
          % (free or "无", [n for n in leased if n not in free], keyword, pages, cap))
    if not free:
        print("  所有号都在 FloodWait 冷却中，本轮跳过")
        return 0
    results = {"ok": 0, "fail": 0, "miss": 0, "dup": 0, "cam": 0, "by_slot": {},
               "scan_done": True, "only_4k": only_4k}
    q = asyncio.Queue()
    if payloads is not None:
        for pl in payloads:
            q.put_nowait(pl)
        print("  池模式：队列 %d 条（不需扫描，全部号做消费者）" % q.qsize())
        await asyncio.gather(*[item_worker(n, q, results, dry, cap) for n in free])
        print("  完成 入库=%d 重复=%d 抢先版=%d 低码率=%d 未取到=%d 失败=%d"
              % (results["ok"], results["dup"], results["cam"], results.get("lowq", 0),
                 results["miss"], results["fail"]))
        if results["by_slot"]:
            print("  各号产出: %s" % ", ".join("slot%d=%d" % (k, v)
                                            for k, v in sorted(results["by_slot"].items())))
        return 0
    scanner = free[0]
    consumers = [n for n in free if n != scanner]
    if not consumers:
        # 只有一个号：**串行**跑（扫描完再消费）。
        # 同一 session 同时开两个 Telethon 客户端会撞 AuthKeyDuplicated，永久废号。
        await scan_pages(scanner, keyword, pages, q, results)
        results["scan_done"] = True
        await item_worker(scanner, q, results, dry, cap)
    else:
        t = asyncio.create_task(scan_pages(scanner, keyword, pages, q, results))
        cs = [asyncio.create_task(item_worker(n, q, results, dry, cap)) for n in consumers]
        await t
        results["scan_done"] = True
        await asyncio.gather(*cs)
        GS.slot_release(scanner, TAG)
    print("  完成 入库=%d 重复=%d 抢先版=%d 低码率=%d 未取到=%d 失败=%d"
          % (results["ok"], results["dup"], results["cam"], results.get("lowq", 0),
             results["miss"], results["fail"]))
    if results["by_slot"]:
        print("  各号产出: %s" % ", ".join("slot%d=%d" % (k, v)
                                        for k, v in sorted(results["by_slot"].items())))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keyword", default="4K")
    ap.add_argument("--pages", type=int, default=2)
    ap.add_argument("--start-page", type=int, default=0, help="0=从断点继续")
    ap.add_argument("--per-page", type=int, default=0, help="(弃用) 见 --limit")
    ap.add_argument("--limit", type=int, default=0, help="本轮最多入库几部，0=号数×3")
    ap.add_argument("--slots", type=int, default=4)
    ap.add_argument("--from-pool", action="store_true", help="从分类爬取池里补缺")
    ap.add_argument("--4k-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    seen = (load_seen().get(a.keyword) or {})
    start = a.start_page or max([2] + [p + 1 for p in (seen.get("pages") or [])])
    pages = list(range(start, start + a.pages))
    print("  关键词 %s：断点页 %d → 本轮 %s" % (a.keyword, start, pages))
    cap = a.limit or max(2, a.slots * 3)
    if a.from_pool:
        pl = pool_payloads(a.__dict__.get("4k_only", False))
        print("  池内缺片 %d 个 payload，本轮上限 %d" % (len(pl), cap))
        if not pl:
            return 0
        return asyncio.run(run(a.keyword, pages, a.slots, a.dry_run, cap, payloads=pl,
                               only_4k=a.__dict__.get("4k_only", False)))
    return asyncio.run(run(a.keyword, pages, a.slots, a.dry_run, cap))


if __name__ == "__main__":
    sys.exit(main())
