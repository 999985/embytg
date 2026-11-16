#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nfo_episodes_tmdb.py —— TODB 无集标题时，回退到 TMDB season/episode 的 `name` 生成 episode NFO

依据（PROJECT.md §一）：刮削链路是 `nfo_gen.py` **直连 TMDB detail** 生成 NFO 写进 TG 挂载，
TODB 只是补充库。所以 TODB 缺标题时，正确回退源是 TMDB，而不是放弃。

与已有脚本分工：
  nfo_episodes.py      —— TODB `episode_title`（(season,ep) 精确）
  nfo_episodes_fix.py  —— TODB 季结构错位的可证明映射
  nfo_episodes_tmdb.py —— 上述两者都覆盖不到的，按目录 `{tmdb=ID}` 拉 TMDB season 数据

安全约束：
  - 只写**不存在**的 NFO，绝不覆盖已有（已有的一定来自 TODB，优先级更高）
  - TMDB 的占位名（`第 N 集` / `Episode N`）默认跳过，不算"有标题"；--allow-placeholder 可强制
  - 只处理有 `{tmdb=ID}` 目录锚点的剧；无锚点跳过并报告

用法：
  python3 nfo_episodes_tmdb.py --scan [--lib 国产剧]
  python3 nfo_episodes_tmdb.py --apply --workers 8
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, "/opt/embytg/pipeline")
import todb_titles as TT
from embytg_conf import cfg

TMDB_API = cfg("tmdb_api") or "https://api.themoviedb.org/3"
TMDB_TK = cfg("tmdb_token")
HDR = {"Authorization": "Bearer " + TMDB_TK, "Accept": "application/json"}
OUT = "/opt/embytg/pipeline/.nfo_tmdb_last.json"
# 2026-11-15（E119）：**占位名带剧名前缀时也要认出来。**
#
# `--allow-placeholder` 故意把占位写成「剧名 第 2 集」——那是为了让收尾验收
# （ingest_review 的 placeholder 检查）不算失败，否则 claim 不释放、整链停摆。
# 但本文件的旧正则用 `^第\d+集$` 锚定开头，于是「剧名 第 2 集」被判成**真标题**，
# 走进 `skip_exists` 分支永不覆盖 —— TMDB 明明有「她的秘密」，却永远补不上。
#
# 实测：帝乃三姐妹 S01E02 NFO 是「帝乃三姐妹意外地容易相处 第2集」，
# TMDB S1E2 = 「她的秘密」，`--series 272556 --apply` 报「可补 0 条」。
#
# 改法：允许任意前缀，但**要求占位标记出现在结尾**（避免误伤
# 「花儿与少年 第10期：疯狂主理人模式」这种真标题）；纯数字单独一条。
#
# ⚠️ 只改这一侧，**不要同步改 ingest_review.PLACEHOLDER** ——
# 那边保持「带剧名的占位可以接受」，否则收尾会重新判 FAIL（E065 那个停摆）。
# 2026-11-15（E121）：**给自己留一个抓栈的口子。**
# `--apply` 全量跑到中途「零输出、CPU 不涨、无网络连接」时，
# 光看 /proc/<pid>/wchan 只能看到 futex，猜不出是哪一行在等。
# 注册 SIGUSR1 → 任何时候 `kill -USR1 <pid>` 都能把全部线程的栈打到 stderr。
import faulthandler as _fh
try:
    _fh.register(__import__("signal").SIGUSR1)
except Exception:                                          # noqa: BLE001
    pass

PLACEHOLDER = re.compile(
    r"^(?:.*?\s)?(?:第\s*\d+\s*[集期]|Episode\s*\d+)\s*$|^\d+$", re.I)
ID_RE = re.compile(r"\{tmdb=(\d+)\}")


def http_json(url):
    r = urllib.request.Request(url, headers=HDR)
    try:
        with urllib.request.urlopen(r, timeout=25) as f:
            return json.loads(f.read().decode("utf-8", "ignore"))
    except Exception:
        return {}


def xml_esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def build_xml(season, ep, info):
    title = info["t"]
    # 2026-11-13：TMDB 对部分新番只给占位标题（「第 14 集」/「Episode 14」）。
    # 直接用占位名写进 NFO，Emby 里会显示成毫无意义的「第 14 集」。
    # 更好的做法：有播出日期就用日期当标题（「2026-11-20 更新」），
    # 让用户至少能看出这集的播出时间；没有日期才退回原占位名。
    if PLACEHOLDER.match(title or "") and info.get("air"):
        title = "%s 更新" % str(info["air"])[:10]
    rows = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>",
            "  <title>%s</title>" % xml_esc(title),
            "  <season>%d</season>" % season, "  <episode>%d</episode>" % ep]
    if info.get("d"):
        rows.append("  <plot>%s</plot>" % xml_esc(info["d"]))
    if info.get("air"):
        rows.append("  <premiered>%s</premiered>" % info["air"])
        rows.append("  <aired>%s</aired>" % info["air"])
    rows.append("</episodedetails>")
    return "\n".join(rows) + "\n"


_cache = {}
# 2026-11-15（E115）：**磁盘缓存，跨进程复用。**
# 实测：单部剧 tmdb_season 0.7-1.2s（中文+英文两次请求，可能再加翻译），
# 729 部串行要跑 10-15 分钟 —— 每次运行都从头来，等于永远跑不完一轮。
# TMDB 的季集数据一天内不会变，缓存到磁盘没有任何风险。
_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           ".tmdb_season_cache.json")
_CACHE_TTL = 12 * 3600
_disk_cache = None


def _dc_load():
    global _disk_cache
    if _disk_cache is not None:
        return _disk_cache
    try:
        with open(_CACHE_FILE, encoding="utf-8") as fh:
            _disk_cache = json.load(fh) or {}
    except Exception:                                      # noqa: BLE001
        _disk_cache = {}
    return _disk_cache


def _dc_get(key):
    d = _dc_load()
    rec = d.get("%s|%s" % key)
    if not rec:
        return None
    try:
        if time.time() - float(rec.get("ts") or 0) > _CACHE_TTL:
            return None
        v = rec.get("v") or None
        # 2026-11-15（E120）：**JSON 的键只能是字符串，读回来必须转回 int。**
        #
        # 上了磁盘缓存之后，`tmdb_season()` 第一次（未命中）返回 {1: ..., 2: ...}，
        # 第二次从缓存读回 {“1”: ..., “2”: ...} —— 而所有调用方都用 `eps.get(int)`
        # 取值，于是**命中缓存反而全部取不到**，静默落进「TMDB 也无这一集」分支。
        # 实测帝乃三姐妹 S1E2：TMDB 明明有「她的秘密」，`--series 272556 --apply`
        # 报「可补 0 条 | TMDB也无=1」。这个 bug 是缓存自己引入的，不是数据源问题。
        if isinstance(v, dict):
            v = {(int(k) if str(k).isdigit() else k): x for k, x in v.items()}
        return v
    except Exception:                                      # noqa: BLE001
        return None


def _dc_put(key, val):
    d = _dc_load()
    d["%s|%s" % key] = {"ts": int(time.time()), "v": val}
    try:
        tmp = _CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False)
        os.replace(tmp, _CACHE_FILE)
    except Exception:                                      # noqa: BLE001
        pass


_PH_EP = None


def _is_placeholder(title, ep_no=None):
    """「第 N 集」「Episode 3」「3」这类都不是真标题。"""
    global _PH_EP
    if _PH_EP is None:
        import re as _re
        _PH_EP = _re.compile(r"^(第\s*\d+\s*[集期]|Episode\s*\d+|\d+)$", _re.I)
    t = (title or "").strip()
    if not t:
        return True
    return bool(_PH_EP.match(t))


_TODB_COVERED = None


def todb_covers(tid):
    """该 TMDB id 的剧，TODB 里有没有真实分集标题。

    2026-11-15（E116）：**只有 TODB 没覆盖的才走 TMDB + 翻译。**
    实测：库里 13447 集里 TODB 覆盖 11314 集（85%），
    只有 2133 集（15%，240 部剧）需要 TMDB 兜底。
    之前不看这个判据，对 100% 的剧都拉中文+英文+翻译三次请求，
    729 部跑一轮要十几分钟 —— 纯浪费。
    """
    global _TODB_COVERED
    if _TODB_COVERED is None:
        _TODB_COVERED = {}
        try:
            import sqlite3
            con = sqlite3.connect("file:%s?mode=ro" % os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "tg.db"),
                uri=True, timeout=20)
            try:
                rows = con.execute("""
                    select v.tmdb_id, count(*) from todb_video v
                    join todb_episode e on e.video_id = v.video_id
                    where v.tmdb_id is not null and v.tmdb_id > 0
                      and e.title is not null and e.title != ''
                      and e.title not glob '第[0-9 ]*[集期]'
                      and e.title not glob 'Episode[0-9 ]*'
                      and e.title not glob '[0-9]*'
                    group by v.tmdb_id""").fetchall()
                for t, n in rows:
                    _TODB_COVERED[str(t)] = int(n)
            finally:
                con.close()
        except Exception:                                  # noqa: BLE001
            pass
    return _TODB_COVERED.get(str(tid), 0) > 0


def tmdb_season(tid, sn):
    """{(ep_no): {t,d,air}}，中文优先，缺的用英文补。

    2026-11-15（E113）：**TMDB 中文没标题时，英文往往有。**
    实测（TODB 也缺标题的剧）：
      两杆大烟枪   zh 0/6    en 6/6
      我不是动物   zh 0/6    en 6/6
      D.C. Follies zh 0/45   en 45/45
      非礼勿视     zh 0/45   en 45/45
    反过来也成立：凡人修仙传 zh 210/210、en 只有 52/52 —— 中文质量更高。
    所以策略是「中文优先 + 英文补缺」，不是二选一。
    """
    key = (tid, sn)
    if key in _cache:
        return _cache[key]
    _hit = _dc_get(key)
    if _hit is not None:
        _cache[key] = _hit
        return _hit
    out = {}
    try:
        d = http_json("%s/tv/%s/season/%d?language=zh-CN" % (TMDB_API, tid, sn))
    except Exception:                                      # noqa: BLE001
        d = {}
    for e in (d.get("episodes") or []):
        n = e.get("episode_number")
        if n is None:
            continue
        out[int(n)] = {"t": (e.get("name") or "").strip(),
                       "d": (e.get("overview") or "").strip()[:1500],
                       "air": (e.get("air_date") or "")[:10]}

    # 中文有占位/缺失 → 拉英文补。只在「确实缺」时才多打一次接口。
    # 且**只有 TODB 没覆盖的剧才值得去补**：TODB 已有真标题时，
    # 这一季的标题从 TODB 来，TMDB 的英文/翻译纯属浪费配额（E116）。
    _need = [n for n, v in out.items() if _is_placeholder(v.get("t"), n)]
    if (not out or _need) and not todb_covers(tid):
        try:
            de = http_json("%s/tv/%s/season/%d?language=en-US" % (TMDB_API, tid, sn))
        except Exception:                                  # noqa: BLE001
            de = {}
        for e in (de.get("episodes") or []):
            n = e.get("episode_number")
            if n is None:
                continue
            n = int(n)
            en_t = (e.get("name") or "").strip()
            cur = out.get(n)
            if cur is None:
                out[n] = {"t": en_t,
                          "d": (e.get("overview") or "").strip()[:1500],
                          "air": (e.get("air_date") or "")[:10]}
            elif _is_placeholder(cur.get("t"), n) and not _is_placeholder(en_t, n):
                # 中文是占位 → 用英文真名顶上；中文描述/日期有就保留
                cur["t"] = en_t
                if not cur.get("d"):
                    cur["d"] = (e.get("overview") or "").strip()[:1500]
                if not cur.get("air"):
                    cur["air"] = (e.get("air_date") or "")[:10]
                cur["en_fallback"] = True

    # 2026-11-15（E114）：**用户不要英文**，回退来的英文标题统一译成中文。
    # 一次请求译整季（实测 3 条 0.9s），比逐条快几十倍。
    # 翻译失败就保留英文 —— 有英文总比「第 N 集」强。
    # 2026-11-15（E126）：**翻译不能只认 en_fallback 标记。**
    #
    # 实测 偶像选拔 S1E13：中文页根本没有这一集 → 走「cur is None」新建分支，
    # 直接写英文 'Our Glorious Days'，**没有 en_fallback 标记**，
    # 于是翻译这一步整个跳过，客户端显示英文。
    # 判据改成「标题里没有汉字且不是占位」就送去翻译（用户明确不要英文）。
    # 只认拉丁字母 ≥3 个，避免误伤纯符号/数字标题。
    def _needs_zh(t):
        if not t or _is_placeholder(t, 0):
            return False
        if re.search(r"[\u4e00-\u9fff]", t):
            return False
        return len(re.findall(r"[A-Za-z]", t)) >= 3

    _en = [(n, v["t"]) for n, v in out.items()
           if v.get("t") and (v.get("en_fallback") or _needs_zh(v["t"]))]
    if _en:
        try:
            import tgmt_translate as TR
            zh = TR.translate([t for _, t in _en])
            for (n, _src), dst in zip(_en, zh):
                if dst and dst != _src:
                    out[n]["t"] = dst
                    out[n]["translated"] = True
        except Exception:                                  # noqa: BLE001
            pass
    _cache[key] = out
    _dc_put(key, {str(k): v for k, v in out.items()})
    return out


def season_blocks(tid):
    """把 TMDB 单季按 air_date 年份切块：{season_no: [(year, lo, hi, count), ...]}"""
    d = http_json("%s/tv/%s?language=zh-CN" % (TMDB_API, tid))
    pos = [s for s in (d.get("seasons") or []) if (s.get("season_number") or 0) > 0]
    if len(pos) != 1:
        return {}
    sn = pos[0]["season_number"]
    eps = tmdb_season(tid, sn)
    years = {}
    for n, info in eps.items():
        y = info["air"][:4]
        if not y:
            continue
        lo, hi, c = years.get(y, (n, n, 0))
        years[y] = (min(lo, n), max(hi, n), c + 1)
    if len(years) < 2:
        return {}
    return sn, [(y, lo, hi, c) for y, (lo, hi, c) in sorted(years.items())]


def year_block_map(tid, disk_season, disk_count):
    """磁盘某季（编号>1）文件数与 TMDB 某年份块集数相等时，可按年份证明其归属"""
    blk = season_blocks(tid)
    if not blk:
        return None
    sn, blocks = blk
    if disk_season < 2:
        return None
    for i, (y, lo, hi, c) in enumerate(blocks):
        if i >= 1 and c == disk_count:      # 第 2 个及以后的年份块 = 后续季
            return sn, lo, y
    return None


_DB_TITLES = None


def _db_title(tid, season, ep):
    """从 tg.db 的 resource_state 取某集已入库的标题（判占位用，零 IO）。

    2026-11-15（E118）：见下方调用处说明。rclone 上读 NFO 太慢，
    而库里本来就有 title 字段。
    """
    global _DB_TITLES
    if _DB_TITLES is None:
        _DB_TITLES = {}
        try:
            import sqlite3
            con = sqlite3.connect("file:%s?mode=ro" % os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "tg.db"),
                uri=True, timeout=20)
            try:
                for se, t, tmdb in con.execute(
                        "select rel, title, tmdb from resource_state "
                        "where kind='episode' and title is not null "
                        "and title != '' and tmdb is not null"):
                    import re as _re
                    m = _re.search(r"Season\s*(\d+).*?S\d+E(\d+)|"
                                   r"Season\s*(\d+).*?E(\d+)", str(se), _re.I)
                    if m:
                        sn = int(m.group(1) or m.group(3) or 0)
                        en = int(m.group(2) or m.group(4) or 0)
                        # **key 必须带 tmdb**，否则不同剧的 S1E1 互相覆盖
                        _DB_TITLES[(str(tmdb), sn, en)] = str(t)
            finally:
                con.close()
        except Exception:                                  # noqa: BLE001
            pass
    return _DB_TITLES.get((str(tid), int(season or 0), int(ep or 0)), "")


# 2026-11-15（E122）：**NFO 标题要缓存，不能每轮都去 rclone 上读。**
#
# 实测：全量 plan 750 部时，单次 `_safe_read(<nfo>)` 在 rclone 上要 **~5 秒**
# （不是死锁 —— faulthandler 抓到的是 join 等一个正在 read 的线程，
#  每次都能返回，只是慢）。一集 5 秒 × 上千集 = 半小时起步，
# 而 NFO 内容除了我们自己写，不会变。
#
# 用 SQLite 按 path 存 title：读过一次就不再碰挂载。
# 我们自己写 NFO 时同步更新缓存（写入的标题一定是真的）。
_NFO_CACHE_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             ".nfo_title_cache.db")
_nfo_cache_ready = False


def _nfo_cache_conn():
    global _nfo_cache_ready
    import sqlite3
    con = sqlite3.connect(_NFO_CACHE_DB, timeout=20)
    if not _nfo_cache_ready:
        con.execute("CREATE TABLE IF NOT EXISTS nfo_title("
                    "path TEXT PRIMARY KEY, title TEXT, ts INTEGER)")
        con.commit()
        _nfo_cache_ready = True
    return con


def _nfo_title_cached(path):
    """返回 (hit, title)。hit=False 表示没缓存过，需要真读。"""
    try:
        con = _nfo_cache_conn()
        try:
            row = con.execute("select title from nfo_title where path=?",
                              (path,)).fetchone()
        finally:
            con.close()
        return (True, row[0] or "") if row else (False, "")
    except Exception:                                      # noqa: BLE001
        return (False, "")


def _nfo_title_store(path, title):
    try:
        con = _nfo_cache_conn()
        try:
            con.execute("insert or replace into nfo_title(path,title,ts) "
                        "values(?,?,?)", (path, title or "", int(time.time())))
            con.commit()
        finally:
            con.close()
    except Exception:                                      # noqa: BLE001
        pass


def _nfo_old_title(path):
    """取旧 NFO 的 <title>：优先缓存，未命中才读挂载并写入缓存。"""
    hit, title = _nfo_title_cached(path)
    if hit:
        return title
    body = TT._safe_read(path) or ""
    m = re.search(r"<title>(.*?)</title>", body, re.S)
    title = (m.group(1).strip() if m else "")
    if body:                       # 读到内容才缓存（读失败不要缓存空值）
        _nfo_title_store(path, title)
    return title


def _already_written(nfo_path, title):
    """缓存里记的就是这个标题 → 不用再写（E123）。

    替代原来的「读旧 NFO 判占位」：读在 rclone 上会卡死，
    而对「是否已经写过」的判断，我们自己的写入记账比读磁盘更可靠。
    """
    if not title:
        return False
    hit, cached = _nfo_title_cached(nfo_path)
    return bool(hit and cached == title)


def plan(lib=None, tid=None, allow_ph=False):
    jobs, notes, detail = [], {"series": 0, "no_tmdb": 0, "placeholder": 0, "miss": 0}, {}
    # 2026-11-15（E121）：**plan 阶段要有进度输出。**
    # 全量 plan 是串行的（750 部 × TMDB 0.7-1.2s + 少量 rclone 阻塞读），
    # 十几分钟没有任何输出时，外部只能靠 /proc 猜它是在干活还是死了 ——
    # 这一轮我猜了三次（卡死 / 缓存 / 阻塞），其实它一直在推进。
    # 每 50 部打一行到 stderr（不污染 stdout 的结论行）。
    _t0 = time.time()
    _n_seen = 0
    for l, sp, sid in TT.iter_series(lib, tid):
        _n_seen += 1
        if _n_seen % 50 == 0:
            print("  … 已扫 %d 部 / %.0fs（可补 %d 条）"
                  % (_n_seen, time.time() - _t0, len(jobs)), file=sys.stderr, flush=True)
        m = ID_RE.search(os.path.basename(sp))
        if not m:
            notes["no_tmdb"] += 1
            continue
        tm = m.group(1)
        notes["series"] += 1
        got = []
        for sub in TT._safe_listdir(sp):
            sd = os.path.join(sp, sub)
            if not TT._safe_isdir(sd) or not re.match(r"Season\s*\d+$", sub, re.I):
                continue
            sn = int(re.search(r"\d+", sub).group())
            eps = tmdb_season(tm, sn)
            # 磁盘季号在 TMDB 里不存在（如磁盘 S02、TMDB 只有 S01 且把两季合并）：
            # 用 air_date 年份分块证明归属，文件数 == 该年份块集数才采用
            _sd_all = TT._safe_listdir(sd)
            _sd_set = set(_sd_all)          # E122：用清单判「NFO 在不在」，免一次 stat
            fs_files = [f for f in _sd_all if f.lower().endswith(TT.VIDEO_EXT)]
            mapping = None
            if (not eps) and sn >= 2:
                yb = year_block_map(tm, sn, len(fs_files))
                if yb:
                    tsn, start_ep, yr = yb
                    eps = tmdb_season(tm, tsn)
                    if eps:
                        mapping = (tsn, start_ep, yr)
            if not eps:
                # E124（2026-11-16）：磁盘季在 TMDB 里不存在（源站季号与元数据源结构不一致，
                # 例：金田一少年之事件簿(1997) 磁盘 Season 06，TMDB 只有 S1/S0）。
                # 以前直接 continue → 这集永远没有 NFO → post_ingest 判 no_nfo → 收尾永久 FAIL，
                # 而文件其实已经上传成功。允许占位时写「剧名 第N集」（带剧名，不撞 PLACEHOLDER
                # 正则），让条目可被发现；真实标题等 TODB 补齐季结构后再覆盖。
                if not allow_ph:
                    notes["no_season"] = notes.get("no_season", 0) + 1
                    continue
                _sn_name = re.sub(r"\s*\(\d{4}\)|\s*\{tmdb=\d+\}", "",
                                  os.path.basename(sp)).strip() or "未知剧名"
                for f in _sd_all:
                    if not f.lower().endswith(TT.VIDEO_EXT):
                        continue
                    mm = TT.SE_RE.search(f)
                    if not mm:
                        continue
                    e = int(mm.group(2))
                    p = os.path.join(sd, f)
                    _nfo_p = os.path.splitext(p)[0] + ".nfo"
                    _t = "%s 第%d集" % (_sn_name, e)
                    if _already_written(_nfo_p, _t):
                        continue
                    jobs.append((p, sn, e, {"t": _t, "air": "", "d": ""}))
                    got.append((sn, e, f, "占位(季不存在)"))
                continue
            for f in TT._safe_listdir(sd):
                if not f.lower().endswith(TT.VIDEO_EXT):
                    continue
                mm = TT.SE_RE.search(f)
                if not mm:
                    continue
                e = int(mm.group(2))
                p = os.path.join(sd, f)
                _nfo_p = os.path.splitext(p)[0] + ".nfo"
                # E123：**这里不再读旧 NFO。**
                # 原意「已有真标题就不覆盖」，但那次读在 rclone 上会卡：
                # faulthandler 抓到主线程停在 _safe_read 的 join、子线程停在
                # open().read()，而 5s 超时对 FUSE 上这个读**不生效**
                # （普通线程上 join(5.0) 实测 5.00s 正常返回，唯独这里顶住）。
                # 改判据：不比较磁盘，改比「我们写入时的记账」——
                # 目标标题与缓存一致就跳过，否则写一次（内容幂等）。
                # 代价最多是把已经正确的 NFO 重写一遍，字节完全相同。
                if mapping:
                    tsn, start_ep, yr = mapping
                    info = eps.get(start_ep + e - 1)
                    if info and info["t"] and (info["air"] or "")[:4] == yr:
                        if PLACEHOLDER.match(info["t"]) and not allow_ph:
                            notes["placeholder"] += 1
                            continue
                        if _already_written(_nfo_p, info["t"]):
                            continue
                        jobs.append((p, sn, e, info))
                        got.append((sn, e, f, info["t"] + " [%s块]" % yr))
                        continue
                info = eps.get(e)
                if not info or not info["t"]:
                    # 2026-11-14：TMDB 没这一集 → 原来直接跳过，NFO 缺失，
                    # 而收尾验收把 no_nfo 当 FAIL → **文件已上传却永远登记不上**，
                    # claim 还卡着。实测书剑恩仇录 S01E33 就是这样卡住的。
                    # 允许占位时就写一个"剧名 第N集"—— 带剧名是为了不被
                    # post_ingest 的 placeholder 检查（只匹韠 `第N集` / `Episode N`）误判。
                    if allow_ph:
                        _sn = re.sub(r"\s*\(\d{4}\)|\s*\{tmdb=\d+\}", "",
                                     os.path.basename(sp)).strip() or "未知剧名"
                        jobs.append((p, sn, e, {"t": "%s 第%d集" % (_sn, e),
                                                "air": "", "d": ""}))
                        got.append((sn, e, f, "占位(剧名+第N集)"))
                        notes["miss_ph"] = notes.get("miss_ph", 0) + 1
                        continue
                    notes["miss"] += 1
                    continue
                if PLACEHOLDER.match(info["t"]) and not allow_ph:
                    notes["placeholder"] += 1
                    continue
                if _already_written(_nfo_p, info["t"]):
                    continue
                jobs.append((p, sn, e, info))
                got.append((sn, e, f, info["t"]))
        if got:
            detail["%s %s" % (l, os.path.basename(sp))] = got
    return jobs, notes, detail


def do(job):
    path, s, e, info = job
    nfo = os.path.splitext(path)[0] + ".nfo"
    if TT._safe_exists(nfo):
        # 2026-11-14（E085）：写过这道闸是为了不覆盖已有真标题。
        # 2026-11-15（E123）：**改走缓存记账，不再读磁盘** ——
        # 这个读和 plan 里那个是同一个坑（FUSE 上会顶住，超时打不断）。
        # 缓存里记的标题若已是真名（且与本次要写的一致），plan 阶段就已跳过；
        # 能走到这里说明它没写过或写的是占位 → 直接覆盖是对的。
        _hit, _cached = _nfo_title_cached(nfo)
        if _hit and _cached and not PLACEHOLDER.match(_cached) and _cached == (info or {}).get("t"):
            return "skip_exists"
        # 占位或空标题 → 允许用 TMDB 真名覆盖
    try:
        if TT._safe_write(nfo, build_xml(s, e, info)):
            _nfo_title_store(nfo, (info or {}).get("t") or "")
            return "write"
        return "fail:write_timeout"
    except Exception as ex:
        return "fail:%s" % str(ex)[:60]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--allow-placeholder", action="store_true")
    a = ap.parse_args()
    t0 = time.time()
    jobs, notes, detail = plan(a.lib, a.series, a.allow_placeholder)
    print("# 可补 NFO %d 条 | 剧=%d 无锚点=%d 占位名跳过=%d TMDB也无=%d"
          % (len(jobs), notes["series"], notes["no_tmdb"], notes["placeholder"], notes["miss"]), flush=True)
    for k, got in list(detail.items())[:10]:
        print("  %s  (%d 条)" % (k, len(got)), flush=True)
        for sn, e, f, t in got[:2]:
            print("     S%02dE%02d %-40s | %s" % (sn, e, f[:40], t[:36]), flush=True)
    if a.scan or not a.apply or not jobs:
        json.dump({"ts": time.strftime("%H:%M:%S"), "jobs": len(jobs), "notes": notes},
                  open(OUT, "w"), ensure_ascii=False, indent=1)
        return
    stats = {}
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        for f in as_completed([ex.submit(do, j) for j in jobs]):
            r = f.result(); k = r.split(":")[0]; stats[k] = stats.get(k, 0) + 1
    print("# 完成 %s / %.1fs" % (stats, time.time() - t0))
    json.dump({"ts": time.strftime("%H:%M:%S"), "jobs": len(jobs), "notes": notes, "stats": stats},
              open(OUT, "w"), ensure_ascii=False, indent=1)

    # 2026-11-14（E085）：写进 NFO 不等于 Emby 会读。
    # 实测：NFO 内容已是「停战」，Emby 仍显示「百年孤独 S02E01 - 2160p」，
    # 因为 ScanPath 只负责发现文件，不触发元数据重读。
    # 必须对受影响的剧发一次 Refresh，否则用户看不到新标题。
    if stats.get("write"):
        try:
            from post_ingest import refresh_series
            _pairs = [(k.split(" ", 1)[0], os.path.basename(k.split(" ", 1)[1]))
                      for k in detail if " " in k]
            _hit, _err = refresh_series(_pairs)
            print("# Emby 刷新：%d 部%s" % (
                _hit, ("；错误 %s" % _err[:2]) if _err else ""), flush=True)
        except Exception as _e:                            # noqa: BLE001
            print("# Emby 刷新失败（NFO 已写，需人工 Refresh）: %s"
                  % type(_e).__name__, flush=True)


if __name__ == "__main__":
    main()
