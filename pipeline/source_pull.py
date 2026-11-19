#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""source_pull.py —— 从第三方 Emby 源服按「入库预算 + 访问规则」拉取单个视频

规则来源
--------
- 伪装/节流/负载均衡/审计：`docs/SOURCE_SERVERS.md`（一律经 `src_guard.Guard`）
- 流量预算：`docs/SOURCE_SERVERS.md §7`（`ingest_quota.py`，日 300GB / 月 10T·安全线 9T）

用法
----
    # 只看能不能拉（不发下载请求）
    python3 source_pull.py --series "小丑回魂：欢迎来到德里镇" --season 1 --ep 3 --dry

    # 真拉（默认存 /opt/embytg/tmp，默认不限速，断点续传）
    python3 source_pull.py --series "小丑回魂：欢迎来到德里镇" --season 1 --ep 3 --apply

    # 拉完直接替换库里的截断文件（旧文件移到 _trash_<ts>/，不删除）
    python3 source_pull.py ... --apply --install "/opt/embytg/mnt/embytg/TV/欧美剧/xxx/Season 01/xxx S01E03 - 尽显端倪 - 2160p.mp4"

设计要点
--------
1. **先问预算**：拉之前用 `ingest_quota.usage()` 算剩余，`need > 剩余` 直接退出（码 3）。
2. **断点续传**：写 `<out>.part`，重跑带 `Range: bytes=<已有>-`，同一源最多 3 次重试。
3. **多源择优**：默认按 `src_guard` 的负载均衡顺序找第一个有该集的源；`--host` 可指定。
4. **装库不删旧**：`--install` 时旧文件 `rename` 到 `_trash_<ts>/`（可恢复），新文件 `.part → rename` 原子落位；
   同名 `.nfo` 不动（标题不变）。
5. **限速**：用户 2026-11-11 明确取消“正常观看节奏”人为限速；
   `--limit-mbps 0`（默认）表示不限速。仍保留 Hills UA、只 GET、单源并发 1、
   ≥1.6s 元数据间隔、单会话 ≤12 次和连续失败熔断。
"""
import argparse
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import src_guard as SG          # noqa: E402
import ingest_quota as Q        # noqa: E402

TMP = "/opt/embytg/tmp"
CHUNK = 262144

# 搜索缓存（2026-11-13）：见 search_series() 的 docstring。
# 键 = (源label, 归一化剧名)；值 = (时间戳, items, 是否命中)
_SEARCH_CACHE = {}
# 找到：3 天。
#
# 2026-11-14 改（原 6 小时）：
# 队列里 1467 部是「库里完全没有」的新剧，一部新剧要下载几十集、跨好几天。
# 正向缓存的键是「源 + 剧名 → Series Id」，而 **Series Id 在一个剧的生命周期里不变**。
# 用 6 小时意味着同一部剧每天被重搜 4 次、3 天被重搜 12 次，全是白烧。
# 改成 3 天 ≈ 覆盖一部剧的下载周期；真的变了（源站重建库）也只是多试一次。
# 负结果仍然保持 6 小时不动 —— 那是"这部剧源站有没有"，可能会新增，必须短。
_SEARCH_TTL_HIT = 3 * 86400
# 没找到：6 小时后再问。
#
# 2026-11-14 改（原来是 45 分钟，这是 3% 命中率的直接原因）：
# 队列里有 288 部剧在任何源上都搜不到（`no_source_probe`），而 series 级冷却语义
# 本来就是 **12 小时**（gha_next3: mark_new_done(days=0.5)）。两层保护互相打架：
# 冷却说"12 小时内别再试"，缓存却说"45 分钟后就重新搜"——于是每 45 分钟把 9 个源
# 全重搜一遍，日配额（2700 次）几乎全烧在"注定找不到"的搜索上，真实命中率掉到 3%。
#
# 取 6 小时的理由：① 与 12h 冷却同量级，冷却到期后仍会重新探测（不会把新上架的剧
# 永久挡掉）；② 源站"新增一整部剧"是低频事件，半天不查没有实际损失；
# ③ 比正缓存（6h）不更短，避免"找到了却立刻忘掉"的抖动。
_SEARCH_TTL_MISS = 6 * 3600
# L2：**磁盘**缓存。这是关键——`gha_next3.py` 每个周期是一个新进程，只处理一个决策，
# 所以进程内字典跨不了周期，等于没做。真正省配额的是这个 SQLite 表。
_SEARCH_DB = os.environ.get("EMBYTG_SRC_CACHE_DB") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".src_search_cache.db")


def human(n):
    return "%.2f GB" % (n / 1e9) if n >= 1e9 else "%.1f MB" % (n / 1e6)


def _norm_title(s):
    return re.sub(r"[\s\-—–－_·:：!！?？.,，'\"“”‘’（）()\[\]【】]+", "",
                  (s or "").strip()).lower()


# ---- 列集缓存（2026-11-14）----
# 实测列集请求涨到 218 次/小时，而每小时只出 4 集：同一部剧的同一季，每次决策都重列一遍。
# 按「完整请求路径」缓存最稳（同请求 → 同响应），TTL 15 分钟：
# 新集上传是小时级事件，15 分钟不会漏；而一次下载会话内会反复列同一季，缓存直接命中。
# 2026-11-14 二次修正：15 分钟太短。
# 实测当天 548 次列集请求只对应 **97 个不同的 (源,SID)** —— 同一部剧被列了最多 46 次，
# 而重复跨了好几小时（15 分钟窗口覆盖不到）。若 TTL 覆盖全天，可省 82% 的列集请求。
# 取 4 小时的理由：剧集列表只在"新一集上传"时变化，日更剧一天一次、
# 周更剧一周一次；4 小时的滞后完全在容忍范围内（一天有 24 小时去捡）。
# 2026-11-14（E102）：4h → 24h。
# 实测今日 3592 次请求里 **列集占 2706 次（75%）**，而 ep_cache 只有 3 条记录 ——
# 缓存等于没生效。原因是同一部剧的决策周期跨越好几个小时（排队、重试、冷却），
# 4 小时窗口覆盖不到，每次决策都重新列一遍。
# 剧集列表是「一天变一次」（日更剧）甚至「一周变一次」（周更剧），
# 24 小时滞后对入库没有任何实际影响 —— 一天有 24 小时去捡。
_EP_TTL = 24 * 3600
# 2026-11-19（E184）：负缓存。列集失败（超时 / 5xx / 429）以前**不缓存**，于是
# 同一部剧、同一个源、同一条 URL 被同一轮决策反复重试：实测 11-18 一天 168 次
# 失败列集里 120 次是重复（48 个不同键），aaa.yusen6 一条 25s 超时 2 分钟内重打 5 次
# ——单键一天白烧 125 秒 + 5 次配额。失败结果按 (源,路径) 记 10 分钟，
# 窗口内**照旧抛同样的异常**给上层（语义不变：仍然失败），只是不再打源站。
_EP_ERR_TTL = int(float(os.getenv("EMBYTG_EP_ERR_TTL", "600")))
_EP_CACHE = {}


def _stat(kind):
    """2026-11-14（E102）：缓存命中率必须可观测。

    之前只能靠「ep_cache 有几条」猜缓存有没有生效，那是结果不是命中率。
    只记一行累计计数，开销可忽略；`python3 source_pull.py --cache-stats` 可看。
    """
    try:
        c = _l2_conn()
        c.execute("CREATE TABLE IF NOT EXISTS cache_stat(kind TEXT PRIMARY KEY, n INTEGER)")
        c.execute("INSERT INTO cache_stat(kind,n) VALUES(?,1) "
                  "ON CONFLICT(kind) DO UPDATE SET n=n+1", (kind,))
        c.commit()
        c.close()
    except Exception:                                       # noqa: BLE001
        pass


def cache_stats():
    out = {}
    try:
        c = _l2_conn()
        c.execute("CREATE TABLE IF NOT EXISTS cache_stat(kind TEXT PRIMARY KEY, n INTEGER)")
        for k, n in c.execute("SELECT kind,n FROM cache_stat").fetchall():
            out[k] = int(n)
        # 2026-11-19（E184）：**活行数**要跟命中率一起看。
        # 之前只看「命中 5115 / 未命中 5174」会以为缓存健康，而表里只有 6 行 ——
        # 命中数掩盖了「写入没落地」。现在一条命令同时给出行数与写入错误计数。
        try:
            out["ep_rows"] = int(c.execute(
                "SELECT count(*) FROM ep_cache WHERE k NOT LIKE '!err%'").fetchone()[0])
            out["ep_err_rows"] = int(c.execute(
                "SELECT count(*) FROM ep_cache WHERE k LIKE '!err%'").fetchone()[0])
        except Exception:                                        # noqa: BLE001
            pass
        c.close()
    except Exception:                                            # noqa: BLE001
        pass
    return out


def _ep_l2_get(k, now):
    try:
        c = _l2_conn()
        c.execute("CREATE TABLE IF NOT EXISTS ep_cache(k TEXT PRIMARY KEY, ts REAL, body TEXT)")
        row = c.execute("SELECT ts, body FROM ep_cache WHERE k=?", (k,)).fetchone()
        c.close()
        if not row:
            _stat("ep_miss")
            return None
        ts, body = row
        if now - float(ts) >= _EP_TTL:
            _stat("ep_expired")
            return None
        _stat("ep_hit")
        return (float(ts), json.loads(body or "null"))
    except Exception:                                       # noqa: BLE001
        return None


def _ep_l2_put(k, rec):
    """写 L2。失败**必须可见**：原来 except: pass 让「缓存写了 5000 次只留下 6 行」
    这种事故静默了四天（2026-11-19 排查实录）。现在失败计入 cache_stat。"""
    c = None
    try:
        ts, data = rec
        now = time.time()
        c = _l2_conn()
        c.execute("CREATE TABLE IF NOT EXISTS ep_cache(k TEXT PRIMARY KEY, ts REAL, body TEXT)")
        c.execute("INSERT OR REPLACE INTO ep_cache VALUES(?,?,?)",
                  (k, ts, json.dumps(data, ensure_ascii=False)))
        # 2026-11-19（E184）：清理基准原本是**记录自己的 ts** —— 任何一条时间单位错的
        # 记录（ms / 0 / 未来时间）都会一次性清空整张表，而异常被吞掉，没人看得见。
        # 改成当前墙钟做基准；一次删超过 50 条记成 ep_purge_bulk，把「整表被抹」变成
        # 可观测事件（本次排查就是因为这个盲区多花了一小时）。
        c.execute("CREATE TABLE IF NOT EXISTS cache_stat(kind TEXT PRIMARY KEY, n INTEGER)")
        cur = c.execute("DELETE FROM ep_cache WHERE k NOT LIKE '!err%' AND ts < ?",
                        (now - _EP_TTL * 4,))
        if (cur.rowcount or 0) > 50:
            c.execute("INSERT INTO cache_stat(kind,n) VALUES('ep_purge_bulk',1) "
                      "ON CONFLICT(kind) DO UPDATE SET n=n+1")
        c.commit()
        c.close()
    except Exception as e:                                       # noqa: BLE001
        try:
            if c is not None:
                c.close()
        except Exception:                                        # noqa: BLE001
            pass
        _stat("ep_put_err:" + type(e).__name__)


# ---- 列集负缓存（2026-11-19 / E184）----
# 失败也记一笔：同一 (源,路径) 在 _EP_ERR_TTL 内直接抛同样的错，不再打源站。
_EP_ERR_PREFIX = "!err\x1f"


def _ep_err_get(k, now):
    try:
        c = _l2_conn()
        c.execute("CREATE TABLE IF NOT EXISTS ep_cache(k TEXT PRIMARY KEY, ts REAL, body TEXT)")
        row = c.execute("SELECT ts, body FROM ep_cache WHERE k=?",
                        (_EP_ERR_PREFIX + k,)).fetchone()
        c.close()
        if not row:
            return None
        ts, body = row
        if now - float(ts) >= _EP_ERR_TTL:
            return None
        _stat("ep_err_hit")
        return (json.loads(body or "{}") or {}).get("err") or "cached failure"
    except Exception:                                            # noqa: BLE001
        return None


def _ep_err_put(k, msg):
    c = None
    try:
        now = time.time()
        c = _l2_conn()
        c.execute("CREATE TABLE IF NOT EXISTS ep_cache(k TEXT PRIMARY KEY, ts REAL, body TEXT)")
        c.execute("INSERT OR REPLACE INTO ep_cache VALUES(?,?,?)",
                  (_EP_ERR_PREFIX + k, now,
                   json.dumps({"err": str(msg)[:200]}, ensure_ascii=False)))
        c.execute("DELETE FROM ep_cache WHERE k LIKE '!err%' AND ts < ?", (now - _EP_ERR_TTL,))
        c.commit()
        c.close()
    except Exception:                                            # noqa: BLE001
        try:
            if c is not None:
                c.close()
        except Exception:                                        # noqa: BLE001
            pass


def _ep_get(g, st, path):
    """带缓存的 Episodes 请求。缓存异常一律退回直接请求（缓存不能成为故障源）。

    2026-11-14：`lite.cn2gias.uk` 对**过大的 Limit 返回 HTTP 400**，和 E056 里
    `list_source` 遇到的是同一件事（那个函数早已加了降级阶梯，这条路径没有）。
    实测同一个真实 SeriesId=256159：
        /Items?SeriesId=256159&...&Limit=50    -> 200，50 条
        /Items?SeriesId=256159&...&Limit=10000 -> 400
        /Shows/256159/Episodes?...             -> 400/404（该源没实现 Shows 前缀）
    该源目录 28684 部（14 源里最大），却因一个分页参数**全量取不到集**：
    今日 113 次请求 105 次 400，报表显示成"白烧配额"。E057 的 hint 直取
    走 `/Shows/{id}/Episodes`，对 lite 必然失败。
    修法：400/404 时先按 Limit 降级重试，再按 `/Items?SeriesId=` 改写重试。
    前者覆盖"参数太大"，后者覆盖"端点没实现"——两种都实测存在。
    """
    key = (getattr(st, "label", "?"), path)
    now = time.time()
    hit = _EP_CACHE.get(key)
    if hit and now - hit[0] < _EP_TTL:
        return hit[1]
    k = key[0] + "\x1f" + key[1]
    l2 = _ep_l2_get(k, now)
    if l2 is not None:
        _EP_CACHE[key] = l2
        return l2[1]
    # 2026-11-19（E184）：近期失败过就直接抛，不再打源站（负缓存 10 分钟）。
    err = _ep_err_get(k, now)
    if err is not None:
        raise RuntimeError("源 %s 列集近 %ds 内失败过（负缓存）：%s"
                           % (key[0], _EP_ERR_TTL, err))
    data = None
    try:
        data = g.get(st, path)
    except Exception as e:                                          # noqa: BLE001
        if not any(s in str(e) for s in ("400", "404")):
            _ep_err_put(k, e)
            raise
        # ① Limit 降级：同一路径换成后端能接受的分页大小
        for lim in ("500", "200", "100"):
            p2 = _rewrite_limit(path, lim)
            if p2 is None:
                break
            try:
                data = g.get(st, p2)
                break
            except Exception as e2:                                 # noqa: BLE001
                if not any(s in str(e2) for s in ("400", "404")):
                    _ep_err_put(k, e2)
                    raise
        # ② 端点改写：没实现 /Shows/{id}/Episodes 的源走 /Items?SeriesId=
        if data is None:
            alt = _shows_to_items_path(path)
            if alt is None:
                raise
            last = None
            for lim in (None, "500", "200", "100"):
                p3 = alt if lim is None else _rewrite_limit(alt, lim)
                if p3 is None:
                    continue
                try:
                    data = g.get(st, p3)
                    break
                except Exception as e3:                             # noqa: BLE001
                    last = e3
                    if not any(s in str(e3) for s in ("400", "404")):
                        _ep_err_put(k, e3)
                        raise
            if data is None:
                if last is not None:
                    _ep_err_put(k, last)
                    raise last
                raise RuntimeError("源 %s 改写端点后仍无可用返回" % st.label)
    rec = (now, data)
    _EP_CACHE[key] = rec
    _ep_l2_put(k, rec)
    return data


_SHOWS_EP_RE = re.compile(r"^/Shows/([^/]+)/Episodes(?:\?(.*))?$")
_LIMIT_RE = re.compile(r"([?&]Limit=)(\d+)")


def _rewrite_limit(path, limit):
    """把路径里的 `Limit=N` 换成 `Limit=<limit>`；没有 Limit 参数返回 None。"""
    if not path or "Limit=" not in path:
        return None
    return _LIMIT_RE.sub(lambda m: m.group(1) + str(limit), path, count=1)


def _shows_to_items_path(path):
    """`/Shows/{id}/Episodes?q` -> `/Items?SeriesId={id}&IncludeItemTypes=Episode&q`

    `/Shows/{id}/Episodes?Fields=A,B&Season=2` 里 Season 是独立语义，
    Emby 的 `/Items` 也认 `ParentIndexNumber`，但不同后端对 Season 的容忍度不同；
    这里保留原 query 原样追加，Season 若被忽略由上层按季分组逻辑兜住。
    """
    m = _SHOWS_EP_RE.match(path or "")
    if not m:
        return None
    sid, query = m.group(1), (m.group(2) or "")
    base = "/Items?SeriesId=%s&IncludeItemTypes=Episode" % sid
    return base + ("&" + query if query else "")


def _cached_hit(label, series):
    """该源是否**已知**有这部剧（来自 L2 搜索缓存，命中才算）。"""
    k = "%s\x1f%s" % (label, re.sub(r"[\s\-—–－_]+", "", (series or "").strip().lower()))
    r = _l2_get(k, time.time())
    return bool(r and r[2])


def _prefer_cached(order, series):
    """把"已知有这个剧"的源稳定前移，其余保持原相对顺序。

    2026-11-13：实测每天 832 次「列集」请求只换来 109 集（每集走 7.6 个源）。
    缓存已经知道哪个源有这部剧，却还按固定轮转顺序盲试 —— 明明第一个源就能命中。
    稳定排序（hot + cold）保证：① 命中率高的源先试；② 组内仍按 claimer 给的
    预算/轮转顺序，不打乱公平性；③ order_labels 被显式指定为空时不放行任何源。
    """
    if not series:
        return order
    hot, cold = [], []
    for st in order:
        (hot if _cached_hit(getattr(st, "label", ""), series) else cold).append(st)
    return hot + cold


def _ordered_sources(g, exclude=None, max_hosts=0, order_labels=None, host_pref=None,
                     series=""):
    """Return a bounded source order.

    ``order_labels`` is produced by the claimer's cross-process budget/rotation
    layer.  An explicit empty list means "no source is allowed", never "fall
    back to all sources".
    """
    exclude = set(exclude or ())
    order = [s for s in g.order()
             if s.label not in exclude
             and (not host_pref or s.label.startswith(host_pref))]
    if order_labels is not None:
        rank = {label: i for i, label in enumerate(order_labels)}
        order = [s for s in order if s.label in rank]
        order.sort(key=lambda s: rank[s.label])
    order = _prefer_cached(order, series)
    # 2026-11-15（E132）：**max_hosts 不能在这里截断源列表。**
    #
    # 原来 `order = order[:max_hosts]` 把「最多试几个源」实现成了
    # 「只保留优先级最高的 N 个源」。实测 fill lane 因此**永久饿死**：
    # eligible 顺序是 nhk → 39.96 → ct.cdn → goemby → emby.bangumi.ca，
    # try_fill 传 max_hosts=2，于是只搜前两个；而真正出片的主力是
    # emby.bangumi.ca（排第 5），**从来轮不到**。
    # 症状：fill 队列 14 条、missing 79 集，全部 `no_valid_source`，
    # 近 3 小时产出从 45 集掉到 14 集。
    # 正确语义：max_hosts 限制的是「拿到几个源的候选」（见 resolve_candidates 的计数），
    # 而不是「允许看几个源」。顺序照旧保留，供上游按优先级试。
    return order


def quality_of(episode):
    """Return real stream quality, not the release name.

    Source Emby exposes MediaStreams only when Fields=MediaSources is requested.
    The first video stream is authoritative; filename is only a last resort.
    """
    ms = (episode.get("MediaSources") or [{}])[0]
    streams = ms.get("MediaStreams") or []
    video = next((s for s in streams if s.get("Type") == "Video"), {})
    width = int(video.get("Width") or 0)
    height = int(video.get("Height") or 0)
    size = int(ms.get("Size") or episode.get("Size") or 0)
    # 2026-11-20 根因修复：Emby 的 RunTimeTicks 是 **100ns 单位**（1e7/秒），
    # 不是秒。以前把原始 ticks 直接写进 quality["runtime"]，下游
    # quality_gate.candidate_verdict 把它当秒算码率 →
    # 3.4GB 的 2160p 单集算出 0.00 Mbps → 判 fake_4k 一票否决。
    # 实测今日 1792 条候选被误杀（全部 dur=ticks、mbps=0.0），
    # 单集请求数从 3.14 涨到 20.07，源日配额 06:40 就烧光。
    # 这里统一换成秒，并保留 runtime_ticks 供需要原始单位的调用方使用。
    runtime_ticks = int(episode.get("RunTimeTicks") or ms.get("RunTimeTicks") or 0)
    runtime = int(round(runtime_ticks / 10000000.0)) if runtime_ticks else 0
    bitrate = int(video.get("BitRate") or ms.get("Bitrate") or 0)
    if not bitrate and size and runtime_ticks:
        bitrate = int(size * 8 / (runtime_ticks / 10000000.0))
    name = "%s %s" % (episode.get("Name") or "", ms.get("Path") or ms.get("Name") or "")
    label, rank = "sd", 0
    if height >= 1800 or width >= 3000:
        label, rank = "2160p", 4
    elif height >= 1400 or width >= 2400:
        label, rank = "1440p", 3
    elif height >= 1000 or width >= 1800:
        label, rank = "1080p", 2
    elif height >= 700 or width >= 1200:
        label, rank = "720p", 1
    elif not width and not height:
        low = name.lower()
        if "2160p" in low or "4k" in low:
            label, rank = "2160p", 4
        elif "1080p" in low:
            label, rank = "1080p", 2
        elif "720p" in low:
            label, rank = "720p", 1
    codec = str(video.get("Codec") or "").lower()
    hdr = 1 if str(video.get("VideoRange") or "").upper() in ("HDR", "HDR10", "HLG") else 0
    codec_pref = 1 if (rank >= 4 and codec in ("hevc", "h265")) else (
        1 if (rank <= 2 and codec in ("h264", "avc")) else 0)
    return {
        "label": label, "rank": rank, "width": width, "height": height,
        "bitrate": bitrate, "codec": codec, "hdr": hdr,
        "size": size, "runtime": runtime, "runtime_ticks": runtime_ticks,
        "codec_pref": codec_pref,
        "score": (rank, bitrate, hdr, codec_pref),
    }


def title_variants(series):
    """片名尾部的 - / — / 全角括号等在源站很常见；只做保守归一化。"""
    s = (series or "").strip()
    normalized = re.sub(r"[\s\-—–－_]+", " ", s).strip()
    compact = re.sub(r"[\s\-—–－_]+", "", s)
    out = []
    for value in (
        s,
        s.strip(" -—–－_"),
        normalized,
        compact,
    ):
        if value and value not in out:
            out.append(value)
    return out[:5]


def _l2_conn():
    import sqlite3
    c = sqlite3.connect(_SEARCH_DB, timeout=8)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS search_cache("
              "k TEXT PRIMARY KEY, ts REAL, hit INT, items TEXT)")
    return c


def _l2_get(k, now):
    """返回 (ts, items, is_hit) 或 None。任何异常都当作未命中（缓存不能成为故障源）。"""
    try:
        c = _l2_conn()
        row = c.execute("SELECT ts, hit, items FROM search_cache WHERE k=?",
                        (k,)).fetchone()
        c.close()
        if not row:
            return None
        ts, hit, blob = row
        ttl = _SEARCH_TTL_HIT if hit else _SEARCH_TTL_MISS
        if now - float(ts) >= ttl:
            return None
        return (float(ts), json.loads(blob or "[]"), bool(hit))
    except Exception:                                       # noqa: BLE001
        return None


def _l2_put(k, rec):
    try:
        ts, items, hit = rec
        c = _l2_conn()
        c.execute("INSERT OR REPLACE INTO search_cache VALUES(?,?,?,?)",
                  (k, ts, 1 if hit else 0, json.dumps(items, ensure_ascii=False)))
        # 顺手清过期（每写一次顺手做，成本 O(1) 索引扫描）
        c.execute("DELETE FROM search_cache WHERE ts < ?",
                  (ts - max(_SEARCH_TTL_HIT, _SEARCH_TTL_MISS) * 2,))
        c.commit()
        c.close()
    except Exception:                                       # noqa: BLE001
        pass


def search_series(g, st, series):
    """返回 Series 搜索结果；空结果只表示搜索没命中，不据此断言无此剧。

    ## 按 (源, 剧名) 缓存（2026-11-13，配额黑洞修复）

    实测今日 2308 次源站请求里 **1476 次是搜索（64%）**，而只出了 287 集 ——
    平均每集要搜 5.1 次。原因是 `resolve_all()` 对**每一集**都要把全部源搜一遍：
    一部 20 集的剧，同一部剧名会被重复搜 20 次 × 8 个源，每次还要试最多 5 个片名变体。

    但「这个源有没有这部剧、它的 Series Id 是多少」在几小时内是**稳定的**，
    完全没有必要每集都问一遍。所以按 `(源label, 归一化剧名)` 缓存：
      · 命中（找到了）：TTL 6 小时 —— Series Id 不会凭空变
      · 未命中（没这部剧）：TTL 45 分钟 —— 比命中短得多，因为源站可能刚补上；
        若用 6 小时会让新上架的剧被忽略半天，那是真的少出片。

    收益：搜索请求从「每集 × 每源 × 最多5变体」降到「每剧 × 每源 × 最多5变体」，
    一部 20 集的剧省下约 95% 的搜索请求。省下来的配额直接变成可扫的剧数。
    """
    key = (getattr(st, "label", "?"),
           re.sub(r"[\s\-—–－_]+", "", (series or "").strip().lower()))
    now = time.time()
    k = key[0] + "\x1f" + key[1]
    cached = _SEARCH_CACHE.get(key)
    if cached:
        ts, items, is_hit = cached
        if now - ts < (_SEARCH_TTL_HIT if is_hit else _SEARCH_TTL_MISS):
            return items

    # L2 磁盘缓存（跨进程）
    l2 = _l2_get(k, now)
    if l2 is not None:
        _SEARCH_CACHE[key] = l2
        return l2[1]

    prefix = SG.ITEMS_PREFIX.get(st.label, "")
    items = []
    for query in title_variants(series):
        d = g.get(st, prefix + "/Items?" + urllib.parse.urlencode(
            {"SearchTerm": query, "Recursive": "true",
             "IncludeItemTypes": "Series", "Limit": 3,
             "Fields": "ProviderIds,ProductionYear"}), search=True)
        got = d.get("Items") or []
        if got:
            items = got
            break
    rec = (now, items, bool(items))
    _SEARCH_CACHE[key] = rec
    _l2_put(k, rec)
    return items


def resolve_all(g, series, season, ep, host_pref=None):
    """返回**所有**提供该集的源 [(st,item,size)]，按负载均衡顺序。
    改进#1（2026-11-10 自审）：原先只取第一个源，一旦该源直链 404 就整单失败；
    自动入库实测 6 次里 5 次是这种 404 → 改成"拿到候选列表，失败换下一个源"。"""
    out = []
    order = [s for s in g.order() if (not host_pref or s.label.startswith(host_pref))]
    order = _prefer_cached(order, series)
    for st in order:
        try:
            items = search_series(g, st, series)
        except Exception as e:                                     # noqa: BLE001
            print("  × %-24s 搜索失败 %s" % (st.label, str(e)[:40]))
            continue
        if not items:
            continue
        sid = items[0]["Id"]
        try:
            eps = _ep_get(g, st, "/Shows/%s/Episodes?Fields=MediaSources,RunTimeTicks&Season=%d" % (sid, season)).get("Items", [])
        except Exception:                                          # noqa: BLE001
            continue
        hit = [x for x in eps if x.get("IndexNumber") == ep]
        if not hit:
            continue
        e = hit[0]
        ms = (e.get("MediaSources") or [{}])[0]
        size = ms.get("Size") or 0
        print("  ✓ %-24s 命中 E%d  %s  %s" % (st.label, ep, human(size), e.get("Name")))
        out.append((st, e, size))
    return out


def resolve_candidates(g, series, season, ep, exclude=None, max_hosts=0, year=0,
                       order_labels=None):
    """Return candidate episodes with real quality, capped by source count.

    This is for the upgrade/new lanes, which must compare quality.  Fill uses
    resolve(), preserving the project's "first valid source" rule.
    """
    order = _ordered_sources(g, exclude, max_hosts, order_labels, series=series)
    out = []
    _hosts_with_hit = 0          # E132：真正「贡献了候选」的源数
    for st in order:
        try:
            items = search_series(g, st, series)
        except Exception as e:                                     # noqa: BLE001
            print("  × %-24s 搜索失败 %s" % (st.label, str(e)[:40]))
            continue
        if not items:
            continue
        target_norm = _norm_title(series)
        items.sort(key=lambda x: (
            0 if _norm_title(x.get("Name")) == target_norm else 1,
            abs(int(x.get("ProductionYear") or 0) - int(year or 0)) if year else 0,
            str(x.get("Id") or ""),
        ))
        sid = items[0]["Id"]
        provider = items[0].get("ProviderIds") or {}
        try:
            q = urllib.parse.urlencode({
                "Season": season,
                "Fields": "MediaSources,RunTimeTicks,ProviderIds",
            })
            eps = _ep_get(g, st, "/Shows/%s/Episodes?%s" % (sid, q)).get("Items", [])
        except Exception:                                          # noqa: BLE001
            continue
        hit = [x for x in eps if x.get("IndexNumber") == ep]
        if not hit:
            continue
        e = hit[0]
        quality = quality_of(e)
        # 2026-11-15（E132b）：**size=0 的候选既不能用、也不该占源名额。**
        # 实测 ibukifuko.tech / xqd 的 Episodes 不带 MediaSources（返回空数组），
        # 于是 quality.size=0 —— 这种候选永远过不了 fill_target 的 min_bytes，
        # 却会被计入 max_hosts 的「已拿到几个源」，把真正的源挤掉。
        # 而 `_prefer_cached()` 会把「缓存里有结果」的源排到前面，
        # 恰好把这两个脏源顶到队首 → 连续两个 size=0 → 直接 break → 整条 lane 饿死。
        if int(quality.get("size") or 0) <= 0:
            continue
        out.append({
            "st": st, "episode": e, "size": int(quality["size"] or 0),
            "series_id": sid, "series_name": items[0].get("Name") or series,
            "year": int(items[0].get("ProductionYear") or 0),
            "provider_ids": provider, "quality": quality,
        })
        _hosts_with_hit += 1
        if max_hosts and _hosts_with_hit >= int(max_hosts):
            break
        # 2026-11-14：已拿到 2160p 就不再比后面的源。
        # 这个函数的目的是"比画质挑最好"，而 rank=4 已经是不可能被超越的档位，
        # 继续列集只是烧配额（实测列集 214 次/小时 vs 每小时出 4 集）。
        # 代价：同为 2160p 时不再比较码率/HDR —— 对"有没有更好的版本"这个判断无影响，
        # upgrade lane 也只在现有是 1080p/720p 时才会来扫，4K 出现即是确定要换。
        if int(quality.get("rank") or 0) >= 4:
            break
    return out


def resolve_series_season(g, series, season, exclude=None, max_hosts=0, year=0,
                           order_labels=None):
    """Resolve a whole season once for the new-series lane.

    Returns source-series dicts with the episode list and real quality.  This
    avoids one search per episode when filling a newly discovered series.
    """
    order = _ordered_sources(g, exclude, max_hosts, order_labels)
    out = []
    for st in order:
        try:
            items = search_series(g, st, series)
        except Exception as e:                                     # noqa: BLE001
            print("  × %-24s 搜索失败 %s" % (st.label, str(e)[:40]))
            continue
        if not items:
            continue
        target_norm = _norm_title(series)
        items.sort(key=lambda x: (
            0 if _norm_title(x.get("Name")) == target_norm else 1,
            abs(int(x.get("ProductionYear") or 0) - int(year or 0)) if year else 0,
            str(x.get("Id") or ""),
        ))
        chosen = items[0]
        sid = chosen["Id"]
        try:
            q = urllib.parse.urlencode({
                "Season": season,
                "Fields": "MediaSources,RunTimeTicks,ProviderIds",
            })
            eps = _ep_get(g, st, "/Shows/%s/Episodes?%s" % (sid, q)).get("Items", [])
        except Exception:                                          # noqa: BLE001
            continue
        rows = []
        for e in eps:
            ep = e.get("IndexNumber")
            if ep is None:
                continue
            rows.append({
                "ep": int(ep), "episode": e, "size": int(
                    ((e.get("MediaSources") or [{}])[0].get("Size") or 0)),
                "quality": quality_of(e),
            })
        if rows:
            out.append({
                "st": st, "series_id": sid,
                "series_name": chosen.get("Name") or series,
                "year": int(chosen.get("ProductionYear") or 0),
                "provider_ids": chosen.get("ProviderIds") or {},
                "episodes": sorted(rows, key=lambda x: x["ep"]),
            })
    return out


def resolve_series_all(g, series, exclude=None, max_hosts=0, year=0,
                       order_labels=None, hint=None):
    """Resolve all seasons of a new series in one Episodes request per source.

    Emby's ``/Shows/{id}/Episodes`` endpoint returns the show's episodes and
    each row carries ``ParentIndexNumber``.  This avoids one season request per
    season while keeping exactly the same Guard/UA/throttle rules as
    ``resolve_series_season``.
    """
    # 2026-11-14：队列条目带着 source_label + source_series_id（建队列时从源站目录就知道了）。
    # 原来这两个字段只用来分片；解析时丢掉 ID 去按剧名重搜，
    # 搜不到就被判 no_source_probe 冷藏 3 天。实测今日新停泊 255 部全是这个原因，
    # 而那些剧正是从那个源的目录里拿到的。
    # 现在：有 hint 就直接 GET /Shows/{id}/Episodes，省掉一次搜索。
    hint_label, hint_sid = (hint or (None, None))
    # 2026-11-14：**先把可选源限制成点名源，再去排序**。
    # `_ordered_sources` 会先截断到 max_hosts 个源；如果点名源不在这 3 个里，
    # 后面再怎么"提到最前"也没用 —— 它根本不在列表里。
    # 结果就是去别家按剧名搜、搜不到 → hint_fetch_failed。
    # 实测：07:51 那轮 attempts=12，**12 次全是 hint_fetch_failed**。
    if hint_label and order_labels and hint_label in list(order_labels):
        order_labels = [hint_label]
    order = _ordered_sources(g, exclude, max_hosts, order_labels)
    if hint_label:
        order = sorted(order, key=lambda _s: 0 if _s.label == hint_label else 1)
        # 2026-11-14：**有点名 ID 就只试那一个源**。
        # 队列是从那个源的目录建的，它没有就是真没有；
        # 继续去别家按剧名搜既贵又不可靠（名字对不上），
        # 实测 new3 一轮 30 次尝试里 15 次走的就是这条贵路。
        # 失败后 30 分钟重试（hint_fetch_failed）比换源重搜更对。
        _hinted = [x for x in order if x.label == hint_label]
        if _hinted:
            order = _hinted
    out = []
    for st in order:
        chosen = None
        sid = str(hint_sid) if (hint_label and st.label == hint_label and hint_sid) else None
        if sid is None:
            try:
                items = search_series(g, st, series)
            except Exception as e:                                 # noqa: BLE001
                print("  × %-24s 搜索失败 %s" % (st.label, str(e)[:40]))
                continue
            if not items:
                continue
            target_norm = _norm_title(series)
            items.sort(key=lambda x: (
                0 if _norm_title(x.get("Name")) == target_norm else 1,
                abs(int(x.get("ProductionYear") or 0) - int(year or 0)) if year else 0,
                str(x.get("Id") or ""),
            ))
            chosen = items[0]
            sid = chosen["Id"]
        try:
            q = urllib.parse.urlencode({
                "Fields": "MediaSources,RunTimeTicks,ProviderIds",
                "Limit": "10000",
            })
            eps = _ep_get(g, st, "/Shows/%s/Episodes?%s" % (sid, q)).get("Items", [])
        except Exception:                                          # noqa: BLE001
            continue
        rows = []
        for e in eps:
            ep = e.get("IndexNumber")
            season = e.get("ParentIndexNumber")
            if ep is None or season is None:
                continue
            if int(season) <= 0 or int(ep) <= 0:
                continue
            rows.append({
                "season": int(season), "ep": int(ep), "episode": e,
                "size": int(((e.get("MediaSources") or [{}])[0].get("Size") or 0)),
                "quality": quality_of(e),
            })
        if rows:
            out.append({
                "st": st, "series_id": sid,
                # hint 路径没有搜索结果，直接用队列条目自带的剧名/年份。
                # （2026-11-14：第一版，没定义 chosen 就引用，NameError。）
                "series_name": (chosen.get("Name") if chosen else None) or series,
                "year": int((chosen.get("ProductionYear") if chosen else 0) or year or 0),
                "provider_ids": (chosen.get("ProviderIds") if chosen else None) or {},
                "episodes": sorted(rows, key=lambda x: (x["season"], x["ep"])),
            })
            # 2026-11-14：这个源已经有 2160p，就不必再问下一个源。
            # 和 resolve_candidates 同一个理由：rank=4 是画质上限，继续列集只是烧配额。
            # 实测每个决策走 3 个源、每次响应 100-300 KB，列集占请求量的 40%。
            if any(int((r.get("quality") or {}).get("rank") or 0) >= 4 for r in rows):
                break
    return out


def resolve(g, series, season, ep, host_pref=None, exclude=None, notes=None,
            order_labels=None):
    """兼容旧调用：只返回第一个候选。

    notes：可选列表，逐源记录未命中原因
    （series_missing / ep_missing / ep_list_fail / search_fail）。
    调用方据此区分「所有源都没有这部剧」与「有剧但缺这一集」——
    前者是系列级问题，逐集重试纯属浪费（11-10 实测 24 集连撞同一堵墙）。
    """
    order = _ordered_sources(g, exclude, 0, order_labels, host_pref)
    for st in order:
        try:
            items = search_series(g, st, series)
        except Exception as e:                                     # noqa: BLE001
            print("  × %-24s 搜索失败 %s" % (st.label, str(e)[:40]))
            if notes is not None:
                notes.append((st.label, "search_fail"))
            continue
        if not items:
            print("  - %-24s 搜索无命中（未判定无此剧；可能是别名/类型/权限）" % st.label)
            if notes is not None:
                notes.append((st.label, "series_search_empty"))
            continue
        sid = items[0]["Id"]
        try:
            eps = _ep_get(g, st, "/Shows/%s/Episodes?Fields=MediaSources,RunTimeTicks&Season=%d" % (sid, season)).get("Items", [])
        except Exception as e:                                     # noqa: BLE001
            print("  × %-24s 取集失败 %s" % (st.label, str(e)[:40]))
            if notes is not None:
                notes.append((st.label, "ep_list_fail"))
            continue
        hit = [x for x in eps if x.get("IndexNumber") == ep]
        if not hit:
            print("  - %-24s 该季 %d 集，无 E%d" % (st.label, len(eps), ep))
            if notes is not None:
                notes.append((st.label, "ep_missing"))
            continue
        e = hit[0]
        ms = (e.get("MediaSources") or [{}])[0]
        size = ms.get("Size") or 0
        print("  ✓ %-24s 命中 E%d  %s  %s" % (st.label, ep, human(size), e.get("Name")))
        return st, e, size
    return None, None, 0


def no_source_verdict(notes):
    """resolve 全部未命中时，判断是「整部剧都不在源上」还是「源有剧但缺这一集」。

    搜索空结果不是「无此剧」的**强**证据：源站可能使用别名、类型不同或
    当前 token 看不到条目。但它是**可用**证据 —— 只要多个源同时搜不到、
    且没有任何一个源见过这部剧（没有 ep_missing / ep_list_fail），
    就可以按系列级处理。

    ## 为什么必须把 series_search_empty 算进来（2026-11-15 根因）

    旧判据只认 `series_absent_confirmed`，而这个 key **全仓库从来没有被写入过**
    （`resolve()` 里搜索空结果记的是 `series_search_empty`）。于是 `absent` 恒为 0，
    函数恒返回 `no_source_episode`，返回码 6 / `no_source_series` 那条分支是死代码。
    实测后果：`.autoingest_failed.json` 里 4 部剧 40 集（射雕/山神/尚公主/当星光坠入花海）
    明明多个源都「无此剧」，却按集级判 7 天冷却；7 天到期后再逐集撞墙，
    每轮继续消耗源配额 —— 正是「重复扫描烧配额」的老问题换了个壳。

    判据同时要求「无任何源见过此剧」：只要有一个源返回了 ep_missing / ep_list_fail，
    说明剧在源上、只是缺集 → 必须留在集级（30 天集级记忆 + 继续等新集）。
    """
    absent = sum(1 for _, k in notes
                 if k in ("series_absent_confirmed", "series_search_empty"))
    present = sum(1 for _, k in notes if k in ("ep_missing", "ep_list_fail", "search_fail"))
    if absent >= 2 and present == 0:
        return "no_source_series"
    return "no_source_episode"


def pull(g, st, item, out, limit_mbps):
    ms = (item.get("MediaSources") or [{}])[0]
    total = ms.get("Size") or 0
    url = "%s/Videos/%s/stream?Static=true&api_key=%s" % (st.host, item["Id"], st.token)
    have = os.path.getsize(out + ".part") if os.path.exists(out + ".part") else 0
    if total and have >= total:
        print("  已有完整文件，跳过下载")
        return True
    for attempt in range(1, 4):
        try:
            headers = {"User-Agent": st.ua}
            if have:
                headers["Range"] = "bytes=%d-" % have
            req = urllib.request.Request(url, headers=headers)
            t0 = time.time()
            done = have
            with urllib.request.urlopen(req, timeout=120, context=SG._ctx) as r, open(out + ".part", "ab") as f:
                while True:
                    b = r.read(CHUNK)
                    if not b:
                        break
                    f.write(b)
                    done += len(b)
                    if limit_mbps:
                        expect = len(b) / (limit_mbps * 1024 * 1024)
                        spent = time.time() - t0
                        if spent < expect:
                            time.sleep(expect - spent)
                    if int(time.time() - t0) % 15 < 1:
                        pct = (done / total * 100) if total else 0
                        print("    %.1f%%  %s / %s  %.1f MB/s" % (pct, human(done), human(total),
                                                                  (done - have) / 1e6 / max(time.time() - t0, .1)),
                              flush=True)
            got = os.path.getsize(out + ".part")
            if total and got < total:
                # 2026-11-13 修：日志原来打 `human(got)/human(total)`，两个数四舍五入后
                # 完全一样（"5.35 GB / 5.35 GB"），根本看不出差多少。改成打**精确差值**。
                # 这个 bug 的真相就是差值小到看不见：源的 MediaSources.Size 比实际流大
                # 约 5 MB，于是 got 永远差一截，而下一轮 Range 从 got 开始必然 416。
                print("  ! 未完成 %s / %s（差 %s），重试 %d"
                      % (human(got), human(total), human(max(1, total - got)), attempt))
                have = got
                time.sleep(5)
                continue
            os.replace(out + ".part", out)
            print("  ✓ 下载完成 %s（%.0fs）" % (human(got), time.time() - t0))
            return True
        except Exception as e:                                     # noqa: BLE001
            code = getattr(e, "code", 0)
            if code == 416:
                # 2026-11-13 修：416 = "你请求的 offset 已在 EOF 之后" = **服务器那边我们
                # 已经拿全了**。这跟 `total`（源声明的 Size）没有关系——源经常虚报大小。
                # 旧代码要求 `got >= total` 才收尾，于是"源虚报 5MB"就变成永远失败：
                # 5.35GB 白下、重试 3 次全 416、最后判"下载失败"，还占掉源配额。
                # 实测案例：pull_e3.log（5.35GB 卡 91.9%）、pull_e8.log（10.44GB 卡 96.7%）。
                got = os.path.getsize(out + ".part") if os.path.exists(out + ".part") else 0
                if got > 0:
                    os.replace(out + ".part", out)
                    print("  ✓ 下载完成（416 收尾，源声明 %s / 实得 %s）"
                          % (human(total or 0), human(got)))
                    return True
            print("  × 第 %d 次失败：%s" % (attempt, str(e)[:60]))
            have = os.path.getsize(out + ".part") if os.path.exists(out + ".part") else 0
            time.sleep(5)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--season", type=int, default=1)
    ap.add_argument("--ep", type=int, required=True)
    ap.add_argument("--host", help="源标签前缀，如 goemby / aaa.yusen6")
    ap.add_argument("--out", help="输出文件路径（默认 /opt/embytg/tmp/<剧>-SxxExx.mkv）")
    ap.add_argument("--install", help="下载后替换到这个库内路径（旧文件移到 _trash_<ts>/）")
    ap.add_argument("--limit-mbps", type=float, default=0.0,
                    help="0=不限速（默认）；>0 时按 MB/s 限速")
    ap.add_argument("--apply", action="store_true",
                    help="真正下载；不加 --apply 就是干跑（--dry 为兼容别名）")
    ap.add_argument("--dry", action="store_true", help="干跑（等同不加 --apply）")
    a = ap.parse_args()

    os.makedirs(TMP, exist_ok=True)
    g = SG.Guard()
    print("== 解析源 ==")
    notes = []
    # 命中即止：先找第一个可用源；只有下载失败才排除它再找下一个。
    # 不能像旧版 resolve_all 那样为了拿候选列表把 8 个源全部搜一遍。
    st, item, size = resolve(g, a.series, a.season, a.ep, a.host, notes=notes)
    if not st:
        # 区分两种「没源」：整部剧都不在任何源（系列级，逐集重试无意义）
        # vs 源有这部剧但缺这一集（集级，30 天后再试）。
        verdict = no_source_verdict(notes)
        if verdict == "no_source_series":
            print("所有源都没有这部剧")
            print("RESULT=no_source_series")
            return 6
        print("没有源提供这一集")
        print("RESULT=no_source_episode")
        return 4
    if a.limit_mbps and a.limit_mbps > 0:
        limit_mbps = a.limit_mbps
    else:
        limit_mbps = 0

    d_gb, _, m_gb, _ = Q.usage()
    c = Q.cfg()
    left = min(c["daily_gb"] - d_gb, c["safety_gb"] - m_gb)
    need = size / 1e9
    print("\n== 预算 ==\n  今日 %.1f/%.0f GB  本月 %.1f/%.0f GB  剩 %.1f GB  本次需要 %.2f GB"
          % (d_gb, c["daily_gb"], m_gb, c["safety_gb"], left, need))
    if size <= 0:
        print("  ⚠ 源未返回 Size，无法预检单文件上限；落地后按实际大小记账")
    if need > left:
        print("  ⛔ 超限，停止（换 1080p 或等次日）")
        print("RESULT=budget_blocked")
        return 3

    out = a.out or os.path.join(TMP, "%s-S%02dE%02d.mkv" % (a.series.replace("/", "-"), a.season, a.ep))
    print("  输出 %s" % out)
    if not a.apply:
        print("\n[干跑] 未下载。确认后加 --apply。")
        return 0

    ok = False
    excluded = set()
    for idx in range(3):
        if idx:
            st, item, size = resolve(g, a.series, a.season, a.ep, a.host, excluded)
            if not st:
                break
            print("  ↻ 换源重试：%s（%s）" % (st.label, human(size)))
            d_gb, _, m_gb, _ = Q.usage()
            left2 = min(c["daily_gb"] - d_gb, c["safety_gb"] - m_gb)
            if size / 1e9 > left2:
                print("  ⛔ 换源后大小 %.2fGB 超过剩余 %.1fGB，停止" % (size / 1e9, left2))
                break
        if pull(g, st, item, out, limit_mbps):
            ok = True
            break
        if os.path.exists(out + ".part"):
            os.remove(out + ".part")     # 换源前清半截，避免下一个源 Range 越界
        excluded.add(st.label)
    if not ok:
        print("所有源都失败（直链不可用）")
        print("RESULT=direct_link_fail")
        return 5

    if a.install:
        ts = time.strftime("%Y%m%d_%H%M%S")
        trash = os.path.join("/opt/embytg/pipeline", "_trash_%s" % ts)
        os.makedirs(trash, exist_ok=True)
        if os.path.exists(a.install):
            old = os.path.join(trash, os.path.basename(a.install))
            os.rename(a.install, old)
            print("  旧文件 → %s" % old)
        os.makedirs(os.path.dirname(a.install), exist_ok=True)
        shutil.copyfile(out, a.install + ".part")
        os.replace(a.install + ".part", a.install)
        print("  ✓ 已装入 %s" % a.install)
        # 装在 teldrive 挂载内的文件会由 resource_state --scan 记账，
        # 再写 .ingest_manual.jsonl 会与 tg.db 重复计费（历史 15.657GB 重复已证实）。
        # 只有落到挂载外的临时/人工路径才写 manual 账。
        if not a.install.startswith("/opt/embytg/mnt/embytg/"):
            try:
                try:
                    real = os.path.getsize(a.install)
                except OSError:
                    real = 0
                gb_rec = round((real if real > 0 else size) / 1e9, 3)
                with open("/opt/embytg/pipeline/.ingest_manual.jsonl", "a", encoding="utf-8") as lf:
                    lf.write(json.dumps({"ts": time.strftime("%F %T"), "gb": gb_rec,
                                         "what": "%s S%02dE%02d" % (a.series, a.season, a.ep),
                                         "src": st.label}, ensure_ascii=False) + "\n")
            except Exception:                                      # noqa: BLE001
                pass
        else:
            print("  teldrive 内落地：由 resource_state 记账，跳过 manual 账本")
        os.remove(out)
        print("  临时文件已清理")
    print("\n%s" % g.summary())
    print("RESULT=ok")
    return 0


if __name__ == "__main__":
    if "--cache-stats" in sys.argv:
        st = cache_stats()
        hit = st.get("ep_hit", 0)
        miss = st.get("ep_miss", 0)
        tot = hit + miss
        print("列集缓存：命中 %d / 未命中 %d / 命中率 %.1f%%"
              % (hit, miss, (100.0 * hit / tot) if tot else 0.0))
        print("  活行数 %s / 负缓存行 %s / 写入错误 %s / 批量清理 %s"
              % (st.get("ep_rows", "?"), st.get("ep_err_rows", "?"),
                 sum(v for k, v in st.items() if k.startswith("ep_put_err:")),
                 st.get("ep_purge_bulk", 0)))
        for k in sorted(st):
            if not k.startswith("ep_") and k not in ("ep_rows", "ep_err_rows"):
                print("  %s: %d" % (k, st[k]))
        sys.exit(0)
    sys.exit(main())
