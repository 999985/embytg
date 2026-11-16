#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""web_lookup.py —— 用搜索 API 反查影视条的 TMDB id。

使用场景：频道里的中文译名（「老千4别西卜之歌」「爱的修学旅行第二季」）
在 TMDB 中文库里搜不到，双源准入只能淘汰。本工具先走搜索引擎找到
TMDB / 豆瓣页面，从 URL 里抠出 tmdb id，再回 TMDB 官方 API 确认真实性。

设计原则：
  1. **搜索只用来「猜 id」，最终必须由 TMDB API 确认** —— 搜索结果不可信，
     写进片库的 id 必须是 TMDB 亲口承认的。
  2. 拿到 id 后回查 `/tv/{id}` 或 `/movie/{id}`，标题必须能对上才采纳，
     否则一律拒绝（宁可不上，不可上错）。
  3. 结果写缓存，同一标题不重复烧搜索额度。

用法：
  python3 web_lookup.py "老千4别西卜之歌"
  python3 web_lookup.py --kind tv "爱的修学旅行"
  python3 web_lookup.py --json "走马上任"
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".web_lookup_cache.json")

BRAVE_KEY = os.environ.get("BRAVE_API_KEY", "BSAxS7uglKHZsqxryrrpqRx_DSIHhVQ")
# 2026-11-14：搜索 API 是**月度配额**，用户明确「不能一直用」。
# 这里做三层节流，确保不会静默烧光：
#   ① 月度硬上限（默认 400 次，按套餐自行调）
#   ② 负数缓存 30 天（搜不到的不再重搜）
#   ③ 只在双源准入失败时才调用（调用点已在 tgpipe 控制）
QUOTA_FILE = os.path.join(HERE, ".web_lookup_quota.json")


def _monthly_cap():
    """月度搜索额度上限。

    2026-11-14：与 E082 同一个坑 —— **不能只读环境变量**。
    tgpipe 由 run_one.sh 拉起时环境里有，但手工跑 / cron 跑就没有，
    于是「改了 runner.env 但没生效」。环境变量优先，读不到就自读 runner.env。
    """
    v = os.environ.get("WEB_LOOKUP_MONTHLY_CAP", "").strip()
    if not v:
        for f in ("/opt/embytg-runner/runner.env",
                  os.path.join(HERE, "config", "web_lookup.env")):
            try:
                for line in open(f, encoding="utf-8"):
                    line = line.strip()
                    if line.startswith("WEB_LOOKUP_MONTHLY_CAP="):
                        v = line.split("=", 1)[1].strip().strip('"').strip("'")
                        raise StopIteration
            except StopIteration:
                break
            except Exception:                              # noqa: BLE001
                continue
    try:
        return max(1, int(v))
    except (TypeError, ValueError):
        return 200                      # 保守默认：额度是月度配额，宁可低


MONTHLY_CAP = _monthly_cap()
FIRECRAWL_KEY = os.environ.get("FIRECRAWL_API_KEY", "REPLACE_ME")
TMDB_TOKEN = os.environ.get("TMDB_READ_TOKEN") or os.environ.get("TMDB_API_KEY", "")

# TMDB 凭据：与项目其它模块走同一个真源（config.json 经 embytg_conf 读取），
# 不要在本文件里硬编码或另起解析，否则会和 tgpipe/nfo_gen 读到不同的值。
def _tmdb_cred():
    if TMDB_TOKEN:
        return TMDB_TOKEN
    try:
        sys.path.insert(0, HERE)
        from embytg_conf import cfg
        return str(cfg("tmdb_token") or "")
    except Exception:                                      # noqa: BLE001
        return ""


UA_WEB = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def http_json(url, headers=None, data=None, timeout=25):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


# ---------------- 搜索后端 ----------------
def search_brave(q, count=8):
    """Brave：额度大，返回 url + title + description。"""
    u = "https://api.search.brave.com/res/v1/web/search?" + urllib.parse.urlencode(
        {"q": q, "count": count})
    try:
        d = http_json(u, {"Accept": "application/json",
                          "X-Subscription-Token": BRAVE_KEY})
    except Exception as e:                                 # noqa: BLE001
        return [], f"brave: {type(e).__name__}"
    out = []
    for r in ((d.get("web") or {}).get("results") or []):
        out.append({"url": r.get("url") or "",
                    "title": r.get("title") or "",
                    "desc": ((r.get("description") or "")
                             + " " + (r.get("extra_snippets") or [""])[0])[:300]})
    return out, ""


FEEDCOOP_KEY = os.environ.get(
    "FEEDCOOP_API_KEY", "3I7D2eIt3jtyfAR6Aaz3CAjLsODMhOzP")
FEEDCOOP_URL = "https://open.feedcoopapi.com/search_api/web_search"


def search_feedcoop(q, count=8):
    """Feedcoop（如意/伏羲）web_search。

    2026-11-14：**参数名是 `Query` / `SearchType`，大小写敏感** ——
    试过 q/query/search_type 全部返回
    `{"Error":{"Message":"query or search type is empty"}}`，
    看着像鉴权失败，其实是参数名不对。响应外层是
    `{"Result": {"ResultCount": N, "WebResults": [...]}}`，
    每条有 Title / Url / Snippet / Summary / Content。
    """
    body = json.dumps({"Query": q, "SearchType": "web",
                       "Count": max(1, min(20, count))}).encode()
    try:
        d = http_json(FEEDCOOP_URL,
                      {"Authorization": "Bearer " + FEEDCOOP_KEY,
                       "Content-Type": "application/json"}, body, timeout=30)
    except Exception as e:                                 # noqa: BLE001
        return [], f"feedcoop: {type(e).__name__}"
    err = ((d.get("ResponseMetadata") or {}).get("Error") or {})
    if err:
        return [], "feedcoop: %s" % str(err.get("Message"))[:60]
    out = []
    for r in ((d.get("Result") or {}).get("WebResults") or []):
        out.append({"url": r.get("Url") or "",
                    "title": r.get("Title") or "",
                    "desc": (str(r.get("Snippet") or "")
                             + " " + str(r.get("Summary") or ""))[:300]})
    return out, ""


def search_firecrawl(q, limit=8):
    """Firecrawl：直接返回 TMDB 页面 URL，最适合反查 id。"""
    body = json.dumps({"query": q, "limit": limit}).encode()
    try:
        d = http_json("https://api.firecrawl.dev/v1/search",
                      {"Authorization": "Bearer " + FIRECRAWL_KEY,
                       "Content-Type": "application/json"}, body)
    except Exception as e:                                 # noqa: BLE001
        return [], f"firecrawl: {type(e).__name__}"
    if not d.get("success"):
        return [], "firecrawl: not success"
    out = []
    for r in (d.get("data") or []):
        out.append({"url": r.get("url") or "",
                    "title": r.get("title") or "",
                    "desc": (r.get("description") or "")[:300]})
    return out, ""


# ---------------- 从结果里抠 tmdb id ----------------
_RE_TMDB = re.compile(r"themoviedb\.org/(tv|movie)/(\d+)")
_RE_TMDB_CN = re.compile(r"themoviedb\.org/(tv|movie)/(\d+)-", re.I)


def extract_ids(results):
    """返回 [(kind, id)]，按出现顺序去重。"""
    seen, out = set(), []
    for r in results:
        u = r.get("url") or ""
        for m in _RE_TMDB.finditer(u):
            kind, tid = m.group(1), int(m.group(2))
            k = (kind, tid)
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out


# ---------------- TMDB 确认 ----------------
def tmdb_verify(kind, tid):
    """拿 TMDB 该条目的真实标题；不存在返回 None。"""
    tok = _tmdb_cred()
    if not tok:
        return None
    base = "https://api.themoviedb.org/3/%s/%d?language=zh-CN" % (
        "tv" if kind == "tv" else "movie", tid)
    for hdr in ({"Authorization": "Bearer " + tok},
                {"Accept": "application/json"}):
        u = base if "Authorization" in hdr else base + (
            "&api_key=" + tok if "?" in base else "?api_key=" + tok)
        try:
            d = http_json(u, hdr)
            if d and not d.get("__err__") and (d.get("name") or d.get("title")):
                return d
        except Exception:                                  # noqa: BLE001
            continue
    return None


def tmdb_search_by_name(name, kind=None, year=None):
    """用 TMDB 自己的 /search 端点按名字找 id。

    feedcoop 常返回豆瓣页，里面带「中文名 原名 (年份)」，
    把原名抠出来丢给 TMDB 搜索，命中率比直接搜中文高得多。
    """
    tok = _tmdb_cred()
    if not tok or not name:
        return None
    paths = []
    if kind == "tv":
        paths = ["tv"]
    elif kind == "movie":
        paths = ["movie"]
    else:
        paths = ["tv", "movie"]
    for kind_path in paths:
        q = {"query": name, "language": "zh-CN"}
        if year:
            q["first_air_date_year" if kind_path == "tv" else "year"] = year
        u = "https://api.themoviedb.org/3/search/%s?%s" % (
            kind_path, urllib.parse.urlencode(q))
        try:
            d = http_json(u, {"Authorization": "Bearer " + tok})
        except Exception:                                  # noqa: BLE001
            continue
        for r in (d.get("results") or [])[:3]:
            nm = r.get("name") or r.get("title") or ""
            if title_match(name, {"name": nm, "title": nm,
                                  "original_name": r.get("original_name"),
                                  "original_title": r.get("original_title")}):
                return {"kind": kind_path, "tmdb_id": int(r.get("id")),
                        "name": nm,
                        "orig": r.get("original_name") or r.get("original_title"),
                        "year": (r.get("first_air_date") or r.get("release_date") or "")[:4]}
    return None


_RE_ORIG = re.compile(r"([A-Za-z][A-Za-z0-9 '\-:,.!?&]{4,}?)\s*(?:\(|（)(\d{4})")


def orig_from_result(r):
    """从搜索结果标题里抠「英文原名 + 年份」。"""
    for txt in (r.get("title") or "", r.get("desc") or ""):
        m = _RE_ORIG.search(txt)
        if m:
            return m.group(1).strip(" -:,"), m.group(2)
    return None, None


def norm(s):
    s = str(s or "").lower()
    s = re.sub(r"[（(].*?[)）]", "", s)
    s = re.sub(r"[^\w\u4e00-\u9fff]+", "", s)
    return s


def title_match(query, d):
    """TMDB 标题必须与搜索词有实质重合，避免「搜 A 得 B」。"""
    q = norm(query)
    if not q:
        return False
    names = [d.get("name"), d.get("title"),
             d.get("original_name"), d.get("original_title")]
    for n in names:
        nn = norm(n)
        if not nn:
            continue
        if q == nn or q in nn or nn in q:
            return True
        # 取最长公共子串长度做宽松判定（中文译名常有出入）
        for ln in range(min(len(q), len(nn)), 2, -1):
            for i in range(len(q) - ln + 1):
                if q[i:i + ln] in nn:
                    if ln >= max(3, len(q) * 0.5):
                        return True
                    break
    return False


# ---------------- 缓存 ----------------
def _month():
    return time.strftime("%Y-%m")


def _quota_load():
    try:
        d = json.load(open(QUOTA_FILE, encoding="utf-8"))
    except Exception:                                      # noqa: BLE001
        d = {}
    m = _month()
    if d.get("month") != m:
        d = {"month": m, "used": 0}
    return d


def _quota_take(n=1):
    """申请 n 次搜索额度。超额返回 False（调用方必须放弃，不许绕过）。"""
    d = _quota_load()
    used = int(d.get("used") or 0)
    if used + n > MONTHLY_CAP:
        return False
    d["used"] = used + n
    tmp = QUOTA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, QUOTA_FILE)
    return True


def quota_status():
    d = _quota_load()
    return {"month": d.get("month"), "used": int(d.get("used") or 0),
            "cap": MONTHLY_CAP, "left": max(0, MONTHLY_CAP - int(d.get("used") or 0))}


def _load_cache():
    try:
        return json.load(open(CACHE, encoding="utf-8"))
    except Exception:                                      # noqa: BLE001
        return {}


def _save_cache(d):
    tmp = CACHE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, CACHE)


def lookup(query, kind=None, use_cache=True):
    """搜一个标题，返回 TMDB 确认过的条目，找不到返回 None。"""
    key = ("%s|%s" % (kind or "any", query)).strip()
    cache = _load_cache()
    if use_cache and key in cache:
        c = cache[key]
        age = time.time() - float(c.get("ts") or 0)
        # 命中缓存直接返回（正负结果都算），30 天内不重搜 —— 省额度
        if c.get("tmdb_id") and age < 30 * 86400:
            return c
        if not c.get("tmdb_id") and age < 30 * 86400:
            return None

    # 2026-11-14：月度配额硬闸。用完就停，不静默烧额度。
    if not _quota_take(1):
        st = quota_status()
        print("# web_lookup 月度额度已用尽（%d/%d），本轮跳过搜索兜底"
              % (st["used"], st["cap"]), file=sys.stderr)
        return None

    q = query
    if kind == "tv":
        q += " 电视剧 tmdb"
    elif kind == "movie":
        q += " 电影 tmdb"
    else:
        q += " tmdb"

    results = []
    errs = []
    # 后端顺序：feedcoop → firecrawl → brave。
    # 拿到 TMDB 候选就停，不烧第二个后端的额度。
    for fn, name in ((search_feedcoop, "feedcoop"),
                     (search_firecrawl, "firecrawl"),
                     (search_brave, "brave")):
        r, err = fn(q)
        if err:
            errs.append(err)
        results.extend(r)
        if extract_ids(results):
            break                     # 拿到候选就不再烧第二个后端

    if not results:
        cache[key] = {"query": query, "kind": kind, "tmdb_id": None,
                      "ts": time.time(), "err": "; ".join(errs) or "no results"}
        _save_cache(cache)
        return None

    # 2026-11-14：搜索常给豆瓣页而不是 TMDB 页（feedcoop 尤其如此）。
    # 豆瓣标题里通常带「中文名 原名 (年份)」，抠出原名丢给 TMDB 自己的
    # /search 端点 —— 用英文原名搜的命中率远高于中文译名。
    for _r in results[:6]:
        oname, oyear = orig_from_result(_r)
        if not oname:
            continue
        hit = tmdb_search_by_name(oname, kind, oyear)
        if not hit:
            continue
        rec = {"query": query, "kind": hit["kind"], "tmdb_id": hit["tmdb_id"],
               "name": hit["name"], "orig": hit.get("orig"),
               "year": hit.get("year") or "", "ts": time.time(),
               "via": "orig_name"}
        cache[key] = rec
        _save_cache(cache)
        return rec

    for k, tid in extract_ids(results):
        if kind and k != kind:
            continue
        d = tmdb_verify(k, tid)
        if not d:
            continue
        if not title_match(query, d):
            continue
        rec = {"query": query, "kind": k, "tmdb_id": tid,
               "name": d.get("name") or d.get("title"),
               "orig": d.get("original_name") or d.get("original_title"),
               "year": (d.get("first_air_date") or d.get("release_date") or "")[:4],
               "ts": time.time()}
        cache[key] = rec
        _save_cache(cache)
        return rec

    cache[key] = {"query": query, "kind": kind, "tmdb_id": None,
                  "ts": time.time(), "err": "candidates=%d 无一通过标题校验"
                  % len(extract_ids(results))}
    _save_cache(cache)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query")
    ap.add_argument("--kind", choices=["tv", "movie"])
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--no-cache", action="store_true")
    a = ap.parse_args()

    r = lookup(a.query, a.kind, use_cache=not a.no_cache)
    if a.json:
        print(json.dumps(r or {"tmdb_id": None}, ensure_ascii=False))
        return 0 if r else 1
    if not r:
        print("未找到（TMDB 无确认条目）")
        return 1
    print("TMDB %s/%d  %s (%s)" % (r["kind"], r["tmdb_id"], r["name"], r["year"]))
    if r.get("orig") and r["orig"] != r["name"]:
        print("  原名: %s" % r["orig"])
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
