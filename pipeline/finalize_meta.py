#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""队列消费完成后的收尾：全量中文刮削兜底 + 去重/挂载复核。

用法（在 /opt/embytg/pipeline 下）:
  python3 finalize_meta.py --audit                 # 只体检，不刷新
  python3 finalize_meta.py --refresh              # 并发 FullRefresh 元数据不全的 Movie/Series
  python3 finalize_meta.py --refresh --all        # 全部 Movie/Series 强制 FullRefresh
  python3 finalize_meta.py --refresh --verify     # 刷完复查覆盖率
  python3 finalize_meta.py --wait                 # 等 Emby 异步扫描队列排空（总数稳定）

设计前提：
  - 18 个中文库 TypeOptions 已由 fix_fetchers.py 补全，provider 正常。
  - 队列登记速度 > Emby ffprobe+provider 速度，扫描期间建的条目 Genres/Overview 会暂时为空。
  - /Library/Refresh 对「已存在且未变化」的条目不重跑 provider，所以收尾必须逐条 FullRefresh 兜底。
"""
import json, os, re, sys, time, subprocess, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

from embytg_conf import cfg  # 统一配置真源: /opt/embytg/config.json

E = cfg("emby_url")
T = cfg("emby_token")
HDR = {"X-Emby-Token": T, "Content-Type": "application/json"}


def api(path, method="GET", body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else (b"{}" if method == "POST" else None)
    r = urllib.request.Request(E + path, method=method, headers=HDR, data=data)
    with urllib.request.urlopen(r, timeout=timeout) as f:
        b = f.read()
        try:
            return json.loads(b)
        except Exception:
            return {}


def paged(types, fields, page=500):
    out, start = [], 0
    while True:
        d = api("/Items?Recursive=true&IncludeItemTypes=%s&Fields=%s&StartIndex=%d&Limit=%d"
                % (types, urllib.parse.quote(fields, safe=","), start, page))
        items = d.get("Items", [])
        out.extend(items)
        start += len(items)
        total = d.get("TotalRecordCount") or 0
        if not items or start >= total:
            break
    return out


FIELDS = "Name,Genres,Overview,People,ProviderIds,Path,Type,ParentIndexNumber,IndexNumber,MediaStreams"


def q_of(name):
    m = re.search(r"(2160p|1080p|720p)", name or "", re.I)
    if m:
        return m.group(1).lower()
    if re.search(r"2160|4K", name or "", re.I):
        return "2160p"
    if "1080" in (name or ""):
        return "1080p"
    if "720" in (name or ""):
        return "720p"
    return "*"


def needs(x):
    """判"元数据不全"：要求有 Tmdb ID 且至少刮出 Overview 或 Genres 之一。
    TMDB 对冷门片常无 genres（genres=[]），只要有 Overview 或 genres 任一即算全；
    缺 Tmdb ID 才是真正未匹配（TODB-only 片 Emby 无 provider，需 NFO 补刮）。"""
    if x.get("Type") not in ("Movie", "Series"):
        return False
    return not ((x.get("ProviderIds") or {}).get("Tmdb")
                and ((x.get("Overview")) or (x.get("Genres"))))


def full_refresh(iid):
    try:
        api("/Items/%s/Refresh?MetadataRefreshMode=FullRefresh&ImageRefreshMode=FullRefresh"
            "&ReplaceAllMetadata=true&ReplaceAllImages=false" % iid, "POST", {}, timeout=180)
        return (iid, True, "")
    except Exception as e:
        return (iid, False, str(e)[:120])


def counts():
    out = {}
    for t in ("Movie", "Series", "Episode"):
        d = api("/Items?Recursive=true&IncludeItemTypes=%s&Limit=1" % t)
        out[t] = d.get("TotalRecordCount") or 0
    return out


def db_dup_check():
    """teldrive DB 层同画质双份复核：同一 (tmdb id 或 归一标题) + 画质 + SxxExx 只应有一份。"""
    # 只统计媒体文件：NFO 和 poster 不是可入库的剧集/电影，排除否则产生假重复
    # （曾把 4532 张 poster.jpg 算成媒体 → 误报「同画质双份 4531 处」）
    sql = ("select name from teldrive.files where kind='file' and status='active' "
           "and lower(name) not like '%.nfo' "
           "and lower(name) not like '%.jpg' "
           # 隔离/去重标记不是媒体：`.bad`（坏件隔离，E-bad）与 `.dup`（E015 同集多画质
           # 留高删低时给低画质文件的后缀）都不进 Emby，算进「同画质双份」= 永久假 FAIL。
           "and lower(name) not like '%.bad' "
           "and lower(name) not like '%.dup' "
           "and lower(name) not like '%.jpeg' "
           "and lower(name) not like '%.png' "
           "and lower(name) not like '%.webp'")
    try:
        r = subprocess.run(
            ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive",
             "-tAc", sql], capture_output=True, text=True, timeout=180)
        names = [x for x in r.stdout.splitlines() if x.strip()]
    except Exception as e:
        return "DB 查询失败: %s" % str(e)[:120], []
    seen, dups = {}, []
    for n in names:
        mm = re.search(r"tmdb(?:id)?[-=](\d+)", n, re.I)
        anchor = "id:" + mm.group(1) if mm else "n:" + re.split(r"\s*[（(]\d{4}|\s*[{[]tmdb|\s+S\d{2}E\d+", n)[0].strip()
        sem = re.search(r"S(\d{2})E(\d+)", n)
        tail = (int(sem.group(1)), int(sem.group(2))) if sem else ()
        k = (anchor, tail, q_of(n))
        if k in seen:
            dups.append((seen[k], n))
        else:
            seen[k] = n
    return "文件 %d 个，唯一条目 %d 个，同画质双份 %d 处" % (len(names), len(seen), len(dups)), dups[:20]


def audit(items=None):
    if items is None:
        items = paged("Movie,Series,Episode", FIELDS)
    ms = [x for x in items if x.get("Type") in ("Movie", "Series")]
    eps = [x for x in items if x.get("Type") == "Episode"]
    bad = [x for x in ms if needs(x)]
    no_pid = [x for x in ms if not (x.get("ProviderIds") or {}).get("Tmdb")]
    series = [x for x in ms if x.get("Type") == "Series"]
    print("# 体检: %d 条（Movie/Series %d, Series %d, Episode %d）" % (len(items), len(ms), len(series), len(eps)))
    print("# 元数据不全 %d / 已全刮 %d（缺 TMDB ID %d）" % (len(bad), len(ms) - len(bad), len(no_pid)))
    if bad[:15]:
        print("# 待补刮样例:")
        for x in bad[:15]:
            print("   %-8s id=%-6s %-24s g=%s" % (x.get("Type"), x["Id"], (x.get("Name") or "")[:24], x.get("Genres")))
    msg, dups = db_dup_check()
    print("# 去重复核: " + msg)
    for a, b in dups:
        print("   DUP %s  <->  %s" % (a[:60], b[:60]))
    # Series/Episode 挂载
    # Season 00（花絮/超前营业）在 Emby 里 ParentIndexNumber 就是 0 —— 旧判据用
    # `not x.get(...)` 把 0 当成缺失，实测 101 条全部是综艺 S00 特辑，属**假 FAIL**
    # （2026-11-23 周度体检：按库逐条查 SeasonNumber/EpisodeNumber 缺失 = 0）。
    orphan = [x for x in eps
              if x.get("ParentIndexNumber") is None or x.get("IndexNumber") is None]
    print("# 挂载复核: Episode 缺季/集号 %d 条" % len(orphan))
    return bad


def wait_stable(rounds=6, gap=30, stable=3):
    print("# 等待 Emby 异步扫描排空（每 %ds 采样，连续 %d 次不变视为稳定）" % (gap, stable), flush=True)
    hist = []
    for i in range(rounds):
        c = counts()
        hist.append(c)
        print("   [%d] %s" % (i, c), flush=True)
        if len(hist) >= stable and all(hist[-1] == hist[-j - 2] for j in range(stable - 1)):
            print("# 扫描已稳定", flush=True)
            return c
        time.sleep(gap)
    print("# 达到采样上限，返回最新值", flush=True)
    return hist[-1]


def main():
    a = sys.argv[1:]
    force_all = "--all" in a
    do_refresh = "--refresh" in a
    do_verify = "--verify" in a
    do_wait = "--wait" in a
    workers = 16
    for i, x in enumerate(a):
        if x == "--workers" and i + 1 < len(a):
            workers = int(a[i + 1])

    if do_wait:
        wait_stable()

    print("# 拉取全部 Movie/Series/Episode ...", flush=True)
    items = paged("Movie,Series,Episode", FIELDS)
    bad = audit(items)
    ms = [x for x in items if x.get("Type") in ("Movie", "Series")]

    if not do_refresh:
        return
    todo = ms if force_all else bad
    if not todo:
        print("# 无需补刮（全部已刮全）", flush=True)
    else:
        print("# 并发 FullRefresh %d 条 (workers=%d) ..." % (len(todo), workers), flush=True)
        ok = fail = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(full_refresh, x["Id"]): x for x in todo}
            for n, f in enumerate(as_completed(futs), 1):
                iid, good, err = f.result()
                if good:
                    ok += 1
                else:
                    fail += 1
                    print("   ! %s %s" % (iid, err), file=sys.stderr, flush=True)
                if n % 200 == 0:
                    print("   .. %d/%d  ok=%d fail=%d  %.1fs" % (n, len(todo), ok, fail, time.time() - t0), flush=True)
        print("# FullRefresh 完成 ok=%d fail=%d / %.1fs" % (ok, fail, time.time() - t0), flush=True)

    if do_verify:
        time.sleep(20)   # provider 队列异步落库
        print("# 复查覆盖率 ...", flush=True)
        items2 = paged("Movie,Series,Episode", FIELDS)
        audit(items2)


if __name__ == "__main__":
    main()
