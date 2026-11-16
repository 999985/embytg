#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 TG 挂载上已生成的 NFO（<plot> / <genre>）直接写进 Emby 条目的 Overview/Genres。

为什么需要这个脚本（E007，2026-11-09 实证）：
  - 本环境 RefreshLibrary State=Idle 后，`POST /Items/{Id}/Refresh?...FullRefresh`
    返回 204 但 provider 阶段根本不跑：`RefreshItem Start` 累计仅 3、
    `Running SeriesNfoProvider`=0、`Running MovieDbSeriesProvider`=0，
    日志只有 397 条 `Refresh metadata requested`。
    → finalize_meta.py --refresh 对「有 Tmdb 但 Genres/Overview 空」的 Series 完全无效。
  - 官方社区（topic/125696）Luke：POST /Items/{Id} 要传完整对象，否则未给字段会被 null 化；
    「Fetch it first with a get request, then update the properties you need」。
  - NFO 在 ingestion 时读、Refresh Metadata 时在线 provider 优先（topic/129626）。
    既然 NFO 已经生成好且是唯一真源，直接写库是规模无关的确定性做法（本地 IO + 分页并发）。

做法（SCALING.md §1：分页流式 / 增量 / 不拉全库进内存 / 不硬编码规模常量）：
  - 分页遍历 Movie + Series，用 `needs()` 同款判据筛「元数据不全」；
  - 对每条从 NFO 解析 <plot>/<genre>（movie: <媒体名>.nfo；tv: <Series目录>/tvshow.nfo）；
  - GET /Users/{adminUid}/Items/{id} 取完整对象 → 覆盖 Overview/Genres → POST /Items/{id}；
  - 只处理 NFO 里真有 plot 的条目（TODB-only 无 NFO 的跳过，交 AUDIT_BAD_MAX 容忍）；
  - 并发 ThreadPoolExecutor，默认 16 workers（对齐 finalize_meta.py）；
  - 不改任何 NFO / 不重启 Emby / 不碰库文件；失败只记录，不回滚。

用法（服务器 cd /opt/embytg/pipeline）：
  python3 nfo_to_emby.py --dry             # 只报清单，不写
  python3 nfo_to_emby.py --apply --workers 16      # 正式写
  python3 nfo_to_emby.py --limit 5         # 小样验证
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
from concurrent.futures import ThreadPoolExecutor, as_completed

from embytg_conf import cfg

E = cfg("emby_url")
T = cfg("emby_token")
HDR = {"X-Emby-Token": T, "Content-Type": "application/json"}
MOUNT = os.environ.get("EMBYTG_MOUNT", "/opt/embytg/mnt/embytg")  # 宿主挂载根（H17）
FIELDS = "Name,Genres,Overview,ProviderIds,Path,Type"

PLOT_RE = re.compile(r"<plot>(.*?)</plot>", re.S | re.I)
GENRE_RE = re.compile(r"<genre>(.*?)</genre>", re.S | re.I)


def api(path, method="GET", body=None, timeout=120):
    data = json.dumps(body).encode("utf-8") if body is not None else (b"{}" if method == "POST" else None)
    r = urllib.request.Request(E + path, method=method, headers=HDR, data=data)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as f:
            raw = f.read()
            return f.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:200]
    except Exception as e:
        return 0, str(e)[:200]


def admin_uid():
    st, d = api("/Users/Query")
    its = (d or {}).get("Items", []) if isinstance(d, dict) else []
    for u in its:
        if (u.get("Policy") or {}).get("IsAdministrator") or u.get("Name") == "admin":
            return u.get("Id")
    return its[0].get("Id") if its else None


def paged_bad(types="Movie,Series", page=500, limit=0):
    """分页只收「元数据不全」条目，不把全库拉进内存（SCALING §1）。"""
    out, start = [], 0
    while True:
        st, d = api("/Items?Recursive=true&IncludeItemTypes=%s&Fields=%s&StartIndex=%d&Limit=%d"
                    % (urllib.parse.quote(types, safe=","),
                       urllib.parse.quote(FIELDS, safe=","), start, page))
        if st != 200 or not isinstance(d, dict):
            break
        its = d.get("Items", [])
        for x in its:
            # 与 finalize_meta.py:68 needs() 完全一致
            if x.get("Type") in ("Movie", "Series") and not (
                    (x.get("ProviderIds") or {}).get("Tmdb")
                    and (x.get("Overview") or x.get("Genres"))):
                out.append(x)
                if limit and len(out) >= limit:
                    return out
        start += len(its)
        if not its or start >= (d.get("TotalRecordCount") or 0):
            break
    return out


def host_nfo(x):
    """容器内 Path -> 宿主 NFO 路径（H17：/mnt/embytg/ -> /opt/embytg/mnt/embytg/）。"""
    p = x.get("Path") or ""
    if not p.startswith("/mnt/embytg/"):
        return None
    host = os.path.join(MOUNT, p[len("/mnt/embytg/"):].lstrip("/"))
    if x.get("Type") == "Series":
        return os.path.join(host, "tvshow.nfo")
    # Movie: 与媒体文件同名的 .nfo
    return os.path.splitext(host)[0] + ".nfo"


def unesc(s):
    return (s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
             .replace("&quot;", '"').replace("&apos;", "'")).strip()


def parse_nfo(path):
    try:
        txt = open(path, encoding="utf-8").read()
    except Exception:
        return None
    m = PLOT_RE.search(txt)
    plot = unesc(m.group(1)) if m else ""
    genres = [unesc(g) for g in GENRE_RE.findall(txt) if unesc(g)]
    if not plot and not genres:
        return None
    return plot, genres


def write_one(uid, x, apply):
    iid = str(x["Id"])
    nfo = host_nfo(x)
    if not nfo or not os.path.isfile(nfo):
        return (iid, "no_nfo", x.get("Name"))
    md = parse_nfo(nfo)
    if not md:
        return (iid, "no_plot", x.get("Name"))
    plot, genres = md
    if not apply:
        return (iid, "dry", "%s plot=%d genres=%d" % (x.get("Name"), len(plot), len(genres)))
    # 取完整对象（社区帖：POST /Items/{id} 必须传完整对象，否则其它字段 null 化）
    st, full = api("/Users/%s/Items/%s" % (uid, iid))
    if st != 200 or not isinstance(full, dict) or not full.get("Id"):
        return (iid, "get_full_fail:%s" % st, x.get("Name"))
    full["Overview"] = plot
    # 2026-11-13：原实现只写 Genres，而 **Emby 会静默忽略纯 Genres 写入**
    # （POST 返回 204，值不变）。这就是「703 部里 482 部 genre 是英文」一直没被
    # nfo_to_emby 修好的原因 —— 而 line 150 的复核用 `Overview or Genres`，
    # Overview 写成功就判 ok，把 genre 的失败完全掩盖了。
    # 实测有效写法：Genres（字符串数组）+ GenreItems（{Name} 对象数组）必须同时写，
    # 再把 'Genres' 加进 LockedFields，否则后续任何 provider 刷新都会再覆盖。
    if genres:
        full["Genres"] = genres
        full["GenreItems"] = [{"Name": g} for g in genres]
        locked = [x for x in (full.get("LockedFields") or []) if x != "Genres"]
        full["LockedFields"] = locked + ["Genres"]
    st2, _ = api("/Items/%s" % iid, "POST", full, timeout=180)
    if st2 not in (200, 204):
        return (iid, "post_fail:%s" % st2, x.get("Name"))
    # 用同一完整端点复核（不带 Fields 的 /Items?Ids= 默认不返回 Overview，会误判）
    st3, chk = api("/Users/%s/Items/%s" % (uid, iid))
    chk = chk if isinstance(chk, dict) else {}
    ok_plot = bool(chk.get("Overview"))
    ok_genre = (not genres) or (chk.get("Genres") or []) == genres
    if not (ok_plot and ok_genre):
        return (iid, "verify_fail(plot=%s genre=%s/%s)"
                % (ok_plot, len(chk.get("Genres") or []), len(genres)), x.get("Name"))
    return (iid, "ok", x.get("Name"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--apply", action="store_true", help="explicit write (default dry)")
    ap.add_argument("--rounds", type=int, default=3,
                    help="self-heal rewrite rounds: residual FullRefresh may blank Overview later")
    a = ap.parse_args()
    apply = a.apply and not a.dry
    uid = admin_uid()
    if not uid:
        print("# ERR no admin uid")
        return 1
    t0 = time.time()
    bad = paged_bad(limit=a.limit)
    print("# bad %d / uid=%s / apply=%s / %.1fs"
          % (len(bad), uid, apply, time.time() - t0))
    if not bad:
        return 0
    # E007 self-heal: each round re-page the still-incomplete items and write again,
    # so queued Refresh Metadata blanking Overview gets repaired (eventual consistency).
    rounds = a.rounds if apply else 1
    for rnd in range(1, rounds + 1):
        if not bad:
            break
        counts = {}
        samples = []
        with ThreadPoolExecutor(max_workers=a.workers) as ex:
            futs = [ex.submit(write_one, uid, x, apply) for x in bad]
            for i, f in enumerate(as_completed(futs), 1):
                iid, status, name = f.result()
                counts[status] = counts.get(status, 0) + 1
                if status not in ("ok", "dry") and len(samples) < 15:
                    samples.append((iid, status, name))
                if i % 100 == 0:
                    print("#  progress %d/%d" % (i, len(bad)))
        print("# round %d done %s" % (rnd, json.dumps(counts, ensure_ascii=False)))
        for smp in samples:
            print("#  err %s %s %s" % smp)
        if not apply or rnd >= rounds:
            break
        time.sleep(5)
        bad = paged_bad(limit=a.limit)
        print("# round %d recheck: still bad %d" % (rnd, len(bad)))
    if apply:
        time.sleep(5)
        left = paged_bad(limit=a.limit)
        print("# final recheck: still bad %d" % len(left))
        for x in left[:10]:
            print("#   left %s %s %s" % (x.get("Id"), x.get("Name"), x.get("Type")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
