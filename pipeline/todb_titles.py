#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""todb_titles.py —— 用 TODB 的 episode_title 给剧集文件命名（用户 m00494）

背景：TODB（theotherdb）每集都有 `episode_title`（如「第1期上：高颜值男女晚宴初crush」），
而我们的文件只有 `剧名 SxxExx - 2160p.mp4`，Emby 的 Episode Name 就是文件名，于是全是一堆
「S08E01 - 2160p」。本脚本把 TODB 标题写进**文件名**（改名=元数据移动，不产生上传字节）：

  剧名 SxxExx - 标题 - 2160p.mp4

TODB 与 TMDB 的季/集结构实测逐条一致（`_todb_align.py` 校验 12902/103147：S0 363、S8 30、S9 34 全一致），
所以集号沿用 renumber_todb 的结果，本脚本只补标题。

用法（服务器 cd /opt/embytg/pipeline）：
  python3 todb_titles.py --scan [--lib 国产剧] [--series 231620]
  python3 todb_titles.py --apply [--lib 国产剧] [--series 103147]
  python3 todb_titles.py --rollback todb_titles_<ts>.json
"""
from nfo_sidecar import rename_with_sidecars   # 改名同步搬 .nfo/海报（E016 防孤儿）
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

MNT = "/opt/embytg/mnt/embytg"
CACHE = "/opt/embytg/pipeline/.todb_title_cache.json"
VIDEO_EXT = (".mp4", ".mkv", ".ts", ".m4v", ".mov", ".avi", ".wmv", ".flv", ".webm")
BASE = "https://theotherdb.org/api"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

SE_RE = re.compile(r"S(\d{1,2})E(\d{1,4})", re.I)
QUAL_RE = re.compile(r"-\s*(\d{3,4}p|SD|4K)\s*$", re.I)
TMDB_RE = re.compile(r"\{tmdb=(\d+)\}")
YEAR_RE = re.compile(r"\((\d{4})\)")
# 「(未知)」不是年份，是入库时没查到年份留下的占位符。以前它会被当成剧名的一部分
# 原样发给 TODB（`title=虎与龙 (未知)`），搜索自然 0 命中 → 这类剧永远拿不到 NFO。
# 查询侧必须剥掉它；文件名/目录名侧不动（改名是另一条链路，见 fix_naming.py）。
YEAR_UNKNOWN_RE = re.compile(r"\s*[\(（]\s*(?:未知|未知年份|unknown|n/?a)\s*[\)）]\s*", re.I)


def query_title(t):
    """把剧名收拾成「可以拿去搜 TODB」的样子：去掉年份占位符。"""
    return YEAR_UNKNOWN_RE.sub("", t or "").strip()
SEASON_WORD = re.compile(r"\s*第[一二三四五六七八九十0-9]+季\s*$")

_cache = {}
if os.path.exists(CACHE):
    try:
        _cache = json.load(open(CACHE))
    except Exception:
        _cache = {}


def save_cache():
    tmp = CACHE + ".tmp"
    json.dump(_cache, open(tmp, "w"), ensure_ascii=False)
    os.replace(tmp, CACHE)


def api(path, **q):
    url = BASE + path
    if q:
        url += "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read().decode("utf-8", "ignore")
    try:
        return json.loads(raw)
    except Exception:
        return {}


def search_title(title):
    """按标题搜 TODB，返回候选 list[{video_id, video_title, date_air, video_type}]"""
    d = api("/video/list", title=title, page_size=20)
    return d.get("items") or []


AMBIG_LOG = "/opt/embytg/pipeline/.todb_ambiguous.json"


def resolve_all(title, year):
    """返回**全部**同名候选（标题归一化后完全相等；年份已知时 |差|<=1）。

    为什么要这个（E021）：`resolve()` 历史上只取排序第一个，哑舍在 TODB 有三个条目
    （3480/30 集、3494/12 集、76215/动态漫），取了 12 集那个 → 18 集被判"数据缺失"。
    需要按季集结构挑的时候用 `todb_resolve.py`；至少要知道"这里有几个候选"。
    """
    out, seen = [], set()
    title = query_title(title)
    base = SEASON_WORD.sub("", title).strip()
    for cand in [title, base, title.replace(" ", ""), base.replace(" ", "")]:
        if not cand:
            continue
        for i2 in search_title(cand):
            if (i2.get("video_type") or "tv") != "tv":
                continue
            vid = int(i2.get("video_id"))
            if vid in seen:
                continue
            nm = i2.get("video_title") or ""
            nb = re.sub(r"[\s\-_·:：!！?？]+", "", nm).lower()
            nt = re.sub(r"[\s\-_·:：!！?？]+", "", title).lower()
            nbase = re.sub(r"[\s\-_·:：!！?？]+", "", base).lower()
            if nb != nt and nb != nbase:
                continue
            cy = str(i2.get("date_air") or "")[:4]
            if year and cy.isdigit() and abs(int(cy) - int(year)) > 1:
                continue
            seen.add(vid)
            out.append({"vid": vid, "title": nm, "year": cy or None,
                        "air": str(i2.get("date_air") or "")[:10]})
    return out


def resolve(title, year):
    """剧名 -> TODB video_id（**保留旧行为，但多候选会记日志**）。

    ⚠️ 只用于"拿到一个大致锚点"。**逐集标题/季集号必须以 `todb_resolve.py` 的结构匹配为准**，
    因为同名多条目（不同年份/版本/续集）在这里无法按季集结构区分（E021）。
    """
    cands = resolve_all(title, year)
    if len(cands) > 1:
        try:
            log = {}
            if os.path.exists(AMBIG_LOG):
                log = json.load(open(AMBIG_LOG))
            log["%s|%s" % (title, year or "")] = {"ts": time.strftime("%F %T"), "cands": cands}
            json.dump(log, open(AMBIG_LOG, "w"), ensure_ascii=False, indent=1)
        except Exception:
            pass
    if cands:
        cands.sort(key=lambda c: (abs(int(c["year"]) - int(year)) if (c["year"] and year) else 99, c["vid"]))
        return int(cands[0]["vid"]), title
    # 兜底：旧逻辑（含「第N季」后缀剥离）
    for cand in [query_title(title), SEASON_WORD.sub("", query_title(title)).strip(),
                 query_title(title).replace(" ", "")]:
        if not cand:
            continue
        items = search_title(cand)
        items = [i2 for i2 in items if (i2.get("video_type") or "tv") == "tv"]
        if not items:
            continue
        if year:
            same = [i2 for i2 in items if str(i2.get("date_air") or "")[:4] == str(year)]
            if same:
                items = same
        items.sort(key=lambda i2: (str(i2.get("date_air") or "")[:4] != str(year), i2.get("video_id")))
        return int(items[0]["video_id"]), cand
    return None, None


def todb_eps(vid):
    """{(season, ep): {t: title, d: desc, air: date}}，带磁盘缓存"""
    key = str(vid)
    if key in _cache:
        return {tuple(int(x) for x in k.split(",")): v for k, v in _cache[key].items()}
    seasons = api("/video/%s/season/all" % vid)
    rows = seasons if isinstance(seasons, list) else (seasons.get("data") or [])
    out = {}
    for s in rows:
        n = int(s.get("season_number") or 0)
        eps = api("/video/%s/season/%d/episode/all" % (vid, n))
        lst = eps if isinstance(eps, list) else (eps.get("data") or [])
        for e in lst:
            num = e.get("episode_number")
            if num is None:
                continue
            out[(n, int(num))] = {"t": (e.get("episode_title") or "").strip(),
                                  "d": (e.get("episode_description") or "").strip(),
                                  "air": (e.get("date_air") or "")[:10]}
    _cache[key] = {"%d,%d" % k: v for k, v in out.items()}
    save_cache()
    return out


BAD = '\\/:*?"<>|'


def sanitize(s, limit=70):
    s = "".join(" " if c in BAD else c for c in s)
    s = re.sub(r"\s+", " ", s).strip(" .-_")
    if len(s) > limit:
        s = s[:limit].rstrip(" .-_")
    return s


def build_name(title, season, ep, t, qual, ext, max_bytes=190):
    """拼文件名；按 UTF-8 字节预算收缩标题，避免超过 ext4/teldrive 的 255 字节上限。"""
    head = "%s S%02dE%02d - " % (sanitize(title, 40), season, ep)
    tail = " - %s%s" % (qual, ext)
    budget = max_bytes - len(head.encode("utf-8")) - len(tail.encode("utf-8"))
    tt = t
    while tt and len(tt.encode("utf-8")) > budget:
        tt = tt[:-1]
    return head + tt.rstrip(" .-_") + tail


def series_title(dirpath):
    base = os.path.basename(dirpath.rstrip("/"))
    t = TMDB_RE.sub("", base).strip()
    y = YEAR_RE.search(t)
    t = YEAR_RE.sub("", t).strip()
    return t, (int(y.group(1)) if y else None)


def scan_series(dirpath, vid, eps):
    title, _ = series_title(dirpath)
    plan, notes = [], []
    for sub in sorted(os.listdir(dirpath)):
        sp = os.path.join(dirpath, sub)
        if not os.path.isdir(sp) or not re.match(r"Season\s*\d+$", sub, re.I):
            continue
        for f in sorted(os.listdir(sp)):
            if not f.lower().endswith(VIDEO_EXT):
                continue
            m = SE_RE.search(f)
            if not m:
                continue
            season, ep = int(m.group(1)), int(m.group(2))
            info = eps.get((season, ep))
            if not info or not info["t"]:
                notes.append(("no-title", f))
                continue
            t = sanitize(info["t"])
            if t and t in f:
                continue                                   # 已带标题
            qm = QUAL_RE.search(os.path.splitext(f)[0])
            qual = qm.group(1) if qm else "SD"
            ext = os.path.splitext(f)[1].lower()
            newname = build_name(title, season, ep, t, qual, ext)
            plan.append({"src": os.path.join(sp, f), "dst": os.path.join(sp, newname),
                         "season": season, "ep": ep, "title": t, "qual": qual})
    return plan, notes


_LD_TIMEOUT = float(os.environ.get("EMBYTG_LISTDIR_TIMEOUT", "20"))


def _safe_listdir(path):
    """列目录，异常/不存在/超时都返回 []。

    2026-11-15（E117）：**rclone 挂载上 os.listdir 会永久阻塞，try/except 救不了。**
    实测 nfo_episodes_tmdb 卡在 D 状态、栈里是 fuse_file_read_iter，
    读的是 TV/国产剧/盛唐奇案 —— 该目录已被删除，rclone 拿不到数据就一直等，
    整个进程永远不结束（5 分钟零输出、零进展）。

    关键点：这种阻塞发生在内核态，不会抛异常，所以必须用**线程 + join 超时**兜住。
    超时的线程会变成僵尸，但主流程能继续 —— 比整个任务卡死强得多。
    """
    try:
        if not os.path.isdir(path):
            return []
    except Exception:                                      # noqa: BLE001
        return []

    import threading
    box = {"v": []}

    def _run():
        try:
            box["v"] = sorted(os.listdir(path))
        except Exception:                                  # noqa: BLE001
            box["v"] = []

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(_LD_TIMEOUT)
    if t.is_alive():
        # 卡住的线程留着（daemon，进程退出会被清），主流程继续
        print("  ! 列目录超时(%.0fs)，跳过：%s" % (_LD_TIMEOUT, path), flush=True)
        return []
    return box["v"]


def _safe_read(path, timeout=None):
    """读文件，超时返回 None。

    2026-11-15（E117 补）：**rclone 上不止列目录会死等，读文件一样会。**
    实测 _safe_listdir 加好后进程仍卡在 D 状态，这次阻塞在
    读某个 `.nfo` 文件（栈同为 filemap_read / fuse_file_read_iter）。

    所以保护必须覆盖**所有** mnt 下的 IO，不能只保护 listdir。
    """
    if timeout is None:
        timeout = _LD_TIMEOUT
    import threading
    box = {"v": None, "err": None}

    def _run():
        try:
            with open(path, encoding="utf-8", errors="ignore") as fh:
                box["v"] = fh.read()
        except Exception as e:                             # noqa: BLE001
            box["err"] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        print("  ! 读文件超时(%.0fs)，跳过：%s" % (timeout, path), flush=True)
        return None
    return box["v"]


def _safe_isdir(path, timeout=None):
    """`os.path.isdir` 同 _safe_exists —— 都是 stat，都会在 rclone 上死等。"""
    if timeout is None:
        timeout = _LD_TIMEOUT
    import threading
    box = {"v": False}

    def _run():
        try:
            box["v"] = os.path.isdir(path)
        except Exception:                                  # noqa: BLE001
            box["v"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        print("  ! isdir 超时(%.0fs)，跳过：%s" % (timeout, path), flush=True)
        return False
    return box["v"]


def _safe_exists(path, timeout=None):
    """`os.path.exists` 在 rclone 上会**永久阻塞**（stat 走同一套 FUSE 请求）。

    2026-11-15（E121）：E117 只包了 listdir / read / write，漏了 stat。
    实测 `nfo_episodes_tmdb --apply` 跑到第 100 部左右卡死在
    `os.path.exists(<mnt>/…/S01E11 - 2160p.nfo)`：CPU 7 分钟只涨 10 秒、
    缓存不再增长、无任何网络连接 —— 而 5 秒的 read 超时救不了它，
    因为阻塞发生在 `exists()` 这一行，根本没走到 read。

    判据同 _safe_read：线程 + join(timeout)，超时返回 False（当作不存在，
    宁可跳过一部也不让整轮卡死）。
    """
    if timeout is None:
        timeout = _LD_TIMEOUT
    import threading
    box = {"v": False}

    def _run():
        try:
            box["v"] = os.path.exists(path)
        except Exception:                                  # noqa: BLE001
            box["v"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        print("  ! stat 超时(%.0fs)，当作不存在：%s" % (timeout, path), flush=True)
        return False
    return box["v"]


def _safe_write(path, text, timeout=None):
    """写文件，超时返回 False。

    2026-11-15（E117 再补）：**写同样会死等。**
    _safe_listdir + _safe_read 都加好后，进程仍卡在
    open(nfo, "w") —— 往一个 rclone 上已失效的路径写，永久阻塞。

    结论：mnt 下的 IO **读、写、列目录全部**要包超时，不能漏一个。
    """
    if timeout is None:
        timeout = _LD_TIMEOUT
    import threading
    box = {"ok": False}

    def _run():
        try:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
            box["ok"] = True
        except Exception:                                  # noqa: BLE001
            pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        print("  ! 写文件超时(%.0fs)，放弃：%s" % (timeout, path), flush=True)
        return False
    return box["ok"]


def iter_series(lib=None, tid=None):
    tv = os.path.join(MNT, "TV")
    for l in _safe_listdir(tv):
        lp = os.path.join(tv, l)
        if not os.path.isdir(lp) or (lib and l != lib):
            continue
        for s in _safe_listdir(lp):
            sp = os.path.join(lp, s)
            if not os.path.isdir(sp):
                continue
            m = TMDB_RE.search(s)
            if not m:
                continue
            if tid and m.group(1) != str(tid):
                continue
            yield l, sp, int(m.group(1))


def run(apply=False, lib=None, tid=None, limit_series=0):
    t0 = time.time()
    total, missing_vid, no_title, series_ok = 0, 0, 0, 0
    all_plan = []
    for l, sp, sid in iter_series(lib, tid):
        title, year = series_title(sp)
        vid, matched = resolve(title, year)
        if not vid:
            missing_vid += 1
            print("!! TODB 无此片: %s" % os.path.basename(sp)[:40])
            continue
        eps = todb_eps(vid)
        plan, notes = scan_series(sp, vid, eps)
        if not plan and not notes:
            continue
        series_ok += 1
        print("== %-26s todb=%-7s 集=%d 需改名=%-4d 无标题=%d  %.0fs" % (
            os.path.basename(sp)[:26], vid, len(eps), len(plan), len(notes), time.time() - t0))
        for p in plan[:6]:
            print("   %-40s -> %s" % (os.path.basename(p["src"])[:40], os.path.basename(p["dst"])[:70]))
        for k, f in notes[:3]:
            print("   ! %-10s %s" % (k, f))
        total += len(plan)
        no_title += len(notes)
        all_plan.extend(plan)
        if limit_series and series_ok >= limit_series:
            break
    print("\n可补标题改名 %d 条，无标题 %d 条，TODB 无此片 %d 部，涉及剧集 %d 部，耗时 %.1fs" % (
        total, no_title, missing_vid, series_ok, time.time() - t0))
    if not apply or not all_plan:
        return all_plan
    return do_apply(all_plan)


def do_apply(plan):
    ts = time.strftime("%Y%m%d_%H%M%S")
    journal = "/opt/embytg/pipeline/todb_titles_%s.json" % ts
    done, fail = [], []
    for p in plan:
        if not os.path.exists(p["src"]):
            fail.append((p["src"], "src-missing"))
            continue
        if os.path.exists(p["dst"]):
            fail.append((p["src"], "dst-exists"))
            continue
        try:
            rename_with_sidecars(p["src"], p["dst"])
            done.append({"from": p["src"], "to": p["dst"]})
        except Exception as e:
            fail.append((p["src"], str(e)[:120]))
    json.dump({"ts": ts, "done": done, "fail": fail}, open(journal, "w"), ensure_ascii=False, indent=1)
    print("\n改名完成 %d 条，失败 %d 条 -> %s" % (len(done), len(fail), journal))
    for f, why in fail[:15]:
        print("  ! %-56s %s" % (os.path.basename(f)[:56], why))
    return done


def rollback(path):
    j = json.load(open(path))
    ok = bad = 0
    for d in j.get("done") or []:
        if os.path.exists(d["to"]) and not os.path.exists(d["from"]):
            os.makedirs(os.path.dirname(d["from"]), exist_ok=True)
            rename_with_sidecars(d["to"], d["from"])
            ok += 1
        else:
            bad += 1
    print("回滚 %d 条，跳过 %d 条" % (ok, bad))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--lib")
    ap.add_argument("--series")
    ap.add_argument("--limit-series", type=int, default=0)
    ap.add_argument("--rollback")
    a = ap.parse_args()
    if a.rollback:
        rollback(a.rollback)
        sys.exit(0)
    run(apply=a.apply, lib=a.lib, tid=a.series, limit_series=a.limit_series)
