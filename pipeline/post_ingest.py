#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""post_ingest.py —— 入库收尾（强制流程）：NFO → Emby Name → 索引 → 验收

用户 2026-11-10 嘱咐：「定时任务入库一定让按流程来」。
入库不是"文件放进去就完事"，必须走完四步，否则会出现
「文件有、NFO 缺 / NFO 是占位 / Emby 名字还是文件名」——2026-11-10 次日复核实测：
16 个文件 Emby Name 与 NFO 不一致、9 个 NFO 是占位标题，全是收尾漏步导致的。

四步（缺一步就判失败）
--------------------
1. **补 NFO**：`todb_resolve.py --apply --series <剧>`（TODB 结构匹配，只补缺失）
   → 仍缺则 `nfo_episodes_tmdb.py --apply --series <剧>`（TMDB 兜底）
2. **写 Emby Name**：`nfo_name_to_emby.py --apply --lib <库>`（E007 直写，不靠扫描）
3. **通知 + 集条目 Refresh**：`/Library/ScanPath`（E036，TargetedScan 插件，能建新条目）
   → 再对**集条目本身**发 Refresh（E115 第五根因：Series 级 Refresh 会枚举季目录造重复条目）
   → 失败时退回 `/Library/Media/Updated`（只能刷新已有）
4. **验收**：抽样检查 `.nfo` 存在、标题非占位、Emby Name == NFO title；输出 PASS/FAIL

用法
----
    python3 post_ingest.py --since today          # 处理今天新入库的剧
    python3 post_ingest.py --series "乡村爱情"     # 指定剧
    python3 post_ingest.py --since today --verify-only
"""
import argparse
import datetime
import glob
import html
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import unicodedata
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ingest_review import source_placeholders  # noqa: E402
import nfo_gen  # noqa: E402
MNT = "/opt/embytg/mnt/embytg/TV"
CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
EMBY = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
TOKEN = CFG["emby_token"]
PLACEHOLDER = re.compile(r"^第\s*\d+\s*[集期]$|^Episode\s*\d+$", re.I)
EP_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
SERIES_LOCK_FIELDS = ["Name", "Overview", "SortName", "OriginalTitle"]


def _fold_sort(s):
    """把标题折成 Emby 的排序键形态：去变音符号 + casefold + 压空白。

    Emby 生成 SortName 时会做 Unicode 兼容分解并丢掉组合字符
    （`Shōgun` → `Shogun`、`Amélie` → `Amelie`），而 NFO `<title>` 是真源、
    保留原字母。直接拿两者比字符串会把**正确的库判成失败**：
    2026-11-21 实测 `Shōgun (2024)` 五轮收尾全部 FAIL 后 claim 被隔离，
    原因就是 `emby_sort='Shogun' != nfo_title='Shōgun'`。
    """
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.casefold().split())


def _sort_eq(emby_sort, title):
    """Emby SortName 与 NFO title 是否等价（只差变音折叠就算等价）。"""
    return _fold_sort(emby_sort) == _fold_sort(title)


def emby(path, method="GET", body=None, timeout=120):
    req = urllib.request.Request(EMBY + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"X-Emby-Token": TOKEN, "Content-Type": "application/json"})
    raw = urllib.request.urlopen(req, timeout=timeout).read().decode()
    return json.loads(raw) if raw.strip().startswith(("{", "[")) else raw


def run(cmd, timeout=1800):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return r.returncode, ((r.stdout or "") + (r.stderr or "")).strip()


def _landed_in_tgdb(rel, _cache={}):
    """rel 是否真的进了台账（resource_state 有行且 landed_at 非空）。

    只用一次只读连接把今日已落地的 rel 集合缓存起来，避免对每份清单各开一次库
    （manifests 有几百份；O(N) 进程内集合查表，与库大小无关）。
    """
    if not _cache:
        s = set()
        try:
            con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                                  uri=True, timeout=30)
            for (r,) in con.execute(
                    "select rel from resource_state where landed_at is not null "
                    "and landed_at > 0"):
                if r:
                    s.add(r.replace("\\", "/"))
            con.close()
        except Exception:                                      # noqa: BLE001
            return True          # 读不到台账时不阻断（宁可多验一部，不要静默少验）
        _cache["set"] = s
    return rel.replace("\\", "/") in _cache["set"]


def affected_series(since, day=None):
    """取「本批新入库的剧」，反推 (库, 剧目录)。

    2026-11-13 修 E031：原实现**只读本机 `logs/autoingest.log`**。11-11 起数据面搬到
    三台云电脑（new3/new4/new5），它们的入库记在各自机器上，这个日志里根本没有 →
    默认选剧恒为 0 部 → `--verify-only` 空跑却返回 0（看起来 PASS，实际一集都没验）。
    这就是「验收动作本身失效」，比缺几条 NFO 严重得多。

    现在以 **tg.db 的 `resource_state.landed_at`** 为主源：每条入库都要在 tg.db 登记，
    不管哪台机器拉的，所以它覆盖全部 lane（实测 17418 条有 landed_at，今日 58 条）。
    再并上两个兼容来源：本机 autoingest.log（Oracle lane 历史）、_gha_manifests 的 mtime
    （云电脑回收的清单，若 tg.db 还没写时的兜底）。
    """
    out = set()
    day_s = day or (datetime.date.today().isoformat() if since == "today" else "")
    if since in ("today", "", None):
        cut = int(time.mktime(time.strptime(day_s or datetime.date.today().isoformat(),
                                            "%Y-%m-%d")))
    else:
        # since 支持 "6h" / "90m" 这类相对窗口
        m = re.match(r"^(\d+(?:\.\d+)?)\s*([hmd])$", str(since))
        mult = ({"m": 60, "h": 3600, "d": 86400}.get(m.group(2), 3600)
                if m else 3600)
        cut = int(time.time() - float(m.group(1)) * mult) if m else int(time.time() - 3600)

    def add_rel(rel):
        if "/TV/" in rel:
            parts = rel.split("/TV/", 1)[1].split("/")
        elif rel.startswith("TV/"):
            parts = rel.split("/", 1)[1].split("/")
        else:
            return
        if len(parts) >= 2:
            out.add((parts[0], parts[1]))

    # 1) 主源：tg.db（覆盖全部 lane）
    try:
        con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                              uri=True, timeout=30)
        for (rel,) in con.execute(
                "select rel from resource_state where rel is not null "
                "and landed_at is not null and landed_at >= ?", (cut,)):
            add_rel(rel)
        con.close()
    except Exception:                                          # noqa: BLE001
        pass

    # 2) Oracle lane 的历史日志（兼容）
    p = os.path.join(HERE, "logs", "autoingest.log")
    if os.path.exists(p):
        for line in open(p, encoding="utf-8", errors="ignore"):
            try:
                r = json.loads(line)
            except Exception:                                  # noqa: BLE001
                continue
            if not r.get("ok"):
                continue
            try:
                ts = time.mktime(time.strptime((r.get("ts") or "")[:19],
                                               "%Y-%m-%d %H:%M:%S"))
            except Exception:                                  # noqa: BLE001
                continue
            if ts < cut:
                continue
            add_rel(r.get("target") or "")

    # 3) 云电脑回收的 manifest（兜底）
    mdir = os.path.join(HERE, "_gha_manifests")
    if os.path.isdir(mdir):
        for f in glob.glob(os.path.join(mdir, "*.json")):
            try:
                if os.path.getmtime(f) < cut:
                    continue
                d = json.load(open(f, encoding="utf-8"))
                # 2026-11-14 修：清单只是「跑完了」的凭据，不代表登记成功。
                # 未登记的清单（E083：字节没进 teldrive / 清单 parts 是别人的片）若被当成
                # 「今日涉及剧」，--verify-only 会去搜一个磁盘上根本不存在的 Series，
                # 报成「用户搜不到」——把入库失败伪装成搜索 bug。
                # 判据必须落到台账：tg.db 有该 rel 且 landed_at 非空才算真的进了库。
                rel0 = d.get("target_rel") or ""
                if rel0 and not _landed_in_tgdb(rel0):
                    continue
                add_rel(rel0)
            except Exception:                                  # noqa: BLE001
                continue
    return out


def _series_dir(lib, sdir):
    return os.path.join(MNT, lib, sdir)


def _read_series_nfo(sdir):
    """读取 Series 根目录 tvshow.nfo；NFO 是标题/身份的本地真源。"""
    p = os.path.join(sdir, "tvshow.nfo")
    try:
        txt = open(p, encoding="utf-8", errors="ignore").read()
    except OSError:
        return {}
    def tag(name):
        m = re.search(r"<%s>(.*?)</%s>" % (name, name), txt, re.S | re.I)
        return html.unescape(m.group(1)).strip() if m else ""
    return {"path": p, "title": nfo_gen.clean_title(tag("title")),
            "originaltitle": nfo_gen.clean_title(tag("originaltitle")),
            "plot": tag("plot"), "tmdbid": tag("tmdbid")}


def ensure_series_assets(series_list):
    """新剧入库前先补 tvshow.nfo+poster，避免 Emby 先猜错身份。"""
    fixed, failed = 0, []
    for lib, sdir in series_list:
        d = _series_dir(lib, sdir)
        m = re.search(r"\{tmdb=(\d+)\}", sdir)
        if not m or not os.path.isdir(d):
            continue
        nfo = os.path.join(d, "tvshow.nfo")
        poster = os.path.join(d, "poster.jpg")
        try:
            meta = _read_series_nfo(d)
            nfo_title = nfo_gen.clean_title(meta.get("title"))
            folder = nfo_gen.folder_title(d)
            title_mismatch = bool(nfo_gen.has_han(folder) and not nfo_gen.has_han(nfo_title))
            tmdb_mismatch = bool(meta.get("tmdbid") and meta.get("tmdbid") != m.group(1))
            nfo_ok = (os.path.getsize(nfo) > 200 and bool(nfo_title)
                      and not title_mismatch and not tmdb_mismatch)
            poster_ok = os.path.getsize(poster) > 1000
        except OSError:
            nfo_ok = poster_ok = False
        if nfo_ok and poster_ok:
            continue
        it = {"kind": "tv", "id": m.group(1), "name": sdir, "dir": d}
        try:
            _, res = nfo_gen.fetch_tv(it)
        except Exception as e:                                  # noqa: BLE001
            failed.append("%s:%s" % (sdir[:24], str(e)[:60]))
            continue
        if res and res.get("nfo") and os.path.exists(res["nfo"]):
            fixed += 1
        else:
            failed.append("%s:%s" % (sdir[:24], (res or {}).get("err", "no_nfo")))
    return fixed, failed


def _tmdb_external_ids(tmdb_id):
    base = (CFG.get("tmdb_api") or "https://api.themoviedb.org/3").rstrip("/")
    key = CFG.get("tmdb_api_key")
    if not key:
        return {}
    q = urllib.parse.urlencode({"api_key": key, "language": "zh-CN"})
    try:
        req = urllib.request.Request(base + "/tv/%s/external_ids?%s" % (tmdb_id, q),
                                     headers={"User-Agent": "Mozilla/5.0"})
        return json.loads(urllib.request.urlopen(req, timeout=20).read().decode())
    except Exception:                                          # noqa: BLE001
        return {}


def repair_series_metadata(series_list):
    """让 Emby Series 的标题/ProviderIds 服从目录锚点与 tvshow.nfo。

    只处理本批剧；完整对象 GET→改字段→POST→复核（E007），并备份原对象。
    """
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                                "Fields": "Path,ProviderIds,Name,SortName", "Limit": "5000"})
    try:
        rows = emby("/Items?" + q).get("Items", [])
    except Exception as e:                                     # noqa: BLE001
        return 0, ["series_query:%s" % str(e)[:70]]
    bypath = {(x.get("Path") or "").rstrip("/"): x for x in rows}
    fixed, failed = 0, []
    for lib, sdir in series_list:
        m = re.search(r"\{tmdb=(\d+)\}", sdir)
        d = _series_dir(lib, sdir)
        if not m or not os.path.isdir(d):
            continue
        meta = _read_series_nfo(d)
        want_tmdb = m.group(1)
        want_name = nfo_gen.clean_title(meta.get("title"))
        folder = nfo_gen.folder_title(d)
        if nfo_gen.has_han(folder) and not nfo_gen.has_han(want_name):
            want_name = folder
        epath = ("/mnt/embytg/TV/%s/%s" % (lib, sdir)).rstrip("/")
        hit = bypath.get(epath)
        if not hit:
            continue
        got_tmdb = str((hit.get("ProviderIds") or {}).get("Tmdb") or "")
        got_name_raw = hit.get("Name") or ""
        got_name = got_name_raw.strip()
        got_sort = (hit.get("SortName") or "").strip()
        # 不能只比较 strip() 后的值：Emby 里已存在 "剧名 " 这类首尾空格时，
        # strip 后相等会被误判为无需修正，脏 Name 就永久留下。
        if (got_tmdb == want_tmdb and
                (not want_name or
                 (got_name == want_name and got_name_raw == want_name
                  and _sort_eq(got_sort, want_name)))):
            continue
        try:
            full = emby("/Users/%s/Items/%s" % (CFG["emby_user_uid"], hit["Id"]))
            ts = time.strftime("%Y%m%d_%H%M%S")
            backup = "/opt/embytg/backup/emby_series_%s_pre_guard_%s.json" % (hit["Id"], ts)
            json.dump(full, open(backup, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
            if got_tmdb != want_tmdb:
                ext = _tmdb_external_ids(want_tmdb)
                pids = {"Tmdb": want_tmdb}
                if ext.get("tvdb_id"):
                    pids["Tvdb"] = str(ext["tvdb_id"])
                if ext.get("imdb_id"):
                    pids["Imdb"] = str(ext["imdb_id"])
                full["ProviderIds"] = pids
                urls = [{"Name": "TheMovieDb", "Url": "https://www.themoviedb.org/tv/" + want_tmdb}]
                if pids.get("Imdb"):
                    urls.append({"Name": "IMDb", "Url": "https://www.imdb.com/title/" + pids["Imdb"]})
                if pids.get("Tvdb"):
                    urls.append({"Name": "TheTVDB", "Url": "https://thetvdb.com/?tab=series&id=" + pids["Tvdb"]})
                full["ExternalUrls"] = urls
            if want_name:
                full["Name"] = want_name
                full["SortName"] = want_name
                full["ForcedSortName"] = want_name
            if meta.get("originaltitle"):
                full["OriginalTitle"] = nfo_gen.clean_title(meta["originaltitle"])
            if meta.get("plot"):
                full["Overview"] = meta["plot"]
            full["PreferredMetadataLanguage"] = "zh-CN"
            full["PreferredMetadataCountryCode"] = "CN"
            full["LockData"] = True
            full["LockedFields"] = sorted(set(full.get("LockedFields") or []) | set(SERIES_LOCK_FIELDS))
            emby("/Items/%s" % hit["Id"], "POST", full)
            chk = emby("/Users/%s/Items/%s?Fields=Path,ProviderIds,Name,SortName" %
                       (CFG["emby_user_uid"], hit["Id"]))
            ok_tmdb = str((chk.get("ProviderIds") or {}).get("Tmdb") or "") == want_tmdb
            ok_name = (not want_name or
                       ((chk.get("Name") or "") == want_name
                        and _sort_eq(chk.get("SortName"), want_name)))
            if ok_tmdb and ok_name:
                fixed += 1
            else:
                failed.append("%s:verify tmdb=%r name=%r" %
                              (sdir[:24], (chk.get("ProviderIds") or {}).get("Tmdb"), chk.get("Name")))
        except Exception as e:                                 # noqa: BLE001
            failed.append("%s:%s" % (sdir[:24], str(e)[:80]))
    return fixed, failed


def media_file_updates(series_list, only_file=None):
    """E008：只通知精确的新媒体文件，禁止拿 Series 目录触发整库刷新。"""
    out = []
    for lib, sdir in series_list:
        d = _series_dir(lib, sdir)
        if not os.path.isdir(d):
            continue
        for dp, _, fs in os.walk(d):
            for f in fs:
                if not f.lower().endswith((".mp4", ".mkv")):
                    continue
                if only_file and f != only_file:
                    continue
                host = os.path.normpath(os.path.join(dp, f))
                rel = os.path.relpath(host, MNT)
                out.append({"Path": "/mnt/embytg/TV/" + rel.replace(os.sep, "/"),
                            "UpdateType": "Created"})
    return out


def scan_running():
    """全库扫描进行中时不要再发库根 Refresh。

    Emby 收到库根 Refresh 会取消当前正在跑的 Scan media library 并重新排队，
    实测会把扫描永远卡在同一个百分比。此时只依赖正在跑的扫描即可。
    """
    try:
        tasks = emby("/ScheduledTasks") or []
    except Exception:  # noqa: BLE001
        return False
    return any("Scan media" in (t.get("Name") or "")
               and t.get("State") == "Running" for t in tasks)


def series_dir_of(path):
    """从媒体文件路径推出剧目录：`.../TV/<库>/<剧目录>/Season NN/file.mkv`。"""
    p = os.path.normpath(path or "")
    parts = p.split(os.sep)
    for i, c in enumerate(parts):
        if re.fullmatch(r"Season\s*\d+", c or "", re.I):
            return os.sep.join(parts[:i])
    return ""


def refresh_episodes(paths, timeout=120):
    """只对**集条目本身**发 Refresh；永不发 Series / Season 级 Refresh。

    E115 第五根因（2026-11-23 02:12、04:15 实测 171873/171874、171900/171901）：
    `Items/{SeriesId}/Refresh?Recursive=false` 在 Emby 4.10 里仍会刷新子季 →
    枚举季目录 → 把「文件已落地、条目还没入库」的新集当新增插入。事故现场：
    02:12:19.424 NFO 任务发 Series Refresh，50ms 后 ingest lane 的 TargetedScan
    建同一条，两条同 guid、同 Path、DateCreated 同秒（DB 里有两条 171873/171874）。
    叶子条目没有子项可枚举，机制上不可能再长出兄弟条目。

    按完整 Path 匹配条目；查不到时用文件名里的 SxxEyy 匹配同季集（改名场景：
    Emby 条目还指着旧文件名，Path 变了但季集号不变）。返回 (hit, errors)。
    """
    want = [os.path.normpath(p) for p in (paths or []) if p]
    if not want:
        return 0, []
    by_series = {}
    for p in want:
        d = series_dir_of(p)
        if d:
            by_series.setdefault(d, []).append(p)
    if not by_series:
        return 0, ["no-series-dir"]
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                               "Fields": "Path", "Limit": "5000"})
    sid = {}
    for s in (emby("/Items?" + q) or {}).get("Items", []):
        sp = os.path.normpath(s.get("Path") or "")
        if sp in by_series:
            sid[sp] = s["Id"]
    hit, errors = 0, []
    for d, ps in by_series.items():
        if d not in sid:
            errors.append("%s:no-series" % os.path.basename(d)[:30])
            continue
        q2 = urllib.parse.urlencode({
            "Recursive": "true", "IncludeItemTypes": "Episode", "ParentId": sid[d],
            "Fields": "Path,IndexNumber,ParentIndexNumber", "Limit": "20000"})
        eps = (emby("/Items?" + q2) or {}).get("Items", [])
        by_path = {os.path.normpath(e.get("Path") or ""): e for e in eps}
        by_num = {}
        for e in eps:
            try:
                by_num[(int(e.get("ParentIndexNumber")), int(e.get("IndexNumber")))] = e
            except (TypeError, ValueError):
                continue
        done = set()
        for p in ps:
            item = by_path.get(p)
            if item is None:
                m = EP_RE.search(os.path.basename(p))
                if m:
                    item = by_num.get((int(m.group(1)), int(m.group(2))))
            if item is None:
                errors.append("%s:not-in-emby" % os.path.basename(p)[:36])
                continue
            eid = str(item.get("Id"))
            if eid in done:
                continue
            done.add(eid)
            try:
                emby("/Items/%s/Refresh?Recursive=false&MetadataRefreshMode=Default" % eid,
                     "POST", timeout=min(timeout, 60))
                hit += 1
            except Exception as e:                             # noqa: BLE001
                errors.append("%s:%s" % (os.path.basename(p)[:28], str(e)[:50]))
    return hit, errors


def refresh_library_roots(libs):
    """新剧还没有 Series Id，集级 Refresh 找不到；插件不可用时才刷库根兜底。
    复用 rescan_libs.py 的 MediaFolders 真源与 Refresh 逻辑，避免再造轮子。"""
    try:
        import rescan_libs
        roots = rescan_libs.lib_roots()
    except Exception as e:                                     # noqa: BLE001
        return 0, ["rescan_libs:%s" % str(e)[:70]]
    hit, errors = 0, []
    wanted = set(libs)
    for r in roots:
        if r.get("name") not in wanted:
            continue
        try:
            if rescan_libs.refresh_lib(r):
                hit += 1
            else:
                errors.append("%s:http" % r.get("name"))
        except Exception as e:                                 # noqa: BLE001
            errors.append("%s:%s" % (r.get("name"), str(e)[:60]))
    return hit, errors


def emby_path_to_local(path):
    """把 Emby 容器路径转换成服务器上的实际挂载路径。"""
    p = path or ""
    if p.startswith("/mnt/embytg/"):
        p = p.replace("/mnt/embytg/", "/opt/embytg/mnt/embytg/", 1)
    return os.path.normpath(p)


def wait_series_ready(series_list, timeout=90, only_file=None):
    """等 Emby 只把本批剧的新集枚举出来；不等待全库扫描。

    only_file 指定时按完整文件路径验收，避免同名季集或旧路径被误判为已收录。
    返回 (ready, pending_names)。timeout=0 时直接返回，不阻塞。
    注意：Series 尚未创建时也要继续等，不能因 pending 为空而立即返回。
    """
    if timeout <= 0:
        return False, [sdir for _, sdir in series_list]
    wants = {}
    for lib, sdir in series_list:
        sp = os.path.join(MNT, lib, sdir)
        if not os.path.isdir(sp):
            continue
        want = set()
        for dp, _, fs in os.walk(sp):
            for f in fs:
                if not f.lower().endswith((".mp4", ".mkv")):
                    continue
                if only_file and f != only_file:
                    continue
                if only_file:
                    want.add(os.path.normpath(os.path.join(dp, f)))
                else:
                    mm = EP_RE.search(f)
                    if mm:
                        want.add((int(mm.group(1)), int(mm.group(2))))
        if want:
            wants[(lib, sdir)] = want
    if not wants:
        return False, [sdir for _, sdir in series_list]
    deadline = time.time() + timeout
    pending = list(wants)
    while time.time() < deadline:
        try:
            q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                                        "Fields": "Path", "Limit": "5000"})
            all_series = emby("/Items?" + q).get("Items", [])
            smap = {}
            for x in all_series:
                p = (x.get("Path") or "").rstrip("/")
                for key in wants:
                    if p.endswith("/" + key[1]):
                        smap[key] = x["Id"]
            pending = []
            for (lib, sdir), want in wants.items():
                sid = smap.get((lib, sdir))
                if not sid:
                    pending.append((lib, sdir))
                    continue
                q2 = urllib.parse.urlencode({"ParentId": sid, "Recursive": "true",
                                             "IncludeItemTypes": "Episode", "Limit": "2000",
                                             "Fields": "Path,ParentIndexNumber,IndexNumber"})
                eps = emby("/Items?" + q2).get("Items", [])
                if only_file:
                    have = {emby_path_to_local(e.get("Path")) for e in eps}
                else:
                    have = {(e.get("ParentIndexNumber"), e.get("IndexNumber")) for e in eps}
                if not (want <= have):
                    pending.append((lib, sdir))
            if not pending:
                return True, []
        except Exception:                                      # noqa: BLE001
            pending = list(wants)
        time.sleep(3)
    return False, [x[1] for x in pending]


def verify(series_list, sample=10, strict_emby=False, only_file=None):
    """抽样验收：NFO 在不在、标题是否占位、Emby Name 是否等于 NFO 标题。
    注意：Emby 的 /Items?Path= 过滤不可靠（实测恒空），改为按 Series 取全部 Episode；
    指定 only_file 时按完整 Path 比对，不能只看季集号（旧路径可能同名）。"""
    SE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
    bad = {"no_nfo": [], "placeholder": [], "name_mismatch": [], "emby_missing": []}
    checked = 0
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                                "Fields": "Path", "Limit": "5000"})
    try:
        all_series = emby("/Items?" + q).get("Items", [])
    except Exception:                                              # noqa: BLE001
        all_series = []
    per_series = max(2, sample // max(len(series_list), 1))
    for lib, sdir in series_list:
        sp = os.path.join(MNT, lib, sdir)
        if not os.path.isdir(sp):
            continue
        sid = next((x["Id"] for x in all_series if (x.get("Path") or "").endswith("/" + sdir)), None)
        emap = {}
        emap_path = {}
        if sid:
            q2 = urllib.parse.urlencode({"ParentId": sid, "Recursive": "true", "IncludeItemTypes": "Episode",
                                         "Fields": "Path,ParentIndexNumber,IndexNumber,Name", "Limit": "2000"})
            for e in emby("/Items?" + q2).get("Items", []):
                emap[(e.get("ParentIndexNumber"), e.get("IndexNumber"))] = e.get("Name")
                p = emby_path_to_local(e.get("Path"))
                if p:
                    emap_path[p] = e
        vids = []
        for dp, _, fs in os.walk(sp):
            for f in fs:
                if f.lower().endswith((".mp4", ".mkv")):
                    p = os.path.join(dp, f)
                    try:
                        vids.append((os.path.getmtime(p), dp, f))
                    except OSError:
                        pass
        vids.sort(reverse=True)          # 最新入库优先，避免抽到旧集误判 PASS
        source_ph = None
        _ph_season = None
        if only_file:
            vids = [v for v in vids if v[2] == only_file]
            if not vids:
                bad["no_nfo"].append(only_file)
                continue
        checked_series = 0
        for _, dp, f in vids:
                if checked_series >= per_series:
                    break
                checked_series += 1
                base = os.path.splitext(os.path.join(dp, f))[0]
                nfo = base + ".nfo"
                checked += 1
                if not os.path.exists(nfo):
                    bad["no_nfo"].append(f)
                    continue
                m = re.search(r"<title>(.*?)</title>", open(nfo, encoding="utf-8", errors="ignore").read(), re.S)
                title = (m.group(1).strip() if m else "")
                if not title or PLACEHOLDER.match(title):
                    _m2 = SE.search(f)
                    _season = int(_m2.group(1)) if _m2 else None
                    _ep_no = int(_m2.group(2)) if _m2 else None
                    if source_ph is None or _ph_season != _season:
                        source_ph = source_placeholders(lib, sdir, _season or 1)
                        _ph_season = _season
                    # 数据源整季都没有真标题时，NFO 占位属来源限制，不冒充刮削失败。
                    #
                    # 2026-11-14（E085）：**部分集没有真标题也不该让整条收尾失败**。
                    # 实测 FX战士久留美 S01：12 集里 7 集有真标题、5 集没有，
                    # E05 落在没标题那 5 集里 → 判 placeholder → 结论 FAIL →
                    # claim 永不释放 → 占着 slot 卡死整条上传链路
                    # （两个 claim 挂了 136 / 84 分钟，2 小时 50 分钟零产出）。
                    #
                    # 判据改成：**本集占位且数据源这一集确实没有真标题** → 记 warning，
                    # 不算失败。真故障（数据源有标题却没写进 NFO）仍然 FAIL。
                    _ep_no = None
                    _m2 = SE.search(f)
                    if _m2:
                        _ep_no = int(_m2.group(2))
                    _src_lacks = (source_ph[0] > 0 and source_ph[1] == 0)
                    if not _src_lacks and _ep_no is not None:
                        # 逐集确认：该集在数据源里是否真的没有标题（季号必须一起传，
                        # 否则只会查 S1 —— 法警小队 S02E05 就是这么被误判成故障的）
                        try:
                            from ingest_review import episode_has_real_title
                            _src_lacks = not episode_has_real_title(
                                lib, sdir, _ep_no, _season)
                        except Exception:              # noqa: BLE001
                            _src_lacks = False
                    if not _src_lacks:
                        bad["placeholder"].append(f)
                    else:
                        print("      ⚠ placeholder(来源无此集标题，不算失败): %s" % f)
                mm = SE.search(f)
                target_path = os.path.normpath(os.path.join(dp, f))
                if only_file and not sid:
                    bad["emby_missing"].append(f)
                elif only_file and sid:
                    ep = emap_path.get(target_path)
                    if not ep:
                        bad["emby_missing"].append(f)
                    elif title and ep.get("Name") != title:
                        bad["name_mismatch"].append(
                            "%s: emby=%r nfo=%r" % (f[:26], ep.get("Name"), title))
                elif mm and not sid:
                    bad["emby_missing"].append(f)
                elif mm and sid:
                    key = (int(mm.group(1)), int(mm.group(2)))
                    if key not in emap:
                        bad["emby_missing"].append(f)
                    elif title and emap[key] != title:
                        bad["name_mismatch"].append("%s: emby=%r nfo=%r" % (f[:26], emap[key], title))
    return checked, bad


def series_searchable(series_list, timeout=15):
    """用户搜索验收：NFO/Emby 显示标题或中文目录名至少一个能命中同一 Series。

    NFO 是标题真源；目录名可能保留更长的检索别名（例如
    ``游戏王：怪兽之决斗GX``），不能只拿目录名做唯一搜索词，否则
    Emby 已正确显示 ``游戏王GX`` 时会被误判为搜索失败。
    """
    bad = []
    uid = CFG["emby_user_uid"]
    for lib, sdir in series_list:
        meta = _read_series_nfo(_series_dir(lib, sdir))
        nfo_title = nfo_gen.clean_title(meta.get("title"))
        folder = nfo_gen.folder_title(sdir)
        candidates = []
        for title in (nfo_title, folder):
            if title and nfo_gen.has_han(title) and title not in candidates:
                candidates.append(title)
        if not candidates:
            continue
        deadline = time.time() + timeout
        hit = False
        while True:
            for title in candidates:
                try:
                    q = urllib.parse.urlencode({
                        "SearchTerm": title, "Recursive": "true",
                        "IncludeItemTypes": "Series", "Fields": "Path,Name",
                        "Limit": "20"})
                    rows = emby("/Users/%s/Items?%s" % (uid, q)).get("Items", [])
                    # 不能用 TotalRecordCount：Emby 对部分短标题会返回 0，
                    # 但 Items 有精确项；只认路径命中的同一 Series。
                    if any((x.get("Path") or "").rstrip("/").endswith("/" + sdir)
                           for x in rows):
                        hit = True
                        break
                except Exception:                              # noqa: BLE001
                    pass
            if hit or time.time() >= deadline:
                break
            time.sleep(2)
        if not hit:
            bad.append("%s: 搜索 %s 未命中 Series" % (
                sdir[:28], " / ".join(repr(x) for x in candidates)))
    return bad


def series_identity(series_list):
    """校验目录 {tmdb=} 与 Emby Series ProviderIds.Tmdb 一致（E027）。

    同名剧/动画可能在 Emby provider 搜索时被错绑到另一部作品；此时文件路径、
    逐集 NFO 和集标题都可能正确，但 Series 的 Name/Overview/People/Poster 会来自
    错误作品。这里把身份不一致作为收尾失败，不能只凭 Path 存在判 PASS。
    """
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Series",
                                "Fields": "Path,ProviderIds,Name,SortName", "Limit": "5000"})
    try:
        rows = emby("/Items?" + q).get("Items", [])
    except Exception as e:                                     # noqa: BLE001
        return ["查询 Emby Series 失败: %s" % str(e)[:80]]
    bad = []
    for lib, sdir in series_list:
        m = re.search(r"\{tmdb=(\d+)\}", sdir)
        if not m:
            continue
        want = m.group(1)
        epath = "/mnt/embytg/TV/%s/%s" % (lib, sdir)
        hit = next((x for x in rows
                    if (x.get("Path") or "").rstrip("/") == epath.rstrip("/")), None)
        if not hit:
            continue
        got = str((hit.get("ProviderIds") or {}).get("Tmdb") or "")
        if got != want:
            bad.append("%s: emby_tmdb=%s path_tmdb=%s name=%r"
                       % (sdir[:28], got or "-", want, hit.get("Name")))
        meta = _read_series_nfo(_series_dir(lib, sdir))
        if not meta.get("title"):
            bad.append("%s: tvshow.nfo 缺 title" % sdir[:28])
        elif (hit.get("Name") or "") != meta["title"]:
            bad.append("%s: emby_name=%r nfo_title=%r"
                       % (sdir[:28], hit.get("Name"), meta["title"]))
        elif not _sort_eq(hit.get("SortName"), meta["title"]):
            bad.append("%s: emby_sort=%r nfo_title=%r"
                       % (sdir[:28], hit.get("SortName"), meta["title"]))
    return bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="today")
    ap.add_argument("--date", help="指定日期 YYYY-MM-DD（配合 --since date）")
    ap.add_argument("--series", action="append", default=[])
    ap.add_argument("--tmdb", type=int, help="按 TMDB ID 精确选择一部剧（GitHub Actions 用）")
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--sample", type=int, default=20)
    ap.add_argument("--wait-emby", type=int, default=20,
                    help="先等本剧 Emby 枚举；超时后定向 Refresh 再等（不等全库扫描）")
    ap.add_argument("--strict-emby", action="store_true",
                    help="Emby 未收录也算失败；默认只把 NFO/Name 作为刮削验收")
    ap.add_argument("--verify-file", help="只验收指定文件名（GitHub Actions 单集收尾用）")
    a = ap.parse_args()

    if a.tmdb:
        series = []
        needle = "{tmdb=%d}" % int(a.tmdb)
        for lib in os.listdir(MNT):
            lp = os.path.join(MNT, lib)
            if not os.path.isdir(lp):
                continue
            for d in os.listdir(lp):
                if os.path.isdir(os.path.join(lp, d)) and needle in d:
                    series.append((lib, d))
        series = sorted(set(series))
    elif a.series:
        series = []
        for root, dirs, _ in os.walk(MNT):
            for d in dirs:
                if any(k in d for k in a.series):
                    lib = os.path.basename(os.path.dirname(os.path.join(root, d)))
                    series.append((lib, d))
        series = sorted(set(series))
    else:
        series = sorted(affected_series(a.since, a.date))

    print("== 入库收尾 ==")
    print("  涉及剧 %d 部：%s" % (len(series), ", ".join(s[1][:18] for s in series[:6]) or "无"))
    if not series:
        print("  没有需要收尾的剧（今天没有成功入库）")
        return 0

    if not a.verify_only:
        libs = sorted({s[0] for s in series})
        af, ae = ensure_series_assets(series)
        print("  ⓪ Series NFO/海报：补 %d 部%s" % (
            af, "" if not ae else "；失败 %s" % ", ".join(ae[:3])))
        rf, re_ = repair_series_metadata(series)
        print("  ⓪ Series 标题/身份：修 %d 部%s" % (
            rf, "" if not re_ else "；失败 %s" % ", ".join(re_[:3])))
        for lib, sdir in series:
            name = sdir.split(" (")[0]
            tm = re.search(r"\{tmdb=(\d+)\}", sdir)
            tid = tm.group(1) if tm else ""
            if not tid:
                print("  ① NFO %-22s 无 tmdb 锚点，跳过自动刮削" % name[:22])
                continue
            # 关键修正：todb_resolve/nfo_episodes_tmdb 的 --series 都要求 TMDB 数字 ID，
            # 旧代码传中文剧名会匹配 0 部，导致 NFO 根本没生成。
            rc, out = run('%s %s/todb_resolve.py --apply --series "%s"'
                          % (sys.executable, HERE, tid), timeout=900)
            print("  ① NFO(TODB) %-22s rc=%d %s" % (
                name[:22], rc, (out.splitlines() or [""])[-1][:70]))
            rc2, out2 = run('%s %s/nfo_episodes_tmdb.py --apply --allow-placeholder --series "%s" --workers 8'
                            % (sys.executable, HERE, tid), timeout=900)
            print("  ① NFO(TMDB兜底) %-16s rc=%d %s" % (
                name[:16], rc2, (out2.splitlines() or [""])[-1][:70]))

        # E008：通知精确的新媒体文件，不能拿 Series 目录去触发整库刷新。
        # E036：改走 TargetedScan 的 /Library/ScanPath。
        # 实测 Media/Updated 喂**目录**返回 204 但不创建 Series；喂**文件**只能刷新已有条目。
        # ScanPath 会向上找最近的已知祖先，自上而下补齐 Series/Season/Episode，再排元数据刷新。
        # 实测：缺失的「初代奥特曼 S01E05」→ Status=Created，ItemId=166580，25 秒后
        # Name 已是 NFO 里的「食虫草的秘密」，集号 5 正确 —— 全程 <1 秒，无整库 ffprobe。
        updates = media_file_updates(series, a.verify_file)
        created = 0
        res = {}
        try:
            import emby_scan
            res = emby_scan.scan_paths([u["Path"] for u in updates])
            created = int(res.get("Created") or 0)
            print("  ② Emby 通知(%s) %d 文件：Created=%d Refreshed=%d Failed=%d"
                  % (res["mode"], len(updates), res.get("Created", 0),
                     res.get("Refreshed", 0), res.get("Failed", 0)))
            if res.get("Created"):
                print("     ↑ 新建了 %d 个条目（旧通路会假成功但条目不存在）" % res["Created"])
        except Exception as e:                                     # noqa: BLE001
            print("  ② Emby 通知失败:", str(e)[:60])
        ready, pending = wait_series_ready(series, a.wait_emby, a.verify_file)
        root_hit, root_err = 0, []
        plugin_ok = (res or {}).get("mode") == "scanpath"
        if not ready and not scan_running() and not plugin_ok:
            # 只有插件不可用（mode != scanpath）才允许目录级刷新作为建条目退路；
            # 插件可用时 TargetedScan 是唯一建条目入口（E115 第三~五根因）。
            root_hit, root_err = refresh_library_roots(sorted({s[0] for s in series}))
            ready, pending = wait_series_ready(series, max(300, a.wait_emby), a.verify_file)
        # E115 第五根因（2026-11-23）：Series 级 Refresh 即使 Recursive=false 也会刷新
        # 子季并枚举季目录，把「文件已落地、条目未入库」的新集插成第二条。本批只刷
        # **集条目本身**（叶子），机制上不可能再造出兄弟条目。
        hit, refresh_err = refresh_episodes([u["Path"] for u in updates])
        rf2, re2 = repair_series_metadata(series)
        print("  ② 集条目 Refresh %d 条；库根 Refresh %d 个；本批枚举 %s%s；身份修 %d%s" % (
            hit, root_hit, "完成" if ready else "未完全完成",
            "" if not pending else "（缺：%s）" % ", ".join(x[:16] for x in pending[:4]),
            rf2, "" if not (refresh_err or root_err or re2) else "；错误 %s" % ", ".join((refresh_err + root_err + re2)[:3])))

        for lib in libs:
            cmd = [sys.executable, os.path.join(HERE, "nfo_name_to_emby.py"),
                   "--apply", "--lib", lib, "--workers", "8"]
            dirs = ["/mnt/embytg/TV/%s/%s" % (l, sdir) for l, sdir in series if l == lib]
            for d in dirs:
                cmd += ["--series-dir", d]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            out = ((r.stdout or "") + (r.stderr or "")).strip()
            print("  ③ Emby Name %-10s rc=%d %s" % (
                lib, r.returncode, (out.splitlines() or [""])[-1][:90]))

    checked, bad = verify(series, a.sample, a.strict_emby, a.verify_file)
    n_bad = sum(len(bad[k]) for k in ("no_nfo", "placeholder", "name_mismatch"))
    if a.strict_emby:
        n_bad += len(bad["emby_missing"])
    print("  ④ 验收：抽样 %d 集 → 无NFO %d / 占位 %d / Emby缺 %d / Name不符 %d%s"
          % (checked, len(bad["no_nfo"]), len(bad["placeholder"]),
             len(bad["emby_missing"]), len(bad["name_mismatch"]),
             "（严格）" if a.strict_emby else "（Emby未收录不阻塞）"))
    for k, v in bad.items():
        for x in v[:3]:
            print("      ✘ %s: %s" % (k, x[:60]))
    identity_bad = series_identity(series)
    print("  ⑤ Series 身份：%s" % ("PASS" if not identity_bad else
                                   "FAIL %d" % len(identity_bad)))
    for x in identity_bad[:5]:
        print("      ✘ identity: %s" % x[:120])
    n_bad += len(identity_bad)
    search_bad = series_searchable(series)
    print("  ⑥ 用户搜索：%s" % ("PASS" if not search_bad else
                                "FAIL %d" % len(search_bad)))
    for x in search_bad[:5]:
        print("      ✘ search: %s" % x[:120])
    n_bad += len(search_bad)
    print("\n  结论：%s" % ("PASS" if n_bad == 0 else "FAIL"))
    return 0 if n_bad == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
