#!/usr/bin/env python3
"""emos_import.py —— 从 emos.enic65.eu.org（视频站，走 PlaybackInfo 直链）导入指定集的片源。

## 为什么要单独写（2026-11-19）

`emos.enic65.eu.org` 与其它源站不是同一套接口：
  · `/Items?SearchTerm=` 直接 502，但 `/Users/{uid}/Items?searchTerm=` 正常；
  · 条目 id 是虚拟 id（`vl-` / `ve-`），`/Videos/{id}/stream` 全 404；
  · 真正可下载的地址只在 `POST /Items/{id}/PlaybackInfo` 返回的
    `MediaSources[0].DirectStreamUrl`（`/emya/video?server=emos&media_id=…`）。
实测该地址支持 Range（206），4MB/0.58s ≈ 7.2 MB/s。

内容质量（航海王 E01 实测）：2880x2160 HEVC 7.5Mbps 1.40GB，
对比库里那份 1920x1080 0.63GB，确实是更高画质。

## 用法

    python3 emos_import.py --series 航海王 --year 1999 --list            # 只列集与画质
    python3 emos_import.py --series 航海王 --year 1999 --eps 1-3 --apply  # 真导 3 集
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgsrc_copy as TC                                    # noqa: E402  (upload/register 复用)

BASE = "https://emos.enic65.eu.org"
UA = "Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)"
USER, PW = "enic", "enic"


class Emos:
    def __init__(self):
        self.tok = None
        self.uid = None

    def _req(self, path, q=None, data=None, method="GET", timeout=60, stream=False):
        url = BASE + path + ("?" + urllib.parse.urlencode(q) if q else "")
        if self.tok:
            url += ("&" if q or "?" in path else "?") + "api_key=" + self.tok
        hd = {"User-Agent": UA}
        if self.tok:
            hd["X-Emby-Token"] = self.tok
        body = None
        if data is not None:
            hd["Content-Type"] = "application/json"
            body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, headers=hd, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if stream:
                return r
            return json.loads(r.read())

    def login(self):
        req = urllib.request.Request(
            BASE + "/Users/AuthenticateByName",
            data=json.dumps({"Username": USER, "Pw": PW}).encode(),
            headers={"User-Agent": UA,
                     "X-Emby-Authorization": 'MediaBrowser Client="embytg", Device="ctl", '
                                             'DeviceId="embytg-ctl-01", Version="1.0.0"',
                     "Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read())
        self.tok, self.uid = d["AccessToken"], d["User"]["Id"]
        return self

    def find_series(self, name, year=None):
        d = self._req("/Users/%s/Items" % self.uid,
                      {"searchTerm": name, "IncludeItemTypes": "Series",
                       "Recursive": "true", "Limit": "20"})
        best = None
        for it in d.get("Items") or []:
            if it.get("Name") == name and (not year or it.get("ProductionYear") == year):
                return it
            if not best and name in (it.get("Name") or ""):
                best = it
        return best

    def episodes(self, series_id, limit=5000):
        out, start = [], 0
        while True:
            d = self._req("/Shows/%s/Episodes" % series_id,
                          {"UserId": self.uid, "StartIndex": str(start), "Limit": "200",
                           "Fields": "Path,MediaSources"})
            items = d.get("Items") or []
            out += items
            if len(out) >= (d.get("TotalRecordCount") or 0) or not items:
                break
            start += len(items)
        return out

    def direct_url(self, item_id):
        d = self._req("/Items/%s/PlaybackInfo" % item_id, {"UserId": self.uid},
                      data={}, method="POST")
        ms = (d.get("MediaSources") or [{}])[0]
        path = ms.get("DirectStreamUrl") or ""
        return (BASE + path) if path else "", ms


def quality_of(ms):
    for s in ms.get("MediaStreams") or []:
        if s.get("Type") == "Video" and s.get("Height"):
            h, w = s["Height"], s.get("Width") or 0
            if w >= 3000 or h >= 1800:
                return "2160p", w, h
            if w >= 1700 or h >= 1000:
                return "1080p", w, h
            return "720p", w, h
    return "SD", 0, 0


def write_nfo(dir_path, name, item):
    """写 NFO：标题用源站中文集名（别让 Emby 显示文件名里的英文画质标签）。"""
    import xml.sax.saxutils as su
    title = (item.get("Name") or "").strip()
    if not title:
        return
    nfo = os.path.join(dir_path, os.path.splitext(name)[0] + ".nfo")
    try:
        os.makedirs(dir_path, exist_ok=True)
        with open(nfo, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n'
                    '<episodedetails>\n  <title>%s</title>\n  <season>%s</season>\n'
                    '  <episode>%s</episode>\n  <plot>%s</plot>\n</episodedetails>\n'
                    % (su.escape(title), item.get("ParentIndexNumber") or 1,
                       item.get("IndexNumber") or 0, su.escape(title)))
    except Exception as ex:
        print("  NFO 写入失败（不影响入库）: %s" % str(ex)[:60], flush=True)


def scan_one(file_path):
    """Emby 定向扫描：必须送**文件路径**；送目录只会 Refreshed，不会建新条目。"""
    try:
        import emby_scan
        r = emby_scan.scan_paths([file_path])
        print("  送扫: Created=%s Refreshed=%s skip=%s" % (
            r.get("Created"), r.get("Refreshed"), r.get("skipped", 0)), flush=True)
    except Exception as ex:
        print("  送扫失败: %s" % str(ex)[:60], flush=True)


def parse_eps(spec):
    out = []
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            out += list(range(int(a), int(b) + 1))
        elif part.strip():
            out.append(int(part))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--year", type=int, default=0)
    ap.add_argument("--eps", default="", help="绝对集号，如 2-5,9")
    ap.add_argument("--target-dir", default="", help="teldrive 目录；缺省按 TMDB 锚点生成")
    ap.add_argument("--tmdb", type=int, default=0)
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=3)
    a = ap.parse_args()

    e = Emos().login()
    s = e.find_series(a.series, a.year or None)
    if not s:
        print("  找不到剧:", a.series); return 2
    print("  剧: %s (%s) id=%s" % (s.get("Name"), s.get("ProductionYear"), s.get("Id")))
    eps_list = e.episodes(s["Id"])
    print("  集数:", len(eps_list))

    if a.list:
        for it in eps_list[:5] + eps_list[-2:]:
            ms = (it.get("MediaSources") or [{}])[0]
            q, w, h = quality_of(ms)
            print("   E%-4s %-22s %-6s %sx%s %.2fGB" % (
                it.get("IndexNumber"), (it.get("Name") or "")[:22], q, w, h,
                (ms.get("Size") or 0) / 1e9))
        return 0

    want = set(parse_eps(a.eps)) if a.eps else set()
    todo = [it for it in eps_list if not want or int(it.get("IndexNumber") or 0) in want]
    # 2026-11-19：emos 同一部剧有「真 4K 2.1GB/14Mbps」也有「1080p remux 13.4GB/32Mbps」。
    # 后者每集 13GB，1342 集要 17TB，本机只剩 35GB 磁盘 —— 全量洗版必须设体积上限。
    # 默认 6GB：能容纳 2-4GB 的高码率 4K，挡住 remux。
    _cap = float(os.environ.get("EMOS_MAX_GB", "6"))
    _over = [it for it in todo if ((it.get("MediaSources") or [{}])[0].get("Size") or 0) / 1e9 > _cap]
    if _over:
        print("  超过 %.0fGB 上限的 %d 集已跳过（remux 不参与洗版）" % (_cap, len(_over)))
    todo = [it for it in todo
            if ((it.get("MediaSources") or [{}])[0].get("Size") or 0) / 1e9 <= _cap]
    todo = todo[: a.limit]
    print("  本次处理 %d 集" % len(todo))

    if not a.apply:
        for it in todo:
            ms = (it.get("MediaSources") or [{}])[0]
            q, w, h = quality_of(ms)
            print("   [dry] E%s %s %s %.2fGB" % (it.get("IndexNumber"), q, h, (ms.get("Size") or 0)/1e9))
        return 0

    slot = 0
    for n in (1, 2, 3, 4):
        if GS.slot_lease(n, "emos-%d" % os.getpid(), ttl=7200):
            slot = n
            break
    if not slot:
        print("  没有空闲 slot"); return 3
    try:
        env = TC.load_env()
        for it in todo:
            url, ms = e.direct_url(it["Id"])
            if not url:
                print("  E%s 无直链，跳过" % it.get("IndexNumber")); continue
            q, w, h = quality_of(ms)
            name = "%s S01E%02d - %s.mkv" % (a.series, int(it.get("IndexNumber") or 0), q)
            # 2026-11-19 事故：上传 lane 的 run_one.sh 结束时会 rm -rf media.slotN，
            # 我们下载到 media.slotN 的文件会被它顺手删掉，表现为
            # 「下载完成 → 上传 FileNotFoundError」。改用独占目录，谁也别动。
            dest = os.path.join("/opt/embytg-runner", "media.emos", name)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            print("  下载 E%s %s %.2fGB → %s" % (it.get("IndexNumber"), q,
                                              (ms.get("Size") or 0)/1e9, name), flush=True)
            t0 = time.time()
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=600) as r, open(dest, "wb") as f:
                while True:
                    chunk = r.read(4 * 1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
            el = max(time.time() - t0, 0.001)
            print("  下载完成 %.2fGB %.1f MB/s" % (os.path.getsize(dest)/1e9,
                                                os.path.getsize(dest)/el/1e6), flush=True)
            parts = None
            for try_slot in [slot] + [x for x in (1, 2, 3, 4) if x != slot]:
                if try_slot != slot and not GS.slot_lease(try_slot, "emos-%d" % os.getpid(), ttl=7200):
                    continue
                try:
                    parts = TC.upload(env, try_slot, dest)
                    break
                except Exception as ex:
                    print("  slot%d 上传失败: %s" % (try_slot, str(ex)[:80]), flush=True)
                    if try_slot != slot:
                        GS.slot_release(try_slot, "emos-%d" % os.getpid())
            if not parts:
                print("  全部 slot 上传失败，保留文件: %s" % dest, flush=True)
                continue
            tdir = a.target_dir or ("TV/国漫/%s" % a.series)
            TC.register(tdir, name, parts)
            write_nfo(os.path.join("/opt/embytg/mnt/embytg", tdir), name, it)
            scan_one(os.path.join("/mnt/embytg", tdir, name))
            os.remove(dest)
        return 0
    finally:
        GS.slot_release(slot, "emos-%d" % os.getpid())


if __name__ == "__main__":
    sys.exit(main())
