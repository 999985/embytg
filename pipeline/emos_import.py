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
import datetime
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgsrc_copy as TC                                    # noqa: E402  (upload/register 复用)

BASE = "https://emos.enic65.eu.org"
UA = "Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)"
HERE = os.path.dirname(os.path.abspath(__file__))
USER, PW = "enic", "enic"


class Emos:
    def __init__(self):
        self.tok = None
        self.uid = None

    def _req(self, path, q=None, data=None, method="GET", timeout=60, stream=False,
             _relogin=True):
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
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                if stream:
                    return r
                return json.loads(r.read())
        except urllib.error.HTTPError as ex:
            # 令牌会过期：夜间窗口一个进程能跑 6h，PlaybackInfo 往往在排队等 slot
            # 1-2h 之后才发出。旧代码只在进程启动时 login 一次，令牌过期后整集
            # 直接 401 报废（2026-11-22 E20）。401 时重登一次再重试，过期时长无关。
            if ex.code == 401 and _relogin:
                self.login()
                return self._req(path, q, data, method, timeout, stream, _relogin=False)
            raise

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
    """源站 PlaybackInfo 自报的画质：只当线索，**入库命名必须用实测分辨率**。

    2026-11-21 事故：源站把 720x576 的 DVD 花絮标成「720p」，旧代码照抄标签，
    于是 `武林外传 S01E01 - 720p.mkv` 实际是 SD，daily_ops 判「标720p实SD」。
    """
    for s in ms.get("MediaStreams") or []:
        if s.get("Type") == "Video" and s.get("Height"):
            h, w = s["Height"], s.get("Width") or 0
            return quality_from_dims(w, h), w, h
    return "SD", 0, 0


Q_ORDER = {"sd": 0, "720p": 1, "1080p": 2, "2160p": 3}
TGDB = os.path.join(HERE, "tg.db")


def quality_from_dims(w, h):
    w, h = int(w or 0), int(h or 0)
    if w >= 3000 or h >= 1800:
        return "2160p"
    if w >= 1700 or h >= 1000:
        return "1080p"
    if w >= 1200 or h >= 700:
        return "720p"
    return "SD"


def q_rank(q):
    return Q_ORDER.get(str(q or "").lower(), 0)


def probe_dims(path):
    """ffprobe 实测下载文件的分辨率；读不出来返回 (0,0)。"""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "json", path],
            capture_output=True, text=True, timeout=120)
        st = (json.loads(r.stdout or "{}").get("streams") or [{}])[0]
        return int(st.get("width") or 0), int(st.get("height") or 0)
    except Exception:                                          # noqa: BLE001
        return 0, 0


def _fold_rel_dir(dirname):
    """目录名去掉年份与 {tmdb=} 锚点后的剧名。"""
    d = re.sub(r"\s*\{tmdb=\d+\}\s*$", "", dirname, flags=re.I)
    d = re.sub(r"\s*\(\d{4}\)\s*$", "", d)
    return d.strip()


def resolve_series_dir(name, tmdb=0):
    """从台账里找已存在的 **带 {tmdb=} 锚点** 的剧目录。

    为什么必须从台账反查（2026-11-21）：默认写到 `TV/国漫/<剧名>` 是无锚点、
    无 Season 目录的野目录 —— Emby 会另建一个同名剧，五账里 teldrive 有、state_key 没有，
    用户搜索看到两部「武林外传」。找不到锚点目录就拒绝入库，宁缺毋错。
    返回 (TV/lib/dir, tmdb, lib) 或 (None, 0, "")。
    """
    try:
        con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=20)
        try:
            rows = con.execute(
                "select rel from resource_state where kind='episode' and rel like 'TV/%'"
            ).fetchall()
        finally:
            con.close()
    except sqlite3.Error:
        return None, 0, ""
    cand = {}
    for (rel,) in rows:
        parts = (rel or "").split("/")
        if len(parts) < 4:
            continue
        m = re.search(r"\{tmdb=(\d+)\}", parts[2])
        if not m:
            continue
        if tmdb and int(m.group(1)) != int(tmdb):
            continue
        if _fold_rel_dir(parts[2]) != name and name not in _fold_rel_dir(parts[2]):
            continue
        key = "/".join(parts[:3])
        cand[key] = cand.get(key, 0) + 1
    if not cand:
        return None, 0, ""
    best = max(cand.items(), key=lambda kv: (kv[1], kv[0]))[0]
    m = re.search(r"\{tmdb=(\d+)\}", best)
    return best, (int(m.group(1)) if m else 0), best.split("/")[1]


def existing_best(tmdb, season, ep):
    """该集已有文件的最高画质（台账口径）。"""
    try:
        con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=20)
        try:
            rows = con.execute(
                "select rel,width,height from resource_state where kind='episode' and tmdb=?",
                (int(tmdb),)).fetchall()
        finally:
            con.close()
    except sqlite3.Error:
        return "SD"
    best = "SD"
    want = re.compile(r"S%02dE%02d" % (int(season), int(ep)), re.I)
    for rel, w, h in rows:
        if not want.search(rel or ""):
            continue
        q = quality_from_dims(w, h) if (w or h) else "SD"
        if q_rank(q) > q_rank(best):
            best = q
    return best


def register_tgdb(folders, fid, name, parts, tmdb, lib, ch="emos"):
    """上传+PG 登记之后补写 tg.db（state_key/resource_state 的锚点）。

    2026-11-21：旧代码只写 teldrive，state_key 里没有这条 → 五账对不上、
    fill 判不出「已有」、每日对账也发现不了（它只比总数）。
    """
    from embytg_conf import store_channel as _store_channel
    ch_id = 0
    try:
        ch_id = int(_store_channel())
    except Exception:                                          # noqa: BLE001
        ch_id = 0
    mani = {"name": name, "mime_type": "video/x-matroska", "channel_id": ch_id,
            "parts": [{"part_no": i, "channel_id": ch_id, "message_id": int(mid),
                       "plain_size": int(sz), "stored_size": int(sz)}
                      for i, (mid, sz) in enumerate(parts, 1)]}
    cmd = [sys.executable, os.path.join(HERE, "pc_register.py"), "--folders", folders,
           "--file-id", fid, "--tgdb", "--ch", ch, "--tmdb", str(int(tmdb)), "--lib", lib]
    try:
        r = subprocess.run(cmd, input=json.dumps(mani, ensure_ascii=False),
                           capture_output=True, text=True, timeout=180)
        out = ((r.stdout or "") + (r.stderr or "")).strip().splitlines()
        print("  tg.db 登记: %s" % (out[-1][:120] if out else "rc=%d" % r.returncode), flush=True)
        return r.returncode == 0
    except Exception as ex:                                    # noqa: BLE001
        print("  tg.db 登记失败: %s" % str(ex)[:80], flush=True)
        return False


def write_nfo(dir_path, name, item, season=None):
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
                    % (su.escape(title), season or item.get("ParentIndexNumber") or 1,
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


def _deadline():
    """夜间窗口的硬截止（epoch 秒）。无 EMOS_DEADLINE 时返回 0 = 不设限。"""
    v = os.environ.get("EMOS_DEADLINE", "").strip()
    try:
        return float(v) if v else 0.0
    except ValueError:
        return 0.0


def _remaining():
    dl = _deadline()
    return (dl - time.time()) if dl else 1e9


def _mani_parts(name):
    """这条文件已经落地的区段数（读 pc_upload 的 manifest，跨尝试单调递增）。

    探针判活必须用「本次尝试新增」而不是输出里的「已完成 N」——后者把上次已存在的
    区段也数进去（2026-11-24 实测：slot4 一个字节没传，输出仍写着「已完成 2」，
    只看它就会把死号判成活着，白等 37 分钟）。
    """
    import glob
    for p in glob.glob("/opt/embytg-runner/_manifests*/%s.json" % name):
        try:
            with open(p, encoding="utf-8") as f:
                return len((json.load(f) or {}).get("parts") or [])
        except Exception:                                  # noqa: BLE001
            continue
    return 0


def pool_rows():
    """{slot: (实测单 worker MB/s, 是否冷却)}，直接读上传池，不猜速度。

    2026-11-24：平滑值会滞后（slot2 表里 1.09，last_mbps 只有 0.02 —— 闸门据此算出
    「8×1.09≈8.7MB/s」很乐观，实际 18 分钟一个区段都没落地）。排序用「平滑值」和
    「最近一次实测」的较小者，避免总是先撞上已经半死的号。
    """
    out = {}
    for r in TC.pool_snapshot():
        mb = float(r.get("mbps") or 0)
        last = float(r.get("last_mbps") or 0)
        samples = [x for x in (mb, last) if x > 0]
        out[int(r["slot"])] = (min(samples) if samples else 0.0, bool(r.get("cooling")))
    return out


def slot_order(rows):
    """按实测吞吐从快到慢；冷却中的排最后（仍可兜底尝试）。"""
    # 2026-11-20：池表里的吞吐可能过期（slot1 记着 1.18，实际 0.14）。
    # 支持 EMOS_SLOTS 白名单：只在这些号里挑，避开长期被限速的号。
    allow = [int(x) for x in (os.environ.get("EMOS_SLOTS") or "").replace(" ", "").split(",")
             if x.isdigit()] or [1, 2, 3, 4]
    return sorted([n for n in allow],
                  key=lambda n: (1 if rows.get(n, (0.0, False))[1] else 0,
                                 -rows.get(n, (0.0, False))[0], n))


def acquire_slot(run_id, jobs):
    """领一个「够快」的空闲号。

    先按实测吞吐从快到慢试租约，领到后再用**这个号**的实测值过闸，不够快立刻还回去。
    否则会出现「闸门看到 slot1 很快（但它被 lane 占着），实际只领到 0.23MB/s 的 slot3」
    这种自欺 —— 昨夜 7 集白烧就是这么发生的：先领号，后看速度。
    """
    floor = float(os.environ.get("EMOS_MIN_OVERALL_MBPS", "2.5"))
    dl = os.environ.get("EMOS_WAIT_DEADLINE", "07:30")
    hh, mm = [int(x) for x in dl.split(":")]
    deadline = datetime.datetime.now().replace(hour=hh, minute=mm, second=0, microsecond=0)
    while True:
        rows = pool_rows()
        for n in slot_order(rows):
            if not GS.slot_lease(n, run_id, ttl=7200):
                continue
            mb = rows.get(n, (0.0, False))[0] or 1.5      # 无实测按中性 1.5
            est = mb * max(1, jobs)
            if est >= floor:
                print("  领到 slot%d（实测 %.2fMB/s×%d≈%.1fMB/s）"
                      % (n, mb, jobs, est), flush=True)
                return n
            GS.slot_release(n, run_id)
            print("  slot%d 实测 %.2fMB/s×%d≈%.1f < %.1f，不占它"
                  % (n, mb, jobs, est, floor), flush=True)
        if datetime.datetime.now() >= deadline:
            if os.environ.get("EMOS_ALLOW_SLOW") == "1":
                # best-effort：闸门是「别把整夜押在慢号上」，不是「宁可不跑」。
                # 探针（EMOS_PROBE_S）会在 10 分钟内判定它到底活不活。
                rows2 = pool_rows()
                for n in slot_order(rows2):
                    if GS.slot_lease(n, run_id, ttl=7200):
                        print("  没有够快的空闲 slot，best-effort 用 slot%d（实测 %.2fMB/s）"
                              % (n, rows2.get(n, (0.0, False))[0]), flush=True)
                        return n
            print("  没有够快的空闲 slot：全线 < %.1fMB/s，已过 %s，本轮不起跑"
                  % (floor, dl), flush=True)
            return 0
        print("  没有够快的空闲 slot，等 10 分钟再试", flush=True)
        time.sleep(600)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--year", type=int, default=0)
    ap.add_argument("--eps", default="", help="绝对集号，如 2-5,9")
    ap.add_argument("--target-dir", default="", help="teldrive 目录；缺省按台账里的 {tmdb=} 锚点目录解析")
    ap.add_argument("--tmdb", type=int, default=0)
    ap.add_argument("--season", type=int, default=1)
    ap.add_argument("--min-quality", default="720p", help="实测画质低于此档不导入（默认 720p）")
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

    run_id = "emos-%d" % os.getpid()
    # 目标目录：只允许落在**台账里已存在的、带 {tmdb=} 锚点**的剧目录。
    # 旧代码缺省写到 `TV/国漫/<剧名>`，于是 Emby 另建同名剧、state_key 缺失（2026-11-21 事故）。
    if a.target_dir:
        tdir = a.target_dir.strip("/")
        m = re.search(r"\{tmdb=(\d+)\}", tdir)
        if not m or not re.search(r"/Season\s*\d+$", tdir):
            print("  拒绝：--target-dir 必须形如 TV/<库>/<剧 (年) {tmdb=N}>/Season NN：%s" % tdir)
            return 2
        tmdb = a.tmdb or int(m.group(1))
        lib = tdir.split("/")[1]
    else:
        base, tmdb0, lib = resolve_series_dir(a.series, a.tmdb)
        if not base:
            print("  拒绝：台账里找不到带 {tmdb=} 锚点的《%s》目录 —— 不允许写到无锚点野目录"
                  "（会造成同名剧/五账不平，宁缺毋错）" % a.series)
            return 2
        tmdb = a.tmdb or tmdb0
        tdir = "%s/Season %02d" % (base, a.season)
    print("  目标目录: %s（tmdb=%s lib=%s）" % (tdir, tmdb, lib), flush=True)
    jobs = int(os.environ.get("TG_UPLOAD_JOBS", "8") or 8)
    slot = acquire_slot(run_id, jobs)
    if not slot:
        return 2
    order = slot_order(pool_rows())
    # 2026-11-24：窗口是有墙钟的（systemd TimeoutStartSec=6h）。以前一个 1.6GB 的集
    # 会挨个号硬试（每个 900–2400s 预算），4 个号烧掉 87 分钟仍 0 产出，
    # 首个集就把整夜吃掉。现在：单集最多试 EMOS_TRY_SLOTS 个号（默认不限，夜间设 2），
    # 且每次尝试前先看剩余预算，不够 300s 就不起。
    _cap = int(os.environ.get("EMOS_TRY_SLOTS", "0") or 0)
    done = 0
    attempted = 0
    try:
        env = TC.load_env()
        for it in todo:
            if _remaining() < 420:
                print("  窗口预算只剩 %ds，本轮提前收工" % max(0, int(_remaining())), flush=True)
                break
            url, ms = e.direct_url(it["Id"])
            if not url:
                print("  E%s 无直链，跳过" % it.get("IndexNumber")); continue
            ep = int(it.get("IndexNumber") or 0)
            title = (it.get("Name") or "").strip()
            if re.search(r"(花絮|预告|特典|制作特辑|Making|Trailer|NCOP|NCED)", title, re.I):
                print("  E%s 是花絮/预告（%s），不当作正片导入" % (ep, title[:24])); continue
            q, w, h = quality_of(ms)
            # 源站自报的画质只当线索：低于下限不下载，已有同档/更高档也不算升级。
            if q_rank(q) < q_rank(a.min_quality):
                print("  E%s 源侧 %s（%sx%s）低于 %s，跳过" % (ep, q, w, h, a.min_quality)); continue
            have = existing_best(tmdb, a.season, ep)
            if q_rank(have) >= q_rank(q):
                print("  E%s 已有 %s ≥ 源侧 %s，不算升级，跳过" % (ep, have, q)); continue
            # 2026-11-19 事故：上传 lane 的 run_one.sh 结束时会 rm -rf media.slotN，
            # 我们下载到 media.slotN 的文件会被它顺手删掉，表现为
            # 「下载完成 → 上传 FileNotFoundError」。改用独占目录，谁也别动。
            tmp = os.path.join("/opt/embytg-runner", "media.emos",
                               "emos_%s_S%02dE%02d.download" % (a.series, a.season, ep))
            os.makedirs(os.path.dirname(tmp), exist_ok=True)
            print("  下载 E%s 源侧 %s %.2fGB" % (ep, q, (ms.get("Size") or 0)/1e9), flush=True)
            t0 = time.time()
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=600) as r, open(tmp, "wb") as f:
                while True:
                    chunk = r.read(4 * 1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
            el = max(time.time() - t0, 0.001)
            print("  下载完成 %.2fGB %.1f MB/s" % (os.path.getsize(tmp)/1e9,
                                                os.path.getsize(tmp)/el/1e6), flush=True)
            # 命名/入库一律按 **ffprobe 实测**，不照抄源站标签（2026-11-21 假 720p 事故）。
            rw, rh = probe_dims(tmp)
            real_q = quality_from_dims(rw, rh) if (rw or rh) else q
            if (rw or rh) and real_q != q:
                print("  E%s 源站标 %s，实测 %s（%sx%s），按实测处理" % (ep, q, real_q, rw, rh), flush=True)
            if q_rank(real_q) < q_rank(a.min_quality):
                print("  E%s 实测 %sx%s = %s 低于 %s，丢弃（源站虚标）" % (ep, rw, rh, real_q, a.min_quality))
                os.remove(tmp); continue
            if q_rank(real_q) <= q_rank(have):
                print("  E%s 实测 %s 不优于已有 %s，丢弃" % (ep, real_q, have))
                os.remove(tmp); continue
            name = "%s S%02dE%02d - %s.mkv" % (a.series, a.season, ep, real_q)
            dest = os.path.join("/opt/embytg-runner", "media.emos", name)
            os.replace(tmp, dest)
            parts = None
            # 快的号先试；超时要换号续传（manifest 认已完成区段），不能一失败就丢集
            attempted += 1
            tries = [slot] + [x for x in order if x != slot]
            if _cap > 0:
                tries = tries[:_cap]
            ep_t0 = time.time()
            ep_cap = float(os.environ.get("EMOS_EP_MAX_S", "0") or 0)
            for try_slot in tries:
                if try_slot != slot and not GS.slot_lease(try_slot, run_id, ttl=7200):
                    continue
                for _att in range(2):        # 每号最多两次：探针（短）→ 整预算续传
                    if _remaining() < 300:
                        print("  E%s 剩余预算 < 300s，不再开新号，留到下一轮" % ep, flush=True)
                        break
                    if ep_cap and time.time() - ep_t0 > ep_cap:
                        print("  E%s 单集已耗 %ds > %ds，放弃换号，留到下一轮"
                              % (ep, int(time.time() - ep_t0), int(ep_cap)), flush=True)
                        break
                    _before = 0
                    try:
                        _before = _mani_parts(name)
                        parts = TC.upload(env, try_slot, dest)
                    except (Exception, SystemExit) as ex:
                        print("  slot%d 上传失败: %s" % (try_slot, str(ex)[:100]), flush=True)
                        # 探针判定用 manifest 增量：本次新增区段 = 慢但活着（续传不丢字节）；
                        # 0 新增 = 死号，换下一个。
                        _prog = _mani_parts(name) - _before
                        if _prog > 0 and _att == 0:
                            os.environ.pop("EMOS_PROBE_S", None)
                            print("  slot%d 探针新增 %d 个区段（活着），给整预算续传"
                                  % (try_slot, _prog), flush=True)
                            continue
                        print("  slot%d 本次新增 %d 个区段（%s）"
                              % (try_slot, _prog, "活着但不快" if _prog else "死号"),
                              flush=True)
                        break
                    else:
                        os.environ.pop("EMOS_PROBE_S", None)
                        break
                if try_slot != slot:
                    GS.slot_release(try_slot, run_id)   # 借来的号用完立刻还，别占 2h
                if parts:
                    break
            if not parts:
                print("  全部 slot 上传失败，保留文件: %s" % dest, flush=True)
                continue
            fid = TC.register(tdir, name, parts)
            # 旧代码只写 teldrive；不写 tg.db 就是「五账里少一本」，对账只比总数发现不了。
            if tmdb and not register_tgdb(tdir, fid, name, parts, tmdb, lib):
                print("  tg.db 登记失败：本条留在 teldrive 但不计入台账，需人工复核", flush=True)
            write_nfo(os.path.join("/opt/embytg/mnt/embytg", tdir), name, it, season=a.season)
            scan_one(os.path.join("/mnt/embytg", tdir, name))
            os.remove(dest)
            done += 1
            print("  E%d 入库完成（本轮 %d/%d）" % (ep, done, attempted), flush=True)
        if attempted and not done:
            # 以前这里无条件 return 0 → 上层把「上传全失败、库里根本没有这一集」
            # 也计成成功（2026-11-24 E43：4 个号全超时，rc=0）。
            print("  本轮 %d 集全部上传失败，0 集入库" % attempted, flush=True)
            return 3
        return 0
    finally:
        GS.slot_release(slot, run_id)


if __name__ == "__main__":
    sys.exit(main())
