#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NFO 前置生成器：并发直连 TMDB 为所有电影/剧集生成标准 NFO + poster。

思路（对应用户的"NFO 前置加速"方案）：
  - 文件名已带 {tmdb=ID} 锚点，无需再搜索，直接按 ID 拉 TMDB detail 中文元数据。
  - 32 线程并发直连 api.themoviedb.org（比 Emby 单线程 provider 快几十倍）。
  - 生成标准 NFO：
      电影:  <电影目录>/<电影文件名>.nfo  (与媒体文件同名)
      剧集:  <Series 目录>/tvshow.nfo  (Kodi/Emby 剧集约定文件名)
  - 下载 poster 到同目录 (poster.jpg / poster.png)。
  - 之后把 Emby 库改成只读 NFO，扫描即为纯本地，秒级完成。

用法 (在 /opt/embytg/pipeline 下):
  python3 nfo_gen.py --scan            # 只列出待生成清单，不写文件
  python3 nfo_gen.py                   # 全量并发生成
  python3 nfo_gen.py --workers 48      # 指定并发数
"""
import html, json, os, re, sys, time, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

from embytg_conf import cfg  # 统一配置真源: /opt/embytg/config.json

MOUNT = cfg("mount")
TMDB_API = cfg("tmdb_api")
TMDB_TK = cfg("tmdb_token")
UA = cfg("ua")
HDR = {"Authorization": "Bearer " + TMDB_TK, "User-Agent": UA, "Accept": "application/json"}

MOVIE_EXT = (".mp4", ".mkv", ".mov", ".avi", ".wmv", ".ts", ".m2ts", ".flv", ".rmvb", ".webm", ".iso")
ZERO_WIDTH_RE = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")
IMG_EXT = (".jpg", ".jpeg", ".png", ".webp")
HAN_RE = re.compile(r"[\u4e00-\u9fff]")

TMDB_GENRES = {
    28: "动作", 12: "冒险", 16: "动画", 35: "喜剧", 80: "犯罪", 99: "纪录",
    18: "剧情", 10751: "家庭", 14: "奇幻", 36: "历史", 27: "恐怖", 10402: "音乐",
    9648: "悬疑", 10749: "爱情", 878: "科幻", 10770: "电视电影", 53: "惊悚",
    10752: "战争", 37: "西部",
}
TMDB_TV_GENRES = {
    10759: "动作冒险", 16: "动画", 35: "喜剧", 80: "犯罪", 99: "纪录", 18: "剧情",
    10751: "家庭", 10762: "儿童", 9648: "悬疑", 10763: "新闻", 10764: "真人秀",
    10765: "科幻奇幻", 10766: "肥皂剧", 10767: "脱口秀", 10768: "战争政治", 37: "西部",
}


def http_json(url):
    r = urllib.request.Request(url, headers=HDR)
    try:
        with urllib.request.urlopen(r, timeout=20) as f:
            return json.loads(f.read().decode("utf-8", "ignore"))
    except Exception:
        return {}


def gen_names(genre_ids, tv=False):
    m = TMDB_TV_GENRES if tv else TMDB_GENRES
    return [m.get(g, "") for g in (genre_ids or []) if m.get(g)]


def xml_esc(s):
    if not s:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def clean_title(text):
    """NFO/目录名/TMDb 标题统一清洗：XML 实体、零宽字符、首尾空白。"""
    s = html.unescape(text or "")
    s = ZERO_WIDTH_RE.sub("", s)
    return s.strip()


def folder_title(dirpath):
    """从 Series 目录名提取人工维护的标题，去掉年份和 tmdb 锚点。"""
    base = os.path.basename(os.path.normpath(dirpath))
    base = re.sub(r"\s*\{tmdb=\d+\}\s*$", "", base, flags=re.I)
    base = re.sub(r"\s*\(\d{4}\)\s*$", "", base)
    return clean_title(base)


def has_han(text):
    return bool(HAN_RE.search(text or ""))


def tv_display_title(md, dirpath):
    """TMDB 中文名优先；缺中文译名时用中文目录名兜底，避免写进英文标题。"""
    tmdb_name = clean_title(md.get("name"))
    original_name = clean_title(md.get("original_name"))
    folder = folder_title(dirpath)
    if has_han(tmdb_name):
        return tmdb_name
    if has_han(folder):
        return folder
    return tmdb_name or original_name or folder


def write_movie_nfo(path, md):
    """电影 NFO：与媒体文件同名 .nfo。"""
    base = os.path.splitext(path)[0]
    nfo = base + ".nfo"
    # 同一分类目录下有多部电影，poster 必须与媒体文件同名，不能共用 poster.jpg
    poster = base + "-poster.jpg"
    g = gen_names(md.get("genres") and [x.get("id") for x in md.get("genres")] or [])
    rows = [
        '<?xml version="1.0" encoding="utf-8"?>',
        "<movie>",
        "  <title>%s</title>" % xml_esc(md.get("title") or md.get("original_title") or os.path.basename(base)),
        "  <originaltitle>%s</originaltitle>" % xml_esc(md.get("original_title") or ""),
        "  <year>%s</year>" % ((md.get("release_date") or "")[:4]),
        "  <releasedate>%s</releasedate>" % (md.get("release_date") or ""),
        "  <runtime>%s</runtime>" % (md.get("runtime") or ""),
        "  <plot>%s</plot>" % xml_esc(md.get("overview") or ""),  # NFO 标准字段是 plot；<overview> 会被 BaseNfoParser 的 default 分支 SkipAsync 丢弃
        "  <tagline>%s</tagline>" % xml_esc(md.get("tagline") or ""),
        "  <vote>%s</vote>" % (md.get("vote_average") or ""),
        "  <status>%s</status>" % (md.get("status") or ""),
        "  <tmdbid>%s</tmdbid>" % md.get("id"),
        "  <imdbid>%s</imdbid>" % ((md.get("imdb_id") or "").replace("tt", "")),
        "  <country>%s</country>" % xml_esc(",".join(x.get("name") or "" for x in md.get("production_countries") or [])),
    ]
    for gname in g:
        rows.append("  <genre>%s</genre>" % xml_esc(gname))
    for x in (md.get("production_companies") or [])[:3]:
        rows.append("  <studio>%s</studio>" % xml_esc(x.get("name") or ""))
    for c in (md.get("credits") and (md.get("credits").get("cast") or []))[:20] or []:
        rows.append("  <actor>")
        rows.append("    <name>%s</name>" % xml_esc(c.get("name") or ""))
        rows.append("    <role>%s</role>" % xml_esc(c.get("character") or ""))
        rows.append("    <order>%s</order>" % (c.get("order") or ""))
        rows.append("  </actor>")
    for c in (md.get("credits") and (md.get("credits").get("crew") or []))[:6] or []:
        if (c.get("job") or "").lower() in ("director", "writer", "producer"):
            rows.append("  <director>%s</director>" % xml_esc(c.get("name") or ""))
    rows.append("</movie>")
    with open(nfo, "w", encoding="utf-8") as f:
        f.write("\n".join(rows) + "\n")
    poster_path = dl_poster(md.get("poster_path"), poster)
    return nfo, poster_path


def write_tv_nfo(dirpath, md):
    """剧集 NFO：Series 目录 tvshow.nfo。"""
    nfo = os.path.join(dirpath, "tvshow.nfo")
    poster = os.path.join(dirpath, "poster.jpg")
    display_title = tv_display_title(md, dirpath)
    g = gen_names([x.get("id") for x in md.get("genres") or []], tv=True)
    rows = [
        '<?xml version="1.0" encoding="utf-8"?>',
        "<tvshow>",
        "  <title>%s</title>" % xml_esc(display_title),
        "  <originaltitle>%s</originaltitle>" % xml_esc(md.get("original_name") or ""),
        "  <year>%s</year>" % ((md.get("first_air_date") or "")[:4]),
        "  <premiered>%s</premiered>" % (md.get("first_air_date") or ""),
        "  <status>%s</status>" % (md.get("status") or ""),
        "  <runtime>%s</runtime>" % (md.get("episode_run_time") or [0])[0],
        "  <plot>%s</plot>" % xml_esc(md.get("overview") or ""),  # NFO 标准字段是 plot；<overview> 会被 BaseNfoParser 的 default 分支 SkipAsync 丢弃
        "  <vote>%s</vote>" % (md.get("vote_average") or ""),
        "  <tmdbid>%s</tmdbid>" % md.get("id"),
        "  <country>%s</country>" % xml_esc(",".join(str(x) for x in (md.get("origin_country") or []))),
    ]
    for gname in g:
        rows.append("  <genre>%s</genre>" % xml_esc(gname))
    for x in (md.get("networks") or [])[:3]:
        rows.append("  <studio>%s</studio>" % xml_esc(x.get("name") or ""))
    for c in (md.get("credits") and (md.get("credits").get("cast") or []))[:20] or []:
        rows.append("  <actor>")
        rows.append("    <name>%s</name>" % xml_esc(c.get("name") or ""))
        rows.append("    <role>%s</role>" % xml_esc(c.get("character") or ""))
        rows.append("    <order>%s</order>" % (c.get("order") or ""))
        rows.append("  </actor>")
    for c in (md.get("credits") and (md.get("credits").get("crew") or []))[:6] or []:
        if (c.get("job") or "").lower() in ("director", "producer"):
            rows.append("  <director>%s</director>" % xml_esc(c.get("name") or ""))
    rows.append("</tvshow>")
    with open(nfo, "w", encoding="utf-8") as f:
        f.write("\n".join(rows) + "\n")
    poster_path = dl_poster(md.get("poster_path"), poster)
    return nfo, poster_path


def dl_poster(poster_path, dest):
    if not poster_path:
        return None
    url = "https://image.tmdb.org/t/p/w500" + poster_path
    try:
        r = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(r, timeout=20) as f:
            data = f.read()
        with open(dest, "wb") as f:
            f.write(data)
        return dest
    except Exception:
        return None


ID_RE = re.compile(r"[{[]tmdb(?:id)?[-=](\d+)[}\]]", re.I)


def scan():
    """遍历挂载，收集 (kind, id, name, dirpath, mediafile) 清单。"""
    movies, series = [], []
    seen = set()
    # Movies
    mdir = os.path.join(MOUNT, "Movies")
    for root, dirs, files in os.walk(mdir):
        for fn in files:
            ext = os.path.splitext(fn)[1].lower()
            if ext in IMG_EXT or ext == ".nfo":
                continue
            if ext not in MOVIE_EXT:
                continue
            m = ID_RE.search(fn)
            if not m:
                continue
            mid = m.group(1)
            base = os.path.splitext(fn)[0]
            key = ("movie", mid)
            if key in seen:
                continue
            seen.add(key)
            movies.append({"kind": "movie", "id": mid, "name": base,
                           "dir": root, "media": os.path.join(root, fn)})
    # TV series (文件夹)
    tvdir = os.path.join(MOUNT, "TV")
    for root, dirs, files in os.walk(tvdir):
        # 只处理含 Season NN 子目录的 Series 文件夹
        if any(re.match(r"^Season\s*\d+", d) for d in dirs):
            m = ID_RE.search(os.path.basename(root))
            if not m:
                continue
            sid = m.group(1)
            key = ("tv", sid)
            if key in seen:
                continue
            seen.add(key)
            series.append({"kind": "tv", "id": sid,
                           "name": os.path.basename(root), "dir": root})
    return movies, series


def nfo_paths(it):
    """返回该项应有的 (nfo, poster) 路径。电影与媒体同名，剧集在 Series 根目录。"""
    if it["kind"] == "movie":
        base = os.path.splitext(it["media"])[0]
        return base + ".nfo", base + "-poster.jpg"
    return os.path.join(it["dir"], "tvshow.nfo"), os.path.join(it["dir"], "poster.jpg")


def already_done(it):
    """E012 增量判据：NFO 与 poster 都已存在且非空 → 跳过，避免每次全量重写
    4577 个文件把 vfs cache 写爆（teldrive 每个小文件上传要发一条 TG 消息，
    1.3s/个，9147 个积压要 50-90 分钟，且下次重启会卡在 cache 恢复阶段）。"""
    nfo, poster = nfo_paths(it)
    try:
        if not (os.path.exists(nfo) and os.path.getsize(nfo) > 200):
            return False
        if not (os.path.exists(poster) and os.path.getsize(poster) > 1000):
            return False
        return True
    except OSError:
        return False


def fetch_movie(it):
    md = http_json("%s/movie/%s?language=zh-CN&append_to_response=credits" % (TMDB_API, it["id"]))
    if not md or not md.get("id"):
        return it, None
    try:
        nfo, poster = write_movie_nfo(it["media"], md)
        return it, {"nfo": nfo, "poster": poster, "name": md.get("title") or md.get("original_title")}
    except Exception as e:
        return it, {"err": str(e)[:80]}


def fetch_tv(it):
    md = http_json("%s/tv/%s?language=zh-CN&append_to_response=credits" % (TMDB_API, it["id"]))
    if not md or not md.get("id"):
        return it, None
    try:
        nfo, poster = write_tv_nfo(it["dir"], md)
        return it, {"nfo": nfo, "poster": poster, "name": tv_display_title(md, it["dir"])}
    except Exception as e:
        return it, {"err": str(e)[:80]}


def main():
    a = sys.argv[1:]
    scan_only = "--scan" in a
    force = "--force" in a
    workers = 32
    kind = "all"
    for i, x in enumerate(a):
        if x == "--workers" and i + 1 < len(a):
            workers = int(a[i + 1])
        if x == "--kind" and i + 1 < len(a):
            kind = a[i + 1]
    movies, series = scan()
    if kind == "movie":
        series = []
    elif kind == "tv":
        movies = []
    print("# 扫描挂载 %s" % MOUNT)
    print("# 电影 %d 个 / 剧集 %d 部 (kind=%s)" % (len(movies), len(series), kind), flush=True)
    if scan_only:
        for it in movies[:5]:
            print("   movie %s  %s" % (it["id"], it["name"]))
        for it in series[:5]:
            print("   tv    %s  %s" % (it["id"], it["name"]))
        return
    # E012 增量模式：NFO+poster 都齐的直接跳过，避免每次全量重写把 vfs cache 写爆
    if not force:
        n_m, n_s = len(movies), len(series)
        movies = [m for m in movies if not already_done(m)]
        series = [s for s in series if not already_done(s)]
        skipped = (n_m - len(movies)) + (n_s - len(series))
        if skipped:
            print("# 增量：跳过已完成 %d 项，待处理 %d 项（--force 可强制全量）"
                  % (skipped, len(movies) + len(series)), flush=True)
    todo = movies + series
    if not todo:
        print("# 无待处理项，退出", flush=True)
        return
    print("# 并发 %d 线程生成 NFO+poster ..." % workers, flush=True)
    ok = fail = nposter = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {}
        for it in movies:
            futs[ex.submit(fetch_movie, it)] = it
        for it in series:
            futs[ex.submit(fetch_tv, it)] = it
        for n, f in enumerate(as_completed(futs), 1):
            it, res = f.result()
            if res and res.get("nfo"):
                ok += 1
                if res.get("poster"):
                    nposter += 1
                if n % 500 == 0:
                    print("   .. %d/%d  ok=%d poster=%d fail=%d  %.1fs" % (n, len(todo), ok, nposter, fail, time.time() - t0), flush=True)
            else:
                fail += 1
                print("   ! %s id=%s %s" % (it["kind"], it["id"], (res or {}).get("err", "no nfo")), file=sys.stderr, flush=True)
    print("# 完成 ok=%d poster=%d fail=%d / %.1fs" % (ok, nposter, fail, time.time() - t0), flush=True)


if __name__ == "__main__":
    main()
