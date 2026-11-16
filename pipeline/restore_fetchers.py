# -*- coding: utf-8 -*-
"""
恢复 18 个中文库的「在线文字 MetadataFetchers」，但保持 ImageFetchers=[]。

背景（E003 补刮 / 2026-11-09）：
  nfo_only.py 曾把 18 库四类型的 MetadataFetchers/ImageFetchers 全清空（NFO-only），
  用于绕开 E001（full 模式下 ffprobe 超时死循环）期间的联网阻塞。
  后果：切 NFO-only 之后新建的 379 个 Series 只有文件名 {tmdb=} 锚点，
  Genres/Overview/People 全空；且 MetadataFetchers=[] 时单条 FullRefresh
  的 provider 阶段不启动（日志 RefreshItem Start=0）。
  E001 改 --vfs-cache-mode writes 后 ffprobe 已不是瓶颈，清空 fetcher 的前提消失。

  本脚本只恢复「文字元数据」抓取（从 /Libraries/AvailableOptions 的
  DefaultLibraryOptions.TypeOptions 按 Type 动态取，不硬编码），
  ImageFetchers/ImageFetcherOrder/ImageOptions 继续留 []：
  - 不联网下图、不往本机 /config/metadata 写图（用户要求「能放 TG 全放 TG」）
  - poster 仍读 TG 挂载上的本地文件
  - 不启用 'Image Capture' 本地抽帧（避免 E001 前的 ffmpeg 抽缩略图开销）

  恢复后对元数据不全的条目批量 FullRefresh 即可补齐（finalize_meta.py --refresh）。

用法:
  python3 restore_fetchers.py --dry      # 只打印将改什么
  python3 restore_fetchers.py            # 应用
  python3 restore_fetchers.py --verify   # 复核四类型 meta/img
"""
import json, sys, urllib.request, urllib.error

from embytg_conf import cfg

EMBY = cfg("emby_url")
TOKEN = cfg("emby_token")
HDR = {"X-Emby-Token": TOKEN, "Content-Type": "application/json",
       "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
MOVIE_LIBS = ["剧情片", "喜剧片", "动作片", "惊悚片", "犯罪片", "科幻片",
              "动画片", "纪录片", "战争片", "其他片"]
TV_LIBS = ["国产剧", "日韩剧", "欧美剧", "港台剧", "国漫", "日漫", "综艺"]
ALL_LIBS = MOVIE_LIBS + TV_LIBS


def req(path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(EMBY + path, data=data, headers=HDR, method=method)
    try:
        with urllib.request.urlopen(r, timeout=60) as f:
            raw = f.read()
            return f.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:300]
    except Exception as e:
        return 0, str(e)[:300]


def default_meta_map():
    """从 AvailableOptions 读四类型默认 MetadataFetchers（按 Type）。"""
    st, d = req("/Libraries/AvailableOptions")
    if st != 200:
        print("! 读 AvailableOptions 失败", st, d)
        return {}
    tos = (d.get("DefaultLibraryOptions") or {}).get("TypeOptions") or []
    m = {t.get("Type"): list(t.get("MetadataFetchers") or []) for t in tos}
    print("# 默认文字 fetcher: " + json.dumps(m, ensure_ascii=False))
    return m


def list_libs():
    st, d = req("/Library/VirtualFolders/Query")
    return (d or {}).get("Items", []) if st == 200 else []


def patch_one(vf, dmap, dry, types=None):
    """types=None → 四类型全恢复；types={'Series'} → 只 Series 恢复，其余类型保持 []。

    只恢复 Series 的理由：本次待补刮的 379 条空条目全是 Series；
    Episode 若恢复 TheTVDB 会对 13000+ 集逐集联网，得不偿失。
    未指定类型一律清空，保证把之前单库实验恢复的 4 类型拉回一致状态（幂等）。
    """
    name, iid = vf.get("Name"), (vf.get("ItemId") or vf.get("Id"))
    o = vf.get("LibraryOptions") or {}
    tt = o.get("TypeOptions") or []
    before = [(t.get("Type"), t.get("MetadataFetchers")) for t in tt]
    changed = 0
    for t in tt:
        typ = t.get("Type")
        want = list(dmap.get(typ) or []) if (types is None or typ in types) else []
        if (t.get("MetadataFetchers") or []) != want:
            changed += 1
        t["MetadataFetchers"] = want          # 仅目标类型恢复文字抓取
        t["MetadataFetcherOrder"] = []
        # 图片抓取保持全关（用户「能放 TG 全放 TG」，poster 读本地）
        t["ImageFetchers"] = []
        t["ImageFetcherOrder"] = []
        t["ImageOptions"] = []
    if dry:
        print("  [dry] %-6s id=%-4s meta %s -> %s (改 %d 类型, types=%s)"
              % (name, iid, [x[1] for x in before],
                 [t.get("MetadataFetchers") for t in tt], changed,
                 types or "ALL"))
        return True
    st, res = req("/Library/VirtualFolders/LibraryOptions", "POST",
                  {"Id": iid, "LibraryOptions": o})
    ok = st in (200, 204)
    print("  %-6s id=%-4s code=%s %s" % (name, iid, st, "" if ok else str(res)[:120]))
    return ok


def main():
    a = sys.argv[1:]
    dry, verify = "--dry" in a, "--verify" in a
    only = None
    if "--lib" in a:
        only = a[a.index("--lib") + 1]
    # 默认只恢复 Series 的文字 fetcher；--all-types 恢复四类型（只作对照实验，勿用于生产）
    types = None if "--all-types" in a else {"Series"}
    dmap = default_meta_map()
    if not dmap:
        print("! 无法取得默认 fetcher，退出（不改动任何库）")
        return
    libs = [v for v in list_libs() if v.get("Name") in ALL_LIBS]
    if only:
        libs = [v for v in libs if v.get("Name") == only]
        print("# 单库模式: %s" % only)
    print("# 找到 %d 个库待处理 (types=%s)" % (len(libs), types or "ALL"))
    if not verify:
        ok = sum(1 for v in libs if patch_one(v, dmap, dry, types))
        print("# 已处理 %d 个库 (dry=%s)" % (ok, dry))
    if verify or dry:
        print("\n# 复核")
        shown = [v for v in list_libs() if v.get("Name") in ALL_LIBS and (not only or v.get("Name") == only)]
        for v in shown:
            tt = (v.get("LibraryOptions") or {}).get("TypeOptions") or []
            print("  %-6s meta=%s img=%s"
                  % (v.get("Name"), [t.get("MetadataFetchers") for t in tt],
                     [t.get("ImageFetchers") for t in tt]))


if __name__ == "__main__":
    main()
