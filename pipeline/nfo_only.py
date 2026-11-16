#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 18 个中文库切换为「只读本地 NFO」模式。

目的：Emby 扫描时不再走网络 provider（TheMovieDb/TheTVDB 单线程、每条 1-2 秒），
直接读我们预先并发生成好的 NFO + poster，扫描退化为纯本地 IO。

应用前把原始 LibraryOptions 备份到 lib_opts_before_nfo.json，支持 --restore 回滚。

用法:
  python3 nfo_only.py --dry      # 只打印将要改什么
  python3 nfo_only.py            # 应用（自动备份）
  python3 nfo_only.py --verify   # 应用后复核
  python3 nfo_only.py --restore  # 从备份恢复原始配置
"""
import json, os, sys, urllib.request, urllib.error

from embytg_conf import cfg  # 统一配置真源: /opt/embytg/config.json

EMBY = cfg("emby_url")
TOKEN = cfg("emby_token")
BACKUP = cfg("lib_opts_before_nfo")

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


def list_libs():
    st, d = req("/Library/VirtualFolders/Query")
    if st != 200:
        print("! 读取库失败", st, d)
        return []
    return d.get("Items", [])


def patch_one(vf, dry):
    name = vf.get("Name")
    iid = vf.get("ItemId") or vf.get("Id")
    o = vf.get("LibraryOptions") or {}
    before_readers = o.get("MetadataReaders")
    before_tt = [(t.get("Type"), t.get("MetadataFetchers")) for t in (o.get("TypeOptions") or [])]

    # 关键：Emby 4.10 的 LibraryOptions 没有 MetadataReaders 字段，
    # 本地 NFO reader 由 DisabledLocalMetadataReaders 控制（空 = 启用）。
    o["DisabledLocalMetadataReaders"] = []
    o["MetadataSavers"] = []
    o["SaveLocalMetadata"] = False
    o["DownloadImagesInAdvance"] = False
    o["CacheImages"] = False
    for t in (o.get("TypeOptions") or []):
        t["MetadataFetchers"] = []          # 关闭远程元数据抓取，只读本地 NFO
        t["MetadataFetcherOrder"] = []
        t["ImageFetchers"] = []             # 关闭远程下图，只读 TG 挂载上的本地 poster
        t["ImageFetcherOrder"] = []
        t["ImageOptions"] = []

    if dry:
        print("  [dry] %-6s id=%-4s readers %s -> Nfo | fetchers %s -> []"
              % (name, iid, before_readers, [x[1] for x in before_tt]))
        return True

    st, res = req("/Library/VirtualFolders/LibraryOptions", "POST",
                  {"Id": iid, "LibraryOptions": o})
    ok = st in (200, 204)
    print("  %-6s id=%-4s code=%s %s" % (name, iid, st, "" if ok else res))
    return ok


def save_backup(libs):
    if os.path.exists(BACKUP):
        print("# 备份已存在，保留最初配置: %s" % BACKUP)
        return
    data = {v.get("Name"): {"Id": v.get("ItemId") or v.get("Id"),
                            "LibraryOptions": v.get("LibraryOptions") or {}}
            for v in libs}
    with open(BACKUP, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("# 备份原始配置 -> %s (%d 库)" % (BACKUP, len(data)))


def restore():
    with open(BACKUP, "r", encoding="utf-8") as f:
        data = json.load(f)
    ok = 0
    for name, rec in data.items():
        st, res = req("/Library/VirtualFolders/LibraryOptions", "POST",
                      {"Id": rec["Id"], "LibraryOptions": rec["LibraryOptions"]})
        ok += 1 if st in (200, 204) else 0
        print("  %-6s restore code=%s" % (name, st))
    print("# 回滚完成 %d/%d" % (ok, len(data)))


def main():
    a = sys.argv[1:]
    dry = "--dry" in a
    verify = "--verify" in a
    do_restore = "--restore" in a

    if do_restore:
        restore()
        return

    libs = [v for v in list_libs() if v.get("Name") in ALL_LIBS]
    print("# 找到 %d / %d 个中文库" % (len(libs), len(ALL_LIBS)))
    missing = set(ALL_LIBS) - {v.get("Name") for v in libs}
    if missing:
        print("! 缺少库:", ", ".join(sorted(missing)))
    if not dry and not verify:
        save_backup(libs)
    if not verify:
        ok = sum(1 for v in libs if patch_one(v, dry))
        print("# 已处理 %d 个库 (dry=%s)" % (ok, dry))

    if verify or dry:
        print("\n# 复核")
        for v in list_libs():
            if v.get("Name") not in ALL_LIBS:
                continue
            o = v.get("LibraryOptions") or {}
            dis = o.get("DisabledLocalMetadataReaders")
            nodis = not dis
            tt = (o.get("TypeOptions") or [])
            nometa = all(not (t.get("MetadataFetchers") or []) for t in tt)
            noimg = all(not (t.get("ImageFetchers") or []) for t in tt)
            flag = "OK " if (nodis and nometa and noimg) else "BAD"
            print("  %s %-6s disabledReaders=%s metaFetchers=%s imgFetchers=%s"
                  % (flag, v.get("Name"), dis,
                     [t.get("MetadataFetchers") for t in tt],
                     [t.get("ImageFetchers") for t in tt]))


if __name__ == "__main__":
    main()