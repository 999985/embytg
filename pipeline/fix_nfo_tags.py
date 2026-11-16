# -*- coding: utf-8 -*-
"""
一次性修复：把 nfo_gen.py 早期写错的 <overview> 标签就地改成 NFO 标准 <plot>。

背景（E003 / 2026-11-09）：
  Emby NfoMetadata 的 BaseNfoParser.cs 的 FetchDataFromXmlNode switch 里：
    case "biography": case "plot": case "review": -> item.Overview
    case "genre":                                 -> item.AddGenre
    default: -> await reader.SkipAsync()  （<overview> 命中这里被整节点丢弃）
  nfo_gen.py 早期把 TMDB JSON 的 overview 字段名直搬成 <overview>，导致简介读不进。
  本脚本只做标签改名，不查网络、不改其它内容，幂等可反复跑。
  改完 mtime 自动更新，供 NFO reader 的 IHasItemChangeMonitor.HasChanged 判定重读。

  注意：串行逐文件读写 FUSE 挂载极慢（实测 4 分钟还没跑完电影段），
  故 collect 用一次 os.walk、改写用线程池并发（NFO_WORKERS，默认 24）。

用法：
  python3 fix_nfo_tags.py            # dry-run，只统计
  python3 fix_nfo_tags.py --apply    # 实际改写
"""
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

MOUNT = os.environ.get("EMBYTG_MOUNT", "/opt/embytg/mnt/embytg")
TAG_RE = re.compile(r"<overview>(.*?)</overview>", re.S | re.I)


def collect(root):
    out = []
    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if not d.startswith(".")]
        for fn in fns:
            if fn.lower().endswith(".nfo"):
                out.append(os.path.join(dp, fn))
    return out


def one(p, apply):
    try:
        with open(p, "r", encoding="utf-8") as f:
            s = f.read()
        if not TAG_RE.search(s):
            return ("skip", p)
        new = TAG_RE.sub(lambda m: "<plot>%s</plot>" % m.group(1), s)
        if apply:
            with open(p, "w", encoding="utf-8") as f:
                f.write(new)
        return ("changed", p)
    except Exception as e:
        return ("err", "%s : %s" % (os.path.relpath(p, MOUNT), str(e)[:100]))


def main():
    apply = "--apply" in sys.argv
    workers = int(os.environ.get("NFO_WORKERS", "24"))
    t0 = time.time()
    paths = collect(MOUNT)
    print("# 收集 %d 个 NFO / %.1fs" % (len(paths), time.time() - t0), flush=True)
    cnt = {"changed": 0, "skip": 0, "err": 0}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(one, p, apply): p for p in paths}
        for i, fu in enumerate(as_completed(futs), 1):
            st, info = fu.result()
            cnt[st] += 1
            if st == "err":
                print("  ! " + info, flush=True)
            if i % 500 == 0:
                print("  .. %d/%d (%.1fs)" % (i, len(paths), time.time() - t0), flush=True)
    print("# NFO 标签修复 %s: 扫描 %d / 含<overview>需改 %d / 已合规跳过 %d / 失败 %d / %.1fs"
          % ("APPLY" if apply else "DRY", len(paths), cnt["changed"], cnt["skip"],
             cnt["err"], time.time() - t0))
    if not apply and cnt["changed"]:
        print("# 加 --apply 实际改写（mtime 会变新，触发 NFO reader 重读）")


if __name__ == "__main__":
    main()
