#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""refresh_series.py —— 对「目录还在、Emby 条目仍指着改名前的旧文件名」的 Series 做定向 refresh

为什么这么做（E019 / E018）：
  - 绝不 `DELETE /Items/{id}` 那些目录还存在的 Series —— Emby 会连**磁盘目录一起删**（日志实证：
    `App: Deleting directory /mnt/embytg/TV/...`），那会毁掉 TG 上的真实文件。
  - 也不会等全库扫描（用户明确：永远不要等 Emby 自动扫，基本没戏 —— 12k 文件 × 4.5s ≈ 16h，
    且中途任何重启都归零）。
  - 唯一安全且确定的做法：对单部 Series 发 `POST /Items/{id}/Refresh?Recursive=true`，
    Emby 会重读该目录 → 旧条目（路径已不存在）被移除、新文件名被登记、NFO 被读取。

用法：
  python3 refresh_series.py --plan              # 列出需要 refresh 的 Series
  python3 refresh_series.py --apply [--limit N] # 逐个 refresh，可断点续跑
  python3 refresh_series.py --verify            # 复核幽灵数
"""
import argparse
import json
import os
import time
import urllib.parse
import urllib.request

c = json.load(open("/opt/embytg/config.json"))
T = c["emby_token"]
B = c.get("emby_url", "http://127.0.0.1:8096").rstrip("/")
HOST = "/opt/embytg/mnt/embytg"
CTR = "/mnt/embytg"
STATE = "/opt/embytg/pipeline/.refresh_series_state.json"
TV_LIBS = ["国产剧", "日韩剧", "欧美剧", "港台剧", "国漫", "日漫", "综艺"]


def api(path, method="GET", body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(B + path, data=data, method=method,
                                 headers={"X-Emby-Token": T, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "ignore")
            return r.status, (json.loads(raw) if raw.strip().startswith(("{", "[")) else raw)
    except Exception as e:
        return 0, str(e)[:120]


def lib_id(name):
    _, d = api("/Library/MediaFolders")
    for x in (d.get("Items") or []):
        if x.get("Name") == name:
            return x.get("Id")
    return None


def dup_episodes(sid):
    """E112：同一 Series 下「IndexNumber 相同或 Path 相同」的重复 Episode 组。

    这不是幽灵（路径都在），`ghost` 判据看不见它，于是 `refresh_series --apply` 永远
    报 PART 也修不掉；只能把多余的那条条目从**库**里删掉。
    `DELETE /Items/{id}` 对 Episode 只删条目不删文件（实测 2026-11-15），
    但为了守住 E020 的教训，本函数只**列出**，删除动作交给 --dedupe-episodes 且必须带 --apply。
    """
    eps = episodes(sid)
    groups, bykey = [], {}
    for e in eps:
        k = (e.get("IndexNumber"), (e.get("Path") or ""))
        bykey.setdefault(k, []).append(e)
    for k, v in bykey.items():
        if len(v) > 1:
            groups.append({"index": k[0], "path": k[1], "keep": v[0], "drop": v[1:]})
    return groups


def episodes(sid):
    out, start = [], 0
    while True:
        q = urllib.parse.urlencode({"ParentId": sid, "Recursive": "true", "IncludeItemTypes": "Episode",
                                    "Fields": "Path,Name", "Limit": 500, "StartIndex": start})
        _, d = api("/Items?" + q)
        it = d.get("Items") or []
        out += it
        if len(out) >= (d.get("TotalRecordCount") or 0) or not it:
            return out
        start += len(it)


def survey():
    plan = []
    for name in TV_LIBS:
        lid = lib_id(name)
        if not lid:
            continue
        q = urllib.parse.urlencode({"ParentId": lid, "Recursive": "true", "IncludeItemTypes": "Series",
                                    "Fields": "Path,Name", "Limit": 500})
        _, d = api("/Items?" + q)
        for s in (d.get("Items") or []):
            D = (s.get("Path") or "").replace(CTR, HOST)
            if not D or not os.path.isdir(D):
                continue          # 目录已不存在 → 不在这里处理（可能需整剧删，另行判断）
            disk = 0
            for r, _, fs in os.walk(D):
                disk += sum(1 for f in fs if f.lower().endswith((".mp4", ".mkv", ".ts")))
            eps = episodes(s["Id"])
            ghost = sum(1 for x in eps if not os.path.exists((x.get("Path") or "").replace(CTR, HOST)))
            if ghost > 0 or len(eps) != disk:
                plan.append({"lib": name, "id": s["Id"], "name": s["Name"],
                             "ghost": ghost, "emby": len(eps), "disk": disk})
    return plan


def verify():
    tot = {}
    for name in TV_LIBS:
        lid = lib_id(name)
        if not lid:
            continue
        q = urllib.parse.urlencode({"ParentId": lid, "Recursive": "true", "IncludeItemTypes": "Series",
                                    "Fields": "Path", "Limit": 500})
        _, d = api("/Items?" + q)
        g = 0
        for s in (d.get("Items") or []):
            D = (s.get("Path") or "").replace(CTR, HOST)
            if not D or not os.path.isdir(D):
                continue
            for x in episodes(s["Id"]):
                if not os.path.exists((x.get("Path") or "").replace(CTR, HOST)):
                    g += 1
        D = os.path.join(HOST, "TV", name)
        disk = 0
        for r, _, fs in os.walk(D):
            disk += sum(1 for f in fs if f.lower().endswith((".mp4", ".mkv", ".ts")))
        q2 = urllib.parse.urlencode({"ParentId": lid, "Recursive": "true", "IncludeItemTypes": "Episode", "Limit": 1})
        _, e = api("/Items?" + q2)
        tot[name] = {"disk": disk, "emby": e.get("TotalRecordCount"), "ghost": g}
        dups = 0
        for s in (d.get("Items") or []):
            dups += sum(len(x["drop"]) for x in dup_episodes(s["Id"]))
        print("  %-6s 磁盘=%-5d Emby=%-5d 幽灵=%d 重复条目=%d"
              % (name, disk, e.get("TotalRecordCount"), g, dups))
    return tot


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--lib")
    ap.add_argument("--wait", type=int, default=25, help="每部 refresh 后等待秒数")
    ap.add_argument("--dedupe-episodes", action="store_true",
                    help="E112：删掉同 Series 下重复的 Episode 条目（只删条目不删文件，需配 --apply）")
    a = ap.parse_args()
    if a.verify:
        print("=== 复核 ===")
        verify()
        return
    if a.dedupe_episodes:
        names = a.lib or None
        total_drop = 0
        for name in ([names] if isinstance(names, str) else TV_LIBS):
            lid = lib_id(name)
            if not lid:
                continue
            q = urllib.parse.urlencode({"ParentId": lid, "Recursive": "true",
                                        "IncludeItemTypes": "Series", "Fields": "Path,Name", "Limit": 500})
            _, d = api("/Items?" + q)
            for s in (d.get("Items") or []):
                gs = dup_episodes(s["Id"])
                if not gs:
                    continue
                for g in gs:
                    print("  %-6s %-24s S%sE%s 重复 %d 条 保留=%s 删=%s"
                          % (name, s["Name"][:24], "?", g["index"], len(g["drop"]) + 1,
                             g["keep"]["Id"], [x["Id"] for x in g["drop"]]), flush=True)
                    if not a.apply:
                        continue
                    # E112 铁律：**绝不对 Episode 发 DELETE /Items/{id}**。
                    # 实测 2026-11-15 12:55：删重复条目 167332 时 Emby 日志打
                    #   App: Deleting file .../为了N S01E03 - 1080p.mkv
                    # 连同真实视频 + NFO 一起从挂载删掉（teldrive 软删，30 天可恢复）。
                    # E020 只说了「目录还在的 Series 不能 DELETE」，没人写「Episode 也不行」，
                    # 于是重复了同一个错。现在只列清单，**不做任何写操作**，交人工按 E112 处理。
                    print("     只报告不删（E112：DELETE /Items 会连磁盘文件一起删）", flush=True)
        print("# 重复条目：%s%d" % ("已删 " if a.apply else "待删 ", total_drop))
        return
    plan = survey()
    print("# 需要 refresh 的 Series: %d（幽灵合计 %d，磁盘文件合计 %d）"
          % (len(plan), sum(p["ghost"] for p in plan), sum(p["disk"] for p in plan)), flush=True)
    for p in plan[:12]:
        print("   %-4s %-26s 幽灵=%-3d Emby=%-3d 磁盘=%d"
              % (p["lib"], p["name"][:26], p["ghost"], p["emby"], p["disk"]))
    if not a.apply:
        json.dump(plan, open("/tmp/refresh_plan.json", "w"), ensure_ascii=False, indent=1)
        return
    st = {"done": [], "fail": []}
    if os.path.exists(STATE):
        try:
            st = json.load(open(STATE))
        except Exception:
            pass
    done_ids = {x["id"] for x in st.get("done", [])}
    todo = [p for p in plan if p["id"] not in done_ids]
    if a.limit:
        todo = todo[:a.limit]
    print("# 本轮处理 %d 部（已完成 %d）" % (len(todo), len(done_ids)), flush=True)
    t0 = time.time()
    for i, p in enumerate(todo, 1):
        before = episodes(p["id"])
        code, _ = api("/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default"
                      "&ImageRefreshMode=Default&ReplaceAllMetadata=false&ReplaceAllImages=false"
                      % p["id"], method="POST", timeout=180)
        # 轮询直到幽灵清零或超时
        ok = False
        for _ in range(max(1, a.wait // 3)):
            time.sleep(3)
            cur = episodes(p["id"])
            g = sum(1 for x in cur if not os.path.exists((x.get("Path") or "").replace(CTR, HOST)))
            if g == 0 and len(cur) == p["disk"]:
                ok = True
                break
        cur = episodes(p["id"])
        g = sum(1 for x in cur if not os.path.exists((x.get("Path") or "").replace(CTR, HOST)))
        rec = {"id": p["id"], "name": p["name"], "lib": p["lib"], "code": code,
               "before": len(before), "after": len(cur), "ghost": g, "disk": p["disk"]}
        (st["done"] if ok or g == 0 else st["fail"]).append(rec)
        json.dump(st, open(STATE, "w"), ensure_ascii=False, indent=1)
        print("  [%d/%d] %-4s %-24s %s Emby %d->%d 幽灵=%d 磁盘=%d %.0fs"
              % (i, len(todo), p["lib"], p["name"][:24], "OK" if ok else "PART",
                 len(before), len(cur), g, p["disk"], time.time() - t0), flush=True)
    print("# 完成：成功 %d / 待处理 %d" % (len(st["done"]), len(st["fail"])))


if __name__ == "__main__":
    main()
