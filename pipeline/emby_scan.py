#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""emby_scan.py —— Emby 入库通知的唯一出口（targeted scan，三档降级）

## 背景（2026-11-13，E035/E036）

Emby 有三条"让它知道有新文件"的路，能力完全不同：

| 方式 | 能刷新已存在条目 | 能创建新 Series | 代价 |
|---|---|---|---|
| `POST /Library/Media/Updated` | ✅ | ❌ 实测喂**目录**返回 204 但不建条目 | O(1) |
| `POST /Items/{libId}/Refresh` | ✅ | ✅ | **整库递归 ffprobe**（综艺库 865 集 ≈ 十几分钟） |
| `POST /Library/ScanPath`（TargetedScan 插件） | ✅ | ✅ 连 Series/Season/Episode 一起建 | **<1 秒** |

之前的代码只有前两条，所以"新剧缺失"只能整库刷新——这是 2026-11-13 那次
综艺库刷新的原因，也是 20 万集时代不可持续的根因。

`targeted-scans`（github.com/d3v1l1989/targeted-scans）的 Emby 插件补上了第三条：
把文件路径给它，它向上找最近的已知祖先，**自上而下**建出缺失的 Series/Season/Episode，
再排队元数据刷新。实测本机 Emby 4.10.2.0 + 插件 1.8.0.0：
  · 不存在的路径 → `PathNotFound`（说明端点活着）
  · 已存在的集   → `Refreshed` + 真实 ItemId（娘道 S01E01 → 165235「瑛子失足坠入大河」）

## 用法

```
python3 emby_scan.py PATH [PATH ...]     # 单个/多个路径
python3 emby_scan.py --from-tgdb --since 2h   # 从 tg.db 取最近落地的集
python3 emby_scan.py --probe             # 只看端点是否可用
```

作为库使用：
```
import emby_scan
r = emby_scan.scan_paths(["/mnt/embytg/TV/..."])   # -> {"Created":1,"Refreshed":2,...}
```

## 降级策略（--fallback）

`ScanPath` 不可用（插件没装/被升级冲掉/404）时：
- `--fallback media`（默认）：退回 `Media/Updated`。**只能刷新已有条目**，新剧建不出来，
  但绝不会整库 ffprobe（保护 Emby 与磁盘）。
- `--fallback library`：退回库级 Refresh。能建新剧，但会整库 ffprobe —— 只有确认
  确实有新剧、且愿意付这个代价时才用。`reconcile.py --repair` 就是这么做的。

默认永不自动升级到 `library`：宁可漏建一个新剧等 reconcile 每天兜，也不要在
20 万集时被一次意外的整库扫描拖死。
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request

import embytg_conf as C

HERE = os.path.dirname(os.path.abspath(__file__))
E = (C.cfg("emby_url", "http://127.0.0.1:8096") or "").rstrip("/")
T = C.cfg("emby_api_key") or C.cfg("emby_token") or ""
TGDB = os.path.join(HERE, "tg.db")
PREFIX = "/mnt/embytg"

# 插件端点可用性缓存（一次运行内只探一次）
_PROBE = {}


def _req(path, method="GET", body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(E + path, method=method,
                               headers={"X-Emby-Token": T,
                                        "Content-Type": "application/json"},
                               data=data)
    with urllib.request.urlopen(r, timeout=timeout) as f:
        raw = f.read()
    return json.loads(raw) if raw.strip() else None


def have_scanpath():
    """探测 /Library/ScanPath 是否存在（插件装没装）。

    用一条一定不存在的路径探：能返回 JSON（哪怕是 PathNotFound）就说明端点活着。
    404/405 → 插件不在。
    """
    if "ok" in _PROBE:
        return _PROBE["ok"]
    try:
        r = _req("/Library/ScanPath", "POST",
                 {"Path": PREFIX + "/__probe_targeted_scan__/x.mp4"}, timeout=20)
        _PROBE["ok"] = isinstance(r, dict) and "Status" in r
    except urllib.error.HTTPError as e:
        _PROBE["ok"] = e.code not in (404, 405)
    except Exception:                                       # noqa: BLE001
        _PROBE["ok"] = False
    return _PROBE["ok"]


def scan_paths(paths, timeout=180):
    """把路径交给 TargetedScan，返回状态计数。

    返回 {"Created":n, "Refreshed":n, "PathNotFound":n, "ParentNotFound":n,
          "Failed":n, "ids":[...], "mode":"scanpath"|"media"|"library"}
    """
    paths = [p for p in paths if p]
    out = {"Created": 0, "Refreshed": 0, "PathNotFound": 0,
           "ParentNotFound": 0, "Failed": 0, "ids": [], "mode": "scanpath"}
    if not paths:
        return out

    if have_scanpath():
        # 优先批量端点，失败再逐条
        try:
            r = _req("/Library/ScanPaths", "POST", {"Paths": paths}, timeout=timeout)
            for it in (r or {}).get("Results") or []:
                st = it.get("Status") or "Failed"
                out[st] = out.get(st, 0) + 1
                if it.get("ItemId"):
                    out["ids"].append(it["ItemId"])
            return out
        except Exception:                                   # noqa: BLE001
            pass
        for p in paths:
            try:
                r = _req("/Library/ScanPath", "POST", {"Path": p}, timeout=60)
                st = (r or {}).get("Status") or "Failed"
                out[st] = out.get(st, 0) + 1
                if r and r.get("ItemId"):
                    out["ids"].append(r["ItemId"])
            except Exception:                               # noqa: BLE001
                out["Failed"] += 1
        return out

    # 插件不在：退回 Media/Updated（只刷新，不建新）
    out["mode"] = "media"
    try:
        _req("/Library/Media/Updated", "POST",
             {"Updates": [{"Path": p, "UpdateType": "Created"} for p in paths]},
             timeout=timeout)
        out["Refreshed"] = len(paths)
    except Exception:                                       # noqa: BLE001
        out["Failed"] += len(paths)
    return out


def paths_from_tgdb(since_h=2.0, limit=2000):
    """从 tg.db 取最近落地的集，换成容器内路径。"""
    cut = int(time.time() - since_h * 3600)
    con = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=60)
    rows = [r[0] for r in con.execute(
        "select rel from resource_state where landed_at >= ? and rel like '%.mp4'"
        " order by landed_at desc limit ?", (cut, limit))]
    con.close()
    return [PREFIX + "/" + r.lstrip("/") for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="*")
    ap.add_argument("--from-tgdb", action="store_true")
    ap.add_argument("--since", default="2h")
    ap.add_argument("--probe", action="store_true")
    a = ap.parse_args()

    if a.probe:
        ok = have_scanpath()
        print("TargetedScan 端点:", "可用" if ok else "不可用（将降级到 Media/Updated）")
        return 0 if ok else 1

    paths = list(a.paths)
    if a.from_tgdb:
        h = float(str(a.since).rstrip("hH")) if str(a.since)[:-1].replace(".", "").isdigit() \
            else 2.0
        paths += paths_from_tgdb(h)
    if not paths:
        print("没有路径。用 PATH 或 --from-tgdb。")
        return 2

    t0 = time.time()
    r = scan_paths(paths)
    print("模式 %s | %d 路径 | %.2fs" % (r["mode"], len(paths), time.time() - t0))
    for k in ("Created", "Refreshed", "PathNotFound", "ParentNotFound", "Failed"):
        if r.get(k):
            print("  %-14s %d" % (k, r[k]))
    if r["ids"][:5]:
        print("  ItemIds:", ",".join(r["ids"][:5]))
    return 0 if not r["Failed"] else 1


if __name__ == "__main__":
    sys.exit(main())
