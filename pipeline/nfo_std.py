#!/usr/bin/env python3
"""nfo_std.py —— 剧集 NFO 的唯一标准写法（别处不许自己拼 XML）。

2026-11-19 事故沉淀：我在 fill_by_ids / upgrade_episodes 里手写了只有
title/season/episode 的 NFO，结果同一部剧里新旧两批的刮削结果不一致
（老的有简介和播出日期，新的没有），客户端里看着就是"刮错了"。

标准字段（与 todb_resolve.write_nfo 产物一致）：
    <title> <season> <episode> <plot> <premiered> <aired> <runtime>

用法：
    from nfo_std import write_episode_nfo
    write_episode_nfo("/mnt/embytg/TV/…/S01E05 - 2160p.mp4", series_tmdb=279155, season=1, ep=5)
"""
import json
import os
import sys
import urllib.request
from xml.sax.saxutils import escape as _esc

HERE = os.path.dirname(os.path.abspath(__file__))
_CACHE_P = os.path.join(HERE, ".nfo_tmdb_cache.json")
_CACHE = {}


def _load():
    if _CACHE:
        return _CACHE
    try:
        _CACHE.update(json.load(open(_CACHE_P, encoding="utf-8")))
    except Exception:                                        # noqa: BLE001
        pass
    return _CACHE


def _save():
    try:
        json.dump(_CACHE, open(_CACHE_P, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception:                                        # noqa: BLE001
        pass


def episode_info(series_tmdb, season, ep):
    """从 TMDB 取单集信息；取不到返回 {}（调用方自行决定是否降级）。"""
    _load()
    key = "%s:S%s:E%s" % (series_tmdb, season, ep)
    if key in _CACHE:
        return _CACHE[key]
    info = {}
    try:
        sys.path.insert(0, HERE)
        from embytg_conf import cfg
        api = cfg("tmdb_api").rstrip("/")
        tok = cfg("tmdb_token")
        url = "%s/tv/%s/season/%s?language=zh-CN" % (api, series_tmdb, season)
        req = urllib.request.Request(url, headers={"Authorization": "Bearer %s" % tok})
        with urllib.request.urlopen(req, timeout=15) as f:
            d = json.loads(f.read())
        for e in d.get("episodes") or []:
            if int(e.get("episode_number") or 0) == int(ep):
                info = {"title": e.get("name") or "",
                        "plot": e.get("overview") or "",
                        "air": e.get("air_date") or "",
                        "runtime": e.get("runtime") or 0}
                break
    except Exception:                                        # noqa: BLE001
        info = {}
    _CACHE[key] = info
    _save()
    return info


def write_episode_nfo_info(video_path, season, ep, info):
    """按标准字段写 `.nfo`，info 由调用方给（TMDB / TODB 归一化后的同一结构）。

    2026-11-19：**唯一写入器的第二个入口。** 占位审计（nfo_src_audit.py）已经拿到了
    标题/简介/日期，不该自己拼 XML —— 同一部剧里两种 XML 是「刮错了」的经典来源。
    info: {title, plot, air, runtime}
    """
    title = (info or {}).get("title") or ""
    if not title:
        return None
    rows = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>",
            "  <title>%s</title>" % _esc(title),
            "  <season>%d</season>" % int(season),
            "  <episode>%d</episode>" % int(ep)]
    if (info or {}).get("plot"):
        rows.append("  <plot>%s</plot>" % _esc(str(info["plot"])[:1500]))
    if (info or {}).get("air"):
        rows += ["  <premiered>%s</premiered>" % info["air"],
                 "  <aired>%s</aired>" % info["air"]]
    if (info or {}).get("runtime"):
        rows.append("  <runtime>%d</runtime>" % int(info["runtime"]))
    rows.append("</episodedetails>")
    path = os.path.splitext(video_path)[0] + ".nfo"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(rows) + "\n")
        return path
    except Exception:                                        # noqa: BLE001
        return None


def write_episode_nfo(video_path, series_tmdb, season, ep, fallback_title=None):
    """按标准字段写 `.nfo`，返回写入路径或 None（拿不到任何标题时不写，避免制造不一致）。"""
    info = episode_info(series_tmdb, season, ep)
    title = info.get("title") or fallback_title or ""
    if not title:
        return None
    rows = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>",
            "  <title>%s</title>" % _esc(title),
            "  <season>%d</season>" % int(season),
            "  <episode>%d</episode>" % int(ep)]
    if info.get("plot"):
        rows.append("  <plot>%s</plot>" % _esc(info["plot"]))
    if info.get("air"):
        rows += ["  <premiered>%s</premiered>" % info["air"],
                 "  <aired>%s</aired>" % info["air"]]
    if info.get("runtime"):
        rows.append("  <runtime>%d</runtime>" % int(info["runtime"]))
    rows.append("</episodedetails>")
    path = os.path.splitext(video_path)[0] + ".nfo"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(rows) + "\n")
        return path
    except Exception:                                        # noqa: BLE001
        return None
