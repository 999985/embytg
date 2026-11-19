#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nfo_src_audit.py —— 集标题「可修复性」审计（daily_ops §4.75 的判据来源）

为什么需要（2026-11-19）：
  4.75 最初按「占位标题占比 >5% 判 FAIL」。实测 1045 集占位里，1007 集在 TODB 与 TMDB
  **两边都是占位**（三源无真名，谷雨 E28-E30 连 TMDB 自己都填「第 28 集」），占比 7.2%
  永远压不到 5% —— 判据把「上游没有数据」当成「我们管线出错」，于是每天必 FAIL，
  人就会开始无视这一条。**能报警但永远报警的检查等于没有检查。**

  真正该守的不变式只有两条：
    ① **任一权威源有真名时，库里不许还是占位**（actionable）
    ② **每个视频必须有可被收尾验收接受的 NFO**（fallback：源无真名时写
       「剧名 第N集」，这是 E119/E124 定的口径，post_ingest 的 PLACEHOLDER 是锚定匹配）
  actionable + fallback = 0 才算干净；其余占位属于「源无数据」，记台账等源更新。

判据分档（CRON_COMMON §三）：
  入口挡住    —— todb_resolve.py「占位不覆盖真名」（2026-11-19 已上）
  15 分钟可检 —— 本审计每轮给出 actionable/fallback，>0 即 FAIL
  人工善后    —— 源确实没有的，只有台账 + 30 天重试（nfo_gap_track.py）

用法：
  python3 nfo_src_audit.py                 # 只审计，写 .nfo_src_audit.json
  python3 nfo_src_audit.py --apply         # 审计 + 写 NFO（真名优先，无真名写「剧名 第N集」）
  python3 nfo_src_audit.py --apply --plot  # 顺带补「源有简介而库里空」的

环境变量（规模相关，全部可覆盖）：
  NFO_AUDIT_TTL_H        审计结果多久算新鲜（默认 0 = 每次真跑）
  NFO_AUDIT_MAX_SERIES   单轮最多审计多少部剧（默认 0 = 不限）
  NFO_AUDIT_LIMIT        每次 Emby 分页条数（默认 1000）
  NFO_AUDIT_REUSE_DAYS   无问题的剧多少天内复用上一轮结论（默认 7）
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

OUT = os.path.join(HERE, ".nfo_src_audit.json")
MNT = "/opt/embytg/mnt/embytg"
CTR = "/mnt/embytg"
# 与 post_ingest.PLACEHOLDER 完全一致（锚定）：只有「整条就是第 N 集」才算占位
PH = re.compile(r"^第\s*(\d+)\s*[集期]$|^Episode\s*(\d+)$", re.I)
TMDB_DIR = re.compile(r"\{tmdb=(\d+)\}")
YEAR = re.compile(r"\((\d{4})\)")
SEASON_DIR = re.compile(r"Season\s*(\d+)$", re.I)
SE = re.compile(r"S(\d{1,2})E(\d{1,4})", re.I)
# 判源侧：带剧名前缀的占位也算占位（nfo_episodes_tmdb 的同一口径）
SRC_PH = re.compile(r"第\s*\d+\s*[集期]$|^Episode\s*\d+$|^\d+$", re.I)


def _api(base, tok, path, timeout=180):
    req = urllib.request.Request(base + path, headers={"X-Emby-Token": tok})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"{}")


def emby_episodes():
    """全库 Episode（Name/Path/Overview），分页拉取 —— N=20 万时只是一次 O(N) 扫描。"""
    from embytg_conf import cfg
    base = (cfg("emby_url") or "http://127.0.0.1:8096").rstrip("/")
    tok = cfg("emby_token")
    limit = int(os.environ.get("NFO_AUDIT_LIMIT") or 1000)
    out, start = [], 0
    while True:
        q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                    "Fields": "Overview,Path", "Limit": limit,
                                    "StartIndex": start})
        d = _api(base, tok, "/Items?" + q, timeout=240)
        it = d.get("Items") or []
        out += it
        if not it or len(out) >= (d.get("TotalRecordCount") or 0):
            return out
        start += len(it)


def series_key(path):
    """从集路径反推 (lib, 剧目录, tmdb, season, 剧名, 年份)。纯字符串解析，不做 FUSE 访问。"""
    ct = path.replace(CTR, "", 1)
    parts = [p for p in ct.split("/") if p]
    si = next((i for i, p in enumerate(parts) if SEASON_DIR.match(p)), -1)
    if si < 2:
        return None
    lib, sdir, season = parts[si - 2], parts[si - 1], parts[si]
    mt = TMDB_DIR.search(sdir)
    ms = SEASON_DIR.match(season)
    if not mt or not ms:
        return None
    name = TMDB_DIR.sub("", sdir).strip()
    my = YEAR.search(name)
    name_clean = re.sub(r"\s*[\(（]\d{4}[\)）]", "", name).strip()
    return {"lib": lib, "dir": sdir, "tmdb": mt.group(1), "season": int(ms.group(1)),
            "title": name_clean, "year": my.group(1) if my else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="写 NFO（真名优先，无真名写「剧名 第N集」）")
    ap.add_argument("--plot", action="store_true", help="顺带补缺失简介（写 NFO，不改标题）")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    import nfo_episodes_tmdb as NT
    import nfo_std
    import todb_titles as TT

    t0 = time.time()
    items = emby_episodes()
    # 1) 候选：①Name 整条是占位；②Name == 文件名主干（=没有 NFO 或 NFO 标题就是文件名）
    cand, noplot = {}, 0
    for it in items:
        nm = (it.get("Name") or "").strip()
        ov = (it.get("Overview") or "").strip()
        p = it.get("Path") or ""
        if not ov:
            noplot += 1
        m = PH.match(nm)
        stem = os.path.splitext(os.path.basename(p))[0]
        is_stem = (nm == stem)
        if not m and not is_stem:
            continue
        sk = series_key(p)
        if not sk:
            continue
        key = "%s/%s" % (sk["lib"], sk["dir"])
        cand.setdefault(key, {"meta": sk, "eps": []})["eps"].append(
            {"id": it.get("Id"), "ep": int((m.group(1) or m.group(2))) if m else None,
             "path": p, "name": nm, "noplot": not ov, "is_stem": is_stem})
    # ep 号兜底：从文件名 SxxEyy 取（Name 不是占位时没有编号可用）
    for key, blk in cand.items():
        for e in blk["eps"]:
            if e["ep"] is None:
                mm = SE.search(os.path.basename(e["path"]))
                e["ep"] = int(mm.group(2)) if mm else None
        blk["eps"] = [e for e in blk["eps"] if e["ep"] is not None]

    max_series = int(os.environ.get("NFO_AUDIT_MAX_SERIES") or 0)
    keys = sorted(cand)
    if max_series:
        keys = keys[:max_series]

    detail, todo, plot_todo = {}, [], []
    # 复用上一轮结论：候选集没变、上轮无可修复项、7 天内 → 不重复打 TODO/TMDB
    try:
        _prior = json.load(open(OUT, encoding="utf-8")).get("series") or {}
    except Exception:                                         # noqa: BLE001
        _prior = {}
    _now = time.time()
    _reuse = float(os.environ.get("NFO_AUDIT_REUSE_DAYS") or 7) * 86400
    for key in keys:
        blk = cand[key]
        meta, eps = blk["meta"], blk["eps"]
        _pv = _prior.get(key) or {}
        _eps_now = sorted(e["ep"] for e in eps)
        if (_pv and not _pv.get("act") and not _pv.get("fb") and _pv.get("ph") == len(eps)
                and sorted(_pv.get("eps") or []) == _eps_now
                and (_now - float(_pv.get("ts") or 0)) < _reuse):
            detail[key] = _pv
            continue
        row = {"ph": len(eps), "act": 0, "fb": 0, "eps": _eps_now, "act_eps": [], "fb_eps": [],
               "dirs": [meta["dir"]], "plot_avail": 0, "ts": int(_now)}
        try:
            tmap = NT.tmdb_season(meta["tmdb"], meta["season"]) or {}
        except Exception:                                     # noqa: BLE001
            tmap = {}
        need_todb = [e for e in eps
                     if SRC_PH.search(((tmap.get(e["ep"]) or {}).get("t") or ""))]
        tmap_todb = {}
        if need_todb:
            try:
                vid, _ = TT.resolve(meta["title"], meta["year"])
                if vid:
                    _all = TT.todb_eps(vid)
                    tmap_todb = {e: v for (s, e), v in _all.items() if s == meta["season"]}
            except Exception:                                 # noqa: BLE001
                tmap_todb = {}
        for e in eps:
            t_info = tmap.get(e["ep"]) or {}
            t_title = (t_info.get("t") or "").strip()
            src, info = None, None
            if t_title and not SRC_PH.search(t_title):
                src, info = "tmdb", {"title": t_title, "plot": t_info.get("d") or "",
                                     "air": t_info.get("air") or "", "runtime": 0}
            else:
                d_info = (tmap_todb.get(e["ep"]) or {}) if isinstance(tmap_todb, dict) else {}
                d_title = (d_info.get("t") or "").strip()
                if d_title and not SRC_PH.search(d_title):
                    src, info = "todb", {"title": d_title, "plot": d_info.get("d") or "",
                                         "air": d_info.get("air") or "", "runtime": 0}
                elif (d_info.get("d") or "").strip() and e["noplot"]:
                    plot_todo.append({"path": e["path"], "season": meta["season"],
                                      "ep": e["ep"],
                                      "info": {"title": e["name"],
                                               "plot": d_info.get("d") or "",
                                               "air": d_info.get("air") or "",
                                               "runtime": 0}})
                    row["plot_avail"] += 1
            if src:
                row["act"] += 1
                row["act_eps"].append(e["ep"])
                todo.append({"path": e["path"], "season": meta["season"], "ep": e["ep"],
                             "src": src, "info": info, "series": meta["title"]})
            elif e["is_stem"]:
                # 源没有真名，但没有可用 NFO（Name 退化成文件名）→ 写「剧名 第N集」占位
                row["fb"] += 1
                row["fb_eps"].append(e["ep"])
                todo.append({"path": e["path"], "season": meta["season"], "ep": e["ep"],
                             "src": "fallback", "series": meta["title"],
                             "info": {"title": "%s 第%d集" % (meta["title"], e["ep"]),
                                      "plot": "", "air": "", "runtime": 0}})
        detail[key] = row

    wrote, failed = 0, 0
    if a.apply and todo:
        for j in todo:
            host = j["path"].replace(CTR, MNT)
            if nfo_std.write_episode_nfo_info(host, j["season"], j["ep"], j["info"]):
                wrote += 1
            else:
                failed += 1
    wrote_plot = 0
    if a.apply and a.plot and plot_todo:
        for j in plot_todo:
            host = j["path"].replace(CTR, MNT)
            if nfo_std.write_episode_nfo_info(host, j["season"], j["ep"], j["info"]):
                wrote_plot += 1

    _act = sum(1 for j in todo if j["src"] != "fallback")
    _fb = sum(1 for j in todo if j["src"] == "fallback")
    res = {"ts": int(time.time()), "iso": time.strftime("%F %T"),
           "episodes": len(items), "placeholder": sum(v["ph"] for v in detail.values()),
           "no_plot": noplot, "actionable": _act, "fallback": _fb,
           "plot_gain": len(plot_todo), "wrote": wrote, "wrote_plot": wrote_plot,
           "failed": failed, "series": detail}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    if a.json:
        print(json.dumps({k: res[k] for k in ("iso", "episodes", "placeholder", "no_plot",
                                              "actionable", "fallback", "plot_gain", "wrote")},
                         ensure_ascii=False))
        return 0
    print("  集 %d｜占位 %d（源有真名 %d / 源无数据 %d）｜缺可用NFO %d｜无简介 %d（可补 %d）｜%.1fs"
          % (res["episodes"], res["placeholder"], _act,
             res["placeholder"] - _act, _fb, noplot, len(plot_todo), time.time() - t0))
    if _act:
        for k in sorted(detail, key=lambda x: -(detail[x].get("act") or 0))[:10]:
            v = detail[k]
            if v.get("act"):
                print("   %-40s 可修复 %d/%d 集 %s" % (k.split("/")[-1][:40], v["act"],
                                                       v["ph"], (v.get("act_eps") or [])[:12]))
    if _fb:
        for k in sorted(detail, key=lambda x: -(detail[x].get("fb") or 0))[:10]:
            v = detail[k]
            if v.get("fb"):
                print("   %-40s 需补占位NFO %d/%d 集 %s" % (k.split("/")[-1][:40], v["fb"],
                                                          v["ph"], (v.get("fb_eps") or [])[:12]))
    if wrote or wrote_plot:
        print("  已写 NFO：标题 %d 条（失败 %d）｜简介 %d 条" % (wrote, failed, wrote_plot))
        dirs = sorted({d for v in detail.values() if v.get("act") for d in v["dirs"]})
        if dirs:
            print("  推送目录（nfo_name_to_emby.py --apply --series-dir）：")
            for d in dirs:
                print("    %s" % d)
    return 0


if __name__ == "__main__":
    sys.exit(main())
