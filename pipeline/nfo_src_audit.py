#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nfo_src_audit.py —— 占位集名「可修复性」审计（daily_ops §4.75 的判据来源）

为什么需要（2026-11-19）：
  4.75 最初按「占位标题占比 >5% 判 FAIL」。实测 1045 集占位里，1007 集在 TODB 与 TMDB
  **两边都是占位**（三源无真名，谷雨 E28-E30 连 TMDB 自己都填「第 28 集」），占比 7.2%
  永远压不到 5% —— 判据把「上游没有数据」当成「我们的管线出错」，于是每天必 FAIL，
  人就会开始无视这一条。**能报警但永远报警的检查等于没有检查。**

  真正该守的不变式只有一条：
      **任一权威源有真名时，库里不许还是占位。**
  本脚本就是这条不变式的量尺：`actionable`（有源有真名而库里是占位）> 0 才是我们的错；
  其余占位属于「源无数据」，记进台账等源更新，不算失败。

判据分档（CRON_COMMON §三）：
  入口挡住   —— todb_resolve.py 占位不覆盖真名（2026-11-19 已上）
  15 分钟可检 —— 本审计 actionable>0 即 FAIL，daily_ops 每轮调用
  人工善后   —— 源确实没有的，只有台账 + 30 天重试

用法：
  python3 nfo_src_audit.py                 # 只审计，写 .nfo_src_audit.json
  python3 nfo_src_audit.py --apply         # 审计 + 把可修复的写成标准 NFO，并打印目录
  python3 nfo_src_audit.py --apply --plot  # 顺带补「有简介但库里空」的（写 NFO，不含标题变更）

环境变量（规模相关，全部可覆盖）：
  NFO_AUDIT_TTL_H        审计结果多久算新鲜（默认 0 = 每次真跑）
  NFO_AUDIT_MAX_SERIES   单轮最多审计多少部剧（默认 0 = 不限）
  NFO_AUDIT_LIMIT        每次 Emby 分页条数（默认 1000）
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
PH = re.compile(r"^第\s*(\d+)\s*集$")
TMDB_DIR = re.compile(r"\{tmdb=(\d+)\}")
YEAR = re.compile(r"\((\d{4})\)")
SEASON_DIR = re.compile(r"Season\s*(\d+)$", re.I)
SE = re.compile(r"S(\d{1,2})E(\d{1,4})", re.I)
# 与 nfo_episodes_tmdb.PLACEHOLDER 同义：带剧名前缀的占位也算占位（判源侧用）
SRC_PH = re.compile(r"第\s*\d+\s*[集期]$|^Episode\s*\d+$|^\d+$", re.I)


def _api(base, tok, path, timeout=120):
    req = urllib.request.Request(base + path, headers={"X-Emby-Token": tok})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"{}")


def emby_episodes():
    """全库 Episode（Name/Path/Overview），分页拉取 —— N=20 万时只是一次 O(N) 扫描。"""
    from embytg_conf import cfg
    base = cfg("emby_url").rstrip("/")
    tok = cfg("emby_token")
    limit = int(os.environ.get("NFO_AUDIT_LIMIT") or 1000)
    out, start = [], 0
    while True:
        q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                    "Fields": "Overview,Path", "Limit": limit,
                                    "StartIndex": start})
        d = _api(base, tok, "/Items?" + q, timeout=180)
        it = d.get("Items") or []
        out += it
        if not it or len(out) >= (d.get("TotalRecordCount") or 0):
            return out
        start += len(it)


def series_key(path):
    """从集路径反推 (剧目录, tmdb, season, 剧名, 年份)。一次性解析，不做 FUSE 访问。"""
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
    name_clean = re.sub(r"\s*[\(\（]\d{4}[\)\）]", "", name).strip()
    return {"lib": lib, "dir": sdir, "tmdb": mt.group(1), "season": int(ms.group(1)),
            "title": name_clean, "year": my.group(1) if my else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="把可修复的写成标准 NFO")
    ap.add_argument("--plot", action="store_true", help="顺带补缺失简介（写 NFO，不改标题）")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    import nfo_episodes_tmdb as NT
    import nfo_std
    import todb_titles as TT

    t0 = time.time()
    items = emby_episodes()
    # 1) 分桶：只关心「标题是纯占位」或「无简介」的集
    ph_by_series, noplot = {}, 0
    for it in items:
        nm = (it.get("Name") or "").strip()
        ov = (it.get("Overview") or "").strip()
        m = PH.match(nm)
        if not ov:
            noplot += 1
        if not m:
            continue
        sk = series_key(it.get("Path") or "")
        if not sk:
            continue
        key = "%s/%s" % (sk["lib"], sk["dir"])
        ph_by_series.setdefault(key, {"meta": sk, "eps": []})["eps"].append(
            {"id": it.get("Id"), "ep": int(m.group(1)), "path": it.get("Path") or "",
             "name": nm, "noplot": not ov})

    max_series = int(os.environ.get("NFO_AUDIT_MAX_SERIES") or 0)
    keys = sorted(ph_by_series)
    if max_series:
        keys = keys[:max_series]

    detail, actionable, plot_gain = {}, [], []
    # 复用上一轮的结论：占位集号与数量都没变、且上一轮判定「源无数据」的剧，
    # 7 天内不再重复打 TODB/TMDB（94 部剧全查一次约 5 分钟，9 成是白查）。
    # 失效条件明确：集号集合变化 / 出现过 actionable / 超过 7 天。
    try:
        _prior = json.load(open(OUT, encoding="utf-8")).get("series") or {}
    except Exception:                                         # noqa: BLE001
        _prior = {}
    _now = time.time()
    for key in keys:
        blk = ph_by_series[key]
        meta, eps = blk["meta"], blk["eps"]
        _pv = _prior.get(key) or {}
        _eps_now = sorted(e["ep"] for e in eps)
        if (_pv and not _pv.get("act") and _pv.get("ph") == len(eps)
                and sorted(_pv.get("eps") or []) == _eps_now
                and (_now - float(_pv.get("ts") or 0)) < 7 * 86400):
            detail[key] = _pv
            continue
        row = {"ph": len(eps), "act": 0, "eps": _eps_now, "act_eps": [],
               "dirs": [meta["dir"]], "plot_avail": 0, "ts": int(_now)}
        # TMDB 一季一次（进程内 + 文件缓存，中文优先/英文补/自动翻译）
        try:
            tmap = NT.tmdb_season(meta["tmdb"], meta["season"]) or {}
        except Exception:                                     # noqa: BLE001
            tmap = {}
        need_todb = [e for e in eps
                     if SRC_PH.search(((tmap.get(e["ep"]) or {}).get("t") or ""))]
        tmap_todb = {}
        if need_todb:
            # 只有 TMDB 给不出真名时才问 TODB（省一次外部依赖；TODB 是主源，不能跳过）
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
                    plot_gain.append({"path": e["path"], "season": meta["season"],
                                      "ep": e["ep"],
                                      "info": {"title": e["name"],
                                               "plot": d_info.get("d") or "",
                                               "air": d_info.get("air") or "",
                                               "runtime": 0}})
                    row["plot_avail"] += 1
            if src:
                row["act"] += 1
                row["act_eps"].append(e["ep"])
                actionable.append({"path": e["path"], "season": meta["season"],
                                   "ep": e["ep"], "src": src, "info": info,
                                   "series": meta["title"]})
        detail[key] = row

    wrote, failed = 0, 0
    if a.apply and actionable:
        for j in actionable:
            host = j["path"].replace(CTR, MNT)
            if nfo_std.write_episode_nfo_info(host, j["season"], j["ep"], j["info"]):
                wrote += 1
            else:
                failed += 1
    wrote_plot = 0
    if a.apply and a.plot and plot_gain:
        for j in plot_gain:
            host = j["path"].replace(CTR, MNT)
            if nfo_std.write_episode_nfo_info(host, j["season"], j["ep"], j["info"]):
                wrote_plot += 1

    res = {"ts": int(time.time()), "iso": time.strftime("%F %T"),
           "episodes": len(items), "placeholder": sum(v["ph"] for v in detail.values()),
           "no_plot": noplot, "actionable": len(actionable),
           "plot_gain": len(plot_gain), "wrote": wrote, "wrote_plot": wrote_plot,
           "failed": failed, "series": detail}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    if a.json:
        print(json.dumps({k: res[k] for k in ("iso", "episodes", "placeholder", "no_plot",
                                              "actionable", "plot_gain", "wrote")},
                         ensure_ascii=False))
        return 0
    print("  集 %d｜占位 %d（源有真名 %d / 源无数据 %d）｜无简介 %d（可补 %d）｜%.1fs"
          % (res["episodes"], res["placeholder"], res["actionable"],
             res["placeholder"] - res["actionable"], noplot, len(plot_gain),
             time.time() - t0))
    if actionable:
        for k in sorted(detail, key=lambda x: -detail[x]["act"])[:10]:
            v = detail[k]
            if v["act"]:
                print("   %-40s 可修复 %d/%d 集 %s" % (k.split("/")[-1][:40], v["act"],
                                                       v["ph"], v.get("act_eps") or v["eps"][:12]))
    if wrote or wrote_plot:
        print("  已写 NFO：标题 %d 条（失败 %d）｜简介 %d 条" % (wrote, failed, wrote_plot))
        dirs = sorted({d for v in detail.values() if v["act"] for d in v["dirs"]})
        print("  推送目录（nfo_name_to_emby.py --apply --series-dir）：")
        for d in dirs:
            print("    %s" % d)
    return 0


if __name__ == "__main__":
    sys.exit(main())
