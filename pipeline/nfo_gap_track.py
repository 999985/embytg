#!/usr/bin/env python3
"""nfo_gap_track.py —— 占位集名/缺简介的台账（不是每个都能补，先记清楚）。

背景：1045 集标题是「第 N 集」、2089 集无简介。实测三条源都不覆盖：
  TMDB     新剧常只给占位（谷雨 E28-E30 就是 TMDB 自己填的「第 28 集」）
  TODB     镜像对多数剧直接返回「第 N 集」占位，没有真名
  豆瓣     tv api 对这些剧 seasons=[] / episodes 404，拿不到分集
所以这不是代码问题，是数据不存在。处理方式：记录下来，按 30 天重试，
源更新后自然补上；不再每天对同一批集重复抓取浪费额度。

用法：python3 nfo_gap_track.py [--retry-days 30]
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
OUT = os.path.join(HERE, ".nfo_gap.json")
PH = re.compile(r"^第\s*(\d+)\s*集$")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retry-days", type=int, default=30)
    a = ap.parse_args()

    sys.path.insert(0, HERE)
    from embytg_conf import cfg
    base = cfg("emby_url").rstrip("/")
    tok = cfg("emby_token")
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                "Fields": "Overview", "Limit": "20000"})
    with urllib.request.urlopen("%s/Items?%s&api_key=%s" % (base, q, tok), timeout=180) as f:
        items = (json.loads(f.read()) or {}).get("Items") or []

    now = int(time.time())
    per = {}
    placeholder = noplot = 0
    for it in items:
        name = (it.get("Name") or "").strip()
        m = PH.match(name)
        has_plot = bool((it.get("Overview") or "").strip())
        if not m and has_plot:
            continue
        s = it.get("SeriesName") or "?"
        row = per.setdefault(s, {"placeholder": 0, "no_plot": 0, "eps": []})
        if m:
            placeholder += 1
            row["placeholder"] += 1
            row["eps"].append(int(m.group(1)))
        if not has_plot:
            noplot += 1
            row["no_plot"] += 1

    try:
        old = json.load(open(OUT, encoding="utf-8"))
    except Exception:                                        # noqa: BLE001
        old = {}
    out = {}
    for s, v in per.items():
        prev = old.get(s) or {}
        out[s] = {"placeholder": v["placeholder"], "no_plot": v["no_plot"],
                  "eps": sorted(v["eps"])[:200],
                  "first_seen": prev.get("first_seen") or now,
                  "retry_at": now + a.retry_days * 86400,
                  "tries": int(prev.get("tries") or 0) + 1}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print("  占位集名 %d 集，无简介 %d 集，涉及 %d 部剧" % (placeholder, noplot, len(out)))
    for s, v in sorted(out.items(), key=lambda x: -x[1]["placeholder"])[:8]:
        print("   %-24s 占位 %2d｜无简介 %2d｜第 %d 次追踪"
              % (s[:24], v["placeholder"], v["no_plot"], v["tries"]))
    print("  已写 %s（%d 天后重试）" % (os.path.basename(OUT), a.retry_days))
    return 0


if __name__ == "__main__":
    sys.exit(main())
