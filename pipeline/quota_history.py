#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""quota_history.py —— 配额效率的每日历史（请求数 ÷ 出片数）

## 为什么需要它

配额（每源 300 次/天 × 9 源）是 10 万集目标的唯一硬约束，而「这次改动到底有没有
提高配额利用率」这个问题，我前几轮是用**临时手写脚本**拍了四次快照来回答的，
每次口径还略有不同（有时数 path 里有没有 SearchTerm，有时数行数）。
**不可持续的度量 = 不可信的结论。**

本脚本把三个数固定成一条日记录，落盘成历史：

    requests      当天源站请求数（分 search / episodes / other）
    episodes      当天真正落地的集数（取 tg.db.resource_state.landed_at，唯一真相）
    per_ep        每出一集要花多少次请求  ← 这就是配额利用率

`per_ep` 下降 = 同样的配额能搬更多片 = 离 10 万更近。任何优化都必须让它下降，
否则就是白改。

## 幂等

同一天重复跑会**覆盖**当天那条记录（不是追加），所以可以随时重算。
写在 `logs/quota_history.jsonl`，一天一行，按日期排序。

## 用法

```
python3 quota_history.py                 # 记录今天 + 打印趋势
python3 quota_history.py --day 2026-11-14
python3 quota_history.py --backfill 7    # 回填最近 7 天（日志还在就能算）
python3 quota_history.py --json
```
"""
import argparse
import collections
import datetime
import json
import os
import re
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REQ_LOG = os.path.join(HERE, "logs", "source_requests.log")
TGDB = os.path.join(HERE, "tg.db")
HIST = os.path.join(HERE, "logs", "quota_history.jsonl")


def count_requests(day):
    """统计某天的请求，按类型分。口径固定在这里，不再各写一套。"""
    c = collections.Counter()
    if not os.path.exists(REQ_LOG):
        return c
    for line in open(REQ_LOG, encoding="utf-8", errors="ignore"):
        if not line.startswith('{"ts": "%s' % day):
            continue
        try:
            d = json.loads(line)
        except Exception:                                   # noqa: BLE001
            continue
        p = d.get("path") or ""
        if "SearchTerm" in p:
            c["search"] += 1
        elif "/Episodes" in p:
            c["episodes"] += 1
        else:
            c["other"] += 1
    return c


def count_episodes(day):
    """当天落地的集数。取 tg.db（与 reconcile 同一本账），不数日志。"""
    try:
        d0 = datetime.datetime.strptime(day, "%Y-%m-%d").timestamp()
    except Exception:                                       # noqa: BLE001
        return 0
    try:
        c = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True, timeout=30)
        n = c.execute("SELECT count(*) FROM resource_state WHERE landed_at >= ? "
                      "AND landed_at < ?", (int(d0), int(d0 + 86400))).fetchone()[0]
        c.close()
        return int(n)
    except Exception:                                       # noqa: BLE001
        return 0


def sources_ok(day):
    """当天每个源的成功出片数（来自预算文件的 ok 计数，只在"当天"有意义）。"""
    try:
        b = json.load(open(os.path.join(HERE, ".gha_src_budget.json"), encoding="utf-8"))
    except Exception:                                       # noqa: BLE001
        return {}
    if (b.get("date") or "") != day:
        return {}
    return {k: int(v.get("ok") or 0) for k, v in (b.get("sources") or {}).items()
            if int(v.get("ok") or 0) > 0}



def queue_state():
    """队列分流进度：多少部已「停泊」（判过、在冷却期）、多少还没扫过。

    2026-11-14：per_ep 单独看会误判。队列里 1926 部剧，每部都要**扫一次**
    才能被判成「已收全」并停泊 7 天。在分流跑完之前，大量请求花在
    "扫了但没新集"上，per_ep 自然高；分流跑完后，每天只重扫 1/7 的存量。
    所以必须把「已停泊/总数」和 per_ep 放在一起看，才能分清
    「效率低是因为分流没跑完」还是「代码变差了」。
    """
    out = {"total": 0, "parked": 0, "reasons": {}}
    try:
        q = json.load(open(os.path.join(HERE, ".gha_queue_new.json"), encoding="utf-8"))
        items = q.get("items") if isinstance(q, dict) else q
        out["total"] = len(items or [])
    except Exception:                                       # noqa: BLE001
        pass
    try:
        d = json.load(open(os.path.join(HERE, ".gha_new_done.json"), encoding="utf-8"))
        now = time.time()
        c = collections.Counter()
        for v in d.values():
            if not isinstance(v, dict):
                continue
            if float(v.get("next_check") or 0) > now:
                out["parked"] += 1
                c[v.get("reason") or "?"] += 1
        out["reasons"] = dict(c)
    except Exception:                                       # noqa: BLE001
        pass
    return out

def build(day):
    req = count_requests(day)
    ep = count_episodes(day)
    tot = sum(req.values())
    # 今天还没过完 → 标 partial。**不能拿半天的 per_ep 和整天的比**，
    # 这是本轮差点得出的错误结论（今天 30.92 vs 昨天 8.05，其实只差在没跑完）。
    partial = (day == datetime.date.today().strftime("%Y-%m-%d"))
    return {
        "day": day,
        "partial": partial,
        "requests": tot,
        "search": req.get("search", 0),
        "episodes_req": req.get("episodes", 0),
        "other": req.get("other", 0),
        "episodes": ep,
        # per_ep 是核心指标：每出一集花几次请求。越小越好。
        "per_ep": round(tot / ep, 2) if ep else None,
        "sources_ok": sources_ok(day),
        "queue": queue_state() if not partial else queue_state(),
        "ts": int(time.time()),
    }


def load_hist():
    rows = []
    if not os.path.exists(HIST):
        return rows
    for line in open(HIST, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:                                   # noqa: BLE001
            continue
    return rows


def save(rows):
    """按 day 去重后整体重写（幂等：同一天只留最新一条）。"""
    by = {}
    for r in rows:
        if r.get("day"):
            by[r["day"]] = r
    out = [by[k] for k in sorted(by)]
    tmp = HIST + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in out:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, HIST)
    return out


def print_trend(rows, limit=10):
    if not rows:
        print("  暂无历史。")
        return
    print("  %-13s %6s %7s %7s %8s %7s %10s"
          % ("日期", "请求", "搜索", "列集", "出片", "请求/集", "分流"))
    for r in rows[-limit:]:
        per = r.get("per_ep")
        mark = "*" if r.get("partial") else " "
        qs = r.get("queue") or {}
        qtxt = ("%d/%d" % (qs.get("parked") or 0, qs.get("total") or 0)) \
            if qs.get("total") else "—"
        print("  %-12s%s%6d %7d %7d %8d %7s %10s"
              % (r["day"], mark, r.get("requests") or 0, r.get("search") or 0,
                 r.get("episodes_req") or 0, r.get("episodes") or 0,
                 ("%.2f" % per) if per else "—", qtxt))
    # 趋势只用**完整日**算，partial 日不参与比较
    vals = [r["per_ep"] for r in rows if r.get("per_ep") and not r.get("partial")]
    if len(vals) >= 2:
        print("  趋势（仅完整日，* = 当天未结束）：最新 %.2f，此前均值 %.2f → %s"
              % (vals[-1], sum(vals[:-1]) / (len(vals) - 1),
                 "改善" if vals[-1] < sum(vals[:-1]) / (len(vals) - 1) else "变差"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day")
    ap.add_argument("--backfill", type=int, default=0)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    rows = load_hist()
    days = []
    if a.backfill:
        today = datetime.date.today()
        days = [(today - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                for i in range(a.backfill)][::-1]
    else:
        days = [a.day or datetime.date.today().strftime("%Y-%m-%d")]

    for d in days:
        rec = build(d)
        rows = [r for r in rows if r.get("day") != d] + [rec]
        if not a.json:
            print("  记录 %s：请求 %d（搜索 %d / 列集 %d）出片 %d 集 → %s 请求/集"
                  % (d, rec["requests"], rec["search"], rec["episodes_req"],
                     rec["episodes"],
                     ("%.2f" % rec["per_ep"]) if rec["per_ep"] else "—"))
    rows = save(rows)

    if a.json:
        print(json.dumps(rows[-12:], ensure_ascii=False, indent=1))
    else:
        print()
        print_trend(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
