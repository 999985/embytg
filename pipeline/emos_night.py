#!/usr/bin/env python3
"""emos_night.py —— 夜间 4K 补版窗口：只补「库里还没有 4K」的集。

为什么不塞进 gha_next3 的常规车道：
  常规 lane 的目标是「有没有这一集」，而这里的目标是「这一集有没有 2160p」。
  混在一起会让 new/fill 的判据变复杂，也会抢白天的上传额度。所以单独一个窗口：
  02:10 起，按 UPLOAD 能力跑固定条数，跑完就退出，不常驻。

判据（全部来自真实数据，不猜）：
  · 源侧：PlaybackInfo 里 VideoStream 宽度 >= 3000 或高度 >= 1800 视为 2160p；
  · 本地：resource_state 里同 (tmdb, season, ep) 是否已有 `- 2160p` 文件；
  · 只补差集，已 4K 的跳过，低于 4K 的源直接放弃（不降级占位）。

用法：
    python3 emos_night.py --dry-run          # 只报本轮会补哪些
    python3 emos_night.py --limit 12         # 真跑，最多 12 集
"""
import argparse
import os
import sqlite3
import sys
import time

sys.path.insert(0, "/opt/embytg/pipeline")
import emos_import as EI                                   # noqa: E402
import gha_state as GS                                     # noqa: E402

# 夜间补版清单：剧名 -> (年份, tmdb, 目标目录)
TARGETS = [
    ("航海王", 1999, 37854, "TV/国漫/航海王 (1999) {tmdb=37854}/Season 01"),
]


def local_4k(tmdb):
    """已有 2160p 的 (season, ep) 集合。"""
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    rows = con.execute("select rel from resource_state where kind='episode' and tmdb=?",
                       (tmdb,)).fetchall()
    con.close()
    import re
    have = set()
    for (rel,) in rows:
        if "2160p" not in (rel or ""):
            continue
        m = re.search(r"S(\d{2})E(\d{2,4})", rel)
        if m:
            have.add((int(m.group(1)), int(m.group(2))))
    return have


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=12)
    ap.add_argument("--minutes", type=int, default=330,
                    help="整轮墙钟预算（分钟），到点前 5 分钟起不再开新集；比 unit 的 TimeoutStartSec 短")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--series", default="")
    a = ap.parse_args()

    todo = [t for t in TARGETS if not a.series or t[0] == a.series]
    picked = []
    for name, year, tmdb, tdir in todo:
        e = EI.Emos().login()
        s = e.find_series(name, year)
        if not s:
            print("  跳过 %s：源站找不到" % name)
            continue
        have = local_4k(tmdb)
        eps = e.episodes(s["Id"])
        miss = []
        for it in eps:
            se = (int(it.get("ParentIndexNumber") or 1), int(it.get("IndexNumber") or 0))
            if se in have:
                continue
            ms = (it.get("MediaSources") or [{}])[0]
            q, w, h = EI.quality_of(ms)
            if q != "2160p":
                continue
            miss.append((se[1], w, h, (ms.get("Size") or 0) / 1e9))
        miss.sort()
        print("  %s：源站 4K 共 %d 集，本地缺 %d 集" % (name, sum(
            1 for it in eps if EI.quality_of((it.get("MediaSources") or [{}])[0])[0] == "2160p"),
            len(miss)))
        for x in miss[: a.limit - len(picked)]:
            picked.append({"name": name, "year": year, "tmdb": tmdb, "dir": tdir,
                           "ep": x[0], "w": x[1], "h": x[2], "gb": x[3]})

    picked = picked[: a.limit]
    if not picked:
        print("  没有要补的集")
        return 0
    print("  本轮计划 %d 集：" % len(picked))
    for p in picked:
        print("   %s E%-4s %sx%s %.2fGB" % (p["name"], p["ep"], p["w"], p["h"], p["gb"]))
    if a.dry_run:
        return 0

    ok = 0
    streak = 0
    deadline = time.time() + a.minutes * 60
    os.environ["EMOS_DEADLINE"] = str(int(deadline))
    run_id = "emos-night-%d" % os.getpid()
    os.environ["EMOS_RUN_ID"] = run_id
    # 预约一个号给整个窗口：lane 收尾后不再领它，我们才拿得到（否则 4 个号全天被 lane 填满）。
    reserved = None
    try:
        rows = EI.pool_rows()
        best = max(rows, key=lambda n: (0 if rows[n][1] else 1, rows[n][0])) if rows else None
        best = int(os.environ.get("EMOS_RESERVE_SLOT") or (best or 1))
        if GS.slot_reserve(best, run_id, ttl=a.minutes * 60 + 900):
            reserved = best
            os.environ["EMOS_RESERVED_SLOT"] = str(best)
            print("  预约 slot%d 给本窗口（%d 分钟）" % (best, a.minutes), flush=True)
    except Exception as _e:                                    # noqa: BLE001
        print("  预约号失败（继续按普通方式抢号）: %s" % str(_e)[:80], flush=True)
    os.environ.setdefault("EMOS_TRY_SLOTS", "2")   # 单集最多试 2 个号，别让一集吃光一夜
    os.environ.setdefault("EMOS_PROBE_S", "600")   # 先 10 分钟探针，落地过区段才给整预算
    os.environ.setdefault("EMOS_EP_MAX_S", "2400")  # 单集上限 40 分钟
    os.environ.setdefault("EMOS_ALLOW_SLOW", "1")  # 没有「够快」的号时按 best-effort 试一次
    os.environ.setdefault("EMOS_CHUNK_MIB", "128")  # 512MiB 分片在慢号上 20 分钟才落地一个区段
    for p in picked:
        if time.time() > deadline - 300:
            print("  窗口到点（--minutes %d），提前收工" % a.minutes, flush=True)
            break
        if streak >= 2:
            print("  连续 2 集 0 产出（上传号全线僵死），本轮收工", flush=True)
            break
        cmd = [sys.executable, "/opt/embytg/pipeline/emos_import.py",
               "--series", p["name"], "--year", str(p["year"]),
               "--eps", str(p["ep"]), "--limit", "1", "--apply",
               "--target-dir", p["dir"]]
        rc = os.spawnv(os.P_WAIT, sys.executable, cmd)
        print("  E%s → rc=%s" % (p["ep"], rc), flush=True)
        ok += (rc == 0)
        streak = 0 if rc == 0 else streak + 1
    if reserved is not None:
        try:
            os.unlink(GS._slot_reserve_path(reserved))
        except OSError:
            pass
        print("  撤预约旗 slot%d" % reserved, flush=True)
    print("  完成 %d/%d 集（窗口 %d 分钟）" % (ok, len(picked), a.minutes), flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
