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
    """已有 2160p 的 (season, ep) 集合（resource_state ∪ state_key）。

    2026-11-26（E213）：resource_state 由 rsscan 每 30 分钟从挂载重建，刚落库的集
    在下次扫描前「看不见」；本窗口刚补完的集若马上再跑一轮就会被重复选、重复下载。
    state_key 是 pc_register 在上传完成当刻同步写入的去重台账（key 形如
    tv|id:37854|(1, 54)|2160p），两处取并集才是真实的「本地已有」。
    """
    con = sqlite3.connect("file:/opt/embytg/pipeline/tg.db?mode=ro", uri=True, timeout=15)
    rows = con.execute("select rel from resource_state where kind='episode' and tmdb=?",
                       (tmdb,)).fetchall()
    prefix = "tv|id:%d|" % int(tmdb)
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)     # 前缀范围查询，不用 LIKE（20 万集）
    keys = con.execute(
        "select k, val from state_key where k >= ? and k < ?", (prefix, upper)).fetchall()
    con.close()
    import re
    have = set()
    for (rel,) in rows:
        if "2160p" not in (rel or ""):
            continue
        m = re.search(r"S(\d{2})E(\d{2,4})", rel)
        if m:
            have.add((int(m.group(1)), int(m.group(2))))
    for k, val in keys:
        # skip 条目是「源禁止转发」的占位，字节不在我们手里，不能算已有。
        if '"skip"' in (val or ""):
            continue
        parts = (k or "").split("|")
        if len(parts) != 4 or parts[3] != "2160p":
            continue
        try:
            se, ep = [int(x) for x in parts[2].strip("()").split(",")]
        except ValueError:
            continue
        have.add((se, ep))
    return have


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=12)
    ap.add_argument("--minutes", type=int, default=330,
                    help="整轮墙钟预算（分钟），到点前 5 分钟起不再开新集；比 unit 的 TimeoutStartSec 短")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--series", default="")
    a = ap.parse_args()

    # 2026-11-25（E208）：开窗前先把「字节已在 teldrive、Emby 没建出条目」的集补扫一遍。
    # 这些集以前被记成「入库完成」，实际 Emby 里没有 → 缺口不缩小且不会再被选中。
    try:
        EI.pending_flush()
    except Exception as _e:                                    # noqa: BLE001
        print("  待扫队列处理失败（不影响本轮）: %s" % str(_e)[:80], flush=True)

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
            miss.append((se[0], se[1], w, h, (ms.get("Size") or 0) / 1e9))
        miss.sort()
        print("  %s：源站 4K 共 %d 集，本地缺 %d 集" % (name, sum(
            1 for it in eps if EI.quality_of((it.get("MediaSources") or [{}])[0])[0] == "2160p"),
            len(miss)))
        for x in miss[: a.limit - len(picked)]:
            picked.append({"name": name, "year": year, "tmdb": tmdb, "dir": tdir,
                           "season": x[0], "ep": x[1], "w": x[2], "h": x[3], "gb": x[4]})

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
    rounds = 0
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
    # 2026-11-25（E206）：昨夜 0/12 的现场复盘 —— 02:10-02:40 四个号同时被 TG 限流到
    # 0.08MB/s（同一时段下载仍有 4.8MB/s、裸 TCP 到 DC 8-13MB/s，所以不是本机带宽），
    # 而本窗口的判据是「128MiB 区段 + 只试 2 个号 + 连错 2 集就整轮收工」：
    # 一个区段要 1600s，探针 600s 内 0 落地 → 判死 → 试两个号 → streak=2 → 收工 0/12。
    # 慢不等于死：区段缩到 32MiB（0.08MB/s 下 400s 落地，进度能累积），
    # 四个号都试（号间差异 0.08-2.6MB/s 是常态），且不许「两集失败就整轮放弃」。
    os.environ.setdefault("EMOS_TRY_SLOTS", "4")   # 四个号都试：单号被限流时换号才有意义
    os.environ.setdefault("EMOS_PROBE_S", "900")   # 32MiB@0.08MB/s≈400s，900s 才判得准
    os.environ.setdefault("EMOS_EP_MAX_S", "3600")  # 单集上限 60 分钟（慢号上传 2GB 要 3h+，靠跨轮续传）
    os.environ.setdefault("EMOS_ALLOW_SLOW", "1")  # 没有「够快」的号时按 best-effort 试一次
    os.environ.setdefault("EMOS_CHUNK_MIB", "32")  # 128MiB 在 0.4MB/s 慢号上 320s/段必超时（同 E205）
    stop = False
    for p in picked:
        if stop:
            break
        # 2026-11-25（E206）：昨夜这里是「连错 2 集 → break → 0/12 收工」。
        # 上传限流是**有窗口的**（同号实测 03:44 是 0.12MB/s、05:41 是 6MB/s），
        # 两集失败只说明「此刻」，不说明「今晚」。改成：同一集冷却后重试，
        # 单集最多 2 次、整轮最多 3 轮冷却；真僵死时成本只有一次下载 + 一段探针。
        for attempt in range(2):
            # 2026-11-26（E213）：子进程的「未起跑」分支（窗口<10min / 选号闸门放弃）
            # 按设计 return 0（不能靠退出码表达「今天不跑」，否则 systemd 记单元失败）。
            # 于是这里 600s 内起跑的子进程必定空转，白占一次 attempt。直接按 600s 收口。
            if time.time() > deadline - 600:
                print("  窗口剩余不足 10 分钟，本轮收工", flush=True)
                stop = True
                break
            cmd = [sys.executable, "/opt/embytg/pipeline/emos_import.py",
                   "--series", p["name"], "--year", str(p["year"]),
                   "--eps", str(p["ep"]), "--limit", "1", "--apply",
                   "--target-dir", p["dir"]]
            rc = os.spawnv(os.P_WAIT, sys.executable, cmd)
            # 2026-11-26（E213）：完成数必须是**台账里的真值**，不是子进程退出码。
            # 今晨 07:2x 的现场：E61-E65 五集全部「窗口剩余不足 10 分钟，本轮不起跑」
            # → 子进程 return 0 → 这里 ok+=1，于是只有 6 集真正落库（E55-E60），
            # 日志却报「完成 11/12 集」。缺口照旧、数字虚高，第二天看日志的人被误导。
            # 判据改成 local_4k（state_key ∪ resource_state 里该集有没有 2160p）：
            # pc_register 登记完成才算成，其余一律不计。
            landed = (p["season"], p["ep"]) in local_4k(p["tmdb"])
            print("  E%s → rc=%s（第 %d 次）%s" % (
                p["ep"], rc, attempt + 1, "已落库" if landed else "未落库"), flush=True)
            if landed:
                ok += 1
                streak = 0
                break
            if rc == 0:
                print("  rc=0 但库里仍无该集 2160p（未起跑或未入库）→ 不计完成", flush=True)
            streak += 1
            pause = min(1800, max(300, int(max(60, deadline - time.time()) / 8)))
            if rounds >= 3 or time.time() + pause > deadline - 300:
                print("  连续 %d 集 0 产出，已冷却 %d 轮，本轮收工" % (streak, rounds), flush=True)
                stop = True
                break
            rounds += 1
            # 2026-11-25（E210）：预约号是「保证有一个号」的手段，不是「只能用它」。
            # 02:10 那夜预约到 slot1（当时实测最优），之后它被 TG 压到 0.41MB/s，
            # 而其余三号全被 lane 占着 → 每一集都撞同一堵墙，整夜 0/12。
            # 0 产出后撤预约旗：下一轮 acquire_slot 会按实测吞吐重新挑，
            # 等到任何一条 lane 交班就把号接过来（等待有 EMOS_DEADLINE 兜底）。
            if reserved is not None:
                try:
                    os.unlink(GS._slot_reserve_path(reserved))
                    print("  撤预约旗 slot%d（该号 0 产出，下一轮重新选号）" % reserved,
                          flush=True)
                except OSError:
                    pass
                except Exception:                              # noqa: BLE001
                    pass
                reserved = None
                os.environ.pop("EMOS_RESERVED_SLOT", None)
            print("  0 产出（上传限流窗口），冷却 %ds 后%s（第 %d 轮冷却）"
                  % (pause, ("重试 E%s" % p["ep"]) if attempt == 0 else "换下一集", rounds),
                  flush=True)
            time.sleep(pause)
    if reserved is not None:
        try:
            os.unlink(GS._slot_reserve_path(reserved))
        except OSError:
            pass
        print("  撤预约旗 slot%d" % reserved, flush=True)
    print("  完成 %d/%d 集（窗口 %d 分钟）@%s" % (ok, len(picked), a.minutes,
                                                time.strftime("%Y-%m-%d %H:%M:%S")), flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
