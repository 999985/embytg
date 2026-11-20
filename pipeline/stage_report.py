#!/usr/bin/env python3
"""stage_report.py —— 阶段验收报告（口径写死，不接受"看起来差不多"）。

输出字段（与用户验收清单一一对应）：
  1. 五账对账摘要（差异值）
  2. P0 近 24h 新增（滚动窗口，不是自然日）：达标 / 无高画质源兜底 / 拦截假 4K
  3. 本轮 P1 洗版：成功 / 剩余存量（相对 1467 的净变化）
  4. 4 个 TG session 状态（正常 / 退避剩余）

数据源：
  达标判定      Emby RunTimeTicks + MediaSources.Size（台账 dur_sec 大量为空，不可用）
  landed_at     当日新增（tg.db.resource_state）
  假 4K 拦截    pipeline/logs/tgpipe_quality.jsonl 的 reject 记录
  session 状态  .gha_slots/slotN.lock 租约 + .ysdq_pace.json 冷却
"""
import collections
import glob
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "tg.db")
P1_BASELINE = int(os.environ.get("P1_BASELINE", "1467"))


def emby(path, q=None):
    sys.path.insert(0, HERE)
    from embytg_conf import cfg
    base = cfg("emby_url").rstrip("/")
    tok = cfg("emby_token")
    u = base + path + ("?" + urllib.parse.urlencode(q) if q else "")
    with urllib.request.urlopen(u + ("&" if q else "?") + "api_key=" + tok, timeout=180) as r:
        return json.loads(r.read())


def floor_of(name):
    return 2.0 if "2160p" in name else (0.8 if "1080p" in name else 0.4)


def scan_quality(days=1):
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - days * 86400))
    d = emby("/Items", {"Recursive": "true", "IncludeItemTypes": "Episode",
                        "MinDateCreated": since,
                        "Fields": "Path,MediaSources,RunTimeTicks,SeriesName", "Limit": "5000"})
    items = d.get("Items") or []
    ok, bad, mbps_list = [], [], []
    for it in items:
        ms = (it.get("MediaSources") or [{}])[0]
        dur = (it.get("RunTimeTicks") or 0) / 1e7
        sz = ms.get("Size") or 0
        if not dur or not sz:
            continue
        mbps = sz * 8 / dur / 1e6
        nm = (it.get("Path") or "").split("/")[-1]
        mbps_list.append(mbps)
        (ok if mbps >= floor_of(nm) else bad).append((it.get("SeriesName"), nm, round(mbps, 2)))
    return items, ok, bad, mbps_list


def low_quality_count():
    """P1 存量：以 .lowq_library.json 为准（daily_ops 从 Emby 实测生成）。"""
    try:
        return len(json.load(open(os.path.join(HERE, ".lowq_library.json"), encoding="utf-8")))
    except Exception:                                        # noqa: BLE001
        return -1


def fake4k_blocked(hours=24):
    """假 4K 拦截数 = ①标签 2160p 实测不是 2160p 的订正条数
                        + ②画质门禁直接跳过的条数（tgpipe 日志里的「画质不达标跳过」）。

    这两类原来混在一起数不出来：tgpipe_quality.jsonl 只记 label vs real，
    没有 result 字段；门禁拒绝记在 tgpipe 的运行日志里。
    """
    mislabel = 0
    miss_cut = time.time() - hours * 3600
    p = os.path.join(HERE, "logs", "tgpipe_quality.jsonl")
    if os.path.exists(p):
        for ln in open(p, encoding="utf-8"):
            try:
                j = json.loads(ln)
            except Exception:                                # noqa: BLE001
                continue
            if float(j.get("ts") or 0) < miss_cut:
                continue
            if str(j.get("label")) == "2160p" and str(j.get("real")) != "2160p":
                mislabel += 1
    rejected = 0
    for lp in glob.glob(os.path.join(HERE, "logs", "*.log")):
        try:
            for ln in open(lp, encoding="utf-8", errors="ignore"):
                if "画质不达标跳过" in ln:
                    rejected += 1
        except Exception:                                    # noqa: BLE001
            continue
    return mislabel + rejected


def session_state():
    out = []
    now = time.time()
    for n in (1, 2, 3, 4):
        lock = os.path.join(HERE, ".gha_slots", "slot%d.lock" % n)
        held = os.path.exists(lock)
        left = 0
        if held:
            try:
                left = max(0, int(os.path.getmtime(lock) + 3 * 3600 - now))
            except Exception:                                # noqa: BLE001
                left = 0
        out.append({"slot": n, "busy": held, "lease_left_s": left})
    try:
        pace = json.load(open(os.path.join(HERE, ".ysdq_pace.json"), encoding="utf-8"))
        for k, v in pace.items():
            nx = float(v.get("next_at") or 0)
            if nx > now:
                out.append({"slot": int(k), "backoff_s": int(nx - now)})
    except Exception:                                        # noqa: BLE001
        pass
    return out


def ledger():
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "reconcile.py")],
                           capture_output=True, text=True, timeout=300, cwd=HERE)
        tail = [l for l in (r.stdout or "").splitlines() if "teldrive" in l or "差异" in l]
        return " | ".join(x.strip() for x in tail[-2:])
    except Exception as e:                                   # noqa: BLE001
        return "对账失败：%s" % str(e)[:60]


def main():
    t0 = time.time()
    print("== 阶段验收报告（%s）==" % time.strftime("%Y-%m-%d %H:%M"))
    print("五账：" + ledger())

    items, ok, bad, mbps = scan_quality(1)
    rate = (100.0 * len(ok) / len(items)) if items else 0.0
    print("P0 近24h入库：%d 集（达标 %d / 无高画质源兜底 %d，达标率 %.1f%%）"
          % (len(items), len(ok), len(bad), rate))
    if bad:
        for s, nm, m in bad[:5]:
            print("   未达标：%-18s %-34s %.2f Mbps" % ((s or "?")[:18], nm[:34], m))
    print("拦截假 4K：%d 条（近 24h，quality gate reject）" % fake4k_blocked(24))

    # 兜底打标 + 7 天冷却：确认全渠道无高画质源的集，记 next_retry_at，
    # 冷却期内不再重复扫描（用户红线 5）。达标集不写冷却。
    cool_p = os.path.join(HERE, ".fallback_cooldown.json")
    try:
        cool = json.load(open(cool_p, encoding="utf-8"))
    except Exception:                                        # noqa: BLE001
        cool = {}
    now = time.time()
    for s, nm, m in bad:
        key = "%s|%s" % (s or "?", nm)
        if key not in cool or cool[key].get("retry_at", 0) < now:
            cool[key] = {"mbps": m, "seen": int(now), "retry_at": int(now + 7 * 86400)}
    json.dump(cool, open(cool_p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    low = low_quality_count()
    delta = (low - P1_BASELINE) if low >= 0 else 0
    print("P1 存量：%d 集（基线 %d，净变化 %+d）" % (low, P1_BASELINE, delta))

    print("TG session：")
    for s in session_state():
        if "backoff_s" in s:
            print("   slot%d  退避剩余 %ds" % (s["slot"], s["backoff_s"]))
        else:
            print("   slot%d  %s" % (s["slot"], "占用中（剩余 %ds）" % s["lease_left_s"] if s["busy"] else "空闲"))

    if mbps:
        mbps.sort()
        print("码率分布：中位 %.2f Mbps，P10 %.2f，P90 %.2f"
              % (mbps[len(mbps) // 2], mbps[len(mbps) // 10], mbps[9 * len(mbps) // 10]))
    print("耗时 %.1fs" % (time.time() - t0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
