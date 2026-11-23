#!/usr/bin/env python3
"""调度层唯一观测面：一条命令看清「谁在干、还能干多少、卡在哪」。

为什么需要它：调度从「按 lane 一把锁」改成「按集 lease + 多 worker」之后，
状态散在三处（.gha_claims/*.json、tg.db.job_claim、scale.json），
靠 tail 日志拼状态必然拼错。这里统一成一个只读入口。

用法：python3 sched_status.py [--json]
"""
import glob
import json
import os
import re
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gha_state as GS  # noqa: E402

LOCAL_SLOTS = {"slot1", "slot2", "slot3", "slot4"}


def _local_pool():
    """本机实际可用号数 = session 总数 − 被预留的槽位。

    2026-11-21：用户决定本机只跑 new / fill / upgrade 三条 lane，
    slot1（new2）闲置并预留 → 可用 3 个。写死 4 会让容量虚高。
    """
    try:
        sys.path.insert(0, HERE)
        import gha_next3 as _G
        return max(1, _G._session_count() - len(_G.reserved_slots()))
    except Exception:                                          # noqa: BLE001
        return 3


def _scale():
    p = os.path.join(os.path.dirname(HERE), "config", "scale.json")
    try:
        return json.load(open(p, encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        return {}


def worker_of(claim):
    """worker 归属：环境变量 → run_id 的 slotN → local。"""
    rid = str(claim.get("run_id") or "")
    m = re.search(r"slot(\d+)", rid)
    if claim.get("worker"):
        return str(claim["worker"])
    return "slot%s" % m.group(1) if m else "local"


def main():
    as_json = "--json" in sys.argv
    now = int(time.time())

    claims = []
    for p in sorted(glob.glob(os.path.join(GS.CLAIM_DIR, "*.json"))):
        try:
            d = json.load(open(p, encoding="utf-8"))
        except Exception:                                      # noqa: BLE001
            continue
        if not GS.active_claim(p):     # 复用 gha_state 的唯一过期判定，别自己写一套
            continue
        d["_file"] = os.path.basename(p)
        d["_age_min"] = round((now - float(d.get("ts") or 0)) / 60)
        d["_worker"] = worker_of(d)
        claims.append(d)

    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(HERE, "tg.db"),
                          uri=True, timeout=20)
    try:
        con.execute("create table if not exists job_claim("
                    "key text primary key, worker text, ts integer, ttl integer)")
        leases = con.execute("select worker, count(*), max(ts) from job_claim "
                             "where ts + ttl >= ? group by worker", (now,)).fetchall()
        landed = con.execute("select count(*) from resource_state where landed_at > ?",
                             (now - 86400,)).fetchone()[0]
    finally:
        con.close()

    local = [c for c in claims if c["_worker"] in LOCAL_SLOTS or c["_worker"] == "local"]
    ext = [c for c in claims if c not in local]
    health = GS.worker_health(min_rate=float((_scale().get("min_success_rate") or 0.5)),
                              consec_limit=int((_scale().get("consecutive_fail_limit") or 3)))
    sc = _scale()
    ext_declared = sum(int(w.get("sessions") or 1)
                       for w in (sc.get("external_workers") or []) if w.get("enabled", True))

    out = {
        "workers": sorted({c["_worker"] for c in claims}),
        "local_active": len(local),
        "external_active": len(ext),
        "capacities": {
            "local": _local_pool(),
            "external_declared": ext_declared,
            "external_observed": len({c["_worker"] for c in ext}),
            "headroom": 1,
            "total": 4 + max(ext_declared, len({c["_worker"] for c in ext}) + 1, 1),
        },
        "leases": [{"worker": w, "count": n, "last_min": round((now - t) / 60)}
                   for w, n, t in leases],
        "claims": [{"file": c["_file"], "worker": c["_worker"],
                    "series": str(c.get("series"))[:24], "ep": c.get("ep"),
                    "age_min": c["_age_min"], "status": c.get("status")} for c in claims],
        "landed_24h": landed,
    }

    if as_json:
        print(json.dumps(out, ensure_ascii=False, indent=1))
        return 0

    print("worker 总览: %s" % ", ".join(out["workers"]))
    print("并发占用  : 本机 %d / 外部 %d  （总容量 %d = %d 本机可用 + %d 外部 + 1 预留）"
          % (out["local_active"], out["external_active"], out["capacities"]["total"],
             out["capacities"]["local"],
             max(out["capacities"]["external_declared"],
                 out["capacities"]["external_observed"])))
    print("条目 lease:")
    for l in out["leases"]:
        print("  %-10s %3d 条，最近 %d 分钟前" % (l["worker"], l["count"], l["last_min"]))
    print("活跃 claim:")
    for c in out["claims"][:12]:
        print("  %-8s %-24s E%-4s %3d 分钟前  %s"
              % (c["worker"], c["series"], c["ep"], c["age_min"], c["status"]))
    if health:
        print("worker 健康度:")
        for w, h in sorted(health.items()):
            print("  %-10s 成功 %-4d worker失败 %-3d 源站失败 %-3d 风控 %-3d 成功率 %.0f%% %s"
                  % (w, h["ok"], h["fail_worker"], h["fail_source"], h["fail_auth"],
                     h["rate"] * 100, "健康" if h["healthy"] else "已退出产能"))
    print("近 24h 入库: %d" % landed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
