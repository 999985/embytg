#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""series_nfo_heal.py —— 补全「逐集 NFO 齐、剧级 tvshow.nfo 缺」的缺口。

## 为什么需要它

`post_ingest.py` 的 ⓪ 步（ensure_series_assets）会给**本批**入库的新剧写
tvshow.nfo+poster。这一步失败是**一次性**的：TMDB 一次超时、FUSE 一次写失败，
这部剧就再也没人碰 —— 因为后续批次只关心"本批"的剧。

2026-11-24 实测：`post_ingest.py --verify-only` 每天报
`⑤ Series 身份：FAIL 6`（tvshow.nfo 缺 title / 文件不存在），而 daily_ops 4.73
只给出「修：post_ingest.py --tmdb <id>」的人工提示，**没有任何自动消费者**。
同一批 6 部剧，用 ensure_series_assets 重跑一次 7 秒全部补齐（fixed=5, failed=[]），
说明缺口纯粹是"没人重试"，不是拿不到数据。

## 判据（缺一不动）

1. 挂载点活着（`mountpoint -q` 判据，不信 systemd is-active，E013）
2. 从 teldrive PG 一条 SQL 找「有 Season 目录但没有 tvshow.nfo」的剧，
   **不 walk FUSE**（800 部剧 stat 要 3 分钟）
3. 冷却：同一部剧 HEAL_COOLDOWN_H 内最多试一次（TMDB 挂了时不刷外部接口）
4. 单轮上限 `--max`（默认 8），串行调用 ensure_series_assets

## 用法

    python3 series_nfo_heal.py                 # 只报告（默认 dry-run）
    python3 series_nfo_heal.py --apply --max 8 # 真补
    python3 series_nfo_heal.py --json          # 机器读

state：`.series_nfo_heal.json`（冷却）；日志：`logs/series_nfo_heal.log`
"""
import argparse
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

MNT = "/opt/embytg/mnt/embytg"
STATE_F = os.path.join(HERE, ".series_nfo_heal.json")
LOG_F = os.path.join(HERE, "logs", "series_nfo_heal.log")
HEAL_COOLDOWN_H = float(os.getenv("SERIES_NFO_COOLDOWN_H", "6"))
PSQL = ["docker", "exec", "-i", "teldrive-postgres", "psql",
        "-U", "teldrive", "-d", "teldrive", "-Atq", "-F", "|"]

SQL_MISSING = """
with dirs as (select id,parent_id,name from files where kind='folder' and deleted_at is null),
     tv as (select id from dirs where name='TV' and parent_id is null),
     lv1 as (select d.id,d.name lib from dirs d join tv on d.parent_id=tv.id),
     lv2 as (select d.id,d.name n2,lv1.lib from dirs d join lv1 on d.parent_id=lv1.id),
     season as (select distinct parent_id from dirs where name ~ '^Season'),
     has_nfo as (select distinct parent_id from files where name='tvshow.nfo' and deleted_at is null)
select lv2.lib||'|'||lv2.n2 from lv2
 where lv2.id in (select parent_id from season)
   and lv2.id not in (select parent_id from has_nfo)
 order by 1;
"""


def log(msg):
    os.makedirs(os.path.dirname(LOG_F), exist_ok=True)
    line = "%s %s" % (time.strftime("%F %T"), msg)
    with open(LOG_F, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    print(line)


def mount_alive():
    try:
        return subprocess.run(["mountpoint", "-q", MNT], timeout=15).returncode == 0
    except Exception:                                          # noqa: BLE001
        return False


def missing_series():
    r = subprocess.run(PSQL + ["-c", SQL_MISSING], capture_output=True,
                       text=True, timeout=120)
    out = []
    for line in (r.stdout or "").splitlines():
        if "|" not in line:
            continue
        lib, name = line.split("|", 1)
        out.append((lib.strip(), name.strip()))
    return out


def load_state():
    try:
        return json.load(open(STATE_F, encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        return {}


def save_state(st):
    tmp = STATE_F + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(st, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, STATE_F)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真补（默认只报告）")
    ap.add_argument("--max", type=int, default=8, help="单轮最多补几部")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    if not mount_alive():
        log("挂载不可用（mountpoint 判据），本轮跳过")
        return 0

    missing = missing_series()
    st = load_state()
    now = time.time()
    cool = {k: v for k, v in st.items() if now - float(v or 0) < HEAL_COOLDOWN_H * 3600}
    todo = [m for m in missing if "%s/%s" % m not in cool]
    report = {"missing": len(missing), "eligible": len(todo),
              "healed": 0, "failed": [], "skipped_cooldown": len(missing) - len(todo)}

    if a.apply and todo:
        import post_ingest as P
        batch, report["failed"] = todo[:max(1, a.max)], []
        fixed, failed = P.ensure_series_assets(batch)
        report["healed"] = int(fixed)
        report["failed"] = failed
        for lib, name in batch:
            cool["%s/%s" % (lib, name)] = now
    save_state(cool)

    msg = ("缺口 %d 部；可补 %d；已补 %d；冷却跳过 %d%s"
           % (report["missing"], report["eligible"], report["healed"],
              report["skipped_cooldown"],
              ("；失败 %s" % ", ".join(report["failed"][:3])) if report["failed"] else ""))
    if a.apply:
        log(msg)
    if a.json:
        print(json.dumps(report, ensure_ascii=False))
    else:
        print(msg)
        for lib, name in todo[:10]:
            print("  - %s/%s" % (lib, name))
    return 0 if not report["failed"] else 2


if __name__ == "__main__":
    sys.exit(main())
