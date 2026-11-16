#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tgdb_daily_sync.py — tg.db 每日增量同步（凌晨 04:00 由 /etc/cron.d 调度）。

设计（2026-11-09 用户授权「凌晨 4 点 / 允许 / 范围与周期自己定」）：

  范围：只同步 TODB 清单层 `todb_video` + 同步水位 `todb_meta(daily_sync_*)`。
        不动 resource_state / moviewatch / state_key / pending / meta_cache / scan_raw
        （本地派生台账与 tgpipe 管线状态，语义不同，不该由外部源覆盖）。
        不跑 next_check_at 复核队列（需 TG 搜源=外部写，当前入库未开闸）。

  依据（只读探针实测 2026-11-09 22:1x）：
    - 网关 A `GET /video/list?sort_by=updated_at&sort_order=desc&page=N&page_size=100`
      确实生效：默认返回 101/1000/1001 升序，带参数返回 379390/378546/379389 倒序，
      乱造 sort_by 返回空 items（有参数校验）。
    - 响应**不返回 updated_at 字段**，因此不能按时间戳比水位；改按
      「倒序翻页 + 内容对比 + 连续 STP_QUIET 页无变化即停」判定收敛。
    - 不能按 `max(video_id)` 判新：本地 max=379320，但新集合里既有更大的
      379390/379389，也有更小的 378546 / 16495（老片元数据被更新）。
    - 全量基线 total=378217（今日 11:21-11:50），网关当前 total=378287，差 70 条。

  安全：
    - 默认 **--dry 只读不写**；加 --apply 才写库。
    - 写库前 `cp tg.db` 到 backup/tg.db.pre_daily_<ts>（改名 agent 共用，可回滚）。
    - WAL + busy_timeout，单条 UPSERT 幂等，同日重跑不产生重复。
    - 只 UPDATE 清单层列（title/origin_title/date_air/year/countrys/languages/
      video_type/synced_at），**不覆盖** find_state / tmdb_id / expect_eps 等状态机列。
    - 不调用 TG、不 POST Emby、不刮削、不转存、不改 CHANNELS。

用法：
  python3 tgdb_daily_sync.py                 # --dry，只打印本该更新几条
  python3 tgdb_daily_sync.py --apply        # 真写库（cron 用这个）
  python3 tgdb_daily_sync.py --status       # 只看上次水位
  python3 tgdb_daily_sync.py --apply --max-pages 5   # 小批量试跑
"""
import argparse
import json
import os
import shutil
import sqlite3
import sys
import time
import urllib.request

DB = "/opt/embytg/pipeline/tg.db"
BACKUP_DIR = "/opt/embytg/backup"
B = "https://theotherdb.org/api"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
PAGE_SIZE = 100
STP_QUIET = 3          # 连续 3 页「无任何新增/变更」即认为追到上次水位
MAX_PAGES_CAP = 200    # 单次上限，防止网关异常导致无限翻页（200 页=2 万条）
RETRIES = 3

# 同步水位键（不复用全量游标 list_page=3783）
K_LAST_TOTAL = "daily_sync_last_total"
K_LAST_AT = "daily_sync_last_at"
K_LAST_PAGES = "daily_sync_last_pages"
K_LAST_NEW = "daily_sync_last_new"
K_LAST_UPD = "daily_sync_last_upd"


def get(path, retries=RETRIES):
    for i in range(retries):
        try:
            req = urllib.request.Request(B + path, headers={
                "accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(2 * (i + 1))


def ro_db():
    c = sqlite3.connect("file:%s?mode=ro" % DB, uri=True)
    c.row_factory = sqlite3.Row
    return c


def rw_db():
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=10000")
    return c


def status():
    c = ro_db()
    print("# tg.db 每日同步水位（只读）")
    for k in [K_LAST_TOTAL, K_LAST_AT, K_LAST_PAGES, K_LAST_NEW, K_LAST_UPD, "list_page"]:
        r = c.execute("SELECT v FROM todb_meta WHERE k=?", (k,)).fetchone()
        v = r[0] if r else "<unset>"
        if k == K_LAST_AT and str(v).isdigit():
            v = "%s (%s)" % (v, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(v))))
        print("  %-24s = %s" % (k, v))
    print("  todb_video rows = %d" % c.execute("SELECT count(*) FROM todb_video").fetchone()[0])
    print("  max(video_id)   = %s" % c.execute("SELECT max(video_id) FROM todb_video").fetchone()[0])
    c.close()
    return 0


def to_tuple(it, now):
    """把网关 item 映射成 todb_video 清单层列；与 todb_sync.py sync_pages 口径一致。"""
    da = it.get("date_air") or ""
    return (
        int(it["video_id"]),
        it.get("video_type"),
        it.get("video_title"),
        it.get("origin_title"),
        da or None,
        (da[:4] or None) if da else None,
        json.dumps(it.get("origin_countrys") or [], ensure_ascii=False),
        json.dumps(it.get("original_languages") or [], ensure_ascii=False),
        now,
    )


UPSERT = """INSERT INTO todb_video(video_id,video_type,title,origin_title,
    date_air,year,countrys,languages,synced_at)
    VALUES(?,?,?,?,?,?,?,?,?)
    ON CONFLICT(video_id) DO UPDATE SET
      video_type=excluded.video_type, title=excluded.title,
      origin_title=excluded.origin_title, date_air=excluded.date_air,
      year=excluded.year, countrys=excluded.countrys,
      languages=excluded.languages, synced_at=excluded.synced_at"""


def changed(c, row):
    """与库内现有行对比：不存在=新增；任一清单层列不同=更新。"""
    vid = row[0]
    r = c.execute("SELECT video_type,title,origin_title,date_air,year,countrys,languages "
                  "FROM todb_video WHERE video_id=?", (vid,)).fetchone()
    if r is None:
        return "new"
    # 库内 year 可能是 int 或 str，统一成 str 比
    cur = tuple("" if x is None else str(x) for x in r)
    new = tuple("" if x is None else str(x) for x in row[1:8])
    return "upd" if cur != new else None


KEEP_SNAPSHOTS = 2    # 每天 133MB，只留最近 2 份，避免吃满磁盘（当前余 31G）


def snapshot():
    """写库前快照共用库（改名 agent 也在用），可回滚；自动保留最近 KEEP_SNAPSHOTS 份。"""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dst = os.path.join(BACKUP_DIR, "tg.db.pre_daily_%s" % time.strftime("%Y%m%d_%H%M%S"))
    # WAL 模式下先 checkpoint，保证单文件快照完整
    c = sqlite3.connect(DB, timeout=30)
    try:
        c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        c.close()
    shutil.copy2(DB, dst)
    # 只按主库快照计数（排除 -shm / -wal 侧文件），否则会把配额算错
    old = sorted(f for f in os.listdir(BACKUP_DIR)
                 if f.startswith("tg.db.pre_daily_")
                 and not f.endswith(("-shm", "-wal")))
    for f in old[:-KEEP_SNAPSHOTS]:
        try:
            os.remove(os.path.join(BACKUP_DIR, f))
            print("# 清理旧快照 %s" % f, flush=True)
        except OSError:
            pass
    return dst


def run(apply, max_pages):
    t0 = time.time()
    now = int(t0)
    c = rw_db() if apply else ro_db()

    head = get("/video/list?sort_by=updated_at&sort_order=desc&page=1&page_size=1")
    total = int(head.get("total") or 0)
    prev_total = 0
    r = c.execute("SELECT v FROM todb_meta WHERE k=?", (K_LAST_TOTAL,)).fetchone()
    if r and str(r[0]).isdigit():
        prev_total = int(r[0])
    print("# gateway total=%d prev_total=%d delta=%d mode=%s" % (
        total, prev_total, total - prev_total, "APPLY" if apply else "DRY"), flush=True)

    snap = None
    if apply:
        snap = snapshot()
        print("# 写库前快照: %s (%d bytes)" % (snap, os.path.getsize(snap)), flush=True)
        c = rw_db()  # checkpoint 后重连

    cap = min(max_pages or MAX_PAGES_CAP, MAX_PAGES_CAP)
    quiet, n_new, n_upd, n_same, pages = 0, 0, 0, 0, 0
    try:
        for p in range(1, cap + 1):
            d = get("/video/list?sort_by=updated_at&sort_order=desc&page=%d&page_size=%d"
                    % (p, PAGE_SIZE))
            items = d.get("items") or []
            if not items:
                print("# page %d 空，停止" % p, flush=True)
                break
            pages = p
            batch_new, batch_upd, batch_same = 0, 0, 0
            rows = []
            for it in items:
                if not it.get("video_id"):
                    continue
                row = to_tuple(it, now)
                kind = changed(c, row)
                if kind is None:
                    batch_same += 1
                else:
                    rows.append(row)
                    if kind == "new":
                        batch_new += 1
                    else:
                        batch_upd += 1
            n_new += batch_new; n_upd += batch_upd; n_same += batch_same
            if apply and rows:
                c.executemany(UPSERT, rows)
                c.commit()
            print("  page %-3d n=%-3d new=%-3d upd=%-3d same=%-3d %s" % (
                p, len(items), batch_new, batch_upd, batch_same,
                "" if (batch_new + batch_upd) else "quiet"), flush=True)
            if batch_new + batch_upd == 0:
                quiet += 1
                if quiet >= STP_QUIET:
                    print("# 连续 %d 页无变化，判定追平上次水位，停止" % STP_QUIET, flush=True)
                    break
            else:
                quiet = 0
        else:
            print("# 达到单次上限 %d 页，本轮停止（下次从同样倒序头部继续，幂等）" % cap, flush=True)

        if apply:
            meta = [
                (K_LAST_TOTAL, str(total)),
                (K_LAST_AT, str(now)),
                (K_LAST_PAGES, str(pages)),
                (K_LAST_NEW, str(n_new)),
                (K_LAST_UPD, str(n_upd)),
            ]
            c.executemany("INSERT INTO todb_meta(k,v) VALUES(?,?) "
                          "ON CONFLICT(k) DO UPDATE SET v=excluded.v", meta)
            c.commit()
            print("# 水位已写入 todb_meta", flush=True)
        else:
            print("# DRY：未写库、未写水位、未生成快照", flush=True)
    finally:
        c.close()

    print("DONE pages=%d new=%d upd=%d same=%d elapsed=%.1fs snapshot=%s" % (
        pages, n_new, n_upd, n_same, time.time() - t0, snap or "-"), flush=True)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真写库；缺省 dry")
    ap.add_argument("--status", action="store_true", help="只看水位")
    ap.add_argument("--max-pages", type=int, default=0, help="单次页数上限（试跑用）")
    a = ap.parse_args()
    if a.status:
        return status()
    return run(a.apply, a.max_pages)


if __name__ == "__main__":
    sys.exit(main())
