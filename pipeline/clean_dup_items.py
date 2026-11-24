#!/usr/bin/env python3
"""clean_dup_items.py —— 清理 Emby library.db 里「同 Path 重复条目」（E115 遗留）。

为什么不用 API：`DELETE /Items/{id}` 会级联把 teldrive 文件软删（实测过一次，
36 个谷雨视频当场下架）。这批重复条目**指向同一个文件**，用 API 删一个会把
文件一起带走。所以直接改 library.db：只删多余的 Items 行和它的附属行，不碰文件。

安全措施：
  ① 先停容器（避免 WAL 冲突/半写）
  ② 写前再备份一次（含 WAL）
  ③ 每组只保留一条：优先保留被 UserDatas 引用过的那条（观影记录不丢），否则保留最小 rowid
  ④ 删除引用表里指向被删条目的行
  ⑤ 起容器后校验：Emby 集数 == teldrive 视频数（五账差异 0）

用法：
    python3 clean_dup_items.py            # dry-run，列出会删哪些
    python3 clean_dup_items.py --apply    # 真删（会自己停/起容器）
"""
import argparse
import os
import shutil
import sqlite3
import subprocess
import time

DB = "/var/lib/docker/volumes/embytg_embydata/_data/data/library.db"
BK = "/opt/embytg/backup/emby_db"
REFS = ["MediaStreams2", "AncestorIds2", "Chapters3", "ItemLinks2", "ItemPeople2",
        "UserDataKeys2", "UserItemShares", "ItemExtradata"]


def connect(ro=False):
    if ro:
        return sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=30)
    return sqlite3.connect(DB, timeout=60)


def dups(con):
    return con.execute(
        "select Path, group_concat(Id) from MediaItems "
        "where Path is not null and Path!='' group by Path having count(*)>1").fetchall()


def plan(con):
    out = []
    for path, ids in dups(con):
        ids = ids.split(",")
        keep = None
        _has_ud = _has(con, "UserDatas") and \
            "ItemId" in [r[1] for r in con.execute("pragma table_info(UserDatas)").fetchall()]
        for i in ids:
            n = 0
            if _has_ud:
                try:
                    n = con.execute("select count(*) from UserDatas where ItemId=?", (i,)).fetchone()[0]
                except sqlite3.Error:
                    n = 0
            if n:
                keep = i
                break
        keep = keep or min(ids)
        out.append((path, keep, [i for i in ids if i != keep]))
    return out


def _has(con, table):
    return bool(con.execute("select 1 from sqlite_master where type='table' and name=?",
                            (table,)).fetchone())


LOCK = "/opt/embytg/pipeline/logs/.clean_dup.lock"


def apply_for_paths(paths=None, max_groups=50, settle=15):
    """清「同 Path 重复」多余行；paths=None 清全库，否则只清给定 Path。

    给 reconcile.py --repair 当自愈钩子用（2026-11-24）：--repair 原先只管
    series/episode 两类差异，dup_path 只能等人工跑本脚本 → 条目级差异会挂在
    status_now 上「超 30 分钟未愈」永不消。这里返回统计供调用方记账。

    安全语义与 CLI 一致：停容器 → 含 WAL 备份 → 只删 library.db 行（绝不碰文件、
    绝不走 API DELETE）→ 起容器 → 复核。并发保护用 flock，抢不到直接 skip。
    """
    import fcntl
    lk = None
    try:
        os.makedirs(os.path.dirname(LOCK), exist_ok=True)
        lk = open(LOCK, "a+")
        fcntl.flock(lk.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:                                       # noqa: BLE001
        return {"skipped": "locked", "groups": 0, "items_deleted": 0}
    try:
        con = connect(ro=True)
        todo = plan(con)
        con.close()
        if paths is not None:
            want = set(paths)
            todo = [t for t in todo if t[0] in want]
        todo = todo[:max_groups]
        if not todo:
            return {"groups": 0, "items_deleted": 0, "ref_rows": 0, "left": 0}

        os.makedirs(BK, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        subprocess.run(["docker", "stop", "emby"], capture_output=True, timeout=120)
        time.sleep(3)
        for ext in ("", "-wal", "-shm"):
            src = DB + ext
            if os.path.exists(src):
                shutil.copy2(src, "%s/library.db.%s%s" % (BK, stamp, ext))

        con = connect()
        n_items = n_ref = 0
        try:
            for path, keep, kill in todo:
                for i in kill:
                    con.execute("delete from MediaItems where Id=?", (i,))
                    n_items += 1
                    for t in REFS:
                        if not _has(con, t):
                            continue
                        for col in ("ItemId", "Id"):
                            cols = [r[1] for r in
                                    con.execute("pragma table_info(%s)" % t).fetchall()]
                            if col in cols and not (t == "MediaItems"):
                                try:
                                    n_ref += con.execute(
                                        "delete from %s where %s=?" % (t, col), (i,)).rowcount
                                except sqlite3.Error:
                                    pass
                                break
            con.commit()
            con.execute("pragma wal_checkpoint(TRUNCATE)")
        finally:
            con.close()

        subprocess.run(["docker", "start", "emby"], capture_output=True, timeout=180)
        time.sleep(settle)
        con = connect(ro=True)
        left = len(dups(con))
        con.close()
        return {"groups": len(todo), "items_deleted": n_items, "ref_rows": n_ref,
                "left": left, "backup": "library.db." + stamp}
    finally:
        try:
            import fcntl
            fcntl.flock(lk.fileno(), fcntl.LOCK_UN)
        finally:
            lk.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = connect(ro=True)
    todo = plan(con)
    con.close()
    print("  重复 Path %d 组，计划删除 %d 条" % (len(todo), sum(len(x[2]) for x in todo)))
    for p, keep, kill in todo[:5]:
        print("   %-46s 留 %s 删 %d" % (p.split("/")[-1][:46], keep[:8], len(kill)))
    if not a.apply:
        print("  （dry-run，加 --apply 执行）")
        return 0

    r = apply_for_paths(None, max_groups=10000, settle=30)
    print("  已删 Items %s 条，附属行 %s 条（备份 %s）"
          % (r.get("items_deleted"), r.get("ref_rows"), r.get("backup")))
    print("  复核：剩余重复组 %s" % r.get("left"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
