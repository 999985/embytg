#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gha_register.py —— 把 GHA 搬上来的文件登记进库（teldrive → NFO → Emby）。

分工原则：**GHA 只搬字节，登记只在控制面做**（teldrive 只听 127.0.0.1）。

流程：
    gha_queue collect     结果落进 tg.db.gha_transport（含 file_id / msg_id）
    gha_register --apply  逐条：写 teldrive（复用 pc_register.py）→ 写 tg.db 台账
                          → 生成 NFO（复用 nfo_std.py）→ 触发 Emby 刷新

派单里必须带元数据，否则登记的目录/季集/TMDB 只能猜：
    assign.json item: {run_id, url, filename, folders, tmdb, lib, season, ep,
                       quality, size, caption}

用法：
    python3 gha_register.py --dry      看计划
    python3 gha_register.py --apply    真登记
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "tg.db")
PY = sys.executable


def _db():
    con = sqlite3.connect(DB, timeout=30)
    con.execute("create table if not exists gha_transport("
                "run_id text primary key, core text, ok integer, reason text, "
                "file_id text, size integer, secs real, ts integer)")
    for col, typ in (("meta", "text"), ("registered", "integer default 0"),
                     ("msg_id", "text")):
        try:
            con.execute("alter table gha_transport add column %s %s" % (col, typ))
        except sqlite3.OperationalError:
            pass
    return con


def pending(con):
    return con.execute("select run_id, file_id, size, meta, msg_id from gha_transport "
                       "where ok=1 and coalesce(registered,0)=0").fetchall()


def register_one(row, apply=False):
    run_id, file_id, size, meta_raw, msg_id = row
    try:
        meta = json.loads(meta_raw or "{}")
    except Exception:                                       # noqa: BLE001
        meta = {}
    if not meta.get("folders"):
        return False, "派单缺 folders 元数据，无法确定入库路径"
    # 分片登记：GHA 现在按段上传（每段 ≤1.8GB），parts 由搬运侧回写。
    parts = meta.get("parts") or []
    if not parts:
        parts = [{"part_no": 1, "message_id": int(msg_id or 0),
                  "plain_size": int(size or 0), "stored_size": int(size or 0),
                  "channel_id": int(meta.get("channel_id") or 0)}]
    manifest = {
        "name": meta.get("filename") or (run_id + ".mp4"),
        "mime_type": meta.get("mime") or "video/mp4",
        "parts": parts,
    }
    cmd = [PY, os.path.join(HERE, "pc_register.py"),
           "--folders", meta["folders"], "--tgdb", "--ch", "gha"]
    if meta.get("tmdb"):
        cmd += ["--tmdb", str(int(meta["tmdb"]))]
    if meta.get("lib"):
        cmd += ["--lib", meta["lib"]]
    if apply and file_id:
        cmd += ["--file-id", file_id]
    if not apply:
        cmd += ["--dry"]
    r = subprocess.run(cmd, input=json.dumps(manifest), capture_output=True, text=True,
                       timeout=300)
    if r.returncode != 0:
        return False, (r.stderr or r.stdout)[-160:]
    return True, (r.stdout or "")[-160:]


def nfo_and_scan(meta, apply=False):
    """NFO 用 nfo_std.write_episode_nfo（项目里唯一的写法），Emby 用 post_ingest。"""
    if not apply:
        return True, "dry"
    mnt = meta.get("mnt") or "/opt/embytg/mnt/embytg"
    folder = os.path.join(mnt, meta["folders"].lstrip("/"))
    video = None
    try:
        for name in sorted(os.listdir(folder)):
            if name.lower().endswith((".mkv", ".mp4", ".ts")):
                video = os.path.join(folder, name)
                break
    except Exception as e:                                  # noqa: BLE001
        return False, "目录不可读: %s（teldrive 可能还没同步）" % str(e)[:60]
    if not video:
        return False, "目录里还没有视频文件（teldrive 未同步完）"
    try:
        import nfo_std
        nfo_std.write_episode_nfo(video, int(meta.get("tmdb") or 0),
                                  int(meta.get("season") or 0), int(meta.get("ep") or 0))
        return True, "NFO 已写"
    except Exception as e:                                  # noqa: BLE001
        return False, "NFO 失败: %s" % str(e)[:80]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=5)
    a = ap.parse_args()
    con = _db()
    rows = pending(con)[:a.limit]
    if not rows:
        print("没有待登记的成功搬运")
        return 0
    done = 0
    for row in rows:
        ok, msg = register_one(row, a.apply)
        print("  %-24s %s %s" % (row[0][:24], "✔" if ok else "✘", msg[:90]))
        if ok:
            try:
                meta = json.loads(row[3] or "{}")
            except Exception:                               # noqa: BLE001
                meta = {}
            nfo_and_scan(meta, a.apply)
            if a.apply:
                con.execute("update gha_transport set registered=1 where run_id=?",
                            (row[0],))
            done += 1
    con.commit()
    con.close()
    print("  %s %d 条" % ("已登记" if a.apply else "可登记", done))
    return 0


if __name__ == "__main__":
    sys.exit(main())
