#!/usr/bin/env python3
"""upgrade_episodes.py —— 从指定频道把某剧的指定集**零上行**换进库。

场景（2026-11-19 用户反馈）：谷雨 E01-E10/E23/E24 从 luolimeimei 进来的是
345-435MB（约 1.0-1.3 Mbps）的假 4K；yingshi9999 的同集是 730-1150MB（约 2-3 Mbps）。
同一集、同一画质标签，码率差一倍 → 直接换成好的那份。

做法：频道里的消息**直接 forward** 到存储频道（不下载不重传），
登记成库里既有的命名（`剧名 SxxExx - 画质.ext`），写 NFO，按文件送扫。
旧的差版本随后单独处理（默认保留，`--replace` 时才删）。

用法：
    python3 upgrade_episodes.py --ch yingshi9999 --tag "#谷雨" --series 谷雨 \
        --dir "TV/国产剧/谷雨 (2026) {tmdb=279155}/Season 01" --eps 24,28 --apply
"""
import argparse
import asyncio
import os
import re
import sys

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgsrc_copy as TC                                    # noqa: E402
import tg_common as Y                                    # noqa: E402

from telethon import TelegramClient                        # noqa: E402
from telethon.sessions import StringSession                # noqa: E402

TAG = "upgrade_episodes.py"


def qual_of(doc):
    for a in (doc.document.attributes or []):
        if hasattr(a, "w"):
            w, h = int(getattr(a, "w", 0) or 0), int(getattr(a, "h", 0) or 0)
            if w >= 3000 or h >= 1800:
                return "2160p", w, h
            if w >= 1700 or h >= 1000:
                return "1080p", w, h
            return "720p", w, h
    return "1080p", 0, 0


def ep_no(text):
    m = re.search(r"第\s*(\d+)\s*集", text or "")
    return int(m.group(1)) if m else 0


def _td(sql):
    import subprocess
    r = subprocess.run(["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive",
                        "-d", "teldrive", "-t", "-A", "-F", "|", "-c", sql],
                       capture_output=True, text=True)
    return [ln.split("|") for ln in r.stdout.strip().splitlines() if ln.strip()]


def folder_id(dir_path):
    rows = _td("select id from files where name='%s' and kind='folder' and deleted_at is null limit 1"
               % dir_path.strip("/").split("/")[-1])
    return rows[0][0] if rows else ""


def old_file_size(dir_path, name):
    rows = _td("select f.size from files f join files p on p.id=f.parent_id "
               "where p.name='%s' and f.name=%s and f.deleted_at is null limit 1"
               % (dir_path.strip("/").split("/")[-1], _q(name)))
    try:
        return int(rows[0][0])
    except Exception:                                          # noqa: BLE001
        return 0


def _q(s):
    return "'" + str(s).replace("'", "''") + "'"


async def drop_old(c, dir_path, name):
    """删旧版本：TG 消息 + teldrive 行（file_parts/files）+ 台账行。"""
    rows = _td("select f.id, pp.message_id from files f "
               "join files p on p.id=f.parent_id join file_parts pp on pp.file_id=f.id "
               "where p.name='%s' and f.name=%s and f.deleted_at is null"
               % (dir_path.strip("/").split("/")[-1], _q(name)))
    if not rows:
        return 0
    mids = [int(r[1]) for r in rows if r[1].isdigit()]
    if mids:
        await c.delete_messages(Y.STORE_CH, mids)
    for fid, _mid in rows:
        _td("delete from file_parts where file_id='%s'; delete from files where id='%s';" % (fid, fid))
    try:
        import sqlite3
        con = sqlite3.connect("/opt/embytg/pipeline/tg.db")
        con.execute("delete from resource_state where rel like ?",
                    ("%%/%s/%s" % (dir_path.strip("/").split("/")[-1], name),))
        con.commit()
        con.close()
    except Exception:                                          # noqa: BLE001
        pass
    return len(rows)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ch", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--series", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--eps", default="")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--replace", action="store_true", help="删掉同集的旧差版本")
    a = ap.parse_args()

    want = {int(x) for x in a.eps.replace(" ", "").split(",") if x.isdigit()}
    env = Y.load_env()
    slot = 0
    for n in (1, 2, 3, 4):
        if GS.slot_lease(n, TAG, ttl=1800):
            slot = n
            break
    if not slot:
        print("  没有空闲 slot")
        return 2
    c = TelegramClient(StringSession(env.get("TG_SESSION_STRING_%d" % slot) or env["TG_SESSION_STRING"]),
                       int(env["TG_API_ID"]), env["TG_API_HASH"])
    await c.connect()
    try:
        ch = await c.get_entity(a.ch)
        cand = {}
        async for m in c.iter_messages(ch, search=a.tag, limit=200):
            if not m.document:
                continue
            n = ep_no(m.raw_text or "")
            if not n or (want and n not in want):
                continue
            q, w, h = qual_of(m)
            dur = 0
            for at in (m.document.attributes or []):
                if hasattr(at, "duration"):
                    dur = int(at.duration or 0)
                    break
            mb = (m.document.size * 8 / dur / 1e6) if dur else 0
            cur = cand.get(n)
            if not cur or m.document.size > cur["size"]:
                cand[n] = {"msg": m.id, "size": m.document.size, "q": q, "mbps": mb, "dur": dur}
        print("  频道内命中 %d 集：%s" % (len(cand), sorted(cand)))
        for n in sorted(cand):
            v = cand[n]
            name = "%s S01E%02d - %s.mp4" % (a.series, n, v["q"])
            print("   E%-3d %6.0fMB %.1fMbps → %s" % (n, v["size"] / 1e6, v["mbps"], name))
            if not a.apply:
                continue
            fwd = await c.forward_messages(Y.STORE_CH, v["msg"], from_peer=ch)
            mid = fwd[0].id if isinstance(fwd, list) else fwd.id
            try:
                fid = TC.register(a.dir, name, [(mid, v["size"])])
                print("     已登记 %s" % fid[:8])
            except Exception as e:                             # noqa: BLE001
                if "duplicate key" not in str(e):
                    raise
                # 同名已存在：比体积，明显更大才替换（旧的差版本删掉）
                old_sz = old_file_size(a.dir, name)
                if not a.replace or old_sz >= v["size"] * 0.95:
                    print("     已存在（旧 %.0fMB / 新 %.0fMB），跳过" % (old_sz / 1e6, v["size"] / 1e6))
                    continue
                print("     替换：旧 %.0fMB → 新 %.0fMB" % (old_sz / 1e6, v["size"] / 1e6))
                await drop_old(c, a.dir, name)
                fid = TC.register(a.dir, name, [(mid, v["size"])])
                print("     已登记 %s" % fid[:8])
            d = os.path.join(Y.MNT, a.dir)
            try:
                # 同 fill_by_ids：NFO 只走 nfo_std，不手拼 XML。
                import re as _re3
                import nfo_std
                _m = _re3.search(r"\{tmdb=(\d+)\}", a.dir)
                _tmdb = int(_m.group(1)) if _m else 0
                _sm = _re3.search(r"S(\d+)E\d+", name)
                _season = int(_sm.group(1)) if _sm else 1
                if _tmdb:
                    nfo_std.write_episode_nfo(os.path.join(d, name), _tmdb, _season, n)
            except Exception:                                  # noqa: BLE001
                pass
            try:
                import emby_scan
                r = emby_scan.scan_paths(["/mnt/embytg/%s/%s" % (a.dir, name)])
                print("     送扫 Created=%s" % r.get("Created"))
            except Exception as e:                             # noqa: BLE001
                print("     送扫失败 %s" % str(e)[:40])
    finally:
        await c.disconnect()
        GS.slot_release(slot, TAG)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
