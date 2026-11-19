#!/usr/bin/env python3
"""drop_old.py —— 洗版后彻底删除旧的低画质文件（用户 2026-11-19 决定）。

用户口径：「不用移出到某个地方，直接删了，保持整洁度和完美度」。
所以旧的 `.superseded/` 方案作废，改为真删：
    ① 删 TG 存储频道里的消息（字节随之释放）
    ② 删 teldrive 的 files / file_parts 行
    ③ 删 resource_state 台账行 + 对应 state_key
    ④ 删 Emby 条目（仅在条目仍指向该文件时）
    ⑤ 清 VFS 缓存，让目录立刻刷新

安全闸：只允许删「同季同集存在更大文件」的那些 —— 绝不删掉该集唯一的一份。
用法：
    python3 drop_old.py --path "TV/欧美剧/幻视任务 (2026) {tmdb=213375}/Season 01/…1080p.mp4"
    python3 drop_old.py --path ... --force      # 跳过体积闸（仅用于已确认的重复条目）
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
MNT = "/opt/embytg/mnt/embytg"
DB = os.path.join(HERE, "tg.db")


def _rows(rel):
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=20)
    try:
        return con.execute("select round(size/1e6) from resource_state where rel=?", (rel,)).fetchone()
    finally:
        con.close()


def _bigger_exists(rel):
    """同季同集是否还有更大的文件（有才允许删）。"""
    m = re.search(r"^(.*)/Season \d+/(.*S\d+E\d+)", rel)
    if not m:
        return False, 0
    season_dir, stem = m.group(1), m.group(2)
    me = _rows(rel)
    my_mb = me[0] if me else 0
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=20)
    try:
        row = con.execute(
            "select max(size)/1e6 from resource_state where rel like ? and rel != ?",
            (season_dir + "/%/" + stem + "%", rel)).fetchone()
    finally:
        con.close()
    return bool(row and row[0] and row[0] > my_mb), my_mb


def _tg_delete(rel):
    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=20)
    try:
        base = os.path.basename(rel)
        rows = con.execute("select id from files where name=? and deleted_at is null", (base,)).fetchall() \
            if _pg_ok() else []
    finally:
        con.close()
    return rows


def _pg_ok():
    return True


def _psql(sql):
    return subprocess.run(["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive",
                           "-d", "teldrive", "-t", "-A", "-c", sql], capture_output=True, text=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    rel = a.path.strip("/")
    name = os.path.basename(rel)

    bigger, my_mb = _bigger_exists(rel)
    if not bigger and not a.force:
        print("  拒绝：该集没有更大的版本，删了会空集（%s）" % rel)
        return 2

    # ① TG 消息
    r = _psql("select pp.message_id from files f join file_parts pp on pp.file_id=f.id "
              "where f.name=%s and f.deleted_at is null" % ("'" + name.replace("'", "''") + "'"))
    msgs = [int(x) for x in (r.stdout or "").strip().splitlines() if x.strip().isdigit()]
    if msgs:
        try:
            import asyncio
            import gha_state as GS
            import ysdq_import as Y
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            env = Y.load_env()

            async def _del():
                slot = 0
                for n in (1, 2, 3, 4):
                    if GS.slot_lease(n, "drop-old", ttl=600):
                        slot = n
                        break
                c = TelegramClient(StringSession(env.get("TG_SESSION_STRING_%d" % slot)
                                                 or env["TG_SESSION_STRING"]),
                                   int(env["TG_API_ID"]), env["TG_API_HASH"])
                await c.connect()
                await c.delete_messages(Y.STORE_CH, msgs)
                await c.disconnect()
                GS.slot_release(slot, "drop-old")
            asyncio.run(_del())
            print("  TG 消息已删: %s" % msgs)
        except Exception as e:                               # noqa: BLE001
            print("  TG 删除失败（继续清台账）: %s" % str(e)[:60])

    # ② teldrive 行
    _psql("delete from file_parts where file_id in (select id from files where name=%s);"
          "delete from files where name=%s;" % ("'" + name.replace("'", "''") + "'",
                                                "'" + name.replace("'", "''") + "'"))
    print("  teldrive 行已删")

    # ③ 台账 + state_key
    con = sqlite3.connect(DB, timeout=30)
    try:
        n1 = con.execute("delete from resource_state where rel=?", (rel,)).rowcount
        n2 = con.execute("delete from state_key where k like ?", ("%" + name + "%",)).rowcount
        con.commit()
    finally:
        con.close()
    print("  台账 %d 行 / state_key %d 行" % (n1, n2))

    # ④ Emby 条目
    # Emby 条目：按路径找到后逐条删（teldrive 行已先删，DELETE 的级联软删无副作用）
    try:
        import json as _json
        import urllib.parse as _up
        import urllib.request as _ur
        sys.path.insert(0, HERE)
        from embytg_conf import cfg as _cfg
        _b = _cfg("emby_url").rstrip("/")
        _t = _cfg("emby_token")
        _want = os.path.basename(rel)
        _q = _up.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                            "Fields": "Path", "Limit": "20000"})
        with _ur.urlopen("%s/Items?%s&api_key=%s" % (_b, _q, _t), timeout=180) as _f:
            _items = (json.loads(_f.read()) or {}).get("Items") or []
        _hit = [i["Id"] for i in _items if (i.get("Path") or "").endswith("/" + _want)]
        for _i in _hit:
            _req = _ur.Request("%s/Items/%s?api_key=%s" % (_b, _i, _t), method="DELETE")
            try:
                _ur.urlopen(_req, timeout=60).read()
            except Exception:                                # noqa: BLE001
                pass
        print("  Emby 条目删除: %d 条" % len(_hit))
    except Exception as _e:                                  # noqa: BLE001
        print("  Emby 清理跳过: %s" % str(_e)[:50])

    # ⑤ VFS
    try:
        import json as _j
        import urllib.request as _ur
        _req = _ur.Request("http://127.0.0.1:5572/vfs/forget", method="POST",
                           data=_j.dumps({"dir": os.path.join(MNT, os.path.dirname(rel))}).encode(),
                           headers={"Content-Type": "application/json"})
        _ur.urlopen(_req, timeout=5).read()
    except Exception:                                        # noqa: BLE001
        pass
    print("  已删除 %s（原 %dMB）" % (name, my_mb))
    return 0


if __name__ == "__main__":
    sys.exit(main())
