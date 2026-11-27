#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pc_register.py — 服务器侧：把 PC 上传到 TG 存储频道的分片登记成 teldrive 文件条目。

stdin: manifest JSON
   { "name": "Arcane S02E01 - ... - 2160p.mkv",
     "mime_type": "video/x-matroska",
     "parts": [ {"part_no":1,"message_id":6029,"plain_size":123,"stored_size":123}, ... ],
     "channel_id": 4413117262,
     "parts": [ {"part_no":1, "channel_id":4413117262, ...} ] }
args:
   --folders "TV/动漫/Arcane (2021) {tmdb=94605}/Season 02"   (从根到所在目录，建缺失目录)
   [--dry]  只打印将执行的登记, 不写库
   [--tgdb] 登记成功后把条目写进 tg.db 的 state_key 去重台账（幂等，走 tgstore 行级 upsert）
   [--file-id <uuid>]  补登记模式：PG 里已有该文件时给已有 file_id，跳过 PG 写入，只补 tg.db
   [--ch NAME]         val.ch 逻辑频道名，默认 pc_upload
   [--tmdb N]          TMDB ID，默认从 --folders 的 {tmdb=N} 解析
   [--lib NAME]        库名，默认取 --folders 第二段（如 欧美剧）

为什么要 --tgdb（2026-11-10 补齐）:
   项目「入库」不止写 teldrive PG，还要写 tg.db：
     - state_key      ← tgpipe.dedupe() 判「本管线已有」的唯一锚点，不写会重复上片
     - resource_state ← ingest_quota.usage() 的唯一数据源，不写会漏记流量
   本脚本负责前者；后者由 resource_state.py --scan 全量重建负责。

规则(与生产 register.py 一致):
 - files.size = sum(part.plain_size)；encryption=false；status=active
 - file_parts 每片 part_no/channel_id/message_id/plain_size/stored_size
 - 复用已验证 schema；只增不改，安全。
"""
import argparse, json, re, subprocess, sys, time, uuid

# 2026-11-15（E106）：不再写死，走频道池
from embytg_conf import store_channel as _store_channel
CHANNEL_ID = _store_channel()
USER_ID = 7582337469
PSQL = ["docker", "exec", "-i", "teldrive-postgres", "psql",
        "-U", "teldrive", "-d", "teldrive", "-Atq", "-v", "ON_ERROR_STOP=1"]


def run_sql(sql):
    p = subprocess.run(PSQL, input=sql.encode("utf-8"),
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode("utf-8", "replace")[:800])
    return p.stdout.decode("utf-8", "replace").strip()


def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def ensure_folder_chain(segments):
    """从根开始逐级建目录，返回最深一级的 folder id。"""
    parent = None
    fid = None
    for seg in segments:
        cond = ("parent_id=%s" % q(parent)) if parent else "parent_id IS NULL"
        out = run_sql("SET search_path TO teldrive,public; "
                      "SELECT id FROM files WHERE kind='folder' AND name=%s AND %s LIMIT 1;" % (q(seg), cond))
        if out:
            fid = out.strip()
        else:
            fid = str(uuid.uuid4())
            run_sql("SET search_path TO teldrive,public; "
                    "INSERT INTO files(id,user_id,parent_id,name,kind,size,mod_time,generation) "
                    "VALUES(%s,%d,%s,%s,'folder',0,now(),1);"
                    % (q(fid), USER_ID, q(parent) if parent else "NULL", q(seg)))
        parent = fid
    return fid


def parse_tgdb_fields(manifest, folders, a):
    """从 --folders + manifest 反推 state_key 字段，锚点与 tgpipe.dedupe() 完全一致。"""
    segments = [s for s in folders.split("/") if s]
    name = manifest["name"]
    m = re.search(r"\{tmdb=(\d+)\}", folders)
    tmdb = a.tmdb or (int(m.group(1)) if m else 0)
    if not tmdb:
        raise ValueError("--folders 里没有 {tmdb=N}，请显式给 --tmdb")
    sm = re.search(r"Season\s*(\d+)", folders, re.I)
    em = re.search(r"S(\d{1,2})E(\d{1,4})", name, re.I)
    # 2026-11-27：Season 00（番外/特辑）是合法季号，旧写法 `if not season` 把它
    # 当成「没解析到」→ 整条登记失败，文件留在 teldrive 但台账空 → claim 卡死。
    # 判「缺失」只认 None，不认 0。
    season = int(sm.group(1)) if sm else (int(em.group(1)) if em else None)
    episode = int(em.group(2)) if em else None
    if season is None or episode is None:
        raise ValueError("无法解析季/集（--folders 需含 Season XX，name 需含 SxxExx）")
    qm = re.search(r"(2160p|1080p|720p|4K)", name, re.I)
    qual = qm.group(1).lower().replace("4k", "2160p") if qm else "sd"
    lib = a.lib or (segments[1] if len(segments) >= 2 else "国产剧")
    parts = sorted(manifest["parts"], key=lambda x: int(x["part_no"]))
    default_ch = int(manifest.get("channel_id") or CHANNEL_ID)
    return {"tmdb": tmdb, "season": season, "episode": episode, "qual": qual,
            "lib": lib, "name": name, "msg": int(parts[0]["message_id"]),
            "parts": [int(p["message_id"]) for p in parts],
            "part_channels": [int(p.get("channel_id") or default_ch) for p in parts]}


def write_tgdb(manifest, folders, file_id, a):
    """写 tg.db.state_key（去重台账）。key/val 与 tgpipe.apply() 逐字段一致。

    走 tgstore.load_state() + save_state(StateStore)：全量 diff 兜底标脏后只 upsert
    变化行，不会重写 2.4 万行、不破坏其它调用方的脏标记。
    """
    import tgstore
    f = parse_tgdb_fields(manifest, folders, a)
    key = "tv|id:%d|(%d, %d)|%s" % (f["tmdb"], f["season"], f["episode"], f["qual"])
    val = {"ch": a.ch, "msg": f["msg"], "name": f["name"], "file_id": file_id,
           "parts": f["parts"], "part_channels": f["part_channels"],
           "src": "tmdb", "mid": f["tmdb"],
           "lib": f["lib"], "ts": int(time.time())}
    st = tgstore.load_state()          # 初始化进程内单例（读全量 2.4 万行）
    old = st.keys.get(key)
    st.keys[key] = val                 # save_state 的 StateStore 分支会 diff 兜底标脏
    tgstore.save_state(st)
    print(json.dumps({"tgdb": "ok", "key": key,
                      "action": "update" if old else "insert",
                      "file_id": file_id, "parts": len(f["parts"]),
                      "lib": f["lib"]}, ensure_ascii=False))


def register(manifest, segments, dry=False):
    name = manifest["name"]
    mime = q(manifest.get("mime_type", "application/octet-stream"))
    parts = sorted(manifest["parts"], key=lambda x: int(x["part_no"]))
    size = sum(int(p["plain_size"]) for p in parts)
    default_ch = int(manifest.get("channel_id") or CHANNEL_ID)
    rows = ",".join("(%d,%d,%d,%d,%d)" % (
        int(p["part_no"]), int(p.get("channel_id") or default_ch), int(p["message_id"]),
        int(p["plain_size"]), int(p.get("stored_size", p["plain_size"]))) for p in parts)

    if dry:
        print("[dry] 目录链: " + " / ".join(segments))
        print("[dry] 文件: %s  size=%d  parts=%d" % (name, size, len(parts)))
        return None

    parent = ensure_folder_chain(segments)
    sql = ("SET search_path TO teldrive,public;\nBEGIN;\n"
           "WITH f AS (INSERT INTO files "
           "(user_id,parent_id,name,kind,mime_type,size,encryption,status,mod_time,generation) "
           "VALUES (%d,%s,%s,'file',%s,%d,false,'active',now(),1) RETURNING id)\n"
           "INSERT INTO file_parts (file_id,part_no,channel_id,message_id,plain_size,stored_size) "
           "SELECT f.id, v.n, v.ch, v.mid, v.ps, v.ss FROM f CROSS JOIN (VALUES %s) "
           "AS v(n,ch,mid,ps,ss) RETURNING file_id;\nCOMMIT;\n"
           % (USER_ID, q(parent), q(name), mime, size, rows))
    out = run_sql(sql)
    fids = re.findall(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", out)
    fid = fids[0] if fids else None
    path = "/".join(segments + [name])
    print(json.dumps({"ok": True, "file_id": fid, "path": path, "size": size,
                      "parts": len(parts)}, ensure_ascii=False))
    return fid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folders", required=True)
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--tgdb", action="store_true",
                    help="登记成功后写 tg.db state_key 去重台账（幂等）")
    ap.add_argument("--file-id",
                    help="补登记模式：给 PG 里已有的 file_id，跳过 PG 写入，只补 tg.db")
    ap.add_argument("--ch", default="pc_upload", help="val.ch 逻辑频道名（默认 pc_upload）")
    ap.add_argument("--tmdb", type=int, default=0, help="TMDB ID（默认从 --folders 解析）")
    ap.add_argument("--lib", help="库名（默认取 --folders 第二段）")
    a = ap.parse_args()
    manifest = json.load(sys.stdin)
    segments = [s for s in a.folders.split("/") if s]

    if a.file_id:
        # 补登记：文件已在 teldrive PG，只把漏掉的 state_key 补上（不重复插 files/file_parts）
        if a.dry:
            print("[dry] 补登记模式：跳过 PG，仅写 tg.db state_key，file_id=%s" % a.file_id)
            return
        write_tgdb(manifest, a.folders, a.file_id, a)
        return

    fid = register(manifest, segments, a.dry)
    if fid and a.tgdb:
        write_tgdb(manifest, a.folders, fid, a)


if __name__ == "__main__":
    main()
