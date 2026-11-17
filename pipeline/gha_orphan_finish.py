#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gha_orphan_finish.py —— 「清单已 done、服务端没登记」的收尾孤儿清扫器。

## 为什么需要它

上传完成后，**是客户端 SSH 回控制面**调 `gha_finish.py` 做登记/刮削/Emby 通知
（见 `/opt/embytg-runner/run_one.sh` 第 615 行）。只要客户端在上传完成之后死掉
——云盒 11-15 全下线、PC 关机、SSH 断、systemd 超时——就会出现：

  - `_gha_manifests/<run>.json` 里 `done=True`、所有分片 message_id 都在
  - teldrive `files` 表**没有**这条记录
  - `tg.db.state_key` / `resource_state` 没有这集
  - Emby 搜不到

字节已经花掉（占 TG 配额），用户永远看不到 —— 2026-11-14 的 new5 lane 就留下
4 集、10.3 GiB 这种孤儿（钻石王牌 E01 / 少年包青天 E02 / 偶像大师 E07 /
恋爱药的魔女 E06），直到 11-17 巡检才发现。

## 判据（缺一不动，全部可证明）

1. manifest `done is True`、`parts` 非空、`actual_width/height > 0`
2. 画质标签与实测分辨率一致（复用 gha_finish 的 quality_from_dims）
3. `target_rel/folders/key/name` 齐全且互相自洽（复用 align 的同一套校验口径）
4. manifest mtime 老于 `--min-age-min`（默认 20 分钟，避开正在收尾的单）
5. **没有**活动 claim 指向同一 run_id / key / target_rel
6. **没有**进程 argv 含该 run_id
7. teldrive 没有该 rel、tg.db 没有该 state_key、resource_state 没有该 rel
8. 不是洗版单（`old_rel` 为空）——洗版要动旧文件改名，只走 gha_finish 原路径

## 流程

`pc_register.py`（复用 gha_finish.register_from_manifest）→ `tdprobe` 直读探测
→ 等 rclone 挂载可见 → `post_ingest.py`（NFO/Name/Emby 严格验收）
→ `verify_actual_emby`（分辨率对齐）→ `resource_state.py --scan`（台账）。

**探测失败就回滚自己刚写的那行**（`files.deleted_at = now()`），绝不留
「PG 有行、Emby 看不到」的鬼记录；不做任何删除 TG 消息的动作。

## 用法

```
python3 gha_orphan_finish.py                 # 只报告（默认）
python3 gha_orphan_finish.py --apply --max 2 # 真收尾，单轮最多 2 个
python3 gha_orphan_finish.py --json          # 机器读
```

journal：`.gha_orphan_journal/<run_id>.json`；日志：`logs/orphan_finish.log`
"""
import argparse
import glob
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import gha_finish as F            # noqa: E402  复用已验证的登记/探测/验收逻辑
import autoingest as A            # noqa: E402
import gha_state as GS            # noqa: E402

MANIFEST_DIR = os.path.join(HERE, "_gha_manifests")
CLAIM_DIR = os.path.join(HERE, ".gha_claims")
JOURNAL_DIR = os.path.join(HERE, ".gha_orphan_journal")
LOG_PATH = os.path.join(HERE, "logs", "orphan_finish.log")
DEAD_LEDGER = os.path.join(HERE, "logs", "td_dead_files.jsonl")
PSQL = ["docker", "exec", "-i", "teldrive-postgres", "psql",
        "-U", "teldrive", "-d", "teldrive", "-Atq"]


def log(msg):
    line = "%s %s" % (time.strftime("%F %T"), msg)
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def claim_guard():
    """所有活动 claim 的 (run_id, key, target_rel) 集合。"""
    runs, keys, rels = set(), set(), set()
    for p in glob.glob(os.path.join(CLAIM_DIR, "*.json")):
        if "lost+found" in p:
            continue
        d = read_json(p)
        if not isinstance(d, dict):
            continue
        if d.get("run_id"):
            runs.add(str(d["run_id"]))
        if d.get("key"):
            keys.add(str(d["key"]))
        if d.get("target_rel"):
            rels.add(str(d["target_rel"]))
    return runs, keys, rels


def process_guard():
    """正在跑的 runner/上传进程 argv 拼成的字符串（判 run_id 是否还活着）。"""
    try:
        out = subprocess.run(
            ["ps", "-eo", "args"], capture_output=True, text=True, timeout=20)
        return out.stdout or ""
    except (OSError, subprocess.SubprocessError):
        return ""


def parse_rel(target_rel, key):
    """从 target_rel + state_key 还原 job 的 lib/dir/tmdb/season/ep。"""
    parts = str(target_rel).split("/")
    if len(parts) < 5 or parts[0] != "TV":
        raise RuntimeError("target_rel 不是 TV 四段结构：%s" % target_rel)
    lib, sdir = parts[1], parts[2]
    m = re.search(r"\{tmdb=(\d+)\}", sdir)
    if not m:
        raise RuntimeError("剧目录缺 tmdb 标：%s" % sdir)
    km = re.match(r"tv\|id:(\d+)\|\((\d+),\s*(\d+)\)\|(.+)$", str(key))
    if not km:
        raise RuntimeError("state_key 格式不认识：%s" % key)
    if int(km.group(1)) != int(m.group(1)):
        raise RuntimeError("key 与目录 tmdb 不一致：%s vs %s" % (key, sdir))
    return {
        "lane": "new",
        "lib": lib,
        "dir": sdir,
        "tmdb": int(km.group(1)),
        "season": int(km.group(2)),
        "ep": int(km.group(3)),
        "quality": km.group(4),
    }


_TD_CACHE = {}


def _td_file_keys():
    """一次取全 teldrive (文件名, 父目录名)，含软删。逐个 docker exec 查
    1057 个 manifest 要 90 秒，一次读完只要 1 秒。"""
    if "keys" in _TD_CACHE:
        return _TD_CACHE["keys"]
    sql = ("SET search_path TO teldrive,public; "
           "SELECT f.name, d.name FROM files f JOIN files d ON d.id=f.parent_id "
           "WHERE f.kind='file';")
    r = subprocess.run(PSQL, input=sql.encode(), capture_output=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError("查 teldrive 文件表失败：%s"
                           % (r.stderr or b"").decode()[:200])
    keys = set()
    for line in (r.stdout or b"").decode("utf-8", "replace").splitlines():
        if "|" in line:
            n, d = line.split("|", 1)
            keys.add((n, d))
    _TD_CACHE["keys"] = keys
    return keys


def td_has_rel(rel):
    """teldrive 里是否已有该路径（文件名 + 父目录名，含软删）。"""
    try:
        return (os.path.basename(rel),
                os.path.basename(os.path.dirname(rel))) in _td_file_keys()
    except (OSError, RuntimeError, subprocess.SubprocessError):
        return True      # 查不到就当已存在：宁可不动，也不制造重复


def qt(s):
    return "'" + str(s).replace("'", "''") + "'"


_DB_CACHE = {}


def _db_sets():
    """一次性把 state_key / resource_state 的键读进内存（1057 个 manifest 逐个
    开库要 90 秒；一次读完是毫秒级）。"""
    if _DB_CACHE:
        return _DB_CACHE
    keys, rels = set(), set()
    con = sqlite3.connect("file:%s?mode=ro" % A.TGDB, uri=True, timeout=30)
    try:
        keys = {r[0] for r in con.execute("select k from state_key")}
        rels = {r[0] for r in con.execute("select rel from resource_state")}
    finally:
        con.close()
    _DB_CACHE.update(keys=keys, rels=rels)
    return _DB_CACHE


def db_has_key(key, rel):
    try:
        d = _db_sets()
        return key in d["keys"] or rel in d["rels"]
    except sqlite3.Error:
        return True      # 读不到库就保守跳过


def rollback_file_row(fid):
    """探测失败时撤掉自己刚写的 teldrive 行（软删，字节不动）。"""
    sql = ("SET search_path TO teldrive,public; UPDATE files "
           "SET deleted_at=now() WHERE id=%s AND kind='file';" % qt(fid))
    subprocess.run(PSQL, input=sql.encode(), capture_output=True, timeout=60)


def write_journal(run_id, obj):
    try:
        os.makedirs(JOURNAL_DIR, exist_ok=True)
        with open(os.path.join(JOURNAL_DIR, run_id + ".json"), "w",
                  encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1)
    except OSError:
        pass


def candidates(min_age_min):
    """返回 (可收尾清单, 跳过清单)，每项含 run_id/path/manifest/reason。"""
    runs, keys, rels = claim_guard()
    ps = process_guard()
    now = time.time()
    ok, skipped = [], []
    for p in sorted(glob.glob(os.path.join(MANIFEST_DIR, "*.json"))):
        d = read_json(p)
        if not isinstance(d, dict) or not d.get("done"):
            continue
        run_id = os.path.basename(p)[:-5]
        item = {"run_id": run_id, "path": p, "manifest": d}
        age_min = (now - os.path.getmtime(p)) / 60.0
        item["age_min"] = round(age_min, 1)
        try:
            if age_min < min_age_min:
                raise RuntimeError("太新（%.0f < %d 分钟）" % (age_min, min_age_min))
            if d.get("old_rel"):
                raise RuntimeError("洗版单，留给 gha_finish 原路径")
            if not (d.get("parts") or []):
                raise RuntimeError("manifest 没有 parts")
            w, h = int(d.get("actual_width") or 0), int(d.get("actual_height") or 0)
            if w <= 0 or h <= 0:
                raise RuntimeError("manifest 缺实测分辨率")
            actual = F.quality_from_dims(w, h)
            if str(d.get("quality") or "").lower() != actual:
                raise RuntimeError("画质与实测不符：%s != %s"
                                   % (d.get("quality"), actual))
            name, rel = str(d.get("name") or ""), str(d.get("target_rel") or "")
            folders, key = str(d.get("folders") or ""), str(d.get("key") or "")
            if not (name and rel and folders and key):
                raise RuntimeError("manifest 缺 name/target_rel/folders/key")
            if os.path.basename(rel) != name:
                raise RuntimeError("name 与 target_rel 不一致")
            if os.path.dirname(rel).replace("\\", "/") != folders:
                raise RuntimeError("folders 与 target_rel 不一致")
            item.update(parse_rel(rel, key))
            item.update(name=name, target_rel=rel, folders=folders, key=key,
                        size=sum(int(p["plain_size"]) for p in d["parts"]))
            if run_id in runs:
                raise RuntimeError("有活动 claim 指向该 run_id")
            if key in keys:
                raise RuntimeError("有活动 claim 指向该 state_key")
            if rel in rels:
                raise RuntimeError("有活动 claim 指向该 target_rel")
            if run_id in ps:
                raise RuntimeError("仍有进程在跑该 run_id")
            if td_has_rel(rel):
                raise RuntimeError("teldrive 已有该文件")
            if db_has_key(key, rel):
                raise RuntimeError("tg.db 已登记")
            ok.append(item)
        except Exception as e:                       # noqa: BLE001
            item["reason"] = str(e)
            skipped.append(item)
    return ok, skipped


def recover(item):
    """登记 + 探测 + 刮削 + Emby 验收。返回 dict 结果。"""
    d, rel, key = item["manifest"], item["target_rel"], item["key"]
    name, lib, tmdb = item["name"], item["lib"], item["tmdb"]
    job = {
        "lane": "new",
        "lib": lib,
        "dir": item["dir"],
        "tmdb": tmdb,
        "season": item["season"],
        "ep": item["ep"],
        "quality": item["quality"],
        "key": key,
        "name": name,
        "target_rel": rel,
        "folders": item["folders"],
        "size": sum(int(p["plain_size"]) for p in d["parts"]),
        "recovered_from_manifest": item["run_id"],
    }
    res = {"run_id": item["run_id"], "rel": rel, "name": name,
           "tmdb": tmdb, "season": item["season"], "ep": item["ep"],
           "size": job["size"]}
    with F.finish_lock(600):
        fid = F.register_from_manifest(job, d)
        res["file_id"] = fid
        log("  REGISTERED %s file_id=%s" % (name, fid))
        import tdprobe
        okp, det = tdprobe.probe_file(fid, ext=os.path.splitext(name)[1])
        if not okp:
            rollback_file_row(fid)
            try:
                os.makedirs(os.path.dirname(DEAD_LEDGER), exist_ok=True)
                with open(DEAD_LEDGER, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "ts": time.strftime("%F %T"), "file_id": fid,
                        "name": name, "target": F.target_abs(rel),
                        "tmdb": tmdb, "season": item["season"],
                        "ep": item["ep"], "detail": det,
                        "action": "orphan_finish_rollback"}, ensure_ascii=False) + "\n")
            except OSError:
                pass
            res.update(ok=False, stage="probe", detail=det)
            return res
        res["probe"] = det
        target = F.target_abs(rel)
        F.wait_mount_file(target, 180)
        r = F.run([sys.executable, os.path.join(HERE, "post_ingest.py"),
                   "--tmdb", str(tmdb), "--wait-emby", "20", "--sample", "5",
                   "--verify-file", name, "--strict-emby"], timeout=1800)
        nfo = os.path.splitext(target)[0] + ".nfo"
        if r.returncode != 0:
            res.update(ok=False, stage="post_ingest", rc=r.returncode)
            return res
        if not os.path.exists(nfo):
            res.update(ok=False, stage="nfo", detail=nfo)
            return res
        F.verify_actual_emby(job, int(d["actual_width"]), int(d["actual_height"]),
                             timeout=180)
        F.run([sys.executable, os.path.join(HERE, "resource_state.py"), "--scan"],
              timeout=1800)
        con = sqlite3.connect("file:%s?mode=ro" % A.TGDB, uri=True, timeout=30)
        try:
            k = con.execute("select 1 from state_key where k=?", (key,)).fetchone()
            r2 = con.execute("select stage,scraped_at from resource_state where rel=?",
                             (rel,)).fetchone()
        finally:
            con.close()
        if not k or not r2 or not r2[1]:
            res.update(ok=False, stage="ledger", state_key=bool(k), row=r2)
            return res
        res.update(ok=True, stage="emby", resource_stage=r2[0])
        return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真收尾（默认只报告）")
    ap.add_argument("--max", type=int, default=2, help="单轮最多收尾几个（默认 2）")
    ap.add_argument("--min-age-min", type=int, default=20,
                    help="manifest 至少多旧才动（默认 20 分钟）")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    ok, skipped = candidates(a.min_age_min)
    if a.json:
        print(json.dumps({
            "orphans": [{k: v for k, v in i.items() if k != "manifest"}
                        for i in ok],
            "skipped": [{k: v for k, v in i.items() if k != "manifest"}
                        for i in skipped if "teldrive 已有该文件" not in
                        str(i.get("reason")) and "已登记" not in str(i.get("reason"))],
        }, ensure_ascii=False, indent=1))
        return 0
    log("== 收尾孤儿扫描：可收尾 %d，跳过 %d ==" % (len(ok), len(skipped)))
    for i in ok:
        log("  CAND %s %.1fGiB age=%.0fmin %s"
            % (i["name"], i["size"] / 2 ** 30 if i.get("size") else
               sum(int(p["plain_size"]) for p in i["manifest"]["parts"]) / 2 ** 30,
               i["age_min"], i["target_rel"]))
    if not a.apply:
        return 0
    n = 0
    for i in ok:
        if n >= a.max:
            break
        n += 1
        log("  -> 收尾 %s" % i["name"])
        res = recover(i)
        write_journal(i["run_id"], res)
        if res.get("ok"):
            log("  OK %s stage=%s probe=%s" % (i["name"], res.get("stage"),
                                               str(res.get("probe"))[:80]))
        else:
            log("  FAIL %s stage=%s %s" % (i["name"], res.get("stage"),
                                           str(res)[:300]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
