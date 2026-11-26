"""把「恐怖」为主类型却被归进动作片的电影迁到惊悚片（E215 同类修复）。

规则：只动 detail 里 genres[0] == 27（恐怖）且当前 lib 不是惊悚片的条目。
做法：同一批 parts 在新目录重新 register → 删旧行 → 刷新挂载 → 删旧 Emby 条目 → 扫新路径。
**不用 SQL 改名**：那会打断挂载映射（2026-11-26 事故）。

用法：
    python3 fix_movie_category.py --json /tmp/cat_audit.json            # dry
    python3 fix_movie_category.py --json /tmp/cat_audit.json --apply
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse as up
import urllib.request as ur

sys.path.insert(0, "/opt/embytg/pipeline")
import tgpipe as TP                                       # noqa: E402
import emby_scan                                          # noqa: E402
from embytg_conf import cfg                               # noqa: E402

PSQL = ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive", "-t", "-A", "-F", "|", "-c"]


def rows_like(name):
    r = subprocess.run(PSQL + ["select id, parent_id, name, size, status from files where name='%s';" % name.replace("'", "''")],
                       capture_output=True, text=True)
    out = []
    for line in r.stdout.strip().splitlines():
        p = line.split("|")
        if len(p) >= 5:
            out.append(p)
    return out


def parts_of(fid):
    r = subprocess.run(PSQL + ["select message_id, plain_size from file_parts where file_id='%s' order by part_no;" % fid],
                       capture_output=True, text=True)
    return [(int(a), int(b)) for a, b in (l.split("|") for l in r.stdout.strip().splitlines() if "|" in l)]


def delete_row(fid):
    subprocess.run(PSQL + ["delete from file_parts where file_id='%s'; delete from files where id='%s';" % (fid, fid)],
                   capture_output=True)


def emby_items(b, tk, term):
    q = up.urlencode({"SearchTerm": term, "Recursive": "true", "IncludeItemTypes": "Movie",
                      "Fields": "Path", "api_key": tk})
    return json.loads(ur.urlopen("%s/Items?%s" % (b, q), timeout=40).read()).get("Items") or []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    detail = json.load(open(a.json, encoding="utf-8"))
    todo = [d for d in detail if d.get("cur") != d.get("want")]
    print("  待迁 %d 部（新类型优先级下分类应变化）" % len(todo))
    for d in todo[:8]:
        print("    %-42s %s → %s" % (d["rel"].split("/")[-1][:42], d["cur"], d["want"]))

    b = cfg("emby_url").rstrip("/")
    tk = cfg("emby_token")
    moved = 0
    for d in todo:
        old_rel, new_rel = d["rel"], d["rel"].replace("Movies/%s/" % d["cur"], "Movies/%s/" % d["want"], 1)
        base = os.path.basename(old_rel)
        stem = os.path.splitext(base)[0]
        if not a.apply:
            continue
        # 同一部片的三个文件（mp4/nfo/poster）一起挪
        sibs = [base] + [base.rsplit(".", 1)[0] + ext for ext in (".nfo", "-poster.jpg")] 
        root = TP.ensure_folder("Movies")
        dst = TP.ensure_folder(d["want"], root)
        ok = 0
        for nm in sibs:
            rs = rows_like(nm)
            if not rs:
                continue
            fid, _pid, _n, size, _st = rs[0]
            parts = parts_of(fid)
            if not parts:
                print("    ! %s 无分片，跳过" % nm[:40])
                continue
            dup = [x for x in rows_like(nm) if x[1] == dst]
            if dup:
                print("    = %s 目标目录已有同名，跳过" % nm[:38])
                continue
            # file_parts 对 (channel_id, message_id) 有唯一约束：同一个 TG 消息
            # 只能属于一个 file 行。所以必须**先删旧行再登记新行**，反过来必撞
            # duplicate key（2026-11-26 第一次迁移就是这样秒退的）。
            old_pid = _pid
            delete_row(fid)
            try:
                TP.register(dst, nm, parts)
            except Exception as e:                           # noqa: BLE001
                print("    ! 登记失败 %s: %s（回滚旧行）" % (nm[:30], str(e)[:60]))
                TP.register(old_pid, nm, parts)
                continue
            ok += 1
        if not ok:
            print("    ! %s 未迁移" % base[:40])
            continue
        moved += 1
        con = sqlite3.connect("/opt/embytg/pipeline/tg.db", timeout=30)
        con.execute("update resource_state set rel=?, path=?, lib=? where rel=?",
                    (new_rel, "/opt/embytg/mnt/embytg/" + new_rel,
                     "Movies/" + d["want"], old_rel))
        con.commit(); con.close()
        print("    ✔ %s → %s（%d 个文件）" % (base[:36], d["want"], ok))

    if not a.apply:
        print("  dry run，未改动；加 --apply 执行")
        return

    # 挂载必须先看得见文件，再送扫。否则 Emby 会建出「Path 对、Size=0」的空条目
    # （2026-11-26 实测 53 条），事后 refresh 也补不回来，只能删条目重建。
    subprocess.run(["rclone", "rc", "vfs/forget", "dir=/Movies/惊悚片"], capture_output=True)
    subprocess.run(["rclone", "rc", "vfs/refresh", "dir=/Movies/惊悚片"], capture_output=True)
    subprocess.run(["rclone", "rc", "vfs/refresh", "dir=/Movies"], capture_output=True)
    ready = 0
    for _ in range(40):
        ready = 0
        for d in todo:
            new_rel = d["rel"].replace("Movies/%s/" % d["cur"], "Movies/%s/" % d["want"], 1)
            if os.path.exists("/opt/embytg/mnt/embytg/" + new_rel):
                ready += 1
        if ready >= len(todo):
            break
        time.sleep(15)
    print("  挂载可见 %d/%d" % (ready, len(todo)))
    os.environ["EMBY_SCAN_DEDUP_SEC"] = "0"
    for d in todo:
        new_rel = d["rel"].replace("Movies/%s/" % d["cur"], "Movies/%s/" % d["want"], 1)
        stem = os.path.splitext(os.path.basename(new_rel))[0]
        term = stem.split(" (")[0]
        for it in emby_items(b, tk, term):
            if (it.get("Path") or "").endswith(os.path.basename(d["rel"])):
                try:
                    req = ur.Request("%s/Items/%s?api_key=%s" % (b, it["Id"], tk), method="DELETE")
                    ur.urlopen(req, timeout=30).read()
                except Exception:                            # noqa: BLE001
                    pass
        emby_scan.scan_paths(["/mnt/embytg/" + new_rel])
    print("  迁移完成：%d 部" % moved)


if __name__ == "__main__":
    main()
