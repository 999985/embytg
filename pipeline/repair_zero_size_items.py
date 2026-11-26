"""修复 Emby 里 Size=0 的「空条目」（Path 对、但从未探出媒体信息）。

成因：迁移/登记时挂载还没刷出文件，TargetedScan 先把条目建出来了 —— Path 正确、
Size=0、无 MediaStreams。事后 `POST /Items/{id}/Refresh` 补不回来（实测）。

做法（不碰字节、不用 SQL 改名、也不让 DELETE 级联）：
  1. 把 teldrive 里这条文件**换个临时名**（同 parts 重新 register，删旧行）
  2. 这时条目 Path 已不存在 → `DELETE /Items/{id}` 不会级联软删真文件
  3. 再把文件名改回来（同样 re-register）
  4. 刷挂载 → scan_paths(新路径) → 条目重建并 probe

用法：
    python3 repair_zero_size_items.py --limit 20            # dry
    python3 repair_zero_size_items.py --limit 20 --apply
"""
import argparse
import json
import os
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


def q(sql):
    """psql -At 输出；**单列查询没有分隔符**，不能按 "|" 过滤（否则永远空 →
    会误判"改名回滚失败"，实测 2026-11-26 三集被留在 _rdy_ 名字上）。"""
    r = subprocess.run(PSQL + [sql], capture_output=True, text=True)
    return [l.split("|") for l in r.stdout.strip().splitlines() if l.strip()]


def zero_items(b, tk, limit):
    out, start = [], 0
    while len(out) < limit:
        qy = up.urlencode({"Recursive": "true", "IncludeItemTypes": "Movie,Episode",
                           "Fields": "Path,MediaSources", "StartIndex": start, "Limit": 500, "api_key": tk})
        d = json.loads(ur.urlopen("%s/Items?%s" % (b, qy), timeout=90).read())
        its = d.get("Items") or []
        if not its:
            break
        for it in its:
            ms = (it.get("MediaSources") or [{}])[0]
            if not ms.get("Size"):
                out.append(it)
                if len(out) >= limit:
                    break
        start += len(its)
        if start >= d.get("TotalRecordCount", 0):
            break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    b = cfg("emby_url").rstrip("/")
    tk = cfg("emby_token")

    items = zero_items(b, tk, a.limit)
    print("  空条目 %d 条" % len(items))
    for it in items[:8]:
        print("    %-30s %s" % ((it.get("Name") or "")[:30], (it.get("Path") or "")[-30:]))
    if not a.apply:
        print("  dry run")
        return

    fixed = 0
    for it in items:
        p = it.get("Path") or ""
        if not p.startswith("/mnt/embytg/"):
            continue
        rel = p[len("/mnt/embytg/"):]
        name = os.path.basename(rel)
        parent_rel = os.path.dirname(rel)
        rows = q("select id,parent_id,name from files where name='%s';" % name.replace("'", "''"))
        if not rows:
            print("    ! %s 在 teldrive 找不到" % name[:34])
            continue
        fid, pid = rows[0][0], rows[0][1]
        parts = [(int(x[0]), int(x[1])) for x in
                 q("select message_id, plain_size from file_parts where file_id='%s' order by part_no;" % fid)]
        if not parts:
            continue
        tmp = "_rdy_" + name
        # 临时改名
        subprocess.run(PSQL + ["delete from file_parts where file_id='%s'; delete from files where id='%s';" % (fid, fid)],
                       capture_output=True)
        TP.register(pid, tmp, parts)
        subprocess.run(["rclone", "rc", "vfs/forget", "dir=/" + parent_rel], capture_output=True)
        subprocess.run(["rclone", "rc", "vfs/refresh", "dir=/" + parent_rel], capture_output=True)
        time.sleep(2)
        # 删空条目（此时旧路径已不存在，不会级联）
        try:
            ur.urlopen(ur.Request("%s/Items/%s?api_key=%s" % (b, it["Id"], tk), method="DELETE"), timeout=30).read()
        except Exception:                                     # noqa: BLE001
            pass
        # 改回原名
        rows = q("select id from files where name='%s';" % tmp.replace("'", "''"))
        if not rows:
            print("    ! 改名回滚失败 %s" % name[:30])
            continue
        fid2 = rows[0][0]
        subprocess.run(PSQL + ["delete from file_parts where file_id='%s'; delete from files where id='%s';" % (fid2, fid2)],
                       capture_output=True)
        TP.register(pid, name, parts)
        subprocess.run(["rclone", "rc", "vfs/refresh", "dir=/" + parent_rel], capture_output=True)
        time.sleep(2)
        os.environ["EMBY_SCAN_DEDUP_SEC"] = "0"
        emby_scan.scan_paths([p])
        fixed += 1
        print("    ✔ %s" % name[:40])
    print("  修好 %d 条" % fixed)


if __name__ == "__main__":
    main()
