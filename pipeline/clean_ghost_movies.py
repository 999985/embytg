#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clean_ghost_movies.py —— 清掉 Emby 里「文件已不存在」的电影条目（幽灵电影）。

## 为什么需要（2026-11-24 日终实测）

`drop_old.py` 洗版删旧低画质时，④ 只查 `IncludeItemTypes=Episode`，**电影洗版后
Emby 的 Movie 条目不会被删**，累积成 227 条幽灵（点开播放失败）。而五账
（reconcile.py）只比 Episode，所以这个洞在全部体检里都是绿的。

`drop_old.py` 的 ④ 已在同日修好（Episode,Movie + 精确 Path），这个脚本负责
**清理存量**，并把「幽灵数」变成一个可每日复核的数字。

## 判据（三重确认才删，删的是条目不是文件）

1. 该 Path 在 teldrive `files` 里没有 active 行
2. 该 Path 在挂载上 `os.path.exists()` 为 False
3. 只有 1+2 同时成立才 `DELETE /Items/{id}`；任何一条不满足 → 只列出不动

## 用法

```
python3 clean_ghost_movies.py --plan     # 只报告（默认）
python3 clean_ghost_movies.py --apply    # 执行删除，逐条复核
python3 clean_ghost_movies.py --plan --json
```
退出码：0 = 无幽灵或全部清干净；1 = 仍有幽灵（--plan 时正常，--apply 后为异常）。
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import embytg_conf as C                                          # noqa: E402

E = (C.cfg("emby_url", "http://127.0.0.1:8096") or "").rstrip("/")
T = C.cfg("emby_api_key") or C.cfg("emby_token") or ""
HOST = "/opt/embytg/mnt/embytg"
CONT = "/mnt/embytg"
MEDIA_RE = r"\.(mp4|mkv|mov|avi|ts|m2ts|iso|wmv|flv|rmvb|webm)$"

Q_TD = r"""
with recursive root as (
  select id, name::text as name, parent_id from teldrive.files
   where status='active' and name='Movies' and parent_id is null limit 1
),
tree as (
  select f.id, f.name::text as name, '/mnt/embytg/Movies/'||f.name::text as path
    from teldrive.files f join root r on f.parent_id=r.id where f.status='active'
  union all
  select f.id, f.name::text, t.path||'/'||f.name::text
    from teldrive.files f join tree t on f.parent_id=t.id where f.status='active'
)
select path from tree where lower(name) ~ '<RE>';
""".replace("<RE>", MEDIA_RE)


def emby_movies():
    out, start = [], 0
    while True:
        u = ("%s/Items?Recursive=true&IncludeItemTypes=Movie&Fields=Path&Limit=500&StartIndex=%d&api_key=%s"
             % (E, start, T))
        with urllib.request.urlopen(u, timeout=180) as r:
            items = (json.load(r) or {}).get("Items") or []
        out += [(i["Id"], i.get("Path") or "") for i in items]
        start += len(items)
        if len(items) < 500:
            return out


def teldrive_movies():
    p = subprocess.run(["docker", "exec", "-i", "teldrive-postgres", "psql",
                        "-U", "teldrive", "-d", "teldrive", "-At"],
                       input=Q_TD, capture_output=True, text=True, timeout=300)
    return set(x.strip() for x in p.stdout.splitlines() if x.strip())


def host_of(path):
    return HOST + path[len(CONT):] if path.startswith(CONT) else path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--json", dest="as_json", action="store_true")
    a = ap.parse_args()

    if not os.path.ismount(HOST):
        print("mount 不可用，放弃（不猜）", file=sys.stderr)
        return 1

    em = emby_movies()
    td = teldrive_movies()
    cands = [(i, p) for i, p in em if p and p not in td]
    ghosts, unsafe = [], []
    for i, p in cands:
        if os.path.exists(host_of(p)):
            unsafe.append((i, p))        # teldrive 没行但盘上还在 → 不碰（可能是在途/改名中间态）
        else:
            ghosts.append((i, p))

    if a.as_json:
        print(json.dumps({"emby": len(em), "teldrive": len(td),
                          "ghosts": [p for _, p in ghosts],
                          "unsafe": [p for _, p in unsafe]}, ensure_ascii=False))
    else:
        print("[%s] Emby=%d teldrive=%d 待清幽灵=%d 存疑(盘上还在)=%d"
              % (time.strftime("%F %T"), len(em), len(td), len(ghosts), len(unsafe)))
        for _, p in ghosts[:10]:
            print("  GHOST", p)
        for _, p in unsafe[:5]:
            print("  SKIP ", p)

    if not a.apply or not ghosts:
        return 1 if ghosts else 0

    ok = bad = 0
    for i, p in ghosts:
        req = urllib.request.Request("%s/Items/%s?api_key=%s" % (E, i, T), method="DELETE")
        try:
            urllib.request.urlopen(req, timeout=60).read()
            ok += 1
        except Exception as e:                                   # noqa: BLE001
            bad += 1
            print("  ! 删除失败 %s: %s" % (p[:60], type(e).__name__), file=sys.stderr)
    print("删除提交：OK=%d 失败=%d" % (ok, bad))

    # 复核：重新枚举，仍未消失的算残留
    left = [(i, p) for i, p in emby_movies() if p and p not in teldrive_movies()]
    left = [(i, p) for i, p in left if not os.path.exists(host_of(p))]
    print("复核：残留幽灵 %d 条" % len(left))
    return 0 if not left else 1


if __name__ == "__main__":
    sys.exit(main())
