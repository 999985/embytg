# -*- coding: utf-8 -*-
"""按库对账：teldrive 剧集文件数 / 去画质唯一集数  vs  Emby Episode 数。

替代 auto_finalize.py 里硬编码的 EP_TARGET=13826（E005）。规模无关：
  - 库名列表从 library.db 动态发现，不写死；
  - teldrive CTE 只从「8 个 TV 库根 folder」出发，不拼全盘树、不碰 Movies；
  - Emby 侧用 SQL GROUP BY 聚合，绝不把全库拉进内存。

判据（为多版本合并留余地，SCALING.md §4）：
  每库 emby_episodes >= teldrive_unique（逻辑集下界，必须覆盖）
                    且 <= teldrive_files（不合并时等于文件数）
  只有「有文件的库」参与；空库（动漫/综艺/纪录剧集/其他剧，0 文件）跳过。
  converged = 所有有文件的库都满足。以后库数/文件数涨 100 倍，公式不变。

用法（服务器端 cd /opt/embytg/pipeline）：
  python3 lib_reconcile.py            # 打印对账表 + JSON
  python3 lib_reconcile.py --json    # 只输出 JSON（供 auto_finalize import）
也可 import：from lib_reconcile import reconcile
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time as _time

DB = "/var/lib/docker/volumes/embytg_embydata/_data/data/library.db"
TMP_PREFIX = "/tmp/reconcile_"
EP_RE = r"S[0-9]{1,2}E[0-9]{1,3}"

# E016（2026-11-10 13:40 自醒实测）：临时库原来固定叫 /tmp/reconcile_libs.db，
# 两个进程同时跑就互相 os.remove → 4 并发实测 3 个 FileNotFoundError → 对账=None。
# 而下游把 gaps=None 当「对账齐」（fail-open），会在对账未知时放行 L3。
# 改成每进程唯一名字，并统一清理（含 sqlite 的 -wal/-shm）。
_TMP_TAG = "%d_%d" % (os.getpid(), int(_time.time() * 1000))


def tmp_path(name):
    return TMP_PREFIX + _TMP_TAG + "_" + name


def _cleanup(tmp):
    for suf in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(tmp + suf)
        except OSError:
            pass


def sh(args, inp=None, timeout=200):
    r = subprocess.run(args, input=inp, capture_output=True, timeout=timeout)
    out = (r.stdout or b"").decode("utf-8", "ignore")
    err = (r.stderr or b"").decode("utf-8", "ignore")
    if r.returncode != 0 and err.strip():
        # 规模化纪律：SQL 报错绝不静默吞成 0，否则会误判「空库」→ 假收敛
        raise RuntimeError("pg 查询失败 rc=%d: %s" % (r.returncode, err.strip()[:400]))
    return out


def tv_lib_names():
    """从 library.db 动态发现所有 TV 库根名（Type=3 CollectionFolder）。"""
    tmp = tmp_path("libs.db")
    try:
        shutil.copy2(DB, tmp)
        import sqlite3
        c = sqlite3.connect(tmp)
        names = []
        for (p,) in c.execute(
                "select Path from MediaItems where Type=3 and Path like '/mnt/embytg/TV/%'"):
            m = re.search(r"/TV/([^/]+)/?$", p or "")
            if m:
                names.append(m.group(1))
        c.close()
    finally:
        _cleanup(tmp)
    return sorted(set(names))


def teldrive_by_lib(libs):
    """每库 teldrive 剧集 (files, uniq)。CTE 从 TV 根 folder 的 8 个库 folder 锚定。

    规模化：只遍历 TV 子树，不拼全盘树（Movies 不碰）；库 folder 用
    `parent_id = TV根id` 精确定位，避免按 name 全表扫。
    teldrive 顶层实测：parent_id is null 只有 Movies / TV 两个 folder，
    TV 的 8 个直接子 folder 就是 8 个库根。
    """
    q = r"""
with recursive roots as (
  select f.id, f.parent_id, f.name::text as name, f.name::text as lib
  from teldrive.files f
  where f.status='active' and f.kind='folder'
    and f.parent_id = (select id from teldrive.files
                       where status='active' and name='TV' and parent_id is null limit 1)
),
tree as (
  select id, parent_id, name, lib from roots
  union all
  select f.id, f.parent_id, f.name, t.lib
  from teldrive.files f join tree t on f.parent_id=t.id
  where f.status='active'
),
eps as (
  -- E004/E015 根因修复：EP_RE 只匹配集号，会把同名 .nfo 和 .dup 也算成
  -- 「剧集文件」。实测 12765 mp4 + 12326 nfo + 1844 dup 全被计入 →
  -- td_files 虚高、假报 libs_with_gap（emby_episodes 其实已对齐）。
  -- 用媒体扩展名白名单而非「排除 nfo/dup」黑名单，避免以后再漏新附属物。
  select lib, name from tree
  where name ~ '%s'
    and lower(name) ~ '\.(mp4|mkv|mov|avi|ts|m2ts|iso|wmv|flv|rmvb|webm)$'
)
select lib,
       count(*) as files,
       count(distinct regexp_replace(name, '\s*-\s*[0-9]{3,4}p.*$', '')) as uniq
from eps group by lib order by lib;
""" % EP_RE
    out = sh(["docker", "exec", "-i", "teldrive-postgres", "psql",
              "-U", "teldrive", "-d", "teldrive", "-At"], inp=q.encode())
    res = {}
    for ln in out.splitlines():
        parts = ln.split("|")
        if len(parts) >= 3:
            try:
                res[parts[0]] = (int(parts[1]), int(parts[2]))
            except ValueError:
                pass
    return res


def emby_by_lib():
    """每库 Emby Episode(Type=8) 数，SQL 聚合。"""
    tmp = tmp_path("ep.db")
    # Emby 的 library.db 是 WAL 模式：只拷主库会漏掉尚未 checkpoint 的写入（实测差 8 集）
    # → 主库 + -wal + -shm 一起拷，SQLite 会自动重放 WAL
    try:
        for suf in ("", "-wal", "-shm"):
            src = DB + suf
            if os.path.exists(src):
                shutil.copy2(src, tmp + suf)
        import sqlite3
        c = sqlite3.connect(tmp)
        agg = {}
        for (p,) in c.execute("select Path from MediaItems where Type=8 and Path like '/mnt/embytg/TV/%'"):
            m = re.search(r"/TV/([^/]+)/", p or "")
            if m:
                agg[m.group(1)] = agg.get(m.group(1), 0) + 1
        total = c.execute("select count(*) from MediaItems where Type=8").fetchone()[0]
        c.close()
    finally:
        _cleanup(tmp)
    return agg, total


def dup_episode_count(lib):
    """统计该库「同 Path 重复的 Episode 条数」（E115 的已知成因）。

    2026-11-15（E124）：`e > f` 不能一律判「口径异常」。
    实测 国产剧 Emby 8300 / 文件 8299，多出的那 1 条是
    偶像选拔 S01E03 的两条**同 Path 重复条目**（167446 / 167447，同一秒创建）
    —— TargetedScan 对同一路径并发处理两次（E115 已留证）。

    **不能靠 DELETE 清掉它**：E115 实测 DELETE /Items 会把 Path 指向的
    真实视频文件连同 .nfo 一起删（204 只是 HTTP 受理）。
    所以这类差异要单独识别、明确报出，而不是混进「不明异常」里 FAIL。
    判据仍是严格的：多出的数量必须**完全由重复条目解释**，否则照样 NG。

    数据源与 emby_by_lib 保持一致：直接查 library.db（Type=8），不走 API。
    """
    tmp = tmp_path("dup.db")
    try:
        for suf in ("", "-wal", "-shm"):
            src = DB + suf
            if os.path.exists(src):
                shutil.copy2(src, tmp + suf)
        import sqlite3
        c = sqlite3.connect(tmp)
        try:
            rows = c.execute(
                "select Path, count(*) c from MediaItems "
                "where Type=8 and Path like ? group by Path having c > 1",
                ("/mnt/embytg/TV/%s/%%" % lib,)).fetchall()
        finally:
            c.close()
        out = []
        for p, n in rows:
            out += [p] * (int(n) - 1)
        return out
    finally:
        _cleanup(tmp)


def reconcile():
    libs = tv_lib_names()
    td = teldrive_by_lib(libs)
    em, ep_total = emby_by_lib()
    rows = []
    for lib in sorted(set(list(td) + list(em) + libs)):
        f, u = td.get(lib, (0, 0))
        e = em.get(lib, 0)
        if f == 0:                       # 空库：不算缺口
            ok, why = True, "空库(0 文件)"
        elif e < u:
            ok, why = False, "缺 %d 集未入库" % (u - e)
        elif e > f:
            # E124：先把「同 Path 重复条目」认出来，能完全解释就不算故障
            dups = dup_episode_count(lib) if e - f > 0 else []
            if len(dups) >= (e - f):
                ok, why = True, ("多 %d 条（已定位：%d 条同 Path 重复条目，"
                                 "E115 成因，不能 DELETE 清）"
                                 % (e - f, len(dups)))
            else:
                ok, why = False, "Emby 多于文件数 %d（口径异常）" % (e - f)
        else:
            ok, why = True, "OK"
        rows.append({"lib": lib, "td_files": f, "td_unique": u,
                     "emby_episodes": e, "diff_vs_unique": e - u,
                     "ok": ok, "note": why})
    active = [r for r in rows if r["td_files"] > 0]
    converged = all(r["ok"] for r in active) if active else False
    return {
        "converged": converged,
        "rows": rows,
        "active_libs": len(active),
        "libs_with_gap": [r["lib"] for r in active if not r["ok"]],
        "episode_total": ep_total,
        "episode_expected_min": sum(r["td_unique"] for r in active),
        "episode_expected_max": sum(r["td_files"] for r in active),
        "td_files_total": sum(r["td_files"] for r in active),
    }


def main():
    r = reconcile()
    if "--json" in sys.argv:
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return
    print("=== 按库对账 (teldrive 文件/唯一集 vs Emby Episode) ===")
    print("  %-10s %9s %9s %9s %8s  %s" %
          ("库", "td文件", "td唯一集", "Emby集", "差(唯一)", "判定"))
    for x in r["rows"]:
        print("  %-10s %9d %9d %9d %8d  %s %s" %
              (x["lib"], x["td_files"], x["td_unique"], x["emby_episodes"],
               x["diff_vs_unique"], "OK" if x["ok"] else "NG", x["note"]))
    print("  ----")
    print("  有文件库 %d 个 / 缺口库 %s" % (r["active_libs"], r["libs_with_gap"]))
    print("  Emby Episode 总数 %d；期望区间 [%d(唯一集), %d(文件数)]；teldrive 文件 %d" %
          (r["episode_total"], r["episode_expected_min"],
           r["episode_expected_max"], r["td_files_total"]))
    print("  converged = %s" % r["converged"])


if __name__ == "__main__":
    main()
