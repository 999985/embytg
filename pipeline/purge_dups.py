#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""purge_dups.py —— 清理 `.dup` 冗余文件（软删，30 天可恢复）

## 背景

库里有 1921 个 `*.mp4.dup`，共占 **1487 GB**。它们是历史去重流程产生的：
当时发现同一集有多个版本时，把差的那份改名加 `.dup` 后缀而不是删除。
后果：
1. 这 1.48 TB 仍然实实在在占着 Telegram 存储（只是对 Emby 不可见）
2. 目录里混着 .dup，人肉排查时极容易误判（"这集有两个文件？"）
3. teldrive 索引里有 1921 条无用节点，拖慢树遍历

## 安全模型（三层）

1. **只删有同集替代的**：逐个检查同目录、同 (S,E) 是否有非 .dup 视频；
   没有的不动（那是唯一副本）。
2. **清晰度不得下降**：替代版本的清晰度标记必须 >= dup 的。实测有 2 个反例
   （老九门 S01E27、万物生灵 S06E03：dup 是 2160p 而保留的是 1080p）——这 2 个不删，
   因为删了等于降画质。
3. **走软删，不硬删**：通过挂载 `rm` → teldrive 标记 `deleted_at`，
   TG 消息还在；`teldrive_cleanup_trash`（retention 720h）30 天后才真正从
   Telegram 删除。**这 30 天就是后悔药**，期间可以恢复。
   想立刻回收空间就手动触发一次 cleanup_trash 并把 retention 调小——不建议，
   除非确认过。

## 用法

```
python3 purge_dups.py --scan            # 只统计（不删）
python3 purge_dups.py --apply --limit 20  # 先删 20 个验证
python3 purge_dups.py --apply           # 全量
python3 purge_dups.py --verify          # 核对回收站里的 .dup 数量
```

清单落在 `logs/dup_purge_<ts>.jsonl`（保留/删除/跳过 逐条原因）。
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
MNT = "/opt/embytg/mnt/embytg"
VIDEO = (".mp4", ".mkv", ".m4v", ".avi", ".ts")
SE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
QUAL = [("2160p", 4), ("1080p", 3), ("720p", 2), ("480p", 1), ("sd", 0)]


def qual(name):
    low = name.lower()
    for k, v in QUAL:
        if k in low:
            return v, k
    return -1, "?"


def scan():
    """返回 (safe, unsafe, orphan)，元素含路径/大小/保留版/清晰度。"""
    safe, unsafe, orphan = [], [], []
    for root, _, fs in os.walk(MNT):
        for f in fs:
            if not f.endswith(".dup"):
                continue
            full = os.path.join(root, f)
            base = f[:-4]
            m = SE.search(base)
            sib = ""
            if m:
                want = (int(m.group(1)), int(m.group(2)))
                for x in os.listdir(root):
                    if x.endswith(VIDEO) and not x.endswith(".dup"):
                        mm = SE.search(x)
                        if mm and (int(mm.group(1)), int(mm.group(2))) == want:
                            sib = x
                            break
            try:
                sz = os.path.getsize(full)
            except Exception:                               # noqa: BLE001
                sz = 0
            if not sib:
                orphan.append({"path": full, "size": sz})
                continue
            qd, qdn = qual(base)
            qs, qsn = qual(sib)
            item = {"path": full, "size": sz, "keep": sib,
                    "qual_dup": qdn, "qual_keep": qsn}
            (safe if qs >= qd else unsafe).append(item)
    return safe, unsafe, orphan


def apply_delete(items, limit=0):
    ts = time.strftime("%Y%m%d_%H%M%S")
    log = os.path.join(HERE, "logs", "dup_purge_%s.jsonl" % ts)
    os.makedirs(os.path.dirname(log), exist_ok=True)
    todo = items[:limit] if limit else items
    ok = fail = 0
    freed = 0
    with open(log, "w", encoding="utf-8") as lg:
        for i, it in enumerate(todo, 1):
            p = it["path"]
            try:
                # 用 rm（走 rclone → teldrive 软删）。不用 unlink 直调，保持一致。
                r = subprocess.run(["rm", "-f", p], capture_output=True, timeout=120)
                if r.returncode == 0 and not os.path.exists(p):
                    ok += 1
                    freed += it["size"]
                    lg.write(json.dumps({"deleted": p, "size": it["size"],
                                         "kept": it["keep"]}, ensure_ascii=False) + "\n")
                else:
                    fail += 1
                    lg.write(json.dumps({"failed": p,
                                         "err": (r.stderr or b"").decode()[:120]},
                                        ensure_ascii=False) + "\n")
            except Exception as e:                          # noqa: BLE001
                fail += 1
                lg.write(json.dumps({"failed": p, "err": str(e)[:120]},
                                    ensure_ascii=False) + "\n")
            if i % 100 == 0:
                print("  %d/%d ok=%d fail=%d 已释放 %.1f GB"
                      % (i, len(todo), ok, fail, freed / 2**30), flush=True)
    print("DONE %d 条：ok=%d fail=%d，释放 %.1f GB（软删，30 天内可恢复）"
          % (len(todo), ok, fail, freed / 2**30))
    print("  日志：%s" % log)
    return ok, fail


def verify():
    """软删后核对 teldrive 里的状态。"""
    q = ("select count(*) filter (where deleted_at is null),"
         " count(*) filter (where deleted_at is not null),"
         " pg_size_pretty(coalesce(sum(size) filter (where deleted_at is not null),0))"
         " from files where name like '%.dup'")
    r = subprocess.run(["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive",
                        "-d", "teldrive", "-tAc", q],
                       capture_output=True, text=True, timeout=120)
    print("  .dup 状态（live | 回收站 | 回收站占用）：" + (r.stdout or "").strip())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    if a.verify:
        verify()
        return 0

    t0 = time.time()
    safe, unsafe, orphan = scan()
    tot = sum(x["size"] for x in safe)
    print("== .dup 扫描（%.0fs）==" % (time.time() - t0))
    print("  安全可删 : %4d 个 / %.1f GB" % (len(safe), tot / 2**30))
    print("  不能删(降画质): %2d 个 / %.1f GB" % (len(unsafe),
                                                 sum(x["size"] for x in unsafe) / 2**30))
    print("  不能删(唯一副本): %2d 个" % len(orphan))
    for x in unsafe[:5]:
        print("    · %s（dup=%s 保留=%s）" % (os.path.basename(x["path"])[:50],
                                            x["qual_dup"], x["qual_keep"]))
    if a.scan:
        return 0
    if a.apply:
        apply_delete(safe, a.limit)
        verify()
    return 0


if __name__ == "__main__":
    sys.exit(main())
