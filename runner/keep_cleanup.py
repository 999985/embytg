#!/usr/bin/env python3
"""_keep 源文件缓存清理（E157）。

背景：上传失败（pc_worker 900s 超时）会把已下好的整集留在 _keep 等下一单复用，
      省掉整集重下的源站配额。但缓存必须有界，否则 8GB/集的残件几天就吃满磁盘。
规则：① TTL（默认 24h）过期即删；② 总容量超过上限（默认 12GiB）时从最旧删起。
两个阈值都可用环境变量覆盖：TG_KEEP_TTL_H / TG_KEEP_CAP_GB。
用法：python3 keep_cleanup.py [--dry]
"""
import os
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
KEEP = os.getenv("TG_KEEP_DIR", os.path.join(BASE, "_keep"))


def main(argv):
    dry = "--dry" in argv
    if not os.path.isdir(KEEP):
        print("keep_cleanup: 无 _keep 目录，跳过")
        return 0
    ttl_h = float(os.getenv("TG_KEEP_TTL_H", "24"))
    cap_gb = float(os.getenv("TG_KEEP_CAP_GB", "16"))
    cap_b = cap_gb * 1e9
    now = time.time()

    items = []
    for name in os.listdir(KEEP):
        path = os.path.join(KEEP, name)
        if not os.path.isfile(path):
            continue
        try:
            st = os.stat(path)
        except OSError:
            continue
        items.append({"path": path, "name": name, "mtime": st.st_mtime, "size": st.st_size})

    kept, dropped = [], []
    for it in items:
        if now - it["mtime"] > ttl_h * 3600:
            dropped.append(("ttl", it))
        else:
            kept.append(it)

    total = sum(it["size"] for it in kept)
    # 硬上限归硬上限，但**绝不能把磁盘余量吃到告警线以内**（disk_warn=20GB）。
    # 反过来也不能定得太紧：上限 8GiB 时，一个 6.8GB 的 2160p 会把刚存的那份自己删掉
    # （2026-11-16 23:35 实测踩到）。所以实际额度 = min(硬上限, 现有余量 - 保留线)。
    try:
        vfs = os.statvfs(BASE)
        free_b = vfs.f_bavail * vfs.f_frsize
    except OSError:
        free_b = 0
    reserve_b = float(os.getenv("TG_KEEP_RESERVE_GB", "20")) * 1e9
    cap_b = min(cap_b, max(0.0, free_b + total - reserve_b))
    for it in sorted(kept, key=lambda x: x["mtime"]):
        if total <= cap_b:
            break
        dropped.append(("cap", it))
        total -= it["size"]

    freed = 0
    gone = []
    for why, it in dropped:
        if dry:
            print("keep_cleanup[dry]: %s %s (%.2f GB)" % (why, it["name"], it["size"] / 1e9))
        else:
            try:
                os.remove(it["path"])
                freed += it["size"]
                gone.append(it)
                print("keep_cleanup: %s %s (%.2f GB)" % (why, it["name"], it["size"] / 1e9))
            except OSError as exc:
                print("keep_cleanup: 删除失败 %s: %s" % (it["name"], exc))
                total += it["size"]
    print("keep_cleanup: 删除 %d 个 / 释放 %.2f GB / 保留 %d 个 %.2f GB（上限 %.1f GB, TTL %.0fh）"
          % (len(gone), freed / 1e9, len(kept) - len(gone), total / 1e9, cap_b / 1e9, ttl_h))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
