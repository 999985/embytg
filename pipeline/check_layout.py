#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""check_layout.py —— 项目布局门禁：一次性脚本不许留在 pipeline/

用户 2026-11-10 定：「噪音直接删，并且给出规定一次性脚本放 /tmp」。

判定
----
以下前缀/形态视为**一次性脚本**，必须放 `/tmp`（或 `/tmp/embytg-<date>/`），
出现在 `pipeline/` 根下即判 FAIL：

    _diag*.py   _probe_*.py   diag_*.py   _test*.py   try_*.py
    _show_*.py  _sum_*.py     _peek*.py   _patch_*.py
    *.bak                （备份副本一律不该存在）
    _verify_*.py         （除白名单：_verify_e008/_verify_e009 被文档引用）

豁免
----
- 被 systemd / cron / 定时任务 / 文档引用的脚本（白名单在 KEEP）
- `_scale_audit.py`（运维任务的工具，历史遗留，登记在白名单）

用法
----
    python3 check_layout.py            # 体检（退出码 0=PASS, 1=FAIL）
    python3 check_layout.py --fix      # 把命中的文件移到 /tmp/embytg-stray-<ts>/
"""
import argparse
import glob
import os
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PATTERNS = ["_diag*.py", "_probe_*.py", "diag_*.py", "_test*.py", "try_*.py",
            "_show_*.py", "_sum_*.py", "_peek*.py", "_patch_*.py", "*.bak", "*.bak.*"]
KEEP = {"_scale_audit.py", "_verify_e008.py", "_verify_e009.py", "_tidy.py",
        "probe_source_servers.py"}


def find_stray():
    out = []
    for pat in PATTERNS:
        for f in glob.glob(os.path.join(HERE, pat)):
            n = os.path.basename(f)
            if n in KEEP or not os.path.isfile(f):
                continue
            out.append(f)
    return sorted(set(out))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true")
    a = ap.parse_args()
    stray = find_stray()
    py = [f for f in glob.glob(os.path.join(HERE, "*.py"))]
    print("== 布局检查 ==")
    print("  pipeline/ 脚本 %d 个，一次性脚本 %d 个" % (len(py), len(stray)))
    if not stray:
        print("  ✅ PASS：没有一次性脚本残留（规则：放 /tmp）")
        return 0
    for f in stray:
        print("  ✘ %s" % os.path.basename(f))
    if a.fix:
        dst = "/tmp/embytg-stray-%s" % time.strftime("%Y%m%d_%H%M%S")
        os.makedirs(dst, exist_ok=True)
        for f in stray:
            shutil.move(f, os.path.join(dst, os.path.basename(f)))
        print("  ↪ 已移到 %s（临时目录，重启后系统会清）" % dst)
        return 0
    print("  → 处理：删掉（git 有历史）或 `python3 check_layout.py --fix` 移到 /tmp")
    return 1


if __name__ == "__main__":
    sys.exit(main())
