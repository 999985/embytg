#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""docs_lint.py —— 文档与布局的不变量检查。

存在理由（2026-11-13）：
  项目曾经有 4 份 ERRORS.md、2 份 AGENTS.md，`Media/Updated` 这条规则散落 37 个文档。
  后果是**同一个事故被独立诊断了两次**（2026-11-12 的 D 态 ffprobe 写在根目录那份没人读的
  副本里，2026-11-13 又从头查了一遍）。文档写得多不等于知识传到了 —— 只有当不一致会
  让检查失败时，才是真的防住。

检查项（任一 FAIL 即返回非 0，daily_ops 会把它算进小结）：
  1. 真源唯一：AGENTS/PROJECT/START_HERE/ERRORS 不能有第二份实体文件（同 inode 的符号链接不算）
  2. 符号链接可达：pipeline/ 下的链接必须指向存在的真实文件
  3. ERRORS 编号唯一：## E0xx 不得重复
  4. INDEX.md 里列的路径必须存在
  5. docs/ 不得残留指向已归档文档的绝对引用（除非显式写 docs/archive/）
  6. 根目录不得出现「与 pipeline/ 同名但内容不同」的脚本（旧副本陷阱）

用法：
  python3 docs_lint.py            # 人类可读
  python3 docs_lint.py --json
"""
import argparse
import hashlib
import json
import os
import re
import sys

ROOT = "/opt/embytg"
PIPE = os.path.join(ROOT, "pipeline")
DOCS = os.path.join(ROOT, "docs")
ARCHIVE = os.path.join(DOCS, "archive")

CANON = {"AGENTS.md": ROOT, "PROJECT.md": ROOT, "START_HERE.md": ROOT,
         "ERRORS.md": DOCS}
LINKED = [os.path.join(PIPE, "AGENTS.md"), os.path.join(PIPE, "PROJECT.md"),
          os.path.join(PIPE, "START_HERE.md"), os.path.join(PIPE, "ERRORS.md"),
          os.path.join(PIPE, "docs")]


def md5(p):
    try:
        with open(p, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except OSError:
        return ""


def check_canonical():
    """真源只能有一份实体文件。"""
    bad = []
    for name, canon_dir in CANON.items():
        canon = os.path.join(canon_dir, name)
        if not os.path.isfile(canon):
            bad.append({"kind": "canon_missing", "detail": canon})
            continue
        real_inode = os.stat(canon).st_ino
        # 全库找同名文件，同 inode（符号链接）或同内容都算合规
        for dirpath, dirnames, filenames in os.walk(ROOT):
            dirnames[:] = [d for d in dirnames
                           if d not in (".git", "cache", "mnt", "backup",
                                        "pgdata", "logs", "_archive")]
            if name not in filenames:
                continue
            p = os.path.join(dirpath, name)
            if os.path.abspath(p) == os.path.abspath(canon):
                continue
            try:
                same_inode = os.stat(p).st_ino == real_inode
            except OSError:
                same_inode = False
            if same_inode:
                continue
            if md5(p) == md5(canon):
                bad.append({"kind": "duplicate_copy", "detail":
                            "%s 与真源内容相同（应改成符号链接）" % p})
            else:
                bad.append({"kind": "diverged_copy", "detail":
                            "%s 与真源 %s 内容不同（危险：两份不同的真相）"
                            % (p, canon)})
    return bad


def check_symlinks():
    bad = []
    for p in LINKED:
        if not os.path.lexists(p):
            bad.append({"kind": "link_missing", "detail": p})
        elif not os.path.exists(p):
            bad.append({"kind": "link_broken",
                        "detail": "%s -> %s" % (p, os.readlink(p))})
    return bad


def check_error_numbers():
    p = os.path.join(DOCS, "ERRORS.md")
    if not os.path.isfile(p):
        return [{"kind": "errors_missing", "detail": p}]
    nums = re.findall(r"(?m)^## (E\d{3})", open(p, encoding="utf-8").read())
    dup = sorted({n for n in nums if nums.count(n) > 1})
    return [{"kind": "error_number_dup", "detail": "%s 重复" % ", ".join(dup)}] if dup else []


def check_index_paths():
    p = os.path.join(DOCS, "INDEX.md")
    if not os.path.isfile(p):
        return [{"kind": "index_missing", "detail": p}]
    txt = open(p, encoding="utf-8").read()
    bad = []
    # 反引号里的 *.md / *.py 路径
    for m in re.findall(r"`([A-Za-z0-9_./\-]+\.(?:md|py|sh))`", txt):
        if m.startswith(("http", "/")):
            continue
        cands = [os.path.join(ROOT, m), os.path.join(PIPE, m),
                 os.path.join(DOCS, m), os.path.join(DOCS, os.path.basename(m))]
        if not any(os.path.exists(c) for c in cands):
            bad.append({"kind": "index_dead_path", "detail": m})
    return bad


def check_archived_refs():
    """docs/ 里不应再有指向已归档文档的引用（写成 docs/archive/ 的除外）。"""
    if not os.path.isdir(ARCHIVE):
        return []
    archived = {f for f in os.listdir(ARCHIVE) if f.endswith(".md")}
    bad = []
    for f in sorted(os.listdir(DOCS)):
        if not f.endswith(".md"):
            continue
        p = os.path.join(DOCS, f)
        txt = open(p, encoding="utf-8", errors="ignore").read()
        for a in archived:
            # 出现 "archive/xxx" 是合规的；裸 "docs/xxx" 或 "`xxx`" 视为未更新
            for m in re.finditer(re.escape(a), txt):
                pre = txt[max(0, m.start() - 12):m.start()]
                if "archive/" in pre:
                    continue
                bad.append({"kind": "archived_ref",
                            "detail": "%s 仍引用已归档的 %s" % (f, a)})
                break
    # INDEX.md 里提到归档文件名是允许的（它在"已归档"小节里说明去向），排除
    return [b for b in bad if b["detail"].split(" 仍引用")[0] != "INDEX.md"]


# 2026-11-14（E062）："_归档目录里放着生产配置"这类问题，
# 任何现有检查都查不出来 —— 它不是错误，只是**命名在骗人**。
# 它是外部评审看出来的，所以把它变成机器能查的规则：
# 生产脚本不得从 _archive/ 读取**运行时依赖**。
# 例外必须写进 ARCH_OK 并注明理由 —— 让例外是一个被审阅过的决定，
# 而不是一个被遗忘的例外。
ARCH_OK = {
    # 它本身就是管归档的，当然会提到归档路径
    "docs_lint.py": "归档目录的检查器",
    # 备份器：读归档目录只为打包，不是运行时依赖
    "env_pack.py": "打包时会遍历归档",
    # src_guard 保留了回退路径（config/ 不存在时读 _archive/)，
    # 目的是部署顺序出错时不断服；主路径已改为 config/。
    "src_guard.py": "仅作为 config/ 不存在时的回退",
    "src_login.py": "仅作为 config/ 不存在时的回退；另写 _archive/sources_probe.json（探针产物）",
    "_tidy.py": "它就是归档目录的整理器",
}


def check_archive_reads():
    """生产脚本不应从 _archive/ 读取运行时依赖。

    E062 的教训：只看"文件在不在"是不够的，还要问
    **"它待的那个目录，名字与它的职责是否一致"。**
    """
    bad = []
    for f in sorted(os.listdir(PIPE)):
        if not f.endswith(".py") or f in ARCH_OK:
            continue
        path = os.path.join(PIPE, f)
        try:
            txt = open(path, encoding="utf-8", errors="ignore").read()
        except Exception:                                      # noqa: BLE001
            continue
        # 只匹配**路径形态**："_archive" 或 ".../_archive/xxx"。
        # 宽松匹配会把文档字符串里提到的 _archive 也算进来，
        # 而这条检查要找的是"真的从那里读文件"。
        for m in re.finditer(r"""["']((?:\.{0,60}/)?_archive(?:/[^"'\s]{0,80})?)["']""", txt):
            lit = m.group(1)
            bad.append({"kind": "archive_read",
                        "detail": "%s 从归档目录读 %s（写进 ARCH_OK 并注明理由）"
                                  % (f, lit)})
    return bad


def check_layers():
    """pipeline/ 下每个脚本必须在 layers.json 里声明所属层。

    2026-11-14 外部评审指出：107 个脚本没有分层声明，
    哪些是生产链路、哪些是运维工具、哪些是历史遗留，
    只能靠文件名和 docstring 猜。

    但光写一份 ARCHITECTURE.md 不解决问题 —— 它会像 PROJECT.md 一样腐烂（那里
    还写着"七个 TG 影视频道"的旧架构）。所以把声明变成可执行的：
    **新增脚本不填 layers.json 就红。**

    四层的定义（写在 layers.json 的 rule 字段里）：
      prod   systemd / timer / run_one.sh 直接可达（生产链路 + 它的依赖）
      cron   Hermes cron 任务里点名调用
      tool   有 CLI 入口（__main__ 或 .sh），供人工运维
      legacy 无入口也无人调用（候选归档）
    """
    path = os.path.join(PIPE, "layers.json")
    try:
        d = json.load(open(path, encoding="utf-8"))
    except Exception as e:                                      # noqa: BLE001
        return [{"kind": "layers", "detail": "layers.json 读不到：%s" % e}]
    declared = {}
    for k in ("prod", "cron", "tool", "legacy"):
        for f in (d.get(k) or []):
            declared.setdefault(f, []).append(k)
    actual = {f for f in os.listdir(PIPE) if f.endswith((".py", ".sh"))}
    bad = []
    for f in sorted(actual - set(declared)):
        bad.append({"kind": "layer_missing",
                    "detail": "%s 未声明所属层（写进 pipeline/layers.json）" % f})
    for f in sorted(set(declared) - actual):
        bad.append({"kind": "layer_stale",
                    "detail": "%s 已不存在，从 layers.json 删掉" % f})
    for f, ks in declared.items():
        if len(ks) > 1:
            bad.append({"kind": "layer_dup",
                        "detail": "%s 同时属于 %s" % (f, ks)})
    return bad


def check_root_dupes():
    bad = []
    for f in sorted(os.listdir(ROOT)):
        if not f.endswith((".py", ".sh")):
            continue
        a = os.path.join(ROOT, f)
        b = os.path.join(PIPE, f)
        if os.path.isfile(a) and os.path.isfile(b):
            if md5(a) != md5(b):
                bad.append({"kind": "root_stale_script",
                            "detail": "%s 与 pipeline/%s 内容不同（根目录版多半是旧副本）"
                                      % (f, f)})
    return bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    problems = []
    for fn in (check_canonical, check_symlinks, check_error_numbers,
               check_index_paths, check_archived_refs, check_archive_reads, check_layers, check_root_dupes):
        try:
            problems.extend(fn())
        except Exception as e:                              # noqa: BLE001
            problems.append({"kind": "check_error",
                             "detail": "%s: %s" % (fn.__name__, str(e)[:70])})
    out = {"verdict": "PASS" if not problems else "FAIL",
           "problems": problems, "checked": 6}
    if a.json:
        print(json.dumps(out, ensure_ascii=False, indent=1))
    else:
        if problems:
            print("docs_lint: FAIL（%d 条）" % len(problems))
            for p in problems:
                print("  ✘ [%s] %s" % (p["kind"], p["detail"]))
        else:
            print("docs_lint: PASS（真源唯一 / 链接可达 / 编号唯一 / "
                  "INDEX 无死链 / 无归档残留引用 / 根目录无旧副本）")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
