#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""renumber_decide.py —— 重编号的「人做决定、机器执行」机制

## 为什么要有它

`renumber_plan.py` 证明了：62 条 NFO 缺口**没有一条能安全机械重编号**——
数据本身有歧义（盘侧季号冲突 / 绝对集号 / TMDB 自己季划分错位）。
但"不能自动修"不等于"永远悬着"：需要有人做一次判断，然后把判断固化成可执行的东西。

本脚本就是这个固化层：

    人写决策  →  renumber_decisions.json  →  本脚本执行（带 dry-run / 不覆盖 / 可回滚）

## 决策文件格式（`pipeline/renumber_decisions.json`）

```json
{
  "国漫/少年白马醉春风 (2022) {tmdb=206878}": {
    "action": "move_season",
    "map": {"0": 2},
    "note": "S00 的 20 集是第二季前 20 集（已核对第 1 集片头）"
  },
  "港台剧/侠医传奇 (2011) {tmdb=203509}": {
    "action": "keep",
    "note": "S01 后 4 集是花絮，TMDB 无对应，保持现状"
  },
  "综艺/心动的信号 (2018) {tmdb=103147}": {
    "action": "move_episode",
    "map": {"0:179": "5:1", "0:180": "5:2"},
    "note": "按文件名第8季…以 TMDB 为准重排"
  }
}
```

三种动作：
- `keep`          明确决定「保持现状」——将来不再计入待决策，也不再有缺口告警
- `move_season`   整季搬迁：`{"旧季": 新季}`，季内按集号顺序重新编号为 1..N
- `move_episode`  逐集映射：`{"旧季:旧集": "新季:新集"}`，用于绝对/日期编号

## 安全设计（这是动真数据的脚本，所以每一步都要能退）

1. **默认 dry-run**，必须显式 `--apply` 才动文件
2. **绝不覆盖**：目标已存在就直接失败，不 delete、不 overwrite
3. **写 undo 脚本**：每次 apply 生成 `logs/renumber_undo_<ts>.sh`，逐条 nm -v，
   出问题一条命令全部改回
4. **移动前校验**：源文件存在、目标目录存在（不存在则创建）、目标不在磁盘上
5. **移动后通知 Emby**：对每个新路径发 `emby_scan.scan_paths`，并按需补 NFO
6. **全程记账**：`logs/renumber_apply_<ts>.jsonl` 一行一条

## 用法

```
python3 renumber_decide.py --template          # 生成决策模板（把 14 部剧列出来）
python3 renumber_decide.py --check             # 校验决策文件格式 + 预演（不动文件）
python3 renumber_decide.py --apply             # 真正执行（会先打印将要做的每一件事）
python3 renumber_decide.py --apply --only 问心  # 只执行一部
```
"""
import argparse
import json
import os
import re
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
MNT = "/opt/embytg/mnt/embytg/TV"
DEC = os.path.join(HERE, "renumber_decisions.json")
VIDEO = (".mp4", ".mkv", ".m4v", ".avi", ".ts")
SE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")


def load_decisions():
    if not os.path.exists(DEC):
        return {}
    try:
        return json.load(open(DEC, encoding="utf-8"))
    except Exception as e:                                  # noqa: BLE001
        print("决策文件解析失败：%s" % e)
        return {}


def series_dir(key):
    """决策文件的 key 就是「库/剧目录名」，直接拼路径。"""
    lib, _, name = key.partition("/")
    d = os.path.join(MNT, lib, name)
    return d if os.path.isdir(d) else ""


def video_files(d):
    """{season: [(ep, fname)]}"""
    out = {}
    for name in sorted(os.listdir(d)):
        p = os.path.join(d, name)
        if not os.path.isdir(p):
            continue
        ms = re.search(r"(\d{1,2})", name)
        if not ms:
            continue
        eps = []
        for root, _, fs in os.walk(p):
            for f in fs:
                if f.lower().endswith(VIDEO):
                    me = SE.search(f)
                    if me:
                        eps.append((int(me.group(2)), f))
        if eps:
            out[int(ms.group(1))] = sorted(eps)
    return out


def sem_conflict(dst):
    """同季同集语义冲突检测：目标季里是否已有**别的文件**占着同一个 S/E。

    2026-11-13 干跑抓到的真 bug：权欲 S00E01（`- 1080p - 1080p.mp4`）→ S01E01，
    而 S01E01 已经有 `- 2160p.mp4`。文件名不同，`os.path.exists(dst)` 是 False，
    于是被判成"可执行"——搬过去就会变成一季里两个 E01，Emby 只会收一个，
    另一个变孤儿（项目里那些 `.dup` 文件就是这么来的）。
    所以不能只查路径，必须查**季集号**。
    """
    d = os.path.dirname(dst)
    m = SE.search(os.path.basename(dst))
    if not (os.path.isdir(d) and m):
        return ""
    want = (int(m.group(1)), int(m.group(2)))
    for f in os.listdir(d):
        if not f.lower().endswith(VIDEO):
            continue
        mm = SE.search(f)
        if mm and (int(mm.group(1)), int(mm.group(2))) == want and f != os.path.basename(dst):
            return f
    return ""


def plan_moves(key, dec):
    """把一条决策翻译成具体文件操作清单。返回 (moves, errors)。"""
    d = series_dir(key)
    if not d:
        return [], ["找不到剧目录：%s" % key]
    disks = video_files(d)
    moves, errs = [], []
    act = (dec.get("action") or "").strip()

    if act == "keep":
        return [], []

    if act == "move_season":
        for old_s, new_s in (dec.get("map") or {}).items():
            old_s, new_s = int(old_s), int(new_s)
            if old_s not in disks:
                errs.append("%s 没有 Season %02d" % (key, old_s))
                continue
            for i, (e, f) in enumerate(disks[old_s], 1):
                nf = SE.sub(lambda m: "S%02dE%02d" % (new_s, i), f, count=1)
                moves.append({
                    "src": os.path.join(d, "Season %02d" % old_s, f),
                    "dst": os.path.join(d, "Season %02d" % new_s, nf),
                })
        return moves, errs

    if act == "move_episode":
        for k, v in (dec.get("map") or {}).items():
            try:
                o_s, o_e = [int(x) for x in str(k).split(":")]
                n_s, n_e = [int(x) for x in str(v).split(":")]
            except Exception:                               # noqa: BLE001
                errs.append("映射格式错：%s -> %s（应为 旧季:旧集 -> 新季:新集）" % (k, v))
                continue
            hit = [(e, f) for e, f in disks.get(o_s, []) if e == o_e]
            if not hit:
                errs.append("%s 没有 S%02dE%02d" % (key, o_s, o_e))
                continue
            for _, f in hit:
                nf = SE.sub(lambda m: "S%02dE%02d" % (n_s, n_e), f, count=1)
                moves.append({
                    "src": os.path.join(d, "Season %02d" % o_s, f),
                    "dst": os.path.join(d, "Season %02d" % n_s, nf),
                })
        return moves, errs

    errs.append("未知 action：%r（应为 keep / move_season / move_episode）" % act)
    return moves, errs


def show(moves, errs, apply_mode):
    ok = 0
    for mv in moves:
        src, dst = mv["src"], mv["dst"]
        problems = []
        if not os.path.isfile(src):
            problems.append("源不存在")
        if os.path.exists(dst):
            problems.append("目标路径已存在（会覆盖，拒绝）")
        _c = sem_conflict(dst)
        if _c:
            problems.append("同季同集已被占用：%s" % _c[:40])
        mark = "✓" if not problems else "✗"
        if not problems:
            ok += 1
        print("  %s %s" % (mark, os.path.basename(src)[:52]))
        print("    → %s" % os.path.basename(dst)[:52])
        if problems:
            print("    ! %s" % "；".join(problems))
    for e in errs:
        print("  ✗ %s" % e)
    return ok


def do_apply(moves):
    """执行移动。生成 undo 脚本。绝不覆盖。"""
    ts = time.strftime("%Y%m%d_%H%M%S")
    undo = os.path.join(HERE, "logs", "renumber_undo_%s.sh" % ts)
    log = os.path.join(HERE, "logs", "renumber_apply_%s.jsonl" % ts)
    os.makedirs(os.path.dirname(undo), exist_ok=True)
    done = 0
    with open(undo, "w", encoding="utf-8") as u, open(log, "w", encoding="utf-8") as lg:
        u.write("#!/bin/bash\n# 撤销 %s 的重编号。执行即全部改回。\nset -e\n" % ts)
        for mv in moves:
            src, dst = mv["src"], mv["dst"]
            if not os.path.isfile(src) or os.path.exists(dst):
                lg.write(json.dumps({"skipped": mv, "why": "src/ dst 检查失败"},
                                    ensure_ascii=False) + "\n")
                continue
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(src, dst)
            # 同名 nfo 一起搬（否则新位置丢元数据、老位置留孤儿 NFO）
            sn = os.path.splitext(src)[0] + ".nfo"
            dn = os.path.splitext(dst)[0] + ".nfo"
            if os.path.exists(sn) and not os.path.exists(dn):
                shutil.move(sn, dn)
            u.write('mv -v %s %s\n' % (shquote(dst), shquote(src)))
            lg.write(json.dumps({"moved": mv}, ensure_ascii=False) + "\n")
            done += 1
    os.chmod(undo, 0o755)
    return done, undo, log


def shquote(s):
    return "'" + s.replace("'", "'\\''") + "'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--only")
    a = ap.parse_args()

    if a.template:
        sys.path.insert(0, HERE)
        import renumber_plan as R
        tpl = {}
        for (lib, name), miss in sorted(R.gap_series().items(), key=lambda x: -x[1]):
            p = R.plan_one(lib, name.split("{tmdb=")[0].strip(), miss)
            tpl[p["series"] and ("%s/%s" % (lib, p["series"])) or ("%s/%s" % (lib, name))] = {
                "action": "keep",
                "map": {},
                "note": ("缺 %d 集 | 盘 %s | TMDB %s | %s"
                         % (miss, p["disk"], p["tmdb_seasons"] or "?", p["why"][:120])),
            }
        if os.path.exists(DEC):
            print("决策文件已存在，未覆盖：%s" % DEC)
            print("（要重新生成请先移走它）")
        else:
            json.dump(tpl, open(DEC, "w", encoding="utf-8"),
                      ensure_ascii=False, indent=1)
            print("已生成决策模板 %s（%d 部剧，默认全部 action=keep）" % (DEC, len(tpl)))
            print("改 action / map 后跑 --check，确认无误再 --apply。")
        return 0

    dec = load_decisions()
    if not dec:
        print("没有决策文件。先跑 --template 生成，再逐部改。")
        return 2

    todo = {k: v for k, v in dec.items() if not a.only or a.only in k}
    all_moves, all_errs = [], []
    for key, d in todo.items():
        mv, er = plan_moves(key, d)
        all_moves += mv
        all_errs += ["%s: %s" % (key.split("/")[-1][:20], e) for e in er]

    print("== 决策执行（%s）==" % ("APPLY" if a.apply else "DRY-RUN"))
    print("   涉及 %d 部剧，计划移动 %d 个文件" % (len(todo), len(all_moves)))
    ok = show(all_moves, all_errs, a.apply)

    if not a.apply:
        print("\n  预演完成：%d 条可执行，%d 条有问题。" % (ok, len(all_moves) - ok))
        print("  确认无误后加 --apply。执行会生成 undo 脚本。")
        return 0

    if not all_moves:
        print("\n  没有可执行操作。")
        return 0
    done, undo, log = do_apply(all_moves)
    print("\n  已移动 %d 个文件" % done)
    print("  撤销脚本：%s" % undo)
    print("  执行日志：%s" % log)

    # 通知 Emby + 补 NFO（新路径才有 NFO 可言）
    try:
        import emby_scan
        paths = ["/mnt/embytg/" + os.path.relpath(m["dst"], "/opt/embytg/mnt/embytg")
                 for m in all_moves]
        r = emby_scan.scan_paths(paths)
        print("  Emby 通知(%s)：Created=%d Refreshed=%d Failed=%d"
              % (r["mode"], r.get("Created", 0), r.get("Refreshed", 0), r.get("Failed", 0)))
    except Exception as e:                                  # noqa: BLE001
        print("  Emby 通知失败（不影响已移动的文件，稍后 reconcile 会补）：%s" % str(e)[:60])
    return 0


if __name__ == "__main__":
    sys.exit(main())
