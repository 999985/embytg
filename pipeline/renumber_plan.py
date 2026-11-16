#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""renumber_plan.py —— 重编号方案生成器（**只读**，不动一个文件）

## 为什么需要它

`naming_health` 长期显示 62 条 NFO 缺口。查下去不是"没刮到"，而是**盘侧季号/集号
与 TMDB 不符**——这类靠补 NFO 永远补不上，必须重编号。而重编号动的是真实目录结构，
一旦搞错就是几千个文件错位 + Emby 条目全部重挂。

所以在动盘之前，先把"要动什么、动多少、有什么风险"算清楚，交给人看。

## 它做什么

对每部有缺口的剧：
1. 读磁盘真实结构（每个 Season 有几集、集号是否连续、文件名长什么样）
2. 读 TMDB 的季/集结构（权威）
3. 给出**具体映射**：`旧路径 → 新路径`，逐条列出
4. 判定风险等级：
   - `safe`   —— 纯季号改名（S00 → S01），集号不变，无覆盖风险
   - `shift`  —— 需要改集号（绝对集号 → 季内集号），逐条映射，需人工确认
   - `dup`    —— 同一集在两个季都存在（重复文件），不能盲改，先人工定夺
   - `manual` —— 推不出唯一映射（集号跳跃/TMDB 数据本身缺失）
5. 统计影响面：移动文件数、涉及的 Emby 条目数、是否需要重新扫库

## 用法

```
python3 renumber_plan.py                  # 人类可读
python3 renumber_plan.py --json           # 机器可读
python3 renumber_plan.py --only 问心       # 只看一部
```

**它绝不执行。** 执行是另一个脚本（`renumber_apply.py`，尚未写），且必须
先有 `--dry-run` 输出与本脚本一致。这是"不可逆操作先报备"的具体落地。
"""
import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request

import embytg_conf as C

HERE = os.path.dirname(os.path.abspath(__file__))
MNT = "/opt/embytg/mnt/embytg/TV"
GAPS = os.path.join(HERE, "docs", "NFO_GAPS_20261113.md")
VIDEO = (".mp4", ".mkv", ".m4v", ".avi", ".ts")
SE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
TMDB_KEY = C.cfg("tmdb_api_key") or C.cfg("tmdb_key") or ""


# ---------------------------------------------------------------- 数据源
def gap_series():
    """从缺口报告里取「(库, 剧目录前缀) -> 缺几集」。

    注意：markdown 表格会把长目录名截断（如 `{tmdb=20687` 缺右括号），
    所以这里只取前缀，真正的目录名由 resolve_dir() 在磁盘上按前缀匹配回来。
    """
    out = {}
    if not os.path.exists(GAPS):
        return out
    for line in open(GAPS, encoding="utf-8"):
        m = re.match(r"\|\s*\d+\s*\|\s*([^|]+?)\s*\|\s*(\d+)\s*\|", line)
        if not m:
            continue
        full = m.group(1).strip()
        lib, _, name = full.partition("/")
        # 截断保护：到 "{tmdb=" 为止都算前缀
        key = name.split("{tmdb=")[0].strip() if "{tmdb=" in name else name
        out.setdefault((lib, key), int(m.group(2)))
    return out


def resolve_dir(lib, prefix):
    """在 MNT/lib 下按前缀找回真实目录名（缺口报告里的名字可能被截断）。"""
    d = os.path.join(MNT, lib)
    if not os.path.isdir(d):
        return ""
    cands = [n for n in os.listdir(d) if n.startswith(prefix)]
    if not cands:
        cands = [n for n in os.listdir(d) if prefix[:12] in n]
    return sorted(cands, key=len)[0] if cands else ""


def disk_struct(lib, sdir):
    """磁盘真实结构：{season: [(ep, fname), ...]}，外加是否有 tvshow.nfo。"""
    d = os.path.join(MNT, lib, sdir)
    out = {}
    if not os.path.isdir(d):
        return out
    for name in sorted(os.listdir(d)):
        p = os.path.join(d, name)
        if not os.path.isdir(p):
            continue
        # Season 00 / Season 01 / Specials / S1 ...
        ms = re.search(r"(\d{1,2})", name)
        if not ms:
            continue
        eps = []
        for root, _, fs in os.walk(p):
            for f in fs:
                if not f.lower().endswith(VIDEO):
                    continue
                me = SE.search(f)
                if me:
                    eps.append((int(me.group(2)), f))
        if eps:
            eps.sort()
            out[int(ms.group(1))] = eps
    return out


def tmdb_seasons(tmdb):
    """TMDB 权威季结构：{season: episode_count}。"""
    if not (tmdb and TMDB_KEY):
        return {}
    u = ("https://api.themoviedb.org/3/tv/%s?api_key=%s&language=zh-CN"
         % (tmdb, TMDB_KEY))
    try:
        with urllib.request.urlopen(u, timeout=25) as f:
            d = json.loads(f.read() or b"{}")
        return {int(s["season_number"]): int(s.get("episode_count") or 0)
                for s in (d.get("seasons") or []) if int(s.get("season_number") or 0) >= 0}
    except Exception:                                       # noqa: BLE001
        return {}


# ---------------------------------------------------------------- 方案

def _has_evidence(eps, season):
    """文件名里是否有**可核对的季集证据**：`第N集` 且 N 与旧集号一致（过半即可）。

    重编号的唯一可接受依据是"数据自己说了它属于第几集"，而不是"数字对得上"。
    没有证据时宁可不改——错误的集号比空 NFO 更伤用户（NFO 空只是没简介，
    集号错会让用户点开错的那一集）。
    """
    if not eps:
        return False
    hit = 0
    for e, f in eps:
        m = re.search(r"第\s*(\d+)\s*[集期话]", f)
        if m and int(m.group(1)) == e:
            hit += 1
    return hit >= max(1, len(eps) // 2)


def plan_one(lib, prefix, missing):
    sdir = resolve_dir(lib, prefix) or prefix
    disks = disk_struct(lib, sdir)
    m = re.search(r"\{tmdb=(\d+)\}", sdir)
    tmdb = m.group(1) if m else ""
    tseasons = tmdb_seasons(tmdb)

    dseasons = {s: len(v) for s, v in disks.items()}
    moves, notes = [], []

    def add_moves(season, target, order):
        """把 season 里的文件按 order(第几个) 映射到 target 季的连续集号。"""
        for i, (e, f) in enumerate(disks[season]):
            ne = order[i] if i < len(order) else i + 1
            nf = SE.sub(lambda mm: "S%02dE%02d" % (target, ne), f, count=1)
            if nf != f or target != season:
                moves.append({"from": "%s/Season %02d/%s" % (sdir, season, f),
                              "to": "%s/Season %02d/%s" % (sdir, target, nf),
                              "old_ep": e, "new_ep": ne})

    risk = "manual"
    for s in sorted(disks):
        eps = [e for e, _ in disks[s]]
        n = len(eps)
        cap = tseasons.get(s)
        contiguous = eps == list(range(1, n + 1))

        # (a) 季存在且集号规整、不超上限 → 不用动
        if cap and contiguous and max(eps) <= cap:
            continue

        # (b) 季存在、文件数 == TMDB 集数 → 原季内按顺序重排。
        #     但**必须有证据**：文件名里带「第N集」且与旧集号一致，或 NFO 里 <episode> 一致。
        #     2026-11-13 第三次踩坑：问心 S00 只有 2 个文件（E03/E04）、TMDB S0 恰好 2 集，
        #     "集数吻合"看起来是铁证，但文件名没有任何标题、也没有第N集标记——
        #     这 2 个文件到底是特别篇第 1-2 集，还是第 3-4 集（TMDB 没收录），无从判断。
        #     把 E03 改成 E01 会让用户看到错误的集号，比留空 NFO 更糟。
        if cap and n == cap and _has_evidence(disks[s], s):
            add_moves(s, s, list(range(1, n + 1)))
            notes.append("S%02d 有 %d 个文件、集号 %s…，文件名/ NFO 有季集证据，季内重排为 E01…E%02d"
                         % (s, n, eps[:4], n))
            risk = "shift"
            continue

        # (c) 跨季搬迁：只接受**强制可推导**的情形——目标季集数与文件数**完全相等**
        #     且该季在盘上为空、且这样的目标季只有一个。
        # 2026-11-13 第二次踩坑：第一版允许"任何集数 >= n 的空季"，结果把
        # 繁华落尽 S01E17-E21（绝对集号）映射到 S03、把二龙湖 S00 的 4 集映射到 S03。
        # 这些映射"看起来合理"，但没有任何证据支持——重编号最贵的错误就是这种。
        # 证据不足时必须停手，交给人，而不是猜一个能跑通的结果。
        cands = []
        for t in sorted(tseasons):
            if tseasons[t] != n:
                continue
            taken = {e for e, _ in disks.get(t, [])}
            if taken & set(range(1, n + 1)):
                continue
            cands.append(t)
        if len(cands) == 1 and _has_evidence(disks[s], s):
            tgt = cands[0]
            add_moves(s, tgt, list(range(1, n + 1)))
            notes.append("Season %02d 的 %d 个文件 → TMDB S%02d（该季恰好 %d 集且盘上为空，"
                         "集数完全吻合）；原集号 %s…"
                         % (s, n, tgt, tseasons[tgt], eps[:4]))
            risk = "shift"
            continue
        if len(cands) > 1:
            notes.append("Season %02d 的 %d 个文件有多个集数吻合的空季 %s，"
                         "无法确定唯一目标 → 人工" % (s, n, cands))
            risk = "manual"
            continue

        # (d) 目标季集数吻合但已被占用 → 合并会撞车
        occupied = [t for t in sorted(tseasons)
                    if tseasons[t] == n and ({e for e, _ in disks.get(t, [])}
                                             & set(range(1, n + 1)))]
        if occupied:
            notes.append("Season %02d 的 %d 个文件与 TMDB S%s 集数吻合但该季已有同号文件，"
                         "合并会覆盖 → 需人工确认保留哪一份"
                         % (s, n, occupied[:3]))
            risk = "dup"
            continue

        # (e) 其余（绝对/日期编号、数据源缺集、集数对不上）→ 一律不动
        if cap and n > cap:
            notes.append("S%02d 有 %d 个文件 > TMDB 的 %d 集，且无安全目标季 → "
                         "多半是重复集/花絮，需人工定夺" % (s, n, cap))
            risk = "dup" if risk != "shift" else risk
        else:
            notes.append("S%02d：盘 %d 个文件、TMDB 该季 %s 集、集号 %s… → "
                         "推不出唯一映射（绝对/日期编号或缺集），只能人工逐集确认"
                         % (s, n, cap if cap else "无此季", eps[:4]))
            if risk != "shift" and risk != "dup":
                risk = "manual"

    # 同一目标季被两处写入 → 撞车，整部降级为人工（2026-11-13：心动的信号 S00 和 S09
    # 都算出"21 个文件 → S05"，单看每条都对，合起来必然互相覆盖）
    tgts = {}
    for mv in moves:
        tgts.setdefault(mv["to"].rsplit("/", 1)[0], 0)
        tgts[mv["to"].rsplit("/", 1)[0]] += 1
    srcs = {}
    for mv in moves:
        key = mv["from"].rsplit("/", 2)[1]
        srcs.setdefault(mv["to"].rsplit("/", 1)[0], set()).add(key)
    clash = [t for t, s in srcs.items() if len(s) > 1]
    if clash:
        notes.append("多组来源都指向同一目标季 %s，映射互相冲突 → 全剧转人工"
                     % [c.rsplit("/", 1)[-1] for c in clash])
        moves = []
        risk = "manual"

    if not moves:
        if risk == "manual" and not notes:
            notes.append("结构与 TMDB 一致，缺口属数据源缺失，不需要重编号")
        elif risk == "manual":
            risk = "manual"
    return {"lib": lib, "series": sdir, "tmdb": tmdb, "missing": missing,
            "disk": dseasons, "tmdb_seasons": tseasons,
            "risk": risk, "why": "；".join(notes) or "无可行映射",
            "moves": moves}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--only")
    a = ap.parse_args()

    gs = gap_series()
    plans = []
    for (lib, sdir), miss in sorted(gs.items(), key=lambda x: -x[1]):
        if a.only and a.only not in sdir:
            continue
        try:
            plans.append(plan_one(lib, sdir, miss))
        except Exception as e:                              # noqa: BLE001
            plans.append({"lib": lib, "series": sdir, "missing": miss,
                          "risk": "error", "why": str(e)[:80], "moves": [],
                          "disk": {}, "tmdb_seasons": {}})

    order = {"safe": 0, "shift": 1, "dup": 2, "manual": 3, "error": 4}
    plans.sort(key=lambda p: (order.get(p["risk"], 9), -p["missing"]))

    if a.json:
        print(json.dumps(plans, ensure_ascii=False, indent=1))
        return 0

    tot_move = sum(len(p["moves"]) for p in plans)
    print("== 重编号方案（只读分析，未动任何文件）==")
    print("   涉及 %d 部剧，缺口 %d 集，需移动文件 %d 个\n"
          % (len(plans), sum(p["missing"] for p in plans), tot_move))
    for risk in ("safe", "shift", "dup", "manual", "error"):
        grp = [p for p in plans if p["risk"] == risk]
        if not grp:
            continue
        tag = {"safe": "✅ 可直接做（纯季号改名）",
               "shift": "⚠️  需改集号（有映射，需确认）",
               "dup": "⛔ 有重复集（先人工定夺）",
               "manual": "⛔ 推不出映射（必须人工）",
               "error": "❌ 分析出错"}[risk]
        print("── %s ── %d 部" % (tag, len(grp)))
        for p in grp:
            print("   %s/%s" % (p["lib"], p["series"][:44]))
            print("      缺 %d 集 | 盘 %s | TMDB %s"
                  % (p["missing"], p["disk"], p["tmdb_seasons"] or "?"))
            print("      → %s" % p["why"])
            for mv in p["moves"][:2]:
                print("        %s" % mv["from"][-52:])
                print("        → %s" % mv["to"][-52:])
            if len(p["moves"]) > 2:
                print("        …… 共 %d 条移动" % len(p["moves"]))
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
