#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""quarantine_audit.py —— 隔离 claim 复核（无人值守）

## 为什么需要它

`run_one.sh` 连续 5 轮收尾失败就把 claim 挪进 `.gha_claims_quarantine/`、发一条 P1、
放掉 lane。**之后再没有人回头看它** —— 隔离清单是个黑洞。

实测（2026-11-16）：`20261116_042110-slot1-new2-b2500b`（金田一 S06E01）12:32 被隔离，
17 分钟后根因（E154：磁盘季在 TMDB 不存在 → 补占位 NFO）就已修复，文件在
TG / teldrive / tg.db / Emby 四处全部正常，`post_ingest --verify-only` 已 PASS，
可隔离清单里那条记录永远留着。结果：一条 P1 告警既没有「自愈」的收口，
也没有「仍未修好」的升级 —— 两种真实状态无法区分。

## 判据（四条全部可证明，缺一条不算好）

1. 文件在挂载上存在（claim 的 `target_rel`）
2. `tg.db.state_key` 有该 key 且带 `file_id`（teldrive 登记过）
3. `tg.db.resource_state` 有 `landed_at` 且 `emby_item` 非空（台账与 Emby 都见过）
4. 文件旁边有 `.nfo` 且 `<title>` 非空（收尾验收看的就是这个）

四条全过 → 移到 `quarantine/resolved/`，日志一行，不惊动人。
任一条不过 → 留在原地；隔离超过 6 小时仍不过 → P1 升级（同一 claim 24h 只报一次）。

## 用法

```
python3 quarantine_audit.py            # 只报
python3 quarantine_audit.py --apply    # 已好的移入 resolved/，并做升级告警
```

`claim_reap.py --apply`（每 15 分钟的 embytg-claimreap.timer）会顺带调用它。
"""
import argparse
import glob
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
QUAR = os.path.join(HERE, ".gha_claims_quarantine")
RESOLVED = os.path.join(QUAR, "resolved")
DB = os.path.join(HERE, "tg.db")
MNT = "/opt/embytg/mnt/embytg"
STATE = os.path.join(HERE, ".quarantine_audit.json")
ESCALATE_AFTER_S = 6 * 3600
REALERT_AFTER_S = 24 * 3600
VIDEO_EXT = (".mkv", ".mp4", ".ts", ".m4v", ".mov", ".avi", ".wmv", ".flv", ".webm")


def _rel_of(d):
    """claim 的目标相对路径（挂在 MNT 下）。老 claim 没有 target_rel 时返回空。"""
    rel = (d.get("target_rel") or "").strip()
    if rel:
        return rel
    return ""


def check_claim(d, db=DB, mnt=MNT):
    """返回 (ok, reasons)。reasons 列出所有不满足的判据（空 = 全过）。"""
    bad = []
    rel = _rel_of(d)
    if not rel:
        return False, ["claim 无 target_rel（无法证明落点）"]
    p = os.path.join(mnt, rel)
    if not os.path.exists(p):
        bad.append("文件不在挂载上")
    key = (d.get("key") or "").strip()
    try:
        con = sqlite3.connect(db, timeout=20)
        try:
            sk = con.execute("select val from state_key where k=?", (key,)).fetchone() if key else None
            rs = con.execute("select landed_at, emby_item from resource_state where rel=?",
                             (rel,)).fetchone()
        finally:
            con.close()
    except Exception as e:                                  # noqa: BLE001
        return False, ["tg.db 读取失败 %s" % type(e).__name__]
    fid = ""
    if sk:
        try:
            fid = (json.loads(sk[0] or "{}") or {}).get("file_id") or ""
        except Exception:                                   # noqa: BLE001
            fid = ""
    if not fid:
        bad.append("tg.db 无 file_id（未登记 teldrive）")
    if not rs:
        bad.append("resource_state 无该文件")
    else:
        if not rs[0]:
            bad.append("resource_state.landed_at 为空")
        if not (rs[1] or ""):
            bad.append("resource_state.emby_item 为空")
    nfo = os.path.splitext(p)[0] + ".nfo"
    if not os.path.exists(nfo):
        bad.append("无 NFO")
    else:
        try:
            body = open(nfo, encoding="utf-8", errors="ignore").read()
        except Exception:                                   # noqa: BLE001
            body = ""
        m = re.search(r"<title>(.*?)</title>", body, re.S)
        if not m or not m.group(1).strip():
            bad.append("NFO 无标题")
    return (not bad), bad


def _load_state():
    try:
        return json.load(open(STATE, encoding="utf-8"))
    except Exception:                                       # noqa: BLE001
        return {}


def _save_state(st):
    try:
        json.dump(st, open(STATE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    except Exception:                                       # noqa: BLE001
        pass


def _alert(msg):
    try:
        subprocess.run([sys.executable, os.path.join(HERE, "alert.py"),
                        "--level", "P1", "--code", "quarantine_stuck", "--msg", msg],
                       capture_output=True, text=True, timeout=60)
        return True
    except Exception:                                       # noqa: BLE001
        return False


def audit(apply=False, quar=QUAR, resolved=RESOLVED, db=DB, mnt=MNT, state_path=STATE):
    """扫隔离清单。返回 {'resolved': [...], 'stuck': [...], 'checked': n}"""
    out = {"resolved": [], "stuck": [], "checked": 0}
    st = _load_state() if state_path == STATE else {}
    now = int(time.time())
    os.makedirs(resolved, exist_ok=True) if apply else None
    for f in sorted(glob.glob(os.path.join(quar, "*.json"))):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:                                   # noqa: BLE001
            continue
        out["checked"] += 1
        ok, bad = check_claim(d, db=db, mnt=mnt)
        rid = d.get("run_id") or os.path.basename(f)[:-5]
        tag = "%-42s %s" % (rid, (d.get("series") or "")[:16])
        if ok:
            out["resolved"].append(rid)
            print("  ✓ %s 数据面完好（文件/台账/Emby/NFO 四项齐）→ resolved" % tag)
            if apply:
                try:
                    os.replace(f, os.path.join(resolved, os.path.basename(f)))
                except Exception as e:                      # noqa: BLE001
                    print("     ! 移动失败 %s" % e)
            st.pop(rid, None)
            continue
        age = now - int(os.path.getmtime(f))
        out["stuck"].append(rid)
        print("  · %s 隔离 %dh 仍不全：%s" % (tag, age // 3600, "、".join(bad)))
        # 升级：超过 6 小时仍未修好，说明是人要看的真问题；24h 只报一次
        rec = st.get(rid) or {}
        if age >= ESCALATE_AFTER_S and now - int(rec.get("last_alert") or 0) >= REALERT_AFTER_S:
            if apply:
                if _alert("隔离 claim %s（%s E%s）已 %dh 仍需人看：%s；见 .gha_claims_quarantine/%s.json"
                          % (rid, (d.get("series") or "")[:20], d.get("ep"), age // 3600,
                             "、".join(bad), rid)):
                    rec["last_alert"] = now
                    st[rid] = rec
    if st or state_path == STATE:
        _save_state(st)
    print("  隔离清单：检查 %d 个，%d 个已好可结案，%d 个仍待修"
          % (out["checked"], len(out["resolved"]), len(out["stuck"])))
    if not apply:
        print("  （只读模式；claim_reap.py --apply 会顺带执行）")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    audit(apply=a.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
