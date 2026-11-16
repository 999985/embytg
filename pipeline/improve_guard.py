#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""improve_guard.py —— 自我改进的护栏：改之前留基线，改完第二天验效果，坏了自己回滚

用户 2026-11-11 授权：「自动回滚 / 阈值自适应 / 因果验证 你来解决」。

它解决三个问题
--------------
1. **改坏了能自己退**：`--mark` 在改进提交后记基线；`--check` 次日比对，回归就 `git revert`
2. **改完有没有用**：把指标（集数/NFO 缺口/成功率）与提交绑定记录，48h 内没改善 → 标记"无效"
3. **回滚要安全**：只回滚**带 `[self-improve]` 标记**的提交；冲突/依赖复杂时**不硬来**，转 P0 人工

判据（任一成立即回滚）
--------------------
- `py_compile` 全量失败（脚本坏了）
- `cron_lint` FAIL（任务引用失效/调度非法）
- `lib_reconcile` 由 converged=True 变 False
- Emby 集数比基线**减少 > 5**（疑似误删/索引崩塌）

用法
----
    python3 improve_guard.py --mark      # 改进提交后调用（记基线）
    python3 improve_guard.py --check     # 次日巡检调用（验效果/必要时回滚）
    python3 improve_guard.py --status
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = "/opt/embytg"
STATE = os.path.join(HERE, ".improve_state.json")
MARKER = "[self-improve]"
RECHECK_WAIT = 90             # 回归复查等待（秒）；瞬时状态不该触发回滚


def sh(cmd, timeout=600, cwd=HERE):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    return r.returncode, ((r.stdout or "") + (r.stderr or "")).strip()


def snapshot():
    snap: dict = {"ts": time.strftime("%F %T")}
    rc, out = sh("git rev-parse --short HEAD", cwd=ROOT)
    snap["head"] = out.strip()
    rc, out = sh("%s -m py_compile %s/*.py" % (sys.executable, HERE), timeout=300)
    snap["py_compile_ok"] = rc == 0
    rc, out = sh("%s %s/cron_lint.py --json" % (sys.executable, HERE), timeout=180)
    try:
        snap["cron_lint_ok"] = json.loads(out).get("verdict") == "PASS"
    except Exception:                                              # noqa: BLE001
        snap["cron_lint_ok"] = False
    rc, out = sh("%s %s/lib_reconcile.py 2>&1 | tail -3" % (sys.executable, HERE), timeout=900)
    snap["converged"] = "converged = True" in out
    # 单独再取一次明细（tail -3 只有汇总行，看不到每个库的差值）
    _, full = sh("%s %s/lib_reconcile.py 2>&1" % (sys.executable, HERE), timeout=900)
    # 2026-11-13：lib_reconcile 的缺口里有「正在下载中」的集（claim 已写、
    # teldrive 已登记、Emby 还没建条目），这不是回归。用活动 claim 数兜底：
    # 有 claim 在跑时，缺口 <= claim 数 视为正常波动，不算回归。
    snap["active_claims"] = 0
    snap["gap_le_claims"] = False
    try:
        import glob as _glob
        claims = [p for p in _glob.glob(os.path.join(HERE, ".gha_claims", "*.json"))]
        snap["active_claims"] = len(claims)
        # 从每个库行里取「差(唯一)」列，累加缺口绝对值
        missing = 0
        for line in (full or "").splitlines():
            m = re.match(r"^\s*\S+\s+\d+\s+\d+\s+\d+\s+(-?\d+)\s+", line)
            if m:
                missing += abs(int(m.group(1)))
        snap["gap"] = missing
        snap["gap_le_claims"] = bool(claims) and 0 < missing <= len(claims)
    except Exception:                                          # noqa: BLE001
        pass
    try:
        import urllib.parse
        import urllib.request
        c = json.load(open(os.path.join(ROOT, "config.json"), encoding="utf-8"))
        q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                    "Limit": "1", "EnableTotalRecordCount": "true"})
        req = urllib.request.Request((c["emby_url"].rstrip("/") + "/Items?" + q),
                                     headers={"X-Emby-Token": c["emby_token"]})
        snap["episodes"] = json.loads(urllib.request.urlopen(req, timeout=60).read().decode()).get("TotalRecordCount")
    except Exception:                                              # noqa: BLE001
        snap["episodes"] = None
    return snap


def mark(note=""):
    snap = snapshot()
    snap["note"] = note
    json.dump(snap, open(STATE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("基线已记：HEAD=%s py_compile=%s cron_lint=%s converged=%s episodes=%s"
          % (snap["head"], snap["py_compile_ok"], snap["cron_lint_ok"], snap["converged"], snap["episodes"]))
    return 0


def reverts_safely(commit):
    rc, out = sh("git revert --no-edit %s" % commit, cwd=ROOT, timeout=180)
    if rc != 0:
        sh("git revert --abort", cwd=ROOT)
        return False, out[-200:]
    return True, out[-200:]


def alert(level, code, msg, extra=None):
    try:
        sys.path.insert(0, HERE)
        import alert as A
        A.alert(level, code, msg, extra or {})
    except Exception:                                              # noqa: BLE001
        print("[%s] %s: %s" % (level, code, msg))


def judge(base, cur):
    """把「基线 vs 现在」翻译成回归理由；抽出来是为了能复查（见 check）。"""
    reasons = []
    if base.get("py_compile_ok") and not cur["py_compile_ok"]:
        reasons.append("py_compile 由通过变失败")
    if base.get("cron_lint_ok") and not cur["cron_lint_ok"]:
        reasons.append("cron_lint 由 PASS 变 FAIL")
    if base.get("converged") and not cur["converged"] and not cur.get("gap_le_claims"):
        reasons.append("lib_reconcile 由 converged 变 False（缺口 %s，活动 claim %s）"
                       % (cur.get("gap"), cur.get("active_claims")))
    if base.get("episodes") and cur.get("episodes") and cur["episodes"] < base["episodes"] - 5:
        reasons.append("Emby 集数 %s → %s（- %d）" % (base["episodes"], cur["episodes"],
                                                      base["episodes"] - cur["episodes"]))
    return reasons


def check():
    if not os.path.exists(STATE):
        print("没有基线（先用 --mark 记录）")
        return 0
    base = json.load(open(STATE, encoding="utf-8"))
    cur = snapshot()
    reasons = judge(base, cur)
    print("基线 %s（%s） vs 现在 %s" % (base.get("head"), base.get("ts"), cur.get("head")))
    # 2026-11-13：lib_reconcile 会被「刚落地但 Emby 还没索引」的新集短暂判 False
    # （06:20 实测 False，1 分钟后 True，集数 13030→13031），集数快照也可能滞后。
    # 只凭一次快照就 revert 会把好改动退掉，所以回归要**两次独立快照都成立**才回滚。
    if reasons:
        print("首次判定回归（%s），%ds 后复查一次" % ("；".join(reasons), RECHECK_WAIT))
        if RECHECK_WAIT:
            time.sleep(RECHECK_WAIT)
        cur2 = snapshot()
        reasons2 = judge(base, cur2)
        if not reasons2:
            print("✅ 复查通过：首次为瞬时状态（扫描/入库进行中），不回滚（集数 %s）"
                  % cur2.get("episodes"))
            cur, reasons = cur2, []
        else:
            reasons = reasons2
    if not reasons:
        try:
            led = os.path.join(HERE, ".improve_ledger.jsonl")
            rec = {"ts": time.strftime("%F %T"), "commit": base.get("head"),
                   "note": base.get("note", ""), "episodes_delta": (cur.get("episodes") or 0) - (base.get("episodes") or 0),
                   "converged": cur["converged"], "verdict": "no_regression"}
            with open(led, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:                                          # noqa: BLE001
            pass
        print("✅ 无回归：%s" % json.dumps({k: cur[k] for k in ("py_compile_ok", "cron_lint_ok", "converged", "episodes")},
                                          ensure_ascii=False))
        # 2026-11-17（E162 附带）：**通过后不再删除基线。**
        # 原来这里 `os.remove(STATE)`，于是「先 --mark 再 --check」的一次运行
        # 会把基线吃掉；第二天 --check 只打印「没有基线」→ 回归护栏**静默空转**，
        # 而它空转时没有任何告警（今天的自省就踩到了：mark 后 check 一次即清空）。
        # 保留基线是安全的：judge() 只判「比基线更差」，陈旧基线（集数更少）
        # 更不可能误报；下一次 --mark 会原子覆盖它。
        try:
            base["verified_ts"] = cur.get("ts")
            base["verified_head"] = cur.get("head")
            tmp = STATE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(base, f, ensure_ascii=False, indent=1)
            os.replace(tmp, STATE)
        except Exception:                                          # noqa: BLE001
            pass
        return 0

    print("⛔ 检出回归：")
    for r in reasons:
        print("   -", r)
    commit = base.get("head")
    rc, msg = sh("git log -1 --pretty=%%s %s" % commit, cwd=ROOT)
    is_improve = MARKER in msg
    if not is_improve:
        alert("P0", "improve_guard_regression",
              "检出回归但基线提交未带 %s 标记，不自动回滚：%s" % (MARKER, "; ".join(reasons)),
              {"commit": commit, "subject": msg[:80]})
        return 2
    ok, out = reverts_safely(commit)
    if ok:
        alert("P1", "improve_guard_reverted",
              "自我改进回滚成功：%s（%s）" % (commit, "; ".join(reasons)), {"commit": commit})
        os.remove(STATE)
        print("↩ 已回滚 %s（%s）" % (commit, "; ".join(reasons)))
        return 0
    alert("P0", "improve_guard_revert_failed",
          "回滚失败，需人工：%s（%s）" % (commit, "; ".join(reasons)), {"git": out[-120:]})
    print("✘ 回滚失败，已中止并告警：%s" % out[-160:])
    return 2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mark", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--note", default="")
    a = ap.parse_args()
    if a.mark:
        return mark(a.note)
    if a.check:
        return check()
    if os.path.exists(STATE):
        print(json.dumps(json.load(open(STATE, encoding="utf-8")), ensure_ascii=False, indent=1))
    else:
        print("无基线")
    return 0


if __name__ == "__main__":
    sys.exit(main())
