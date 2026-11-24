#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""timer_audit.py — 每天巡检「定时任务 + runner」日志，出事故必须留痕。

用户口径（2026-11-23）：
  「记得日志一定要记录出事情 其次就是每天的定时任务也要检查日志」

此前的问题：daily_ops 检查的是业务指标（五账、磁盘、队列），**从不看 journal**。
于是 `embytg-ghsync` 连续几天推送失败、`embytg-mount` 偶发掉挂载、
runner 每分钟刷 no_session —— 全都躺在日志里没人看。事故不是没发生，
是没被记录成事故。

本脚本做三件事：
  1. 扫 embytg-* 单元的失败（Failed with result / 非零退出）
  2. 统计 runner loop.log 的退出码与原因分布（rc=1 / no_session / no_job …）
  3. 有**新增**失败 → 写 alerts 日志 + 走 alert.py 出口；全绿 → 只留一行摘要

2026-11-23（E198）——「同一场事故别报第二遍」：
  旧判据把所有单元近 24h 的 failure 全量再报一次。于是 07:37 那批
  「run_one 以 rc=1 表示没活干」的架构问题（364 条）被每一次巡检重复告警，
  第二天又因为窗口还盖着它再报一遍；真正的性质是**已经修好的旧事故**，
  却永远占用告警位，把新事故挤掉。
  现在每个单元记 journal cursor，只报 cursor 之后的新失败；
  首次运行/换机没有 cursor 时退回「上次巡检时间之后」，再退回 24h 窗口。
  同时 runner 判据只数**真正领到活**的迭代（dur>=20s）——3 秒就退出的
  空转探测不是失败，之前 80% 的「失败率」全是这类空转。

用法：
  python3 timer_audit.py            # 人读
  python3 timer_audit.py --json     # 机读
  python3 timer_audit.py --quiet    # 只在有失败时输出（给 timer 用）
"""
import json
import os
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER_LOG = "/opt/embytg-runner/logs/loop.log"
STATE = os.path.join(HERE, ".timer_audit.json")
FAIL_RE = re.compile(r"Failed with result '([^']+)'")
REASON_RE = re.compile(r"reason=([a-z_]+)")
CURSOR_RE = re.compile(r"cursor:\s*(\S+)")


def _sh_rc(cmd, timeout=60):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                           timeout=timeout)
        return (r.stdout or ""), r.returncode
    except Exception:                                       # noqa: BLE001
        return "", 127


def _sh(cmd, timeout=60):
    return _sh_rc(cmd, timeout)[0]


def journal_cursor(unit):
    """单元当前 journal 游标；拿不到返回空串。"""
    out = _sh("journalctl -u %s -n0 --show-cursor --no-pager -o cat 2>/dev/null"
              % shlex.quote(unit), timeout=15)
    m = CURSOR_RE.search(out)
    return m.group(1) if m else ""


def _failure_lines(txt):
    """返回 (失败次数, 最后一条原因)。失败计数只看 systemd 的 result 行。"""
    fails = FAIL_RE.findall(txt)
    last = ""
    for line in reversed(txt.splitlines()):
        if "Failed with result" in line or "error" in line.lower():
            last = line.strip()[:160]
            break
    return len(fails), last


def service_failures(hours=24, cursors=None, since_ts=None):
    """每个 embytg 单元**自上次巡检以来**的失败次数与最后一条原因。

    取数优先级：
      1. 该单元上次记下的 journal cursor（精确，不受 24h 窗口限制）
      2. 上次巡检的时间戳（首次升级后没有 cursor 的迁移路径）
      3. 近 hours 小时（全新安装）
    窗口内的历史失败数另存 window_fails，只作背景，不触发告警。
    """
    cursors = cursors or {}
    units = [u.strip() for u in _sh(
        "systemctl list-units 'embytg-*' --all --no-legend --no-pager "
        "| awk '{print $1}'").splitlines() if u.strip()]
    out = []
    new_cursors = {}
    for u in units:
        window = _sh("journalctl -u %s --since '-%dh' --no-pager -o cat"
                     % (shlex.quote(u), hours), timeout=30)
        window_fails, last = _failure_lines(window)

        new_txt, mode = "", "window"
        cur = cursors.get(u)
        if cur:
            new_txt, rc = _sh_rc("journalctl -u %s --after-cursor=%s "
                                 "--no-pager -o cat"
                                 % (shlex.quote(u), shlex.quote(cur)))
            # cursor 可能因 journal 轮转失效 → 退回时间判据，不要静默漏报
            mode = "cursor" if rc == 0 else ""
        if mode != "cursor" and since_ts:
            new_txt = _sh("journalctl -u %s --since '@%d' --no-pager -o cat"
                          % (shlex.quote(u), int(since_ts)), timeout=30)
            mode = "since"
        if mode not in ("cursor", "since"):
            mode = "window"

        if mode == "window":
            new_fails, last_new = window_fails, last
        else:
            new_fails, last_new = _failure_lines(new_txt)

        new_cursors[u] = journal_cursor(u) or cursors.get(u, "")

        if not new_fails and not window_fails:
            continue
        out.append({"unit": u, "fails": new_fails, "window_fails": window_fails,
                    "mode": mode, "last": last_new or last})
    out.sort(key=lambda x: (-x["fails"], -x["window_fails"]))
    return out, new_cursors


def runner_stats(since_ts=None):
    """loop.log 里最近 24h 的退出码与原因分布。

    判据用 **EXIT 行**（真领到活、真跑完的单）而不是 [loop] idle 行：
    空转探测的 rc=1 表示 no_job/no_session/source_budget，是调度状态不是失败；
    旧版把这些一起计数，得出「80% 失败率」的假信号。
    since_ts 之后的失败另记 job_fail_new —— 巡检只对新失败告警。

    2026-11-24（E202）：**没有领到活的 EXIT 也不是失败。**
    上一版仍把 `[slot3] NOJOB … IDLE_WITH_ERRORS` 后的 `EXIT rc=1` 当成整单失败，
    于是「三个号都被租约占住、一条 lane 领不到单」这种调度状态又变成告警
    （06:49 的「整单失败×5」里 4 条就是它）。现在按 slot 记账：
    只有该 slot 自上次 EXIT 以来出现过 CLAIMED（真领到活）且 rc≠0 才算失败。

    另加两条**静默判据**（这是 2026-11-24 停摆 8h 没人知道的直接原因）：
      · last_exit_ts：最后一次 EXIT 距今多久 —— 一条 lane 八小时没跑完过任何单，
        日志里不会出现任何错误行，只能靠「时间」判定。
      · lock_blocked：`runner slot N already active` 的刷屏次数 —— 心跳泄漏
        持有 slot 锁的指纹。
    """
    if not os.path.exists(RUNNER_LOG):
        return {"exists": False}
    cutoff = time.time() - 24 * 3600
    job_rc0 = job_rc1 = 0
    job_fail_new = job_done_new = 0
    idle_rc1 = 0
    idle_exit_rc1 = 0
    lock_blocked = 0
    last_exit_ts = 0
    log_start_ts = 0
    claimed = {}                                 # slot -> 自上次 EXIT 以来是否领到活
    reasons = {}
    try:
        with open(RUNNER_LOG, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if "already active" in line:
                    # 这些行是 run_one 抢锁失败时 echo 的，**不带时间戳**，
                    # 只能计总数、由巡检比对上一轮的总数得出增量。
                    lock_blocked += 1
                    continue
                if line[:2] != "20":
                    continue
                try:
                    ts = time.mktime(time.strptime(line[:19], "%Y-%m-%d %H:%M:%S"))
                except Exception:                            # noqa: BLE001
                    continue
                if not log_start_ts:
                    log_start_ts = int(ts)
                if ts < cutoff:
                    continue
                m = REASON_RE.search(line)
                if m:
                    reasons[m.group(1)] = reasons.get(m.group(1), 0) + 1
                sm = re.search(r"\[slot(\d+)\]", line)
                slot = sm.group(1) if sm else ""
                if slot and "CLAIMED" in line:
                    claimed[slot] = True
                em = re.search(r"\bEXIT\s+\S+\s+rc=(\d+)", line)
                if em:
                    rc = int(em.group(1))
                    got_work = claimed.get(slot, False)
                    claimed[slot] = False
                    last_exit_ts = max(last_exit_ts, int(ts))
                    if rc == 0:
                        job_rc0 += 1
                    else:
                        job_rc1 += 1
                        if not got_work:
                            idle_exit_rc1 += 1
                    if since_ts and ts >= since_ts:
                        job_done_new += 1
                        if rc != 0 and got_work:
                            job_fail_new += 1
                    continue
                if "[loop] idle rc=" in line and " rc=1 " in line:
                    idle_rc1 += 1
    except Exception:                                       # noqa: BLE001
        return {"exists": True, "error": "read_failed"}
    silent_from = last_exit_ts or log_start_ts
    return {"exists": True, "rc0": job_rc0, "rc1": job_rc1,
            "job_fail_new": job_fail_new, "job_done_new": job_done_new,
            "idle_rc1": idle_rc1, "idle_exit_rc1": idle_exit_rc1,
            "lock_blocked": lock_blocked, "last_exit_ts": last_exit_ts,
            "log_start_ts": log_start_ts,
            "silent_sec": int(time.time() - silent_from) if silent_from else None,
            "reasons": dict(sorted(reasons.items(), key=lambda x: -x[1])[:8])}


def _load_state():
    try:
        with open(STATE, encoding="utf-8") as fh:
            return json.load(fh) or {}
    except Exception:                                       # noqa: BLE001
        return {}


def _paused():
    """人为暂停期间「没产出」是设计内的，不能当停摆报。"""
    for name in (".gha_paused", ".gha_paused_new", ".gha_paused_all"):
        if os.path.exists(os.path.join(HERE, name)):
            return True
    return False


SILENCE_SEC = int(os.environ.get("TIMER_AUDIT_SILENCE_SEC", 2 * 3600))
LOCK_STUCK_NEW = int(os.environ.get("TIMER_AUDIT_LOCK_SPAM", 40))


def main():
    as_json = "--json" in sys.argv
    quiet = "--quiet" in sys.argv
    prev = _load_state()
    cursors = prev.get("cursors") if isinstance(prev.get("cursors"), dict) else {}
    since_ts = prev.get("ts")
    svc, new_cursors = service_failures(cursors=cursors, since_ts=since_ts)
    run = runner_stats(since_ts=since_ts)
    prev_run_raw = prev.get("runner")
    prev_run = prev_run_raw if isinstance(prev_run_raw, dict) else {}

    # 判据：自上次巡检以来有**新增**的单元失败或真·整单失败（领到活却 rc≠0）。
    # 24h 的历史失败只作背景：修好的事故不该每天重报一次。
    new_fail = sum(x["fails"] for x in svc)
    job_fail_new = int(run.get("job_fail_new") or 0)
    rc1 = int(run.get("rc1") or 0)
    rc0 = int(run.get("rc0") or 0)
    bad_ratio = (rc1 / max(1, rc0 + rc1)) if (rc0 + rc1) >= 5 else 0.0

    # 静默判据：一条 lane 连续 N 小时没跑完过任何单 —— 日志里一个错误行都没有。
    silent_sec = run.get("silent_sec")
    paused = _paused()
    silent = (isinstance(silent_sec, int) and silent_sec >= SILENCE_SEC and not paused)

    # 锁卡死指纹：run_one 抢不到 slot 锁而空转的次数在两次巡检之间暴涨。
    cur_lock = int(run.get("lock_blocked") or 0)
    prev_lock = int(prev_run.get("lock_blocked") or 0)
    if "lock_blocked" not in prev_run:
        # 首次带这条判据运行（旧状态没有基线）→ 只记基线，不拿历史累计数当增量报警
        lock_new = 0
    else:
        lock_new = cur_lock - prev_lock
        if lock_new < 0:                 # 日志轮转 → 增量不可比，按全部算
            lock_new = cur_lock
    lock_stuck = lock_new >= LOCK_STUCK_NEW and not paused

    problems = bool(new_fail) or bool(job_fail_new) or silent or lock_stuck

    report = {"ts": int(time.time()), "services": svc, "runner": run,
              "new_fail": new_fail, "problems": problems,
              "silent": silent, "lock_stuck": lock_stuck,
              "lock_new": lock_new, "lock_total": cur_lock,
              "cursors": new_cursors}

    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=1))
    elif not quiet or problems:
        print("== 自上次巡检以来的新失败 ==")
        if not new_fail:
            print("   无新增（24h 窗口内历史失败 %d 条，属已记录事故）"
                  % sum(x["window_fails"] for x in svc))
        for x in svc[:8]:
            if not x["fails"]:
                continue
            print("   %-32s %3d 次 (窗口 %d) | %s"
                  % (x["unit"], x["fails"], x["window_fails"], x["last"][:70]))
        print("== runner 退出与原因 ==")
        print("   24h 整单 rc=0 %d | rc=1 %d（其中空手 %d，不计失败）| 失败占比 %.0f%%"
              % (rc0, rc1, int(run.get("idle_exit_rc1") or 0), bad_ratio * 100))
        print("   上次巡检后整单 %d 个，其中失败 %d 个；空转探测 rc=1 %d 次（不计）"
              % (int(run.get("job_done_new") or 0), job_fail_new,
                 int(run.get("idle_rc1") or 0)))
        if silent_sec is not None:
            print("   距最后一次整单收尾 %d 分钟（阈值 %d 分钟）%s"
                  % (silent_sec // 60, SILENCE_SEC // 60,
                     " ← 静默！" if silent else ""))
        print("   抢锁失败累计 %d 次（本轮 +%d，阈值 %d）%s"
              % (cur_lock, lock_new, LOCK_STUCK_NEW,
                 " ← slot 锁可能被泄漏进程占住" if lock_stuck else ""))
        _rs = run.get("reasons")
        if not isinstance(_rs, dict):
            _rs = {}
        for k, v in _rs.items():
            print("   %-24s %d" % (k, v))

    try:
        with open(STATE, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=1)
    except Exception:                                       # noqa: BLE001
        pass

    if problems:
        try:
            sys.path.insert(0, HERE)
            import alert as A
            top = ", ".join("%s×%d" % (x["unit"].replace(".service", ""), x["fails"])
                            for x in svc[:4] if x["fails"])
            if job_fail_new:
                top = (top + "，" if top else "") + "整单失败×%d" % job_fail_new
            if silent:
                top = (top + "，" if top else "") + "runner 静默 %d 分钟零收尾" % (int(silent_sec or 0) // 60)
            if lock_stuck:
                top = (top + "，" if top else "") + "抢锁空转 +%d 次（slot 锁可能未释放）" % lock_new
            code = "timer_silent" if (silent or lock_stuck) and not (new_fail or job_fail_new) else "timer_audit"
            A.alert("P1" if (silent or lock_stuck) else "warn", code,
                    "定时任务异常：%s" % top)
        except Exception:                                   # noqa: BLE001
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
