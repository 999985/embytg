#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""claim_reap.py —— 僵尸 claim 回收（无人值守必备）

## 为什么需要它

`run_one.sh` 由 systemd 以 `TimeoutStartSec=7200`（2 小时）运行。超时会被 kill，
但 **claim 文件不会被释放** —— 于是那一集永远挂在 `status=running`，
该 lane 每分钟都在等一个永远不会完成的 claim。

2026-11-13 实测一例：`new4` 的「加油吧威基基 S01E07」（5.1 GB）
· claim 文件 mtime 停在 20:32，跑的时候已经 00:10（3.6 小时）
· 目标文件不存在、tg.db 无记录、全盘没有对应的 `.part`
· 该 lane 因此空转 3.6 小时 = 五分之一产能丢失
· 而 `daily_ops` 报「卡单 0」——因为判据读的是 `started_ts` 字段，**那个字段根本不存在**，
  `d.get('started_ts')` 恒为 None，age 被算成 0 分钟。假判据比没有判据更糟。

## 判据（必须可证明，不能拍脑袋）

**主判据 = 服务超时上限 + 余量。**
`TimeoutStartSec=7200s`，所以任何 `running` 状态超过 **150 分钟** 的 claim
在数学上不可能是活的（进程早被 systemd 杀了）。默认阈值就取 150 分钟。

辅助判据（`--aggressive` 才用，45 分钟）：claim 文件 mtime 老、且看不到下载证据
（无 `.part`、无相关进程）。这条对本机 lane 可靠，对云盒 lane **不可靠**——
云盒的 `.part` 在本机看不见，所以默认不用它，避免误杀正在下载的 5GB 大文件。

## 用法

```
python3 claim_reap.py              # 只报（默认）
python3 claim_reap.py --apply      # 释放超时的
python3 claim_reap.py --aggressive --apply   # 也用 45 分钟 + 下载证据判据
```

释放走 `gha_cancel.py`，与人工操作同一条路径（不绕过防重，不改 claim 内容）。
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CLAIMS = os.path.join(HERE, ".gha_claims")
RUNNER_SERVICE_TIMEOUT_S = 7200      # 见 /etc/systemd/system/embytg-runner.service
SAFE_AGE_MIN = RUNNER_SERVICE_TIMEOUT_S / 60 + 30     # 150 分钟
AGGRESSIVE_AGE_MIN = 45
# 非 slot1 的本机槽位（slot2/3/4）早期回收年龄：实测一集 6.6 GB 的上传要 40 分钟，
# 但这三种判据同时成立时才用（无 work 目录 + 无进程引用 run_id），真在跑的上传必有 work 目录。
EARLY_REAP_AGE_MIN = {"1": 5, "2": 30, "3": 30, "4": 30}
EARLY_REAP_DEFAULT_MIN = 30


def local_slots():
    """本机 systemd 上真实存在的 runner 槽位（字符串集合）。

    为什么不能硬编码：`NODES.md §2` 早已写明「slot 1/2/3/4 都是**本机锁槽**，
    不是机器编号」。2026-11-16 修根因时却看到判据里写的是「slot1 = 本机，
    slot2–5 = 云盒」——那是云盒还在服役时的假设。云盒 2026-11-15 全部下线后，
    slot2/slot3 由 `ensure_lanes.sh` 在本机拉起，于是 `slot3` 的 `lane=new`
    僵尸 claim 既不算「空壳」也不算「无进程」，一直挂到 150 分钟超时：
    实测 `20261115_205847-slot3-new-239e64`（半是蜜糖 S01E10）从 04:58 挂到
    07:12，132 分钟内该槽位不接新活。

    现在改成从 systemd 实际在管哪些槽位推导 —— 加一个槽位不需要改这段代码。
    """
    slots = set()
    try:
        out = subprocess.run(["systemctl", "list-units", "--all", "--no-legend",
                              "embytg-runner*"], capture_output=True, text=True,
                             timeout=10).stdout
    except Exception:                                        # noqa: BLE001
        return slots
    for ln in out.splitlines():
        m = re.search(r"embytg-runner@(\d+)\.service", ln)
        if m:
            slots.add(m.group(1))
            continue
        m = re.search(r"embytg-runner(\d*)\.service", ln)     # 专属 unit：runner=slot1 / runner4=slot4
        if m:
            slots.add(m.group(1) or "1")
    return slots


def _claim_slot(claim):
    m = re.search(r"-slot(\d+)-", str(claim.get("run_id") or ""))
    return m.group(1) if m else ""


def is_local_claim(claim):
    """这份 claim 是不是**本机**消费的（本机看不到进程/目录才说明它死了）。

    云盒 era 的兜底：run_id 里没有 slot 段、或 slot 不在本机在管的槽位集合里
    （例如个人 PC 间歇上片时的 claim），一律按「本机看不见 ≠ 它死了」处理。
    """
    rid = str(claim.get("run_id") or "")
    slot = _claim_slot(claim)
    if slot:
        return slot in _LOCAL_SLOTS
    lane = claim.get("lane") or ""
    return "slot1" in rid or lane in ("new2", "fill", "upgrade")


_LOCAL_SLOTS = local_slots()


def claim_age_min(path):
    """用文件 mtime 算年龄。**不要用 claim 里的 timestamp 字段** ——
    那些字段（如 started_ts）不一定存在，缺字段会算成 0 分钟（假活）。"""
    try:
        return (time.time() - os.path.getmtime(path)) / 60.0
    except OSError:
        return -1


def download_evidence(claim):
    """本机上是否有它的下载痕迹（.part 或相关进程）。云盒上的不可见 → 只作辅助判据。"""
    name = os.path.basename(claim.get("target_rel") or claim.get("name") or "")
    stem = os.path.splitext(name)[0]
    if stem:
        for pat in ("/opt/embytg/**/%s*.part" % stem,
                    "/opt/embytg-runner/**/%s*.part" % stem,
                    "/tmp/**/%s*.part" % stem):
            if glob.glob(pat, recursive=True):
                return "有 .part"
    run_id = claim.get("run_id") or ""
    # 2026-11-15 修：**上传阶段也必须算「活着」。**
    # 原来只 pgrep `run_one.sh|pc_upload|gha_download`，漏了 gha_upload.py ——
    # 一个已经下完、正在慢慢上传的 claim（TG 限速 0.3 MB/s 时单集要 1 小时+）
    # 在超时判据看来「无下载痕迹」，150 分钟一到就被回收，gha_cancel 直接
    # 杀掉正在跑的 gha_upload（实测 辐射 E2 190/214 段时差点被砍）。
    # 判据：只要有进程命令行里带这个 run_id，就算活着，时间阈值不适用。
    try:
        out = subprocess.run(["pgrep", "-af",
                              "run_one.sh|pc_upload|gha_download|gha_upload|gha_finish"],
                             capture_output=True, text=True, timeout=15).stdout
    except Exception:                                       # noqa: BLE001
        out = ""
    if run_id and run_id in out:
        return "有进程引用 run_id"
    return ""


WORK_ROOT = "/opt/embytg-runner/work"


def no_runner_process(claim):
    """本机上根本没有任何 runner/上传进程在跑 → 这个 claim 必然是僵尸。

    2026-11-14（E082）：僵尸 claim 的真正来源是 `cancel_claim` 走 SSH 失败后
    被 `|| true` 吞掉（现在已改成直连 + 重试 + 显式失败记录）。
    但**兜底判据必须有**：只要 claim 在、进程全没了，它就一定死了，
    不该等 150 分钟的超时上限。

    注意：云盒的 claim 由云盒上的进程消费，本机看不到 →
    所以只对**本机 lane** 用这条判据（l它的 run_id 前缀带 slot1）。
    """
    rid = claim.get("run_id") or ""
    if not is_local_claim(claim):
        return False                        # 非本机槽位（如个人 PC 间歇上片），本机看不见进程，跳过
    try:
        out = subprocess.run(
            ["pgrep", "-af", "run_one.sh|gha_download.py|gha_upload.py|pc_upload.py|gha_finish.py"],
            capture_output=True, text=True, timeout=15).stdout
    except Exception:                       # noqa: BLE001
        return False
    live = [ln for ln in out.splitlines() if ln.strip()]
    if not live:
        return True                         # 一个都没有 → 全死光
    return rid not in out


def no_work_dir(claim):
    """本机上它的 work 目录根本不存在 → 从未真正开跑，可安全回收。

    2026-11-14：判据原先只看时间（150 分钟），于是「空壳 claim」——
    文件在、但没有 work 目录、没有进程、没有 .part —— 会白占一个 session
    槽位 2.5 小时，其他节点全部排队。实测 new5 的 claim 挂了 36 分钟，
    work 目录压根不存在，claim_reap 却判它「保留」。

    为什么这条对云盒也可靠：work 目录建在**控制面**（Oracle）的
    /opt/embytg-runner/work/<run_id>/，云端只负责拉流上传，
    与「.part 文件在云盒上看不见」是两回事。
    """
    rid = claim.get("run_id") or ""
    lane = claim.get("lane") or ""
    if not rid:
        return False                       # 没有 run_id 就没法判断，别误杀
    # 2026-11-15 修（根因，实测）：云盒（new3/new4/new5）的 work 目录建在**云盒本地**
    # /opt/embytg-runner/work/<run_id>/，本机查不到 → 以前对云盒恒判「空壳」。
    # 实测 box3 上 20261114_164555-slot3-new5-837505 正在 gha_upload.py 上传（进程 35696），
    # 而控制面 no_work_dir 判它空壳 → 被 claim_reap 反复杀掉，白烧一轮下载+46 次中止。
    # 判据必须与 no_runner_process 同口径：只看本机能证明的东西。
    if not is_local_claim(claim):
        return False                       # 非本机槽位的 work 目录不在本机，不适用
    return not os.path.isdir(os.path.join(WORK_ROOT, rid))


def main():
    # 条目级 claim 的过期清理（2026-11-20）：TTL 到期的 key 必须放回去，
    # 否则某一集会被永久锁住、谁也领不了。
    try:
        import gha_state as _GS
        _n = _GS.reap_items()
        print("  条目级 claim 过期清理: %d 条" % _n)
    except Exception as _e:                                   # noqa: BLE001
        print("  claim 清理跳过: %s" % str(_e)[:50])
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--aggressive", action="store_true")
    ap.add_argument("--age-min", type=float, default=0)
    a = ap.parse_args()

    print("  本机槽位=%s（systemd 实际在管；slot 是锁槽不是机器编号）"
          % (",".join(sorted(_LOCAL_SLOTS)) or "?（systemctl 查询失败，只按时间判）"))

    limit = a.age_min or (AGGRESSIVE_AGE_MIN if a.aggressive else SAFE_AGE_MIN)
    reaped = 0
    checked = 0
    for f in sorted(glob.glob(os.path.join(CLAIMS, "*.json"))):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:                                   # noqa: BLE001
            continue
        checked += 1
        if (d.get("status") or "") != "running":
            continue
        age = claim_age_min(f)
        ev = download_evidence(d)
        series = (d.get("series") or "")[:20]
        ep = d.get("ep")
        tag = "%-8s %s E%s" % (os.path.basename(f), series, ep)

        too_old = age > limit
        empty = no_work_dir(d)
        orphan = no_runner_process(d)
        # 本机槽位的早期回收年龄（2026-11-16：slot2/3/4 也是本机槽，但一集 6.6 GB
        # 上传要 40 分钟，所以它们起步门槛放到 30 分钟；slot1 沿用 5/3 分钟）
        _slot = _claim_slot(d) or "1"
        _early = EARLY_REAP_AGE_MIN.get(_slot, EARLY_REAP_DEFAULT_MIN)
        _empty_age = 5 if _slot == "1" else _early
        _orphan_age = 3 if _slot == "1" else _early
        # 严格模式：只按时间（可证明）；激进模式：时间到了且看不到下载证据
        # 2026-11-15：**有活进程引用 run_id 时，时间阈值不适用。**
        # 「真失败」的特征是没人管、一直在；一个还在上传的 claim 只是慢。
        live = "有进程引用" in ev
        should = too_old and not live and (not a.aggressive or not ev)
        # 2026-11-14：空壳 claim（无 work 目录）不受时间阈值保护 ——
        # 它从来没开跑过，占着 session 槽位只是阻塞别人。
        if empty and age > _empty_age:
            should = True
        # 2026-11-14（E082）：claim 活着但进程全没了 → 立刻回收，不等 150 分钟
        if orphan and age > _orphan_age:
            should = True
        print("  %s age=%.0fmin 阈值=%.0f %s%s%s %s"
              % (tag, age, limit,
                 ("证据:" + ev) if ev else "无下载痕迹",
                 " [无 work 目录]" if empty else "",
                 " [无进程]" if orphan else "",
                 "→ 回收" if should else "→ 保留"))

        if should:
            reaped += 1
        if should and a.apply:
            rid = d.get("run_id") or d.get("job_id")
            # 2026-11-15 修：原来固定文案「超时 Xmin > Ymin，服务上限 120min，必为孤儿」，
            # 但空壳/无进程回收时 age 往往只有 5–13 分钟，日志变成
            # 「超时 13min > 150min」这种自相矛盾的记录，事后没法用它判根因。
            # 现在文案必须说清**是哪条判据触发的**。
            if too_old:
                why = "超时 %.0fmin > %.0fmin（服务上限 120min）" % (age, limit)
            elif orphan:
                why = "无 runner 进程存活（age %.0fmin）" % age
            elif empty:
                why = "空壳 claim（无 work 目录，age %.0fmin）" % age
            else:
                why = "无下载证据且超时（age %.0fmin）" % age
            r = subprocess.run([sys.executable, os.path.join(HERE, "gha_cancel.py"),
                                "--run-id", str(rid),
                                "--reason", "claim_reap: " + why],
                               capture_output=True, text=True, timeout=120)
            ok = '"ok": true' in (r.stdout or "")
            print("     %s %s" % ("✓ 已释放" if ok else "✗ 释放失败", (r.stdout or r.stderr)[:90]))
            if not ok:
                reaped -= 1

    print("  检查 %d 个 claim，%s %d 个"
          % (checked, "已回收" if a.apply else "待回收", reaped))
    if not a.apply:
        print("  （只读模式；加 --apply 执行）")

    # 2026-11-16：**隔离清单要有人回头复核。**
    # 收尾连续失败 5 轮的 claim 被挪进 .gha_claims_quarantine/ 后就没人管了：
    # 根因修好之后，那条 P1 既不会自动收口，也不会升级，事后只能人肉翻 resume.log。
    # 判据见 quarantine_audit.py 头注释（文件/台账/Emby/NFO 四项齐才算好）。
    try:
        import quarantine_audit
        print("== 隔离 claim 复核 ==")
        quarantine_audit.audit(apply=a.apply)
    except Exception as e:                                  # noqa: BLE001
        print("  ! 隔离复核失败 %s: %s" % (type(e).__name__, e))
    return 0


if __name__ == "__main__":
    sys.exit(main())
