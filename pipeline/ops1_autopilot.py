#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ops1_autopilot.py — 运维任务无人值守闭环（2026-11-10 用户长期授权）

用户原话：「你是运维，我要无人值守，你有义务：自动入库，刮削自动纠错，监控与自愈，
服务器磁盘满维护，优化服务器性能，感知 -> 分析 -> 决策 -> 执行，全流程留痕与审计，
我将离开全靠你和另一个运维，你自己为自己做决定」。

【设计】每 15 分钟由 cron 调一次，单实例 flock。一轮 = 感知 → 分析 → 决策 → 执行 → 留痕。
所有「执行」分级，红线绝不跨：

  L0 只读感知：status_now.collect()（现场/对账/队列/备份/loop/日志闭环）+ quota + 磁盘 + shim
  L1 安全自愈（可自动执行，幂等、不改媒体资产、不外部写）：
     - loop 0 实例 → 恢复 auto_finalize.py --loop
     - shim 头行不是 #!/bin/sh → p0_3_selfheal.sh（镜像升级静默失效）
     - LATEST 包缺失/验签失败 → 标记需重打包（不自动停 loop 抢锁）
     - 磁盘分级：>90% 或 余<10G 时按白名单清 **过期缓存**（详见 disk_guard），
       绝不删 mnt/ 媒体、绝不删 backup/ 回滚证据、绝不删运维守护正在写的 .part
     - 性能：prewarm 缓存命中率低于阈值时由既有 cron 自然补（不额外并发）
  L2 刮削自纠（自动，但只补「确实能证明」的，不猜标题）：
     - 缺 NFO 的新片 → nfo_gen.py --kind all --workers 24（幂等 already_done）
     - 逐集链：nfo_episodes*.py / nfo_name_to_emby.py 只在 naming_health 报 Name 不一致时跑
  L3 自动入库（不可逆 TG 云端写，**六道闸门全过才执行，且有每日冷却**）：
     G1 当日工单 open_gate=True（Scan 非 Running、队列空、loop 在跑、日志闭环、配额有余）
     G0 入库总开关 ingest_quota.enabled()（E019：用户 2026-11-11「已启用 GitHub 入库，本机
        不再负责入库」→ ingest_enabled=false。这是**不可逆 TG 写的最后一道闸**，
        以前 G3 只看配额余量、不看开关，导致总闸关掉后 L3 仍会在冷却到期时真灌）
     G2 lib_reconcile gaps=[]（按库区间判据 unique≤emby≤files，见 g2_reconcile_ok；
        不再要求全局 td_files==episode_total —— 那条与多版本合并口径冲突，见 E018）
     G3 ingest_quota --check --need <估算> 通过
     G4 每日最多一次（真灌冷却 20h，**只由真灌消耗**；状态缺失=未知→不放行，需 --force-l3），
        单批 ≤ MAX_ITEMS=30
     G5 运维守护未在拉片（无 .part / pull 日志近期活跃）→ 否则跳过/缩小批次
     G6 先 `tgpipe.py scan --dry` 预演（不写队列、不消耗冷却），再走 START_HERE §2 增量六步；
        失败即熔断（写 state，不再重试）
     ⚠ L3 默认 DRY（安全起步）；要真自动把 config 里 ops1_autopilot.l3_enabled 打开，
       且必须是我审核过一轮 dry 日志后亲手开。用户授权「无人值守」不等于无闸门。
     ⚠ E177（2026-11-18）：**L1/L2 的 dry 与 L3 的 dry 是三件事，不许共用一个开关**。
       以前 `dry = a.dry or DRY_ONLY`（DRY_ONLY 来自 l3_enabled=false，11-11 用户暂停上片后恒 True），
       结果「不上片」把**运维自己的**磁盘自愈(L1)和刮削补缺(L2)一起冻住：
       24h 审计 96/96 轮 `[DRY] 会跑 nfo_gen`、`disk_guard` 只打印不执行，
       NFO 补缺最后一次真跑停在 2026-11-11 18:00。现在 l1_enabled/l2_enabled 独立，
       默认 True；`--dry` 仍是全局只读。

【留痕与审计】每轮每步写 logs/ops1_audit.jsonl（时间/阶段/观测/决策/动作/结果/耗时），
另写 logs/ops1_autopilot.log 人读；重大动作同步 AgentCommunication.log（只在有动作时，
避免 15min 一次刷屏）。state 在 .ops1_autopilot_state.json（冷却/熔断/连续失败）。

【红线（AGENTS/OPS1_MANUAL）】不猜标题；不在 Scan Running 时触发刷新/重启/再扫描；
不看 204 就宣布完成；不无冷快照改 tg.db；不用 git add -A；不手写时间；
目录还在的 Series 不 DELETE /Items/{id}；不碰运维守护源服链路。
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request

PIPE = "/opt/embytg/pipeline"
ROOT = "/opt/embytg"
BK = os.path.join(ROOT, "backup")
LOGDIR = os.path.join(PIPE, "logs")
AUDIT = os.path.join(LOGDIR, "ops1_audit.jsonl")
HLOG = os.path.join(LOGDIR, "ops1_autopilot.log")
STATE = os.path.join(PIPE, ".ops1_autopilot_state.json")
CFG = os.path.join(ROOT, "config.json")

MAX_ITEMS = 30
COOLDOWN_H = 20
# E017：预演与真灌的冷却必须分开。预演（DRY_ONLY / --dry / 六步前的 scan --dry）
# 一个字节都没入库，不能消耗 20h 的真灌冷却，否则「只读巡检」会把当天 L3 关掉。
# 预演只用自己的节流窗口，且只在 DRY_ONLY 的重复预演间生效（省 FUSE 扫描）。
PREVIEW_MIN_INTERVAL_H = 1
# E177：三级各自的开关。以前只有 l3_enabled，且被当成「全机 dry」的总闸：
#   11-11 用户暂停上片（l3_enabled=false）后，L1 磁盘自愈与 L2 刮削补缺也一起被冻住 ——
#   24h 审计 96/96 轮 `[DRY] 会跑 nfo_gen`，`last_l2_ts` 停在 2026-11-11 18:00。
# L1/L2 是运维自己的动作（不上片、不写 TG 云端），与上片暂停令无关，默认放行。
# 只有 `--dry`（人工只读巡检）能让三层全停。
def _cfg_flag(key, default):
    try:
        c = json.load(open(CFG, encoding="utf-8"))
        v = (c.get("ops1_autopilot") or {}).get(key)
        return bool(default if v is None else v)
    except Exception:
        return bool(default)


def l3_enabled():
    # 默认 false：真入库是不可逆 TG 云端写，读不到配置就当「不许灌」
    return _cfg_flag("l3_enabled", False)


L1_ENABLED = _cfg_flag("l1_enabled", True)
L2_ENABLED = _cfg_flag("l2_enabled", True)
DRY_L3 = not l3_enabled()
DRY_L1 = not L1_ENABLED
DRY_L2 = not L2_ENABLED
# 兼容旧名（日志/外部脚本可能引用）：DRY_ONLY == 「L3 只预演」
DRY_ONLY = DRY_L3
CACHE = os.path.join(ROOT, "cache")
DISK_WARN_PCT = 80       # 13:12 实测 82%/余18G；85% 太宽，等不到自愈就满了
DISK_CRIT_PCT = 88
MIN_FREE_MB = 15000      # 余<15G 即介入（disk_watch 的 12G 是最后防线，autopilot 要更早）

sys.path.insert(0, PIPE)
import status_now  # noqa: E402  单一状态入口，口径不分叉
import ingest_quota  # noqa: E402  E019：入库总开关的唯一真源（别在这里再读一份 json）


def ingest_switch():
    """E019：入库总开关。返回 (enabled, why)；读不到 = 未知 → 按「停用」处理（fail-closed）。

    用户 2026-11-11：改用 GitHub Actions 上片，「本机（甲骨文）不再负责入库」
    → `.ingest_budget.json` 的 ingest_enabled=false。但 G3 只问「配额还有余量吗」，
    停用当天 `--check --need 1` 依然返回 rc=0「✅ 可以下 1.00 GB」→ 冷却一到期
    L3 就会真的往 TG 云转发（不可逆）。总开关必须真的能关掉动作，不是只关掉文案。
    """
    try:
        ok = bool(ingest_quota.enabled())
        return ok, ("ingest_enabled=true" if ok else "ingest_enabled=false(GitHub 接管)")
    except Exception as e:                       # 开关读不到 → 不许猜「开」
        return False, "开关读取失败:%s" % str(e)[:60]



def now():
    return time.strftime("%F %T %Z")


def sh(cmd, timeout=120):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return -9, "", "TIMEOUT"
    except Exception as e:
        return -1, "", str(e)[:200]


def audit(phase, observe, decision, action="", result="", t0=None):
    rec = {"ts": now(), "phase": phase, "observe": observe,
           "decision": decision, "action": action, "result": result,
           "elapsed_s": round(time.time() - t0, 2) if t0 else None}
    os.makedirs(LOGDIR, exist_ok=True)
    with open(AUDIT, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    line = "[%s] %-6s | %s | -> %s | %s" % (
        rec["ts"], phase, str(observe)[:110], str(decision)[:90], str(result)[:90])
    with open(HLOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)
    return rec


def load_state():
    try:
        return json.load(open(STATE, encoding="utf-8"))
    except Exception:
        return {"last_l3_ts": 0, "l3_fails": 0, "l3_fused": False, "cycles": 0}


def save_state(s):
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=1)


def last_l3_from_audit(max_lines=20000):
    """E017 自愈：state 丢失/首次运行时，从审计里回填「上次真灌」时间。

    返回 (ts 或 None, 来源说明)。三种结论必须分开，不能混成一个 None：
      - 找到真灌记录 → 用它的时间做冷却基准（不 fail-open）；
      - 审计可读但一条真灌都没有 → 证据表明「从未真灌过」→ 冷却可以放行（不会同日两灌）；
      - 审计读不到 → 真正的「未知」→ 调用方 fail-closed（需 --force-l3）。
    """
    try:
        with open(AUDIT, encoding="utf-8") as f:
            lines = f.readlines()[-max_lines:]
    except Exception as e:
        return None, "审计不可读:%s" % str(e)[:40]
    best = None
    for l in lines:
        try:
            r = json.loads(l)
        except Exception:
            continue
        if r.get("phase") == "执行" and str(r.get("result", "")).startswith("L3 真入库"):
            try:
                ts = time.mktime(time.strptime(r["ts"][:19], "%Y-%m-%d %H:%M:%S"))
            except Exception:
                continue
            best = ts if best is None else max(best, ts)
    if best:
        return best, "审计回填"
    return None, "审计无真灌记录"


def g2_reconcile_ok(rec):
    """E018：G2 用 lib_reconcile 的**规范判据**（按库区间 unique ≤ emby ≤ files）。

    旧口径在 E016 的 fail-closed 之上又加了全局 `td_files == episode_total`，这是错的：
      - 与 lib_reconcile 自己的 converged 定义冲突：多版本合并（1080p+2160p 同集）时
        files > emby 属**正常**（SCALING §4 明确留余地），全局等式一出现就永久挡住 L3
        = 结构性假闭环（闸门永远不过，脚本 15min 一轮却什么也不做）；
      - 它还暗含「所有 Episode 都挂在 /mnt/embytg/TV/ 下」的假设（episode_total 是
        Emby 全库 Type=8 计数）。
    现在只认 lib_reconcile 自己给出的 gaps（= libs_with_gap），它与 status_now /
    ops1_daily / ops1_selfcheck 三处口径**同一来源**，四处不再分叉。

    仍然 fail-closed（这是 E016 的底线，不能倒退）：
      - gaps is None（lib_reconcile 失败/并发冲突）→ 不过；
      - gaps 非空 → 不过；
      - td_files/episode 缺失、非数值或 ≤0（teldrive CTE 没真返回东西）→ 不过。
    返回 (ok, why)；why 会进审计 gate_detail，让「为什么没过」自带数字。
    """
    if not isinstance(rec, dict):
        return False, "reconcile 缺失"
    gaps = rec.get("gaps")
    if gaps is None:
        return False, "对账未知(gaps=None, err=%s)" % (rec.get("err") or "-")
    try:
        td = int(rec.get("td_files"))
        ep = int(rec.get("episode"))
    except (TypeError, ValueError):
        return False, "td/ep 非数值(td=%r ep=%r)" % (rec.get("td_files"), rec.get("episode"))
    if gaps != []:
        return False, "gaps=%s" % (gaps,)
    if td <= 0 or ep <= 0:
        return False, "空对账(td=%d ep=%d)不可放行" % (td, ep)
    return True, "gaps=[] td=%d ep=%d(区间判据)" % (td, ep)


def loop_pids():
    import glob
    out = []
    for p in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            a = [x.decode("utf8", "ignore") for x in open(p, "rb").read().split(b"\0") if x]
        except Exception:
            continue
        if (any(x.endswith("auto_finalize.py") for x in a) and "--loop" in a
                and not any(x in ("-c", "sh", "bash") for x in a[:2])):
            out.append(int(p.split("/")[2]))
    return out


def disk_info():
    rc, out, _ = sh("df -m / | tail -1", 10)
    try:
        parts = out.split()
        total, used, avail = int(parts[1]), int(parts[2]), int(parts[3])
        pct = int(parts[4].rstrip("%"))
        return {"total_mb": total, "used_mb": used, "avail_mb": avail, "pct": pct}
    except Exception:
        return {"avail_mb": 999999, "pct": 0}


def dir_mb(path):
    rc, out, _ = sh("du -sm %s 2>/dev/null | cut -f1" % path, 60)
    try:
        return int(out)
    except Exception:
        return 0


def active_pulls():
    """运维守护是否正在从源服拉片（.part / pull 日志近 10 分钟活跃）。"""
    act = []
    for p in (os.path.join(ROOT, "tmp/*.part"), os.path.join(LOGDIR, "pull_*.log"),
              os.path.join(PIPE, "_trash_*/*.part")):
        import glob
        for f in glob.glob(p):
            try:
                if time.time() - os.path.getmtime(f) < 600:
                    act.append(os.path.basename(f))
            except OSError:
                pass
    return act


def nfo_gen_running():
    """§7.4：标题类任务同一时间只许一个（nfo_gen 会写同一批 NFO+poster）。
    E177 起 L2 会真跑，必须自己判重，不能指望人工盯着。"""
    try:
        _, out, _ = sh("ps -eo args | grep '[n]fo_gen.py' | grep -v 'ops1_autopilot' | head -3", 10)
        return bool(out.strip())
    except Exception:
        return True          # 判不了 → 当作在跑（不抢）


def disk_guard(dinfo, dry):
    """磁盘分级自愈：只清**确定可再生的运行时缓存**，绝不碰媒体/备份/在写的 .part。

    白名单（按风险从低到高）：
      1) rclone 日志（>200MB 才截断，保留尾部）
      2) cache/vfs 里 mtime>1h 且已上传完（rclone `to upload 0` 时）的 .nfo 残件
      3) cache/vfs_pending_* / vfsMeta_pending_* 历史迁移残留（>1天）
      4) cache/transcode 旧转码（>1天）
    绝不删：mnt/（媒体在 TG 挂载）、backup/（回滚证据）、cache/varc（运维守护拉片缓存，
      由源服任务自己管；只在 >90% 时对**超过 6h 未动**的孤块做告警，不自动删）
    """
    acts = []
    if dinfo["pct"] < DISK_WARN_PCT and dinfo["avail_mb"] > MIN_FREE_MB:
        return acts, ("磁盘正常，无需自愈" if not dry else "磁盘正常，无需自愈(DRY)")
    level = "CRIT" if (dinfo["pct"] >= DISK_CRIT_PCT or dinfo["avail_mb"] < MIN_FREE_MB) else "WARN"
    acts.append("level=%s pct=%d%% avail=%dMB" % (level, dinfo["pct"], dinfo["avail_mb"]))
    # 1) rclone 日志截断
    # E177：旧写法 `tail -c 20M > tmp && mv tmp rl` 换掉了 inode，rclone 仍写旧 inode
    # → 空间一个字节都没释放，而审计写着「rclone.log 截断到20M」= 假自愈。
    # 现在原地回写同一 inode（cat > 文件 = O_TRUNC 后写入），空间真的释放。
    rl = os.path.join(CACHE, "rclone-mount.log")
    try:
        if os.path.getsize(rl) > 200 * 1048576:
            if not dry:
                rc, _, _ = sh("tail -c 20M %s > %s.tmp && cat %s.tmp > %s && rm -f %s.tmp"
                              % (rl, rl, rl, rl, rl), 120)
                acts.append("rclone.log 原地截断到20M rc=%d (now=%dMB)"
                            % (rc, os.path.getsize(rl) // 1048576))
            else:
                acts.append("[DRY] 会截断 rclone.log 到20M")
    except OSError:
        pass
    # 2) vfs .nfo 残件（确认 rclone 上传队列空）
    _, cleaned, _ = sh("tail -3 %s 2>/dev/null | grep 'vfs cache: cleaned' | tail -1" % rl, 10)
    queue_empty = ("to upload 0" in cleaned and "uploading 0" in cleaned)
    import glob
    stale = [f for f in glob.glob(os.path.join(CACHE, "vfs", "**", "*.nfo"), recursive=True)
             if os.path.isfile(f) and time.time() - os.path.getmtime(f) > 3600]
    if queue_empty and stale:
        if not dry:
            n = 0
            for f in stale:
                try:
                    os.remove(f); n += 1
                except OSError:
                    pass
            acts.append("清 vfs 过期 .nfo 残件 %d 个" % n)
        else:
            acts.append("[DRY] 会清 vfs 过期 .nfo 残件 %d 个" % len(stale))
    # 3) 历史 pending 残留（>1天）
    olddirs = glob.glob(os.path.join(CACHE, "vfs_pending_*")) + glob.glob(os.path.join(CACHE, "vfsMeta_pending_*"))
    for d in olddirs:
        try:
            if os.path.isdir(d) and time.time() - os.path.getmtime(d) > 86400:
                acts.append(("[DRY] 会清 " if dry else "清 ") + "历史残留目录 %s (%dMB)" % (os.path.basename(d), dir_mb(d)))
                if not dry:
                    sh("rm -rf '%s'" % d, 60)
        except OSError:
            pass
    # 4) varc（运维守护拉片缓存）：WARN 只告警；CRIT 才清 >6h 未动的孤儿块，
    #    绝不碰近 6h 在写的 .part/数据块（那是它正在拉的片）。
    varc_dir = os.path.join(CACHE, "varc")
    varc = dir_mb(varc_dir)
    if level == "CRIT":
        import glob as _g
        old = []
        for f in _g.glob(os.path.join(varc_dir, "**", "*"), recursive=True):
            try:
                if os.path.isfile(f) and time.time() - os.path.getmtime(f) > 6 * 3600:
                    old.append(f)
            except OSError:
                pass
        freed = sum(os.path.getsize(f) for f in old) / 1048576
        if not dry and old:
            for f in old:
                try:
                    os.remove(f)
                except OSError:
                    pass
        acts.append("%svarc >6h 孤儿块 %d 个 / %.0fMB（近6h在写的一律保留）"
                    % ("[DRY] 会清 " if dry else "清 ", len(old), freed))
    else:
        acts.append("cache/varc=%dMB（运维守护资产，WARN只告警；CRIT才清>6h孤块）" % varc)
    return acts, ("disk_guard 完成" if not dry else "disk_guard DRY（未执行，仅报告）")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="只感知+决策，不执行任何动作（含 L1）")
    ap.add_argument("--force-l3", action="store_true",
                    help="只忽略每日冷却跑一次 L3 判定；G0 入库总开关不可绕过（2026-11-11 用户暂停上片）")
    # E018：main() 末尾按 "--json" in sys.argv 决定输出机器可读摘要，但 argparse 之前
    # 不认识这个参数 → 一加 --json 就 "unrecognized arguments" 退出（自证手段用不了）。
    ap.add_argument("--json", action="store_true", help="输出本轮机器可读摘要（给自醒/巡检看）")
    a = ap.parse_args()
    # E177：三层各用各的 dry。--dry 仍是全局只读（人工巡检/自醒验证用）。
    dry_all = a.dry
    dry_l1 = dry_all or DRY_L1
    dry_l2 = dry_all or DRY_L2
    dry_l3 = dry_all or DRY_L3
    t0 = time.time()
    st = load_state()
    st["cycles"] = st.get("cycles", 0) + 1

    # ---------------- 感知 ----------------
    d = status_now.collect()
    dinfo = disk_info()
    pulls = active_pulls()
    rcq, qout, _ = sh("/usr/bin/python3 %s/ingest_quota.py --check --need 1" % PIPE, 30)
    rcsh, shimhead, _ = sh("docker exec emby head -1 /bin/ffprobe 2>/dev/null", 20)
    observe = {
        "scan": d["scan"]["state"], "counts": d["counts"],
        "gaps": d["reconcile"].get("gaps"), "td": d["reconcile"].get("td_files"),
        "ep": d["reconcile"].get("episode"),
        "pending": d["ingest"].get("pending"), "cursor": d["ingest"].get("cursor"),
        "loop": d["loop_pids"], "open_tasks": len(d["open_tasks"]),
        "latest": d["latest"].get("archive"), "sha_ok": d["latest"].get("sha256_ok"),
        "disk": dinfo, "quota_rc": rcq, "shim": shimhead.strip(),
        "ops2_pulling": pulls[:4],
    }
    audit("感知", observe, "进入分析", t0=t0)

    # ---------------- 分析 + 决策 ----------------
    decisions = []
    # L1-1 loop
    if not d["loop_pids"]:
        decisions.append(("L1", "恢复 auto_finalize --loop", "loop 0 实例"))
    # L1-2 shim
    if shimhead.strip() != "#!/bin/sh":
        decisions.append(("L1", "p0_3_selfheal.sh 重部署", "shim 头行异常"))
    # L1-3 备份验签
    if not d["latest"].get("sha256_ok"):
        decisions.append(("L1", "标记需重打包(不自动抢锁)", "LATEST 验签失败"))
    # L1-4 磁盘
    if dinfo["pct"] >= DISK_WARN_PCT or dinfo["avail_mb"] < MIN_FREE_MB:
        decisions.append(("L1", "disk_guard 分级清理", "pct=%d avail=%dMB" % (dinfo["pct"], dinfo["avail_mb"])))
    # L1-5 日志闭环（不代别人结，只告警）
    if d["open_tasks"]:
        decisions.append(("L1", "告警: 未闭环条目(不代结)", "%d 条" % len(d["open_tasks"])))
    # L1-6 AI 自醒兜底：连续 2 天没有「自醒完成」留痕 → 脚本提醒 AI 没醒来
    # （用户 13:26：全靠脚本靠不住；AI 没醒时脚本要能发现，但不代替 AI 自省）
    try:
        jlog = os.path.join(ROOT, "AgentCommunication.log")
        txt = open(jlog, encoding="utf-8", errors="ignore").read()
        hits = [l for l in txt.splitlines() if "自醒" in l and l.startswith("## [")]
        import re as _re
        days = set()
        for l in hits:
            m = _re.match(r"## \[(\d{4}-\d{2}-\d{2})", l)
            if m:
                days.add(m.group(1))
        today = time.strftime("%Y-%m-%d")
        # 只统计「完成」类自醒条目（广播/开始不算真的醒过）
        done_days = set()
        for l in txt.splitlines():
            if l.startswith("## [") and "自醒" in l and "完成" in l:
                m = _re.match(r"## \[(\d{4}-\d{2}-\d{2})", l)
                if m:
                    done_days.add(m.group(1))
        if not done_days:
            decisions.append(("L1", "告警: selfwake_stale(尚无自醒完成留痕)", "AI 自醒任务需核查 cron/模型"))
        elif len([x for x in done_days if x < today]) == 0 and today not in done_days:
            decisions.append(("L1", "告警: selfwake_stale(连续无自醒)", "最近完成日=%s" % max(done_days)))
    except Exception:
        pass
    # L1-7 定时任务自身健康（15:0x 全量审计新增漏项）：
    # selfwake_stale 只看「有没有留痕」，看不出「job 被 pause/删了/模型又坏了」。
    # 2026-11-13 交接：qwenpaw 停用，改查 Hermes cron 任务表（enabled / last_error）。
    try:
        with open("/root/.hermes/cron/jobs.json", encoding="utf-8") as _f:
            _jobs = json.load(_f)
        _jobs = _jobs if isinstance(_jobs, list) else (_jobs or {}).get("jobs", [])
        _st = next((x for x in _jobs if x.get("id") == "99579e72b50a"), None)   # 运维任务每日AI自醒
        if _st is None:
            decisions.append(("L1", "告警: AI自醒任务在 Hermes cron 中不存在", "job 可能被删"))
        else:
            if not _st.get("enabled", True):
                decisions.append(("L1", "告警: AI自醒任务被禁用（Hermes cron enabled=false）", "需 resume"))
            if _st.get("last_error"):
                decisions.append(("L1", "告警: AI自醒任务 last_error", str(_st.get("last_error"))[:100]))
    except Exception:
        pass
    # L1-8 双运营「同刻改脚本」互斥：运维守护 06:10 自省 / 我 09:00 自醒都可能改 pipeline/*.py。
    # 两道证据：① 协作日志里对方未结的「改脚本」开始条目；② 近 10 分钟 pipeline/*.py
    # 有被改（QwenPaw 自动任务未必写「开始」，用 mtime 兜底）。命中则本轮不动脚本、只告警。
    try:
        with open(os.path.join(ROOT, "AgentCommunication.log"), encoding="utf-8", errors="ignore") as f:
            jt = f.read()
        log_hit = bool(re.search(r"(?:hermes-ops|Hermes-运维|运维守护) \| 开始 \|[^\n]*(改|自省|优化)",
                                 jt.split("hermes-ops")[-1][-400:]))
        import glob as _g
        recent_py = [f for f in _g.glob(os.path.join(PIPE, "*.py"))
                     if time.time() - os.path.getmtime(f) < 600]
        if log_hit or recent_py:
            decisions.append(("L1", "告警: 近10min有脚本改动/运维守护自省(避免同文件覆盖)",
                              "log=%s 文件=%s" % (log_hit, [os.path.basename(x) for x in recent_py[:3]])))
    except Exception:
        pass
    # L2 刮削自纠：只对「确实新增的冷文件」补，且每日最多一次。
    # 理由：NFO 缺口里多数是 TODB/TMDB 双缺（KB §6 不猜标题，永久补不上），
    # 每 15min 全量 nfo_gen --scan 会白烧 FUSE 遍历（SCALING §1 增量优先）。
    nfo_gap = d.get("nfo_gap") or {}
    # 首次运行当作「一天前跑过」：允许首轮补一次，但不显示天文数字、也不每15min空转
    last_l2 = st.get("last_l2_ts") or (time.time() - 86400)
    recent_video = 0
    try:
        recent_video = int(d.get("recent_video_30m"))
    except (TypeError, ValueError):
        pass
    if (nfo_gap.get("missing", 0) > 0 and d["scan"]["state"] != "Running"
            and (recent_video > 0 or (time.time() - last_l2) >= 86400)
            and (time.time() - last_l2) >= 3600):
        decisions.append(("L2", "nfo_gen 补缺(幂等)", "缺%d条/近30分钟新增%d/距上次%.1fh"
                          % (nfo_gap["missing"], recent_video, (time.time() - last_l2) / 3600)))
        # E017：只有真的跑了 nfo_gen 才写节流时间（dry 时这一步并不执行，写进去等于
        # 用一次只读巡检白占 1h 的增量窗口；13:14:12 的全局 --dry 就这么干过）。
        if not dry_l2:
            st["last_l2_ts"] = time.time()
    # L3 自动入库：六道闸门
    # 【E017】冷却只由「真灌」消耗，缺失状态从审计回填而不是 fail-open：
    #   旧口径 `st.get("last_l3_ts", 0)` → 1970 年（498413h）＝冷却闸门不存在（fail-open）；
    #   同时 dry 预演也写 last_l3_ts → 13:20:23 的全局 --dry 只读巡检把冷却烧到 13:20:30，
    #   之后 13:30/13:41/13:42/13:45 四轮全被 G4 挡住、L3 真入库 0 次 = 假闭环。
    #   现在：预演只写 last_l3_preview_ts；state 缺失时先查审计回填；真读不到才算未知 → 不放行。
    last_l3 = st.get("last_l3_ts")
    l3_src = "state"
    if not last_l3:
        last_l3, l3_src = last_l3_from_audit()
        if last_l3 and not dry_l3:
            st["last_l3_ts"] = last_l3   # 回填落盘，下次不用再扫审计
    if last_l3:
        hours_since = (time.time() - last_l3) / 3600.0
        cooled = hours_since >= COOLDOWN_H
        g4_note = "%.1fh%s" % (hours_since, "(审计回填)" if l3_src != "state" else "")
    elif l3_src == "审计无真灌记录":
        hours_since = None
        cooled = True                    # 证据：从未真灌过 → 不存在「同日两灌」
        g4_note = "无真灌记录→首次可跑"
    else:
        hours_since = None
        cooled = False                   # 未知 → fail-closed
        g4_note = "未知(%s)需--force-l3" % l3_src
    g4_ok = bool(a.force_l3 or cooled)
    g2_ok, g2_why = g2_reconcile_ok(d["reconcile"])
    sw_on, sw_why = ingest_switch()
    # E020：G1 曾经自己复算第二个条件 `and not d["open_tasks"]`。但 status_now
    # （10.2h 前更新）已把协作日志拆成 recent(≤3h)/historical(>3h)，并明确
    # 「协作日志是交接记录，不是运行态闸门」→ verdict 只报 recent、不判阻塞。
    # 结果：11 条**历史**未闭环让 G1 从 11-11 起每轮都是 False，而 status_now 同时
    # 说【稳定】；简报连挂 5 天「未过=['G0','G1']」，我一直在追一个幽灵闸门。
    # 现在只认 status_now 的单一结论 + recent 条目；字段缺失退回全量（不 fail-open）。
    _recent = d.get("open_tasks_recent")
    if _recent is None:                       # 旧版 status_now 没这个字段 → 退回全量
        _recent = d.get("open_tasks") or []
    g1_ok = (status_now.verdict(d)[0] == "稳定") and not _recent
    g1_why = "verdict=%s/recent未闭环=%d/历史未闭环=%d" % (
        status_now.verdict(d)[0], len(_recent), len(d.get("open_tasks_historical") or []))
    gate = {
        # E019：总开关放第一位。G3 只代表「配额够」，不代表「现在允许入库」。
        # 2026-11-11 19:08 用户「上片也暂停，只交韩国阿里云」：G0 是暂停令的最后闸，
        # --force-l3 只用于绕过每日冷却做判定，绝不允许越过总停用开关（防误灌 TG 云端写）。
        "G0 入库总开关": (sw_on, sw_why),
        "G1 状态稳定": (g1_ok, g1_why),
        # E016：对账必须「真跑过且齐」（gaps 显式 == []），对账未知时放行 L3 是事故。
        # E018：但「齐」只认 lib_reconcile 的按库区间判据，不再叠加全局 td==ep 等式
        # （那会随多版本合并永久挡死 L3，且与 status_now/ops1_daily/selfcheck 口径分叉）。
        "G2 对账齐": (g2_ok, g2_why),
        "G3 配额": (rcq == 0, "rc=%d" % rcq),
        "G4 冷却+未熔断": (g4_ok and not st.get("l3_fused"),
                     "%s/熔断=%s" % (g4_note, st.get("l3_fused"))),
        "G5 运维守护避让": (not pulls, str(pulls[:2])),
        "G6 Scan非Running": (d["scan"]["state"] != "Running", d["scan"]["state"]),
    }
    gate_all = all(v[0] for v in gate.values())
    if gate_all:
        decisions.append(("L3", "自动入库判定通过 → 执行六步SOP%s" % ("(DRY)" if dry_l3 else ""),
                          "gates=%s" % {k: v[0] for k, v in gate.items()}))
    else:
        decisions.append(("L3", "跳过自动入库", "未过=%s" % [k for k, v in gate.items() if not v[0]]))
    audit("分析", {"gate": {k: v[0] for k, v in gate.items()},
                  "gate_detail": {k: v[1] for k, v in gate.items()},
                  "cooldown_h": (round(hours_since, 1) if hours_since is not None else None),
                  "dry": {"all": dry_all, "l1": dry_l1, "l2": dry_l2, "l3": dry_l3},
                  "pulls": pulls[:2]},
          decisions, t0=t0)

    # ---------------- 执行 ----------------
    executed = []
    for lvl, what, why in decisions:
        t1 = time.time()
        if lvl == "L1" and "恢复 auto_finalize" in what:
            if dry_l1:
                res = "[DRY] 会 nohup /usr/bin/python3 -u auto_finalize.py --loop"
            else:
                _, out, err = sh("cd %s && nohup /usr/bin/python3 -u auto_finalize.py --loop >> %s/auto_finalize.stdout.log 2>&1 < /dev/null & echo $!" % (PIPE, BK), 20)
                time.sleep(3)
                res = "loop 恢复 pid=%s now=%s" % (out.strip(), loop_pids())
        elif lvl == "L1" and "selfheal" in what:
            rc, out, err = sh("bash %s/p0_3_selfheal.sh" % PIPE, 120) if not dry_l1 else (0, "[DRY]", "")
            res = "rc=%d %s %s" % (rc, out[:80], err[:80])
        elif lvl == "L1" and "重打包" in what:
            res = "仅标记：需人工/下轮在 flock 空闲时 force-pack（避免与常驻 loop 抢锁）"
        elif lvl == "L1" and "disk_guard" in what:
            acts, res = disk_guard(dinfo, dry_l1)
            res = "%s | %s" % (res, "; ".join(acts[:6]))
        elif lvl == "L1" and "告警" in what:
            res = ("只告警不代结（§7.3）；AI 自醒 stale 需查 Hermes cron 状态 / 模型 provider"
                   if "selfwake" in what else "只告警不代结（§7.3）")
        elif lvl == "L2" and "nfo_gen" in what:
            if dry_l2:
                res = "[DRY] 会跑 nfo_gen.py --kind all --workers 24（幂等）"
            elif nfo_gen_running():
                # §7.4：同一时间只允许一个标题类任务（会写同一批 NFO/poster）
                res = "跳过：已有 nfo_gen 在跑（串行保护），本轮不写 last_l2_ts"
                st.pop("last_l2_ts", None)
            else:
                rc, out, err = sh("cd %s && timeout 840 /usr/bin/python3 nfo_gen.py --kind all --workers 24 2>&1 | tail -5" % PIPE, 900)
                res = "rc=%d %s" % (rc, (out or err)[:160])
        elif lvl == "L3" and gate_all:
            # 【增量六步】不用 ingest_sop.sh（它硬编码 --full 拉全 7 频道历史，
            # 13:14 实测 300s 超时，且会产生 716 条巨型队列 = 11:36 乱局来源）。
            # 按 START_HERE §2 的六步，scan 不带 --full = 只拉各频道 last_msg 之后的新消息。
            PY = "/usr/bin/python3"
            last_prev = st.get("last_l3_preview_ts") or 0
            prev_age_h = (time.time() - last_prev) / 3600.0 if last_prev else None
            # E017：DRY_ONLY / --dry 下不重复预演（省 FUSE 扫描），但节流只作用于「预演本身」，
            # 绝不因此写 last_l3_ts —— 真灌路径（dry=False）永远先预演再执行，不受节流影响。
            if dry_l3 and prev_age_h is not None and prev_age_h < PREVIEW_MIN_INTERVAL_H:
                res = ("预演节流：距上次预演 %.1fh < %.0fh（DRY_ONLY 不重复烧 FUSE）；"
                       "真灌未执行、真灌冷却未消耗" % (prev_age_h, PREVIEW_MIN_INTERVAL_H))
            else:
                # 先 --dry 预演（必做，不写队列）
                rc_dry, out_dry, err_dry = sh(
                    "cd %s && timeout 240 %s tgpipe.py scan --kinds movie,tv --dry 2>&1 | tail -10"
                    % (PIPE, PY), 300)
                st["last_l3_preview_ts"] = time.time()   # 只记预演时间，不碰 last_l3_ts
                if rc_dry != 0:
                    res = "L3 增量 dry 预演失败 rc=%d，熔断不真灌：%s" % (rc_dry, (err_dry or out_dry)[:120])
                    st["l3_fails"] = st.get("l3_fails", 0) + 1
                    if st["l3_fails"] >= 2:
                        st["l3_fused"] = True
                elif dry_l3:
                    res = "L3 增量 dry 预演 OK（DRY_L3，真灌未执行，冷却未消耗）：%s" % out_dry[:140].replace("\n", " ")
                else:
                    # 真执行：六步逐步跑，任一步失败即熔断（不继续灌）
                    # 2026-11-13 按运维守护实测建议 + 用户 11-09 原话「永远不要等着 emby 来自动扫
                    # 基本没戏」/ NAMING_RUNBOOK「等 Emby 扫描替你刮=方向错了」：
                    # 刮削自主(nfo_gen 第3步已完成)，第5步**不再无条件全库扫**。
                    # 默认走 E008 增量 Media/Updated + 单剧 Refresh；只有「新增文件>20
                    # 且距上次全库扫>30min」才用 _scan_now.py（建条目需要），并写冷却状态。
                    new_media = 0
                    try:
                        new_media = int(d.get("recent_video_30m") or 0)
                    except (TypeError, ValueError):
                        pass
                    last_scan = st.get("last_full_scan_ts", 0)
                    scan_cool = (time.time() - last_scan) >= 30 * 60
                    need_full_scan = (new_media > 20 and scan_cool)
                    step5 = (("5/6 _scan_now(条件全库扫)", "timeout 120 %s _scan_now.py" % PY, 150)
                             if need_full_scan else
                             ("5/6 post_ingest 增量通知+单剧Refresh(不扫全库)",
                              "timeout 600 %s post_ingest.py --since today" % PY, 660))
                    if need_full_scan:
                        st["last_full_scan_ts"] = time.time()
                    steps = [
                        ("1/6 增量 scan --queue", "timeout 300 %s tgpipe.py scan --kinds movie,tv --queue" % PY, 360),
                        ("2/6 apply --from-queue --loop", "timeout 1800 %s tgpipe.py apply --from-queue --loop --max-items %d --acct2 /opt/embytg/.tg_string2"
                         % (PY, MAX_ITEMS), 1900),
                        ("3/6 nfo_gen 自主刮削", "timeout 900 %s nfo_gen.py --kind all --workers 24" % PY, 960),
                        ("4/6 restore_fetchers --verify", "timeout 120 %s restore_fetchers.py --verify" % PY, 150),
                        step5,
                        ("6/6 finalize_meta --audit", "timeout 900 %s finalize_meta.py --audit" % PY, 960),
                    ]
                    trace = []
                    for name, cmd, to in steps:
                        rc_s, out_s, err_s = sh("cd %s && %s 2>&1 | tail -6" % (PIPE, cmd), to)
                        trace.append("%s rc=%d" % (name, rc_s))
                        if rc_s != 0:
                            break
                    ok = all("rc=0" in x for x in trace) and len(trace) == len(steps)
                    res = "L3 真入库 %s | %s | %s" % (
                        "OK" if ok else "PARTIAL/FUSE", "; ".join(trace),
                        (out_s or err_s)[:100].replace("\n", " "))
                    # 只有真灌才写冷却（E017）
                    st["last_l3_ts"] = time.time()
                    if ok:
                        st["l3_fails"] = 0
                    else:
                        st["l3_fails"] = st.get("l3_fails", 0) + 1
                        if st["l3_fails"] >= 2:
                            st["l3_fused"] = True
        else:
            res = "跳过（闸门未过或 DRY）"
        executed.append({"level": lvl, "what": what, "why": why, "result": res,
                         "elapsed_s": round(time.time() - t1, 2)})
        audit("执行", why, what, action=lvl, result=res, t0=t1)

    # ---------------- 留痕 / 收口 ----------------
    save_state(st)
    summary = {"cycle": st["cycles"],
               "dry": {"all": dry_all, "l1": dry_l1, "l2": dry_l2, "l3": dry_l3},
               "decisions": len(decisions),
               "executed": executed, "gate_all": gate_all,
               "cooldown_h": (round(hours_since, 1) if hours_since is not None else None)}
    audit("留痕", {"audit_file": AUDIT, "state": STATE},
          "本轮闭环完成",
          result="cycles=%d gate_all=%s dry(all/l1/l2/l3)=%s/%s/%s/%s"
                 % (st["cycles"], gate_all, dry_all, dry_l1, dry_l2, dry_l3), t0=t0)
    if "--json" in sys.argv:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
