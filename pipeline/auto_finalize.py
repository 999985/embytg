#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EmbyTG 收尾自动化（服务器端常驻）：扫描收敛 → 补刮兜底 → 审计复核 → 打包备份。

用户 m04207 的条件：「如果 emby 刮削完，并且完全正常运营中，那么备份一份到本项目」。
本脚本负责把「刮削完 + 正常运营」判定出来并打包；拉回 F:\\emby\\backup\\ 由本地
pull_backup.ps1 / 定时器完成（见 manifest 的 state=DONE）。

为什么常驻：扫描要数小时，DSH 会话不会一直在线。服务器端 nohup --loop 自持状态机，
每 60s 采样一轮，所有阶段写 /opt/embytg/backup/finalize_state.json 断点续跑，
进程被杀/重启后从 stage 继续，不重复补刮、不重复打包。

阶段（stage）:
  MONITOR  — 等扫描收敛：RefreshLibrary 非 Running + lib_reconcile 按库对账全齐（规模无关，见 docs/SCALING.md §4）+ 连续
             STABLE_ROUNDS 轮 Episode 条目数稳定。ffprobe 补媒体信息不阻塞备份（E006）。
  REFRESH  — 扫描 Idle 后跑 `nfo_to_emby.py --apply`：把 TG 挂载上已生成的 NFO
              <plot>/<genre> 直接写进 Emby 条目的 Overview/Genres（确定性本地 IO）。
              E007：FullRefresh(Refresh Metadata) 204 但 provider 阶段根本不跑，
              且其排队请求会把刚写好的 Overview 异步冲回空。故不再发 FullRefresh。
  AUDIT    — 复核：元数据不全 <= AUDIT_BAD_MAX、同画质双份 0、挂载异常 0、
             按库对账全齐 + 全链路健康。不达标回 REFRESH（最多 2 次）。
  HOLD     — 补刮上限用尽仍不达标：进入等待，**绝不提前打包残缺库**，每轮
             重新体检，每 HOLD_REFRESH_EVERY 轮再补一次刮，直到达标才 PACK。
  PACK     — 打包：Emby library.db + system.xml、teldrive pg_dump、全部明文
             配置/会话/systemd/文档/主力脚本/队列状态 + final_audit.txt +
             manifest.json（时间戳/各计数/SHA256）。
  DONE     — 写 LATEST.json（state=DONE, 包绝对路径, sha256, 摘要），本地据此拉回。

用法:
  python3 auto_finalize.py --loop          # 常驻（生产）
  python3 auto_finalize.py --once          # 只跑一轮采样（调试/定时器）
  python3 auto_finalize.py --status        # 打印状态机快照
  python3 auto_finalize.py --force-pack    # 跳过收敛等待直接打包（应急）

所有阈值/路径在下方常量，环境变量可覆盖（SAMPLE_GAP / AUDIT_BAD_MAX 等）。
"""
import fcntl
import glob
import gzip
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta

from embytg_conf import cfg

# E005 / SCALING.md §4：收敛判据改为「按库对账」，不再硬编码 EP_TARGET。
# lib_reconcile.py 与本脚本同目录，nohup 启动时脚本目录在 sys.path 里。
try:
    from lib_reconcile import reconcile
except Exception as _e:                       # 缺模块时绝不假收敛
    def reconcile():
        return {"converged": False, "libs_with_gap": ["reconcile 模块不可用"],
                "episode_expected_min": -1, "episode_expected_max": -1}


# ---------------------------------------------------------------- 常量
ROOT = "/opt/embytg"
PIPE = os.path.join(ROOT, "pipeline")
RUNNER_DIR = "/opt/embytg-runner"     # 数据面节点侧脚本（云电脑/PC 用）
BK = os.path.join(ROOT, "backup")
STATE_F = os.path.join(BK, "finalize_state.json")
LATEST = os.path.join(BK, "LATEST.json")
LOG = os.path.join(BK, "auto_finalize.log")
LOCK = os.path.join(BK, ".auto_finalize.lock")

EMBY = cfg("emby_url")
TOKEN = cfg("emby_token")
HDR = {"X-Emby-Token": TOKEN, "Content-Type": "application/json",
       "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
EMBY_LOG = "/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt"
EMBY_DATA = "/var/lib/docker/volumes/embytg_embydata/_data"
RCLONE_LOG = os.path.join(ROOT, "cache", "rclone-mount.log")

EP_TARGET = int(os.environ.get("EP_TARGET", "13826"))
AUDIT_BAD_MAX = int(os.environ.get("AUDIT_BAD_MAX", "20"))   # 容忍 9 条 TODB-only + 少量 TVDB 无果
SAMPLE_GAP = int(os.environ.get("SAMPLE_GAP", "60"))
STABLE_ROUNDS = int(os.environ.get("STABLE_ROUNDS", "2"))
MAX_REFRESH = 2
# 2026-11-13 灾备改造：DONE 不再永久待机。包龄到点或集数涨够就重打，
# 不依赖 PC 侧 pull_backup.ps1（PC 已改间歇运行，那条触发链断了）。
PACK_MAX_AGE_H = int(os.environ.get("PACK_MAX_AGE_H", "24"))
PACK_EP_DELTA = int(os.environ.get("PACK_EP_DELTA", "50"))
# 本地保留几代全量包（超出自动清最旧的，防止 disk 被备份撑爆）
KEEP_PACKS = int(os.environ.get("KEEP_PACKS", "2"))
HOLD_REFRESH_EVERY = int(os.environ.get("HOLD_REFRESH_EVERY", "10"))  # HOLD 每 10 轮(10min)再补刮一次

# 备份文件清单（源绝对路径 -> 包内相对路径）。不存在则跳过并在 manifest 记录。
BACKUP_FILES = [
    (EMBY_DATA + "/data/library.db", "emby/library.db"),
    (EMBY_DATA + "/config/system.xml", "emby/config/system.xml"),
    (ROOT + "/config.json", "config/config.json"),
    (ROOT + "/compose.yaml", "config/compose.yaml"),
    (ROOT + "/.env", "config/.env"),
    (ROOT + "/.token.json", "config/.token.json"),
    (ROOT + "/.apikey.json", "config/.apikey.json"),
    (ROOT + "/.bot_tokens.json", "config/.bot_tokens.json"),
    (ROOT + "/config/rclone.conf", "config/rclone.conf"),
    (ROOT + "/config/config.toml", "config/config.toml"),
    (ROOT + "/.tg_string", "sessions/.tg_string"),
    (ROOT + "/.tg_string2", "sessions/.tg_string2"),
    (ROOT + "/.tg_user.session", "sessions/.tg_user.session"),
    ("/etc/systemd/system/embytg-mount.service", "systemd/embytg-mount.service"),
    ("/etc/cron.d/p03_ffprobe_shim", "systemd/p03_ffprobe_shim.cron"),
    ("/etc/cron.d/embytg_tgdb_daily", "systemd/embytg_tgdb_daily.cron"),
    (ROOT + "/PROJECT.md", "docs/PROJECT.md"),
    (ROOT + "/AGENTS.md", "docs/AGENTS.md"),
    (ROOT + "/README.md", "docs/README.md"),
    (ROOT + "/pipeline/docs/ERRORS.md", "docs/ERRORS.md"),
    (ROOT + "/pipeline/docs/SCALING.md", "docs/SCALING.md"),
    (ROOT + "/pipeline/docs/ROADMAP.md", "docs/ROADMAP.md"),
]
# Hermes-运维 12:14 修真 bug：上面 5 个曾被写成裸字符串塞进列表字面量，
# pack() 里 `for src, rel in BACKUP_FILES` 会解包成单字符 → 脚本从未进包。
# 统一走下面的 (src, rel) 元组循环。
for _n in ["tgpipe.py", "nfo_gen.py", "nfo_only.py", "finalize_meta.py",
           "restore_fetchers.py", "fix_nfo_tags.py", "disk_watch.sh",
           "embytg_conf.py", "auto_finalize.py", "lib_reconcile.py",
           "rescan_libs.py", "nfo_to_emby.py", "_tidy.py", "pull_backup.ps1", "_scale_audit.py", "_verify_e008.py", "_verify_e009.py"]:
    BACKUP_FILES.append((os.path.join(PIPE, _n), "scripts/" + _n))
for _n in ["p0_3_ffprobe_shim.sh", "p0_3_install.sh", "p0_3_rollback.sh",
           "p0_3_verify.sh", "p0_3_status.sh", "p0_3_selfheal.sh",
           "p0_3_rewarm.py"]:
    BACKUP_FILES.append((os.path.join(PIPE, _n), "scripts/" + _n))
# 2026-11-09 21:56 补齐当前态：E012 集号标准化 / P0-1 SQLite 地基 / 供给侧与缺口工具
# （MIGRATION.md §2 实测 07:39 包缺这些；不存在的文件由 pack 跳过并在 manifest 记录）
for _n in ["renumber_todb.py", "tgstore.py", "migrate_to_sqlite.py",
           "resource_state.py", "moviewatch.py", "todb_watch.py",
           "todb_sync.py", "todb_titles.py", "gap_list.py",
           "build_ingest_worklist.py", "tgdb_daily_sync.py"]:
    BACKUP_FILES.append((os.path.join(PIPE, _n), "scripts/" + _n))
for _n in ["api.sh", "rebind-emby.sh"]:
    BACKUP_FILES.append((os.path.join(ROOT, _n), "scripts/" + _n))
for _n in ["tg_state.json", "tg_pending.json", "tg_meta_cache.json",
           "lib_opts_before_nfo.json"]:
    BACKUP_FILES.append((os.path.join(PIPE, _n), "state/" + _n))
# 2026-11-09 21:56 补齐当前态文档：迁移风险 / TODB 网关 / 共享行动日志
# （2026-11-13：旧 Mission 报告/工单已按用户要求清除，不再入包）
# 2026-11-14：OPS_JOURNAL 已归档到 docs/archive/（旧的 docs/ 路径恒缺失，会在
# files_missing 里刷三条假信号，把真缺失盖住）。指向真身，已归档的不再按原名找。
for _n in ["MIGRATION.md", "TODB_API.md"]:
    BACKUP_FILES.append((os.path.join(PIPE, "docs", _n), "docs/" + _n))
BACKUP_FILES.append((os.path.join(ROOT, "docs", "archive", "OPS_JOURNAL.md"),
                     "docs/archive/OPS_JOURNAL.md"))
# 2026-11-10 08:2x 补齐刮削/命名工具链（E019/E020）：逐集 NFO、季错位补全、Name 直写、体检、幽灵清理
for _n in ["nfo_episodes.py", "nfo_episodes_fix.py", "nfo_episodes_tmdb.py",
           "nfo_name_to_emby.py", "naming_health.py", "refresh_series.py",
           "probe_source_servers.py", "prewarm_ffprobe.sh", "fix_series_all.sh",
           "todb_resolve.py", "nfo_sidecar.py", "agent_log.py",
           "gen_nfo_gap_report.py", "fix_shaonian_shidai.py",
           "status_now.py",          # Hermes-运维 12:13 补：AGENTS §7.6 R3 单一状态入口
           "prewarm_daemon.py",      # Hermes-运维 12:42 补：ffprobe 并发预热(用户「以后都打开」)
           "ops1_daily.py", "ops1_selfcheck.py",  # Hermes-运维 13:02 补：运维任务每日值守 + 收尾反思
           "ops1_autopilot.py",  # Hermes-运维 13:22 补：无人值守闭环(感知→分析→决策→执行→留痕)
           "ops1_selfwake.py",   # Hermes-运维 13:45 补：AI每日自醒上下文简报
           "r2_backup.py",       # Hermes-运维 2026-11-11 补：R2 异地备份(免费额度硬闸门)
           "src_guard.py", "ingest_quota.py",
           "apply_rename_plan.py", "fix_series_identity.py", "fix_nfo_titles.py",
           "relabel_series.py", "fix_shenmu.py",
           # hermes-ops 13:30+ 补：无人值守闭环与自动入库（此前漏在包外）
           "autoops.py", "autoingest.py", "self_audit.py", "source_pull.py", "daily_ops.py",
           "src_login.py", "check_layout.py",
           "post_ingest.py", "ingest_review.py", "runner_report.py", "alert.py", "restore_drill.py",
           "cron_lint.py", "improve_guard.py", "adapt_thresholds.py"]:
    BACKUP_FILES.append((os.path.join(PIPE, _n), "scripts/" + _n))
for _n in ["OPS_HANDBOOK.md", "SELF_IMPROVE.md", "SOURCE_SERVERS.md", "CRON_SCHEDULE.md", "PROJECT_LAYOUT.md", "OPS_COVERAGE.md"]:
    BACKUP_FILES.append((os.path.join(PIPE, "docs", _n), "docs/" + _n))
for _n in [".ingest_budget.json", ".ingest_manual.jsonl", ".autoingest_queue.json",
           ".autoingest_failed.json", ".self_backlog.json",
           ".gha_ep_fail.json",      # 2026-11-18：按集退避表（live），换机后不该重新撞死集
           ".gha_new_done.json"]:    # 2026-11-18：剧集级冷却表（live），同上
    BACKUP_FILES.append((os.path.join(PIPE, _n), "state/" + _n))
for _f in ["每日巡检与自省.json", "远程上片核查.json", "四小时看护.json",
           "日终收尾.json", "周度体检.json"]:
    BACKUP_FILES.append((os.path.join(PIPE, "cron_specs", _f), "cron/" + _f))
for _n in ["NAMING_RUNBOOK.md", "KB.md", "README.md",
           "SCALE_READINESS_20261110.md",   # Hermes-运维 09:56 补：十万级就绪度报告（此前漏在包外）
           "R2_BACKUP.md"]:      # Hermes-运维 2026-11-11 补：R2 异地备份记录
    BACKUP_FILES.append((os.path.join(PIPE, "docs", _n), "docs/" + _n))
# 2026-11-14：OPS1_MANUAL / OPS1_SELFWAKE 已归档到 docs/archive/（qwenpaw 停用后
# 由 Hermes cron 接管），按原名从 docs/ 取会恒缺 —— 改指真身。
for _n in ["OPS1_MANUAL.md", "OPS1_SELFWAKE.md"]:
    BACKUP_FILES.append((os.path.join(ROOT, "docs", "archive", _n),
                         "docs/archive/" + _n))
BACKUP_FILES.append((ROOT + "/AgentCommunication.log", "docs/AgentCommunication.log"))


# ---------------------------------------------------------------- 工具
def log(msg):
    line = "[%s] %s" % (datetime.now().strftime("%F %T"), msg)
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def api(path, method="GET", body=None, timeout=90):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(EMBY + path, data=data, headers=HDR, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as f:
            raw = f.read()
            return f.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:300]
    except Exception as e:
        return 0, str(e)[:300]


def item_count(t):
    st, d = api("/Items?Recursive=true&IncludeItemTypes=%s&Limit=1&EnableTotalRecordCount=true" % t)
    return (d or {}).get("TotalRecordCount", 0) if st == 200 else -1


def empty_series():
    """返回 (Series总数, 元数据空数)。分页遍历，不写死 Limit（SCALING.md §1：
    以后几十万条目时固定 700 会漏判）。"""
    total, empty, start, page = 0, 0, 0, 500
    while True:
        st, d = api("/Items?Recursive=true&IncludeItemTypes=Series"
                    "&Fields=Name,Genres,Overview&StartIndex=%d&Limit=%d" % (start, page))
        if st != 200:
            return (-1, -1) if start == 0 else (total, empty)
        its = (d or {}).get("Items", [])
        total += len(its)
        empty += sum(1 for x in its if not (x.get("Genres") or x.get("Overview")))
        start += len(its)
        if not its or start >= ((d or {}).get("TotalRecordCount") or 0):
            break
    return total, empty


def scan_state():
    st, d = api("/ScheduledTasks")
    if st != 200 or not isinstance(d, list):
        return "Unknown", -1.0
    for t in d:
        if t.get("Key") == "RefreshLibrary":
            return t.get("State"), round(t.get("CurrentProgressPercentage") or 0, 1)
    return "Unknown", -1.0


def ffprobe_total():
    try:
        r = subprocess.run(["grep", "-c", "ProcessRun 'ffprobe' Execute", EMBY_LOG],
                           capture_output=True, text=True, timeout=30)
        return int((r.stdout or "0").strip() or 0)
    except Exception:
        return -1


def ffprobe_timeout(window_min=30):
    """近 window_min 分钟内的 ffprobe 超时条数。

    E005：原先 `grep -c` 数全日志累计，把 E001 `full` 时代的旧超时和
    writes 模式下 2 条偶发远程 I/O 抖动都算进去，导致 `audit_ok` 的
    `==0` 永不满足、状态机永不打包。子代理 289a7d8f 调研：2454 条 probe
    里 2 条超时(0.08%)、唯一路径单调推进、无 code137/OOM → 可忽略；
    升级为不可忽略的条件是「持续增长 / 同路径反复 / code137 / OOM」。
    """
    try:
        r = subprocess.run(["bash", "-lc",
                            "grep 'Timeout while running Ffprobe' %s | tail -200" % EMBY_LOG],
                           capture_output=True, text=True, timeout=30)
        now = datetime.now()
        n = 0
        for ln in (r.stdout or "").splitlines():
            m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", ln)
            if not m:
                continue
            try:
                t = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if now - t <= timedelta(minutes=window_min):
                n += 1
        return n
    except Exception:
        return -1


def ffprobe_window(window_min=30, tail_lines=20000):
    """E009：近 window_min 分钟 ffprobe 窗口信号，规模无关（SCALING §1）。

    只读日志尾部 tail_lines 行（tail 从文件尾读，不 grep 全文件），返回：
      timeout  近窗口超时条数（仅记录，不作判据）
      repeat   同路径反复超时（>=2 次）的路径数 —— 真故障信号
      probe    近窗口成功发起 ffprobe 数
      rate     timeout/(timeout+probe) 超时率 —— 规模无关比值
    判据只看 repeat==0 且 rate<=阈值 且无 OOM；绝对条数随后台补流按量
    线性增长，不该卡打包。读取失败返回负值让判据保守不达标。
    """
    cutoff = datetime.now() - timedelta(minutes=window_min)
    try:
        r = subprocess.run(["tail", "-n", str(tail_lines), EMBY_LOG],
                           capture_output=True, text=True, timeout=30)
        lines = (r.stdout or "").splitlines()
    except Exception:
        return {"timeout": -1, "repeat": -1, "probe": -1, "rate": -1.0}
    to_paths, probe_n = [], 0
    for ln in lines:
        m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", ln)
        if not m:
            continue
        try:
            t = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if t < cutoff:
            continue
        if "Timeout while running Ffprobe" in ln:
            pm = re.search(r'-i file:"([^"]+)"', ln)
            to_paths.append(pm.group(1) if pm else ln)
        elif "ProcessRun 'ffprobe' Execute" in ln:
            probe_n += 1
    cnt = {}
    for pp in to_paths:
        cnt[pp] = cnt.get(pp, 0) + 1
    repeat = sum(1 for c in cnt.values() if c >= 2)
    denom = len(to_paths) + probe_n
    rate = round(len(to_paths) / denom, 4) if denom else 0.0
    return {"timeout": len(to_paths), "repeat": repeat, "probe": probe_n, "rate": rate}


def probe_killed(window_min=30):
    """近 window_min 分钟 ffprobe 被 kill (code 137 / OOM) 的条数——真故障信号。"""
    try:
        r = subprocess.run(["bash", "-lc",
                            "grep \"ProcessRun 'ffprobe' Process exited with code 137\" %s | tail -200"
                            % EMBY_LOG], capture_output=True, text=True, timeout=30)
        now = datetime.now()
        n = 0
        for ln in (r.stdout or "").splitlines():
            m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", ln)
            if not m:
                continue
            try:
                t = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if now - t <= timedelta(minutes=window_min):
                n += 1
        return n
    except Exception:
        return -1


def grep_count(path, pat):
    try:
        r = subprocess.run(["grep", "-c", pat, path], capture_output=True, text=True, timeout=30)
        return int((r.stdout or "0").strip() or 0)
    except Exception:
        return -1


def health():
    """全链路健康快照。"""
    st, _ = api("/System/Info")
    # 挂载模式
    try:
        ps = subprocess.run(["ps", "-eo", "args"], capture_output=True, text=True, timeout=30).stdout
        prod = [l for l in ps.splitlines() if "rclone mount embytg: /opt/embytg/mnt/embytg" in l]
        mode = "writes" if (prod and "--vfs-cache-mode writes" in prod[0]) else ("full" if prod else "NONE")
    except Exception:
        mode = "UNKNOWN"
    # 容器
    try:
        up = subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                            capture_output=True, text=True, timeout=30).stdout.split()
    except Exception:
        up = []
    # 磁盘
    try:
        df = subprocess.run(["df", "-m", "/"], capture_output=True, text=True, timeout=30).stdout.split()
        avail_mb = int(df[-3]) if len(df) >= 3 else -1
    except Exception:
        avail_mb = -1
    return {
        "emby_http": st, "mount_mode": mode, "containers_up": len(up),
        "avail_mb": avail_mb,
        "rclone_errors": grep_count(RCLONE_LOG, "ERROR"),
        "ffprobe_timeout": ffprobe_timeout(),       # 近 30min 偶发超时数（E005：不再数全日志）
        "probe_killed": probe_killed(),             # 近 30min code137/OOM 数（真故障）
        "ffprobe_timeout_total": grep_count(EMBY_LOG, "Timeout while running Ffprobe"),  # 累计仅记录
    }


def snapshot():
    state, prog = scan_state()
    ser_total, ser_empty = empty_series()   # 只调一次（分页遍历全库，别重复付费）
    return {
        "ts": datetime.now().strftime("%F %T"),
        "state": state, "prog": prog,
        "movie": item_count("Movie"), "series": item_count("Series"),
        "episode": item_count("Episode"),
        "ser_total": ser_total, "ser_empty": ser_empty,
        "ffprobe": ffprobe_total(),
        "health": health(),
    }


def load_state():
    if os.path.exists(STATE_F):
        try:
            return json.load(open(STATE_F, encoding="utf-8"))
        except Exception:
            pass
    return {"stage": "MONITOR", "stable": 0, "refresh_rounds": 0,
            "last": None, "history": [], "notes": []}


def save_state(s):
    os.makedirs(BK, exist_ok=True)
    tmp = STATE_F + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_F)


def converged(cur, last, s):
    """连续 STABLE_ROUNDS 轮：非 Running + 按库对账全齐 + Episode 条目数稳定。

    E005 / SCALING.md §4：不再用硬编码 EP_TARGET 作下限。按库对账
    (lib_reconcile.reconcile) 是规模无关的真判据——库数/文件数涨 100 倍公式不变。
    只在非 Running 时跑对账（CTE + 复制 library.db 有成本），避免长扫描期高频空转。
    E006：ffprobe 补 MediaStreams 不阻塞收敛（详见下方 flat 注释）。
    """
    if cur["state"] == "Running":
        s["stable"] = 0
        return False
    if last is None:
        return False
    try:
        rec = reconcile()
    except Exception as e:
        log("  ! 对账异常，保守判不收敛：%s" % str(e)[:200])
        s["stable"] = 0
        return False
    s["reconcile"] = {
        "converged": rec["converged"], "gaps": rec["libs_with_gap"],
        "ep_min": rec["episode_expected_min"], "ep_max": rec["episode_expected_max"],
    }
    if not rec["converged"]:
        s["stable"] = 0
        log("  收敛待命：按库对账未齐 %s（Episode 期望 [%d,%d]），继续等"
            % (rec["libs_with_gap"], rec["episode_expected_min"], rec["episode_expected_max"]))
        return False
    # E006 / SCALING.md §4：收敛只要求「条目入库稳定」，**不再要求 ffprobe 不涨**。
    # 理由（2026-11-09 03:45 实测）：日韩剧 1310 集全部入库、按库对账 converged=True
    # 之后，Emby 仍在串行补 E001 欠账的 MediaStreams（+6 条/20s，约 4795 条待补，
    # 数小时），这是「媒体信息增强」不是「条目入库」，不该阻塞「刮削完+正常运营」
    # 的备份判定。ffprobe 故障仍由 health() 的「近 30min 超时<=2 + 无 OOM-kill」兜住。
    flat = (cur["episode"] == last.get("episode"))
    if flat:
        s["stable"] += 1
    else:
        s["stable"] = 0
    return s["stable"] >= STABLE_ROUNDS


def run_refresh():
    """E007：不再发 FullRefresh（provider 阶段不跑且会冲掉已写 Overview），
    改跑 nfo_to_emby.py --apply，从 TG 挂载 NFO 直接写 Emby 条目。"""
    log("REFRESH: 扫描 Idle，跑 nfo_to_emby.py --apply（NFO 直写，E007）")
    try:
        r = subprocess.run(
            ["python3", "nfo_to_emby.py", "--apply", "--workers", "16"],
            cwd=PIPE, capture_output=True, text=True, timeout=3600)
        out = (r.stdout or "") + (r.stderr or "")
        p = os.path.join(BK, "refresh_%d.log" % int(time.time()))
        with open(p, "w", encoding="utf-8") as f:
            f.write(out)
        log("  rc=%d 输出-> %s" % (r.returncode, p))
        for ln in out.splitlines():
            if ln.startswith("# ") or "完成" in ln or "复查" in ln:
                log("  | " + ln)
        return out
    except Exception as e:
        log("  ! refresh 异常 " + str(e)[:200])
        return ""


def run_audit():
    try:
        r = subprocess.run(["python3", "finalize_meta.py", "--audit"],
                           cwd=PIPE, capture_output=True, text=True, timeout=1200)
        out = (r.stdout or "") + (r.stderr or "")
    except Exception as e:
        out = "AUDIT 异常: %s" % e
    p = os.path.join(BK, "final_audit.txt")
    with open(p, "w", encoding="utf-8") as f:
        f.write("# 生成时间 %s\n" % datetime.now().strftime("%F %T"))
        f.write(out + "\n")
    bad = dups = orphan = no_pid = 0
    for ln in out.splitlines():
        if "元数据不全" in ln:
            try:
                bad = int(ln.split("元数据不全")[1].split("/")[0].strip())
            except Exception:
                pass
        if "同画质双份" in ln:
            try:
                dups = int(ln.split("同画质双份")[1].split("处")[0].strip())
            except Exception:
                pass
        if "Episode 缺季/集号" in ln:
            try:
                orphan = int(ln.split("Episode 缺季/集号")[1].split("条")[0].strip())
            except Exception:
                pass
        if "缺 TMDB ID" in ln:
            try:
                no_pid = int(ln.split("缺 TMDB ID")[1].split("）")[0].split(")")[0].strip())
            except Exception:
                pass
    return {"bad": bad, "dups": dups, "orphan": orphan, "no_pid": no_pid,
            "raw": out, "path": p}


def audit_ok(a, snap):
    h = snap["health"]
    # E005：收敛判据用按库对账，不硬编码 EP_TARGET
    try:
        rec = reconcile()
        ep_converged = rec["converged"]
        gap = rec["libs_with_gap"]
    except Exception as e:
        ep_converged, gap = False, ["reconcile 异常: %s" % str(e)[:80]]
    pw = ffprobe_window()   # E009：规模无关窗口信号
    checks = {
        "元数据不全<=%d" % AUDIT_BAD_MAX: a["bad"] <= AUDIT_BAD_MAX,
        "同画质双份=0": a["dups"] == 0,
        "挂载异常=0": a["orphan"] == 0,
        "按库对账全齐%s" % ("(缺口 %s)" % gap if gap else ""): ep_converged,
        "emby_http=200": h["emby_http"] == 200,
        "mount=writes": h["mount_mode"] == "writes",
        "ffprobe无同路径反复超时(近30min)": pw["repeat"] == 0,
        "ffprobe超时率<=5%(近30min)": 0 <= pw["rate"] <= 0.05,
        "ffprobe无OOM-kill": h.get("probe_killed", 0) == 0,
        "磁盘可用>8G": h["avail_mb"] > 8000,
    }
    return all(checks.values()), checks


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def pg_dump_gz(tmp_path):
    log("  pg_dump teldrive -> gzip")
    r = subprocess.run(
        ["docker", "exec", "teldrive-postgres", "pg_dump", "-U", "teldrive", "-d", "teldrive"],
        capture_output=True, timeout=900)
    if r.returncode != 0:
        log("  ! pg_dump rc=%d %s" % (r.returncode, (r.stderr or b"").decode("utf-8", "ignore")[:200]))
        return None
    with open(tmp_path, "wb") as f:
        with gzip.GzipFile(fileobj=f, mode="wb", mtime=0) as gz:
            gz.write(r.stdout)
    return os.path.getsize(tmp_path)


def pack(snap, audit, checks):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = "embytg_backup_%s" % ts
    out_tar = os.path.join(BK, name + ".tar.gz")
    log("PACK: %s" % out_tar)
    included, missing = [], []
    manifest = {
        "backup_name": name,
        "created": datetime.now().strftime("%F %T %z"),
        "host": "root@132.145.125.171:22323",
        "note": "EmbyTG 刮削完成+正常运营 全量明文备份。恢复见 docs/PROJECT.md。",
        "snapshot": snap,
        "audit": {k: audit[k] for k in ("bad", "dups", "orphan", "no_pid")},
        "checks": checks,
        "files": [],
    }
    # 2026-11-11 恢复演练发现：备份包从未包含 tg.db（片单台账）→ 先 checkpoint 再打包
    try:
        import sqlite3 as _sq
        _c = _sq.connect(os.path.join(PIPE, "tg.db"))
        _c.execute("pragma wal_checkpoint(TRUNCATE)")
        _c.close()
    except Exception as e:                                         # noqa: BLE001
        log("  ! tg.db checkpoint 失败：%s" % str(e)[:60])
    # 2026-11-23 周度体检修根因：BACKUP_FILES 是模块级全局，pack() 每调一次就
    # 无条件 append 一份 tg.db（没有 _seen_rel 闸门）。--loop 进程跑 41 轮后，
    # 包里塞了 41 份 state/tg.db —— 实测 20261123_071921 包解开 state/ = 5536MB，
    # 压缩后 1.66GB，其中 5.5GB 是同一份 141MB 台账的副本。同时进程内还留着启动时
    # 存在、之后已删除的旧条目（docs/BOX_CONTROL_20261113.md 等），让「关键文档齐」
    # 这条判据对长跑进程恒定 FAIL。
    # 修法：每轮在本地副本上组装、按包内相对路径去重，绝不污染全局。
    files, _seen_rel = [], set()
    for _src, _rel in BACKUP_FILES:
        if _rel in _seen_rel:
            continue
        _seen_rel.add(_rel)
        files.append((_src, _rel))
    for _src, _rel in ((os.path.join(PIPE, "tg.db"), "state/tg.db"),):
        if os.path.isfile(_src) and _rel not in _seen_rel:
            _seen_rel.add(_rel)
            files.append((_src, _rel))

    # 2026-11-13 灾备改造：脚本/文档不再手写白名单，改成按目录自动发现。
    # 根因：手写名单漏了 35 个现役脚本（gha_next3/gha_finish/gha_state/pc_upload/
    # _scan_now/_refresh_lib/lane_queue/cron_lint/improve_guard/... ），
    # 恢复出来的机器 Emby 能起但一集片子都收不进来。
    # 规则：pipeline 下所有 .py/.sh 全收（排除 __pycache__、*.bak*、*.pyc、日志）；
    # 文档目录 docs/ 全收 .md；/etc/cron.d/embytg_* 与 systemd unit 全收。
    for _dir, _arc, _exts in (
            (PIPE, "scripts/", (".py", ".sh")),
            # 2026-11-14：RUNNER_DIR 必须**连非脚本的凭据文件一起收**。
            # 这里原先只收 .py/.sh，于是 runner.env（TG_API_ID/HASH、
            # TG_SESSION_STRING、TG_STORE_CH、限速与超时）从未进过任何一个包 ——
            # 空机恢复后脚本都在，却一个 TG 账号都登不上，五台上片节点全起不来。
            # 判据：把「脚本」和「运行必需文件」分成两个 exts 集合。
            (RUNNER_DIR, "scripts/", (".py", ".sh", ".env")),
            (os.path.join(ROOT, "docs"), "docs/", (".md",)),
            (os.path.join(PIPE, "docs"), "docs/", (".md",)),
            # 2026-11-14 自省修复：docs/archive/ 被归档后没进包 ——
            # OPS_JOURNAL / OPS1_MANUAL / OPS1_SELFWAKE 这三份「运维审计脉络」
            # 只在 archive 子目录里，而上面两行只 listdir 一层，备份包里静默缺失
            # （LATEST.json files_missing 记了三条，但没人当错误看）。
            # 归档文档是「换机后唯一能解释历史决策的证据链」，必须一起走。
            (os.path.join(ROOT, "docs", "archive"), "docs/archive/", (".md",)),
            (os.path.join(PIPE, "cron_specs"), "cron/", (".json",))):
        if not os.path.isdir(_dir):
            continue
        for _f in sorted(os.listdir(_dir)):
            if not _f.endswith(_exts):
                continue
            if ".bak" in _f or _f.startswith("__"):
                continue
            _rel = _arc + _f
            if _rel in _seen_rel:
                continue
            _seen_rel.add(_rel)
            files.append((os.path.join(_dir, _f), _rel))
    for _f in sorted(glob.glob("/etc/cron.d/embytg_*") + glob.glob("/etc/cron.d/p03_*")):
        _rel = "systemd/" + os.path.basename(_f)
        if _rel not in _seen_rel:
            _seen_rel.add(_rel)
            files.append((_f, _rel))
    for _f in sorted(glob.glob("/etc/systemd/system/embytg-*")):
        if os.path.isdir(_f):
            # drop-in 目录（*.service.d）本身是配置真源，但 os.path.isfile 会判 False
            # → 旧代码只在 files_missing 里记一条目录名，内容一份都没进包（换机后
            # drop-in 的 Environment=/TimeoutStartSec 全丢）。展开成逐个文件。
            for _g in sorted(glob.glob(os.path.join(_f, "*"))):
                if not os.path.isfile(_g):
                    continue
                _rel = "systemd/%s/%s" % (os.path.basename(_f), os.path.basename(_g))
                if _rel not in _seen_rel:
                    _seen_rel.add(_rel)
                    files.append((_g, _rel))
            continue
        _rel = "systemd/" + os.path.basename(_f)
        if _rel not in _seen_rel:
            _seen_rel.add(_rel)
            files.append((_f, _rel))
    log("  BACKUP_FILES 自动发现后共 %d 项" % len(files))

    tmp_dump = os.path.join(BK, ".teldrive_dump_%s.sql.gz" % ts)
    try:
        with tarfile.open(out_tar, "w:gz", compresslevel=6) as tf:
            # 1) 静态文件
            for src, rel in files:
                if not os.path.isfile(src):
                    missing.append(rel)
                    continue
                # 2026-11-13（E029）：library.db 是 **WAL 模式**（实测有 4.2MB library.db-wal）。
                # Emby 官方 WAL 文档明确：只复制主文件、而提交还在 WAL 里，会丢掉那些提交。
                # 这不是"可能"，是时序问题 —— 扫描写入高峰期必然命中，恢复出来就是少条目。
                # 正确做法：SQLite 在线备份 API（con.backup），它在 WAL 模式下产出一致快照，
                # 且不需要停 Emby。tg.db 那边之前用 wal_checkpoint(TRUNCATE) 是因为没有
                # 别的进程独占它；library.db 被 Emby 持有，checkpoint 不可靠，只能用 backup API。
                if rel == "emby/library.db":
                    snap_db = os.path.join(BK, ".library_snap_%s.db" % ts)
                    try:
                        import sqlite3 as _sq2
                        _s = _sq2.connect(src, timeout=60)
                        _d = _sq2.connect(snap_db)
                        with _d:
                            _s.backup(_d)
                        _d.close()
                        _s.close()
                        tf.add(snap_db, arcname=os.path.join(name, rel))
                        included.append(rel + " (sqlite backup snapshot)")
                        os.remove(snap_db)
                        continue
                    except Exception as e:                 # noqa: BLE001
                        log("  ! library.db 在线备份失败，回退直接复制：%s" % str(e)[:80])
                        for _p in (snap_db,):
                            if os.path.exists(_p):
                                os.remove(_p)
                tf.add(src, arcname=os.path.join(name, rel))
                included.append(rel)
            # 2) teldrive DB dump
            sz = pg_dump_gz(tmp_dump)
            if sz:
                tf.add(tmp_dump, arcname=os.path.join(name, "teldrive/teldrive_dump.sql.gz"))
                included.append("teldrive/teldrive_dump.sql.gz (%d B)" % sz)
            # 3) 审计原文
            if os.path.isfile(audit["path"]):
                tf.add(audit["path"], arcname=os.path.join(name, "stats/final_audit.txt"))
            # 4) README
            readme = (
                "EmbyTG 全量备份 %s\n\n"
                "触发条件（用户 m04207）：Emby 刮削完 + 完全正常运营。\n"
                "自动化：/opt/embytg/pipeline/auto_finalize.py --loop\n\n"
                "目录：\n"
                "  emby/        library.db + system.xml（Emby 全部媒体库/条目/配置）\n"
                "  teldrive/    teldrive DB pg_dump（files / file_parts / channels 全部映射）\n"
                "  config/      全部明文凭证与端点（config.json 为唯一真源）\n"
                "  sessions/    两个 TG StringSession\n"
                "  systemd/     rclone mount unit（当前 --vfs-cache-mode writes）\n"
                "  docs/        PROJECT.md / AGENTS.md / README.md / ERRORS.md\n"
                "  scripts/     现行主力脚本（含 auto_finalize.py）\n"
                "  state/       队列/去重/元数据缓存/库配置备份\n"
                "  stats/       final_audit.txt（收敛时审计输出）\n"
                "  manifest.json  快照/审计/校验清单\n\n"
                "恢复顺序：先起 compose.yaml 的 teldrive+postgres，导入 teldrive_dump.sql.gz，\n"
                "再按 config/rclone.conf + systemd unit 挂 rclone，最后起 emby 并还原 library.db。\n"
                "DATA_KEY / SIGNING_KEY 在 config/.env，丢失则 TG 文件映射永久不可解。\n" % ts)
            b = readme.encode("utf-8")
            info = tarfile.TarInfo(os.path.join(name, "README.txt"))
            info.size = len(b)
            tf.addfile(info, io.BytesIO(b))
            # 5) manifest（先写不含自身 sha 的版本，tar 收尾后再算包 sha 写 LATEST）
            # 2026-11-14：`files` 字段从来没人赋值 —— 实测最近三个包的 manifest.json
            # 里 "files": [] 恒空，等于「意图合约」根本不存在，任何 restore 校验都
            # 无从比对（这正是 docs/archive 三份文档漏包一整天没被发现的直接原因）。
            manifest["files"] = included
            manifest["files_included"] = included
            manifest["files_missing"] = missing
            # 2026-11-14 自省：缺失清单以前只写进 manifest，没人当错误看 ——
            # 三份运维审计文档（OPS_JOURNAL/OPS1_MANUAL/OPS1_SELFWAKE）在
            # docs/archive/ 里静默漏了整整一天。现在把「关键文档缺失」升为 FAIL 判据：
            # 它们不是可选项，是换机后唯一能解释历史决策的证据链。
            _crit = [m for m in missing if m.startswith("docs/")]
            if _crit:
                checks["备份关键文档齐(无 docs/ 缺失)"] = False
                log("  !! 关键文档缺失 %d 项：%s" % (len(_crit), ", ".join(_crit[:6])))
            else:
                checks["备份关键文档齐(无 docs/ 缺失)"] = True
            mb = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
            mi = tarfile.TarInfo(os.path.join(name, "manifest.json"))
            mi.size = len(mb)
            tf.addfile(mi, io.BytesIO(mb))
        with open(os.path.join(BK, name + "_manifest.json"), "wb") as f:
            f.write(json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
        digest = sha256(out_tar)
        size = os.path.getsize(out_tar)
        # 2026-11-11 防复发断言：单包 >200MB 说明 BACKUP_FILES 又被塞了目录树/日志/WAL
        # （19:33 曾达 1.2G / 43327 文件）。只告警不删包（打包已完成），但必须留痕可查。
        if size > 200 * 1048576:
            log("  !! BACKUP_BLOAT: 包 %.1fMB > 200MB，疑似 BACKUP_FILES 被塞入目录树；请净化清单（净化脚本见 docs/R2_BACKUP.md）" % (size / 1048576.0))
        latest = {
            "state": "DONE", "created": manifest["created"],
            "archive": out_tar, "archive_name": name + ".tar.gz",
            "sha256": digest, "size_bytes": size,
            "snapshot": snap,
            "audit": manifest["audit"], "checks": checks,
            "files_included": len(included), "files_missing": missing,
        }
        tmp = LATEST + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(latest, f, ensure_ascii=False, indent=2)
        os.replace(tmp, LATEST)
        log("  PACK DONE %s / %.1f MB / sha256=%s / 文件 %d 缺失 %d"
            % (latest["archive_name"], size / 1048576.0, digest[:16], len(included), len(missing)))
        # 只保留最近 KEEP_PACKS 个全量包（本地备份目录不要无限长）
        try:
            _packs = sorted(glob.glob(os.path.join(BK, "embytg_backup_*.tar.gz")),
                            key=os.path.getmtime, reverse=True)
            for _old in _packs[KEEP_PACKS:]:
                os.remove(_old)
                _mf = _old.replace(".tar.gz", "_manifest.json")
                if os.path.exists(_mf):
                    os.remove(_mf)
                log("  清理旧包 %s" % os.path.basename(_old))
        except Exception as e:                                 # noqa: BLE001
            log("  ! 清理旧包失败：%s" % str(e)[:60])
        return latest
    finally:
        if os.path.exists(tmp_dump):
            os.remove(tmp_dump)


# ---------------------------------------------------------------- 状态机
def step(s, force_pack=False):
    cur = snapshot()
    last = s.get("last")
    s["history"] = (s.get("history") or [])[-19:] + [{
        "ts": cur["ts"], "state": cur["state"], "ep": cur["episode"],
        "ff": cur["ffprobe"], "ser_empty": cur["ser_empty"]}]
    log("%s | scan=%s prog=%s | M/S/E=%s/%s/%s | Series空=%s | ffprobe=%s | mount=%s http=%s"
        % (s["stage"], cur["state"], cur["prog"], cur["movie"], cur["series"],
           cur["episode"], cur["ser_empty"], cur["ffprobe"],
           cur["health"]["mount_mode"], cur["health"]["emby_http"]))

    if s["stage"] == "MONITOR":
        if force_pack:
            # 应急：跳过收敛等待，先即时体检一次，然后直接打包（不补刮、不回 HOLD）
            log("  FORCE-PACK：跳过收敛等待，即时体检后打包")
            a = run_audit()
            _, checks = audit_ok(a, cur)
            s["audit_result"] = {k: a[k] for k in ("bad", "dups", "orphan", "no_pid")}
            s["checks"] = checks
            s["stage"] = "PACK"
        elif converged(cur, last, s):
            log("  -> REFRESH (收敛 stable=%d)" % s["stable"])
            s["stage"] = "REFRESH"
        s["last"] = cur

    elif s["stage"] == "REFRESH":
        run_refresh()
        s["refresh_rounds"] = s.get("refresh_rounds", 0) + 1
        s["stage"] = "AUDIT"
        s["last"] = cur

    elif s["stage"] == "AUDIT":
        a = run_audit()
        ok, checks = audit_ok(a, cur)
        log("  AUDIT bad=%d dups=%d orphan=%d no_pid=%d Episode=%d checks=%s"
            % (a["bad"], a["dups"], a["orphan"], a["no_pid"], cur["episode"], checks))
        s["audit_result"] = {k: a[k] for k in ("bad", "dups", "orphan", "no_pid")}
        s["checks"] = checks
        s["last"] = cur
        if ok:
            s["stage"] = "PACK"
            log("  -> PACK（全部达标）")
        elif s["refresh_rounds"] < MAX_REFRESH:
            log("  -> 回 REFRESH 再补一轮 (round %d)" % s["refresh_rounds"])
            s["stage"] = "REFRESH"
        else:
            # 达到补刮上限仍不达标：不强行打包残缺库，进入 HOLD 等自然补齐
            log("  -> HOLD（补刮已达 %d 轮仍未达标，等扫描/后台补齐，不提前备份）" % s["refresh_rounds"])
            s["stage"] = "HOLD"
            s["hold_rounds"] = 0

    elif s["stage"] == "HOLD":
        # 每轮重新体检；达标才 PACK；每 HOLD_REFRESH_EVERY 轮再补一次刮（防止死等）
        s["hold_rounds"] = s.get("hold_rounds", 0) + 1
        a = run_audit()
        ok, checks = audit_ok(a, cur)
        s["audit_result"] = {k: a[k] for k in ("bad", "dups", "orphan", "no_pid")}
        s["checks"] = checks
        s["last"] = cur
        if ok:
            log("  HOLD 第 %d 轮体检达标 -> PACK" % s["hold_rounds"])
            s["stage"] = "PACK"
        elif s["hold_rounds"] >= HOLD_REFRESH_EVERY:
            log("  HOLD 第 %d 轮仍差 bad=%d，再补一轮 NFO 直写" % (s["hold_rounds"], a["bad"]))
            s["hold_rounds"] = 0
            s["refresh_rounds"] = MAX_REFRESH - 1   # 允许再跑一次 REFRESH 后回 AUDIT/HOLD
            s["stage"] = "REFRESH"
        else:
            log("  HOLD 第 %d 轮 bad=%d 未达标，继续等（不打包）" % (s["hold_rounds"], a["bad"]))

    elif s["stage"] == "PACK":
        a = {"bad": (s.get("audit_result") or {}).get("bad", -1),
             "dups": (s.get("audit_result") or {}).get("dups", -1),
             "orphan": (s.get("audit_result") or {}).get("orphan", -1),
             "no_pid": (s.get("audit_result") or {}).get("no_pid", -1),
             "path": os.path.join(BK, "final_audit.txt"), "raw": ""}
        latest = pack(cur, a, s.get("checks") or {})
        s["stage"] = "DONE"
        s["latest"] = latest
        s["notes"].append("DONE %s" % latest["created"])
        log("  -> DONE，LATEST.json 已写，等待本地 pull_backup.ps1 拉回 F:\\emby\\backup\\")

    elif s["stage"] == "DONE":
        # 2026-11-13 灾备改造：原来是「DONE 就一直待机，等 PC 侧 pull_backup.ps1 拉回才重打」。
        # 用户已明确 PC 不再常驻，那条触发链断了两天，R2 每天上传的仍是 11-11 的旧包。
        # 改成自驱：包龄 >= PACK_MAX_AGE_H 或集数较包内快照涨 >= PACK_EP_DELTA 就重新打包。
        latest = s.get("latest") or {}
        age_h = 1e9
        try:
            _c = datetime.strptime((latest.get("created") or "").strip()[:19], "%Y-%m-%d %H:%M:%S")
            age_h = (datetime.now() - _c).total_seconds() / 3600.0
        except Exception:                                      # noqa: BLE001
            pass
        prev_ep = int(((latest.get("snapshot") or {}).get("episode")) or 0)
        ep_now = int(cur.get("episode") or 0)
        due = age_h >= PACK_MAX_AGE_H or (prev_ep and ep_now - prev_ep >= PACK_EP_DELTA)
        if due:
            log("  -> 重新打包：包龄 %.1f h (阈值 %d) / 集数 %d->%d (增量阈值 %d)"
                % (age_h, PACK_MAX_AGE_H, prev_ep, ep_now, PACK_EP_DELTA))
            # 跳过 REFRESH/HOLD 长循环：先即时体检，达标即打，不达标也打（灾备优先于完美）
            a = run_audit()
            _, checks = audit_ok(a, cur)
            s["audit_result"] = {k: a[k] for k in ("bad", "dups", "orphan", "no_pid")}
            s["checks"] = checks
            s["refresh_rounds"] = 0
            s["stage"] = "PACK"
        else:
            log("  DONE，待机（包 %s，包龄 %.1f h，集数 +%d）"
                % (latest.get("archive_name"), age_h, max(0, ep_now - prev_ep)))

    save_state(s)
    return s


def main():
    a = sys.argv[1:]
    os.makedirs(BK, exist_ok=True)
    if "--status" in a:
        print(json.dumps(load_state(), ensure_ascii=False, indent=2))
        return
    # 单实例锁
    lf = open(LOCK, "w")
    try:
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("已有 auto_finalize 实例在运行，退出")
        return
    s = load_state()
    if "--once" in a:
        step(s)
        return
    if "--force-pack" in a:
        # 应急：一次调用内连续推进到 DONE，确保立即产出备份包。
        # 2026-11-13：DONE 阶段的定时重打也受 PACK_MAX_AGE_H 约束，
        # force-pack 必须无视该阈值（本参数的全部意义就是「现在就打」）。
        log("FORCE-PACK 调用：连续推进状态机直到 DONE（忽略包龄阈值）")
        if s.get("stage") == "DONE":
            s["stage"] = "PACK"
            s["refresh_rounds"] = 0
        for _ in range(8):
            step(s, force_pack=(s["stage"] == "MONITOR"))
            if s["stage"] in ("DONE", "HOLD", "BLOCKED"):
                break
        return
    log("LOOP 启动 (gap=%ss EP_TARGET=%s AUDIT_BAD_MAX=%s) stage=%s"
        % (SAMPLE_GAP, EP_TARGET, AUDIT_BAD_MAX, s["stage"]))
    while True:
        try:
            s = step(s)
            if s["stage"] == "DONE":
                # DONE 后降频，仍定期确认包完整
                time.sleep(SAMPLE_GAP * 5)
            else:
                time.sleep(SAMPLE_GAP)
        except Exception as e:
            log("! 轮次异常: %s" % str(e)[:300])
            time.sleep(SAMPLE_GAP)


if __name__ == "__main__":
    main()
