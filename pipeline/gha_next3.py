#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gha_next3.py — claim one task from the new/fill/upgrade lanes.

The three lanes share one global claim lock and one source-lease namespace:

  new      : whole series absent from the TG library, highest real quality
  fill     : missing episodes in an existing series, current series quality
  upgrade  : existing episode below 2160p, replace only with higher resolution
  upgrade2 : second runner shard of the same upgrade queue; a stable hash
             partition keeps the two shards from claiming the same episode

No media is downloaded here. stdout is JSON only; it is redirected to the
runner's private job file and must not be printed in Actions logs.
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
import time
import urllib.parse

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import autoingest as A          # noqa: E402
import gha_state as GS          # noqa: E402
import post_ingest as P         # noqa: E402
import source_pull as SP        # noqa: E402
import src_guard as SG          # noqa: E402
import lane_queue as LQ         # noqa: E402
import quality_gate as QG       # noqa: E402 —— 画质判据唯一入口（两条线：STOCK/ADMIT）

LOCK = os.path.join(HERE, ".gha_next.lock")
# 2026-11-13 软租约改造后，源解析不再整源排除被占用的源（那会让第 4/5 台
# 机器永远 NOJOB）。这里保留一个模块级硬排除集合，留空；将来若某个源彻底
# 不可用需要全局禁用，就往这里加，不要再用「被 claim 占用」当排除理由。
HARD_EXCLUDE = frozenset()
PAUSE_FILE = os.path.join(HERE, ".gha_paused")
CHANNELS_FILE = os.path.join(HERE, ".gha_channels.json")
CHANNEL_RR = os.path.join(HERE, ".gha_channel_rr")
NEW_DONE = os.path.join(HERE, ".gha_new_done.json")

# 2026-11-17（E160）：「源站没有这一集」的按集冷却时长（小时）。
# 之前这种负证据一条都不落，同一集每轮重搜（今日 335 次源请求里 238 次
# 落在重复 target 上）。默认 6h：新上架的集最多晚 6h 被补到，
# 而重复请求从「每小时几十次」降到「每天 4 次」。
FILL_NO_SOURCE_COOL_HOURS = float(os.environ.get("FILL_NO_SOURCE_COOL_HOURS") or 6)

# 这些 why 意味着「根本没向源站发请求」，不应占用 --max-attempts 名额。
CHARGE_FREE_WHY = {
    "cooldown", "series_active", "no_missing",           # 本地判据即可跳
    "hint_source_busy", "source_budget",                  # 预算/租约
    "hint_fetch_failed",                                  # 点名源当次取不到（30min 后重试）
}
NEW_BUILDER = os.path.join(HERE, "build_three_queues.py")
NEW_REFRESH = os.path.join(HERE, ".gha_new_refresh.json")
# 2026-11-15（E131）：老身份名 Codex-Korea-Ingest 是韩国节点时代的遗留，
# 它让协作日志看起来「一直有个 Codex 在干活」，实际是这三条脚本的机械写入。
# 用户已确认现在只有 hermes-ops 在维护，改成中性名以免误导排查。
AGENT = "embytg-pipeline"
# 授权闸门的自动重试间隔（6 小时）：够长不白烧配额，够短能自愈
GATE_RETRY_SEC = int(os.environ.get("GATE_RETRY_SEC", "21600"))
PLACEHOLDER = re.compile(r"^第\s*\d+\s*[集期]$")
CHANNEL_PART_LIMIT = 480000
# 2026-11-15（E106）：**lane 不再写死，从 config/scale.json 动态生成。**
#
# 原来 LANES/CURSOR_FILES/QUEUE_FILES 三张表都手写 new/new2/new3/new4/new5，
# 加一台机器就得改三处代码，而且 mode 与 lane 名的映射散落在十几处
# `if lane in ("new","new2",...)` 里 —— 加 new6 时漏改一处就是静默 bug。
#
# 现在：lane 名、归属机器、领哪个队列，全部来自配置文件。
# 加机器 = 往 config/scale.json 的 lanes.items 加一行，代码不用动。
def _load_scale():
    try:
        with open(os.path.join(HERE, "config", "scale.json"), encoding="utf-8") as fh:
            return json.load(fh) or {}
    except Exception:                                      # noqa: BLE001
        return {}


_SCALE = _load_scale()
_LANE_ITEMS = [x for x in (_SCALE.get("lanes", {}).get("items") or [])
               if isinstance(x, dict) and x.get("lane") and x.get("enabled", True)]

# lane → 领哪个队列（new/fill/upgrade）
LANE_MODE = {x["lane"]: (x.get("mode") or "new") for x in _LANE_ITEMS}
# 配置里没有时兜底，保证老逻辑仍能跑
if not LANE_MODE:
    LANE_MODE = {"new": "new", "new2": "new", "new3": "new", "new4": "new",
                 "new5": "new", "fill": "fill", "upgrade": "upgrade",
                 "upgrade2": "upgrade"}

LANES = tuple(LANE_MODE.keys())
CURSOR_FILES = {ln: os.path.join(HERE, ".gha_cursor_%s" % ln) for ln in LANES}
_QF = os.path.join(HERE, ".gha_queue_%s.json")
QUEUE_FILES = {ln: (_QF % LANE_MODE[ln]) for ln in LANES}
Q_RANK = {"2160p": 4, "1440p": 3, "1080p": 2, "720p": 1, "sd": 0}
SE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")
VIDEO_MAGIC = (b"\x1aE\xdf\xa3", b"RIFF", b"\xff\xd8")
_EMBY_EP_CACHE = {}
_EMBY_NAME_CACHE = {}
# 2026-11-14：负结果不能永久缓存。
# 原来 `_EMBY_EP_CACHE[want] = ({}, "")` 对「Emby 里还没这部剧」写死进进程内字典，
# 同一进程后续调用全部命中空 mapping → 该剧每一集都被判「缺」→ 逐集探测 → duplicate 洪水。
# 王冠（tmdb 65494）今天 00:11–04:23 因此被记 160 条 duplicate：剧是**刚入库**，
# 六条 lane 每轮都撞一次，直到 new_done 冷却才停。
# 规则：空 mapping 只缓存 EMBY_NEG_TTL 秒；有内容的按 EMBY_POS_TTL 秒。
EMBY_NEG_TTL = float(os.environ.get("EMBYTG_EMBY_NEG_TTL", "90"))
EMBY_POS_TTL = float(os.environ.get("EMBYTG_EMBY_POS_TTL", "1800"))


class BadSource(RuntimeError):
    def __init__(self, label, reason):
        super().__init__(reason)
        self.label = str(label or "")
        self.reason = str(reason or "source_preflight_failed")[:200]


# 2026-11-14（E075）：本次进程持有的 TG session 租约 (slot, run_id) 或 None。
# 领片成功 → 交给 claim（由 gha_cancel/gha_finish 归还）；
# 领片失败 / 异常 → 由 finally 里的 release_held_lease() 立刻归还。
_HELD_LEASE = None


def hold_lease(slot, rid):
    global _HELD_LEASE
    _HELD_LEASE = (slot, rid)


def release_held_lease():
    """归还本进程仍持有的租约（成功领片后调用 clear_held_lease 避免误还）。"""
    global _HELD_LEASE
    if _HELD_LEASE is None:
        return
    slot, rid = _HELD_LEASE
    _HELD_LEASE = None
    try:
        GS.slot_release(slot, rid)
    except Exception:                                  # noqa: BLE001
        pass


def clear_held_lease():
    """领片成功：租约随 claim 走，本进程不再负责归还。"""
    global _HELD_LEASE
    _HELD_LEASE = None


def emit(obj):
    print(json.dumps(obj, ensure_ascii=False), flush=True)


def read_claim(lane="fill"):
    return GS.read_claim(lane)


def read_claims():
    return GS.read_claims()


def write_claim(lane, job):
    GS.write_claim(lane, job)


def refresh_new_queue(limit=500, cooldown=3600):
    """Refill the permanent new lane from TODB without touching fill/upgrade."""
    try:
        last = float(json.load(open(NEW_REFRESH, encoding="utf-8")).get("ts") or 0)
    except Exception:                                      # noqa: BLE001
        last = 0
    if time.time() - last < cooldown:
        return {"ok": False, "reason": "new_builder_cooldown"}
    if not os.path.exists(NEW_BUILDER):
        return {"ok": False, "reason": "new_builder_missing"}
    try:
        p = subprocess.run(
            [sys.executable, NEW_BUILDER, "--lane", "new",
             "--new-limit", str(limit)],
            cwd=HERE, capture_output=True, text=True, timeout=300)
        row = {"ts": int(time.time()), "rc": p.returncode,
               "stdout": (p.stdout or "")[-500:],
               "stderr": (p.stderr or "")[-500:]}
        with open(NEW_REFRESH, "w", encoding="utf-8") as fh:
            json.dump(row, fh, ensure_ascii=False)
        os.chmod(NEW_REFRESH, 0o600)
        return {"ok": p.returncode == 0, "reason": "built" if p.returncode == 0
                else "new_builder_failed", "detail": row}
    except Exception as e:                                 # noqa: BLE001
        return {"ok": False, "reason": "new_builder_failed",
                "detail": str(e)[:200]}


def broadcast(msg):
    try:
        subprocess.run([sys.executable, os.path.join(HERE, "agent_log.py"), "note",
                        "--name", AGENT, "--task", "韩国双槽上片",
                        "--broadcast", msg],
                       capture_output=True, text=True, timeout=30)
    except Exception:                                      # noqa: BLE001
        pass


def ingest_busy():
    try:
        p = subprocess.run(["pgrep", "-af", "autoingest.py --run|source_pull.py"],
                           capture_output=True, text=True, timeout=10)
        return any(("autoingest.py --run" in x or "source_pull.py" in x)
                   and "gha_" not in x for x in (p.stdout or "").splitlines())
    except Exception:                                      # noqa: BLE001
        return False


def duplicate_log(item, ep, key, target, why, season=None):
    try:
        with open(os.path.join(HERE, "logs", "autoingest.log"), "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.strftime("%F %T"), "event": "duplicate",
                                "source": "github_actions",
                                "lane": item.get("lane"), "series": item["series"],
                                # 2026-11-14：以前取 item.get("season")，但命中 duplicate 时
                                # item2 还没设置 season → 王冠 160 条日志全是 season:null，
                                # 无法区分「S01E1 还是 S02E1 被判重」，排查成本翻倍。
                                # 优先用调用方给的 season（来自 by_ep 的 key）。
                                "season": (season if season is not None
                                           else item.get("season")),
                                "ep": ep, "key": key,
                                "why": why, "target": target},
                               ensure_ascii=False) + "\n")
    except Exception:                                      # noqa: BLE001
        pass


def channel_pool():
    channels = []
    limit = CHANNEL_PART_LIMIT
    try:
        data = json.load(open(CHANNELS_FILE, encoding="utf-8"))
        limit = int(data.get("part_limit") or limit)
        for row in data.get("channels") or []:
            cid = int(row.get("id") or 0)
            if cid and cid not in channels:
                channels.append(cid)
    except Exception:                                      # noqa: BLE001
        pass
    if not channels:
        try:
            cid = int(A.CFG.get("store_channel_id") or 0)
            if cid:
                channels.append(cid)
        except Exception:                                  # noqa: BLE001
            pass
    return channels, limit


def choose_store_channel():
    channels, part_limit = channel_pool()
    if not channels:
        return 0, []
    counts = {}
    try:
        rows = A.psql_rows(
            "select channel_id,count(*) from file_parts group by channel_id;")
        for line in rows.splitlines():
            f = line.split("|")
            if len(f) >= 2:
                counts[int(f[0])] = int(f[1])
    except Exception:                                      # noqa: BLE001
        counts = {}
    usable = [c for c in channels if counts.get(c, 0) < part_limit]
    if not usable:
        raise RuntimeError("所有存储频道都接近分片上限，需先加频道")
    idx = 0
    try:
        idx = int(open(CHANNEL_RR, encoding="utf-8").read().strip() or 0)
    except Exception:                                      # noqa: BLE001
        idx = 0
    selected = usable[idx % len(usable)]
    tmp = CHANNEL_RR + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str((idx + 1) % len(usable)))
    os.chmod(tmp, 0o600)
    os.replace(tmp, CHANNEL_RR)
    return selected, usable


def load_cursor(lane):
    try:
        return max(0, int(open(CURSOR_FILES[lane], encoding="utf-8").read().strip() or 0))
    except Exception:                                      # noqa: BLE001
        return 0


def save_cursor(lane, value):
    path = CURSOR_FILES[lane]
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(int(value)))
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def _session_speeds(now_min=180):
    """每个 session 的实测上传速率（MB/s）→ {slot: mbps}。

    2026-11-15（E134）：**选槽不能只看「谁最闲」，TG 会按账号当天流量限速。**
    实测 session1 当天已上传约 200GB，18:35 起从 4~5 MB/s 掉到 1.25 MB/s，
    而分配逻辑仍按「最闲优先」把活派给它 → 1.46GB 一集要传 19 分钟，
    近 3h 产能从 12 集/h 掉到 3 集/h（同机 session2 一直是 3.8 MB/s）。
    速率本来就记在 upload.log 里（`合计 X GB` / `用时 Ns`），读出来用即可。

    量法：**只看已完成任务**的 effective MB/s（合计/用时），每槽取最新一份；
    没有完成样本的在飞任务用「累计 MB/s」兜底。在飞长任务的累计值是全程
    平均，尾部变慢时会低估，不能当现状。
    """
    base = os.environ.get("EMBYTG_RUNNER_WORK", "/opt/embytg-runner/work")
    now = time.time()
    done, live = {}, {}
    try:
        names = os.listdir(base)
    except OSError:
        return {}
    for nm in names:
        p = os.path.join(base, nm, "upload.log")
        try:
            mt = os.path.getmtime(p)
            age = now - mt
            if age > now_min * 60:
                continue
            txt = open(p, encoding="utf-8", errors="ignore").read()[-6000:]
        except OSError:
            continue
        ss = re.findall(r"session=(\d+)", txt)
        if not ss:
            continue
        try:
            slot = int(ss[-1])
        except ValueError:
            continue
        if slot <= 0:
            continue
        _sz = re.findall(r"合计\s+([\d.]+)\s*GB", txt)
        _du = re.findall(r"用时\s+([\d.]+)s", txt)
        spd = None
        if _sz and _du and float(_du[-1]) > 0:
            spd = float(_sz[-1]) * 1024.0 / float(_du[-1])
            if slot not in done or mt > done[slot][0]:
                done[slot] = (mt, spd)
        else:
            _c = re.findall(r"累计\s+([\d.]+)\s*MB/s", txt)
            if _c and float(_c[-1]) > 0 and (slot not in live or mt > live[slot][0]):
                live[slot] = (mt, float(_c[-1]))
    out = {k: v[1] for k, v in live.items()}
    out.update({k: v[1] for k, v in done.items()})     # 完成样本优先
    return out


def _demote_throttled(order, speeds, ratio=0.5):
    """把被限速的 session 压到候选队尾（不是拉黑）。

    判据：本次速率 < ratio × 同机**最快**账号的速率 → 这次排最后。
    用相对值而不是绝对阈值：线路整体变慢时不会把全班都判死。
    一个健康候选都不剩（全班一起慢）→ 退回原顺序，排序不能让 lane 停摆；
    没有样本的槽视为健康（宁可试一下，也不能因为没数据就饿死账号）。
    """
    if not speeds or len(order) < 2:
        return list(order)
    top = max(speeds.values() or [0])
    if top <= 0:
        return list(order)
    good = [s for s in order if not (0 < speeds.get(s, 0) < ratio * top)]
    bad = [s for s in order if s not in good]
    return good + bad if good else list(order)


def session_slot(active, run_id=None):
    """挑一个**真正空闲**的 TG session 并独占它，返回 slot 号（1 起）。

    2026-11-14（E075）：旧版只统计 claim 里的 slot 标签取最小值。
    标签不等于占用 —— 两台机器会算出同一个"最小"，然后拿同一个 TG 账号
    从两个 IP 同时上传，TG 报 AuthKeyDuplicatedError 直接作废授权。

    现在必须拿到租约才算数；全被占则返回 0（调用方当 no_job 处理）。
    2026-11-15（E134）：租约顺序之后再过一道「限速降级」，见 _demote_throttled。
    """
    # 先按 claim 标签排个偏好顺序（省得每次都从 1 试起）
    counts = {}
    for d in active:
        try:
            slot = int(d.get("tg_session_slot") or 0)
        except Exception:                                  # noqa: BLE001
            slot = 0
        if slot:
            counts[slot] = counts.get(slot, 0) + 1
    # 2026-11-14（E075）：slot 数量必须跟实际配置的 TG 账号数一致。
    # 代码里写死 (1, 2) 是错的 —— 实测 runner.env 里 TG_SESSION_STRING_2 是空值，
    # 只有 1 个账号，于是"slot2"会把同一个账号第二次发出去，照样撞车。
    n_sess = _session_count()
    _res = reserved_slots()
    # 预留槽不参与上传（E081：tgpipe 独占某个账号做转存）
    _cand = [x for x in range(1, n_sess + 1) if x not in _res]
    order = sorted(_cand, key=lambda s: (counts.get(s, 0), s))
    # 2026-11-15（E134）：先按「最闲」排，再把被 TG 限速的账号压到队尾。
    try:
        _before = list(order)
        order = _demote_throttled(order, _session_speeds())
        if order != _before:
            print("session_slot: 限速降级 %s → %s" % (_before, order), file=sys.stderr)
    except Exception:                                      # noqa: BLE001
        pass                                               # 量速失败不影响领活

    for slot in order:
        if GS.slot_lease(slot, run_id or ("next3-%d" % os.getpid())):
            return slot
    return 0


def reserved_slots():
    """被别的进程预留的账号槽（例如 tgpipe 独占某个账号做转存）。

    2026-11-14（E082）：**不能只依赖环境变量**。
    实测云盒上跑的是旧版 run_one.sh，不会导出 RUNNER_RESERVED_SLOTS，
    于是 new4/new5 双双拿到 slot=3，同一个 TG 账号被两个进程同时用 ——
    这正是 AuthKeyDuplicated 的成因。
    所以这里「环境变量优先，读不到就直接读 runner.env」。
    """
    raw = os.environ.get("RUNNER_RESERVED_SLOTS", "").strip()
    if not raw:
        envf = os.environ.get("EMBYTG_RUNNER_ENV", "/opt/embytg-runner/runner.env")
        try:
            for line in open(envf, encoding="utf-8"):
                line = line.strip()
                if line.startswith("RUNNER_RESERVED_SLOTS="):
                    raw = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
        except Exception:                                  # noqa: BLE001
            raw = ""
    return {int(x) for x in raw.split(",") if x.strip().isdigit()}


def _session_count(default=1):
    """本机供**上传**使用的 TG session 数量。

    优先读 runner.env 里非空的 TG_SESSION_STRING[_N]；读不到就保守返回 1
    （单账号串行，宁可慢也不撞 AuthKeyDuplicated）。

    2026-11-14（E081）：`RUNNER_RESERVED_SLOTS` 可把若干槽位留给别的进程
    （例如 tgpipe 用账号3 做频道转存）。被预留的槽不算进上传池，
    避免两个进程抢同一个账号 —— 那样 TG 会作废整个授权。
    """
    envf = os.environ.get("EMBYTG_RUNNER_ENV", "/opt/embytg-runner/runner.env")
    reserved = reserved_slots()
    try:
        n = 0
        seen = set()
        for line in open(envf, encoding="utf-8"):
            line = line.strip()
            if not line.startswith("TG_SESSION_STRING"):
                continue
            _, _, v = line.partition("=")
            v = v.strip().strip('"').strip("'")
            if v and v not in seen:
                seen.add(v)
                n += 1
        # 注意：这里返回**账号总数**，预留只在 session_slot 的候选列表里去重。
        # 早先把预留扣在这里，又在下游加回来，导致范围算错（只拿到 slot1）。
        return n or default
    except Exception:                                  # noqa: BLE001
        return default


def strip_lib(lib):
    return (lib or "").split("/")[-1] or "国产剧"


def ext_of(episode):
    c = str(((episode.get("MediaSources") or [{}])[0]).get("Container")
            or episode.get("Container") or "").lower()
    if "mkv" in c:
        return ".mkv"
    if "mp4" in c or "m4v" in c:
        return ".mp4"
    return ".mkv"


def magic_ok(head):
    return head[:4] in VIDEO_MAGIC or b"ftyp" in head[:32]


def probe_source(job):
    """Read total size, first bytes and a short throughput sample.

    A 4MB Range sample with a 6s deadline rejects sources that connect but
    deliver bytes far below the floor (measured: ct.cdn.bgp.yt 0.06MB/s held
    a lane for hours).  The same probe still validates size/type before the
    bulk transfer starts.
    """
    url = str(job.get('source_url') or '')
    if not url:
        return 0, False
    sample = max(1 << 20, int(os.getenv('CLAIM_SPEED_BYTES', str(4 << 20))))
    min_mbps = float(os.getenv('CLAIM_MIN_MBPS', '1.0'))
    window = float(os.getenv('CLAIM_SPEED_SEC', '6'))
    headers = {
        'User-Agent': job.get('source_ua') or '',
        'Accept': '*/*',
        'Accept-Encoding': 'identity',
        'Range': 'bytes=0-%d' % (sample - 1),
    }
    label = job.get('source_label') or '?'
    t0 = time.time()
    got = 0
    head = b''
    total = 0
    timed_out = False
    try:
        with requests.Session() as s:
            s.trust_env = False
            r = s.get(url, headers=headers, stream=True,
                      timeout=(window, window), allow_redirects=True, verify=False)
            try:
                if r.status_code == 206:
                    match = re.search(r'/\s*(\d+)\s*$', r.headers.get('Content-Range') or '')
                    total = int(match.group(1)) if match else 0
                elif r.status_code == 200:
                    total = int(r.headers.get('Content-Length') or 0)
                else:
                    total = 0
                for block in r.iter_content(65536):
                    if not block:
                        continue
                    if len(head) < 64:
                        head += block[:64 - len(head)]
                    got += len(block)
                    if got >= sample or time.time() - t0 >= window:
                        break
            finally:
                r.close()
    except requests.exceptions.Timeout:
        timed_out = True
    except Exception as exc:
        print('SOURCE_SIZE_PROBE_FAILED source=%s error=%r' % (label, exc),
              file=sys.stderr, flush=True)
        return 0, False
    elapsed = max(time.time() - t0, 0.05)
    rate = got / elapsed / 1e6
    if total >= 8 * 1024 * 1024 and (timed_out or got >= (1 << 20) or elapsed >= window):
        if rate < min_mbps:
            raise BadSource(label, 'source_too_slow:%.2fMB/s' % rate)
    if timed_out and got == 0:
        raise BadSource(label, 'source_too_slow:no_data_%ds' % int(window))
    if total < 20 * 1024 * 1024 or total > 12 * 1024 * 1024 * 1024:
        print('SOURCE_SIZE_PROBE_IGNORED total=%d magic=%s source=%s' % (
            total, 'OK' if magic_ok(head) else 'BAD', label),
            file=sys.stderr, flush=True)
        total = 0
    return total, magic_ok(head)



def target_existing(item, ep):
    return A.target_path(item, ep)


def target_with_quality(lib, dirname, series, season, ep, quality, ext):
    return os.path.join(
        A.MNT, strip_lib(lib), dirname, "Season %02d" % int(season),
        "%s S%02dE%02d - %s%s" % (series, int(season), int(ep), quality, ext))


def rank_of(label):
    return Q_RANK.get((label or "sd").lower(), 0)


def upgrade_shard(item):
    """Stable two-way partition; pruning the queue cannot move an item."""
    raw = "%s|%s|%s|%s" % (
        item.get("tmdb"), item.get("season"), item.get("ep"),
        item.get("current_rel") or item.get("series") or "")
    return int(hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8], 16) % 2


def new_shard(item, shards=2):
    """Stable partition of the new queue (new / new2 / new3 / new4 / new5)."""
    raw = "%s|%s|%s|%s" % (
        item.get("tmdb"), item.get("source_series_id"),
        item.get("series") or "", item.get("year") or "")
    return int(hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8], 16) % max(2, int(shards))


def invalid_candidate(episode, size, min_bytes, max_bytes):
    name = str(episode.get("Name") or "").strip()
    if not size or int(size) < min_bytes:
        return "size=%s" % size
    if PLACEHOLDER.match(name):
        return "placeholder=%s" % name
    if int(size) > max_bytes:
        return "too_big=%.2fGB" % (int(size) / 1e9)
    return ""


def check_existing(item, season, ep, target, quality):
    if not target:
        return None, "no_target"
    if os.path.exists(target):
        return None, "path_exists"
    qual = quality or A.qual_of(os.path.basename(target))
    key = A.state_key_for(item["tmdb"], season, ep, os.path.basename(target))
    try:
        has_state = A.state_key_exists(key)
        has_res = A.resource_has_episode(
            {"tmdb": item["tmdb"], "season": season}, ep, qual)
        has_td = A.td_path_exists(target)
    except Exception as e:                                 # noqa: BLE001
        return key, "check_fail:%s" % str(e)[:80]
    why = ",".join(x for x, ok in (("state_key", has_state),
                                   ("resource_state", has_res),
                                   ("teldrive", has_td)) if ok)
    return key, why


def _series_abs(item):
    return os.path.normpath(os.path.join(
        A.MNT, strip_lib(item["lib"]), item["dir"]))


def emby_real_series_name(item):
    """Series name Emby actually exposes for this local folder, or "".

    The queue's ``series`` field is the source站's title (``装甲骑兵 VOTOMS``);
    Emby shows the TMDB title (``装甲骑兵 波德姆兹``).  Looking the folder up by
    the queue name silently finds nothing, so Emby cross-check degrades to 0
    and the same already-ingested series gets rescanned every cursor cycle.
    """
    want = _series_abs(item)
    if want in _EMBY_NAME_CACHE:
        return _EMBY_NAME_CACHE[want]
    name = ""
    try:
        q = urllib.parse.urlencode({
            "Recursive": "true", "IncludeItemTypes": "Series",
            "Fields": "Path", "Limit": "5000",
        })
        for series in (P.emby("/Items?" + q) or {}).get("Items", []):
            got = os.path.normpath(P.emby_path_to_local(series.get("Path") or ""))
            if got == want:
                name = series.get("Name") or ""
                break
    except Exception:                                      # noqa: BLE001
        name = ""
    _EMBY_NAME_CACHE[want] = name
    return name


def emby_effective_episodes(item):
    """Map Emby's effective (season, episode) to the indexed paths.

    A file can live in ``Season 02`` while its sidecar NFO says
    ``<season>1</season>``.  Path-only dedupe then misses the duplicate that
    Emby actually shows as S01.  NFO is the project's source of truth and Emby
    is its indexed result, so this map is the authoritative cross-check.
    """
    want = _series_abs(item)
    _hit = _EMBY_EP_CACHE.get(want)
    if _hit is not None:
        _mapping, _why, _ts = _hit
        _ttl = EMBY_NEG_TTL if not _mapping else EMBY_POS_TTL
        if time.time() - _ts < _ttl:
            return _mapping, _why
    try:
        q = urllib.parse.urlencode({
            "Recursive": "true", "IncludeItemTypes": "Series",
            "Fields": "Path,Name", "Limit": "5000",
        })
        rows = (P.emby("/Items?" + q) or {}).get("Items", [])
        sid = None
        by_path, by_name = {}, {}
        for series in rows:
            got = os.path.normpath(P.emby_path_to_local(series.get("Path") or ""))
            by_path[got] = series
            name = (series.get("Name") or "").strip().lower()
            if name:
                by_name.setdefault(name, series)
        hit = by_path.get(want)
        if not hit:
            # 目录名里的标题（去掉年份/tmdb 后缀）与 Emby Name 对不上时退一步按名字找
            folder = os.path.basename(want)
            guess = folder.split(" (")[0].strip().lower()
            hit = by_name.get(guess)
            if hit:
                _EMBY_NAME_CACHE[want] = hit.get("Name") or ""
        if not hit:
            # 2026-11-14：这里返回的 why 以前是 ""，等于告诉调用方
            # 「Emby 查到了、只是零集」。真相是「Emby 里根本没有这部剧」——
            # 刚入库的剧组会长时间处于这个状态。上层必须能区分，
            # 否则「没有这个 Series」会被当成「每一集都缺」→ 逐集探测 → duplicate 洪水。
            _EMBY_EP_CACHE[want] = ({}, "emby_series_absent", time.time())
            return {}, "emby_series_absent"
        sid = hit.get("Id")
        q = urllib.parse.urlencode({
            "ParentId": sid, "Recursive": "true", "IncludeItemTypes": "Episode",
            "Limit": "10000",
            "Fields": "Path,ParentIndexNumber,IndexNumber,MediaStreams,Size",
        })
        episodes = (P.emby("/Items?" + q) or {}).get("Items", [])
        mapping = {}
        for episode in episodes:
            try:
                season = int(episode.get("ParentIndexNumber"))
                ep = int(episode.get("IndexNumber"))
            except (TypeError, ValueError):
                continue
            if season < 0 or ep <= 0:
                continue
            local = os.path.normpath(P.emby_path_to_local(episode.get("Path") or ""))
            video = next((s for s in (episode.get("MediaStreams") or [])
                          if (s.get("Type") or "").lower() == "video"), {})
            mapping.setdefault((season, ep), []).append({
                "path": local,
                "width": int(video.get("Width") or 0),
                "height": int(video.get("Height") or 0),
                "size": int(episode.get("Size") or 0),
                "id": episode.get("Id"),
            })
        _EMBY_EP_CACHE[want] = (mapping, "", time.time())
    except Exception as e:                                 # noqa: BLE001
        _EMBY_EP_CACHE[want] = ({}, "check_fail:emby:%s" % str(e)[:100], time.time())
    _mapping, _why, _ = _EMBY_EP_CACHE[want]
    return _mapping, _why


def effective_position(item, rel):
    """Return the season/episode Emby actually exposes for one local path."""
    mapping, why = emby_effective_episodes(item)
    if why:
        return int(item["season"]), int(item["ep"]), why
    old_abs = os.path.normpath(os.path.join(
        A.MNT_ROOT, str(rel or "").replace("/", os.sep)))
    for (season, ep), rows in mapping.items():
        if any(os.path.normpath(row["path"]) == old_abs for row in rows):
            return season, ep, ""
    return int(item["season"]), int(item["ep"]), ""


def probe_failed(hits):
    """探测失败 ≠ 已存在 —— check_fail 前缀不能当作「在库」的证据。

    2026-11-23 事故：06:08 Emby 容器重启，9 秒内 46 次判重全部拿到
    `check_fail:emby:<Connection refused>`。旧代码只看 `if hits:`，于是
      - new 车道：整季 `_miss` 全被判成已入库 → series_complete → 7 天冷却；
      - fill 车道：47 集记 duplicate + fill_ep_present（7 天）/ 23 部剧
        fill_all_present（12h）→ 这些集可能真的缺，却再也不会重试。
    一次 9 秒的重启就吞掉两天产量。判据应当是：只有 state_key /
    resource_state / teldrive / emby_effective 才算命中，check_fail 只说明
    「这次没判断成」，必须按「未知」处理 —— 不记 duplicate、不落任何冷却。
    """
    return any(str(h).startswith("check_fail") for h in (hits or []))


def episode_anywhere(item, season, ep):
    """Return the reasons/list path when this episode already exists at ANY quality."""
    hits = []
    season, ep = int(season), int(ep)
    prefix = "tv|id:%d|(%d, %d)|" % (int(item["tmdb"]), season, ep)
    try:
        con = _tgdb_ro()
        # 2026-11-13 可扩展性：原来用 `k LIKE 'prefix%'`。SQLite 对 LIKE 的
        # 前缀优化在有索引时通常生效，但实测 20 万行库里，命中排在后面的 tmdb
        # 时要 20ms/次、命中前面只要 0.07ms —— 差 300 倍，而且随行数放大。
        # 改用范围查询（k >= prefix AND k < prefix_upper），稳定走主键索引：
        # 20 万行实测 0.01ms/次，与位置无关。
        hi = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        # 2026-11-20：命中要带**落地锚点**才作数。tgpipe 的频道清点键
        # 形状相同但没有 file_id/src（"只在频道里见过"），把它当已入库
        # 会让 6362 集永远补不进来（267 部剧），还每天刷 duplicate。
        for (val,) in con.execute(
                "select val from state_key where k>=? and k<? limit 8", (prefix, hi)):
            try:
                d = json.loads(val) if val else {}
            except Exception:                              # noqa: BLE001
                d = {}
            if isinstance(d, dict) and (d.get("file_id") or d.get("src")):
                hits.append("state_key")
                break
        # 2026-11-13 可扩展性：原来对「本剧所有 rel」逐条正则匹配，超长剧是 O(N²)。
        # 改成先按剧目录建一次索引（_ep_index_for），再 O(1) 查表。
        root_prefix = "TV/%s/%s/Season %02d/" % (
            strip_lib(item["lib"]), item["dir"], season)
        if _ep_index_for(item, root_prefix).get((season, ep)):
            hits.append("resource_state")
    except Exception as e:                                 # noqa: BLE001
        return ["check_fail:%s" % str(e)[:80]], None
    mapping, emby_why = emby_effective_episodes(item)
    if emby_why == "emby_series_absent":
        # 剧还没进 Emby（刚入库 / 索引滞后）：不能拿「Emby 零集」当判据，
        # 但也不能直接返回「没有」。真正的判据是本地账本与 teldrive 索引，
        # 上面已经查过 state_key 与 _ep_index_for，走到这里说明它俩也没命中。
        # 此时返回 hits + 这一条 why，让上层知道是 Emby 缺剧而不是文件缺。
        rel = _ep_index_for(item, root_prefix).get((season, ep))
        if rel:
            hits.append("teldrive")
            return hits, rel
        return hits, None
    if emby_why:
        hits.append(emby_why)
        return hits, None
    effective = mapping.get((season, ep)) or []
    if effective:
        hits.append("emby_effective")
        return hits, effective[0]["path"]
    try:
        rel = _ep_index_for(item, root_prefix).get((season, ep))
        if rel:
            hits.append("teldrive")
            return hits, rel
    except Exception as e:                                 # noqa: BLE001
        return ["check_fail:%s" % str(e)[:80]], None
    return hits, None


def _tgdb_ro():
    """进程内复用只读连接。

    2026-11-13 可扩展性：episode_anywhere() 每次调用都 sqlite3.connect() 一次，
    超长剧一轮扫描上千次就是上千次 open/close。实测单集 2.78ms 里大头在这里。
    连接按 mtime 失效（tg.db 被写入后自动重连），避免读到陈旧视图。
    """
    global _TGDB_CONN, _TGDB_MTIME
    try:
        mt = os.path.getmtime(A.TGDB)
    except OSError:
        mt = 0
    if _TGDB_CONN is not None and _TGDB_MTIME == mt:
        return _TGDB_CONN
    try:
        if _TGDB_CONN is not None:
            _TGDB_CONN.close()
    except Exception:                                      # noqa: BLE001
        pass
    _TGDB_CONN = sqlite3.connect("file:%s?mode=ro" % A.TGDB, uri=True,
                                 timeout=30, check_same_thread=False)
    _TGDB_MTIME = mt
    return _TGDB_CONN


_TGDB_CONN = None
_TGDB_MTIME = None


def _ep_index_for(item, root_prefix):
    """为「一部剧」建一次路径索引，避免逐集遍历全库。

    2026-11-13 可扩展性修复（目标：十万到二十万集，海贼王/柯南这种超长剧）：
    旧实现在 episode_anywhere() 尾部 `for rel in paths.values()` 遍历**全库**
    所有路径来找某一集。实测：全库 4.3 万条时，连续判断 20 集要 60ms；
    按 20 万条 × 1000 集推算要 14 秒 —— 一轮领片扫描几十集就是几分钟，
    五路并发直接把 IO 打满。
    改成：一次遍历全库，只保留该剧目录下、且能解析出 (season, ep) 的路径，
    建成 {(season, ep): rel} 字典。之后每集 O(1) 查表。
    索引按 root_prefix 缓存在进程内（同一轮领片所有集共用）。
    """
    import re as _re
    key = root_prefix
    cache = _EP_PATH_CACHE.get(key)
    if cache is not None and time.time() - cache["ts"] < 30:
        return cache["map"]
    m = {}
    try:
        _, paths = A.td_snapshot()
        for rel in paths.values():
            if not rel.startswith(root_prefix):
                continue
            name = rel.rsplit("/", 1)[-1]
            if not name.lower().endswith((".mp4", ".mkv", ".m4v", ".avi", ".ts", ".mov")):
                continue
            hit = SE_RE.search(name)
            if hit:
                m.setdefault((int(hit.group(1)), int(hit.group(2))), rel)
    except Exception:                                      # noqa: BLE001
        pass
    _EP_PATH_CACHE[key] = {"ts": time.time(), "map": m}
    return m


_EP_PATH_CACHE = {}


def _rank_dims(width, height):
    """分辨率等级 —— 统一走 quality_gate 的 OR 判档。

    2026-11-19：原先这里按 `h>=1800 or w>=3000` 判 4K，会把 3840x1608 的宽银幕 4K
    判成 4 级、把 1440x1080 判成 2 级以外的档位；现在单一真源在 quality_gate.tier()：
      width≥3840 **或** height≥2160 → 2160p
      width≥1920 **或** height≥1080 → 1080p
    """
    return QG.rank_of_dims(width, height)


def upgrade_duplicate(item, target, quality, old_rel):
    """Check upgrade target without being fooled by the old filename's fake quality."""
    season, ep = int(item["season"]), int(item["ep"])
    base = os.path.basename(target)
    key = A.state_key_for(item["tmdb"], season, ep, base)
    mapping, emby_why = emby_effective_episodes(item)
    if emby_why:
        return key, emby_why
    try:
        con = sqlite3.connect("file:%s?mode=ro" % A.TGDB, uri=True, timeout=30)
        want_rank = rank_of(quality)
        old_abs = os.path.normpath(os.path.join(
            A.MNT_ROOT, old_rel.replace("/", os.sep)))
        path_to_effective = {}
        for (effective_season, effective_ep), rows in mapping.items():
            for row in rows:
                path_to_effective[os.path.normpath(row["path"])] = (
                    effective_season, effective_ep)
        for rel, width, height in con.execute(
                "select rel,width,height from resource_state where tmdb=? and rel is not null",
                (int(item["tmdb"]),)):
            abs_rel = os.path.normpath(os.path.join(
                A.MNT_ROOT, (rel or "").replace("/", os.sep)))
            if abs_rel == old_abs:
                continue
            m = SE_RE.search(rel or "")
            effective = path_to_effective.get(abs_rel)
            if effective:
                if effective != (season, ep):
                    continue
            elif not m or int(m.group(1)) != season or int(m.group(2)) != ep:
                continue
            if _rank_dims(width, height) >= want_rank:
                return key, "resource_state=%s" % rel
        con.close()
    except Exception as e:                                 # noqa: BLE001
        return key, "check_fail:%s" % str(e)[:80]
    try:
        if A.td_path_exists(target):
            return key, "teldrive"
    except Exception as e:                                 # noqa: BLE001
        return key, "check_fail:%s" % str(e)[:80]
    return key, ""


def valid_candidates(cands, min_bytes, max_bytes, item=None):
    """体积/接口校验 + 画质闸门（quality_gate，两条线）。

    * 假 4K（2160p 但 <2.0Mbps）**一票否决**，直接不进候选
    * 其余候选打两个标：`qg_admit`（达新源准入线）/ `qg_stock_ok`（不低于存量线）
    * 排序：先按「过准入线」再按 score，避免拿"分辨率对了但码率更差"的那份
    """
    out = []
    reasons = []
    for c in cands:
        why = invalid_candidate(c["episode"], c["size"], min_bytes, max_bytes)
        if why:
            reasons.append("%s:%s" % (c["st"].label, why))
            continue
        try:
            admit = QG.candidate_verdict(c, item, line="admit")
            stock = QG.candidate_verdict(c, item, line="stock")
            c["qg"] = admit
            c["qg_admit"] = bool(admit.get("ok"))
            c["qg_stock_ok"] = not stock.get("ok")      # stock.ok=True 表示"该洗"= 不达标
            if admit.get("reason") == "fake_4k":
                reasons.append("%s:fake_4k(%sMbps)" % (c["st"].label, admit.get("mbps")))
                QG.log_reject("candidate", (c.get("episode") or {}).get("Name", ""),
                              c.get("size"), 0, admit.get("tier"), admit.get("mbps"), "fake_4k")
                continue
        except Exception as e:                          # noqa: BLE001
            # 闸门自身出错不许卡死选源：退化成"未过线"，仍可兜底
            c["qg"] = {"ok": False, "reason": "gate_error:%s" % str(e)[:60]}
            c["qg_admit"] = False
            c["qg_stock_ok"] = True
        out.append(c)
    out.sort(key=lambda c: (bool(c.get("qg_admit")), (c.get("qg") or {}).get("rank", 0),
                            c["quality"]["score"]), reverse=True)
    return out, reasons


def fill_target(item, ep, cands, min_bytes, max_bytes):
    if not target_existing(item, ep):
        return None, "no_target"
    good, reasons = valid_candidates(cands, min_bytes, max_bytes, item=item)
    if not good:
        # 2026-11-21：**「没搜到」≠「源站没有」。**
        # 实测大量集被标成 no_valid_source 后锁 6 小时，实际原因是那一轮
        # 源站在 429 / 超时（yuntv 熔断、bangumi 证书告警等），根本没真正搜成。
        # 这种情况下小时级冷却会把整个队列冻住 —— 用户看到的「没活」就是这么来的。
        # 凡是原因里含 429 / timeout / proxy / ssl 的，一律归为「暂时不可用」，
        # 走分钟级短退避，等源缓过来立刻重试。
        _r = " ".join(str(x).lower() for x in (reasons or []))
        _soft = any(k in _r for k in ("429", "timeout", "timed out", "proxy",
                                      "ssl", "certificate", "connection", "refused"))
        if _soft:
            return None, "sources_unavailable" + ((":" + ",".join(reasons[:4])) if reasons else "")
        return None, "no_valid_source" + ((":" + ",".join(reasons[:4])) if reasons else "")
    c = good[0]
    q = c["quality"]["label"]
    if rank_of(q) < 1:
        return None, "below_720"
    if not c.get("qg_admit"):
        # 无高画质源兜底：收入当前最高版本防空集，单独记账，不计入达标产出
        QG.log_fallback("fill", {"series": item.get("series"), "season": item.get("season"),
                                 "ep": ep, "label": q,
                                 "mbps": (c.get("qg") or {}).get("mbps"),
                                 "reason": (c.get("qg") or {}).get("reason")})
    target = target_with_quality(
        item["lib"], item["dir"], item["series"], item["season"], ep,
        q, ext_of(c["episode"]))
    return (target, c), ""


def same_label_better(c, cur_size, ratio=None):
    """同画质但明显更大的文件 = 更高码率 = 更好画质。

    2026-11-19（用户口径「upgrade 能上最高画质就应该上最高画质」）：
    原判据只比分辨率等级，`1080p 0.63GB` 与 `1080p 3GB` 在它眼里完全等价 →
    再好的版本也永远不会替换。这里补一条：同标签且体积 ≥ 当前 × ratio 时也算可升级。
    ratio 默认 1.5（`UPGRADE_SAME_LABEL_RATIO` 可覆盖），避免轻微差异来回洗版。
    """
    try:
        r = float(ratio if ratio is not None else
                  os.environ.get("UPGRADE_SAME_LABEL_RATIO", "1.5"))
    except Exception:                                       # noqa: BLE001
        r = 1.5
    sz = int(c.get("size") or 0)
    cur = int(cur_size or 0)
    return bool(cur) and sz >= cur * r


def cand_score(c, item):
    """候选排序键：分辨率 → 码率 → 帧率（用户 2026-11-19：三者都要看，选最好的）。

    码率用「体积 × 8 / 时长」算真实值；拿不到时长就用条目 runtime（默认 40 分钟）。
    帧率取源站给的 fps（有就比，没有按 0，不会因此把候选压下去）。
    """
    q = c.get("quality") or {}
    res = rank_of(q.get("label"))
    size = float(c.get("size") or 0)
    dur = 0
    for k in ("duration", "runtime", "duration_sec"):
        try:
            dur = float((c.get("episode") or {}).get(k) or q.get(k) or c.get(k) or 0)
        except (TypeError, ValueError):
            dur = 0
        if dur:
            break
    if not dur:
        try:
            dur = float(item.get("runtime") or 0) * 60
        except (TypeError, ValueError):
            dur = 0
    mbps = (size * 8 / dur / 1e6) if (dur and size) else 0.0
    try:
        fps = float(q.get("fps") or (c.get("episode") or {}).get("fps") or 0)
    except (TypeError, ValueError):
        fps = 0.0
    return (res, round(mbps, 2), round(fps, 1))


def upgrade_target(item, cands, min_bytes, max_bytes):
    current_rank = rank_of(item.get("current_quality"))
    good = [c for c in cands if rank_of(c["quality"]["label"]) > current_rank]
    # 注意：同画质替换的目标文件名与现有文件**完全相同**（都是 `S01E01 - 1080p.mkv`），
    # 直接被 check_existing 的 path_exists 挡住；强行覆盖又要先删旧文件（不可逆）。
    # 所以这条默认关闭，只有显式 UPGRADE_SAME_LABEL=1 且下游支持「原地替换」时才开。
    # 2026-11-19：P1 洗版主路径。同画质但码率高 1.5 倍以上也算升级，
    # 落地名加 -hq 后缀（`SxxExx - 2160p-hq.mp4`），与旧文件同 base 不同 version，
    # Emby 会合并成同一集的多个版本 —— 满足「低画质不删、默认高画质」的红线。
    # ⚠ 2026-11-19 实测：Emby 4.10 **不会**把同 base 不同 version 的剧集文件合并成多版本
    # （幻视任务 E1-E7：2 个文件 → 2 条独立条目，各 1 个 MediaSource）。
    # 所以 -hq 落地会产生重复条目，默认关闭；要开必须用户在「重复条目」和「保留低画质」之间选。
    if item.get("current_size") and os.environ.get("UPGRADE_SAME_LABEL", "0") == "1":
        # 同画质更优版本（高码率）也纳入，按体积比判
        good += [c for c in cands
                 if rank_of(c["quality"]["label"]) == current_rank
                 and same_label_better(c, item.get("current_size"))]
        good.sort(key=lambda c: -(c.get("size") or 0))
    good, reasons = valid_candidates(good, min_bytes, max_bytes, item=item)
    if not good:
        return None, "no_higher_quality" + ((":" + ",".join(reasons[:4])) if reasons else "")
    # 排序（红线 1）：先看质量门禁是否放行，再按 best_pick 的
    # 「真2160p > 达标1080p > 同档 HEVC/AV1 > 更高码率」排。
    try:
        import best_pick as _BP
        _view = [{"label": ((c.get("quality") or {}).get("label") or ""),
                  "size": int(c.get("size") or 0),
                  "duration": float((c.get("episode") or {}).get("duration") or 0),
                  "vcodec": ((c.get("episode") or {}).get("vcodec") or "")} for c in good]
        _ranked = _BP.rank_candidates(_view)
        if _ranked:
            _idx = []
            for o in _ranked:
                for i, v in enumerate(_view):
                    if i not in _idx and v["label"] == o["label"] and v["size"] == o["size"]:
                        _idx.append(i)
                        break
            good = [good[i] for i in _idx]
            if len(_idx) < len(_view):
                reasons = list(reasons) + ["fake4k_veto=%d" % (len(_view) - len(_idx))]
    except Exception:                                          # noqa: BLE001
        pass
    good.sort(key=lambda c: (bool(c.get("qg_admit")), cand_score(c, item)), reverse=True)
    c = good[0]
    q = c["quality"]["label"]
    if rank_of(q) < 2:
        return None, "below_1080"
    if not c.get("qg_admit"):
        QG.log_fallback("upgrade", {"series": item.get("series"), "season": item.get("season"),
                                    "ep": item.get("ep"), "label": q,
                                    "mbps": (c.get("qg") or {}).get("mbps"),
                                    "reason": (c.get("qg") or {}).get("reason")})
    _suffix = "-hq" if rank_of(q) == current_rank else ""
    target = target_with_quality(
        item["lib"], item["dir"], item["series"], item["season"], item["ep"],
        q + _suffix, ext_of(c["episode"]))
    return (target, c), ""


def new_done_hot(video_id):
    try:
        d = json.load(open(NEW_DONE, encoding="utf-8"))
        rec = d.get(str(video_id)) or {}
        return time.time() < float(rec.get("next_check") or 0)
    except Exception:                                      # noqa: BLE001
        return False


def new_done_hot_set():
    """一次读出所有在冷却中的 video_id。

    2026-11-14：`new_done_hot()` 每条都重读一次 JSON（30 次/轮），
    而选源时需要"这个源还有没有未冷却的条目"——必须批量算。
    """
    try:
        d = json.load(open(NEW_DONE, encoding="utf-8"))
        now = time.time()
        return {str(k) for k, v in d.items()
                if isinstance(v, dict) and now < float(v.get("next_check") or 0)}
    except Exception:                                      # noqa: BLE001
        return set()


def fill_item_key(item):
    """fill/upgrade 条目的稳定冷却键。

    2026-11-15 根因（狂怒追缉 17 条 / 王冠 3 条 duplicate 反复）：
    `.gha_queue_fill.json` 的条目**没有 video_id**（build_three_queues.load_fill
    只搬 lib/dir/series/season/missing/tmdb）。而冷却有两个出口都只认 video_id：
      ① 队列过滤 `items = [x for x in items if str(x.get("video_id")) not in _hot]`
      ② try_fill 里 `mark_new_done(item.get("video_id") or ...)`
    于是 fill 条目**永远不可能被冷却**：写进去的键是 `fill:<tmdb>:S<n>`，
    过滤时读的是 `None`，两边对不上。一条"声称缺、其实全在库"的 fill 条目
    会一直待在队列里，每轮游标转到就重查一遍并记一条 duplicate。

    用 (tmdb, season) 做键：季是 fill 的最小粒度（missing 是某季的集号列表），
    同一剧换季要各自判定，用 tmdb 单键会把 S1 的冷却误伤 S2。
    """
    tmdb = item.get("tmdb")
    season = item.get("season")
    if tmdb is None or season is None:
        return ""
    try:
        return "fill:%d:S%d" % (int(tmdb), int(season))
    except (TypeError, ValueError):
        return ""


def ep_cool_key(item, season, ep):
    """fill 车道的**按集**冷却键（E128）。

    2026-11-15 根因（今日 53 条 duplicate 全是 lane=fill、why=三本账全命中）：
    `try_fill()` 遇到「这一集其实已在库」只 `duplicate_log()` 就 continue，
    **不写任何冷却**。季级冷却（fill_item_key）只在「声明的 missing 全部在库」
    时才触发；而现实是某季 9 集里 8 集已在库、只剩 1 集真缺 ——
    季级永远不满足，于是每轮游标转回来都把这 8 集重查一遍并各记一条 duplicate。
    实测：绝望写手 19 条、狂怒追缉 17 条、行尸走肉 8 条，合计占当日 duplicate 的 83%。

    键用 (tmdb, season, ep) —— 同一集不同季必须分开，否则会误伤。
    """
    tmdb = item.get("tmdb")
    if tmdb in (None, "", "None") or season is None or ep is None:
        return ""
    try:
        return "fill-ep:%d:S%d:E%d" % (int(tmdb), int(season), int(ep))
    except (TypeError, ValueError):
        return ""


def ep_cooled(hot, item, season, ep):
    k = ep_cool_key(item, season, ep)
    return bool(k) and k in hot


def item_cool_key(item):
    """队列条目在冷却表里的键：new 车道用 video_id，fill/upgrade 用 fill:tmdb:S季。"""
    vid = item.get("video_id")
    if vid not in (None, "", "None"):
        return str(vid)
    return fill_item_key(item)


def mark_new_done(video_id, days: float = 7, reason=""):
    # 2026-11-15（E128）：**读-改-写必须加 flock。**
    # 五条 lane 并发写同一份 .gha_new_done.json，无锁时「后写覆盖先写」，
    # 刚记的冷却会凭空消失（同 E095 那族静默失效）。锁文件独立，避免动数据文件本身。
    days = float(days)
    import fcntl
    lock_path = NEW_DONE + ".lock"
    with open(lock_path, "a+") as lk:
        fcntl.flock(lk.fileno(), fcntl.LOCK_EX)
        try:
            try:
                d = json.load(open(NEW_DONE, encoding="utf-8"))
            except Exception:                              # noqa: BLE001
                d = {}
            d[str(video_id)] = {"ts": int(time.time()),
                                "next_check": int(time.time() + float(days) * 86400),
                                "reason": str(reason or "")[:80]}
            tmp = NEW_DONE + ".tmp.%d" % os.getpid()
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=1)
            os.chmod(tmp, 0o600)
            os.replace(tmp, NEW_DONE)
        finally:
            fcntl.flock(lk.fileno(), fcntl.LOCK_UN)


TMDB_VALID = os.path.join(HERE, ".tmdb_id_valid.json")
_tmdb_memo = {}


def tmdb_id_valid(tmdb, kind="tv", ttl_ok=30 * 86400, ttl_bad=7 * 86400):
    """TMDB 锚点到底存不存在：True / False / None（无法判定，不拦）。

    2026-11-15 根因（slot1 20261115_133536「Mattaku Saikin…」连跑 144 次）：
    队列条目自带 `tmdb: 295123`，而 TMDB 里根本没有这个 id（404 status_code 34）。
    一路没人校验：源站解析 → 下载 1.4GB → 上传 → 入库 → 收尾要求「剧集 NFO +
    tvshow.nfo + Series 身份」，而 NFO 生成靠的正是这个锚点 —— 于是
    `todb_resolve`/`nfo_episodes_tmdb` 都报「可补 0 条」，post_ingest 永远
    rc=2，claim 永远收不掉。**一集坏数据能把一条 lane 锁死一整晚。**

    正确的拦截点在「落 claim 之前」：不存在的锚点根本不该开工。
    三态而不是布尔：TMDB 429/5xx/网络断**不能**判死，否则接口抖一下
    就把好剧冷藏 7 天；只有明确的 404 才判 False。

    缓存：正的 30 天（TMDB id 不会变），负的 7 天（真的不存在）。
    """
    try:
        tid = int(tmdb)
    except (TypeError, ValueError):
        return None
    if tid <= 0:
        return None
    mkey = (kind, tid)
    if mkey in _tmdb_memo:
        return _tmdb_memo[mkey]
    key = "%s:%d" % (kind, tid)
    now = time.time()
    rec = {}
    try:
        with open(TMDB_VALID, encoding="utf-8") as f:
            rec = (json.load(f) or {}).get(key) or {}
    except Exception:                                      # noqa: BLE001
        rec = {}
    if rec and now < float(rec.get("until") or 0):
        val = bool(rec.get("ok"))
        _tmdb_memo[mkey] = val
        return val
    try:
        from embytg_conf import cfg
        tk = cfg("tmdb_token")
        api = (cfg("tmdb_api") or "https://api.themoviedb.org/3").rstrip("/")
    except Exception:                                      # noqa: BLE001
        tk, api = "", "https://api.themoviedb.org/3"
    if not tk:
        return None
    val = None
    try:
        r = requests.get("%s/%s/%d" % (api, kind, tid),
                         headers={"Authorization": "Bearer " + tk,
                                  "Accept": "application/json"}, timeout=12)
        if r.status_code == 200:
            val = True
        elif r.status_code == 404:
            val = False
        # 401/403/429/5xx → None：配置或限流问题，不能拿来判剧不存在
    except Exception:                                      # noqa: BLE001
        val = None
    if val is None:
        return None
    try:
        import fcntl
        with open(TMDB_VALID + ".lock", "a+") as lk:
            fcntl.flock(lk.fileno(), fcntl.LOCK_EX)
            try:
                try:
                    with open(TMDB_VALID, encoding="utf-8") as f:
                        d = json.load(f)
                except Exception:                          # noqa: BLE001
                    d = {}
                d[key] = {"ok": bool(val), "ts": int(now),
                          "until": int(now + (ttl_ok if val else ttl_bad))}
                if len(d) > 8000:                          # 防无限膨胀
                    cut = sorted(d.items(), key=lambda kv: kv[1].get("ts") or 0)
                    d = dict(cut[-6000:])
                tmp = TMDB_VALID + ".tmp.%d" % os.getpid()
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(d, f, ensure_ascii=False)
                os.chmod(tmp, 0o600)
                os.replace(tmp, TMDB_VALID)
            finally:
                fcntl.flock(lk.fileno(), fcntl.LOCK_UN)
    except Exception:                                      # noqa: BLE001
        pass
    _tmdb_memo[mkey] = val
    return val


def job_tmdb_kind(job):
    rel = str(job.get("target_rel") or "")
    return "movie" if rel.startswith("Movies/") or "/Movies/" in rel else "tv"


def existing_dir(tmdb):
    try:
        con = sqlite3.connect("file:%s?mode=ro" % A.TGDB, uri=True, timeout=30)
        row = con.execute(
            "select lib,title from resource_state where tmdb=? and kind='episode' "
            "and title is not null limit 1", (int(tmdb),)).fetchone()
        con.close()
        if row:
            return strip_lib(row[0]), row[1]
    except Exception:                                      # noqa: BLE001
        pass
    return None, None


def pick_new_episode(item, sources, min_bytes, max_bytes):
    # 2026-11-14：**不要只看源站的 ProviderIds**。
    #
    # 原来：没有任何源返回 ProviderIds.Tmdb → 直接判 no_tmdb 弃掉。
    # 实测 06:00 那轮 30 次尝试**全部**是 no_tmdb —— 而那些条目自带
    # `tmdb` 字段（建队列时已经用 TODB/TMDB 匹配过，98% 有值）。
    # 就像 series_id 那个坑一样：**手里有确定的 ID，却去依赖源站的可选字段**。
    # 顺序：源站 ProviderIds → 队列条目自带 tmdb。两个都没有才算真的 no_tmdb。
    provider_src = next((s for s in sources if (s.get("provider_ids") or {}).get("Tmdb")), None)
    tmdb = 0
    if provider_src:
        try:
            tmdb = int(provider_src["provider_ids"]["Tmdb"])
        except Exception:                                  # noqa: BLE001
            tmdb = 0
    if not tmdb:
        try:
            tmdb = int(item.get("tmdb") or 0)
        except Exception:                                  # noqa: BLE001
            tmdb = 0
    if not tmdb:
        return None, None, "no_tmdb"
    series_name = (provider_src or {}).get("series_name") or item["series"]
    year = int((provider_src or {}).get("year") or item.get("year") or 0)
    lib, old_dir = existing_dir(tmdb)
    dirname = old_dir or ("%s (%s) {tmdb=%d}" % (
        series_name, year or item.get("year") or "未知", tmdb))
    item2 = {
        **item, "tmdb": tmdb, "series": series_name,
        "year": year, "lib": lib or item.get("lib") or "国产剧",
        "dir": dirname,
    }
    by_ep = {}
    dup_hits = []
    for s in sources:
        for row in s.get("episodes") or []:
            by_ep.setdefault((int(row["season"]), int(row["ep"])), []).append((s, row))

    # 2026-11-13 根因修复：源站把整部剧都列出来时，逐集调 episode_anywhere() 会
    # 对「已经入库的每一集」各打一条 duplicate 日志，并且每次都重查 state_key +
    # resource_state + teldrive 快照 + Emby。娘道 E1 一天被记 32 次就是这么来的：
    # 剧还在连载（E15/E17 陆续补进来），每轮都要把 E1..E14 重新确认一遍。
    #
    # 修复思路不是「不查」，而是「先算差集」：源站集数 - 库内集数 = 待补集数。
    #   - 差 0（源站该给的都给了）→ series_complete，7 天冷却，不逐集验证
    #   - 差 <= 2 且库里已经有很多集 → 只验证差的那些，不遍历已入库的
    #   - 差 > 2 → 正常逐集流程（新连载剧，需要精细判断）
    _have, _emby_why = emby_effective_episodes(item2)
    _have_keys = set(_have.keys()) if _have else set()
    if _emby_why == "emby_series_absent" and by_ep:
        # 2026-11-14 根因修复（王冠 160 条 duplicate / 4 小时）：
        # 剧刚入库、Emby 还没建 Series 时，emby_effective_episodes 返回空 mapping，
        # 下面 `_miss = [k for k in by_ep if k not in _have_keys]` 会把**每一集**
        # 都算成缺失，然后逐集调 episode_anywhere() —— 每次都命中 state_key，
        # 于是每轮刷出 N 条 duplicate，直到 new_done 冷却才停。
        # 正确判据：Emby 缺这部剧时，用本地账本 state_key 算差集（那才是入库真相）。
        # 判据与 episode_anywhere 的 state_key 分支完全一致（范围查询，不用 LIKE）。
        try:
            _con = _tgdb_ro()
            for _k in by_ep:
                _p = "tv|id:%d|(%d, %d)|" % (int(item2["tmdb"]), _k[0], _k[1])
                _hi = _p[:-1] + chr(ord(_p[-1]) + 1)
                if _con.execute("select 1 from state_key where k>=? and k<? limit 1",
                                (_p, _hi)).fetchone():
                    _have_keys.add(_k)
        except Exception:                                  # noqa: BLE001
            pass
    _have_n = len(_have_keys)
    if by_ep:
        _miss = [k for k in by_ep if k not in _have_keys]
        if not _miss:
            return item2, None, "series_complete"
        # 2026-11-13 二次修正：Emby 索引滞后于 teldrive 入库。刚 finish 的集
        # 在库里已经有 state_key/resource_state，但 Emby 还没建条目，于是这里
        # 判成「缺」，逐集流程又一集集重新确认（亚托莉 E6 刚入库那轮就是这情况）。
        # 所以「缺」不等于「真缺」：对每个候选缺集再走一次 episode_anywhere()，
        # 命中的从 _miss 里剔除；只有确实查不到的才算真缺。
        _real_miss = []
        _probe_broken = False
        for k in _miss:
            hits, _p = episode_anywhere(item2, k[0], k[1])
            if probe_failed(hits):
                # 判断没做成（Emby/tg.db 短暂不可用）：本地账本已查过没命中，
                # 按真缺处理；但绝不能由此判 series_complete（7 天冷却）。
                _probe_broken = True
                _real_miss.append(k)
                continue
            if hits:
                continue                      # 已入库，只是 Emby 索引还没跟上
            _real_miss.append(k)
        if _probe_broken:
            return item2, None, "probe_unavailable"
        if not _real_miss:
            return item2, None, "series_complete"
        if _have_n >= 5 and len(_real_miss) <= 2:
            # 只对「确认库里没有的」那几集做完整判定，已入库的一律不碰
            by_ep = {k: by_ep[k] for k in _real_miss}
        elif len(_real_miss) < len(_miss):
            # 部分是真缺：只扫真缺的，避免把索引滞后误判成 duplicate 洪水
            by_ep = {k: by_ep[k] for k in _real_miss}
        # 2026-11-14 根因修复（哆啦A梦 06:17–06:38 被记 93 条 duplicate）：
        # 上面两支只在「有一部分命中」时才收缩 by_ep。当 _miss 全部是真缺
        # （_have 很少、源站列的集多数还没入库），by_ep 仍是**源站全集**，
        # 下面逐个 episode_anywhere() 就会把已经入库的那几集再确认一遍并各记一条
        # duplicate —— 而它们在上面的 _miss 循环里已经被判定为「已入库」了，
        # 这是纯粹的重查 + 重复日志（每轮重来一次，直到 new_done 冷却）。
        # 真正的判据只有一条：by_ep 只保留「确认库里没有的集」。
        by_ep = {k: by_ep[k] for k in _real_miss}

    for (season, ep) in sorted(by_ep):
        hits, _ = episode_anywhere(item2, season, ep)
        if probe_failed(hits):
            # 判断没做成：不记 duplicate、不落冷却，整部本轮放弃，下一轮再来。
            return item2, None, "probe_unavailable"
        if hits:
            dup_hits.append((ep, hits))
            duplicate_log(item2, ep, None, None, ",".join(hits), season=season)
            continue
        cands = []
        for s, row in by_ep[(season, ep)]:
            cands.append({
                "st": s["st"], "episode": row["episode"], "size": row["size"],
                "quality": row["quality"],
            })
        good, _ = valid_candidates(cands, min_bytes, max_bytes)
        if not good:
            continue
        c = good[0]
        q = c["quality"]["label"]
        if rank_of(q) < 2:
            continue
        target = target_with_quality(
            item2["lib"], dirname, series_name, season, ep, q, ext_of(c["episode"]))
        key, why = check_existing(item2, season, ep, target, q)
        if why:
            if why not in ("path_exists",) and not why.startswith("check_fail"):
                duplicate_log(item2, ep, key, target, why, season=season)
            continue
        item2 = {**item2, "season": season}
        return item2, {"target": target, "candidate": c,
                       "season": season, "ep": ep, "key": key}, ""
    # 全部候选集都已存在：整部剧在源站能给的范围内已经收完。
    # 之前这里直接 return "series_complete"（无条件），却不落 new_done 冷却，
    # 导致每次游标轮转到它都重跑一遍源站探测 + N 次 dedupe（伪装者 134 次/2h）。
    if by_ep and dup_hits:
        if len(dup_hits) == len(by_ep):
            return item2, None, "series_complete"
        # 源站只列了部分集、其余都是已入库的重复：新集要么源站没有、要么尺寸/画质不合格。
        # 这一支以前落到 no_candidate 不落 new_done → 每 5 分钟把同样 N 条 duplicate 重写一遍
        # （娘道 14 分钟 2 轮）。给短冷却等源站补集。
        return item2, None, "series_partial_done"
    return item2, None, "no_candidate"


def _lane_gated(path):
    """该 lane 的授权闸门要不要生效。

    2026-11-15（E107）：**闸门必须能自己解。** 用户要求可扩展性 ——
    一个需要人工 `rm` 文件才能恢复的状态机，机子多了就是灾难。

    实测教训：box2 的 session 作废是真，但同一天它网络也退化了
    （只有 cbonline 169.254.1.1，无公网 IPv4 路由），光重登 session
    解决不了；闸门一旦落下就永久挡着，网络恢复后也没人记得去删文件。

    规则：文件不存在 → 不拦；存在但超过 GATE_RETRY_SEC → 允许再试一次
    （重试若仍失败，gha_cancel 会刷新文件时间，自动重新计）；
    刚落下 → 拦。
    """
    if not os.path.exists(path):
        return False
    # E116：授权类错误（AuthKeyDuplicated/Unregistered/SESSION_REVOKED）是 TG 侧永久吊销，
    # 重试一万次也不会成功，只能人工重登。闸门文件以 `permanent` 开头=永不放行；
    # 只有「网络类」失败才允许 GATE_RETRY_SEC 后放行再试（那才可能自愈）。
    try:
        head = open(path, encoding="utf-8").read(16)
    except OSError:
        head = ""
    if head.startswith("permanent"):
        return True
    try:
        age = time.time() - os.path.getmtime(path)
    except OSError:
        return True
    if age > GATE_RETRY_SEC:
        try:
            print(json.dumps({"ok": True, "gate_retry": os.path.basename(path),
                              "gate_age_min": round(age / 60, 1)}), flush=True)
        except Exception:                                  # noqa: BLE001
            pass
        return False
    return True


def _lane_scan_gated(lane):
    """resume_scan 连环失败熔断（E129）。

    与授权闸门（_lane_gated）同一纪律、不同策略：这是**客户端配置问题**
    （远端机器没设 TG_RESUME_SCAN_MODE=skip），修好即自愈，
    所以窗口短（默认 3600s），到期自动放行一次；再连败会刷新计时。
    """
    if not lane:
        return False
    path = os.path.join(HERE, ".gha_lane_scan_broken", str(lane))
    if not os.path.exists(path):
        return False
    try:
        retry = float(os.environ.get("SCAN_GATE_RETRY_SEC", "3600"))
    except Exception:                                      # noqa: BLE001
        retry = 3600.0
    try:
        age = time.time() - os.path.getmtime(path)
    except OSError:
        return True
    if age > retry:
        try:
            print(json.dumps({"ok": True, "scan_gate_retry": str(lane),
                              "gate_age_min": round(age / 60, 1)}), flush=True)
        except Exception:                                  # noqa: BLE001
            pass
        return False
    return True


def _source_url(st, episode_id):
    if getattr(st, "label", "") == "emos.enic65.eu.org":
        import emos_direct
        return emos_direct.direct_url(st.host, st.token, episode_id)
    return "%s/Videos/%s/stream?Static=true&api_key=%s" % (
        st.host.rstrip("/"), episode_id, st.token)


def make_job(lane, item, season, ep, target, candidate, key, store_ch, store_channels,
             slot, run_id, upgrade=False):
    episode = candidate["episode"]
    quality = candidate["quality"]["label"]
    ms = (episode.get("MediaSources") or [{}])[0]
    runtime = int(episode.get("RunTimeTicks") or ms.get("RunTimeTicks") or 0)
    rel = os.path.relpath(target, A.MNT_ROOT).replace("\\", "/")
    job = {
        "job_id": "%s-%s" % (time.strftime("%Y%m%d_%H%M%S"), os.urandom(3).hex()),
        "run_id": run_id,
        "lane": lane,
        "queue_lane": ("upgrade" if lane == "upgrade2" else
                       ("new" if lane in ("new2", "new3", "new4", "new5") else lane)),
        "series": item["series"],
        "tmdb": int(item["tmdb"]),
        "lib": strip_lib(item["lib"]),
        "dir": os.path.basename(os.path.dirname(os.path.dirname(target))),
        "season": int(season),
        "ep": int(ep),
        "key": key,
        "target_rel": rel,
        "folders": os.path.dirname(rel),
        "name": os.path.basename(target),
        "size": int(candidate["size"] or 0),
        "runtime": runtime,
        "quality": quality,
        "store_channel_id": store_ch,
        "store_channels": store_channels,
        "tg_session_slot": slot,
        "source_label": candidate["st"].label,
        "source_ua": candidate["st"].ua,
        "source_url": _source_url(candidate["st"], episode["Id"]),
        "status": "running",
        "ts": int(time.time()),
    }
    reported_size = int(job.get("size") or 0)
    actual_size, valid_video = probe_source(job)
    if not valid_video:
        raise BadSource(candidate["st"].label, "source_preflight_not_video")
    if actual_size and actual_size != reported_size:
        job["source_reported_size"] = reported_size
        job["size"] = actual_size
        print("SOURCE_SIZE_CORRECTED reported=%d actual=%d source=%s" % (
            reported_size, actual_size, job.get("source_label") or "?"),
            file=sys.stderr, flush=True)
    if upgrade:
        job["upgrade"] = True
        job["old_rel"] = item.get("current_rel")
        job["old_quality"] = item.get("current_quality")
    return job


def try_fill(item, available_labels, leased, min_bytes, max_bytes, run_id,
             store_ch, store_channels, slot, max_eps_scan=2, leased_load=None):
    missing = [int(x) for x in (item.get("missing") or [])]
    if missing:
        offset = int(time.time() // 1800) % len(missing)
        missing = missing[offset:] + missing[:offset]
    # 2026-11-15（E128）：**跳过已经确认「在库」的集。**
    # 原来只记 duplicate 不落冷却，队列里的陈旧 missing 每轮都会重扫一遍源站。
    _hot = new_done_hot_set()
    _season = int(item["season"])
    _before = len(missing)
    missing = [e for e in missing if not ep_cooled(_hot, item, _season, e)]
    if missing and len(missing) < _before:
        print("fill: 跳过 %d 集已确认在库（按集冷却中）"
              % (_before - len(missing)), file=sys.stderr, flush=True)
    if not missing:
        return None
    for raw_ep in missing[:max(1, int(max_eps_scan))]:
        ep = int(raw_ep)
        hits, existing_rel = episode_anywhere(item, int(item["season"]), ep)
        if probe_failed(hits):
            # 判断没做成（Emby 重启 / tg.db 短暂不可用）：既不算在库、也不落
            # 冷却 —— 这一轮到此为止，后面每一集都会以同样方式失败。
            print("fill: 判重探测失败，本轮跳过 %s S%dE%d（%s）" % (
                item.get("series"), _season, ep, ",".join(hits)[:60]),
                file=sys.stderr, flush=True)
            return None
        if hits:
            duplicate_log(item, ep, None, existing_rel, ",".join(hits),
                          season=int(item["season"]))
            _k = ep_cool_key(item, _season, ep)
            if _k:
                mark_new_done(_k, days=7, reason="fill_ep_present")
            continue
        tgt = target_existing(item, ep)
        if not tgt:
            continue
        allowed = GS.eligible_sources(available_labels, leased, load=leased_load)
        if not allowed:
            continue
        g = SG.Guard(verbose=False)
        _st_stats = {}
        try:
            with contextlib.redirect_stdout(sys.stderr):
                cands = SP.resolve_candidates(
                    g, item["series"], int(item["season"]), ep,
                    exclude=HARD_EXCLUDE, max_hosts=2, year=item.get("year", 0),
                    order_labels=allowed, stats=_st_stats)
        finally:
            GS.charge_guard(g)
        chosen, why2 = fill_target(item, ep, cands, min_bytes, max_bytes)
        _is_policy_skip = False
        if not chosen:
            # 2026-11-17（E160）：**「源站没有这一集」也必须落按集冷却。**
            #
            # 事故形状：自审 7 天窗口 109 次尝试、其中 72 次失败原因是
            # 「无源可提供（源有剧但缺这一集）」，而 `.gha_ep_fail` /
            # `.gha_new_done` 里一条对应的记录都没有 —— why2 算出来了却直接
            # `continue`。于是队列游标每轮转回来都重搜一遍：今日 335 次源请求
            # 里 238 次落在重复 target（`乘风破浪的姐姐 第七季` 单日 23 次、
            # `法警小队` 9 次），既烧源站配额又占 lane。
            # 与 E128（命中「已在库」不落冷却）是同一类漏写，只是方向相反。
            #
            # 判据可信度：E132 之后 `max_hosts` 只限制「拿到几个**有命中**的
            # 源」，所以 cands 为空 = 所有可用源都搜过、都没有这一集。
            #
            # 不冷却 `no_target`：那是本地算不出目标路径，与源站无关。
            # too_big 是「本时段策略跳过」，不是「源站没有这一集」：
            # 落按集冷却会让 4GB+ 的集整天不再尝试，而它夜里本该允许。
            _is_policy_skip = "too_big" in str(why2 or "")
            # 2026-11-22：**「所有源这一轮都失败」不是「所有源都没有这一集」。**
            # 前者给整集落 6h 冷却 = 把源站故障记成缺片（今日 2263 条队列停泊里
            # 大部分就是这种形状：mirror 回 HTML、401，被当成缺片）。
            # 只有在「有源确实答过、并且答的是没有」时才落集级冷却。
            _all_err = (int(_st_stats.get("errors") or 0)
                        >= max(1, int(_st_stats.get("sources") or 0)))
            if (str(why2 or "").startswith("no_valid_source") and _all_err
                    and not _is_policy_skip):
                print("fill: 全部源本轮失败/坏应答（非缺片），不落集级冷却 %s S%dE%d（%d/%d）"
                      % (item.get("series"), _season, ep,
                         int(_st_stats.get("errors") or 0), int(_st_stats.get("sources") or 0)),
                      file=sys.stderr, flush=True)
                continue
            if why2 and str(why2) != "no_target" and not _is_policy_skip:
                _k3 = ep_cool_key(item, _season, ep)
                if _k3:
                    try:
                        mark_new_done(
                            _k3, days=FILL_NO_SOURCE_COOL_HOURS / 24.0,
                            reason="fill_no_source:%s" % str(why2)[:60])
                        print("fill: 无源冷却 %s S%dE%d（%s，%.0fh）" % (
                            item.get("series"), _season, ep,
                            str(why2)[:40], FILL_NO_SOURCE_COOL_HOURS),
                            file=sys.stderr, flush=True)
                    except Exception:                  # noqa: BLE001
                        pass
            continue
        if _is_policy_skip:
            print("fill: 体积超时段上限，本轮跳过 %s S%dE%d（%s）" % (
                item.get("series"), _season, ep, str(why2)[:40]),
                file=sys.stderr, flush=True)
            continue
        target, c = chosen
        key, why = check_existing(item, int(item["season"]), ep, target, c["quality"]["label"])
        if why:
            if str(why).startswith("check_fail"):
                # check_existing 自己抛了异常（tg.db/teldrive 不可用）：
                # 这不是「已入库」，不能落 fill_ep_present（7 天）冷却。
                print("fill: check_existing 探测失败，本轮跳过 %s S%dE%d（%s）" % (
                    item.get("series"), _season, ep, str(why)[:60]),
                    file=sys.stderr, flush=True)
                return None
            # check_existing() 的 why（state_key / resource_state / teldrive）说明
            # **这一集已经入库**，只是当前目标画质/路径对不上（例如库里已有 1080p，
            # 而源站这一集给的是别的画质 → 目标路径不同 → 判成"缺"）。
            # 原来这里直接 `continue`：不记日志、不落冷却，下一轮游标转回来又重查
            # 一遍源站 + dedupe。现在与 episode_anywhere() 命中同等对待：记一条
            # duplicate（带 why，便于归因），并计入本剧已收完的集数。
            duplicate_log(item, ep, key, target, why, season=int(item["season"]))
            _k2 = ep_cool_key(item, _season, ep)
            if _k2:
                mark_new_done(_k2, days=7, reason="fill_ep_present:%s" % why[:40])
            continue
        # 2026-11-15（E132c）：**探源失败要换下一个候选，不能整条 lane 直接抛。**
        # 实测 goemby.entertang.work 对「火线家族 S1E7」返回的不是视频
        # （probe total=0 magic=BAD），而 make_job() 抛 BadSource 会一路冒到
        # run_one.sh，这一轮就白跑；下一轮又选中同一个候选（同序），形成死循环。
        # 改成：记一次源失败（进冷却，下游不再选它）+ 换下一个候选继续试。
        _cands_left = [x for x in (cands or []) if x is not c]
        while True:
            try:
                return make_job("fill", item, item["season"], ep, target, c, key,
                                store_ch, store_channels, slot, run_id)
            except BadSource as _bs:
                try:
                    GS.record_source_failure(_bs.label, "source_preflight_not_video")
                except Exception:                          # noqa: BLE001
                    pass
                print("fill: 源 %s 不是视频，换下一个候选（E132c）" % _bs.label,
                      file=sys.stderr, flush=True)
                _nxt = None
                for _x in _cands_left:
                    _t2, _c2 = None, None
                    _ch2, _w2 = fill_target(item, ep, [_x], min_bytes, max_bytes)
                    if _ch2:
                        _nxt = _ch2
                        break
                    _cands_left.remove(_x)
                if not _nxt:
                    break
                target, c = _nxt
                key, why = check_existing(item, int(item["season"]), ep,
                                          target, c["quality"]["label"])
                if why:
                    break
    # 2026-11-15 根因修复（狂怒追缉 17 条 / 王冠 3 条 duplicate，「同一剧每轮反复」）：
    # fill 车道**整条 item 都没有产出 job** 时，以前直接 return None —— 不落任何冷却。
    # 于是队列游标每轮转回来又重走一遍：episode_anywhere() 命中 → duplicate_log，
    # 一天 40 轮就是 40 条同剧日志，而且每次都重查 state_key/resource_state/Emby。
    #
    # 判据：本轮声明的 missing 里，**已经全部在库（任意画质）** = 这条 item 是
    # 队列构建时的陈旧缺口（build 在 05:35，而 E4..E8 是 04:xx-05:31 陆续入库的）。
    # 这种条目在库里不会自己变「有货」，只有源站补集才会 —— 与 new 车道的
    # series_partial_done 同理，给 12h 短冷却。
    # 不算冷却是「确实还有真缺但本轮没扫到」（max_eps_scan=2 轮转）：
    # 那种情况下一轮会轮到别的集，不能冷却，否则真缺永远补不上。
    _missing = [int(x) for x in (item.get("missing") or [])]
    if _missing:
        _still = []
        _probe_broken = False
        for _e in _missing:
            _h, _ = episode_anywhere(item, int(item["season"]), _e)
            if probe_failed(_h):
                _probe_broken = True
                break
            if not _h:
                _still.append(_e)
        if _probe_broken:
            # 探测没做成 ≠ 全在库：绝不能落 fill_all_present，下一轮重来。
            return None
        if not _still:
            try:
                # 2026-11-15：键必须与队列过滤用的 item_cool_key() 一致，
                # 否则写了冷却也过滤不掉（原来写 fill:<tmdb>:S<n>，过滤读 None）。
                mark_new_done(item_cool_key(item) or "fill:%s:S%s" % (
                    item.get("tmdb"), item.get("season")),
                    days=0.5, reason="fill_all_present")
            except Exception:                              # noqa: BLE001
                pass
            return None
    return None


def try_upgrade(item, available_labels, leased, min_bytes, max_bytes, run_id,
                store_ch, store_channels, slot, lane="upgrade", leased_load=None):
    old_rel = str(item.get("current_rel") or "")
    old_abs = os.path.join(A.MNT_ROOT, old_rel.replace("/", os.sep))
    if not old_rel or not os.path.exists(old_abs):
        return None
    season, ep, why = effective_position(item, old_rel)
    if why:
        print("upgrade: effective-position check failed: %s" % why,
              file=sys.stderr)
        return None
    item = {**item, "season": season, "ep": ep}
    allowed = GS.eligible_sources(available_labels, leased, load=leased_load)
    if not allowed:
        return None
    g = SG.Guard(verbose=False)
    try:
        with contextlib.redirect_stdout(sys.stderr):
            cands = SP.resolve_candidates(
                g, item["series"], int(item["season"]), int(item["ep"]),
                exclude=HARD_EXCLUDE, max_hosts=3, year=item.get("year", 0),
                order_labels=allowed)
    finally:
        GS.charge_guard(g)
        chosen, why = upgrade_target(item, cands, min_bytes, max_bytes)
    if not chosen:
        return None
    target, c = chosen
    pre_dup = os.path.abspath(target) == os.path.abspath(old_abs)
    if os.path.exists(target) and not pre_dup:
        return None
    if pre_dup:
        key = A.state_key_for(item["tmdb"], int(item["season"]), int(item["ep"]),
                              os.path.basename(target))
    else:
        key, why2 = upgrade_duplicate(
            item, target, c["quality"]["label"], old_rel)
        if why2:
            return None
    job = make_job(lane, item, item["season"], item["ep"], target, c, key,
                   store_ch, store_channels, slot, run_id, upgrade=True)
    if pre_dup:
        job["pre_dup"] = True
    return job


def try_new(item, available_labels, leased, min_bytes, max_bytes, run_id,
            store_ch, store_channels, slot, lane="new", leased_load=None, active=None, allowed=None):
    if new_done_hot(item.get("video_id")):
        return None, "cooldown"
    # 该剧已有 claim 在跑（任意集）→ 整部跳过，别逐集重扫。
    # 见 series_has_active_claim() 的说明：亚托莉 E9 下载中时，E1..E8 每轮被
    # 重复确认 8 次，一天能烧掉一个源的配额。
    if series_has_active_claim(item.get("tmdb"), active):
        return None, "series_active"
    # 队列里已经没有任何缺口（missing 为空）却在跑 new 车道：直接长冷却，
    # 不要为了「凑一条」去源站把整部剧已入库的集重扫一遍。
    # 必须排在源站预算闸门之前：源配额打满时也要能把这类残项关掉。
    missing_field = item.get("missing")
    if isinstance(missing_field, list) and not missing_field:
        mark_new_done(item["video_id"], days=7, reason="no_missing")
        return None, "no_missing"
    # 2026-11-14：允许外部传入 allowed，让主循环一轮只算一次（见循环前的排序）。
    # 否则每个条目都调一次 eligible_sources，既推进 rr 游标 30 次，也无法据此排序。
    if allowed is None:
        allowed = GS.eligible_sources(available_labels, leased, load=leased_load)
    if not allowed:
        return None, "source_budget"
    # 2026-11-14：队列条目点名了 source_label（目录只保证**那个源**有这部剧）。
    # 若那个源当前不在可用名单（配额/租约/轮转），原来会退化成
    # "去另外 2 个源按剧名搜" —— 搜不到就判 no_source_probe 冷藏 3 天，
    # 白烧 2-3 次搜索。实测 no_job 一次试 30 条，这类空转占了搜索量的大头。
    # 现在：点名源不可用 → 零成本跳过，不冷藏，下一轮它回来了自然就做。
    _hint_label = item.get("source_label")
    if _hint_label and _hint_label not in set(allowed):
        return None, "hint_source_busy"
    g = SG.Guard(verbose=False)
    try:
        with contextlib.redirect_stdout(sys.stderr):
            # 2026-11-14：把队列带的「哪个源+哪个 series_id」传进去。
            # 队列是从源站目录建的，因此这两个字段是确定的；
            # 原来只用他们分片，解析时重新按剧名搜 —— 搜不到就
            # no_source_probe 冷藏 3 天（今日 255 部）。
            sources = SP.resolve_series_all(
                g, item["series"], exclude=HARD_EXCLUDE, max_hosts=3,
                year=item.get("year", 0), order_labels=allowed,
                hint=(item.get("source_label"), item.get("source_series_id")))
    finally:
        GS.charge_guard(g)
    if not sources:
        # 2026-11-14：**区分"源站没有"和"这次没取到"。**
        # 队列条目带着确定的 source_label + source_series_id，
        # 这种情况下取不到集，多半是**临时失败**（目录过期、404、超时、
        # /Shows/{id}/Episodes 报错）。而原来一律按 no_source_probe 冷藏 2-4 天。
        # 实测 new3 一轮 30 次尝试里 15 次 no_source，全是这类。
        # 现在：有点名 ID 的 → 30 分钟后重试；真的按剧名搜不到 → 才冷藏几天。
        hinted = bool(item.get("source_label") and item.get("source_series_id"))
        if hinted:
            mark_new_done(item["video_id"], days=0.02, reason="hint_fetch_failed")
            return None, "hint_fetch_failed"
        # 2026-11-14：12 小时 → 3 天。
        # 队列里有 288 部剧在任何源上都搜不到（no_source_probe）。按 12 小时冷却，
        # 它们每天全部回来一次，每部都要把 9 个源搜一遍 —— 288 × 9 = 2592 次，
        # 几乎等于全天配额（2700）。而"源站新增一整部老剧"是极低频事件，
        # 每天重试只是在烧配额。3 天一次既能及时捡到新增，又把这项开销降到 1/6。
        # 打散到期时间：同一天失败的一批剧如果都写 +3 天，三天后会**同时**回来，
        # 一次 288 部 × 9 源 = 2592 次请求，把自己造的尖峰打满当天配额。
        # 展开成 2~4 天随机，长期看是均匀回扫，不会形成周期性尖峰。
        mark_new_done(item["video_id"], days=random.uniform(2.0, 4.0),
                      reason="no_source_probe")
        return None, "no_source"
    item2, chosen, why = pick_new_episode(item, sources, min_bytes, max_bytes)
    if not chosen:
        if why == "series_complete":
            mark_new_done(item["video_id"], days=7, reason="series_complete")
        elif why == "series_partial_done":
            # 源站只列了部分集、其余全是已入库的重复：给短冷却等源站补集，
            # 否则它会每 5 分钟把同样 N 条 duplicate 重写一遍。
            mark_new_done(item["video_id"], days=0.5, reason="series_partial_done")
        elif why in ("no_tmdb", "bad_tmdb"):
            # 2026-11-14：3 天 → 30 天。源站没给 tmdb 的条目**不会自己变好**，
            # 而实测一轮 30 次尝试里有 16 次花在这类条目上（排序后它们被顶到最前）。
            # 它们没有 tmdb 就写不出 NFO/建不了剧，扫了也是白扫。
            mark_new_done(item["video_id"], days=30, reason=why)
        return None, why
    job = make_job(lane, item2, chosen["season"], chosen["ep"], chosen["target"],
                   chosen["candidate"], chosen["key"], store_ch, store_channels,
                   slot, run_id)
    return job, ""


def active_job_duplicate(job, active):
    """Reject a job already held by another shard during a shard transition."""
    try:
        key = (str(job.get("tmdb")), int(job.get("season")), int(job.get("ep")))
    except Exception:                                      # noqa: BLE001
        return False
    for other in active:
        try:
            other_key = (str(other.get("tmdb")),
                         int(other.get("season")), int(other.get("ep")))
        except Exception:                                  # noqa: BLE001
            other_key = None
        if other_key == key:
            return True
        if job.get("target_rel") and other.get("target_rel") == job.get("target_rel"):
            return True
    return False


def series_has_active_claim(tmdb, active):
    """该剧是否已有 claim 正在跑（不论哪一集）。

    2026-11-13：亚托莉在源站只列到 E8、而 E9 已由另一路 claim 领取下载中。
    差集判断只看 Emby 索引，E9 还没入库 → 判成 series_complete 的路径走不通，
    逐集流程把 E1..E8 又确认一遍，每轮 8 条 duplicate，一天下来占满配额。
    真正该问的是「这部剧现在有没有人正在处理」—— 有的话整部跳过，
    等那条 claim 落地，而不是每 5 分钟重扫一遍。
    """
    if not tmdb:
        return False
    # 2026-11-20：**从「整部串行」放宽到可配置的每剧并行度。**
    # 原来一部剧同时只能有一条 claim —— 队列集中在少数几部剧时，
    # 加机器也不会加吞吐（这是继 lane 锁、ingest 闸门之后的第三处串行点）。
    # 默认仍是 1（保护源站配额、避免整部重扫），但可以通过
    # EMBYTG_SERIES_PARALLEL 调高，配合 series_scan_cache 复用扫描结果。
    _limit = int(os.environ.get("EMBYTG_SERIES_PARALLEL") or 1)
    _seen = 0
    for other in active or []:
        try:
            if str(other.get("tmdb")) == str(tmdb):
                _seen += 1
                if _seen >= _limit:
                    return True
        except Exception:                                  # noqa: BLE001
            continue
    return False


def effective_max_gb(lane, hour=None):
    """单集体积上限（GB）。

    产能实测（2026-11-17）：今日 107 集里 >4GB 的 16 集吃掉了 39% 的流量与
    绝大部分 lane 时间（4.6–7.3GB 的单集在慢号上要传 1 小时+），而中位数只有
    1.49GB。白天让大文件挤占 lane 就是用小文件的机会成本换大文件。

    规则：
      · upgrade 车道永远 8GB —— 它存在的意义就是追 4K/大文件；
      · 白天（08:00–02:00）new/new2/fill 上限 4GB，把流水线留给中位数大小的集；
      · 凌晨（02:00–08:00）全车道放开 8GB —— 那时候本来闲着。

    超限的条目不会被丢弃：当晚就会重新进入候选（too_big 只是本轮跳过）。
    """
    if lane == "upgrade":
        return 8.0
    h = time.localtime().tm_hour if hour is None else hour
    return 8.0 if (2 <= h < 8) else 4.0


TG_POOL_JSON = os.getenv("EMBYTG_TG_POOL", "/opt/embytg-runner/config/tg_pool.json")


def lane_rate_mbps(slot, path=None, max_age_sec=3 * 3600, now=None):
    """读账号池里该槽位的实测吞吐（MB/s）。无数据 / 无该槽 / 样本过期 → 0（=未知）。

    「过期当作未知」是给慢号留的回血通道：lane 因为太慢被限了大文件后不再有上传样本，
    若把旧值当永久事实，它会永远领不到活（自我实现的预言）。超过 max_age_sec 就重新
    按未测量处理，放它跑一次、用真实结果覆盖旧结论。
    """
    p = path or TG_POOL_JSON
    t = time.time() if now is None else float(now)
    try:
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        e = (d.get("nodes") or {}).get(str(int(slot))) or {}
        mb = float(e.get("mbps") or 0)
        up = float(e.get("updated") or 0)
    except Exception:                                       # noqa: BLE001
        return 0.0
    if mb <= 0:
        return 0.0
    if max_age_sec and up and (t - up) > float(max_age_sec):
        return 0.0                                          # 过期 → 当未测量
    return mb


def rate_limited_max_gb(base_gb, mbps, budget_sec=None, floor_gb=0.5):
    """E174：单集上限再按「该号实测吞吐 × 上传墙钟预算」压一次。

    事故（2026-11-18，E171/E173 的源头）：本机 four lane 各自绑定一个账号（单会话），
    实测 0.06 / 2.62 / 0.96 / 0.21 MB/s，而 `effective_max_gb()` 只看车道与时段。
    于是 0.06 MB/s 的 lane 反复领 3.5–5.4GB 的集：上传数学上跑不完 `timeout 9000`，
    11-17 17:24 起每小时失败一次（11 次）= 11 小时墙钟 + 11 集源站配额白烧。

    判据：`mbps × budget ÷ 1000`（GB）与车道上限取小；mbps<=0（未测量）不设限。
    floor_gb 防止慢号彻底断粮（低于它的集照领，用于回血与测速）。
    """
    if mbps is None or float(mbps) <= 0:
        return base_gb
    b = float(budget_sec if budget_sec is not None
              else os.getenv("EMBYTG_LANE_UPLOAD_BUDGET_SEC", "6000"))
    cap = float(mbps) * max(60.0, b) / 1000.0
    return round(min(float(base_gb), max(float(floor_gb), cap)), 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--lane", choices=LANES, default="fill")
    ap.add_argument("--max-active", type=int, default=2)
    ap.add_argument("--max-size-gb", type=float, default=None,
                    help="单集体积上限(GB)；不填=按车道与时段自动（见 effective_max_gb）")
    ap.add_argument("--min-size-mb", type=float, default=20.0)
    ap.add_argument("--max-attempts", type=int, default=10)
    ap.add_argument("--max-eps-scan", type=int, default=2)
    ap.add_argument("--lock-wait", type=int, default=900)
    ap.add_argument("--source-allow", default=os.getenv("GHA_SOURCE_ALLOW", ""),
                    help="逗号分隔的源标签白名单；为空表示不限制")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    # 2026-11-20：**锁按 lane 分，不再全进程独占。**
    # 原来是单文件 .gha_next.lock，谁抢到谁独占整个「选片+扫描」过程——
    # 5 台机器排队等一把锁，加机器等于没加。lane 之间没有共享可变状态
    # （候选来源不同、落 claim 靠条目 lease 互斥），没必要互相等。
    _lane_lock = LOCK.replace(".lock", ".%s.lock" % a.lane)
    fd = os.open(_lane_lock, os.O_CREAT | os.O_RDWR, 0o600)
    deadline = time.time() + max(0, a.lock_wait)
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if time.time() >= deadline:
                emit({"ok": False, "reason": "busy"})
                return 0
            time.sleep(2)

    if os.path.exists(PAUSE_FILE) and not a.force:
        try:
            detail = open(PAUSE_FILE, encoding="utf-8").read().strip()
        except Exception:                                  # noqa: BLE001
            detail = ""
        emit({"ok": False, "reason": "paused", "detail": detail[:200]})
        return 0

    # 2026-11-15（E075 二次复发，box2）：TG 授权被吊销后，该 lane 每次领片都会
    # 下载完整一集再在上传时炸 AuthKeyDuplicated —— 白烧一次下载 + 一条配额，且日志只留
    # "upload_failed"，根因要挖三层才看得到。实测 box2 从 11-14 01:28 起空转 23 小时、
    # 240 个工作目录全废。所以：授权失效必须变成**显式闸门文件**，不是靠人记得去看 upload.log。
    # 恢复方式：人工重登后 `rm pipeline/.gha_lane_auth_dead/<lane>`（或 gha_cancel 时自动清）。
    _dead = os.path.join(HERE, ".gha_lane_auth_dead", a.lane)
    if _lane_gated(_dead) and not a.force:
        try:
            _why = open(_dead, encoding="utf-8").read().strip()
        except Exception:                                  # noqa: BLE001
            _why = ""
        emit({"ok": False, "reason": "lane_auth_dead", "lane": a.lane,
              "detail": _why[:200]})
        return 0

    # 2026-11-15（E129）：resume_scan 连环失败 → 短熔断（1h），到期自动再试。
    # 目的不是惩罚那台机器，而是别再让它每分钟白占一个 claim 槽。
    if _lane_scan_gated(a.lane) and not a.force:
        _p = os.path.join(HERE, ".gha_lane_scan_broken", a.lane)
        try:
            _why2 = open(_p, encoding="utf-8").read().strip()
        except Exception:                                  # noqa: BLE001
            _why2 = ""
        emit({"ok": False, "reason": "lane_scan_broken", "lane": a.lane,
              "detail": _why2[:200]})
        return 0

    active = read_claims()
    # 2026-11-20：删掉「同 lane 已有 claim 就不许再领」的串行门闩。
    # 用户口径：worker/机器增加，吞吐应按可用 worker、session 和源供给扩展，
    # 领任务不该成为机器数量的串行瓶颈。互斥改由 tg.db.job_claim 的「按集」lease
    # 保证：两个 worker 可以同跑一个 lane，但同一集只会被一个抢到。
    _ = a.lane
    # 2026-11-15（E105）：**并发上限由池子决定，不由人工调参。**
    #
    # 用户原话：「不用总是调上限吧，不是有池子的概念吗，
    # 如果以后上传机子更多、账号更多呢」——一针见血。
    # 写死 --max-active 3 的害处：加第 4 个账号后必须手工改成 4，
    # 否则第 4 条 lane 永远空转；加第 5 个又得再改一次。这不是池化，
    # 这是把人工配置伪装成并发控制。
    #
    # 真正的上限 = **当前可用的 TG 账号槽位数**（_session_count 减去预留）。
    # 加账号 → 自动多一条并发；减账号 → 自动收敛。人工传的
    # --max-active 只作为**上限的天花板**（可以调低保护，不能突破池子容量）。
    _pool_cap = max(1, _session_count() - len(reserved_slots()))
    # 2026-11-20：外部 worker（PC）自带 session，不能因为本机 4 个号全忙就被挡住。
    # 用户口径：多一台 PC 就该多一份产量。这里把 scale.json 里声明的外部 session
    # 计入总容量；外部 worker 的活跃 claim 已经算进 active，不会重复放宽。
    # 2026-11-20 二次修订：**加机器不需要报备**。
    # 容量外部部分取三者最大：scale.json 声明数、当前活跃 claim 里出现的外部 worker 数、
    # 以及 1 格「新机器预留位」。新机器第一次来就能领到活（预留位），领到之后
    # 它出现在 active 里，容量自动增长；下线后 active 消失，容量自动回落。
    _ext_declared = 0
    _sc = {}
    try:
        _sc = json.load(open(os.path.join(os.path.dirname(HERE), "config", "scale.json"),
                             encoding="utf-8"))
        _ext_declared = sum(int(w.get("sessions") or 1) for w in (_sc.get("external_workers") or [])
                            if w.get("enabled", True))
    except Exception:                                          # noqa: BLE001
        _ext_declared = 0
    _local_names = {"slot1", "slot2", "slot3", "slot4", "local"}
    _ext_seen = set()
    for _c in active:
        _w = str(_c.get("worker") or "")
        _rid = str(_c.get("run_id") or "")
        for _tok in list(_local_names) + ["slot5", "slot6", "slot7", "slot8"]:
            if _tok and _tok != "local" and (_tok in _rid or _w.startswith(_tok)):
                if _tok not in _local_names:
                    _ext_seen.add(_tok)
    # +1 是「下一台新机器」的位置：否则当外部 worker 已满员时，
    # 新机器第一次来就撞 cap，还是得报备。（第二轮自查发现）
    # 坏节点不该稀释产能：连续失败/成功率过低的 worker 从有效容量里剔除，
    # 但保留 1 格预留位，让新机器和「已经恢复的旧机器」都能重新进场。
    _sc_health = _sc if isinstance(_sc, dict) else {}   # _sc 在 try 里可能未绑定
    _health = GS.worker_health(
        min_rate=float(_sc_health.get("min_success_rate") or 0.5),
        consec_limit=int(_sc_health.get("consecutive_fail_limit") or 3))
    _unhealthy = {w for w, h in _health.items() if not h.get("healthy", True)}
    _ext_ok = {w for w in _ext_seen if w not in _unhealthy
               or os.environ.get("EMBYTG_IGNORE_HEALTH") == "1"}
    # 末尾 +1 是「当前这个外部 claimant 自己的位置」：否则外部 worker 自己
    # 计入 active 之后又被自己的位置挡住（实测 active=4/cap=4 空转）。
    # +1 = 当前 claimant 自己的位置；再 +1 = 下一台新机器的位置。
    # 原来只 +1，导致「本机 3 + PC 1 + 遗留 claim 2」把 cap 顶满后，
    # 外部 worker 连自己那一格都算进去，反而领不到（实测 active=6/cap=6）。
    _ext_cap = max(_ext_declared, len(_ext_ok) + 1, 1) + 2
    _total_cap = _pool_cap + _ext_cap
    # 2026-11-21：--max-active 只管本机 worker（默认 2）。外部 worker（GHA/PC）
    # 自带 session，不该被这个本机旋钮挡住——否则「来了就跑」永远跑不起来：
    # 实测 active=4、cap=2、pool_cap=3，外部单子被本机上限卡死。
    _is_ext_early = (os.environ.get("EMBYTG_EXTERNAL") == "1"
                     or bool(re.search(r"slot([5-9])", str(getattr(a, "run_id", "") or ""))))
    if _is_ext_early:
        _cap = _total_cap
    else:
        _cap = min(int(a.max_active or _total_cap), _total_cap)
    if len(active) >= _cap and not a.force:
        emit({"ok": False, "reason": "max_active", "active": len(active),
              "cap": _cap, "pool_cap": _pool_cap})
        return 0
    # 2026-11-20：ingest_busy 原本是全局闸门 —— 只要 autoingest 在跑，
    # 所有 worker 一律领不到活，等于把并发压回 1。
    # 改成本机首次并发限制：允许 1 个继续，其余等（默认关闭，可用
    # EMBYTG_INGEST_GATE=1 恢复旧行为）。
    if ingest_busy() and not a.force:
        _gate = os.environ.get("EMBYTG_INGEST_GATE", "0") == "1"
        if _gate or len(active) >= 1:
            emit({"ok": False, "reason": "autoingest_busy",
                  "active": len(active)})
            return 0

    if LQ.is_done(a.lane) and not a.force:
        emit({"ok": False, "reason": "lane_done", "lane": a.lane})
        return 0

    # 2026-11-14（E097）：**队列池化 —— 自己的队列空了就去抢别人的活。**
    #
    # 用户原话：「上片应该是随时能上就上，你不上换另一个也一样」。
    # 原来 lane 和队列是硬绑的：new2 只能领 new、fill 只能领 fill。
    # 于是 new 队列空了、fill 还有 12 条时，new2 的号只能空转
    # （今晚实测 NOJOB reason=no_job 出现多次，而 fill/upgrade 都有货）。
    #
    # 改成：按优先级依次尝试本 lane 队列 → 其他队列。
    #   · 优先级：new > fill > upgrade（新剧优先，升级任务不能挤掉新剧）
    #   · 并发安全不靠分片，靠 claim（同剧有活跃 claim 就跳过）
    #     + state_key 去重（同一集任何画质已入库就不下）——本来就有
    #   · 全空才真的 idle
    _PRIORITY = ("new", "fill", "upgrade")
    _my = ("new" if a.lane in ("new", "new2", "new3", "new4", "new5")
           else a.lane)
    _try = [_my] + [x for x in _PRIORITY if x != _my]

    qpath = QUEUE_FILES[a.lane]
    if not os.path.exists(qpath) and a.lane != "new":
        emit({"ok": False, "reason": "no_queue", "lane": a.lane})
        return 0
    data = {}
    items = []
    if os.path.exists(qpath):
        data = json.load(open(qpath, encoding="utf-8"))
        items = [GS.norm_job(x) for x in (data.get("items") or [])]

    # 本队列空 → 依次尝试其他队列（池化取活）
    if not items:
        for _alt in _try[1:]:
            _ap = QUEUE_FILES.get(_alt)
            if not _ap or not os.path.exists(_ap):
                continue
            try:
                _d = json.load(open(_ap, encoding="utf-8"))
                _it = _d.get("items") or []
            except Exception:                              # noqa: BLE001
                continue
            if _it:
                emit({"ok": True, "pooled_from": _alt, "lane": a.lane})
                data, items = _d, [GS.norm_job(x) for x in _it]
                # 不改进程内的 a.lane：后面 claim 写入、日志标签都依赖原 lane，
                # 改它会让 claim 归属错乱。只借用它的数据。
                _pooled_from = _alt
                break
    # 实际消费的队列类型（可能是池化借来的）
    _qkind = locals().get("_pooled_from") or (
        "new" if a.lane in ("new", "new2", "new3", "new4", "new5") else a.lane)
    if _qkind == "new" and not items:
        built = refresh_new_queue()
        if not built.get("ok") and built.get("reason") != "new_builder_cooldown":
            emit({"ok": False, "reason": built.get("reason"),
                  "lane": a.lane, "detail": built.get("detail")})
            return 0
        if os.path.exists(qpath):
            data = json.load(open(qpath, encoding="utf-8"))
            items = [GS.norm_job(x) for x in (data.get("items") or [])]
    if _qkind == "fill":
        items = A.dedupe_items(items)
    # 池化借来的活不做分片：分片是按"这条 lane 该看队列的哪一份"设计的，
    # 借来的活本来就不属于本 lane，硬筛会 KeyError（a.lane 不在映射表里），
    # 也可能把它筛掉、等于没借到。
    _pooled = bool(locals().get("_pooled_from"))
    if _qkind == "new" and not _pooled:
        if os.path.exists(os.path.join(HERE, ".gha_new5_enabled")):
            shards = 5
            shard = {"new": 0, "new2": 1, "new3": 2,
                     "new4": 3, "new5": 4}[a.lane]
        elif os.path.exists(os.path.join(HERE, ".gha_new3_enabled")):
            shards = 3
            if a.lane in ("new4", "new5"):
                emit({"ok": False, "reason": "new_shard_disabled",
                      "lane": a.lane})
                return 0
            shard = {"new": 0, "new2": 1, "new3": 2}[a.lane]
        elif os.path.exists(os.path.join(HERE, ".gha_new2_enabled")):
            shards = 2
            if a.lane in ("new3", "new4", "new5"):
                emit({"ok": False, "reason": "new_shard_disabled",
                      "lane": a.lane})
                return 0
            shard = 1 if a.lane == "new2" else 0
        else:
            shards = 1
            if a.lane != "new":
                emit({"ok": False, "reason": "new_shard_disabled",
                      "lane": a.lane})
                return 0
            shard = 0
        _unsharded = list(items)          # 2026-11-14：全量副本
        # 2026-11-14（第九版）：**取消分片过滤**。
        # 原意是把队列分给五条 lane，但实测：每条 lane 只看 1/3，
        # 而那 1/3 里大半条目已停泊（今日 883/6000）→ lane 反复 no_job，
        # 实测 07:19/07:21 两轮放行各花 6 次请求、零产出。
        # 并行安全本来就不靠分片：靠 claim（同剧有活跃 claim 就跳过）
        # + state_key 去重（同一集任何画质已入库就不下）。
        # 分片只带来“饿死”，不带来安全。
        if shards > 1 and not os.path.exists(os.path.join(HERE, ".gha_no_shard")):
            full_items = len(items)
            items = [x for x in items if new_shard(x, shards) == shard]
            if not items:
                emit({"ok": False, "reason": "no_job", "lane": a.lane,
                      "detail": "new_shard_empty", "queue_remaining": full_items})
                return 0
    if not items:
        emit({"ok": False, "reason": "empty_queue", "lane": a.lane})
        return 0
    # 2026-11-14：分片块在 `if a.lane in (...)` 里，某些 lane 不会经过那里，
    # _unsharded 就未定义。用 locals() 取，避免 NameError。
    _unsharded = locals().get("_unsharded") or list(items)
    if a.lane in ("upgrade", "upgrade2"):
        shard = 1 if a.lane == "upgrade2" else 0
        full_items = len(items)
        items = [item for item in items if upgrade_shard(item) == shard]
        if not items:
            emit({"ok": False, "reason": "no_job", "lane": a.lane,
                  "detail": "upgrade_shard_empty",
                  "queue_remaining": full_items})
            return 0
    start = load_cursor(a.lane) % len(items)
    items = items[start:] + items[:start]

    # 2026-11-13：源站日配额全满时，不要拿 1900 部的 `new` 队列逐条空转
    # （实测今日 1107 条 duplicate 扫描 vs 完成 233 条，runner_report 判"队列空转"）。
    # 本轮只做两件事：把能确定"没有缺口"的残项一次性关掉（missing==[] → 7 天冷却，
    # 这是既有语义，不新增预算/阈值），然后干净地返回 source_budget 让 timer 下一轮再来。
    # 注意 eligible_sources 会推进 rr 游标，这里只在「一轮都没有源可用」时才走到。
    if _qkind == "new":
        probe_labels = sorted(SG.Guard(verbose=False).states)
        if a.source_allow:
            probe_labels = [x for x in probe_labels
                            if x in {y.strip() for y in a.source_allow.split(",") if y.strip()}]
        if not GS.eligible_sources(probe_labels, set(), load={}):
            closed = 0
            for it in items[:200]:
                mf = it.get("missing")
                if isinstance(mf, list) and not mf and not new_done_hot(it.get("video_id")):
                    mark_new_done(it["video_id"], days=7, reason="no_missing")
                    closed += 1
            emit({"ok": False, "reason": "source_budget", "lane": a.lane,
                  "detail": "all_sources_saturated; no_missing_closed=%d" % closed})
            return 0

    # 2026-11-13 软租约：leased 不再作为「排除集合」传给源解析，否则
    # 一个源被占用就会让所有节点都拿不到（实测 3 个能产出的源被 3 个 claim
    # 由 eligible_sources(load=...) 处理。exclude 只保留真正的硬排除（空集合）。
    # 2026-11-22：**外部 worker 不该被本机的源租约挡住。**
    # 「单源并发 1」是为了不让同一出口/同一账号打同一个源；
    # GHA 在 GitHub 的网络里，出口 IP 完全不同，源站看到的是另一个客户端，
    # 所以本机占着某个源不代表 GHA 不能用它。
    # 实测：3 个本机 worker 占满快源后，GHA 永远 source_budget: queue_sources_throttled。
    _is_ext_now = (os.environ.get("EMBYTG_EXTERNAL") == "1"
                   or bool(re.search(r"slot([5-9])", str(getattr(a, "run_id", "") or ""))))
    leased = set() if _is_ext_now else GS.leased_sources(active)
    leased_load = GS.leased_source_load(active)
    probe = SG.Guard(verbose=False)
    available_labels = sorted(probe.states)
    if a.source_allow:
        allow = {x.strip() for x in a.source_allow.split(",") if x.strip()}
        available_labels = [x for x in available_labels if x in allow]
        if not available_labels:
            emit({"ok": False, "reason": "source_allow_empty", "lane": a.lane,
                  "detail": ",".join(sorted(allow))})
            return 0
    _size_explicit = a.max_size_gb is not None
    if a.max_size_gb is None:
        a.max_size_gb = effective_max_gb(a.lane)
    max_bytes = int(a.max_size_gb * 1e9)
    min_bytes = int(a.min_size_mb * 1024 * 1024)
    store_ch, store_channels = choose_store_channel()
    # 2026-11-14（E075）：session 是跨机器的排他资源，拿不到租约就别领片。
    # 硬领只会让两台机器用同一个 TG 账号从不同 IP 上传 → AuthKeyDuplicatedError
    # → TG 作废授权（要人工重新登录才能恢复）。宁可空转等，也不能撞。
    _rid = getattr(a, "run_id", None) or ("next3-%d" % os.getpid())
    # 2026-11-20：**外部 worker 用自己的 session，不租本机的 1-4 号。**
    # 之前只加了容量计算，领任务仍要 session_slot —— 本机四号全忙时 PC 照样 no_session，
    # "多一台机器多一份产量"就是句空话。判据：EMBYTG_EXTERNAL=1 或 run_id 含 slot5+。
    import re as _re_x
    _is_ext = (os.environ.get("EMBYTG_EXTERNAL") == "1"
               or bool(_re_x.search(r"slot([5-9])", str(_rid))))
    if _is_ext:
        slot = 0          # 0 = 用调用方自己的账号，不占本机租约
        print("  外部 worker（%s）：跳过本机 session 租约，使用自带 session"
              % _rid, file=sys.stderr)
    else:
        slot = session_slot(active, run_id=_rid)
        if not slot:
            emit({"ok": False, "reason": "no_session",
                  "detail": "所有 TG session 均被占用（租约未释放或 TTL 未到）"})
            return 0
    # E174：慢号不许领「它算不完」的大文件。
    # 单会话模式下每条 lane 绑定一个账号，0.06MB/s 的号领到 3.5–5.4GB 的集
    # 就必然撞 timeout（E171：11 小时白烧 + 11 集源站配额）。
    if not _size_explicit:
        _mb = lane_rate_mbps(slot)
        _lim = rate_limited_max_gb(a.max_size_gb, _mb)
        if _lim < a.max_size_gb:
            print("  单集上限 %.2fGB → %.2fGB（slot%s 实测 %.2fMB/s × %ss 上传预算）"
                  % (a.max_size_gb, _lim, slot, _mb,
                     os.getenv("EMBYTG_LANE_UPLOAD_BUDGET_SEC", "6000")),
                  file=sys.stderr, flush=True)
            a.max_size_gb = _lim
            max_bytes = int(_lim * 1e9)

    # 2026-11-14（E075 修正）：拿到租约后，**任何非成功领片的出口都必须归还**，
    # 否则一次 NOJOB 就把唯一的 session 锁死 3 小时，整条链路停摆。
    # 成功领片时租约随 claim 走，由 gha_cancel/gha_finish 在收尾时归还。
    hold_lease(slot, _rid)
    # 2026-11-14：先把「点名源当前可用」的条目排到最前。
    # 起因：加了 hint_source_busy 短路后，一轮 30 次尝试里 24 次是"源不可用"直接跳过——
    # 便宜但没产出。队列 6000 条散在 9 个源上，而节流时只有 1-2 个源可用，
    # 随机顺序撞中率约 1/9。按可用源排序后，30 次尝试全部落在可用源上。
    # 2026-11-14（第二版）：**反过来问** ——“队列里有活的源，哪些可用”。
    # 第一版先问“哪些源可用”再拿结果去排队列，实测撞上了这个坑：
    # 轮转只返回 goemby.entertang.work 一个源，而它在队列里条目数为 **0**
    # （目录只有 200 部、新增 11 部没进最终 6000），
    # 于是 30 次尝试全部 hint_source_busy，一次活也接不到。
    # 现在：先取队列里实际存在的点名源，在这个子集里让预算/轮转选；
    # 选不出来才回退到全量源（保底）。
    # ……而且必须是"还有**未冷却**条目"的源。
    # 上一版只筛"队列里有条目"的源，结果选中一个只有 18 条、
    # 而且全在冷却中的源（它“用量最少”所以总被选中）→
    # 每轮一进来就撞空，五条 lane 集体停摆（实测近 15 分钟 0 集，
    # 而配额只用了 43%）。这是个死锁：越没用的源越容易被选，
    # 而它没活就越不会被用。
    _hot = new_done_hot_set()
    # 2026-11-14（第十版）：**先把冷却中的条目踢出循环**。
    # 实测 07:41 那轮：attempts=30，skip={'cooldown':29,'hint_fetch_failed':1}。
    # 游标恰好停在一段连续停泊区，30 次尝试全撞在冷却条目上 ——
    # 而队列里实际还有 5283 条可用。
    # 冷却条目对 try_new 是免费的，但它们把尝试次数吃光了，
    # 等于用 30 次机会去证明"这一段都在冷却"。直接过滤掉。
    # 2026-11-14（E074）：按集失败退避 —— 下不动的集别反复喂给 lane。
    _epbad = GS.episode_blocked_set()
    if _epbad:
        _before = len(items)
        items = [x for x in items
                 if str(x.get("key") or x.get("video_id")) not in _epbad]
        if len(items) != _before and os.environ.get("GHA_DEBUG"):
            print("[ep_backoff] 过滤 %d 条长冷却集" % (_before - len(items)))
    if _hot:
        # 2026-11-15：fill/upgrade 条目没有 video_id，以前这里读 None → 永远不进
        # 冷却过滤 → 陈旧的 fill item 每轮都被重新探测。改走 item_cool_key()。
        items = [x for x in items if item_cool_key(x) not in _hot]
        if not items:
            emit({"ok": False, "reason": "no_job", "lane": a.lane,
                  "detail": "all_cooldown:%d" % len(_hot)})
            return 0
    _qsrc = sorted({x.get("source_label") for x in items
                    if x.get("source_label")
                    and item_cool_key(x) not in _hot})
    if not _qsrc:
        _qsrc = sorted({x.get("source_label") for x in items if x.get("source_label")})
    try:
        _allowed_now = (GS.eligible_sources(_qsrc, leased, load=leased_load)
                        if _qsrc else [])
    except Exception:                                            # noqa: BLE001
        _allowed_now = []
    # 2026-11-14：**不要回退到全量源**。
    # 旧行为：_qsrc 的槽位关着 → 回退到全量源 → 轮转选中
    # goemby（队列里 **0 条**）→ 排序后第一条就不匹配 → break
    # → `attempts=0` 的 no_job。看起来像"没活干"，实际是"选中了一个没有任何条目的源"。
    # 有活的源都被限制时，诚实的答案就是 source_budget。
    if _qsrc and not _allowed_now:
        # 2026-11-14：**跨分片兜底**。
        # 分片是为了把队列分给五条 lane，但它把每条 lane 限在 1/3 的条目上。
        # 实测：本分片里只有 chapanda（127/105 满）和 ct（burst 61/60 满）等 4 个源全被卡，
        # 而另两个分片里 ok=10 的 39.96 和 lite 反而闲着 ——
        # 于是 lane 一分钟一次的重试全是空的（实测 40 分钟 0 集）。
        # 分片只是分工，不该让 lane 饿死；并行安全由 claim + state_key 保证，不靠分片。
        _qsrc_all = sorted({x.get("source_label") for x in _unsharded
                            if x.get("source_label")
                            and str(x.get("video_id")) not in _hot})
        _allowed_now = GS.eligible_sources(_qsrc_all, leased, load=leased_load)
        if _allowed_now:
            items = _unsharded
            print("  本分片源全部不可用，跨分片兜底到 %s"
                  % ",".join(_allowed_now), file=sys.stderr, flush=True)
    if _qsrc and not _allowed_now:
        emit({"ok": False, "reason": "source_budget", "lane": a.lane,
              "detail": "queue_sources_throttled:%d" % len(_qsrc)})
        return 0
    _aset = set(_allowed_now)
    if _aset:
        items = sorted(items, key=lambda _x: 0 if _x.get("source_label") in _aset else 1)
    attempts = 0
    # 2026-11-14：no_job 只告诉我们“试了 N 条都跳过”，不告诉为什么。
    # 排查时只能猜（cooldown？series_active？no_source？）。
    # 这里记一份直方图随 no_job 一起输出。
    skip_reasons = {}
    for item in items:
        # 2026-11-14：已按"点名源是否可用"排序。撞到第一个不匹配的就停 ——
        # 继续往下试只会得到 hint_source_busy 或 no_source，前者白耗循环，
        # 后者更坏：会把**真实存在的剧**误判成 no_source_probe 冷藏 2-4 天。
        # （实测：拿 12 个条目直接走 hint+max_hosts=1，12/12 全部取得集，
        #   平均 50 集/部 —— 说明 hint 本身可靠，失败都是"拿了错的源去试"造成的。）
        if _aset and item.get("source_label") not in _aset:
            break
        if attempts >= a.max_attempts:
            break
        # 2026-11-14（E088）：预算/租约类跳过不消耗尝试次数。
        # 原来 `attempts += 1` 打在 try_* 之前，实测一轮 10 次机会里 9 次被
        # hint_fetch_failed / hint_source_busy 吃掉（6h 内 81 条），
        # 队列里**真有货**的条目一次都没轮到就 return 0，白跑一轮。
        # 只统计「真发起源站请求」的条目：下面的 why 若属于未发请求的早退，
        # 记账时退票（见 finally）。
        skip_charge = False
        try:
            if a.lane == "fill":
                job = try_fill(item, available_labels, leased, min_bytes, max_bytes,
                               a.run_id, store_ch, store_channels, slot,
                               a.max_eps_scan, leased_load=leased_load)
            elif a.lane in ("upgrade", "upgrade2"):
                job = try_upgrade(item, available_labels, leased, min_bytes, max_bytes,
                                  a.run_id, store_ch, store_channels, slot,
                                  lane=a.lane, leased_load=leased_load)
            else:
                job, why = try_new(item, available_labels, leased, min_bytes, max_bytes,
                                   a.run_id, store_ch, store_channels, slot, a.lane,
                                   leased_load=leased_load, active=active,
                                   allowed=_allowed_now)
                if not job and why:
                    if why in CHARGE_FREE_WHY:
                        skip_charge = True
                    skip_reasons[why] = skip_reasons.get(why, 0) + 1
                    print("new: %s %s" % (item.get("series", "")[:30], why),
                          file=sys.stderr)
        except BadSource as e:
            print("source preflight rejected %s: %s" % (
                e.label[:80], e.reason), file=sys.stderr)
            try:
                GS.record_source_failure(e.label, e.reason)
            except Exception as record_error:              # noqa: BLE001
                print("source failure record failed: %s" % (
                    str(record_error)[:160]), file=sys.stderr)
            available_labels = [
                label for label in available_labels if label != e.label]
            continue
        except Exception as e:                             # noqa: BLE001
            print("lane=%s 解析失败 %s: %s" % (
                a.lane, item.get("series", "")[:30], str(e)[:160]), file=sys.stderr)
            continue
        finally:
            # 被 try_new 在**未发请求阶段**提前返回的（cooldown / series_active /
            # no_missing / hint_source_busy / hint_fetch_failed）不计入
            # --max-attempts（默认 10）：实测 6h 内 81 条这类空跳把整轮名额吃光，
            # 队列里真有货的条目一次都没轮到。
            if not skip_charge:
                attempts += 1
        if not job:
            continue
        if active_job_duplicate(job, active):
            print("active duplicate skipped %s S%02dE%02d" % (
                job.get("series", "")[:30], int(job.get("season", 0)),
                int(job.get("ep", 0))), file=sys.stderr)
            continue
        # 2026-11-14（E074 修正）：队列路径的过滤拦不住**搜索补缺**——
        # new2/new3 这类是实时搜源站生成 job，不经过 items 列表。
        # 所以必须在落 claim 前最后拦一次，否则下不动的集会无限重领。
        _jk = str(job.get("key") or job.get("video_id"))
        if _jk in GS.episode_blocked_set():
            skip_reasons["ep_backoff"] = skip_reasons.get("ep_backoff", 0) + 1
            attempts += 1
            continue
        # 2026-11-15：**落 claim 前最后一道闸 —— TMDB 锚点必须真实存在。**
        # 坏锚点是「死循环级」故障：NFO 永远补不出来 → 收尾永远 FAIL →
        # claim 永远不释放（实测 slot1 烧了一整晚 144 轮）。
        # 这里拦下来，代价是一次 HTTP 请求；放过去，代价是 1.4GB 下载 + 一条 lane 卡死。
        _tv = tmdb_id_valid(job.get("tmdb"), job_tmdb_kind(job))
        if _tv is False:
            skip_reasons["bad_tmdb"] = skip_reasons.get("bad_tmdb", 0) + 1
            print("claim rejected: tmdb id 不存在 %s (%s) %s S%02dE%02d"
                  % (job.get("tmdb"), job_tmdb_kind(job), job.get("series", "")[:24],
                     int(job.get("season") or 0), int(job.get("ep") or 0)),
                  file=sys.stderr)
            try:
                mark_new_done(item_cool_key(item) or "bad_tmdb:%s" % job.get("tmdb"),
                              days=7, reason="tmdb_404")
            except Exception:                              # noqa: BLE001
                pass
            attempts += 1
            continue
        # 条目级 claim：同一集只能被一个节点领走，不同节点领不同集。
        # 没有这一层，多节点只是排队（claim 文件按 lane 分，同 lane 只能一个）。
        # 2026-11-20：**不健康 worker 只能低调重试，不能持续烧配额。**
        # 实测 PC(slot5) 成功率 37%、slot1 连续失败 5 次，都是 upload_failed：
        # 每次都先下完整个文件再传失败，白白消耗源站配额。
        # 规则：判为不健康的 worker，距上次尝试 ≥10 分钟才允许再试一次（探活），
        # 连续成功会自然恢复（consec_fail 归零）。
        try:
            if os.environ.get("EMBYTG_IGNORE_HEALTH") != "1":
                _h = GS.worker_health().get(_worker) or {}
                if _h and not _h.get("healthy", True):
                    if time.time() - int(_h.get("last_ts") or 0) < 600:
                        emit({"ok": False, "reason": "worker_probation",
                              "worker": _worker,
                              "detail": "成功率 %.0f%%，10 分钟后才允许再试"
                                        % (_h.get("rate", 0) * 100)})
                        return 0
        except Exception:                                     # noqa: BLE001
            pass
        _wkey = GS.item_key(job)      # 去画质后缀，同集不同画质也算同一个 item
        # worker 身份：优先环境变量，其次从 run_id 里的 slotN 推断。
        # PC 的远程调用不会传 EMBYTG_WORKER，但它的 run_id 一定含 slot5，
        # 这样不依赖对方改脚本也能正确归属（用户 2026-11-20 反馈）。
        import re as _re_w
        _slot_m = _re_w.search(r"slot(\d+)", str(job.get("run_id") or a.run_id or ""))
        _worker = os.environ.get("EMBYTG_WORKER") or (
            "slot%s" % _slot_m.group(1) if _slot_m else "local")
        if os.environ.get("EMBYTG_HOST"):
            _worker = "%s:%s" % (os.environ["EMBYTG_HOST"], _worker)
        if not GS.claim_item(_wkey, _worker):
            skip_reasons["claim_taken"] = skip_reasons.get("claim_taken", 0) + 1
            attempts += 1
            continue
        write_claim(a.lane, job)
        mode = "洗版" if job.get("upgrade") else ("补缺" if a.lane == "fill" else "新增")
        broadcast("三线领取[%s/%s]：%s S%02dE%02d，画质=%s，源=%s，频道=%s，会话=%s；"
                  "已查 Emby有效季集/state_key/resource/teldrive（任意画质）。" % (
                      a.lane, mode, job["series"][:20], job["season"], job["ep"],
                      job.get("quality"), job["source_label"],
                      job.get("store_channel_id"), job.get("tg_session_slot")))
        clear_held_lease()   # 租约随 claim 走，交给 cancel/finish 归还
        emit({"ok": True, "job": job})
        return 0

    save_cursor(a.lane, (start + attempts) % len(items))
    emit({"ok": False, "reason": "no_job", "lane": a.lane,
          "attempts": attempts, "skip_reasons": skip_reasons})
    return 0


def _main_guarded():
    """包一层 finally：领片失败/异常时立刻归还 session 租约。

    2026-11-14（E075）：main() 里有几十个 return 0 的出口（no_job、
    source_budget、no_session、paused…）。任何一个出口漏了归还，
    唯一的 TG session 就被锁 3 小时，整条链路停摆 —— 实测出现过。
    在这里统一兜住，比在每个出口手写一次可靠。
    """
    try:
        return main() or 0
    finally:
        release_held_lease()


if __name__ == "__main__":
    sys.exit(_main_guarded())
