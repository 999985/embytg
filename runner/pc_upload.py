#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pc_upload.py — PC 端把本地视频上传到 TG 存储频道(多进程按 512MiB 区段并行)。
- 每区段 = 一条 TG 文档(一个 teldrive part)。
- 每区段由一个独立进程 pc_worker.py 单连接上传; 多进程并行叠加带宽
  (实测: 单进程约 0.6MB/s, 3 进程聚合约 1.9MB/s; 进程越多越快, 受代理上行/网络上限约束)。
- 逐区段落盘 manifest, 可断点续传(重跑跳过已完成段)。

用法:
  python pc_upload.py "F:\\阿里视频\\xxx.mkv" [--name 显示名.mkv] [--chunk-mib 512] [--jobs 8]
  1GB 级文件建议 --chunk-mib 64 --jobs 16；大文件可用 512MiB 分片。

产物: F:\\emby_pc\\_manifests\\<源basename>.json  (服务器登记用)
"""
import argparse, hashlib, json, os, re, subprocess, sys, time, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# 2026-11-14（E095）：账号池模块。之前用 s.replace("import hashlib", ...)
# 插入，但这行是**合并导入**（import argparse, hashlib, ...），
# 没有独立的 "import hashlib" 可匹配 → import 从未被插入 →
# 每次回填都 NameError: name 'TP' is not defined，被 except 吞掉。
# 教训：给 import 打补丁要确认目标行的确切形态，别假设一行一个模块。
import tg_pool as TP

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.join(HERE, "pc_worker.py")
MANI_DIR = os.getenv("TG_MANIFEST_DIR", os.path.join(HERE, "_manifests"))
_store_ch = (os.getenv("TG_STORE_CH", "").split(",")[0] or "").strip()
STORE_CH = int(_store_ch) if _store_ch.isdigit() else 0
# 2026-11-14（E089）：原来写死只读 TG_SESSION_STRING / _2 两个账号。
# 加了第 3、第 4 个账号后，slot3/slot4 传 --session-slot 会直接被挡：
#   "--session-slot 超出可用会话数: 2"
# 于是 slot4（Carlis）每集都上传失败重试两次后放弃，账号白加。
# 改成动态扫描所有 TG_SESSION_STRING[_N]，按 N 排序，不能写死。
SESSIONS = []
_extra = []
for _k, _v in os.environ.items():
    if not _k.startswith("TG_SESSION_STRING"):
        continue
    _v = (_v or "").strip()
    if not _v:
        continue
    _suffix = _k[len("TG_SESSION_STRING"):]
    if _suffix.startswith("_") and _suffix[1:].isdigit():
        _idx = int(_suffix[1:])
    elif not _suffix:
        _idx = 1
    else:
        continue                      # 形如 TG_SESSION_STRING_X 的噪声键，跳过
    _extra.append((_idx, _v))
for _idx, _v in sorted(_extra):
    if _v not in SESSIONS:
        SESSIONS.append(_v)


def human(n):
    return "%.2f GB" % (n / 1e9) if n >= 1e9 else "%.1f MB" % (n / 1e6)


def auto_worker_timeout(chunk_bytes, mbps, floor=1800, cap=2700,
                        safety=1.8, overhead=180, min_rate=0.05):
    """E167：按「区段字节 ÷ 实测单 worker 吞吐」推算墙钟预算，而不是写死 900s。

    事故（2026-11-17）：TG 侧把单号压到 0.15–0.18 MB/s 时，一个 128MiB 区段
    物理上要 745–900s，任何抖动都越线 → 8 个区段同一秒全部 `worker 超时（900s）`、
    manifest `parts=[]`、整轮 30 分钟产出 0（当日 upload_failed 26 次）。
    固定超时把「慢但活着」和「僵死」判成同一件事，这是判据错误。

    - 无实测（mbps<=0）→ 返回 floor=1800（=128MiB @ 0.075MB/s，
      覆盖「池子值是乐观的旧样本」的情况；有测量才有资格放宽）
    - 有实测 → est×safety+overhead，夹在 [floor, cap]
    - cap 默认 2700 > worker 自己的 TG_CHUNK_DEADLINE(2400)：
      真正的僵死由 worker 的 part 级 90s×6 兜底先退出，父进程只兜「慢」。
    """
    try:
        r = float(mbps or 0)
    except (TypeError, ValueError):
        r = 0.0
    if r <= 0:
        return int(floor)
    est = (float(chunk_bytes) / 1e6) / max(float(min_rate), r)
    return int(min(cap, max(floor, est * safety + overhead)))


def feasible_weights(slots, weights, chunk_bytes, mbps_by_slot,
                     floor_timeout=1800, safety=0.9, min_rate_floor=0.0):
    """E173：把「一个区段在 floor 预算内物理上跑不完」的会话权重压到 0。

    事故（2026-11-18，E171 的源头）：池里四个号实测 0.06 / 2.62 / 0.96 / 0.21 MB/s，
    平滑加权轮询仍按 0.06 的权重把 5GB 级文件的分片派给 0.06 那个号 ——
    128MiB 区段要 2237s，任何抖动都越过 2700s 上限，**一个慢号就能让整轮失败**
    （`ZERO_PROGRESS 0 个分片落地`）。E171 只把失败代价砍半，这里才是不让它发生。

    判据：`chunk_bytes / mbps > floor_timeout × safety` → 该号跑不完一个区段，权重 0。
    全部不可行时**保留最快的那个**（宁可慢，不许停摆）。

    返回 (new_weights, excluded_slot_list)；无实测数据的槽位保持原权重。
    """
    try:
        n = len(weights)
    except TypeError:
        return list(weights or []), []
    slots = list(slots or [])
    w = [float(x) for x in (weights or [])]
    if not w:
        return [], []
    mbps = {}
    for s, v in (mbps_by_slot or {}).items():
        try:
            mbps[int(s)] = float(v or 0)
        except (TypeError, ValueError):
            continue
    budget = float(floor_timeout) * float(safety)
    excl = []
    for i in range(n):
        sl = slots[i] if i < len(slots) else None
        try:
            m = mbps.get(int(sl), 0.0) if sl is not None else 0.0
        except (TypeError, ValueError):
            m = 0.0
        if m <= 0 or m <= float(min_rate_floor or 0):
            continue                      # 没有实测/低于硬地板：不据此排除
        if (float(chunk_bytes) / 1e6) / m > budget:
            excl.append(sl)
            w[i] = 0.0
    if excl and not any(x > 0 for x in w):
        # 全班都跑不完：留下「最快的那个」，让上层按超时/退避去处理，绝不停摆
        best = max(range(n), key=lambda i: (mbps.get(int(slots[i]), 0.0)
                                            if i < len(slots) else 0.0, -i))
        w = [0.0] * n
        w[best] = max(0.01, float(weights[best]))
        excl = [s for j, s in enumerate(slots[:n]) if j != best]
    return w, excl


def pick_worker_timeout(explicit, slot, mbps_by_slot, chunk_bytes):
    """决定某个区段实际用的超时：显式正数优先，否则按该账号实测吞吐推算。

    单点函数，专为「槽位 → 吞吐 → 预算」这条映射加测试（下标映射错一次就会
    把快号的预算套到慢号上，症状和没改一样）。
    """
    try:
        if explicit and int(explicit) > 0:
            return int(explicit)
    except (TypeError, ValueError):
        pass
    try:
        mb = float((mbps_by_slot or {}).get(int(slot or 0), 0) or 0)
    except (TypeError, ValueError):
        mb = 0.0
    return auto_worker_timeout(chunk_bytes, mb)


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            b = f.read(8 * 1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def run_region_once(src, offset, length, name, session=None, channel=0,
                    timeout=900):
    env = os.environ.copy()
    if session:
        env["TG_SESSION_STRING"] = session
        env.pop("TG_SESSION_FILE", None)
    if channel:
        env["TG_STORE_CH"] = str(channel)
    _t0 = time.time()
    try:
        r = subprocess.run([sys.executable, WORKER, "--src", src,
                            "--offset", str(offset), "--length", str(length),
                            "--name", name], capture_output=True, text=True,
                           encoding="utf-8", errors="replace", env=env,
                           timeout=timeout)
    except subprocess.TimeoutExpired as e:
        # E167：超时也要把 worker 已经说出的话带出来。
        # 旧写法只报「worker 超时（900s）」，把「part N RPC 超时」这种
        # 能区分「僵死」与「慢」的证据全丢在变量里，排查时只能猜。
        _tail = ""
        try:
            _raw = e.stderr or b""
            if isinstance(_raw, bytes):
                _raw = _raw.decode("utf-8", "replace")
            _tail = (_raw or "").strip().splitlines()[-1][:200] if _raw else ""
        except Exception:                                  # noqa: BLE001
            _tail = ""
        raise RuntimeError("worker 超时（%ss）%s" % (timeout, (" | " + _tail) if _tail else ""))
    # 2026-11-14（E099）：返回 worker 的**真实耗时**，供账号池算吞吐。
    # 不能用「提交到完成」的时间：8 个 worker 并发时，后提交的区段
    # 一直在排队，134MB / 1300s ≈ 0.1 MB/s，把 7.57 MB/s 的通道
    # 记成 0.1 —— 池子据此把所有号都判成慢号，权重全被压低。
    for line in (r.stdout or "").splitlines():
        if line.startswith("MSG="):
            return int(line[4:].strip()), time.time() - _t0
    raise RuntimeError("worker 未返回 MSG (rc=%s): %s" % (r.returncode, (r.stderr or "")[-300:]))


FLOOD_REST_MAX = int(os.environ.get("PC_FLOOD_REST_MAX", "1200"))  # 20 分钟以内就地休息


class QuotaRest(RuntimeError):
    """撞到 TG 额度且等待时间过长 → 本轮放弃，交给上层稍后重试。"""


def _flood_seconds(msg):
    """从 Telethon 的 FloodWait 文本里抠出等待秒数（A wait of N seconds / FLOOD_WAIT_N）。"""
    if not msg:
        return 0
    m = re.search(r"(?:wait of|FLOOD_WAIT[ _]?)\s*(\d+)", msg, re.I)
    return int(m.group(1)) if m else 0


def run_region(src, offset, length, name, session=None, channel=0, timeout=900,
               attempts=2):
    """单个区段失败（含僵死连接、FloodWait、RPC 错误）时换新连接重试。

    2026-11-16（用户口径）：**跑到额度就休息，不要降速硬撑。**
    Telegram 的 FloodWait 自带等待秒数，短的就地睡够再继续（这就是"休息"），
    长于 FLOOD_REST_MAX 就抛 QuotaRest 让本轮干净退出，下一轮再来，
    避免占着 slot 睡几小时。
    """
    last = None
    for i in range(1, max(1, attempts) + 1):
        try:
            return run_region_once(src, offset, length, name, session, channel,
                                   timeout)
        except Exception as e:  # noqa: BLE001
            last = e
            _msg = str(e)[:300]
            print("  ! 区段 %s 第 %d 次失败: %s" % (name, i, _msg[:160]),
                  file=sys.stderr, flush=True)
            _fs = _flood_seconds(_msg)
            if _fs:
                if _fs <= FLOOD_REST_MAX:
                    print("  撞到额度，休息 %ds 后继续（不降速）" % (_fs + 5),
                          flush=True)
                    time.sleep(_fs + 5)
                    continue
                raise QuotaRest("额度需等待 %ds（> %ds），本轮休息" % (_fs, FLOOD_REST_MAX))
            # E171（2026-11-18 巡检）：**超时不是瞬时错误，重试同一个区段没有意义。**
            # 事故：slot3 lane 从 11-17 17:24 起每小时一次 UPLOAD failed，全部是
            # 「4 个区段 × 2 轮 × 1800s 超时、零字节落地」，每次白烧一小时 + 一整集源站配额。
            # 1800s 预算已经由 auto_worker_timeout 按实测吞吐算过（见 E167）：
            # 再给同样的 1800s 只会同样超时，差别只是把这一小时翻倍。
            # 超时 → 立刻交回上层（gha_upload rc=6 → run_one E164 跳过第二次 attempt），
            # 让 lane 尽早去领别的活 / 别的账号。要恢复旧行为：TG_RETRY_ON_TIMEOUT=1
            if "worker 超时" in _msg and os.getenv("TG_RETRY_ON_TIMEOUT", "0") != "1":
                print("  ! 区段 %s 超时不重试（预算耗尽，重试同样超时）" % name,
                      file=sys.stderr, flush=True)
                raise
            if i < attempts:
                time.sleep(5)
    raise last


SLOW_MBPS = 2.0          # 低于此视为「通道被限速」
RECENT_FAST_MBPS = 3.0   # 近期样本里有人这么快，才值得开试探轮验证
PROBE_MAX_JOBS = 2       # 试探轮并发上限（成本封顶）
PROBE_MAX_TIMEOUT = 480  # 试探轮单段超时上限（秒）


PROBE_STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "config", ".probe_state.json")
PROBE_INTERVAL_SEC = 6 * 3600   # 无论证据如何，每 6h 放一次试探（TG 日限速会随跨日恢复）


def _last_probe_ts():
    try:
        with open(PROBE_STATE, encoding="utf-8") as fh:
            return float(json.load(fh).get("last_probe_ts") or 0)
    except Exception:                                      # noqa: BLE001
        return 0.0


def _mark_probe_ts(ts=None):
    try:
        os.makedirs(os.path.dirname(PROBE_STATE), exist_ok=True)
        tmp = PROBE_STATE + ".tmp.%d" % os.getpid()
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"last_probe_ts": float(ts or time.time())}, fh)
        os.replace(tmp, PROBE_STATE)
    except Exception:                                      # noqa: BLE001
        pass


def _adapt_speed_plan(rows, jobs, chunk_mib, timeout, force_probe=False,
                      rand=None, now=None, last_probe_ts=0.0):
    """按账号池实测吞吐决定本轮的并发 / 分片 / 超时。纯函数，可单测。

    演进脉络（别再回退）：
      E092：TG 限速时 8 worker 全卡在 900s 超时边缘 → 整条一起崩 → 要降并发。
      E094：池子空时 _fastest=0，条件不成立 → 自适应永不触发 → 冷启动裸奔。
      E139：降档会自我强化（2.0 这个数本身就是降档的产物）→ 加试探轮。
      E147：**试探轮不能只看概率，要看证据**。全池最快都不到 2.5 MB/s 时，
            试探等于用最贵的参数撞墙；而且试探轮本身不限并发，
            一次失败就是一个完整任务的时间。现在：
              · 只有在 `last_mbps` 里见过 >= RECENT_FAST_MBPS 的样本才试探
                （说明「通道曾经快」，2.0 可能是旧降档的残留）；
              · 试探轮 jobs<=2、timeout<=480s，失败代价封顶；
              · PC_FORCE_PROBE=1 人工对照实验仍然强制试探（但也受成本上限）。

    返回 {jobs, chunk_mib, timeout, mode, notes}。
    """
    import random as _random
    rows = rows or []
    live = [r for r in rows if not r.get("cooling")]
    mbs = [float(r.get("mbps") or 0) for r in live if (r.get("mbps") or 0) > 0]
    recent = [float(r.get("last_mbps") or 0) for r in live]
    fastest = max(mbs) if mbs else 0.0
    recent_fast = max(recent) if recent else 0.0
    notes = []

    plan = {"jobs": jobs, "chunk_mib": chunk_mib, "timeout": timeout,
            "mode": "normal", "fastest": fastest, "recent_fast": recent_fast}

    slow = False
    if not mbs:
        fails = sum(int(r.get("fails") or 0) for r in live)
        if fails > 0:
            fastest = 1.0
            slow = True
            plan["fastest"] = fastest
            notes.append("  无实测数据但已有 %d 次失败，按保守基线降档" % fails)
        else:
            notes.append("  池内无实测数据，本轮按原参数跑（跑完就有基线了）")
            plan["notes"] = notes
            return plan
    else:
        slow = fastest < SLOW_MBPS

    if not slow:
        plan["notes"] = notes
        return plan

    # --- 降档证据成立：先判断这次该「试探」还是「直接降」 ---
    draw = rand if rand is not None else _random.random()
    _now = float(now if now is not None else time.time())
    _stale = (_now - float(last_probe_ts or 0)) >= PROBE_INTERVAL_SEC
    probe = False
    if force_probe:
        probe = True
    elif recent_fast >= RECENT_FAST_MBPS and draw < 0.25:
        probe = True
    elif _stale:
        # 兜底出口：每 6h 至少验证一次。E139 的教训是「判据不能把自己当证据」，
        # 只靠 recent_fast 会重演同一个锁死（降档 → 永远测不到快 → 永不试探）。
        # TG 的日流量限制随跨日恢复，所以必须留一条与历史样本无关的出口。
        probe = True

    if probe:
        _oj, _ot = jobs, timeout
        jobs = min(jobs, PROBE_MAX_JOBS)
        timeout = min(timeout, PROBE_MAX_TIMEOUT)
        plan.update(jobs=jobs, timeout=timeout, mode="probe")
        notes.append("  降档试探轮：jobs %d→%d, timeout %d→%ds"
                     "（近期最快 %.2f MB/s 支持一次验证；成本已封顶）"
                     % (_oj, jobs, _ot, timeout, recent_fast))
        plan["notes"] = notes
        return plan

    # --- 真降档：降并发 + 切小分片 + 按实测算超时 ---
    _oj, _ot, _oc = jobs, timeout, chunk_mib
    jobs = min(jobs, 2)
    chunk_mib = min(chunk_mib, 32)
    per = max(0.01, fastest / max(1, jobs))
    est = chunk_mib / per
    timeout = max(timeout, int(est * 3) + 300)
    plan.update(jobs=jobs, chunk_mib=chunk_mib, timeout=timeout, mode="degraded")
    notes.append("  限速自适应：jobs %d→%d, 分片 %d→%dMiB, timeout %d→%ds"
                 "（池内最快 %.2f MB/s，单 worker 约 %.3f MB/s）"
                 % (_oj, jobs, _oc, chunk_mib, _ot, timeout, fastest,
                    fastest / max(1, jobs)))
    plan["notes"] = notes
    return plan



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("--name", default=None)
    ap.add_argument("--chunk-mib", type=int, default=512)
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--store-ch", type=int, default=0,
                    help="本次分片写入的 TG 存储频道；缺省读 TG_STORE_CH")
    ap.add_argument("--session-slot", type=int, default=0,
                    help="只使用第 N 个 TG 会话（1/2）；0=所有会话按分片轮换")
    ap.add_argument("--worker-timeout", type=int, default=0,
                    help="单个区段 worker 的最长等待秒数；0=按账号实测吞吐自动推算（E167）")
    ap.add_argument("--no-md5", action="store_true",
                    help="续传时本地文件可能是稀疏文件，不计算全文件 MD5")
    a = ap.parse_args()

    # 2026-11-16（E147）：自适应判据抽成纯函数（可单测），并修正 E139 的两处缺陷。
    #   ① 试探轮必须有「近期真的快过」的证据才开 —— 否则全池都慢时，25% 的任务
    #      拿满并发 8 去撞墙，一次赔 30~60 分钟，还把 claim 释放回队首让下一轮
    #      再领同一集（实测 现在就出发 S03E09 在 05:35/06:40/07:45/08:49 反复重来）。
    #   ② 试探轮自身必须有成本上限（jobs<=2、timeout<=480s），跑不完也是小钱。
    # 2026-11-16（用户决定）：**不用限速上片，跑满速；撞到额度就休息。**
    # 自适应降档默认关闭（PC_ADAPTIVE=1 才启用），因为它会自我强化：
    # 降档 → 吞吐更低 → 下一轮判据更成立 → 永远压在 2MB/s。
    # 现在按配置的 jobs/chunk 全速跑，Telegram 给的 FloodWait 就是休息信号。
    _ADAPTIVE = os.environ.get("PC_ADAPTIVE") == "1"
    if not _ADAPTIVE:
        print("  满速模式：jobs=%d 分片=%dMiB（未启用限速自适应，PC_ADAPTIVE=1 可开）"
              % (a.jobs, a.chunk_mib), flush=True)
        _plan = {"jobs": a.jobs, "chunk_mib": a.chunk_mib,
                 "timeout": a.worker_timeout, "mode": "full", "notes": []}
    else:
        _plan = _adapt_speed_plan(
            TP.snapshot(), a.jobs, a.chunk_mib, a.worker_timeout,
            force_probe=(os.environ.get("PC_FORCE_PROBE") == "1"),
            now=time.time(), last_probe_ts=_last_probe_ts())
    a.jobs = _plan["jobs"]
    a.chunk_mib = _plan["chunk_mib"]
    a.worker_timeout = _plan["timeout"]
    if _plan["mode"] == "probe":
        _mark_probe_ts()
    for _line in _plan["notes"]:
        print(_line, flush=True)

    src = os.path.abspath(a.src)
    if not os.path.exists(src):
        raise SystemExit("源文件不存在: %s" % src)
    store_ch = int(a.store_ch or STORE_CH)
    if not store_ch:
        raise SystemExit("缺少存储频道: --store-ch 或 TG_STORE_CH")
    active_sessions = list(SESSIONS)
    if a.session_slot:
        if a.session_slot < 1 or a.session_slot > len(SESSIONS):
            raise SystemExit("--session-slot 超出可用会话数: %d" % len(SESSIONS))
        active_sessions = [SESSIONS[a.session_slot - 1]]
    total = os.path.getsize(src)
    display = a.name or os.path.basename(src)
    base = os.path.splitext(os.path.basename(src))[0]
    chunk = a.chunk_mib * 1024 * 1024
    nreg = (total + chunk - 1) // chunk

    os.makedirs(MANI_DIR, exist_ok=True)
    mani_path = os.path.join(MANI_DIR, base + ".json")
    mime = "video/x-matroska" if src.lower().endswith((".mkv", ".webm")) else "video/mp4"
    mani = {"name": display, "src": src, "total_size": total, "channel_id": store_ch,
            "chunk_bytes": chunk, "mime_type": mime,
            "parts": [], "src_md5_full": None}
    if os.path.exists(mani_path):
        try:
            old = json.load(open(mani_path, encoding="utf-8"))
            old_chunk = int(old.get("chunk_bytes") or 0)
            if old.get("total_size") == total and old_chunk == chunk:
                mani = old
                if not mani.get("parts"):
                    mani["channel_id"] = store_ch
            elif old.get("total_size") == total and old_chunk != chunk:
                if old.get("parts"):
                    # 2026-11-14（E098）：**拒绝覆盖是死路，不是保护。**
                    # 这个任务会永久卡在这里：每次 attempt 都在同一行退出，
                    # 既不上传也不失败，claim 一直占着 slot（实测整条 lane 空转）。
                    # 触发场景很真实：自适应把分片从 128 降到 32，留下一份
                    # 32MiB 清单；下次按 128 跑就再也过不去。
                    #
                    # 正确处理：分片大小变了 → 旧分片全部作废，重头传。
                    # 上界代价是重传，但不重传就是永久卡死，重传明显更优。
                    # 先把旧清单改名留证（便于排查），再清空分片记录。
                    try:
                        _bak = "%s.chunk%d.bak" % (mani_path, int(old_chunk or 0))
                        os.replace(mani_path, _bak)
                        print("  分片大小 %s→%s，旧清单已改名保留：%s"
                              % (old_chunk, chunk, os.path.basename(_bak)),
                              flush=True)
                    except OSError:
                        pass
                    # 注意：不能只留 {"parts": []} —— name/channel_id/
                    # total_size/mime_type 后面都要用，丢了会写出残废清单。
                    mani = {"name": display, "src": src, "total_size": total,
                            "channel_id": store_ch, "chunk_bytes": chunk,
                            "mime_type": mime, "parts": [],
                            "src_md5_full": None}
                    print("  旧分片作废，从头上传（避免永久卡死）", flush=True)
                else:
                    print("  忽略旧清单：分片大小已从 %s 调整为 %s"
                          % (old_chunk, chunk), flush=True)
        except (OSError, ValueError):
            pass
    parts = {p["part_no"]: p for p in mani.get("parts", [])}
    for part in parts.values():
        part.setdefault("channel_id", int(mani.get("channel_id") or store_ch))

    def save():
        mani["parts"] = [parts[k] for k in sorted(parts)]
        tmp = mani_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(mani, f, ensure_ascii=False, indent=1)
        os.replace(tmp, mani_path)

    print("== %s  %s  区段 %d (每段 %dMiB, 并发 %d 进程)  已完成 %d ==" % (
        display, human(total), nreg, a.chunk_mib, a.jobs, len(parts)), flush=True)
    if not mani.get("src_md5_full") and not a.no_md5 and not mani.get("resume_sparse"):
        print("== 计算源 MD5 ==", flush=True)
        mani["src_md5_full"] = md5_file(src)
        save()
    print("   md5=%s" % (mani.get("src_md5_full") or "skipped"), flush=True)

    todo = []
    for c in range(nreg):
        idx = c + 1
        if idx in parts:
            print("  区段 %d/%d 已存在(msg=%d) 跳过" % (idx, nreg, parts[idx]["message_id"]), flush=True)
            continue
        off = c * chunk
        todo.append((idx, off, min(chunk, total - off)))

    t0 = time.time()
    done_bytes = [0]

    # 2026-11-14（E090）：权重不再写死 1,1,1,1，改成**账号池实测权重**。
    # 事故背景：TG 把上传压到 0.41 MB/s 而本机出口 100 MB/s，慢号和快号
    # 平分片 → 快号早早干完、慢号拖成长尾 → 900s worker 超时 → 整条失败。
    # 池子按 EWMA 吞吐给每个号算权重，慢号自动降权、连败进冷却、到期回池。
    # TG_SESSION_WEIGHTS 仍可手工覆盖（调试用），但不作为默认。
    # 2026-11-14（E096）：**多会话模式下 session_no 必须映射回真实槽位。**
    # gha_upload 在多会话模式（TG_UPLOAD_MULTI_SESSION=1）下**不传**
    # --session-slot，于是 pc_upload 里 session_no = si + 1（数组下标+1）。
    # 但数组下标 ≠ 账号槽位：slot3 被 tgpipe 预留、真实可用是 1/2/4，
    # 于是所有回填都记到池子的 slot1 上 —— 池子学错了对象，
    # 把"最快的那个号"的成绩记在 slot1 名下，权重完全失真。
    # 这里建一张 下标 → 真实 slot 的映射表，回填时用它。
    _slot_of_idx = []
    _disc = []
    try:
        _disc = TP.discover_sessions()
    except Exception:                                      # noqa: BLE001
        _disc = []
    _all_slots = [d["slot"] for d in _disc]
    if a.session_slot:
        _slot_of_idx = [a.session_slot]
    elif _pool_w:
        _slot_of_idx = sorted(_pool_w.keys())
    elif _all_slots:
        _slot_of_idx = _all_slots
    # 长度不匹配就退回 si+1，并在日志里喊一声，不静默错记
    if len(_slot_of_idx) != len(active_sessions):
        print("  ! 槽位映射长度不符: sessions=%d slots=%d，回填将退化为下标"
              % (len(active_sessions), len(_slot_of_idx)), flush=True)
        _slot_of_idx = list(range(1, len(active_sessions) + 1))

    weights = []
    _slot_ids = []            # E173：weights 每一项对应的真实账号槽位（下标≠槽位）
    _pool_w = {}
    if active_sessions:
        try:
            _pool_w = TP.weights()
        except Exception:                                  # noqa: BLE001
            _pool_w = {}
        raw = os.getenv("TG_SESSION_WEIGHTS", "").strip()
        if raw:
            try:
                parsed = [max(0.01, float(x)) for x in
                          raw.replace(":", ",").split(",") if x.strip()]
            except ValueError:
                parsed = []
            if len(parsed) == len(active_sessions):
                weights = parsed
        if not weights and _pool_w:
            _slots = ([a.session_slot] if a.session_slot
                      else sorted(_pool_w.keys()))
            if len(_slots) == len(active_sessions):
                weights = [max(0.01, float(_pool_w.get(sl, 1.0))) for sl in _slots]
                _slot_ids = list(_slots)
        if not weights:
            weights = [1.0] * len(active_sessions)
        # E173：把「一个区段在该号预算内物理上跑不完」的会话权重压到 0。
        # 平滑加权轮询只看相对权重，0.06 的号照样会分到 1/17 的分片；
        # 而它传 128MiB 要 2237s → 一个慢号就能让整轮 ZERO_PROGRESS（E171 的源头）。
        if os.getenv("TG_SESSION_FEASIBILITY", "1") == "1" and _slot_ids:
            try:
                _cw = max(1, int(a.chunk_mib)) * 1024 * 1024
                weights, _excl = feasible_weights(_slot_ids, weights, _cw, _pool_w)
                if _excl:
                    print("  排除不可行会话 slot=%s（%dMiB 区段在该号 1800s 预算内跑不完）"
                          % (",".join(str(x) for x in _excl), int(a.chunk_mib)), flush=True)
            except Exception as _fe:                       # noqa: BLE001
                # 不许静默：但也不许因为筛除失败就不上传
                print("  ! 会话可行性筛除失败（按原权重继续）: %s: %s"
                      % (type(_fe).__name__, str(_fe)[:80]), flush=True)
        print("  池权重=%s" % ",".join("%.2f" % x for x in weights), flush=True)

    # 每个会话保持独立的小 worker 池；总 worker 数仍由 --jobs 控制。
    session_jobs = [a.jobs]
    if active_sessions:
        n = len(active_sessions)
        per_session, extra = divmod(a.jobs, n)
        session_jobs = [per_session] * n
        for i in range(extra):
            session_jobs[n - 1 - i] += 1

    # 分片按会话实测带宽加权，不再简单轮流分配。这样快会话能分到更多
    # 区段，慢会话也不会在快会话结束后独自形成长时间尾巴。
    part_sessions = []
    if active_sessions:
        current = [0.0] * len(active_sessions)
        total_weight = sum(weights)
        for _ in range(len(todo)):
            for i in range(len(current)):
                current[i] += weights[i]
            pick = max(range(len(current)), key=lambda i: (current[i], -i))
            part_sessions.append(pick)
            current[pick] -= total_weight
        counts = [part_sessions.count(i) for i in range(len(active_sessions))]
        print("  会话权重=%s worker=%s 区段=%s" % (
            ",".join("%.2f" % x for x in weights), session_jobs, counts), flush=True)

    if active_sessions:
        executors = [ThreadPoolExecutor(max_workers=max(1, session_jobs[i]))
                     for i in range(len(active_sessions))]
    else:
        executors = [ThreadPoolExecutor(max_workers=a.jobs)]

    futs = {}
    fu_t0 = {}
    _mbps_by_slot = {}
    try:
        for _r in TP.snapshot():
            _mbps_by_slot[int(_r.get("slot") or 0)] = float(_r.get("mbps") or 0)
    except Exception:                                      # noqa: BLE001
        _mbps_by_slot = {}
    _wt_seen = {}
    for pos, (idx, off, length) in enumerate(todo):
        nm = "%s.part%02d" % (base, idx)
        si = 0
        if active_sessions:
            si = part_sessions[pos]
            session = active_sessions[si]
            ex = executors[si]
            session_no = a.session_slot or (si + 1)
        else:
            session = None
            ex = executors[0]
            session_no = a.session_slot or 0
        # E167：墙钟预算按「该区段字节 ÷ 该账号实测单 worker 吞吐」推算。
        # 写死 900s 时，TG 慢到 0.15MB/s 就等于「物理上不可能完成」——
        # 8 个区段整批超时、manifest 一片没落地，整轮产出 0。
        if a.worker_timeout and a.worker_timeout > 0:
            _wt = a.worker_timeout
        else:
            _sl = (_slot_of_idx[si] if _slot_of_idx and si < len(_slot_of_idx)
                   else session_no)
            _mb = _mbps_by_slot.get(int(_sl or 0), 0.0)
            _wt = pick_worker_timeout(0, _sl, _mbps_by_slot, length)
            _wt_seen[(_sl, _mb)] = _wt
        _fu = ex.submit(run_region, src, off, length, nm, session, store_ch,
                        _wt)
        fu_t0[_fu] = time.time()
        futs[_fu] = (idx, length, session_no)
    if _wt_seen:
        print("  区段超时=auto（实测吞吐→预算）：%s" % "，".join(
            "slot%s %.2fMB/s→%ds" % (k[0], k[1], v)
            for k, v in sorted(_wt_seen.items(), key=lambda x: str(x[0][0]))),
            flush=True)
    elif a.worker_timeout and a.worker_timeout > 0:
        print("  区段超时=固定 %ds（TG_WORKER_TIMEOUT 显式覆盖）"
              % a.worker_timeout, flush=True)
    try:
        for fu in as_completed(futs):
            idx, length, session_no = futs[fu]
            # run_region 现在返回 (msg, 耗时)；兼容旧签名
            _res = fu.result()
            if isinstance(_res, tuple):
                msg, _dur = _res
            else:
                msg, _dur = _res, time.time() - fu_t0.get(fu, time.time())
            parts[idx] = {"part_no": idx, "message_id": msg,
                          "channel_id": store_ch,
                          "plain_size": length, "stored_size": length}
            done_bytes[0] += length
            save()
            el = time.time() - t0
            print("  OK 区段 %d/%d -> msg=%d  %s  session=%s  累计 %.2f MB/s" % (
                idx, nreg, msg, human(length), session_no or "-",
                done_bytes[0] / el / 1e6), flush=True)
            # 2026-11-14（E090）：把这一片的**实测吞吐**回填账号池。
            # 没有这一步，池子只能记住「谁快」却不能「谁刚刚变快/变慢」，
            # 权重就永远停在初始值，等于没做池化。
            try:
                # fu_t0 是提交时间，含排队；用 worker 自己的耗时才准（E099）
                _mb = (length / 1e6) / max(0.001, _dur)
                # session_no 是 1 起的数组下标；映射回真实账号槽位再记账，
                # 否则多会话模式下全部记到 slot1（E096）。
                _idx0 = int(session_no or 1)
                if 1 <= _idx0 <= len(_slot_of_idx):
                    _sn = _slot_of_idx[_idx0 - 1]
                else:
                    _sn = _idx0
                TP.report(_sn, True, _mb)
            except Exception as _pe:                       # noqa: BLE001
                # 别静默：池子回填失败等于池化失效，必须看得见
                print("  ! 池回填失败 slot=%r: %s: %s"
                      % (session_no, type(_pe).__name__, str(_pe)[:80]), flush=True)
    except BaseException:
        # Do not wait for the other 7 workers after one part fails.  They may
        # be stuck in long Telegram retries; waiting here hides the traceback
        # for hours and leaves the whole job unable to move to its retry.
        try:
            save()
        except Exception:
            pass
        # 2026-11-14（E090）：失败也要回填池子，否则「谁在失败」永远学不到，
        # 连败计数无法累积，坏号永远不会被冷却。
        try:
            for _fu2, (_i2, _l2, _s2) in futs.items():
                if _fu2.done() and _fu2.exception() is not None:
                    _i2w = int(_s2 or 1)
                    _sn2 = (_slot_of_idx[_i2w - 1]
                            if 1 <= _i2w <= len(_slot_of_idx) else _i2w)
                    TP.report(_sn2, False, None, str(_fu2.exception())[:100])
        except Exception:                              # noqa: BLE001
            pass
        traceback.print_exc()
        sys.stderr.flush()
        sys.stdout.flush()
        for ex in executors:
            ex.shutdown(wait=False, cancel_futures=True)
        os._exit(1)
    else:
        for ex in executors:
            ex.shutdown(wait=True)

    got = sum(p["plain_size"] for p in parts.values())
    mani["done"] = (len(parts) == nreg and got == total)
    save()
    el = time.time() - t0
    print("\n== 完成 %d/%d 段 合计 %s / 源 %s  %s  用时 %.1fs ==" % (
        len(parts), nreg, human(got), human(total),
        "OK" if mani["done"] else "不完整", el), flush=True)
    print("MANIFEST=%s" % mani_path)


if __name__ == "__main__":
    try:
        main()
    except QuotaRest as _e:
        # 不是错误，是"到额度了"：干净退出，让 lane 稍后再来。
        print("# 额度休息：%s" % _e, flush=True)
        sys.exit(3)
