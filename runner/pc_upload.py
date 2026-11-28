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
import argparse, hashlib, json, os, re, signal, subprocess, sys, time, traceback
from concurrent.futures import (ThreadPoolExecutor, as_completed, wait,
                                FIRST_COMPLETED)

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
# E216：worker 路径可注入（默认就是同目录的 pc_worker.py）。
# 早停闸门要在「慢号」现场验证，真跑一次 TG 上传代价太大，测试用假 worker 注入。
WORKER = os.getenv("TG_PC_WORKER") or os.path.join(HERE, "pc_worker.py")
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


def _iter_completed(futs, stall_sec=0, poll=5.0):
    """按完成顺序产出 future；连续 stall_sec 秒没有新完成 → 抛 UploadStalled。

    **不能用 `as_completed(timeout=)`**：它的 timeout 是「总时长」而不是「每次等待
    的窗口」（3.13 实测：三个 future 分别在 0.5/1.2/1.9s 完成，timeout=1 在 1.0s
    就抛 TimeoutError）。用它当僵死判据，会把所有耗时超过窗口的正常上传整单掐死。

    这里用 wait(FIRST_COMPLETED) 自己滚窗口：每拿到一个完成就重置计时，
    只有「真的 N 秒一个新完成的都没有」才判僵死。
    """
    pending = set(futs)
    last = time.time()
    while pending:
        done, pending = wait(pending, timeout=poll,
                             return_when=FIRST_COMPLETED)
        if done:
            last = time.time()
            for f in done:
                yield f
            continue
        if stall_sec and stall_sec > 0 and (time.time() - last) >= float(stall_sec):
            raise UploadStalled(
                "连续 %.0fs 没有任何区段落地（%d 个仍挂着）"
                % (time.time() - last, len(pending)))


class JobTooSlow(RuntimeError):
    """E216：整单 ETA 超过墙钟预算 —— 不是「文件坏了」，是「这个号现在跑不完」。"""


class UploadStalled(RuntimeError):
    """E221：本轮**一个区段都没落地**且已经过了「一段的物理下限时间」——僵死，不是慢。"""


def stall_window(chunk_bytes, ceiling, min_rate=0.05, floor=600):
    """E221：零进展僵死判据的墙钟窗 —— 「一个区段在最慢可活速率上传完要多久」。

    事故（2026-11-27 全天）：`as_completed(futs)` 只在**有区段落地**时才评估
    E216 的整单 ETA 闸门；一旦一个区段都没落地，那段判据（done<=0 / parts<2
    直接 return False）永远不触发，只能等每个 worker 撞满 1800s 墙钟超时 ——
    实测 12 次 `ZERO_PROGRESS 0 个分片落地`，每次白占一条 lane 30 分钟、
    连池子都学不到「这个号已经死了」（零进展不给吞吐样本）。

    判据换成「零落地」这件**可观测的硬事实**：32MiB 区段在 0.05MB/s 下也只需
    671s，超过这个时间还一段没落地，就不是「慢」而是「连接已经僵了」。
    上限取 worker 墙钟预算，下限 600s（别让起步抖动判死）。
    """
    try:
        cb = float(chunk_bytes or 0)
        cap = float(ceiling or 0)
        mr = max(0.01, float(min_rate or 0.05))
    except (TypeError, ValueError):
        return 600
    if cb <= 0:
        return max(600, int(cap or 600))
    w = (cb / 1e6) / mr
    w = max(float(floor), w)
    if cap > 0:
        w = min(cap, w)
    return int(w)


def jobs_for_rate(mbps, cap, slow=0.5, mid=1.0):
    """E229：按实测吞吐收窄并发 —— 限速号上单连接比 8 并发快 3.3 倍。

    2026-11-28 10:30 现场 A/B（slot1、同一文件、3MB × N 并发，pc_worker 直传）：

        concurrency=1  合计 0.663 MB/s
        concurrency=2  合计 0.202 MB/s
        concurrency=4  合计 0.194 MB/s
        concurrency=8  合计 0.197 MB/s

    限速的瓶颈是**按连接分配的上行额度**：多开连接不增加总量，只是把同一份
    额度切碎。而切碎的代价是致命的 —— 每个 32MiB 区段都要 1000s 以上才落地，
    僵死闸门（按整轮零落地判）必然先开火，于是「慢」被判成「死」，
    一天 58 次 ZERO_PROGRESS、每次白烧 11 分钟 + 一次源站下载。

    规则：实测 <0.5MB/s → 1 个 worker；<1.0 → 2 个；更快才用满配置的并发。
    不做「永远压低」的降档：这两个阈值远低于 E219 的健康区间（2MB/s 以上），
    快号完全不受影响；而且成功分片会立刻把 mbps 抬回真实值，自动解除收窄。
    """
    try:
        mb = float(mbps or 0)
        cp = max(1, int(cap or 1))
    except (TypeError, ValueError):
        return 1
    if mb <= 0:
        return cp
    if mb < float(slow):
        return 1
    if mb < float(mid):
        return min(cp, 2)
    return cp


def stall_window_measured(chunk_bytes, mbps, ceiling, safety=1.5, floor=600,
                          min_rate=0.05):
    """E229：有实测吞吐时，僵死窗按「这个号一段的真实耗时 × 安全系数」。

    事故（2026-11-28 08:05 告警「整单失败×61」，24h 内 58 次 ZERO_PROGRESS）：
    `stall_window` 的 0.05MB/s 是**硬地板**，而池里慢号的实测吞吐恰好就压在
    0.04–0.05（slot1 last 0.04 / slot4 0.05）。32MiB ÷ 0.05 = 671s，闸门 = 675s
    —— 一点余量都没有。RPC 建连、源文件 MD5、8 个 worker 争抢任何一项抖一下，
    第一段就落在 675s 之后 → 判僵死 → 失败 → 池冷却 30min → 复领同一集再撞一次。
    实测每次白烧 11 分钟（675s + 收尾）+ 一次源站下载，一天 58 次。

    判据还是「能不能落地」，但阈值必须用**刚刚实测到的这个号的速率**来算，并留
    1.5 倍余量：0.05MB/s → 1006s（< worker 墙钟 1800，来得及落地）；0.02MB/s →
    2516s 被墙钟截到 1800（真慢就按墙钟死，而不是按错的地板值死）。快号
    （≥0.1MB/s）仍被 600s 下限兜住 → 真僵死照样 10 分钟发现，不会变迟钝。
    """
    try:
        cb = float(chunk_bytes or 0)
        cap = float(ceiling or 0)
        mb = float(mbps or 0)
        sf = max(1.0, float(safety or 1.0))
        fl = float(floor or 600)
    except (TypeError, ValueError):
        return 600
    if cb <= 0:
        return max(600, int(cap or 600))
    if mb <= 0:
        # 没有实测：退回硬地板口径，保持 E221 的行为
        return stall_window(cb, cap)
    w = ((cb / 1e6) / max(0.01, mb)) * sf
    w = max(fl, w)
    if cap > 0:
        w = min(cap, w)
    return int(w)


def _kill_workers():
    """杀掉本进程派生、还在跑的 pc_worker（E216）。

    父进程早停/失败退出后，worker 变成孤儿进程继续往存储频道灌
    **没人登记**的区段：烧 TG 配额、污染频道、下次复领还得重新扫。
    只杀 ppid==本进程 且 cmdline 含 pc_worker.py 的，不碰别人。
    """
    me = os.getpid()
    killed = []
    try:
        pids = [p for p in os.listdir("/proc") if p.isdigit()]
    except OSError:
        return []
    for pid in pids:
        try:
            with open("/proc/%s/stat" % pid, encoding="utf-8",
                      errors="replace") as f:
                ppid = int(f.read().rsplit(")", 1)[1].split()[1])
            if ppid != me:
                continue
            with open("/proc/%s/cmdline" % pid, "rb") as f:
                if b"pc_worker.py" not in f.read():
                    continue
            os.kill(int(pid), signal.SIGKILL)
            killed.append(int(pid))
        except (OSError, ValueError, IndexError):
            continue
    if killed:
        print("  已掐掉 %d 个仍在传的 worker（不留孤儿继续烧配额）" % len(killed),
              flush=True)
    return killed


def job_eta_exceeded(remaining_bytes, done_bytes, elapsed, budget,
                     min_elapsed=300, parts_done=0, min_parts=2):
    """E216：按本轮**实测已落地字节**推整单 ETA，超预算就早停。

    事故（2026-11-26 08:08，slot4）：池内 EWMA 还留着几小时前的 1.11MB/s，
    TG 实际已把这个号压到 ~0.2MB/s 聚合。1.5GB 的文件按 1800s/区段的预算跑，
    22 个区段落地后 part19/part25 各撞一次 1800s，attempt1+attempt2 共 5609s
    才收尾，产出 0 —— 一条 lane 白占 1.5 小时，而那段时间 slot2 实测 1.7MB/s。

    判据用「已落地字节」而不是池表的 mbps：池表是上一轮的样本，TG 限速是
    分钟级的，只有本轮实测说了算。manifest 本来就能续传，所以早停不丢任何
    已完成区段 —— 交回 claim，下一轮换个号复领续传即可。

    返回 True 表示「剩下的活按当前速率跑不完预算」，调用方应早停。
    """
    try:
        rem = float(remaining_bytes)
        done = float(done_bytes)
        el = float(elapsed)
        bud = float(budget)
    except (TypeError, ValueError):
        return False
    if bud <= 0 or rem <= 0 or done <= 0 or el <= 0:
        return False
    if el < float(min_elapsed) or int(parts_done) < int(min_parts):
        return False                      # 样本太少，别拿起步抖动判死
    return (rem / (done / el)) > bud


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
    raise RuntimeError("worker 未返回 MSG (rc=%s): %s" % (r.returncode, _err_brief(r.stderr)))


_ERR_RE = re.compile(r"^[A-Za-z_][\w.]*(?:Error|Exception|Wait|Cancel\w*)\b.*$", re.M)


def _err_brief(stderr, limit=300):
    """把 worker stderr 压成「异常行 | 末行」，而不是只保留最后 300 个字符。

    2026-11-18 修：旧写法取 `stderr[-300:]`，落点在 traceback 中途
    （实测得到 `return future.result()` + `File "/opt/emby-run`），
    **看不到异常类型**，part08/part09 反复失败时无从判断是 RPC 还是本地错误。
    现在优先取最后一条形如 `XxxError: ...` 的行，再附末行。
    """
    s = (stderr or "").strip()
    if not s:
        return ""
    m = _ERR_RE.findall(s)
    last = s.splitlines()[-1].strip()
    if m:
        head = m[-1].strip()
        out = head if head == last else "%s | last: %s" % (head, last)
    else:
        out = last
    return out[-limit:]


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
    # 2026-11-27（E219）：分片按「该号实测吞吐」自动选，不再全局 32MiB。
    # 32MiB 是为慢号（0.2MB/s × 32MB ≈ 160s < RPC 240s）设的下限，
    # 但快号（5MB/s）拿 32MiB 会把 1.4GB 拆成 43 条消息 —— 消息越多越容易撞
    # FloodWait/part 失败，近 48h 88 次 UPLOAD_FAILED 里大部分是这个形状。
    # 目标：每片约 180 秒传完，夹在 [32, 512] MiB。
    if os.environ.get("TG_CHUNK_AUTO", "0") == "1":
        try:
            _slot_no = (SESSIONS.index(active_sessions[0]) + 1) if active_sessions else 0
            _mbps = 0.0
            for _r in TP.snapshot():
                if int(_r.get("slot") or 0) == _slot_no:
                    _mbps = float(_r.get("mbps") or 0)
                    break
            if _mbps > 0:
                # 2026-11-28（E230）：实测四个号今天只有 0.04-0.7MB/s。
                # 32MiB 下限在 0.04MB/s 上要 800s，直接撞 600s 僵死闸门 →
                # ZERO_PROGRESS 0 个分片落地（近 24h 68 次，占失败 85%）。
                # 改成「按 120 秒目标 + 8MiB 下限」：0.04MB/s → 8MiB(200s)、
                # 0.7MB/s → 84MiB。慢号也能在闸门内落地第一个分片。
                _want = max(8, min(512, int(_mbps * 120)))
                if _want != a.chunk_mib:
                    print("  分片自适应：slot%d %.2fMB/s → %dMiB（原 %dMiB）"
                          % (_slot_no, _mbps, _want, a.chunk_mib), flush=True)
                a.chunk_mib = _want
        except Exception:                                    # noqa: BLE001
            pass
    chunk = a.chunk_mib * 1024 * 1024
    # E229：并发按该号实测吞吐收窄（见 jobs_for_rate 的现场 A/B）。
    try:
        _jr_rate = 0.0
        if a.session_slot:
            for _r in TP.snapshot():
                if int(_r.get("slot") or 0) == int(a.session_slot):
                    _jr_rate = float(_r.get("mbps") or 0)
                    break
        _jr = jobs_for_rate(_jr_rate, a.jobs)
        if _jr != a.jobs:
            print("  并发按实测收窄：slot%s %.2fMB/s → jobs %d→%d"
                  "（限速号单连接最快，见 E229）"
                  % (a.session_slot or "-", _jr_rate, a.jobs, _jr), flush=True)
            a.jobs = _jr
    except Exception:                                        # noqa: BLE001
        pass
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
    _pool_w = {}          # 必须先初始化：多会话模式（--session-slot 0）走 `elif _pool_w:`
                          # 时原代码会 UnboundLocalError（E216 顺带修掉）
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
    # E216：整单墙钟预算，与 gha_next3.rate_limited_max_gb 同一口径（默认 6000s）。
    # 0 = 关闭早停（只跑不判）。
    try:
        _eta_budget = int(os.getenv("EMBYTG_UPLOAD_ETA_BUDGET_SEC",
                                    os.getenv("EMBYTG_LANE_UPLOAD_BUDGET_SEC",
                                              "6000")) or 0)
    except ValueError:
        _eta_budget = 6000
    _eta_parts = 0            # 本轮（不含历史 manifest）落地的区段数
    # 样本门槛：默认「至少 300s 且至少 2 段」才判；测试/调试可用环境变量收紧。
    try:
        _eta_min_el = float(os.getenv("EMBYTG_UPLOAD_ETA_MIN_ELAPSED", "300"))
        _eta_min_parts = int(os.getenv("EMBYTG_UPLOAD_ETA_MIN_PARTS", "2"))
    except ValueError:
        _eta_min_el, _eta_min_parts = 300.0, 2
    # E221：零进展僵死窗。_stall<=0 表示关闭；未设 = 自动按「一段在最慢可活速率
    # 上传完的时间」推算（见 stall_window）。
    _stall_env = os.getenv("EMBYTG_UPLOAD_STALL_SEC")
    if _stall_env is None:
        _wt_ceil = a.worker_timeout if (a.worker_timeout and a.worker_timeout > 0) \
            else (max(_wt_seen.values()) if _wt_seen else 1800)
        # E229：优先用**这个号刚实测到的吞吐**算闸门。E221 的 0.05MB/s 只是硬地板，
        # 而慢号的实测值本来就贴着 0.04–0.05 —— 拿地板值当阈值等于「按体检报告
        # 判死刑」：671s 的理论值与 675s 的闸门只差 4 秒，任何抖动都必然被误杀。
        _stall_rate = 0.0
        try:
            if a.session_slot:
                _stall_rate = float(_mbps_by_slot.get(int(a.session_slot), 0.0) or 0.0)
            elif _mbps_by_slot:
                _stall_rate = max(float(v or 0.0) for v in _mbps_by_slot.values())
        except (TypeError, ValueError):
            _stall_rate = 0.0
        if _stall_rate > 0:
            _stall = stall_window_measured(chunk, _stall_rate, _wt_ceil)
            print("  僵死闸门口径：实测 slot%s %.2fMB/s × 1.5 → %ds"
                  % (a.session_slot or "-", _stall_rate, _stall), flush=True)
        else:
            _stall = stall_window(chunk, _wt_ceil)
    else:
        try:
            _stall = int(_stall_env or 0)
        except ValueError:
            _stall = 0
    if _stall > 0:
        print("  僵死闸门=%ds（这么久还没有任何区段落地 → 判僵死，交回 claim）"
              % _stall, flush=True)
    try:
        for fu in _iter_completed(futs, _stall):
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
            # E216：整单 ETA 闸门。判的是「剩下的活按**本轮实测**速率跑不完预算」，
            # 不是「这一片慢」：慢号不该占着 lane 磨完整个文件。manifest 里已完成
            # 的区段全部保留，交回 claim 让更快的号复领续传，一个字节都不白传。
            _eta_parts += 1
            if _eta_budget > 0:
                _el2 = time.time() - t0
                _rem = total - sum(p["plain_size"] for p in parts.values())
                if job_eta_exceeded(_rem, done_bytes[0], _el2, _eta_budget,
                                    min_elapsed=_eta_min_el,
                                    min_parts=_eta_min_parts,
                                    parts_done=_eta_parts):
                    _rate2 = done_bytes[0] / max(1.0, _el2)      # B/s
                    raise JobTooSlow(
                        "剩余 %.0fMB ÷ 实测 %.2fMB/s = ETA %ds > 预算 %ds"
                        "（本轮已落地 %d/%d 段）"
                        % (_rem / 1e6, _rate2 / 1e6, _rem / max(1.0, _rate2),
                           _eta_budget, len(parts), nreg))
    except JobTooSlow as _js:
        # E216：早停不是「文件坏了」，是「这个号现在跑不完」，所以：
        #   ① manifest 落盘（已完成区段全保留，下轮续传）
        #   ② 记一次池失败（连败两次 → 该号 30 分钟冷却，别再派大文件）
        #   ③ 掐掉还在跑的 worker（不留孤儿继续烧 TG 配额）
        #   ④ 以 rc=7 退出 → run_one.sh 跳过第二次 attempt，claim 立刻释放
        try:
            save()
        except Exception:                                  # noqa: BLE001
            pass
        print("ETA_TOO_SLOW %s" % _js, flush=True)
        try:
            _i3 = int(session_no or 1)
            _sn3 = (_slot_of_idx[_i3 - 1]
                    if 1 <= _i3 <= len(_slot_of_idx) else _i3)
            TP.report(_sn3, False, None, "eta_too_slow")
        except Exception:                                  # noqa: BLE001
            pass
        _kill_workers()
        sys.stdout.flush()
        sys.stderr.flush()
        for ex in executors:
            ex.shutdown(wait=False, cancel_futures=True)
        os._exit(7)
    except UploadStalled as _st:
        # E221：整整 _stall 秒没有任何区段落地 —— 判「僵死」，不判「慢」。
        # 慢由 E216 的整单 ETA 闸门兜；而零落地时那段判据根本不触发
        # （job_eta_exceeded 的 done<=0 / parts<min 直接 return False），
        # 于是只能等每个 worker 撞满 1800s 墙钟，一条 lane 白占 30 分钟、产出 0。
        # 实测 2026-11-27 全天 12 次 ZERO_PROGRESS，单次实测 1803s，全部是这条路径。
        _el3 = time.time() - t0
        _pend = sum(1 for _f in futs if not _f.done())
        try:
            save()
        except Exception:                                  # noqa: BLE001
            pass
        # 池子必须学到「这个号刚才一段都没传上去」：零进展不产生吞吐样本，
        # 不显式记失败，它的旧 mbps 会一直把它排在最前面（本次事故的成因）。
        _reported = set()
        try:
            for _fu3, (_i3, _l3, _s3) in futs.items():
                if _fu3.done():
                    continue
                _n3 = int(_s3 or 1)
                _sl3 = (_slot_of_idx[_n3 - 1]
                        if 1 <= _n3 <= len(_slot_of_idx) else _n3)
                if _sl3 in _reported:
                    continue
                _reported.add(_sl3)
                TP.report(_sl3, False, None, "stall_no_part")
        except Exception:                                  # noqa: BLE001
            pass
        if _eta_parts <= 0:
            print("ZERO_PROGRESS 本轮 %.0fs 内 0 个区段落地（%d 个 worker 在传）"
                  " —— 判僵死，交回 claim 换号复领" % (_el3, _pend), flush=True)
            _rc = 6
        else:
            print("STALL_NO_PART 本轮 %.0fs 无新区段落地（已落地 %d 段，"
                  "%d 个 worker 仍挂着）—— 尾巴僵死，manifest 保留交回 claim 续传"
                  % (_el3, _eta_parts, _pend), flush=True)
            _rc = 7
        _kill_workers()
        sys.stdout.flush()
        sys.stderr.flush()
        for ex in executors:
            ex.shutdown(wait=False, cancel_futures=True)
        os._exit(_rc)
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
