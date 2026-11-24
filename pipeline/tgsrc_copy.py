#!/usr/bin/env python3
"""tgsrc_copy.py —— 从「禁止转发」的频道把片源搬到自己的存储频道。

## 为什么需要它（2026-11-18 实测）


    noforwards=True     但成员可以 get_messages
    也能 iter_download  实测两个频道各拉 512KB 成功（2.4GB / 3.9GB 的文件）

也就是说 **禁的是「转发」这个动作，不是「读取字节」**。所以办法是：
下载 → 用 pc_upload 重新上传到存储频道 → 按 teldrive 的 file_parts 登记。

代价：这条路**不为零流量**。转发（tgpipe）是服务器端引用，不产生出站；
复制要完整下载（入站，免费）+ 完整上传（出站，计 Oracle 10TB 额度）。

## 用法

        --dir "电影/测试" --name "测试.mp4" --dry     # 只看，不动
        --dir "电影/测试" --name "测试.mp4"           # 真搬
"""
import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, "/opt/embytg/pipeline")
import gha_state as GS                                     # noqa: E402
import tgpipe as TP                                        # noqa: E402

RUNNER = "/opt/embytg-runner"
ENV = os.path.join(RUNNER, "runner.env")


# 2026-11-20：上传超时必须是可被上层判定的失败，不能是 SystemExit。
# emos_import 的换号重试用的是 `except Exception`，而 SystemExit 继承 BaseException
# → 超时直接掀掉整个进程，换号/续传一次都没跑到（昨夜 8 个尝试里 7 个这样废掉）。
class UploadTimeout(RuntimeError):
    """progress = 本次尝试真正落地的区段数（None = 从输出里看不出来）。

    「慢但活着」和「僵死」必须分开：前者换号只会丢掉已传字节，后者换号才有意义。
    """

    def __init__(self, msg, progress=None):
        super().__init__(msg)
        self.progress = progress


def _ensure_session_env():
    """tg_pool.discover_sessions() 只认环境变量，而调用方通常只有 runner.env 的键值，
    并没有 export。不补这一步，池子永远是空的（实测 snapshot() → []）。"""
    if any(k.startswith("TG_SESSION_STRING") and v for k, v in os.environ.items()):
        return
    try:
        for k, v in load_env().items():
            if k.startswith("TG_SESSION_STRING") and v:
                os.environ[k] = v
    except Exception as _e:                                        # noqa: BLE001
        print("  ! 读 runner.env 失败: %s" % str(_e)[:80], flush=True)


def pool_snapshot():
    """上传池快照（线程/进程内直接可用；槽位号即 TG_SESSION_STRING_N 的 N）。"""
    _ensure_session_env()
    try:
        if RUNNER not in sys.path:
            sys.path.insert(0, RUNNER)
        import tg_pool as _pool                                    # noqa: WPS433
        return _pool.snapshot()
    except Exception as _e:                                        # noqa: BLE001
        print("  ! 读上传池失败（预算按默认吞吐估算）: %s" % str(_e)[:80], flush=True)
        return []


def slot_mbps(slot):
    """该 slot 单 worker 实测吞吐（MB/s）。0 = 池里还没有数据。"""
    for r in pool_snapshot():
        if int(r.get("slot") or 0) == int(slot or 0):
            return float(r.get("mbps") or 0)
    return 0.0


def upload_budget(size_bytes, mbps, jobs):
    """上传总预算：按「实测单 worker × 并发 × 0.6 折」推算，不再假设 8MB/s。

    昨夜实测 0.14-0.23MB/s（8 并发 → 整体约 1.1-1.8MB/s），1.3GB 在 1200s 里
    连一个 512MiB 区段都传不完：8 个尝试 7 个被杀，manifest 0 个区段。

    2026-11-24 修正：外层预算必须 ≥ pc_upload 自己的单区段预算
    （auto_worker_timeout：floor 1800 / cap 2700），否则「区段还在正常落地、
    外层先到 900s SIGKILL」——1.66GB 在 slot2 实测连续两轮都是这么废掉的。
    区段是并发跑的，整轮墙钟 ≈ 一个区段 + md5/登记开销。
    """
    try:
        jobs = max(1, int(jobs))
    except (TypeError, ValueError):
        jobs = 8
    try:
        mbps = float(mbps or 0)
    except (TypeError, ValueError):
        mbps = 0.0
    chunk = 512 * 1024 * 1024
    if mbps <= 0:
        per_chunk = 1800
    else:
        per_chunk = int(min(2700, max(1800, (chunk / 1e6) / max(0.05, mbps) * 1.8 + 180)))
    budget = per_chunk + 420
    return max(900, min(3600, budget))


def load_env(path=ENV):
    d = {}
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            d[k] = v
    return d


def session_for(env, slot):
    return env["TG_SESSION_STRING"] if slot == 1 else env["TG_SESSION_STRING_%d" % slot]


async def fetch_one(env, slot, channel, msg_id, dest):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    client = TelegramClient(StringSession(session_for(env, slot)),
                            int(env["TG_API_ID"]), env["TG_API_HASH"])
    await client.connect()
    try:
        ent = await client.get_entity(channel)
        m = await client.get_messages(ent, ids=msg_id)
        if not m or not m.media:
            raise SystemExit("消息 %s 没有媒体" % msg_id)
        doc = m.document
        size = getattr(doc, "size", 0) or 0
        print("  源文件 %s  %.2f GB" % (msg_id, size / 1e9), flush=True)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        done = 0
        t0 = time.time()
        with open(dest, "wb") as f:
            async for chunk in client.iter_download(m, request_size=1024 * 1024):
                f.write(chunk)
                done += len(chunk)
                if done % (256 * 1024 * 1024) < 1024 * 1024:
                    el = max(time.time() - t0, 0.01)
                    print("    %.2f/%.2f GB  %.1f MB/s" % (done / 1e9, size / 1e9, done / el / 1e6),
                          flush=True)
        el = time.time() - t0
        print("  下载完成 %.2f GB  %.1f MB/s" % (done / 1e9, done / el / 1e6), flush=True)
        return dest
    finally:
        await client.disconnect()


def upload(env, slot, path):
    cmd = [os.path.join(RUNNER, "venv/bin/python"), os.path.join(RUNNER, "pc_upload.py"),
           path, "--session-slot", str(slot), "--store-ch", env["TG_STORE_CH"].split(",")[0]]
    env2 = dict(os.environ)
    env2.update({k: v for k, v in env.items()})
    print("  上传: %s" % " ".join(cmd[1:]), flush=True)
    # 2026-11-19：原来没有 timeout、且 capture_output 把进度全吞进内存 ——
    # 2.09GB 那单卡了 40 分钟，日志里既没进度也没报错，无从判断是慢还是死。
    # 现在按体积给预算（下限 20 分钟，按 8MB/s 估），超时就杀并打印尾部。
    try:
        _sz = os.path.getsize(path)
    except OSError:
        _sz = 0
    _jobs = int(os.environ.get("TG_UPLOAD_JOBS", "8") or 8)
    _budget = upload_budget(_sz, slot_mbps(slot), _jobs)
    # 探针预算：第一个「没验证过今天还活着」的号，只给 EMOS_PROBE_S（默认 600s）。
    # 落地任何区段=活着，继续给它整预算；一个字节都没落地=死号，立刻换下一个。
    _probe = int(os.environ.get("EMOS_PROBE_S", "0") or 0)
    if _probe > 0:
        _budget = min(_budget, _probe)
    # 2026-11-24：夜间窗口（EMOS_DEADLINE=epoch）不能把单次上传预算开到窗口之外，
    # 否则 systemd 到点 SIGTERM 连人带文件一起砍，日志里连「完成 x/y」都等不到。
    _dl = (os.environ.get("EMOS_DEADLINE") or "").strip()
    if _dl:
        try:
            _rem = float(_dl) - time.time()
        except ValueError:
            _rem = None
        if _rem is not None:
            if _rem < 300:
                raise UploadTimeout("窗口预算不足 300s，不做上传（slot%s）" % slot)
            _budget = max(300, int(min(_budget, _rem - 60)))
    try:
        r = subprocess.run(cmd, env=env2, capture_output=True, text=True, timeout=_budget)
    except subprocess.TimeoutExpired as _te:
        _tail = ((_te.stdout or b"") if isinstance(_te.stdout, bytes) else (_te.stdout or ""))
        if isinstance(_tail, bytes):
            _tail = _tail.decode("utf-8", "ignore")
        print("  上传超时（%ds，%.2fGB）：%s" % (_budget, _sz / 1e9, str(_tail)[-300:]), flush=True)
        # 从输出里判断这次到底是「慢但活着」还是「一个字节都没落地」
        _prog = None
        _m = re.search(r"已完成\s*(\d+)", str(_tail))
        _exist = len(re.findall(r"区段\s*\d+/\d+\s*已存在", str(_tail)))
        if _m:
            _prog = max(int(_m.group(1)), _exist)
        elif _exist:
            _prog = _exist
        raise UploadTimeout("上传超时（%ds，%.2fGB，slot%s）" % (_budget, _sz / 1e9, slot),
                            progress=_prog)
    out = (r.stdout or "") + (r.stderr or "")
    mani = ""
    for line in out.splitlines():
        if line.startswith("MANIFEST="):
            mani = line.split("=", 1)[1].strip()
    if r.returncode != 0 or not mani:
        print(out[-800:], flush=True)
        raise SystemExit("上传失败 rc=%s" % r.returncode)
    j = json.load(open(mani, encoding="utf-8"))
    # parts 是 **list**（每项 part_no/message_id/channel_id/plain_size），按 part_no 排序
    raw = j["parts"]
    if isinstance(raw, dict):
        raw = list(raw.values())
    raw = sorted(raw, key=lambda x: int(x.get("part_no") or 0))
    return [(int(p["message_id"]), int(p["plain_size"])) for p in raw]


def register(dir_path, name, parts):
    parent = None
    for seg in dir_path.strip("/").split("/"):
        parent = TP.ensure_folder(seg, parent)
    fid, total = TP.register(parent, name, parts)
    print("  已登记 teldrive: %s  %s  %.2f GB" % (fid[:8], name, total / 1e9), flush=True)
    return fid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--msg", type=int, required=True)
    ap.add_argument("--dir", required=True, help="teldrive 目标目录，如 电影/测试")
    ap.add_argument("--name", required=True)
    ap.add_argument("--slot", type=int, default=0)
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    env = load_env()
    slot = a.slot
    if not slot:
        for n in (1, 2, 3, 4):
            if GS.slot_lease(n, "tgsrc-%d" % os.getpid(), ttl=7200):
                slot = n
                break
        if not slot:
            raise SystemExit("没有空闲 slot")
    else:
        if not GS.slot_lease(slot, "tgsrc-%d" % os.getpid(), ttl=7200):
            raise SystemExit("slot %d 被占用" % slot)
    rid = "tgsrc-%d" % os.getpid()
    try:
        if a.dry:
            print("  DRY: 会用 slot%d 从 @%s 拉 msg=%d，目标 %s/%s"
                  % (slot, a.channel, a.msg, a.dir, a.name))
            return 0
        dest = os.path.join(RUNNER, "media.slot%d" % slot, a.name)
        fetch_one_sync = asyncio.run(fetch_one(env, slot, a.channel, a.msg, dest))
        try:
            parts = upload(env, slot, fetch_one_sync)
        except UploadTimeout as _ex:
            print("  %s" % _ex, flush=True)
            return 1
        register(a.dir, a.name, parts)
        try:
            os.remove(fetch_one_sync)
        except OSError:
            pass
        return 0
    finally:
        GS.slot_release(slot, rid)


if __name__ == "__main__":
    sys.exit(main())
