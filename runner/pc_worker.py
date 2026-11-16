#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pc_worker.py — 单进程上传"一个区段"为一条 TG 文档(单连接)。
由 pc_upload_mp.py 调度；独立进程 => 独立 msg_id, 多进程可叠加带宽。
成功时 stdout 打印: MSG=<message_id>
"""
import argparse, asyncio, ctypes, os, signal, sys, time
from urllib.parse import urlparse

# 父进程(pc_upload)被 kill/timeout 时，worker 必须一起退出；否则会留下
# 孤儿进程继续占带宽和内存，下一次 attempt 再起 4 个 worker 就会翻倍。
try:
    _parent_pid = os.getppid()
    ctypes.CDLL("libc.so.6", use_errno=True).prctl(
        1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG
    if os.getppid() != _parent_pid:
        os._exit(137)
except Exception:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import functions
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.types import (InputChannel, InputPeerChannel, InputFileBig,
                               DocumentAttributeFilename)
from telethon.errors import FloodWaitError, RPCError

def required_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit("缺少环境变量: %s" % name)
    return value


API_ID = int(required_env("TG_API_ID"))
API_HASH = required_env("TG_API_HASH")
SESS_FILE = os.getenv("TG_SESSION_FILE", "").strip()
SESSION_STRING = os.getenv("TG_SESSION_STRING", "").strip()
STORE_CH = int(required_env("TG_STORE_CH").split(",")[0].strip())


def _proxy_from_env():
    raw = os.getenv("TG_PROXY", "").strip()
    if not raw:
        return None
    u = urlparse(raw)
    if u.scheme not in ("http", "https", "socks5", "socks5h"):
        raise RuntimeError("TG_PROXY 只支持 http/socks5: %s" % raw)
    d = dict(proxy_type="http" if u.scheme.startswith("http") else "socks5",
             addr=u.hostname, port=u.port, rdns=True)
    if u.username:
        d["username"] = u.username
        d["password"] = u.password or ""
    return d


PROXY = _proxy_from_env()
if PROXY is None and not SESSION_STRING:
    # 本机旧行为：默认走 7897；GitHub Actions 用 TG_SESSION_STRING 时默认直连。
    PROXY = dict(proxy_type="http", addr="127.0.0.1", port=7897, rdns=True)
MTPART = 512 * 1024
MAX_FLOOD_WAIT = int(os.getenv("TG_MAX_FLOOD_WAIT", "300"))
# 单个 RPC 的硬超时：坏连接不会自己报错，会把整条上传流水线挂住。
# 超时按“失败”处理并重试，而不是无限等待。
REQ_TIMEOUT = int(os.getenv("TG_PART_RPC_TIMEOUT", "90"))
SEND_TIMEOUT = int(os.getenv("TG_SEND_RPC_TIMEOUT", "300"))
# 单个区段（默认 128MiB）的总时限；超过就失败，交给 pc_upload 重试。
CHUNK_DEADLINE = int(os.getenv("TG_CHUNK_DEADLINE", "2400"))


async def _save(client, fid, idx, pc, data, tries=6):
    for a in range(1, tries + 1):
        try:
            await asyncio.wait_for(
                client(functions.upload.SaveBigFilePartRequest(
                    fid, idx, pc, data)),
                timeout=REQ_TIMEOUT)
            return
        except asyncio.TimeoutError:
            print("part %d RPC 超时 %ss，重试 %d/%d" % (
                idx, REQ_TIMEOUT, a, tries), file=sys.stderr, flush=True)
            await asyncio.sleep(2)
        except FloodWaitError as e:
            if e.seconds > MAX_FLOOD_WAIT:
                raise RuntimeError("part %d FloodWait %ss 超过上限 %ss" % (
                    idx, e.seconds, MAX_FLOOD_WAIT))
            await asyncio.sleep(e.seconds + 3)
        except (RPCError, OSError):
            await asyncio.sleep(2)
    raise RuntimeError("part %d 失败" % idx)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--offset", type=int, required=True)
    ap.add_argument("--length", type=int, required=True)
    ap.add_argument("--name", required=True)      # 该区段文档名
    a = ap.parse_args()

    if SESSION_STRING:
        session_str = SESSION_STRING
    elif SESS_FILE:
        session_str = open(SESS_FILE).read().strip()
    else:
        raise SystemExit("缺少 TG_SESSION_STRING 或 TG_SESSION_FILE")
    c = TelegramClient(StringSession(session_str), API_ID, API_HASH,
                       proxy=PROXY, connection_retries=4, timeout=180)
    await c.connect()
    res = await c(GetChannelsRequest([InputChannel(STORE_CH, 0)]))
    ch = res.chats[0]
    peer = InputPeerChannel(ch.id, ch.access_hash)

    pc = (a.length + MTPART - 1) // MTPART
    fid = int.from_bytes(os.urandom(8), "big", signed=True)
    f = open(a.src, "rb")
    started = time.monotonic()
    try:
        for pi in range(pc):
            if time.monotonic() - started > CHUNK_DEADLINE:
                raise RuntimeError("区段 %s 超过总时限 %ss" % (a.name, CHUNK_DEADLINE))
            f.seek(a.offset + pi * MTPART)
            data = f.read(MTPART)
            await _save(c, fid, pi, pc, data)
    finally:
        f.close()

    inf = InputFileBig(fid, pc, a.name)
    for a2 in range(1, 6):
        try:
            msg = await asyncio.wait_for(
                c.send_file(peer, inf, force_document=True,
                            attributes=[DocumentAttributeFilename(a.name)]),
                timeout=SEND_TIMEOUT)
            print("MSG=%d" % msg.id, flush=True)
            break
        except asyncio.TimeoutError:
            print("send 超时 %ss，重试 %d" % (SEND_TIMEOUT, a2),
                  file=sys.stderr, flush=True)
            await asyncio.sleep(3)
        except FloodWaitError as e:
            if e.seconds > MAX_FLOOD_WAIT:
                raise RuntimeError("send FloodWait %ss 超过上限 %ss" % (
                    e.seconds, MAX_FLOOD_WAIT))
            await asyncio.sleep(e.seconds + 3)
        except (RPCError, OSError) as e:
            print("send retry %d: %s" % (a2, repr(e)[:60]), file=sys.stderr, flush=True)
            await asyncio.sleep(3)
    else:
        raise RuntimeError("send 失败")
    try:
        await asyncio.wait_for(c.disconnect(), timeout=15)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
