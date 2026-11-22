#!/usr/bin/env python3
"""GHA 侧搬运：读 pipeline/queue/assign.json → 下载 → 上传 TG → 提交结果。

环境变量：GH_SESSION_STRING / TG_API_ID / TG_API_HASH / TG_STORE_CH / LIMIT / DRY / TOKEN
DRY=1 时只下载不上传（用于验证链路，不需要消耗号）。

约束：只用消耗号；不回连 Oracle；结果写回 pipeline/queue/result-*.json 由控制面轮询。
"""
import asyncio
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent      # pipeline/
REPO = HERE.parent                                  # 仓库根
QUEUE = HERE / "queue"
UA = ("Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)")


def load_assign():
    p = QUEUE / "assign.json"
    if not p.exists():
        print("无 assign.json，退出")
        return []
    return (json.loads(p.read_text(encoding="utf-8")) or {}).get("items") or []


def commit_result(rec):
    QUEUE.mkdir(exist_ok=True)
    path = QUEUE / ("result-%s.json" % rec["run_id"])
    path.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")
    if not os.environ.get("TOKEN"):
        print("  本地模式：结果写到 %s" % path.name)
        return
    rel = str(path.relative_to(REPO))
    for cmd in (["git", "config", "user.email", "gha@local"],
                ["git", "config", "user.name", "gha-transport"],
                ["git", "add", rel],
                ["git", "commit", "-m", "result %s" % rec["run_id"]],
                ["git", "push"]):
        r = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
        if r.returncode and cmd[1] not in ("config", "commit"):
            print("  %s 失败: %s" % (" ".join(cmd[:2]), (r.stderr or "")[:120]))


def download(url, dest):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
        total = 0
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
            total += len(chunk)
    return total


PART_MB = int(os.environ.get("GHA_PART_MB") or 1800)


def split(path, part_bytes):
    """把文件切成若干段——单条消息有 2GB 上限，但整集不必塞进一条。"""
    parts = []
    with open(path, "rb") as f:
        idx = 0
        while True:
            data = f.read(part_bytes)
            if not data:
                break
            idx += 1
            p = "%s.part%03d" % (path, idx)
            with open(p, "wb") as o:
                o.write(data)
            parts.append((idx, p, len(data)))
    return parts


async def upload_parts(parts, caption, meta):
    """逐段上传，返回 teldrive 需要的 parts 列表（与 pc_upload.py 同构）。"""
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    s = os.environ.get("GH_SESSION_STRING") or ""
    if not s:
        raise RuntimeError("缺少 GH_SESSION_STRING（消耗号），DRY=1 可先验证链路")
    ch = int(os.environ["TG_STORE_CH"])
    c = TelegramClient(StringSession(s), int(os.environ["TG_API_ID"]),
                       os.environ["TG_API_HASH"])
    await c.connect()
    if not await c.is_user_authorized():
        raise RuntimeError("GH_SESSION_STRING 未授权（消耗号可能已失效）")
    out = []
    n = len(parts)
    for idx, path, size in parts:
        msg = await c.send_file(
            ch, path, caption="%s [%d/%d]" % (caption or "", idx, n),
            attributes=[], supports_streaming=False, force_document=True)
        out.append({"part_no": idx, "message_id": msg.id,
                    "plain_size": size, "stored_size": size,
                    "channel_id": meta.get("channel_id") or 0})
        print("  part %d/%d 完成 msg=%s %.0fMB" % (idx, n, msg.id, size / 1e6))
    await c.disconnect()
    return out


def main():
    limit = int(os.environ.get("LIMIT") or 1)
    dry = os.environ.get("DRY", "1") in ("1", "true", "True")
    items = load_assign()[:limit]
    if not items:
        print("队列为空，无事可做")
        return 0
    for it in items:
        t0 = time.time()
        rec = {"run_id": it["run_id"], "ok": False, "reason": "", "core": it.get("core"),
               "dry": dry}
        try:
            with tempfile.TemporaryDirectory() as td:
                dest = os.path.join(td, it.get("filename") or "file.bin")
                size = download(it["url"], dest)
                rec["size"] = size
                print("  下载完成 %.1f MB" % (size / 1e6))
                if dry:
                    rec["ok"] = True
                    rec["reason"] = "dry_run"
                    print("  DRY：跳过上传")
                else:
                    parts = split(dest, PART_MB * 1024 * 1024)
                    print("  切成 %d 段（每段 ≤%dMB）" % (len(parts), PART_MB))
                    rec["parts"] = asyncio.run(
                        upload_parts(parts, it.get("caption") or "", rec.get("meta") or {}))
                    rec["file_id"] = rec["parts"][0]["message_id"]
                    rec["meta"] = it.get("meta") or {}
                    rec["ok"] = True
                    print("  全部完成，共 %d 段" % len(rec["parts"]))
        except Exception as e:                                  # noqa: BLE001
            rec["reason"] = str(e)[:200]
            print("  失败: %s" % rec["reason"])
        rec["secs"] = round(time.time() - t0, 1)
        commit_result(rec)
    return 0


if __name__ == "__main__":
    sys.exit(main())
