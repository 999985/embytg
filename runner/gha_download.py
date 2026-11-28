#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 gha_next.py 领取的源直链下载一集。

默认是完整下载；--resume 会先只读扫描 TG 存储频道里已经成功的
`<name>.partNN` 消息，把断点写进 manifest，再只对缺失的分片发 HTTP Range。
这样 runner 重启后不会重新传已有分片，也不会为了续传重下整集。
"""
import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time

import requests


def http_proxies():
    raw = (os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "").strip()
    if not raw or os.getenv("EMBYTG_FORCE_DIRECT") == "1":
        return {}
    return {"http": raw, "https": raw}


def tg_proxy():
    raw = os.getenv("TG_PROXY", "").strip()
    if not raw:
        return None
    from urllib.parse import urlparse
    u = urlparse(raw)
    return dict(proxy_type="http" if u.scheme.startswith("http") else "socks5",
                addr=u.hostname, port=u.port, rdns=True,
                username=u.username, password=u.password or "")

FIXED_UA = "Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)"
MAGIC = (b"\x1aE\xdf\xa3", b"RIFF", b"\xff\xd8")
PART_RE = re.compile(r"\.part(\d+)$")
NAME_QUALITY_RE = re.compile(
    r"(?i)(\s+-\s+)(2160p|1440p|1080p|720p|480p|sd)(\.[^.]+)$")
Q_RANK = {"2160p": 4, "1440p": 3, "1080p": 2, "720p": 1, "480p": 0, "sd": 0}
PARTIAL_DIR = os.getenv(
    "TG_PARTIAL_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "_partials"),
)
MIN_SPEED_MBPS = float(os.getenv("TG_MIN_DOWNLOAD_MBPS", "1.5"))
SPEED_SAMPLE_MB = int(os.getenv("TG_SPEED_SAMPLE_MB", "32"))
MAX_ETA_SEC = int(os.getenv("TG_MAX_DOWNLOAD_ETA_SEC", "7200"))
DOWNLOAD_RETRIES = max(1, int(os.getenv("TG_DOWNLOAD_RETRIES", "3")))
RETRY_BASE = max(0, int(os.getenv("TG_DOWNLOAD_RETRY_BASE", "30")))
RETRY_CAP = max(RETRY_BASE, int(os.getenv("TG_DOWNLOAD_RETRY_CAP", "90")))


class SourceTooSlow(RuntimeError):
    """源站速度低于保护阈值，应该释放 claim 并换源。"""


class ResumeScanError(RuntimeError):
    """TG 断点扫描失败；不能把这种情况误记为源站失败。"""


class QualityRejected(RuntimeError):
    """Downloaded bytes do not match the claimed quality or lane threshold."""


def magic_ok(head):
    return head[:4] in MAGIC or b"ftyp" in head[:32]


def _partial_paths(job, expect):
    raw = "\0".join((
        str(job.get("name") or ""),
        str(job.get("source_url") or ""),
        str(int(expect or 0)),
    ))
    key = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]
    os.makedirs(PARTIAL_DIR, exist_ok=True)
    base = os.path.join(PARTIAL_DIR, key)
    return base + ".part", base + ".json"


def _read_partial_meta(path):
    try:
        with open(path, encoding="utf-8") as f:
            value = json.load(f)
        return value if isinstance(value, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _write_partial_meta(path, value):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def _partial_size(job, expect, part, meta_path):
    if not os.path.exists(part):
        return 0
    meta = _read_partial_meta(meta_path)
    if (
        str(meta.get("name") or "") != str(job.get("name") or "")
        or str(meta.get("source_url") or "") != str(job.get("source_url") or "")
        or int(meta.get("total_size") or 0) != int(expect or 0)
    ):
        print("  ! 忽略无源校验的旧断点: %s" % part, file=sys.stderr)
        return 0
    have = os.path.getsize(part)
    if expect and have > int(expect):
        return 0
    return have


def _prune_partials(max_age_sec=None):
    """清理不再可能续传的残片。

    2026-11-14（E104 同批）：原 TTL 是 **7 天**，实测 `_partials.slot1` 攒了
    11 份 12–50 小时前的残片共 3.7G、`_partials.slot4` 2.7G —— 这些文件
    **永远不可能被用上**：续传依赖 claim + manifest，claim 最长 150 分钟就被
    `claim_reap.py` 回收，重领时 state_key 对不上就直接从头下。
    留 7 天纯属占盘（磁盘 99G，可用已从 37G 掉到 25G）。

    改为 **6 小时**（= 4 倍 claim 超时，留足慢速大文件的中断窗口），
    可用 `TG_PARTIAL_TTL_H` 覆盖。
    """
    if max_age_sec is None:
        try:
            max_age_sec = float(os.getenv("TG_PARTIAL_TTL_H", "6")) * 3600.0
        except (TypeError, ValueError):
            max_age_sec = 6 * 3600.0
    try:
        names = os.listdir(PARTIAL_DIR)
    except OSError:
        return
    cutoff = time.time() - max_age_sec
    for name in names:
        if not (name.endswith(".part") or name.endswith(".json")):
            continue
        path = os.path.join(PARTIAL_DIR, name)
        try:
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
        except OSError:
            pass


def _speed_gate(job, done, session_bytes, t0, expect, checked):
    """在采样窗口后拦截慢源；返回是否已完成本次检查。"""
    if checked:
        return True
    elapsed = max(time.time() - t0, 0.1)
    if session_bytes < SPEED_SAMPLE_MB * 1024 * 1024 and elapsed < 20:
        return False
    if session_bytes <= 0:
        return False
    rate = session_bytes / elapsed / 1e6
    remaining = max(0, int(expect or 0) - int(done))
    eta = (remaining / 1e6) / max(rate, 0.001) if remaining else 0
    if rate < MIN_SPEED_MBPS:
        raise SourceTooSlow(
            "%s 速度 %.2f MB/s 低于 %.2f MB/s（已下载 %.1f%%）"
            % (job.get("source_label") or "source", rate, MIN_SPEED_MBPS,
               (done * 100.0 / expect) if expect else 0)
        )
    if expect and eta > MAX_ETA_SEC:
        raise SourceTooSlow(
            "%s 预计剩余 %.1f 分钟，超过上限 %.1f 分钟"
            % (job.get("source_label") or "source", eta / 60.0,
               MAX_ETA_SEC / 60.0)
        )
    return True


def _file_name(msg):
    for attr in getattr(getattr(msg, "document", None), "attributes", []):
        name = getattr(attr, "file_name", None)
        if name:
            return name
    return ""


def _manifest_path(base):
    root = os.getenv(
        "TG_MANIFEST_DIR",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "_manifests"),
    )
    os.makedirs(root, exist_ok=True)
    return os.path.join(root, base + ".json")


def quality_from_dims(width, height):
    width, height = int(width or 0), int(height or 0)
    if height >= 1800 or width >= 3000:
        return "2160p"
    if height >= 1400 or width >= 2400:
        return "1440p"
    if height >= 1000 or width >= 1800:
        return "1080p"
    if height >= 700 or width >= 1200:
        return "720p"
    return "sd"


def probe_media(path):
    """Measure the downloaded bytes; source Emby metadata is not trusted."""
    exe = shutil.which("ffprobe")
    if not exe:
        raise RuntimeError("ffprobe 未安装，拒绝用源站元数据猜画质")
    cmd = [
        exe, "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height,codec_name",
        "-of", "json", path,
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if p.returncode != 0:
        raise RuntimeError("ffprobe 失败：%s" % (p.stderr or "")[-300:])
    try:
        stream = json.loads(p.stdout)["streams"][0]
        width = int(stream.get("width") or 0)
        height = int(stream.get("height") or 0)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError("ffprobe 输出无法解析：%r" % (e,))
    if width <= 0 or height <= 0:
        raise RuntimeError("ffprobe 未返回有效宽高")
    return {
        "width": width,
        "height": height,
        "codec": str(stream.get("codec_name") or ""),
        "quality": quality_from_dims(width, height),
    }


def corrected_name(name, quality):
    match = NAME_QUALITY_RE.search(name or "")
    if not match:
        raise RuntimeError("目标文件名没有可替换的画质后缀：%s" % name)
    return name[:match.start(2)] + quality + name[match.end(2):]


def apply_actual_quality(job_file, payload, job, target, probe):
    """Rewrite the local job/name/key to the measured quality before upload."""
    actual = probe["quality"]
    old_name = job["name"]
    new_name = corrected_name(old_name, actual)
    new_target = os.path.join(os.path.dirname(target), new_name)
    old_base = os.path.splitext(os.path.basename(old_name))[0]
    new_base = os.path.splitext(os.path.basename(new_name))[0]
    old_manifest = _manifest_path(old_base)
    new_manifest = _manifest_path(new_base)
    if old_manifest != new_manifest and os.path.exists(new_manifest):
        raise RuntimeError("修正后的 manifest 已存在：%s" % new_manifest)
    if new_target != target:
        if os.path.exists(new_target):
            raise RuntimeError("实测画质修正目标已存在：%s" % new_target)
        os.replace(target, new_target)
    new_rel = os.path.join(
        os.path.dirname(job["target_rel"]), new_name).replace("\\", "/")
    old_key = str(job.get("key") or "")
    new_key = old_key.rsplit("|", 1)[0] + "|" + actual if old_key else ""
    reported = job.get("quality")
    job.update({
        "name": new_name,
        "target_rel": new_rel,
        "folders": os.path.dirname(new_rel).replace("\\", "/"),
        "key": new_key,
        "quality": actual,
        "source_reported_quality": reported,
        "actual_width": int(probe["width"]),
        "actual_height": int(probe["height"]),
        "actual_quality": actual,
    })
    if payload.get("job") is job:
        payload["job"] = job
    tmp = job_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    os.replace(tmp, job_file)

    if old_manifest != new_manifest and os.path.exists(old_manifest):
        seed = json.load(open(old_manifest, encoding="utf-8"))
        seed.update({
            "name": new_name,
            "src": new_target,
            "actual_width": int(probe["width"]),
            "actual_height": int(probe["height"]),
            "actual_quality": actual,
        })
        with open(new_manifest, "w", encoding="utf-8") as f:
            json.dump(seed, f, ensure_ascii=False, indent=1)
        os.remove(old_manifest)
    return new_target


def _load_old_manifest(path, total, chunk):
    try:
        d = json.load(open(path, encoding="utf-8"))
        if int(d.get("total_size") or 0) == total and int(d.get("chunk_bytes") or 0) == chunk:
            return d
    except Exception:  # noqa: BLE001
        pass
    return {}


def _part_valid(part, idx, total, chunk):
    try:
        if int(part.get("part_no")) != idx:
            return False
        length = min(chunk, total - (idx - 1) * chunk)
        return (
            int(part.get("plain_size") or 0) == length
            and int(part.get("message_id") or 0) > 0
            and int(part.get("channel_id") or 0) > 0
        )
    except Exception:  # noqa: BLE001
        return False


def list_tg_parts(job, total, chunk, scan_limit):
    """只读扫描 job 指定的所有 TG 存储频道，返回 {part_no: part}。

    任一频道在所有会话里都扫描失败时必须停止，不能把“没看见”当成“没上传”，
    否则一次网络抖动就会把已有分片重新传一遍。
    """
    # 2026-11-14（E089）：与 pc_upload.py 同一个坑，写死只读前两个账号。
    # 第 3/4 个账号上传的分片，在这里扫不到 —— 会被当成"没上传"重传一遍。
    # 动态扫描所有 TG_SESSION_STRING[_N]。
    sessions = []
    _extra = []
    for _k, _v in os.environ.items():
        if not _k.startswith("TG_SESSION_STRING"):
            continue
        _v = (_v or "").strip()
        if not _v:
            continue
        _sfx = _k[len("TG_SESSION_STRING"):]
        if _sfx.startswith("_") and _sfx[1:].isdigit():
            _i = int(_sfx[1:])
        elif not _sfx:
            _i = 1
        else:
            continue
        _extra.append((_i, _v))
    for _i, _v in sorted(_extra):
        if _v not in sessions:
            sessions.append(_v)
    channels = []
    for raw in (job.get("store_channels") or []):
        try:
            channel = int(raw)
        except Exception:  # noqa: BLE001
            continue
        if channel and channel not in channels:
            channels.append(channel)
    for raw in (job.get("store_channel_id"), os.getenv("TG_STORE_CH", "").strip()):
        try:
            channel = int(raw or 0)
        except Exception:  # noqa: BLE001
            channel = 0
        if channel and channel not in channels:
            channels.append(channel)
    if not sessions or not channels or total <= 0:
        return {}
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from telethon.tl.functions.channels import GetChannelsRequest
        from telethon.tl.types import InputChannel
    except Exception as e:  # noqa: BLE001
        print("  ! TG 断点扫描不可用：%s" % str(e)[:100], file=sys.stderr)
        return {}

    base = os.path.splitext(os.path.basename(job["name"]))[0]
    api_id = int(os.getenv("TG_API_ID", "0") or 0)
    api_hash = os.getenv("TG_API_HASH", "").strip()
    if not api_id or not api_hash:
        return {}

    async def scan_one(client, channel, base):
        found = {}
        entity = (await client(GetChannelsRequest(
            [InputChannel(int(channel), 0)]))).chats[0]
        async for msg in client.iter_messages(entity, limit=scan_limit):
            doc = getattr(msg, "document", None)
            if not doc:
                continue
            fname = _file_name(msg)
            if not fname.startswith(base + "."):
                continue
            match = PART_RE.search(fname)
            if not match:
                continue
            idx = int(match.group(1))
            if idx < 1 or (idx - 1) * chunk >= total:
                continue
            expected = min(chunk, total - (idx - 1) * chunk)
            if int(getattr(doc, "size", 0) or 0) != expected:
                continue
            item = {
                "part_no": idx,
                "message_id": int(msg.id),
                "channel_id": int(channel),
                "plain_size": expected,
                "stored_size": expected,
                "date_ts": msg.date.timestamp() if getattr(msg, "date", None) else 0,
            }
            old = found.get(idx)
            if not old or item["date_ts"] >= old["date_ts"]:
                found[idx] = item
        return found

    async def scan_session(session, targets, base):
        client = TelegramClient(StringSession(session), api_id, api_hash,
                                connection_retries=2, timeout=60,
                                proxy=tg_proxy())
        found = {}
        errors = {}
        await asyncio.wait_for(client.connect(), timeout=90)
        try:
            for channel in targets:
                try:
                    got = await scan_one(client, channel, base)
                    for idx, item in got.items():
                        old = found.get(idx)
                        if not old or item["date_ts"] >= old["date_ts"]:
                            found[idx] = item
                except Exception as e:  # noqa: BLE001
                    errors[int(channel)] = str(e)[:120]
        finally:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=15)
            except Exception:  # noqa: BLE001
                pass
        return found, errors

    async def scan_all():
        found = {}
        remaining = list(channels)
        for session in sessions:
            if not remaining:
                break
            try:
                got, errors = await scan_session(session, remaining, base)
            except Exception as e:  # noqa: BLE001
                print("  ! TG 会话扫描失败：%s" % str(e)[:120], file=sys.stderr)
                continue
            for idx, item in got.items():
                old = found.get(idx)
                if not old or item["date_ts"] >= old["date_ts"]:
                    found[idx] = item
            remaining = [c for c in remaining if c in errors]
            for channel, err in errors.items():
                print("  ! TG 频道 %s 扫描失败：%s" % (channel, err), file=sys.stderr)
        if remaining:
            raise RuntimeError("TG 断点扫描未覆盖频道 %s，停止以避免重复上传" % remaining)
        if found:
            print("  TG 断点：找到 %d/%d 片（%d 个频道，扫描上限 %d）" % (
                len(found), (total + chunk - 1) // chunk, len(channels), scan_limit),
                flush=True)
        return found

    return asyncio.run(scan_all())


def write_seed(job, target, total, chunk, parts):
    base = os.path.splitext(os.path.basename(job["name"]))[0]
    path = _manifest_path(base)
    old = _load_old_manifest(path, total, chunk)
    merged = {}
    for source in (list(parts.values()), old.get("parts") or []):
        for part in source:
            idx = int(part.get("part_no") or 0)
            fallback_ch = int(part.get("channel_id") or old.get("channel_id")
                              or job.get("store_channel_id")
                              or os.getenv("TG_STORE_CH", "0") or 0)
            candidate = dict(part)
            candidate["channel_id"] = fallback_ch
            if _part_valid(candidate, idx, total, chunk):
                merged.setdefault(idx, {
                    "part_no": idx,
                    "message_id": int(part["message_id"]),
                    "channel_id": fallback_ch,
                    "plain_size": int(part["plain_size"]),
                    "stored_size": int(part.get("stored_size", part["plain_size"])),
                })
    ordered = [merged[i] for i in sorted(merged)]
    seed = {
        "name": job["name"],
        "src": target,
        "total_size": total,
        "channel_id": int(job.get("store_channel_id")
                          or os.getenv("TG_STORE_CH", "0") or 0),
        "store_channels": [int(x) for x in (job.get("store_channels") or [])],
        "chunk_bytes": chunk,
        "mime_type": "video/x-matroska" if target.lower().endswith((".mkv", ".webm")) else "video/mp4",
        "parts": ordered,
        "src_md5_full": old.get("src_md5_full"),
        "resume_sparse": bool(ordered),
        "done": len(ordered) == (total + chunk - 1) // chunk and sum(
            int(p["plain_size"]) for p in ordered) == total,
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(seed, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)
    print("  RESUME manifest: %d/%d 片已在 TG，缺 %d 片" % (
        len(ordered), (total + chunk - 1) // chunk,
        max(0, (total + chunk - 1) // chunk - len(ordered))), flush=True)
    return path, merged


def download_full(job, target, expect):
    range_meta_path = target + ".range.json"
    if os.path.exists(range_meta_path):
        try:
            os.remove(target)
        except OSError:
            pass
        try:
            os.remove(range_meta_path)
        except OSError:
            pass
    if os.path.exists(target) and (not expect or os.path.getsize(target) == expect):
        print("  本地完整文件已存在，跳过下载", flush=True)
        print("MEDIA=%s\nSIZE=%d" % (target, os.path.getsize(target)))
        return target
    part, meta_path = _partial_paths(job, expect)
    have = _partial_size(job, expect, part, meta_path)
    if expect and have == expect:
        os.replace(part, target)
        try:
            os.remove(meta_path)
        except OSError:
            pass
        print("MEDIA=%s\nSIZE=%d" % (target, have))
        return target
    headers = {"User-Agent": job.get("source_ua") or FIXED_UA, "Accept": "*/*"}
    # E227（2026-11-28）：lite.cn2gias.uk 的 stream 必须带 Emby 客户端身份头 +
    # MediaSourceId，否则一律 404。job 里由 gha_next3 填好，这里透传。
    headers.update(job.get("source_headers") or {})
    if have:
        headers["Range"] = "bytes=%d-" % have
        print("  本地断点续传：已有 %.2f GB" % (have / 1e9), flush=True)
    with requests.Session() as s:
        s.trust_env = False
        s.proxies.update(http_proxies())
        r = s.get(job["source_url"], headers=headers, stream=True,
                  timeout=(60, 300), allow_redirects=True, verify=False)
        if r.status_code not in (200, 206):
            raise SystemExit("源返回 HTTP %s" % r.status_code)
        if have and r.status_code == 200:
            have = 0
        if r.status_code == 206:
            content_range = (r.headers.get("Content-Range") or "").strip()
            if not content_range.startswith("bytes %d-" % have):
                raise SystemExit("断点 Range 不匹配：%s" % content_range[:100])
        _write_partial_meta(meta_path, {
            "name": job.get("name"),
            "source_url": job.get("source_url"),
            "source_label": job.get("source_label"),
            "total_size": int(expect or 0),
            "etag": r.headers.get("ETag") or "",
            "last_modified": r.headers.get("Last-Modified") or "",
            "updated_at": int(time.time()),
        })
        mode = "ab" if have else "wb"
        t0 = time.time()
        done = have
        start_done = have
        first = (have == 0)
        checked = False
        with open(part, mode) as f:
            for block in r.iter_content(256 * 1024):
                if not block:
                    continue
                if first:
                    if not magic_ok(block[:64]):
                        raise SystemExit("下载内容不是视频容器（magic 不符）")
                    first = False
                # 2026-11-15（E130）：**写入前必须防「超过声明大小」。**
                #
                # 实测 `辐射 S01E07`（声明 6.61GB）的 .part 写到 **26.9GB** 还在涨，
                # 直接吃到磁盘 85%：源站 206 响应的 Content-Range 起点对，
                # 但流里继续吐数据，而写入侧从不校验总长。
                # 后果不只是占盘 —— 这个文件永远不可能"下完"，lane 被它长期占住。
                #
                # 2026-11-15 补（E130b）：**第一版判据太严，把好文件也毙了。**
                # 实测 `辐射 S01E08`：declared=6769493874，最后一块写到 6769606656，
                # 只超 **112KB（0.0017%）**——那是探针与实际长度的微小差异，
                # 一 raise 就整集失败，fill lane 直接停摆（近 3h 从 45 集掉到 14 集）。
                # 真正要挡的是「数量级异常」（26.9GB vs 6.61GB）。
                # 所以：小超额 → 只写到 declared 处并结束（多余字节丢弃）；
                # 大超额（> 8MiB 或 >1%）→ 仍然 fail-fast，那才是源站/断点错乱。
                if expect and done + len(block) > int(expect):
                    _over = done + len(block) - int(expect)
                    _tol = max(8 * 1024 * 1024, int(int(expect) * 0.01))
                    if _over > _tol:
                        raise SystemExit(
                            "下载超过声明大小（declared=%d, wrote=%d+%d, over=%d）"
                            % (int(expect), done, len(block), _over))
                    f.write(block[:int(expect) - done])
                    done = int(expect)
                    print("  截断多余字节 %d（<容差 %d），按声明大小收尾"
                          % (_over, _tol), flush=True)
                    break
                f.write(block)
                done += len(block)
                checked = _speed_gate(
                    job, done, done - start_done, t0, expect, checked)
                if done // (64 * 1024 * 1024) != (done - len(block)) // (64 * 1024 * 1024):
                    print("  %.1f%% %s / %s %.2f MB/s" % (
                        done / expect * 100 if expect else 0,
                        "%.2fGB" % (done / 1e9), "%.2fGB" % (expect / 1e9) if expect else "?",
                        done / 1e6 / max(time.time() - t0, 0.1)), flush=True)
                    _write_partial_meta(meta_path, {
                        "name": job.get("name"),
                        "source_url": job.get("source_url"),
                        "source_label": job.get("source_label"),
                        "total_size": int(expect or 0),
                        "etag": r.headers.get("ETag") or "",
                        "last_modified": r.headers.get("Last-Modified") or "",
                        "updated_at": int(time.time()),
                    })
        got = os.path.getsize(part)
        if expect and got != expect:
            raise SystemExit("大小不符：下载 %d / 源 %d" % (got, expect))
        os.replace(part, target)
        try:
            os.remove(meta_path)
        except OSError:
            pass
        print("MEDIA=%s\nSIZE=%d" % (target, got))
    return target


def download_missing_ranges(job, target, total, chunk, existing):
    missing = [i for i in range(1, (total + chunk - 1) // chunk + 1) if i not in existing]
    if not missing:
        if not os.path.exists(target) or os.path.getsize(target) != total:
            with open(target, "wb") as f:
                f.truncate(total)
        try:
            os.remove(target + ".range.json")
        except OSError:
            pass
        print("NO_SOURCE_DOWNLOAD: 所有分片都已在 TG，只生成稀疏占位文件", flush=True)
        return target

    range_meta_path = target + ".range.json"
    range_meta = _read_partial_meta(range_meta_path)
    if (
        str(range_meta.get("name") or "") != str(job.get("name") or "")
        or str(range_meta.get("source_url") or "") != str(job.get("source_url") or "")
        or int(range_meta.get("total_size") or 0) != int(total)
    ):
        range_meta = {}
        done_ranges = set()
    else:
        done_ranges = set(int(x) for x in (range_meta.get("done_ranges") or []))
    if os.path.exists(target) and os.path.getsize(target) == total:
        mode = "r+b"
    else:
        if os.path.exists(target):
            os.remove(target)
        with open(target, "wb") as f:
            f.truncate(total)
        done_ranges = set()
        mode = "r+b"
    range_meta.update({
        "name": job.get("name"),
        "source_url": job.get("source_url"),
        "source_label": job.get("source_label"),
        "total_size": total,
        "chunk_bytes": chunk,
    })
    _write_partial_meta(range_meta_path, dict(range_meta, done_ranges=sorted(done_ranges)))
    headers_base = {
        "User-Agent": job.get("source_ua") or FIXED_UA,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
    }
    with requests.Session() as s, open(target, mode) as f:
        s.trust_env = False
        s.proxies.update(http_proxies())
        for n, idx in enumerate(missing, 1):
            if idx in done_ranges:
                print("  RANGE part%02d 已在本地断点，跳过" % idx, flush=True)
                continue
            start = (idx - 1) * chunk
            length = min(chunk, total - start)
            end = start + length - 1
            headers = dict(headers_base, Range="bytes=%d-%d" % (start, end))
            r = s.get(job["source_url"], headers=headers, stream=True,
                      timeout=(60, 300), allow_redirects=True, verify=False)
            if r.status_code == 200:
                r.close()
                print("  ! 源不支持 Range，回退完整下载", flush=True)
                f.close()
                try:
                    os.remove(target)
                except OSError:
                    pass
                return download_full(job, target, total)
            if r.status_code != 206:
                r.close()
                raise SystemExit("缺片 Range 返回 HTTP %s" % r.status_code)
            content_range = (r.headers.get("Content-Range") or "").strip()
            if content_range and not content_range.startswith(
                    "bytes %d-%d/" % (start, end)):
                r.close()
                raise SystemExit("缺片 part%02d 返回了错误的 Range：%s" % (
                    idx, content_range[:80]))
            got = 0
            t0 = time.time()
            checked = False
            for block in r.iter_content(256 * 1024):
                if not block:
                    continue
                f.seek(start + got)
                f.write(block)
                got += len(block)
                checked = _speed_gate(job, got, got, t0, length, checked)
                if got > length:
                    break
            r.close()
            if got != length:
                raise SystemExit("缺片 part%02d 大小不符：%d/%d" % (idx, got, length))
            f.flush()
            done_ranges.add(idx)
            range_meta["done_ranges"] = sorted(done_ranges)
            _write_partial_meta(range_meta_path, range_meta)
            print("  RANGE part%02d 完成 %d/%d" % (idx, n, len(missing)), flush=True)
    try:
        os.remove(range_meta_path)
    except OSError:
        pass
    return target


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True)
    ap.add_argument("--out-dir", default=os.path.join(os.environ.get("RUNNER_TEMP", "."), "media"))
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--resume", action="store_true",
                    help="从 TG 已上传分片恢复，只下载缺失 Range")
    ap.add_argument("--skip-resume-scan", action="store_true",
                    help="新任务跳过 TG 历史扫描（Bot 会话无 GetHistory 权限时用；"
                         "有半成品分片的任务禁止使用）")
    ap.add_argument("--chunk-mib", type=int, default=512)
    ap.add_argument("--scan-limit", type=int, default=3000)
    a = ap.parse_args()
    payload = json.load(open(a.job, encoding="utf-8"))
    job = payload.get("job") if payload.get("ok") and isinstance(payload.get("job"), dict) else payload
    if not job.get("source_url"):
        raise SystemExit("没有可下载的 job")
    os.makedirs(a.out_dir, exist_ok=True)
    _prune_partials()
    target = os.path.join(a.out_dir, job["name"])
    expect = int(job.get("size") or 0)

    if a.probe:
        headers = {"User-Agent": job.get("source_ua") or FIXED_UA,
                   "Accept": "*/*", "Range": "bytes=0-1048575"}
        with requests.Session() as s:
            s.trust_env = False
            s.proxies.update(http_proxies())
            r = s.get(job["source_url"], headers=headers, stream=True,
                      timeout=(60, 180), allow_redirects=True, verify=False)
            r.raise_for_status()
            head = b""
            for block in r.iter_content(1024):
                head += block
                if len(head) >= 1024:
                    break
            print("PROBE status=%s type=%s len=%s range=%s magic=%s" % (
                r.status_code, r.headers.get("Content-Type"),
                r.headers.get("Content-Length"), r.headers.get("Content-Range"),
                "OK" if magic_ok(head) else "BAD"))
            return 0 if magic_ok(head) else 2

    chunk = a.chunk_mib * 1024 * 1024
    existing = {}
    if a.resume and expect:
        if a.skip_resume_scan:
            print("SKIP_RESUME_SCAN: 新任务，跳过 TG 历史扫描", file=sys.stderr)
        else:
            try:
                existing = list_tg_parts(job, expect, chunk, a.scan_limit)
                _, existing = write_seed(job, target, expect, chunk, existing)
            except Exception as e:  # noqa: BLE001
                print("TG_RESUME_SCAN_FAILED: %r" % (e,), file=sys.stderr)
                return 4

    if not existing:
        operation = lambda: download_full(job, target, expect)
    else:
        operation = lambda: download_missing_ranges(job, target, expect, chunk, existing)

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            operation()
            try:
                probe = probe_media(target)
            except Exception as e:  # noqa: BLE001
                raise QualityRejected("真实画质探测失败：%s" % str(e)[:300])
            actual_rank = Q_RANK.get(probe["quality"], 0)
            if job.get("lane") == "new" and actual_rank < 2:
                raise QualityRejected(
                    "实测画质低于新增线门槛：%s (%dx%d)" % (
                        probe["quality"], probe["width"], probe["height"]))
            if job.get("lane") == "fill" and actual_rank < 1:
                raise QualityRejected(
                    "实测画质低于补缺线门槛：%s (%dx%d)" % (
                        probe["quality"], probe["width"], probe["height"]))
            if job.get("lane") == "upgrade":
                old_rank = Q_RANK.get(
                    str(job.get("old_quality") or "sd").lower(), 0)
                if actual_rank <= old_rank:
                    raise QualityRejected(
                        "洗版实测画质未提升：%s <= %s (%dx%d)" % (
                            probe["quality"], job.get("old_quality"),
                            probe["width"], probe["height"]))
            try:
                target = apply_actual_quality(a.job, payload, job, target, probe)
            except Exception as e:  # noqa: BLE001
                raise QualityRejected("真实画质修正失败：%s" % str(e)[:300])
            print("MEDIA=%s\nSIZE=%d\nACTUAL=%dx%d %s" % (
                target, os.path.getsize(target), probe["width"],
                probe["height"], probe["quality"]))
            return 0
        except QualityRejected as e:
            print("QUALITY_REJECTED: %s" % e, file=sys.stderr)
            return 5
        except SourceTooSlow as e:
            print("SOURCE_TOO_SLOW: %s" % e, file=sys.stderr)
            return 3
        except Exception as e:  # noqa: BLE001
            if attempt >= DOWNLOAD_RETRIES:
                print("DOWNLOAD_FAILED after %d attempts: %r" % (attempt, e),
                      file=sys.stderr)
                return 2
            wait = min(RETRY_BASE * attempt, RETRY_CAP)
            print("DOWNLOAD_RETRY %d/%d in %ss: %r" % (
                attempt, DOWNLOAD_RETRIES, wait, e), file=sys.stderr, flush=True)
            time.sleep(wait)
    return 2


if __name__ == "__main__":
    sys.exit(main() or 0)
