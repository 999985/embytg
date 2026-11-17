#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""调用本项目已有的 pc_upload.py；GitHub runner 只换 Telegram 会话来源。"""
import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def _mani_dir():
    return os.getenv("TG_MANIFEST_DIR", os.path.join(HERE, "_manifests"))


def zero_progress(src):
    """E164：本次 attempt 一个分片都没落地时返回 True。

    判据只看 pc_upload 自己写的 manifest 里 parts 是否为空 —— 不看日志文本
    （日志格式会变，manifest 是契约）。读不到 manifest 一律返回 False，
    宁可多试一次，也不要因为路径问题把正常失败判成零进展。
    """
    base = os.path.splitext(os.path.basename(src))[0]
    path = os.path.join(_mani_dir(), base + ".json")
    try:
        with open(path, encoding="utf-8") as f:
            mani = json.load(f)
    except (OSError, ValueError):
        return False
    return not (mani.get("parts") or [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True)
    ap.add_argument("--media-dir", required=True)
    ap.add_argument("--manifest-out", required=True)
    a = ap.parse_args()
    payload = json.load(open(a.job, encoding="utf-8"))
    job = payload.get("job") if payload.get("ok") and isinstance(payload.get("job"), dict) else payload
    src = os.path.join(a.media_dir, job["name"])
    if not os.path.exists(src):
        raise SystemExit("媒体不存在: %s" % src)
    cmd = [sys.executable, os.path.join(HERE, "pc_upload.py"), src,
           "--name", job["name"],
           "--chunk-mib", os.getenv("TG_CHUNK_MIB", "512"),
           "--jobs", os.getenv("TG_UPLOAD_JOBS", "6"),
           "--worker-timeout", os.getenv("TG_WORKER_TIMEOUT", "0")]
    store_ch = int(job.get("store_channel_id") or 0)
    if store_ch:
        cmd.extend(["--store-ch", str(store_ch)])
    session_slot = int(job.get("tg_session_slot") or 0)
    # 双账号分片轮换：两个 session 都已验证在 emby 存储频道且有发送权限。
    # 保留单账号模式作为回退：TG_UPLOAD_MULTI_SESSION=0。
    multi_session = os.getenv("TG_UPLOAD_MULTI_SESSION", "0") == "1"
    if session_slot and not multi_session:
        cmd.extend(["--session-slot", str(session_slot)])
    try:
        r = subprocess.run(cmd, cwd=HERE, timeout=int(os.getenv("TG_UPLOAD_TIMEOUT", "3600")))
    except subprocess.TimeoutExpired:
        raise SystemExit("pc_upload 超时，保留已完成分片供下次续传")
    if r.returncode != 0:
        # E164：零进展（一个分片都没落地）时不要在同一轮里重试第二次。
        # 实测 2026-11-17 05:48 终末的后宫 S01E04：attempt1 4 个区段全部
        # worker 超时（900s）落地 0 片，attempt2 结果完全相同，白占 lane 30 分钟。
        # 退出码 6 = 零进展，由 run_one.sh 决定跳过重试（文件留 _keep 下次硬链接复用）。
        if zero_progress(src):
            print("ZERO_PROGRESS 0 个分片落地，交由上层跳过重试（文件保留待复领）")
            return 6
        return r.returncode
    mani_dir = os.getenv("TG_MANIFEST_DIR", os.path.join(HERE, "_manifests"))
    base = os.path.splitext(os.path.basename(src))[0]
    path = os.path.join(mani_dir, base + ".json")
    if not os.path.exists(path):
        raise SystemExit("上传完成但找不到 manifest: %s" % path)
    with open(path, encoding="utf-8") as f:
        mani = json.load(f)
    required = ("actual_width", "actual_height", "actual_quality",
                "target_rel", "folders", "key", "quality")
    missing = [k for k in required if not job.get(k)]
    if missing:
        raise SystemExit("job 缺少真实画质/登记字段: %s" % ",".join(missing))
    if str(job["actual_quality"]).lower() != str(job["quality"]).lower():
        raise SystemExit("job 实测画质与目标画质不一致: %s != %s" % (
            job["actual_quality"], job["quality"]))
    if mani.get("name") != job["name"]:
        raise SystemExit("manifest.name 与修正后的 job.name 不一致")
    mani.update({
        "actual_width": int(job["actual_width"]),
        "actual_height": int(job["actual_height"]),
        "actual_quality": job["actual_quality"],
        "target_rel": job["target_rel"],
        "folders": job["folders"],
        "key": job["key"],
        "quality": job["quality"],
        "source_reported_quality": job.get("source_reported_quality"),
    })
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(mani, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)
    with open(a.manifest_out, "w", encoding="utf-8") as f:
        f.write(path)
    print("MANIFEST=%s" % path)
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
