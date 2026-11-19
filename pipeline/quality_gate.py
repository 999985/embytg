#!/usr/bin/env python3
"""quality_gate.py —— 画质闸门：不是「分辨率够高」就叫好片。

## 为什么需要它

2026-11-19 用户反馈「谷雨的质量很低，都是 300-400MB」。
实测那批文件确实标着 2160p（3840x1600 HEVC），但单集只有 345-435MB ——
按 45 分钟算约 1.0-1.6 Mbps，是**压得很狠的高分辨率垃圾**。
原来所有判据只看 width/height，所以这种片一路绿灯入库。

## 判据（码率下限，按分辨率分级）

    2160p  >= 4.0 Mbps
    1080p  >= 2.0 Mbps
    720p   >= 1.0 Mbps
    其它   >= 0.6 Mbps

下限取「能看」而不是「最好」，避免把正常的低码率老片全砍掉；
真正要拦的是「4K 标签 + 短视频码率」这种明显虚标的。

用法：
    from quality_gate import too_low
    too_low(size_bytes, duration_sec, "2160p")   # -> (bool, mbps)
"""
import os
import sys

sys.path.insert(0, "/opt/embytg/pipeline")

FLOOR = {"2160p": 2.0, "1080p": 0.8, "720p": 0.4}
FLOOR_DEFAULT = 0.6


def mbps(size_bytes, dur_sec):
    if not size_bytes or not dur_sec:
        return 0.0
    return float(size_bytes) * 8.0 / float(dur_sec) / 1e6


def too_low(size_bytes, dur_sec, qual):
    """返回 (是否太差, 实际 Mbps)。时长未知时按 0 处理 = 不判（宁可放过不误杀）。"""
    if not dur_sec:
        return False, 0.0
    m = mbps(size_bytes, dur_sec)
    return m < FLOOR.get(qual, FLOOR_DEFAULT), m


def qual_from_dims(w, h):
    w, h = int(w or 0), int(h or 0)
    if w >= 3000 or h >= 1800:
        return "2160p"
    if w >= 1700 or h >= 1000:
        return "1080p"
    if w >= 1200 or h >= 700:
        return "720p"
    return "SD"


def log_reject(kind, name, size, dur, qual, mbps_val, extra=""):
    """被闸门拦下的都留痕，方便事后看「是不是误杀」。"""
    import json
    import time
    p = "/opt/embytg/pipeline/logs/quality_gate.jsonl"
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": int(time.time()), "kind": kind, "name": name,
                                "size": int(size or 0), "dur": round(float(dur or 0), 1),
                                "qual": qual, "mbps": round(mbps_val, 2), "note": extra},
                               ensure_ascii=False) + "\n")
    except Exception:                                          # noqa: BLE001
        pass
