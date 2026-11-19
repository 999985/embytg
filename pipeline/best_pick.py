#!/usr/bin/env python3
"""best_pick.py —— 一步到位选最优源（红线 1 与 3 的唯一实现处）。

排序规则（严格按用户口径，不要自己发明）：
    真 2160p(码率≥2.0)  >  达标 1080p(≥0.8)  >  其余兜底
    同档位内：HEVC/AV1 优于 H.264  >  码率更高

一票否决（红线 3）：
    标称 2160p 但实测/估算码率 < 2.0  → 直接丢，不参与排序，也不允许兜底

事前粗筛（红线 2）：
    size ÷ 参考时长 估算码率，明显低于阈值或时长异常（过短=预告片）先跳过，
    避免白白消耗下载额度。真正的判定仍以入库前 ffprobe 实测为准。

用法：
    from best_pick import rank_candidates, admit
    best = rank_candidates(cands)[0]
"""
from __future__ import annotations

FLOOR = {"2160p": 2.0, "1080p": 0.8, "720p": 0.4, "sd": 0.15}
CODEC_RANK = {"av1": 2, "hevc": 2, "h265": 2, "h.265": 2, "h264": 1, "h.264": 1, "avc": 1}
REF_SEC = 40 * 60            # 单集参考时长：40 分钟
MIN_SEC = 10 * 60            # 短于 10 分钟按预告片/花絮处理，拒绝


def _norm_codec(c):
    return str(c or "").strip().lower()


def estimate_mbps(size_bytes, duration_sec, ref_sec=REF_SEC):
    dur = float(duration_sec or 0) or float(ref_sec)
    if not size_bytes or dur <= 0:
        return 0.0
    return float(size_bytes) * 8 / dur / 1e6


def quality_class(label, mbps):
    """返回 'hq2160' / 'ok1080' / 'low' / 'fake'。fake 即红线 3 的一票否决。"""
    lab = str(label or "").lower()
    if "2160" in lab or "4k" in lab:
        return "hq2160" if mbps >= FLOOR["2160p"] else "fake"
    if "1080" in lab:
        return "ok1080" if mbps >= FLOOR["1080p"] else "low"
    if mbps >= FLOOR["1080p"]:
        return "ok1080"
    return "low"


def admit(cand, ref_sec=REF_SEC):
    """事前粗筛：能不能把它纳入候选池。返回 (ok, reason)。"""
    size = int(cand.get("size") or 0)
    if size <= 0:
        return False, "no_size"
    dur = float(cand.get("duration") or 0)
    if dur and dur < MIN_SEC:
        return False, "too_short(%.0fs)" % dur
    mbps = float(cand.get("mbps") or 0) or estimate_mbps(size, dur, ref_sec)
    cls = quality_class(cand.get("quality") or cand.get("label"), mbps)
    if cls == "fake":
        return False, "fake_4k(%.2fMbps)" % mbps
    return True, cls


def rank_key(cand, ref_sec=REF_SEC):
    """排序键，越大越好。"""
    size = int(cand.get("size") or 0)
    dur = float(cand.get("duration") or 0)
    mbps = float(cand.get("mbps") or 0) or estimate_mbps(size, dur, ref_sec)
    cls = quality_class(cand.get("quality") or cand.get("label"), mbps)
    tier = {"hq2160": 3, "ok1080": 2, "low": 1}.get(cls, 0)
    codec = CODEC_RANK.get(_norm_codec(cand.get("vcodec") or cand.get("codec")), 0)
    return (tier, codec, round(mbps, 3), int(size))


def rank_candidates(cands, ref_sec=REF_SEC, with_rejects=False):
    """过滤掉不允许的候选并按最优排序。"""
    kept, rejected = [], []
    for c in cands or []:
        ok, why = admit(c, ref_sec)
        if ok:
            kept.append(c)
        else:
            rejected.append((c, why))
    kept.sort(key=lambda c: rank_key(c, ref_sec), reverse=True)
    return (kept, rejected) if with_rejects else kept


def is_better(new, old, ref_sec=REF_SEC):
    """洗版判定：只在真·更高档或同档更高码率时才算升级（红线 5：P1 只收达标源）。"""
    return rank_key(new, ref_sec) > rank_key(old, ref_sec)
