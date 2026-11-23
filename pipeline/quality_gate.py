#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""quality_gate.py —— 画质闸门：不是「分辨率够高」就叫好片。

## 为什么需要它

2026-11-19 用户反馈「谷雨的质量很低，都是 300-400MB」。
实测那批文件确实标着 2160p（3840x1600 HEVC），但单集只有 345-435MB ——
按 45 分钟算约 1.0-1.6 Mbps，是**压得很狠的高分辨率垃圾**。
原来所有判据只看 width/height，所以这种片一路绿灯入库。

## 两条线必须分开（2026-11-19 修订）

    STOCK 存量洗版线（宽）：2160p < 2.0 / 1080p < 0.8  → 只用来判定「这集要不要进 P1」
    ADMIT 新源准入线：2160p ≥ 2.0 / 1080p ≥ 2.0     → 新入库与洗版交付
    （2026-11-23 由 4.0 下调：源池 4K p50=3.26，4.0 不可达；且与「2160p≥2.0 达标」
      口径一致，避免新片阶段拒 3.2Mbps 的 4K、当晚 upgrade 又收进来）

把 ADMIT 用在存量上：库内 4K 码率中位只有 3.88Mbps（实测 209 集样本），
一半存量会被塞进 P1（1443 → 3000+），永远洗不完。
把 STOCK 用在新入库上：等于没有把关，谷雨那种片照样进来。

## 分辨率一律 OR 判档（不能用单一维度）

    width ≥ 3840  或  height ≥ 2160  → 2160p
    width ≥ 1920  或  height ≥ 1080  → 1080p

两个方向都会误杀，实测数据：
  * 宽银幕 4K 是 3840x1608/1606/1632（2.39:1 遮幅），只按 height 会掉到 720p —— 库里有 3400+ 集
  * 4:3 老片是 1440x1080，只按 width 会掉到 720p

## 假 4K 一票否决

2160p 但 < 2.0 Mbps：任何线都不收、不当达标（既不做首收也不做洗版交付）。

## 码率来源

台账 `resource_state.dur_sec` 全库 0 行有值（实测），不走台账。
优先级：Emby `MediaSources[0].Bitrate`（实测）> Size/RunTimeTicks > Size/估算时长。
Emby 对新入库文件的 Bitrate 有延迟：空值时**不要直接否决**，先触发探测重试，
仍无值再降级 Size ÷ 时长估算。

## 用法

    import quality_gate as QG
    QG.tier(3840, 1608)                       # -> "2160p"
    QG.judge(3840, 1608, 1.8, "stock")        # -> {"ok": False, "reason": "fake_4k", ...}
    QG.judge(1920, 1080, 2.4, "admit")        # -> {"ok": True, ...}
    QG.judge_emby_item(item, "admit", probe)  # Emby 条目直判，自动处理 Bitrate 延迟
    QG.too_low(size, dur, "2160p")            # 向后兼容：存量线，(是否太差, mbps)

自检：python3 quality_gate.py --selfcheck
"""
import json
import os
import sys
import time

sys.path.insert(0, "/opt/embytg/pipeline")

# ── 阈值（两条线，务必不要混用）────────────────────────────────────────────
# 2026-11-23：准入线做成环境变量可覆盖 —— 源池质量是外生的，改线不该改代码。
#   QG_ADMIT_2160=4.0  QG_ADMIT_1080=2.0  （实测源池 4K 候选 p50=3.26 / p90=3.76，
#   4.0 在当前源池几乎不可达，调低只需改这个变量，不用动 gha_next3）
def _env_float(name, default):
    try:
        return float(os.environ.get(name) or default)
    except (TypeError, ValueError):
        return float(default)


STOCK_MIN_MBPS = {"2160p": 2.0, "1080p": 0.8, "720p": 0.4}   # 低于此值 = 该洗
STOCK_FLOOR_DEFAULT = 0.6
# 2026-11-23：2160p 准入线 4.0 → 2.0，与「2160p ≥2.0Mbps 达标」口径一致。
# 原来新片准入要 4.0，而 upgrade 收 2.0 —— 同一部 3.2Mbps 的 4K：
# 新片阶段被拒（于是落 1080p），当晚 upgrade 又把它收进来。等于自己制造返工。
ADMIT_MIN_MBPS = {"2160p": _env_float("QG_ADMIT_2160", 2.0),
                  "1080p": _env_float("QG_ADMIT_1080", 2.0)}   # 达到此值 = 可交付
FLOOR_MBPS = 2.0                                              # 假 4K 一票否决线

RANK = {"2160p": 4, "1440p": 3, "1080p": 2, "720p": 1, "sd": 0}


# ── 分辨率判档（OR 逻辑）──────────────────────────────────────────────────
def tier(width, height):
    """按 OR 逻辑判档 → 2160p / 1440p / 1080p / 720p / sd。"""
    w, h = int(width or 0), int(height or 0)
    if w >= 3840 or h >= 2160:
        return "2160p"
    if w >= 2560 or h >= 1440:
        return "1440p"
    if w >= 1920 or h >= 1080:
        return "1080p"
    if w >= 1280 or h >= 720:
        return "720p"
    return "sd"


def qual_from_dims(w, h):
    """向后兼容入口（老调用方用这个名字），返回 2160p/1080p/720p/SD。"""
    t = tier(w, h)
    return "SD" if t == "sd" else t


def rank_of_dims(width, height):
    """给 gha_next3._rank_dims 用的兼容入口。"""
    return RANK[tier(width, height)]


def rank_of_label(label):
    s = str(label or "").strip().lower()
    for k in ("2160p", "1440p", "1080p", "720p"):
        if s.startswith(k):
            return RANK[k]
    return RANK["sd"]


# ── 码率 ─────────────────────────────────────────────────────────────────
def mbps(size_bytes, dur_sec):
    """向后兼容：体积 + 时长 → Mbps。取不到返回 0.0。"""
    if not size_bytes or not dur_sec:
        return 0.0
    return float(size_bytes) * 8.0 / float(dur_sec) / 1e6


def mbps_or_none(size_bytes, dur_sec):
    """同上，但取不到返回 None（区分「0 码率」与「不知道」）。"""
    try:
        s, d = float(size_bytes or 0), float(dur_sec or 0)
    except (TypeError, ValueError):
        return None
    if s <= 0 or d <= 0:
        return None
    return s * 8 / d / 1e6


# 拿不到时长时的兜底估算（秒），按题材给经验值，可用环境变量覆盖
DEFAULT_DURATION = {
    "动漫": 24 * 60, "国漫": 24 * 60, "动画": 24 * 60,
    "国剧": 45 * 60, "国产剧": 45 * 60, "日韩剧": 45 * 60, "欧美剧": 45 * 60,
    "综艺": 90 * 60, "纪录": 50 * 60, "电影": 100 * 60,
}
FALLBACK_DURATION_SEC = int(os.environ.get("QG_FALLBACK_DURATION", 40 * 60))


def estimated_duration(*keys):
    for key in keys:
        if not key:
            continue
        for name, sec in DEFAULT_DURATION.items():
            if name in str(key):
                return sec
    return FALLBACK_DURATION_SEC


def bitrate_of_emby_item(item, probe=None, retries=2, delay=2.0):
    """Emby 条目 → (mbps, source)。

    Bitrate 为空/0 时先触发探测（probe 回调，通常是 POST /Items/{id}/Refresh），
    重试仍无值则降级 Size ÷ 时长。**空值不等于不达标，不许直接否决。**
    source ∈ {'emby','size/runtime','size/est','unknown'}
    """
    def _src():
        ms = item.get("MediaSources") or [{}]
        return ms[0] or {}

    ms = _src()
    br = ms.get("Bitrate") or item.get("Bitrate") or 0
    for i in range(max(0, int(retries))):
        if br:
            break
        if probe:
            try:
                probe(item)
            except Exception:            # noqa: BLE001 —— 探测失败不影响判定链
                pass
        time.sleep(delay * (i + 1))
        br = _src().get("Bitrate") or 0
    if br:
        return br / 1e6, "emby"

    size = _src().get("Size") or item.get("Size") or 0
    rt = item.get("RunTimeTicks") or _src().get("RunTimeTicks") or 0
    if size and rt:
        v = mbps_or_none(size, float(rt) / 1e7)
        if v:
            return v, "size/runtime"
    if size:
        v = mbps_or_none(size, estimated_duration(item.get("LibraryName"), item.get("Type")))
        if v:
            return v, "size/est"
    return None, "unknown"


# ── 判定 ─────────────────────────────────────────────────────────────────
def judge(width, height, mbps_val, line="admit"):
    """核心判定。

    line='admit' 新源准入：2160p≥2.0 / 1080p≥2.0（2026-11-23 起）
    line='stock' 存量洗版（宽）：低于 2160p<2.0 / 1080p<0.8 才算需要洗

    返回 dict(tier, rank, ok, reason, mbps, line)。
    """
    t = tier(width, height)
    out = {"tier": t, "rank": RANK[t], "line": line, "ok": False, "reason": "",
           "mbps": None if mbps_val is None else round(float(mbps_val), 2)}
    if mbps_val is None:
        out["reason"] = "bitrate_unknown"      # 不知道 ≠ 否决
        return out
    m = float(mbps_val)

    # 假 4K 一票否决：任何线都不接受
    if t == "2160p" and m < FLOOR_MBPS:
        out["reason"] = "fake_4k"
        return out

    if line == "stock":
        need = STOCK_MIN_MBPS.get(t)
        if need is None:
            out["reason"] = "below_1080p"
            return out
        out["ok"] = m < need                    # ok = 需要洗版
        out["reason"] = "needs_upgrade" if out["ok"] else "stock_ok"
        return out

    need = ADMIT_MIN_MBPS.get(t)
    if need is None:
        out["reason"] = "below_1080p"
        return out
    out["ok"] = m >= need
    out["reason"] = "" if out["ok"] else "below_admit_line"
    return out


def judge_emby_item(item, line="admit", probe=None, retries=2):
    ms = (item.get("MediaSources") or [{}])[0] or {}
    w = item.get("Width") or ms.get("Width") or 0
    h = item.get("Height") or ms.get("Height") or 0
    m, src = bitrate_of_emby_item(item, probe=probe, retries=retries)
    res = judge(w, h, m, line=line)
    res["bitrate_source"] = src
    return res


# ── 候选预筛（下载前，只有体积没有实测）──────────────────────────────────
def judge_candidate(size_bytes, width, height, dur_sec, line="admit"):
    return judge(width, height, mbps_or_none(size_bytes, dur_sec), line=line)


def candidate_verdict(c, item=None, line="admit"):
    """把 gha_next3 的候选 dict 喂进来（不改 c）。"""
    q = c.get("quality") or {}
    w, h = q.get("width") or 0, q.get("height") or 0
    if not (w or h):                        # 质量分级没给像素时按标签兜底
        r = rank_of_label(q.get("label"))
        w, h = {4: (3840, 2160), 3: (2560, 1440),
                2: (1920, 1080), 1: (1280, 720)}.get(r, (0, 0))
    dur = 0
    for k in ("duration", "runtime", "duration_sec"):
        try:
            dur = float((c.get("episode") or {}).get(k) or q.get(k) or c.get(k) or 0)
        except (TypeError, ValueError):
            dur = 0
        if dur:
            break
    if not dur and item:
        try:
            dur = float(item.get("runtime") or 0) * 60
        except (TypeError, ValueError):
            dur = 0
    if not dur:
        dur = estimated_duration(item.get("lib") if item else None)
    # 2026-11-20：单位护栏。Emby 的 RunTimeTicks 是 100ns（1e7/秒），
    # 一旦上游把原始 ticks 当秒传进来，码率会算出 0.00 → 真 4K 被
    # 「假 4K 一票否决」。单集/单部电影不可能超过 24 小时，
    # 所以 >86400 只可能是「不是秒」，按 ticks 换算。
    if dur > 86400:
        dur = dur / 10000000.0
    return judge_candidate(c.get("size") or 0, w, h, dur, line=line)


# ── 向后兼容：存量线（tgpipe.py / ysdq_auto.py 在用）──────────────────────
def too_low(size_bytes, dur_sec, qual):
    """(是否太差, mbps)。时长未知按「不判」处理，宁可放过不误杀。"""
    if not dur_sec:
        return False, 0.0
    m = mbps(size_bytes, dur_sec)
    return m < STOCK_MIN_MBPS.get(qual, STOCK_FLOOR_DEFAULT), m


# ── 记账 ─────────────────────────────────────────────────────────────────
def log_reject(kind, name, size, dur, qual, mbps_val, extra=""):
    """被闸门拦下的都留痕，方便事后看「是不是误杀」。"""
    p = "/opt/embytg/pipeline/logs/quality_gate.jsonl"
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": int(time.time()), "kind": kind, "name": name,
                                "size": int(size or 0), "dur": round(float(dur or 0), 1),
                                "qual": qual, "mbps": round(float(mbps_val or 0), 2),
                                "note": extra}, ensure_ascii=False) + "\n")
    except Exception:                                          # noqa: BLE001
        pass


def log_fallback(where, detail):
    """无高画质源兜底入库的记账（不计入达标产出）。"""
    p = os.environ.get("QG_FALLBACK_LOG", "/opt/embytg/pipeline/logs/quality_fallback.jsonl")
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        rec = {"ts": int(time.time()), "where": where}
        if isinstance(detail, dict):
            rec.update(detail)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:                                          # noqa: BLE001
        pass


# ── 自检 ─────────────────────────────────────────────────────────────────
def _selfcheck():
    bad = []

    def chk(cond, msg):
        if not cond:
            bad.append(msg)

    # 1) OR 判档：两个方向都不能误杀
    chk(tier(3840, 1608) == "2160p", "3840x1608 应判 2160p（宽银幕 4K）")
    chk(tier(1440, 1080) == "1080p", "1440x1080 应判 1080p（4:3 老片）")
    chk(tier(1920, 800) == "1080p", "1920x800 应判 1080p（遮幅）")
    chk(tier(1280, 720) == "720p", "1280x720 应判 720p")
    chk(tier(3840, 2160) == "2160p", "3840x2160 应判 2160p")

    # 2) 两条线分开
    chk(judge(3840, 2160, 3.0, "stock")["ok"] is False, "4K@3.0 存量已达标，不该进 P1")
    chk(judge(3840, 2160, 3.0, "admit")["ok"] is False, "4K@3.0 不够准入线 4.0")
    chk(judge(3840, 2160, 4.5, "admit")["ok"] is True, "4K@4.5 应准入")
    chk(judge(1920, 1080, 1.0, "stock")["ok"] is False, "1080p@1.0 存量已达标（线 0.8）")
    chk(judge(1920, 1080, 1.0, "admit")["ok"] is False, "1080p@1.0 不够准入线 2.0")
    chk(judge(1920, 1080, 2.4, "admit")["ok"] is True, "1080p@2.4 应准入")
    chk(judge(1280, 720, 5.0, "admit")["ok"] is False, "720p 永不满足准入线")

    # 3) 假 4K 一票否决（两条线都一样）
    for ln in ("stock", "admit"):
        r = judge(3840, 1608, 1.8, ln)
        chk(r["ok"] is False and r["reason"] == "fake_4k", "假4K 在 %s 线必须否决" % ln)

    # 4) 码率未知不等于否决
    chk(judge(1920, 1080, None, "admit")["reason"] == "bitrate_unknown", "码率未知应标记")

    # 5) Bitrate 延迟：空 → probe 重试 → 仍空降级 Size/Runtime
    calls = {"n": 0}
    item = {"Width": 1920, "Height": 1080, "RunTimeTicks": 45 * 60 * 10_000_000,
            "MediaSources": [{"Size": 700 * 1024 * 1024}]}

    def probe(_it):
        calls["n"] += 1

    m, src = bitrate_of_emby_item(item, probe=probe, retries=2, delay=0)
    chk(calls["n"] == 2 and src == "size/runtime", "空 Bitrate 应重试 2 次再降级")
    expect = 700 * 1024 * 1024 * 8 / (45 * 60) / 1e6
    chk(m is not None and abs(m - expect) < 0.01, "降级码率计算应正确")

    m2, src2 = bitrate_of_emby_item(
        {"MediaSources": [{"Bitrate": 2500000}]}, retries=0)
    chk(m2 is not None and abs(m2 - 2.5) < 1e-6 and src2 == "emby", "有 Bitrate 时直接用实测")

    # 6) 向后兼容：too_low 走存量线，时长未知不误杀
    chk(too_low(300 * 1024 * 1024, 45 * 60, "2160p")[0] is True, "300MB/45min 的 4K 应判太差")
    chk(too_low(0, 0, "2160p")[0] is False, "时长未知不应判太差")

    if bad:
        print("selfcheck FAIL:")
        for b in bad:
            print("  -", b)
        return False
    print("selfcheck: PASS (%d 条断言)" % 18)
    return True


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--selfcheck":
        sys.exit(0 if _selfcheck() else 1)
    if len(args) >= 3 and "x" in args[0]:
        w, h = (int(x) for x in args[0].split("x", 1))
        line = args[2] if len(args) > 2 else "admit"
        print(json.dumps(judge(w, h, float(args[1]), line), ensure_ascii=False))
    else:
        sys.exit(0 if _selfcheck() else 1)
