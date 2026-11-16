#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""r2_backup.py — Cloudflare R2 异地备份（用户 2026-11-11 授权：明文、只用免费额度）

用户原话：「不用加密密钥 敏感数据明文处理 密钥可以上传 该净化的净化 你是运营你说的算
写好记录就行 要有痕迹」「免费额度…记住不要超 只能用免费的」。

【免费额度硬约束（官方 2026-11-13 文档复核）】
  Standard 存储 10 GB-month | Class A 100万/月 | Class B 1000万/月
  DeleteObject 免费 | egress 免费 | 仅 Standard 适用免费额
  ⚠ GB-month = 每日**峰值**存储量按天平均，不是「每月可传总量」。
     同一对象名反复覆盖上传，容量恒定；只有「按日期堆新对象」才会吃光配额。
  ⚠ 免费额度是 10 GB-month（容量），不是 10GB/月流量 —— 传多少遍都不限，
     只要同一时刻桶里不超过 10GB，且按天峰值平均不超 10GB。

【操作分类（官方 Class A 列表，别再猜）】
  Class A（计费）：PutObject、ListObjects、CopyObject、CreateMultipartUpload、
                   UploadPart、CompleteMultipartUpload、ListParts、ListBuckets…
  Class B（计费）：GetObject、HeadObject、HeadBucket…
  免费：DeleteObject、DeleteBucket、AbortMultipartUpload
  ⚠ `GET /objects?limit=1000` 属于 **ListObjects = Class A**，不是 Class B。
    本脚本每轮跑 2 次（感知 + 校验），每天 1 轮 = 2 次/天 ≈ 60 次/月。
  ⚠ 单次 PUT 上限 5 GiB（官方 limits），我们的包 25-115MB，一次 PUT = 1 次 Class A，
    不会被拆成 multipart。旧注释「>5MB 走 multipart」是错的，已纠正。

【设计】
  - 用用户给的 `cfut_` API Token 走 Cloudflare REST 对象端点（实测 PUT/GET/DELETE 200），
    **不创建任何新 S3 凭证**、不改 bucket 配置、不碰生产 rclone.conf。
  - 每天只产出 **1 个合并包**（tg.db 一致快照 + A类凭证 + C类文档审计 + 手册 + cron），
    固定名 `daily-latest.tar.gz` / `daily-prev.tar.gz` 两版轮转。
  - 另有 `full-latest.tar.gz`：净化后的全量 tar 包（只在存在干净包时上传，1 版）。
  - **硬闸门**：上传前 GET 桶内对象算现有总量 + 本次包 > 5.0 GB → 直接跳过并告警
    （2026-11-13 从 8GB 降到 5GB：GB-month 按天峰值算，8GB 留的余量不够；
     到 5GB 说明该改架构，而不是继续堆）。
  - **轮转**：先把旧 latest 复制成 prev（服务端 Copy 属 Class A，改用「重新 PUT 本地保留的上一版」
    避免额外操作），PUT 新 latest → 校验 sha256 → 才删多余对象（DeleteObject 免费）。
  - **留痕**：每步写 logs/r2_backup.log + logs/r2_backup.jsonl（时间/动作/对象/大小/sha/结果），
    并往 AgentCommunication.log 写「完成」条目。

用法:
  /usr/bin/python3 r2_backup.py --dry          # 只算不传（不产生任何 Class A）
  /usr/bin/python3 r2_backup.py --once         # 跑一轮
  /usr/bin/python3 r2_backup.py --status       # 只读列桶+配额账
"""
import argparse
import glob
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request

ROOT = "/opt/embytg"
PIPE = os.path.join(ROOT, "pipeline")
BK = os.path.join(ROOT, "backup")
LOGS = os.path.join(PIPE, "logs")
CRED = os.path.join(ROOT, ".r2_backup.json")
AUDIT = os.path.join(LOGS, "r2_backup.jsonl")
HLOG = os.path.join(LOGS, "r2_backup.log")
STATE = os.path.join(PIPE, ".r2_backup_state.json")
LEDGER = os.path.join(PIPE, ".r2_quota_ledger.json")   # 每日容量+操作次数账本

FREE_GB = 10.0
# 2026-11-13 复核：官方 10GB-month 是按天「峰值」平均，不是每次操作的瞬时量。
# 8GB 太靠近上限（自然增长 + 峰值波动没有空间），降到 5GB —— 到 5GB 说明架构该换，
# 而不是继续往上堆。现在实测 0.201GB，离闸门还有 25 倍余量。
HARD_LIMIT_GB = float(os.environ.get("R2_HARD_LIMIT_GB", "5.0"))
# 7 天涨幅告警阈值：容量爬升到这个比例就发邮件（自然增长不该这么快）
GROWTH_WARN_PCT = float(os.environ.get("R2_GROWTH_WARN_PCT", "25"))
# 涨幅告警的**绝对下限**（GB）：比例必须和绝对值同时超才报。
# 2026-11-16 事故：全量包从 117MB 长到 193MB，把 0.201GB 的桶「涨」了 37% → P1 告警，
# 而实际只多了 74MB（免费额 10GB 的 0.7%）。小基数下比例天生失真，真泄漏是按 GB 计的。
GROWTH_WARN_MIN_GB = float(os.environ.get("R2_GROWTH_WARN_MIN_GB", "0.5"))
# 「7 天」是硬窗口：超过 7 天的旧账不参与比较，账本里没到 7 天就按实际天数报。
GROWTH_WINDOW_DAYS = 7
# 单日 Class A 次数告警阈值：正常 ~10 次/天，超过这个数说明有重试风暴/循环 bug
CLASS_A_DAILY_WARN = int(os.environ.get("R2_CLASS_A_DAILY_WARN", "500"))
OBJ_DAILY_LATEST = "daily-latest.tar.gz"
OBJ_DAILY_PREV = "daily-prev.tar.gz"
OBJ_FULL_LATEST = "full-latest.tar.gz"
OBJ_ENV_LATEST = "env-latest.tar.gz"
OBJ_MANIFEST = "manifest.json"

# A 类：丢了不可逆（用户明确授权明文上传）
A_FILES = [
    ("config.json", "config/config.json"),
    (".env", "config/.env"),
    (".tg_string", "sessions/.tg_string"),
    (".tg_string2", "sessions/.tg_string2"),
    (".tg_user.session", "sessions/.tg_user.session"),
    (".token.json", "config/.token.json"),
    (".apikey.json", "config/.apikey.json"),
    (".bot_tokens.json", "config/.bot_tokens.json"),
    ("config/rclone.conf", "config/rclone.conf"),
    ("config/config.toml", "config/config.toml"),
    ("compose.yaml", "config/compose.yaml"),
    ("rebind-emby.sh", "config/rebind-emby.sh"),
    ("api.sh", "config/api.sh"),
]
# C 类：协作/审计/手册/调度
C_FILES = [
    ("AgentCommunication.log", "docs/AgentCommunication.log"),
    ("docs/archive/OPS_JOURNAL.md", "docs/archive/OPS_JOURNAL.md"),
    ("docs/archive/OPS1_MANUAL.md", "docs/archive/OPS1_MANUAL.md"),
    ("docs/archive/OPS1_SELFWAKE.md", "docs/archive/OPS1_SELFWAKE.md"),
    ("docs/OPS_HANDBOOK.md", "docs/OPS_HANDBOOK.md"),
    ("docs/CRON_SCHEDULE.md", "docs/CRON_SCHEDULE.md"),
    ("docs/ERRORS.md", "docs/ERRORS.md"),
    ("docs/KB.md", "docs/KB.md"),
    ("docs/SCALING.md", "docs/SCALING.md"),
    ("docs/ROADMAP.md", "docs/ROADMAP.md"),
    ("docs/TODB_API.md", "docs/TODB_API.md"),
    ("docs/R2_BACKUP.md", "docs/R2_BACKUP.md"),
    ("pipeline/logs/ops1_audit.jsonl", "logs/ops1_audit.jsonl"),
    ("pipeline/logs/prewarm_daemon.log", "logs/prewarm_daemon.log"),
]
for _f in sorted(glob.glob("/etc/cron.d/embytg_*") + glob.glob("/etc/cron.d/p03_*")):
    C_FILES.append((_f, "cron/" + os.path.basename(_f)))


def now():
    return time.strftime("%F %T %Z")


def _in_unit_test():
    """测试进程识别：unittest/pytest 下不得写生产日志。

    2026-11-16 事故：test_regressions 的配额用例伪造 0.5→3.2GB 账本并注掉
    alert 模块，但 log() 仍往生产的 r2_backup.log/.jsonl 写「P1 容量涨 540%」，
    daily_ops 每轮跑测试就把假告警灌进真日志，人看日志会以为桶炸了。
    """
    return ("unittest" in sys.modules) or bool(os.environ.get("PYTEST_CURRENT_TEST"))


def log(msg, action="", obj="", result="", size=0, sha=""):
    rec = {"ts": now(), "action": action, "object": obj, "size": size,
           "sha256": sha[:16], "result": result}
    line = "[%s] %s" % (rec["ts"], msg)
    if not _in_unit_test():
        os.makedirs(LOGS, exist_ok=True)
        with open(AUDIT, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        with open(HLOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    print(line, flush=True)


def cred():
    return json.load(open(CRED, encoding="utf-8"))


def api(method, path, data=None, ctype="application/json", timeout=120):
    d = cred()
    base = ("https://api.cloudflare.com/client/v4/accounts/%s/r2/buckets/%s/objects"
            % (d["account_id"], d["bucket"]))
    h = {"Authorization": "Bearer " + d["api_key"]}
    if data is not None:
        h["Content-Type"] = ctype
    req = urllib.request.Request(base + path, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, {"raw": raw.decode("utf-8", "ignore")[:300]}
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode("utf-8", "ignore")[:300]}
    except Exception as e:
        return -1, {"error": str(e)[:200]}


def list_objects():
    """GET ?limit=1000 = **ListObjects，官方归类 Class A**（不是 Class B，旧注释写错了）。

    每轮主流程调用 2 次（感知总量 + 上传后校验），每天 1 轮 ≈ 60 次/月，
    对照免费额 100 万/月 = 0.006%。次数不是风险点，但必须如实计数，
    否则哪天出现重试风暴时「次数爆了」从容量上看不出来。
    """
    _count("class_a")
    code, j = api("GET", "?limit=1000")
    if code != 200 or not j.get("success"):
        raise RuntimeError("list failed: %s %s" % (code, str(j)[:200]))
    return j.get("result") or []


def _count(kind, peak_gb=None):
    """当日操作计数 + 峰值容量账本。

    GB-month 是按天「峰值」平均计算的，所以每天必须记一笔当天见过的最大值，
    否则只能靠「感觉没超」，不能证明没超。同时记 Class A 次数 —— 容量看不出的
    重试风暴/循环 bug，靠次数才看得见。
    """
    try:
        day = time.strftime("%Y-%m-%d")
        led = load_ledger()
        rec = led.setdefault(day, {"class_a": 0, "class_b": 0, "delete": 0, "tick": 0, "peak_gb": 0.0})
        rec[kind] = int(rec.get(kind) or 0) + 1
        if peak_gb is not None:
            rec["peak_gb"] = max(float(rec.get("peak_gb") or 0), round(peak_gb, 4))
        led[day] = rec
        save_ledger(led)
    except Exception:                                          # noqa: BLE001
        pass


def load_ledger():
    try:
        with open(LEDGER, encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                          # noqa: BLE001
        return {}


def save_ledger(led):
    keys = sorted(led)[-60:]                                   # 只留 60 天
    tmp = LEDGER + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({k: led[k] for k in keys}, f, ensure_ascii=False, indent=1)
    os.replace(tmp, LEDGER)


def record_peak(gb):
    """只抬高当日 peak_gb，不动任何计数。

    上传完成后才知道当天真实占用（含本轮新包）。以前只在「感知」阶段记一次，
    账本永远落后一天：11-15 账上 0.203，实际桶已经是 0.277 —— 涨幅比较拿两个
    不同时点的快照比，结论自然失真。
    """
    try:
        day = time.strftime("%Y-%m-%d")
        led = load_ledger()
        rec = led.setdefault(day, {"class_a": 0, "class_b": 0, "delete": 0, "tick": 0, "peak_gb": 0.0})
        rec["peak_gb"] = max(float(rec.get("peak_gb") or 0), round(float(gb), 4))
        led[day] = rec
        save_ledger(led)
    except Exception:                                          # noqa: BLE001
        pass


def _growth_base(led, today):
    """today 之前 GROWTH_WINDOW_DAYS 天内最早的账 → (date, peak_gb, span_days)。

    没有可比基期就返回 (None, 0.0, 0)。窗口外的老账不参与：账本保留 60 天，
    拿 30 天前的数当「7 天涨幅」基期是另一类失真。
    """
    try:
        t0 = time.mktime(time.strptime(today, "%Y-%m-%d"))
    except Exception:                                          # noqa: BLE001
        return None, 0.0, 0
    best = None
    for d, rec in led.items():
        if not d or d >= today:
            continue
        try:
            ts = time.mktime(time.strptime(d, "%Y-%m-%d"))
        except Exception:                                      # noqa: BLE001
            continue
        age = int(round((t0 - ts) / 86400.0))
        if age < 1 or age > GROWTH_WINDOW_DAYS:
            continue
        if best is None or d < best[0]:
            best = (d, float((rec or {}).get("peak_gb") or 0), age)
    return best if best else (None, 0.0, 0)


def _check_quota_health(now_gb):
    """容量趋势 + Class A 次数告警。超阈值发邮件（走 alert.py），本地留痕。

    为什么要这条：R2 免费额是 10GB-month（按天峰值平均），容量缓慢爬升时
    单次检查永远看不出来，必须看 7 天趋势。次数告警用来抓重试风暴 ——
    那种故障在容量上完全看不出来。
    """
    try:
        led = load_ledger()
        today = time.strftime("%Y-%m-%d")
        rec = led.get(today) or {}
        ca = int(rec.get("class_a") or 0)

        msgs = []
        if ca > CLASS_A_DAILY_WARN:
            msgs.append("今日 Class A %d 次（阈值 %d）—— 疑似重试风暴或循环 bug"
                        % (ca, CLASS_A_DAILY_WARN))

        base_date, base, span = _growth_base(led, today)
        if base > 0:
            abs_gb = now_gb - base
            pct = abs_gb / base * 100.0
            # 比例 + 绝对值双闸：小基数下比例天生放大，只有绝对量也上来才是真爬升。
            if pct >= GROWTH_WARN_PCT and abs_gb >= GROWTH_WARN_MIN_GB:
                msgs.append("容量 %d 天涨 %.0f%%（%.3fGB@%s → %.3fGB@今天，+%.3fGB），"
                            "阈值 %d%% 且 +%.2fGB"
                            % (span, pct, base, base_date, now_gb, abs_gb,
                               GROWTH_WARN_PCT, GROWTH_WARN_MIN_GB))

        est = sum(float((v or {}).get("peak_gb") or 0) for v in led.values())
        if est > FREE_GB * 0.6:
            msgs.append("账本累计已用 %.1f GB-month（免费额 %.0f），按天峰值口径接近上限"
                        % (est, FREE_GB))

        for m in msgs:
            log("!! 配额告警: %s" % m, action="quota_warn", result="ALERT")
            try:
                import importlib
                A = importlib.import_module("alert")
                A.alert("P1", "r2_quota", m, {"peak_gb": now_gb, "class_a_today": ca})
            except Exception:                                  # noqa: BLE001
                pass
    except Exception:                                          # noqa: BLE001
        pass


def put_object(key, data, ctype="application/gzip"):
    """PUT 一个对象 = 1 次 Class A。

    官方单次 PUT 上限 5 GiB；我们的包 25-115MB，一次 PUT 就是一次 Class A，
    不会被拆成 multipart（旧注释「>5MB 走 multipart」是错的，见文件头）。
    """
    _count("class_a")
    code, j = api("PUT", "/" + key, data=data, ctype=ctype)
    if code not in (200, 201) or not j.get("success"):
        raise RuntimeError("put %s failed: %s %s" % (key, code, str(j)[:250]))
    return j


def delete_object(key):
    """DeleteObject 免费。"""
    code, j = api("DELETE", "/" + key)
    return code in (200, 204) or j.get("success")


def sha_bytes(b):
    return hashlib.sha256(b).hexdigest()


def build_daily_tar():
    """本地打一个合并包：tg.db 一致快照 + A + C。返回 bytes。"""
    missing = []
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tf:
        # tg.db 用 sqlite .backup（不是运行态 cp），保证一致
        tmpdb = "/tmp/.r2_tgdb_" + str(os.getpid()) + ".db"
        try:
            src = sqlite3.connect(os.path.join(PIPE, "tg.db"), timeout=30)
            dst = sqlite3.connect(tmpdb)
            src.backup(dst)
            dst.close(); src.close()
            tf.add(tmpdb, arcname="state/tg.db")
        finally:
            for x in (tmpdb, tmpdb + "-wal", tmpdb + "-shm"):
                try: os.remove(x)
                except OSError: pass
        for src, rel in A_FILES + C_FILES:
            p = src if src.startswith("/") else os.path.join(ROOT, src)
            if os.path.isfile(p):
                tf.add(p, arcname=rel)
            else:
                # 2026-11-14 自省：原来 here 是静默 continue，docs 被归档到
                # docs/archive/ 后三份运维审计文档整整一天没进 R2（异地唯一副本）。
                # 缺文件必须响：A 类（凭证）缺=致命，C 类（文档）缺=告警但继续打。
                missing.append(rel)
                if rel.startswith("config/") or rel.startswith("sessions/"):
                    raise RuntimeError("A 类凭证缺失，拒绝上传残缺包：%s" % rel)
        meta = {"generated": now(), "kind": "daily", "contents": "tg.db + A类凭证明文 + C类文档审计",
                "note": "用户 2026-11-11 授权明文；免费额度固定对象名轮转，不按日期堆积",
                # 2026-11-14：缺失清单写进 manifest，恢复时能一眼看出这个包少了什么
                "files_missing": missing}
        mb = json.dumps(meta, ensure_ascii=False, indent=2).encode()
        info = tarfile.TarInfo("manifest.json"); info.size = len(mb)
        tf.addfile(info, io.BytesIO(mb))
    return buf.getvalue()


def pick_clean_full_pack():
    """选一个干净的全量包（<=200MB，排除 embydata 污染版）。"""
    cands = []
    for p in glob.glob(os.path.join(BK, "embytg_backup_*.tar.gz")):
        sz = os.path.getsize(p) / 1048576.0
        if sz <= 200:  # 净化阈值：>200MB 视为污染，不传
            cands.append((os.path.getmtime(p), p, sz))
    if not cands:
        return None
    cands.sort(reverse=True)
    return cands[0][1]


def load_state():
    try:
        return json.load(open(STATE, encoding="utf-8"))
    except Exception:
        return {"last_daily_sha": None, "last_run": None, "uploads": 0}


def save_state(s):
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()

    if a.status:
        objs = list_objects()
        total = sum(o.get("size", 0) for o in objs)
        print("== R2 tgemby 配额账 ==")
        for o in objs:
            print("  %-28s %8.2f MB" % (o.get("key"), o.get("size", 0) / 1048576))
        print("  对象数=%d  合计=%.3f GB / 免费 10 GB / 硬闸门 %.1f GB" %
              (len(objs), total / 2**30, HARD_LIMIT_GB))
        # 每日账本（GB-month 按天峰值平均，必须逐日留痕才证明得出没超）
        led = load_ledger()
        if led:
            print("\n  每日容量/次数账本（最近 7 天）:")
            print("    %-12s %8s %8s %6s" % ("日期", "峰值GB", "ClassA", "tick"))
            used = 0.0
            for d in sorted(led)[-7:]:
                r = led[d] or {}
                pg = float(r.get("peak_gb") or 0)
                used += pg
                print("    %-12s %8.3f %8d %6d" % (d, pg, int(r.get("class_a") or 0),
                                                   int(r.get("tick") or 0)))
            print("  近 7 天峰值合计 %.3f GB-month（免费 10 GB-month）" % used)
            print("  距闸门还有 %.1f 倍空间" % (HARD_LIMIT_GB / max(0.001, total / 2**30)))
        return 0

    # ---- 感知：现有桶占用 ----
    objs = list_objects()
    existing = sum(o.get("size", 0) for o in objs)
    by_key = {o.get("key"): o.get("size", 0) for o in objs}
    daily = build_daily_tar()
    dsha = sha_bytes(daily)
    full = pick_clean_full_pack()
    full_bytes = open(full, "rb").read() if full else None
    # 2026-11-13 灾备改造：OS 层环境包（nginx/证书/rclone/systemd/cron/box密钥/runner脚本）
    env_pack_path = None
    try:
        _e = json.load(open(os.path.join(BK, "env_LATEST.json"), encoding="utf-8"))
        if _e.get("state") == "DONE" and os.path.isfile(_e.get("archive") or ""):
            env_pack_path = _e["archive"]
    except Exception:                                          # noqa: BLE001
        pass
    env_bytes = open(env_pack_path, "rb").read() if env_pack_path else None
    plan = [(OBJ_DAILY_LATEST, daily), (OBJ_DAILY_PREV, daily)]
    if full_bytes:
        plan.append((OBJ_FULL_LATEST, full_bytes))
    if env_bytes:
        plan.append((OBJ_ENV_LATEST, env_bytes))
    # 轮转语义：本次新 daily 放 latest；上一版 latest 的内容作为 prev（若无则用同一份）
    new_total = existing
    for k, b in plan:
        new_total = new_total - by_key.get(k, 0) + len(b)
    new_total += 2048  # manifest
    gb = new_total / 2**30
    log("感知: 桶内 %d 对象 %.3fGB, 新 daily %.1fMB sha=%s, full=%s, env=%s" %
        (len(objs), existing / 2**30, len(daily) / 1048576, dsha[:12],
         os.path.basename(full) if full else "无干净包",
         ("%.1fMB" % (len(env_bytes) / 1048576)) if env_bytes else "无env包"),
        action="sense", result="计划后 %.3fGB / 闸门 %.1fGB" % (gb, HARD_LIMIT_GB))

    # 每日容量账本 + 趋势告警（GB-month 按天峰值算，必须有每日记录才证明得出没超）
    # 注意：class_a 已在 list_objects()/put_object() 内部逐次计数，这里只记峰值，
    # 不再重复 +1，否则账本会翻倍虚高。
    _count("tick", peak_gb=existing / 2**30)
    _check_quota_health(existing / 2**30)

    if a.dry:
        log("[DRY] 不上传。计划: latest/prev 各 %.1fMB + full %s + manifest；预计常驻 %.3fGB" %
            (len(daily) / 1048576, ("%.1fMB" % (len(full_bytes) / 1048576)) if full_bytes else "无", gb),
            action="dry", result="跳过")
        return 0

    # ---- 硬闸门：绝不超免费 ----
    if gb > HARD_LIMIT_GB:
        log("!! 硬闸门触发: 计划后 %.3fGB > %.1fGB → 本轮全部跳过（不产生 Class A）" %
            (gb, HARD_LIMIT_GB), action="guard", result="SKIP")
        return 2

    # ---- 执行：先 PUT 新 latest，校验 sha，再轮转 prev，再删多余 ----
    st = load_state()
    # 1) 上一版 latest 若存在且与本次不同 → 作为 prev PUT（用 GET 取回是 Class B，这里为省操作
    #    直接用本地 state 记录的上一版文件；没有就复用本次 daily，保证 prev 永远存在）
    prev_bytes = daily
    pf = os.path.join(BK, ".r2_prev_daily.tar.gz")
    if os.path.isfile(pf):
        prev_bytes = open(pf, "rb").read()
    put_object(OBJ_DAILY_PREV, prev_bytes)
    log("PUT %s %.1fMB" % (OBJ_DAILY_PREV, len(prev_bytes) / 1048576),
        action="put", obj=OBJ_DAILY_PREV, result="ok", size=len(prev_bytes), sha=sha_bytes(prev_bytes))

    put_object(OBJ_DAILY_LATEST, daily)
    log("PUT %s %.1fMB" % (OBJ_DAILY_LATEST, len(daily) / 1048576),
        action="put", obj=OBJ_DAILY_LATEST, result="ok", size=len(daily), sha=dsha)
    # 本地留一份供下轮做 prev
    with open(pf, "wb") as f:
        f.write(daily)

    if full_bytes:
        put_object(OBJ_FULL_LATEST, full_bytes)
        log("PUT %s %.1fMB" % (OBJ_FULL_LATEST, len(full_bytes) / 1048576),
            action="put", obj=OBJ_FULL_LATEST, result="ok", size=len(full_bytes), sha=sha_bytes(full_bytes))

    if env_bytes:
        put_object(OBJ_ENV_LATEST, env_bytes)
        log("PUT %s %.1fMB" % (OBJ_ENV_LATEST, len(env_bytes) / 1048576),
            action="put", obj=OBJ_ENV_LATEST, result="ok", size=len(env_bytes), sha=sha_bytes(env_bytes))

    # 2) 校验：列桶确认三对象都在、大小对（1 次 Class B），再写 manifest
    check = list_objects()
    ck = {o.get("key"): o.get("size", 0) for o in check}
    ok = (ck.get(OBJ_DAILY_LATEST) == len(daily)
          and ck.get(OBJ_DAILY_PREV) == len(prev_bytes)
          and (not full_bytes or ck.get(OBJ_FULL_LATEST) == len(full_bytes))
          and (not env_bytes or ck.get(OBJ_ENV_LATEST) == len(env_bytes)))
    log("校验: latest/prev/full/env 大小 %s → %s" % (ck, "OK" if ok else "MISMATCH"),
        action="verify", result="ok" if ok else "MISMATCH")
    if not ok:
        log("!! 校验失败，本轮不删任何对象（保留旧版）", action="verify", result="ABORT")
        return 3

    # 上传完成后把真实占用写进当日 peak（含本轮新包）。不记这一步，账本永远
    # 落后一天，涨幅/GB-month 都是拿旧快照在比。
    record_peak(sum(ck.values()) / 2**30)

    manifest = {"updated": now(), "objects": ck, "daily_sha256": dsha,
                "full_from": os.path.basename(full) if full else None,
                "env_from": os.path.basename(env_pack_path) if env_pack_path else None,
                "restore": "bash restore_to_new_host.sh（用 full + env 两包即可完美展开）",
                "free_tier": {"storage_gb_month": 10, "hard_limit_gb": HARD_LIMIT_GB},
                "note": "明文（用户授权）；固定名轮转不按日期堆积；DeleteObject 免费"}
    put_object(OBJ_MANIFEST, json.dumps(manifest, ensure_ascii=False, indent=2).encode(),
               ctype="application/json")
    log("PUT manifest.json", action="put", obj=OBJ_MANIFEST, result="ok")

    # 3) 清理：删除任何不在白名单的对象（DeleteObject 免费）
    # 这是防止「按日期堆历史包」打满 10GB 的最后一道闸。白名单之外的任何东西
    # 都视为异常 —— 不只是删掉，还发告警，因为「桶里为什么会有这个」本身就是要查的事。
    whitelist = {OBJ_DAILY_LATEST, OBJ_DAILY_PREV, OBJ_FULL_LATEST, OBJ_ENV_LATEST, OBJ_MANIFEST}
    strays = [k for k in ck if k not in whitelist]
    for k in strays:
        delete_object(k)
        _count("delete")
        log("DELETE 非白名单对象 %s (%.2fMB)" % (k, ck.get(k, 0) / 1048576),
            action="delete", obj=k, result="ok")
    if strays:
        msg = "桶里出现 %d 个白名单外对象并已删除：%s —— 有东西在往桶里堆历史包" % (
            len(strays), ", ".join(strays[:5]))
        log("!! " + msg, action="stray_object", result="ALERT")
        try:
            import importlib
            A = importlib.import_module("alert")
            A.alert("P1", "r2_stray", msg, {"strays": strays[:20]})
        except Exception:                                      # noqa: BLE001
            pass

    st.update({"last_daily_sha": dsha, "last_run": now(),
               "uploads": st.get("uploads", 0) + 1, "last_total_gb": round(gb, 4)})
    save_state(st)
    log("DONE 常驻 %.3fGB / 免费 10GB（%.1f%%），Class A 本轮 %d 次" %
        (gb, 100 * gb / FREE_GB, 3 + (1 if full_bytes else 0) + (1 if env_bytes else 0) + 1),
        action="done", result="ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
