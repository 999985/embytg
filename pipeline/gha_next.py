#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gha_next.py — 给 GitHub Actions 领取一个“确定缺集”的上片任务。

只复用现有轮子：
  autoingest.py  缺口队列 / 三重防重 / 失败记忆 / 目标命名
  source_pull.py 源服解析（Hills UA / 单源并发 1 / Guard 审计）
  src_guard.py   请求级规则

不下载媒体；只把源直链和登记字段输出给 runner。stdout 必须只有 JSON，
由 workflow 重定向到 runner 临时文件，绝不写进 Actions 日志。
"""
import argparse
import contextlib
import fcntl
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import autoingest as A          # noqa: E402
import source_pull as SP        # noqa: E402
import src_guard as SG          # noqa: E402

CLAIM = os.path.join(HERE, ".gha_claim.json")
LOCK = os.path.join(HERE, ".gha_next.lock")
AGENT = "Codex-Korea-Ingest"
PLACEHOLDER = re.compile(r"^第\s*\d+\s*[集期]$")


def emit(obj):
    print(json.dumps(obj, ensure_ascii=False), flush=True)


def read_claim():
    try:
        d = json.load(open(CLAIM, encoding="utf-8"))
        if d.get("status") == "running" and time.time() - float(d.get("ts") or 0) < 6 * 3600:
            return d
    except Exception:                                      # noqa: BLE001
        pass
    return None


def broadcast(msg):
    try:
        subprocess.run([sys.executable, os.path.join(HERE, "agent_log.py"), "note",
                        "--name", AGENT, "--broadcast", msg],
                       capture_output=True, text=True, timeout=30)
    except Exception:                                      # noqa: BLE001
        pass


def ingest_busy():
    """24h autoingest 正在跑时，GitHub 不抢任务，避免同一集双下载。"""
    try:
        p = subprocess.run(["pgrep", "-af", "autoingest.py --run|source_pull.py"],
                           capture_output=True, text=True, timeout=10)
        return any(("autoingest.py --run" in x or "source_pull.py" in x)
                   and "gha_" not in x for x in (p.stdout or "").splitlines())
    except Exception:                                      # noqa: BLE001
        return False


def duplicate_log(item, ep, key, target, why):
    try:
        with open(os.path.join(HERE, "logs", "autoingest.log"), "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.strftime("%F %T"), "event": "duplicate",
                                "source": "github_actions", "series": item["series"],
                                "season": item["season"], "ep": ep, "key": key,
                                "why": why, "target": target},
                               ensure_ascii=False) + "\n")
    except Exception:                                      # noqa: BLE001
        pass


def invalid_candidate(episode, size, min_bytes, max_bytes):
    """GitHub runner 只接受完整、非占位、放得进 14GB 磁盘的源。"""
    name = str(episode.get("Name") or "").strip()
    if not size or int(size) < min_bytes:
        return "size=%s" % size
    if PLACEHOLDER.match(name):
        return "placeholder=%s" % name
    if int(size) > max_bytes:
        return "too_big=%.2fGB" % (int(size) / 1e9)
    return ""


def resolve_valid(g, item, ep, target, min_bytes, max_bytes):
    """先走项目现有 resolve；命中的源明显无效时，再用 resolve_all 找同集其它源。"""
    with contextlib.redirect_stdout(sys.stderr):
        st, episode, size = SP.resolve(g, item["series"], int(item["season"]), ep)
    why = invalid_candidate(episode, size, min_bytes, max_bytes) if st else ""
    if st and not why:
        return st, episode, size, ""

    tried = {st.label} if st else set()
    reasons = ["%s:%s" % (st.label, why)] if st else []
    with contextlib.redirect_stdout(sys.stderr):
        cands = SP.resolve_all(g, item["series"], int(item["season"]), ep)
    for cst, cep, csize in cands:
        if cst.label in tried:
            continue
        why = invalid_candidate(cep, csize, min_bytes, max_bytes)
        if not why:
            return cst, cep, csize, ""
        reasons.append("%s:%s" % (cst.label, why))
    return None, None, 0, ",".join(reasons)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--max-size-gb", type=float, default=8.0)
    ap.add_argument("--min-size-mb", type=float, default=20.0)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    fd = os.open(LOCK, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        emit({"ok": False, "reason": "busy"})
        return 0

    old = read_claim()
    if old and not a.force:
        emit({"ok": False, "reason": "claimed", "job_id": old.get("job_id"),
              "run_id": old.get("run_id")})
        return 0
    if ingest_busy() and not a.force:
        emit({"ok": False, "reason": "autoingest_busy"})
        return 0

    if not os.path.exists(A.QUEUE):
        emit({"ok": False, "reason": "no_queue"})
        return 0

    data = json.load(open(A.QUEUE, encoding="utf-8"))
    items = A.dedupe_items(data.get("items") or [])
    max_bytes = int(a.max_size_gb * 1e9)
    min_bytes = int(a.min_size_mb * 1024 * 1024)

    for item in items:
        for raw_ep in item.get("missing") or []:
            ep = int(raw_ep)
            tgt = A.target_path(item, ep)
            if not tgt or os.path.exists(tgt):
                continue
            qual = A.qual_of(os.path.basename(tgt))
            key = A.state_key_for(item["tmdb"], item["season"], ep, os.path.basename(tgt))
            try:
                has_state = A.state_key_exists(key)
                has_res = A.resource_has_episode(item, ep, qual)
                has_td = A.td_path_exists(tgt)
            except Exception as e:                         # noqa: BLE001
                print("查重失败，跳过 %s S%02dE%02d: %s" % (
                    item["series"][:12], item["season"], ep, str(e)[:80]), file=sys.stderr)
                continue
            if has_state or has_res or has_td:
                why = ",".join(x for x, ok in (("state_key", has_state),
                                               ("resource_state", has_res),
                                               ("teldrive", has_td)) if ok)
                duplicate_log(item, ep, key, tgt, why)
                print("防重命中 %s S%02dE%02d: %s" % (
                    item["series"][:12], item["season"], ep, why), file=sys.stderr)
                continue

            fail = A.load_fail()
            if A.hot(fail, item["series"], item["season"], ep):
                continue

            g = SG.Guard(verbose=False)
            try:
                st, episode, size, why = resolve_valid(
                    g, item, ep, tgt, min_bytes, max_bytes)
            except Exception as e:                     # noqa: BLE001
                print("源解析失败：%s" % str(e)[:160], file=sys.stderr)
                continue
            if not st:
                if why:
                    print("无有效源 %s S%02dE%02d：%s" % (
                        item["series"][:12], item["season"], ep, why[:300]), file=sys.stderr)
                continue

            ms = (episode.get("MediaSources") or [{}])[0]
            runtime = int(ms.get("RunTimeTicks") or 0)
            rel = os.path.relpath(tgt, A.MNT_ROOT).replace("\\", "/")
            job_id = "%s-%s" % (time.strftime("%Y%m%d_%H%M%S"), os.urandom(3).hex())
            job = {
                "job_id": job_id,
                "run_id": a.run_id,
                "series": item["series"],
                "tmdb": int(item["tmdb"]),
                "lib": item["lib"],
                "dir": item["dir"],
                "season": int(item["season"]),
                "ep": ep,
                "key": key,
                "target_rel": rel,
                "folders": os.path.dirname(rel),
                "name": os.path.basename(tgt),
                "size": int(size or 0),
                "runtime": runtime,
                "source_label": st.label,
                "source_ua": st.ua,
                "source_url": "%s/Videos/%s/stream?Static=true&api_key=%s" % (
                    st.host.rstrip("/"), episode["Id"], st.token),
                "status": "running",
                "ts": int(time.time()),
            }
            tmp = CLAIM + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(job, f, ensure_ascii=False, indent=1)
            os.chmod(tmp, 0o600)
            os.replace(tmp, CLAIM)
            broadcast("GitHub Actions 领取上片：%s S%02dE%02d，源=%s，大小=%.2fGB；"
                      "已三重查重，其他人请勿重复下载/登记同一路径。" % (
                          item["series"][:20], item["season"], ep, st.label,
                          (size or 0) / 1e9))
            emit({"ok": True, "job": job})
            return 0

    emit({"ok": False, "reason": "no_job"})
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
