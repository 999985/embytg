#!/usr/bin/env python3
"""控制面侧：给 GitHub Actions 发活、收结果。

    python3 gha_queue.py publish --limit 2     # 把候选写进 queue/assign.json 并推送
    python3 gha_queue.py collect               # 拉回结果，落到 tg.db.gha_transport
    python3 gha_queue.py status                # 看最近发活/回收情况

为什么用仓库当传输带，而不是让 GHA 直接 SSH 回来：
仓库是公开的，GitHub Secrets 里放一把能登 root 的私钥，等于把服务器交给任何
能改 workflow 的人。改成「控制面推任务 → GHA 搬字节 → 提交结果 → 控制面拉回」，
GHA 侧只需要一个消耗号 + 频道 id，最小权限。

登记仍然只能在控制面做：teldrive 只监听 127.0.0.1。
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
# 2026-11-21：队列实际在 pipeline/queue（gh_sync 只同步 pipeline/ 与 .github/，
# 放仓库根上不去）。这里跟 gha_transport_run.py 保持一致。
QUEUE = os.path.join(HERE, "queue")
DB = os.path.join(HERE, "tg.db")


def _git(*args):
    return subprocess.run(["git", "-C", REPO] + list(args),
                          capture_output=True, text=True)


def _db():
    con = sqlite3.connect(DB, timeout=30)
    con.execute("create table if not exists gha_transport("
                "run_id text primary key, core text, ok integer, reason text, "
                "file_id text, size integer, secs real, ts integer)")
    return con


def _release_claim(rid, why):
    """派单失败必须把已经落下的 claim 收回去。

    2026-11-22（P0）：gha_next3 一到手就写 claim，publish_auto 却可能因为
    「没有直链 / 解析失败」直接 return 1 —— claim 留在 .gha_claims 里当
    running 记账，占满 max_active（实测 3 条 slot6 孤儿把 active 顶到 6/6，
    embytg-gha-stock 每轮 no_job），还要等 claim_reap 150 分钟才回收。
    派单失败 = 这一单没派出去，claim 必须当场释放。
    """
    try:
        subprocess.run([sys.executable, os.path.join(HERE, "gha_cancel.py"),
                        "--run-id", rid, "--reason", "publish_abort: " + why],
                       capture_output=True, text=True, timeout=120, cwd=HERE)
    except Exception:                                        # noqa: BLE001
        pass


def publish_auto():
    """从调度器直接领一单，转成 GHA 派单（真实直链 + 元数据）。

    用户要求：「不用手工填，让他也领单，传完回填」+「保证随时有事可做」。
    所以派单必须由调度器产生，而且 GHA 侧不需要任何回连能力。
    """
    import subprocess
    # run_id 必须带 slotN 才会被识别成「外部 worker」：否则会跟本机抢容量，
    # 本机满负荷时永远 no/ max_active（实测就是这么失败的）。
    rid = "gha-auto-slot6-%d" % int(time.time())
    # 「随时有事可做」：一条 lane 没活就换下一条，别直接空手回去。
    # 顺序按欠活量（fill 补缺 → upgrade 洗版 → new 发现新剧）。
    job, out = None, ""
    for lane in ("fill", "upgrade", "new"):
        try:
            r = subprocess.run([sys.executable, os.path.join(HERE, "gha_next3.py"),
                                "--lane", lane, "--run-id", rid,
                                # 2026-11-22：--max-attempts 1 意味着「试一个系列就退出」。
                                # 队列里有 393 部剧 / 6000 集缺口，但每次只试 1 部，
                                # 那一部恰好无源就报 no_job —— 用户看到的「没活」就是这么来的。
                                "--max-attempts", os.environ.get("GHA_MAX_ATTEMPTS", "8"),
                                "--lock-wait", "120"],
                               capture_output=True, text=True, timeout=600, cwd=HERE,
                               env={**os.environ, "EMBYTG_EXTERNAL": "1",
                                    "EMBYTG_WORKER": "gha",
                                    # 预留时长：GHA 每 6 小时才醒一次，锁 30 分钟
                                    # 根本等不到它。默认 6 小时，可用
                                    # EMBYTG_CLAIM_TTL 覆盖。本地 worker 看到
                                    # 已占用会自动跳过这一集，等于给 GHA 预留。
                                    "JOB_CLAIM_TTL": os.environ.get("EMBYTG_CLAIM_TTL", "21600")})
        except Exception as e:                               # noqa: BLE001
            print("  %s 调用失败: %s" % (lane, str(e)[:60]))
            continue
        out = (r.stdout or "").strip()
        # 2026-11-21：gha_next3 每次会打**多行 JSON**（scan_gate_retry / source_budget /
        # no_job / job）。原来取「最后一行」，于是经常拿到 no_job 或 gate 提示，
        # 真正的 job 行被跳过 —— 表现为「领到活但没有直链」。
        # 正确做法：在所有行里挑 ok=true 且带 job 的那一条。
        cand = None
        for line in (r.stdout or "").splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except Exception:                                # noqa: BLE001
                continue
            if obj.get("ok") and obj.get("job"):
                cand = obj
        if cand is None:
            for line in reversed((r.stdout or "").splitlines()):
                line = line.strip()
                if line.startswith("{"):
                    try:
                        cand = json.loads(line)
                        break
                    except Exception:                        # noqa: BLE001
                        continue
        if cand and cand.get("ok"):
            _j = cand.get("job") or {}
            _sz = int(_j.get("size") or 0)
            _src = str(_j.get("source_label") or "")
            # 2026-11-22 实测：run #6 派了 1.2GB / bangumi 的集，
            # 20 分钟连下载都没完成（该源一直有证书告警、速度极慢），
            # 最后只能取消。GHA 的窗口有限（单 job 45 分钟），
            # 慢源 + 大文件必须挡在派单之外，否则每次都是白跑。
            # 2026-11-22 用户决定（方案 C）：**只按源站快慢筛，不按体积筛。**
            # 依据：run #6 失败是 bangumi 太慢，不是 1.2GB 大；
            # GHA 自己下载有 20MB/s，1-3GB 的集在窗口内完全跑得完。
            # 窗口已放宽到 90 分钟：慢源 1MB/s 也能搬 5GB 左右，
            # 所以慢源不再一律拒绝，只拒绝「慢源 + 超大」（>2GB）这种必超时的组合。
            # 2026-11-22 实测修正：bangumi 从 GitHub 网络也慢得离谱——
            # run #8 拉 1GB 卡了 35 分钟还没完成（run #6 同样），
            # 说明这不是「文件大」的问题，是那条源对 GHA 不可用。
            # 结论：GHA 只用快源（xqd / lite / aaa / emos / chapanda），
            # 慢源（bangumi / 39.96 / ct.bgp）一律不派，与体积无关。
            # 2026-11-22 二次修正（用户提醒）：**文件本来就是分片段上传的**，
            # 2GB 只是单条消息上限，不是整集上限。gha_transport_run.py 现在
            # 按 GHA_PART_MB（默认1800）切片，逐段上传，teldrive 拼回整集。
            # 所以不再按体积过滤，只留一个防呆上限（默认 8GB）避免单 job 超窗口。
            _CAP = int(os.environ.get("GHA_MAX_FILE_MB", "8000")) * 1024 * 1024
            _SLOW_SRC = ("bangumi", "39.96.113.82", "ct.cdn.bgp")
            if _sz > _CAP:
                print("  跳过 %s（%.0fMB > %dMB 上限，非 Premium 号传不了）"
                      % (str(_j.get("series"))[:12], _sz / 1e6, _CAP // 1024 // 1024))
                try:
                    import gha_state as _GS
                    _GS.release_item(_GS.item_key(_j), _GS.worker_id(rid))
                    _hit = _GS.find_claim(str(_j.get("run_id") or rid))
                    if _hit and _hit[0] and os.path.exists(_hit[0]):
                        os.remove(_hit[0])
                except Exception:                            # noqa: BLE001
                    pass
                continue
            if any(k in _src for k in _SLOW_SRC):
                print("  跳过 %s（%s %.0fMB）：慢源，GHA 窗口不够"
                      % (str(_j.get("series"))[:12], _src, _sz / 1e6))
                # 跳过时必须把 claim 文件也删掉：只释放 job_claim 的话，
                # claim 文件仍被 read_claims() 计入 active，几次跳过后
                # 就把容量顶满（实测 active=7/cap=7 就是这么来的）。
                try:
                    import gha_state as _GS
                    _GS.release_item(_GS.item_key(_j), _GS.worker_id(rid))
                    _hit = _GS.find_claim(str(_j.get("run_id") or rid))
                    if _hit and _hit[0] and os.path.exists(_hit[0]):
                        os.remove(_hit[0])
                except Exception:                            # noqa: BLE001
                    pass
                continue
            job = cand
            print("  lane %s 领到活" % lane)
            break
        print("  lane %s 无活（%s）" % (lane, (cand or {}).get("reason") or out[-60:]))
    if job and job.get("job") and not job.get("source_url"):
        job = job["job"]          # 外层是 {ok, job:{...}}，直链在内层
    if not job or job.get("ok") is False:
        print("  没领到活: %s" % ((job or {}).get("reason") or out[-120:]))
        return 1
    url = job.get("source_url") or ""
    if not url:
        print("  领到的活没有直链，跳过")
        _release_claim(rid, "no_source_url")
        return 1
    # job 的字段名是 target_rel / name / folders（make_job 里定义的），
    # 原来取 target/rel → 全是空字符串，派出去的单子 filename 和 folders 都空，
    # GHA 上传时文件名丢失、登记时无处可放。
    target = job.get("target_rel") or job.get("target") or job.get("rel") or ""
    folders = job.get("folders") or (os.path.dirname(target) if target else "")
    filename = job.get("name") or (os.path.basename(target) if target else "")
    item = {
        "run_id": rid,
        "core": job.get("key") or "",
        "filename": filename,
        "url": url,
        "size": job.get("size") or 0,
        "caption": job.get("series") or "",
        "meta": {"folders": folders, "tmdb": job.get("tmdb"),
                 "season": job.get("season"), "ep": job.get("ep"),
                 "channel_id": int(os.environ.get("TG_STORE_CH") or 4413117262),
                 "mnt": "/opt/embytg/mnt/embytg"},
    }
    os.makedirs(QUEUE, exist_ok=True)
    open(os.path.join(QUEUE, "assign.json"), "w", encoding="utf-8").write(
        json.dumps({"generated_at": int(time.time()), "items": [item]},
                   ensure_ascii=False, indent=1))
    print("  已派单:", item["filename"][:60], "|", item["size"] // 1024 // 1024, "MB")
    return 0


def publish(limit=2):
    """从低码率洗版池里挑可交付的活，写成 assign.json。"""
    sys.path.insert(0, HERE)
    import best_pick  # noqa: F401  确保导入链健康
    con = _db()
    rows = con.execute(
        "select rel, size, dur_sec from resource_state "
        "where rel like '%.mp4' or rel like '%.mkv' order by landed_at desc limit ?",
        (limit,)).fetchall()
    con.close()
    if not rows:
        print("  没有可发活的记录")
        return 1
    os.makedirs(QUEUE, exist_ok=True)
    items = []
    for rel, size, dur in rows:
        rid = "gha-%d-%s" % (int(time.time()), os.path.basename(rel)[:24].replace(" ", "_"))
        items.append({"run_id": rid, "core": rel, "filename": os.path.basename(rel),
                      "size": size, "dur_sec": dur, "caption": ""})
    # URL 需要上游源站直链，这里留空表示「等控制面补 url」——先把骨架跑通
    for it in items:
        it.setdefault("url", "")
    open(os.path.join(QUEUE, "assign.json"), "w", encoding="utf-8").write(
        json.dumps({"generated_at": int(time.time()), "items": items},
                   ensure_ascii=False, indent=1))
    if os.environ.get("GH_PUSH") == "1":
        _git("add", "pipeline/queue/assign.json")
        _git("commit", "-m", "assign %d" % len(items))
        _git("push")
    print("  已生成 %d 条派单（url 为空的需要补直链才会被 GHA 执行）" % len(items))
    return 0


def _results_from_github():
    """直接从 GitHub API 拉结果文件。

    2026-11-21：本地 pipeline/queue 被 .gitignore 忽略，git pull 拿不到
    GHA 提交的 result-*.json（实测 collect 收到 0 条，而文件明明在仓库里）。
    仓库才是权威，本地目录只作兜底。
    """
    try:
        tok = open("/root/.gh_token", encoding="utf-8").read().strip()
    except Exception:                                          # noqa: BLE001
        return []
    import base64
    import urllib.request
    H = {"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json"}
    out = []
    try:
        lst = json.loads(urllib.request.urlopen(urllib.request.Request(
            "https://api.github.com/repos/999985/embytg/contents/pipeline/queue",
            headers=H), timeout=25).read())
    except Exception:                                          # noqa: BLE001
        return []
    for x in lst if isinstance(lst, list) else []:
        if not x["name"].startswith("result-"):
            continue
        try:
            c = json.loads(urllib.request.urlopen(urllib.request.Request(
                x["url"], headers=H), timeout=25).read())
            out.append(json.loads(base64.b64decode(c["content"]).decode()))
        except Exception:                                      # noqa: BLE001
            continue
    return out


def collect():
    _git("pull", "--rebase", "--autostash")
    con = _db()
    got = 0
    packed = _results_from_github()
    for r in packed:
        cur = con.execute("insert or ignore into gha_transport"
                          "(run_id, core, ok, reason, file_id, size, secs, ts, meta, msg_id) "
                          "values(?,?,?,?,?,?,?,?,?,?)",
                          (r.get("run_id"), r.get("core"), 1 if r.get("ok") else 0,
                           r.get("reason"), str(r.get("file_id") or ""),
                           int(r.get("size") or 0), float(r.get("secs") or 0),
                           int(time.time()),
                           json.dumps({**(r.get("meta") or {}),
                                      "parts": r.get("parts") or []}, ensure_ascii=False),
                           str(r.get("file_id") or "")))
        if cur.rowcount:
            got += 1
    for name in sorted(os.listdir(QUEUE)) if os.path.isdir(QUEUE) else []:
        if not name.startswith("result-") or not name.endswith(".json"):
            continue
        try:
            r = json.load(open(os.path.join(QUEUE, name), encoding="utf-8"))
        except Exception:                                       # noqa: BLE001
            continue
        cur = con.execute("insert or ignore into gha_transport"
                          "(run_id, core, ok, reason, file_id, size, secs, ts) "
                          "values(?,?,?,?,?,?,?,?)",
                          (r.get("run_id"), r.get("core"), 1 if r.get("ok") else 0,
                           r.get("reason"), str(r.get("file_id") or ""),
                           int(r.get("size") or 0), float(r.get("secs") or 0),
                           int(time.time())))
        if cur.rowcount:
            got += 1
    con.commit()
    con.close()
    print("  新回收 %d 条结果" % got)
    return 0


def status():
    con = _db()
    rows = con.execute("select run_id, ok, reason, size, secs from gha_transport "
                       "order by ts desc limit 10").fetchall()
    ok = con.execute("select count(*) from gha_transport where ok=1").fetchone()[0]
    bad = con.execute("select count(*) from gha_transport where ok=0").fetchone()[0]
    con.close()
    print("  GHA 搬运：成功 %d 失败 %d" % (ok, bad))
    for r in rows:
        print("    %-34s %s %6.1fs %s" % (r[0][:34], "✔" if r[1] else "✘",
                                          r[4] or 0, (r[2] or "")[:40]))
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "publish-auto":
        sys.exit(publish_auto())
    if cmd == "publish":
        n = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 2
        sys.exit(publish(n))
    if cmd == "collect":
        sys.exit(collect())
    sys.exit(status())
