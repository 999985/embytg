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
QUEUE = os.path.join(REPO, "queue")
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
        _git("add", "queue/assign.json")
        _git("commit", "-m", "assign %d" % len(items))
        _git("push")
    print("  已生成 %d 条派单（url 为空的需要补直链才会被 GHA 执行）" % len(items))
    return 0


def collect():
    _git("pull", "--rebase", "--autostash")
    con = _db()
    got = 0
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
    if cmd == "publish":
        n = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 2
        sys.exit(publish(n))
    if cmd == "collect":
        sys.exit(collect())
    sys.exit(status())
