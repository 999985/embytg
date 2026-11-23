#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""等源站配额恢复 → 派单 → 触发 GHA → 收结果 → 登记。全程无人值守。

为什么需要等待：2026-11-21 实测，本机 3 个 worker 已把当日源站配额吃满，
fill/upgrade 无候选、new 报 queue_sources_throttled:6。此时强行跑 GHA 只会
拿到 no_job，所以这里按间隔重试，一有活就自动走完整链路。

用法：python3 gha_e2e_once.py [--rounds 12] [--gap 600]
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
QUEUE = os.path.join(HERE, "queue", "assign.json")
REPO_SLUG = "999985/embytg"


def token():
    return open("/root/.gh_token", encoding="utf-8").read().strip()


def api(path, method="GET", body=None):
    tok = token()
    r = urllib.request.Request("https://api.github.com" + path, method=method,
                               headers={"Authorization": "Bearer " + tok,
                                        "Accept": "application/vnd.github+json",
                                        "X-GitHub-Api-Version": "2022-11-28"},
                               data=json.dumps(body).encode() if body else None)
    return urllib.request.urlopen(r, timeout=40)


def push_assign():
    import base64
    content = base64.b64encode(open(QUEUE, "rb").read()).decode()
    path = "/repos/%s/contents/pipeline/queue/assign.json" % REPO_SLUG
    try:
        sha = json.loads(api(path).read())["sha"]
    except Exception:                                       # noqa: BLE001
        sha = None
    body = {"message": "assign: gha e2e real job", "content": content, "branch": "main"}
    if sha:
        body["sha"] = sha
    api(path, "PUT", body)
    print("  派单已推送到仓库")


def dispatch():
    api("/repos/%s/actions/workflows/transport.yml/dispatches" % REPO_SLUG, "POST",
        {"ref": "main", "inputs": {"limit": "1", "dry_run": "false"}})
    print("  已触发 GHA 真实上传")


def wait_run(timeout=1800):
    t0 = time.time()
    while time.time() - t0 < timeout:
        time.sleep(15)
        runs = json.loads(api("/repos/%s/actions/runs?per_page=5" % REPO_SLUG).read())["workflow_runs"]
        cand = [r for r in runs if r["name"] == "transport"]
        if cand and cand[0]["status"] == "completed":
            print("  run #%s %s" % (cand[0]["run_number"], cand[0].get("conclusion")))
            return cand[0]
    print("  等待超时")
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=12)
    ap.add_argument("--gap", type=int, default=600)
    a = ap.parse_args()
    for i in range(a.rounds):
        print("[%s] 第 %d/%d 轮：尝试派单" % (time.strftime("%H:%M"), i + 1, a.rounds))
        r = subprocess.run([sys.executable, os.path.join(HERE, "gha_queue.py"), "publish-auto"],
                           cwd=HERE, capture_output=True, text=True, timeout=900,
                           env={**os.environ, "EMBYTG_EXTERNAL": "1", "EMBYTG_WORKER": "gha"})
        out = (r.stdout or "")[-200:]
        print("  " + out.replace("\n", "\n  "))
        try:
            items = json.load(open(QUEUE, encoding="utf-8")).get("items") or []
        except Exception:                                   # noqa: BLE001
            items = []
        if items:
            print("  领到活:", items[0]["filename"][:60])
            push_assign()
            dispatch()
            if wait_run():
                subprocess.run([sys.executable, os.path.join(HERE, "gha_queue.py"), "collect"],
                               cwd=HERE, timeout=300)
                subprocess.run([sys.executable, os.path.join(HERE, "gha_register.py"), "--apply"],
                               cwd=HERE, timeout=600)
                print("  链路完成：搬运 + 登记")
            return 0
        time.sleep(a.gap)
    print("  所有轮次都没领到活（源站配额仍未恢复）")
    return 1


if __name__ == "__main__":
    sys.exit(main())
