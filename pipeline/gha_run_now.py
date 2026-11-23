#!/usr/bin/env python3
"""把现有 queue/assign.json 推上去、触发 GHA、等结果、收库、登记。"""
import base64
import json
import subprocess
import sys
import time
import urllib.request

HERE = "/opt/embytg/pipeline"
SLUG = "999985/embytg"


def tok():
    return open("/root/.gh_token", encoding="utf-8").read().strip()


def api(path, method="GET", body=None):
    r = urllib.request.Request(
        "https://api.github.com" + path, method=method,
        headers={"Authorization": "Bearer " + tok(),
                 "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28"},
        data=json.dumps(body).encode() if body else None)
    return urllib.request.urlopen(r, timeout=40)


def main():
    content = base64.b64encode(open(HERE + "/queue/assign.json", "rb").read()).decode()
    p = "/repos/%s/contents/pipeline/queue/assign.json" % SLUG
    try:
        sha = json.loads(api(p).read())["sha"]
    except Exception:
        sha = None
    body = {"message": "assign: real job", "content": content, "branch": "main"}
    if sha:
        body["sha"] = sha
    api(p, "PUT", body)
    print("派单已推送")
    api("/repos/%s/actions/workflows/transport.yml/dispatches" % SLUG, "POST",
        {"ref": "main", "inputs": {"limit": "1", "dry_run": "false"}})
    print("已触发 GHA")
    t0 = time.time()
    run = None
    while time.time() - t0 < 1800:
        time.sleep(15)
        runs = json.loads(api("/repos/%s/actions/runs?per_page=5" % SLUG).read())["workflow_runs"]
        c = [r for r in runs if r["name"] == "transport"]
        if c and c[0]["status"] == "completed":
            run = c[0]
            break
    if not run:
        print("等待超时")
        return 1
    print("run #%s %s" % (run["run_number"], run.get("conclusion")))
    subprocess.run([sys.executable, HERE + "/gha_queue.py", "collect"], cwd=HERE, timeout=300)
    r = subprocess.run([sys.executable, HERE + "/gha_register.py", "--apply"],
                       cwd=HERE, timeout=900, capture_output=True, text=True)
    print((r.stdout or "")[-600:])
    print((r.stderr or "")[-300:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
