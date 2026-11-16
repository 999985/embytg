#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Control-plane only: dispatch the GitHub Actions workflow when no run is active.

Never downloads or uploads media. The server only sends a small GitHub API
request; GitHub-hosted runners remain the media data plane.
"""
import fcntl
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = "/opt/embytg/pipeline"
CLAIM = os.path.join(BASE, ".gha_claim.json")
CLAIM_DIR = os.path.join(BASE, ".gha_claims")
TOKEN_FILE = os.path.join(BASE, ".gha_dispatch_token")
LOCK = "/tmp/embytg-gha-dispatch.lock"
REPO = "ss5-star/embytg-cloud"
WORKFLOW = "embytg-upload.yml"
API = "https://api.github.com"


def log(msg):
    print("%s %s" % (time.strftime("%F %T"), msg), flush=True)


def request(path, method="GET", body=None):
    token = open(TOKEN_FILE, encoding="utf-8").read().strip()
    if not token:
        raise RuntimeError("empty dispatch token")
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "User-Agent": "embytg-control-plane",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", "replace")
        if resp.status == 204 or not raw.strip():
            return resp.status, None
        return resp.status, json.loads(raw)


def claim_active():
    paths = [CLAIM]
    try:
        paths += [os.path.join(CLAIM_DIR, x) for x in os.listdir(CLAIM_DIR)
                  if x.endswith(".json")]
    except Exception:
        pass
    for path in paths:
        try:
            d = json.load(open(path, encoding="utf-8"))
            if (d.get("status") == "running"
                    and time.time() - float(d.get("ts") or 0) < 6 * 3600):
                return True
        except Exception:
            continue
    return False


def main():
    lf = open(LOCK, "w")
    try:
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("another dispatch check is running; skip")
        return 0

    if not os.path.exists(TOKEN_FILE):
        log("token missing; skip")
        return 0
    if claim_active():
        log("active claim; skip")
        return 0

    try:
        code, data = request(
            "/repos/%s/actions/workflows/%s/runs?per_page=10" % (REPO, WORKFLOW)
        )
        active = [
            r for r in (data or {}).get("workflow_runs", [])
            if r.get("status") in ("queued", "in_progress", "requested", "waiting")
        ]
        if active:
            log("active GitHub run %s; skip" % active[0].get("id"))
            return 0
        code, _ = request(
            "/repos/%s/actions/workflows/%s/dispatches" % (REPO, WORKFLOW),
            method="POST",
            body={"ref": "main", "inputs": {"mode": "ingest"}},
        )
        log("dispatched GitHub Actions run (HTTP %s)" % code)
        return 0
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:300]
        log("GitHub HTTP %s: %s" % (e.code, body))
        return 1
    except Exception as e:
        log("dispatch failed: %s" % str(e)[:300])
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
