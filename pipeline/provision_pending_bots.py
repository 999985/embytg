#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""provision_pending_bots.py —— 把还没启用的下载 bot 重新送进 teldrive provisioning。

背景（2026-11-24）：新申请 8 个 bot（14-21），其中 4 个（14/16/18/20）回收了 token，
另外 4 个（15/17/19/21）token 拿到了、也加了频道管理员，但 teldrive 的
provisioning 一直失败：

    verify Telegram bot: callback: rpcDoRequest: rpc error code 500: RPC_CALL_FAIL

RPC_CALL_FAIL 是 Telegram 侧的 500，属于瞬时故障（同一晚转发也遇到过，重试即成功）。
这里做成定时重试：每次只补没启用的，全部启用就立刻退出，不打扰任何东西。

用法：
    python3 provision_pending_bots.py            # 有 pending 才动作
    python3 provision_pending_bots.py --all      # 也重试已有失败记录的
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

API = "http://127.0.0.1:8080"
TOKENS = "/opt/embytg/.bot_tokens.json"


def api_token():
    try:
        out = os.popen("bash /opt/embytg/api.sh 2>/dev/null").read().strip().splitlines()
        return out[-1] if out else ""
    except Exception:                                       # noqa: BLE001
        return ""


def call(tok, method, path, body=None):
    h = {"Authorization": "Bearer " + tok, "Content-Type": "application/json",
         "Idempotency-Key": str(uuid.uuid4())}
    req = urllib.request.Request(API + path, method=method, headers=h,
                                 data=json.dumps(body).encode() if body is not None else None)
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, (r.read() or b"")


def main(a):
    tok = api_token()
    if not tok:
        print("拿不到 API token")
        return 1
    try:
        _, body = call(tok, "GET", "/v1/bots?limit=200")
        items = (json.loads(body) or {}).get("items") or []
    except Exception as e:                                  # noqa: BLE001
        print("读 bot 列表失败: %s" % str(e)[:80])
        return 1
    pending = [b for b in items if not b.get("enabled")]
    if not pending:
        print("全部已启用（%d 个）" % len(items))
        return 0
    print("待启用 %d 个：%s" % (len(pending), [b.get("id") for b in pending]))
    ok = 0
    for b in pending:
        try:
            st, _ = call(tok, "POST", "/v1/bots/%s/provision" % b["id"], {})
            print("  provision %s → %s" % (b.get("id"), st))
            ok += 1
        except urllib.error.HTTPError as e:
            print("  provision %s → HTTP %s %s" % (b.get("id"), e.code,
                                                   (e.read() or b"")[:80].decode("utf-8", "replace")))
        except Exception as e:                              # noqa: BLE001
            print("  provision %s → %s" % (b.get("id"), str(e)[:60]))
        time.sleep(3)
    return 0 if ok else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true")
    sys.exit(main(ap.parse_args()))
