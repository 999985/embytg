#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""src_login.py —— 用账号密码换 Emby token（**UA 必须是 Hills**）

用户 2026-11-10 澄清：「茶百道有账号密码，自己登录拿到就行了，UA 使用 Hills」。
原先项目写的是"绝不登录"，但真正的封号判据是 **UA**（网页/浏览器 UA 才会被封），
Hills UA 登录是被允许的。本工具把这件事做成**有闸门的一次性动作**：

规则
----
1. UA 恒为 `Hills Windows/1.5.1`，并带 Emby 客户端头 `X-Emby-Authorization`（Client=Hills）
2. 只调 `/Users/AuthenticateByName`，每个源最多 2 次尝试（间隔 5s）
3. 拿到 token 写回 `_archive/sources_probe.json`，并把该源从 `src_guard.TOKEN_STALE` 移除
4. 全程审计到 `logs/source_requests.log`（kind=login）
5. **不打印密码**，token 只显示前 8 位

用法
----
    python3 src_login.py --list                 # 看哪些源有账号密码
    python3 src_login.py --label emby.chapanda.site          # 登录并保存 token
    python3 src_login.py --label emby.chapanda.site --dry    # 只看会发什么请求
"""
import argparse
import hashlib
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
# 2026-11-14（E062）：跟 src_guard 保持一致。
# 这个脚本原来也硬编码 _archive/sources.json，而配置已挪到 config/ ——
# 如果不改，下次登录源就会“文件不存在”。这个错是新加的
# check_archive_reads 自动抓到的 —— 新检查第一次运行就发现了我刚制造的回归。
SOURCES = os.path.join(HERE, "config", "sources.json")
if not os.path.exists(SOURCES):
    SOURCES = os.path.join(HERE, "_archive", "sources.json")
PROBE = os.path.join(HERE, "_archive/sources_probe.json")
UA = "Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)"
_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE


def device_id(host):
    """稳定 DeviceId（同一台设备多次登录看起来是同一个人）"""
    return hashlib.md5(("embytg-hills-" + host).encode()).hexdigest()[:16]


def load_sources():
    d = json.load(open(SOURCES, encoding="utf-8"))
    return d if isinstance(d, list) else list(d.values())


def pick(label):
    for x in load_sources():
        if x.get("label") == label:
            return x
    return None


def login(host, user, pw, dry=False):
    url = host.rstrip("/") + "/Users/AuthenticateByName"
    body = json.dumps({"Username": user, "Pw": pw}).encode()
    hdr = {
        "User-Agent": UA,
        "X-Emby-Authorization": 'MediaBrowser Client="Hills Windows", Device="Windows", '
                                'DeviceId="%s", Version="1.5.1"' % device_id(host),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if dry:
        print("  [dry] POST %s\n        UA=%s\n        X-Emby-Authorization=%s" % (url, UA, hdr["X-Emby-Authorization"]))
        return None
    for attempt in (1, 2):
        t0 = time.time()
        status = 0
        try:
            req = urllib.request.Request(url, data=body, headers=hdr, method="POST")
            with urllib.request.urlopen(req, timeout=30, context=_ctx) as r:
                status = r.status
                data = json.loads(r.read().decode())
            tok = data.get("AccessToken")
            _audit(host, status, time.time() - t0, bool(tok))
            if tok:
                print("  ✓ 登录成功，token=%s…（用户 %s）" % (tok[:8], (data.get("User") or {}).get("Name")))
                return tok
            print("  ! 返回里没有 AccessToken")
        except urllib.error.HTTPError as e:
            status = e.code
            _audit(host, status, time.time() - t0, False)
            print("  × HTTP %d（第 %d 次）" % (e.code, attempt))
            if e.code in (401, 403):
                print("    → 账号/密码不对，或该站要求先解除限制；不再重试")
                break
        except Exception as e:                                     # noqa: BLE001
            _audit(host, status, time.time() - t0, False)
            print("  × 失败：%s" % str(e)[:70])
        time.sleep(5)
    return None


def _audit(host, status, dur, ok):
    p = os.path.join(HERE, "logs", "source_requests.log")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": time.strftime("%F %T"), "session_ua": UA[:24], "host": host,
                            "label": host, "path": "/Users/AuthenticateByName (login)", "status": status,
                            "dur_ms": int(dur * 1000), "ok": ok}, ensure_ascii=False) + "\n")


def save_token(label, host, tok):
    d = json.load(open(PROBE, encoding="utf-8"))
    by = d.setdefault("by_label", {})
    rec = by.setdefault(label, {})
    rec.update({"alive": True, "auth": True, "token": tok, "url": host,
                "ts": time.strftime("%F %T"), "token_from": "src_login.py(Hills UA)"})
    json.dump(d, open(PROBE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("  ✓ 已写入 sources_probe.json")


def clear_stale(label):
    p = os.path.join(HERE, "src_guard.py")
    s = open(p, encoding="utf-8").read()
    if label in s:
        import re
        s2 = re.sub(r'\n?\s*"%s":\s*"[^"]*",?' % re.escape(label), "", s)
        if s2 != s:
            open(p, "w", encoding="utf-8").write(s2)
            print("  ✓ 已从 src_guard.TOKEN_STALE 移除")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--label")
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    if a.list:
        print("有账号密码的源：")
        for x in load_sources():
            print("  %-24s user=%-8s pw=%s  urls=%s" % (x.get("label"), x.get("user"),
                                                        "*" * len(str(x.get("pw") or "")),
                                                        str(x.get("urls"))[:50]))
        return 0

    if not a.label:
        ap.error("需要 --label 或 --list")
    x = pick(a.label)
    if not x:
        print("✗ sources.json 里没有 %s" % a.label)
        return 1
    host = (x.get("urls") or [""])[0]
    user, pw = x.get("user"), x.get("pw")
    if not pw:
        print("✗ %s 没有密码" % a.label)
        return 1
    print("== 登录 %s（user=%s）==" % (a.label, user))
    tok = login(host, user, pw, a.dry)
    if a.dry:
        return 0
    if not tok:
        return 1
    save_token(a.label, host, tok)
    clear_stale(a.label)
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
