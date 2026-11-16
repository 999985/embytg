#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""probe_source_servers.py —— 只读探测第三方 Emby 源服，看它们怎么处理剧集季结构/集标题

硬约束（源站风控 m00764/m00768/m00926/m00930，违反会被封号；详见 docs/SOURCE_SERVERS.md）：
  1. 绝不触发 /Users/AuthenticateByName 或任何登录端点 —— 只用 sources_probe.json 里已存的 token
  2. UA 必须伪装 `Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)`
  3. 只发 GET，不写、不下载、不改
  4. 每次请求间隔 ≥1.5s，单服请求数 ≤ 12，避免"同 IP 高频"特征

用法：
  python3 probe_source_servers.py --list
  python3 probe_source_servers.py --server goemby --query 咒术回战
  python3 probe_source_servers.py --server goemby --seasons <seriesId>
"""
import argparse
import json
import ssl
import time
import urllib.parse
import urllib.request

HILLS = "Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)"
PROBE = "/opt/embytg/pipeline/_archive/sources_probe.json"
_SLEEP = 1.6
_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE


def load():
    d = json.load(open(PROBE))
    return d.get("by_label") or {}


def get(base, path, token, timeout=25):
    url = base.rstrip("/") + path
    req = urllib.request.Request(url, headers={
        "User-Agent": HILLS,
        "X-Emby-Token": token,
        "Accept": "application/json",
    })
    time.sleep(_SLEEP)
    with urllib.request.urlopen(req, timeout=timeout, context=_ctx) as r:
        raw = r.read().decode("utf-8", "ignore")
    return json.loads(raw) if raw.strip().startswith(("{", "[")) else raw


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--server")
    ap.add_argument("--query")
    ap.add_argument("--seasons")
    a = ap.parse_args()
    d = load()
    if a.list:
        for k, v in d.items():
            print("%-24s alive=%-5s auth=%-5s token=%s server=%s items=%s"
                  % (k, v.get("alive"), v.get("auth"),
                     "yes" if v.get("token") else "no",
                     v.get("server"), v.get("total")))
        return
    if not a.server:
        print("need --server"); return
    cand = [(k, v) for k, v in d.items() if a.server.lower() in k.lower() and v.get("token")]
    if not cand:
        print("no server with token matching", a.server); return
    name, info = cand[0]
    base, tok = info["url"], info["token"]
    print("== %s  url=%s  server=%s  ver=%s" % (name, base, info.get("server"), info.get("version")))
    if a.query:
        for typ in ("Series", "Movie"):
            q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": typ,
                                        "SearchTerm": a.query, "Limit": 10,
                                        "Fields": "ProviderIds,ProductionYear"})
            try:
                r = get(base, "/emby/Items?" + q, tok)
            except Exception as e:
                print("  query %s ERR %s" % (typ, str(e)[:80])); continue
            items = r.get("Items") or []
            print("  %s 命中 %d" % (typ, r.get("TotalRecordCount", len(items))))
            for it in items[:6]:
                print("    %-7s %-34s year=%-5s tmdb=%s" % (
                    it.get("Id"), (it.get("Name") or "")[:34], it.get("ProductionYear"),
                    (it.get("ProviderIds") or {}).get("Tmdb")))
    if a.seasons:
        sid = a.seasons
        try:
            se = get(base, "/emby/Shows/%s/Seasons?Fields=Name,IndexNumber,ChildCount" % sid, tok)
            print("  Seasons:", [(x.get("IndexNumber"), x.get("Name"), x.get("ChildCount")) for x in (se.get("Items") or [])])
        except Exception as e:
            print("  seasons ERR", str(e)[:80])
        for sn in (1, 2):
            q = urllib.parse.urlencode({"Season": sn, "Fields": "Name,ParentIndexNumber,IndexNumber", "Limit": 400})
            try:
                r = get(base, "/emby/Shows/%s/Episodes?" % sid + q, tok)
            except Exception as e:
                print("  S%d ERR %s" % (sn, str(e)[:60])); continue
            eps = r.get("Items") or []
            named = [e for e in eps if (e.get("Name") or "").strip()]
            print("  S%d 集数=%d 有Name=%d 例=%s" % (sn, len(eps), len(named),
                  [(e.get("IndexNumber"), (e.get("Name") or "")[:26]) for e in eps[:4]]))


if __name__ == "__main__":
    main()
