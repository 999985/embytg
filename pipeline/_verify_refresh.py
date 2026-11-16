#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""决定性验证：State=Idle + 按库对账齐时，单条 FullRefresh 是否真补刮空 Series。

E003 结论是「Running 期 Refresh 被吞」；现在 Idle，验证 provider 阶段是否启动。
另报告 ffprobe 增速，判断 converged() 里「ffprobe 不涨」判据该不该保留。
"""
import json, os, sys, time, urllib.request, urllib.error
E = "http://127.0.0.1:8096"
TOKEN = "REPLACE_ME"
HDR = {"X-Emby-Token": TOKEN, "Content-Type": "application/json", "Accept": "application/json"}
LOG = "/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt"
PIPE = "/opt/embytg/pipeline"
sys.path.insert(0, PIPE)


def api(p, method="GET", timeout=60):
    r = urllib.request.Request(E + p, headers=HDR, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as f:
            raw = f.read()
            return f.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return 0, str(e)[:150]


def series_fields(iid):
    st, it = api("/Items/%d?Fields=Genres,Overview,ProviderIds,DateLastRefreshed" % iid)
    if st != 200 or not isinstance(it, dict):
        return None
    return (it.get("Genres") or [], bool((it.get("Overview") or "").strip()),
            it.get("ProviderIds") or {}, it.get("DateLastRefreshed"))


# 找空 Series（Genres=[] 且 Overview 空），优先欧美剧艾丽 125191 + 一个新日韩剧
candidates = [125191, 138826, 138829]
logbase = int(os.popen("grep -c \"ProviderManager: RefreshItem Start\" %s" % LOG).read().strip() or 0)
print("=== 基线 RefreshItem Start 累计 =", logbase)

before = {}
for iid in candidates:
    f = series_fields(iid)
    before[iid] = f
    print("  基线 Id=%-8s G=%s OV=%s pid=%s" % (iid, f[0] if f else None,
          f[1] if f else None, list((f[2] if f else {}).keys())))

print("\n=== 逐条 POST FullRefresh（Idle 期） ===")
for iid in candidates:
    st, _ = api("/Items/%d/Refresh?MetadataRefreshMode=FullRefresh&ImageRefreshMode=None"
                "&ReplaceAllMetadata=true&ReplaceAllImages=false" % iid, "POST", timeout=60)
    print("  Id=%-8s FullRefresh -> HTTP %s" % (iid, st))

print("\n=== 20s 后读回 ===")
time.sleep(20)
for iid in candidates:
    f = series_fields(iid)
    print("  20s  Id=%-8s G=%s OV=%s pid=%s lastRef=%s" % (
        iid, f[0] if f else None, f[1] if f else None,
        list((f[2] if f else {}).keys()), f[3] if f else None))
logafter = int(os.popen("grep -c \"ProviderManager: RefreshItem Start\" %s" % LOG).read().strip() or 0)
print("  RefreshItem Start 累计 %d -> %d （新增 %d）" % (logbase, logafter, logafter - logbase))
print("  日志命中 SeriesNfoProvider/MovieDbSeriesProvider/RefreshItem Start（tail 60）:")
print("   ", os.popen("tail -60 %s | grep -cE 'SeriesNfoProvider|MovieDbSeriesProvider|RefreshItem Start'" % LOG).read().strip())

# ffprobe 增速与对账
fp0 = int(os.popen("grep -c \"ProcessRun 'ffprobe' Execute\" %s" % LOG).read().strip() or 0)
print("\n=== ffprobe 20s 增速 ===")
time.sleep(20)
fp1 = int(os.popen("grep -c \"ProcessRun 'ffprobe' Execute\" %s" % LOG).read().strip() or 0)
print("  ffprobe %d -> %d (+%d / 20s)" % (fp0, fp1, fp1 - fp0))

from lib_reconcile import reconcile
r = reconcile()
print("\n=== 按库对账 ===")
print("  converged=%s gaps=%s Episode总=%s 期望[%d,%d]" % (
    r["converged"], r["libs_with_gap"], r["episode_total"],
    r["episode_expected_min"], r["episode_expected_max"]))

st, ts = api("/ScheduledTasks")
print("\n=== 非 Idle 任务 ===", [(t.get("Name"), t.get("State")) for t in (ts or []) if t.get("State") != "Idle"])
