#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证 Idle 期 FullRefresh 是否真正补上空 Series 元数据（E003 收尾决定性实验 v2）。

修正 _verify_refresh.py 的两个 bug：
  - 单条 GET /Items/{Id} 在本环境 404 -> 用列表查询 GET /Items?Ids=<id>
  - ImageRefreshMode=None 非法 -> 用 finalize_meta.py:80 的真实格式
只读 + 对选中样本发 FullRefresh（与收尾脚本行为一致，安全）。
"""
import json, time, urllib.request, urllib.parse

from embytg_conf import cfg

E = cfg("emby_url")
T = cfg("emby_token")
HDR = {"X-Emby-Token": T, "Content-Type": "application/json"}


def api(path, method="GET", body=None, timeout=180):
    data = json.dumps(body).encode() if body is not None else (b"{}" if method == "POST" else None)
    r = urllib.request.Request(E + path, method=method, headers=HDR, data=data)
    with urllib.request.urlopen(r, timeout=timeout) as f:
        raw = f.read()
        return f.status, (json.loads(raw) if raw else {})


def get_item(iid):
    st, d = api("/Items?Ids=%s&Fields=Name,Genres,Overview,ProviderIds,Path,DateLastRefreshed,Type" % iid)
    if st != 200:
        return None
    its = (d or {}).get("Items", [])
    return its[0] if its else None


def brief(x):
    if not x:
        return "None"
    g = x.get("Genres") or []
    ov = x.get("Overview") or ""
    return "G=%s OV=%s(len=%d) pid=%s dlr=%s" % (
        g, "Y" if ov else "N", len(ov), x.get("ProviderIds"), x.get("DateLastRefreshed"))


def main():
    # 1) 找出当前「元数据空」的 Series（分页，不写死）
    empties, start, page = [], 0, 500
    while True:
        st, d = api("/Items?Recursive=true&IncludeItemTypes=Series&Fields=Name,Genres,Overview"
                    "&StartIndex=%d&Limit=%d" % (start, page))
        its = (d or {}).get("Items", [])
        for x in its:
            if not (x.get("Genres") or x.get("Overview")):
                empties.append(str(x["Id"]))
        start += len(its)
        if not its or start >= ((d or {}).get("TotalRecordCount") or 0):
            break
    print("# 空元数据 Series 总数 = %d" % len(empties))
    sample = empties[:3]
    if not sample:
        print("# 无空 Series，无需验证")
        return
    print("# 样本 Ids = %s" % sample)

    # 2) before
    print("-- BEFORE --")
    for iid in sample:
        print("  %s  %s" % (iid, brief(get_item(iid))))

    # 3) FullRefresh（finalize_meta.py:80 的真实格式）
    path = ("/Items/%s/Refresh?MetadataRefreshMode=FullRefresh"
            "&ImageRefreshMode=FullRefresh&ReplaceAllMetadata=true&ReplaceAllImages=false")
    for iid in sample:
        try:
            st, _ = api(path % iid, "POST", {})
            print("  FullRefresh %s -> HTTP %s" % (iid, st))
        except Exception as e:
            print("  FullRefresh %s -> ERR %s" % (iid, str(e)[:120]))

    # 4) 轮询 20s / 40s / 60s
    for sec in (20, 40, 60):
        time.sleep(20)
        print("-- AFTER %ds --" % sec)
        for iid in sample:
            print("  %s  %s" % (iid, brief(get_item(iid))))

    # 5) 日志里 RefreshItem Start 是否出现（provider 阶段真的跑了）
    import subprocess
    log = "/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt"
    try:
        r = subprocess.run(["bash", "-lc",
                            "grep -c 'ProviderManager: RefreshItem Start' %s" % log],
                           capture_output=True, text=True, timeout=30)
        print("# RefreshItem Start 累计 = %s" % (r.stdout or "0").strip())
    except Exception as e:
        print("# 日志计数失败 " + str(e)[:80])


if __name__ == "__main__":
    main()
