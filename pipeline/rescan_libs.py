# -*- coding: utf-8 -*-
"""停止卡在补探测的全库扫描 → 确认 Idle → 分库 Recursive Refresh 重发现新文件。

背景（本环境实证，2026-11-09）：
  - 全库 RefreshLibrary State=Running prog=29.6 已 21h，ffprobe 在国产剧串行补
    E001 欠账的 MediaStreams（4795 条待补），结果确实逐个落库，扫描健康。
  - 但日韩剧 1310 文件 / Emby 仅 2 Episode（缺 1308），96 个 Series 未入库。
    Running 期 POST /Items/187/Refresh Recursive 返回 204 但被吞（60s 无变化）。
  - 网上方案（子代理 289a7d8f，官方 dev.emby.media）：
      DELETE /ScheduledTasks/Running/{Id} = Stops a scheduled task；
      POST /Items/{ParentId}/Refresh?Recursive=true = 分库扫描发现新文件。
    采纳 S1+S2；否决 full 回滚 / 删 NFO / 改 STRM（E001 实测 + 会丢库）。

用法:
  python3 rescan_libs.py --stop --lib 日韩剧     # 停止 + 刷一个库 + 90s 验证
  python3 rescan_libs.py --stop --all           # 停止 + 全部 8 TV 库
  python3 rescan_libs.py --lib 日韩剧           # 不停止，只刷（Running 期会被吞）
  python3 rescan_libs.py --status               # 只读采样
"""
import json, os, shutil, sqlite3, subprocess, sys, time, urllib.request, urllib.error
from datetime import datetime

E = "http://127.0.0.1:8096"
TOKEN = "REPLACE_ME"
HDR = {"X-Emby-Token": TOKEN, "Content-Type": "application/json", "Accept": "application/json"}
DB = "/var/lib/docker/volumes/embytg_embydata/_data/data/library.db"
LOG = "/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt"
BKDIR = "/opt/embytg/backup"


def api(p, method="GET", timeout=120):
    r = urllib.request.Request(E + p, headers=HDR, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as f:
            raw = f.read()
            return f.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return 0, str(e)[:200]


def sh(c, timeout=60):
    return (subprocess.run(["bash", "-lc", c], capture_output=True, text=True,
                           timeout=timeout).stdout or "").strip()


def task_info():
    st, ts = api("/ScheduledTasks")
    for t in (ts or []):
        if t.get("Key") == "RefreshLibrary":
            return t
    return {}


def lib_roots():
    """库根 ParentId 真源 = GET /Library/MediaFolders（web UI 三点菜单用的 Id）。

    E005 教训：library.db 的 Type=3 Folder Id（日韩剧=187、国产剧=127433）
    与 VirtualFolders Id（日韩剧=186、国产剧=184）不是一回事。MediaFolders
    返回 18 个 CollectionFolder：Id=VirtualFolders Id、Path=None、
    CollectionType=tvshows/movies —— **必须按 CollectionType 过滤**，
    按 Path 过滤会得到 0 个。子代理 289a7d8f 引官方 82769/135723：新系列
    没 Id，必须用「库根 parent folder 的 Id」= 这里的 Id。
    GET /Items/{Id} 对 CollectionFolder 返回 404 属正常，不影响 Refresh。
    """
    st, d = api("/Library/MediaFolders")
    out = []
    if st == 200 and isinstance(d, dict):
        for it in d.get("Items", []):
            if it.get("CollectionType") == "tvshows":
                out.append({"id": it.get("Id"), "name": it.get("Name"),
                            "path": "/mnt/embytg/TV/" + (it.get("Name") or ""),
                            "src": "MediaFolders"})
        print("  /Library/MediaFolders -> HTTP %s，TV 库 %d 个" % (st, len(out)))
    if not out:
        print("  ! MediaFolders 不可用，回落 library.db Type=3")
        shutil.copy2(DB, "/tmp/rescan_roots.db")
        c = sqlite3.connect("/tmp/rescan_roots.db")
        rows = c.execute(
            "select Id,Name,Path from MediaItems where Type=3 and Path like '/mnt/embytg/TV/%'").fetchall()
        c.close()
        os.remove("/tmp/rescan_roots.db")
        out = [{"id": r[0], "name": r[1], "path": r[2], "src": "DB"} for r in rows]
    # 打印每个 Id 的直接子项数（Series 数），留证据：正确库根应≈该库 Series 数
    for r in out:
        st2, d2 = api("/Items?ParentId=%s&Limit=1&EnableTotalRecordCount=true" % r["id"])
        n = (d2 or {}).get("TotalRecordCount") if st2 == 200 else -1
        r["children"] = n
        print("    Id=%-6s %-10s 子项(Series)=%-6s src=%s" %
              (r["id"], r["name"], n, r["src"]))
    return out


def ep_by_lib():
    shutil.copy2(DB, "/tmp/rescan_ep.db")
    c = sqlite3.connect("/tmp/rescan_ep.db")
    out = {}
    for r in c.execute("select Path from MediaItems where Type=8 and Path like '/mnt/embytg/TV/%'"):
        p = r[0] or ""
        lib = p.split("/TV/")[1].split("/")[0] if "/TV/" in p else "?"
        out[lib] = out.get(lib, 0) + 1
    c.close()
    os.remove("/tmp/rescan_ep.db")
    return out


def series_by_lib():
    shutil.copy2(DB, "/tmp/rescan_s.db")
    c = sqlite3.connect("/tmp/rescan_s.db")
    out = {}
    for r in c.execute("select Path from MediaItems where Type=6 and Path like '/mnt/embytg/TV/%'"):
        p = r[0] or ""
        lib = p.split("/TV/")[1].split("/")[0] if "/TV/" in p else "?"
        out[lib] = out.get(lib, 0) + 1
    c.close()
    os.remove("/tmp/rescan_s.db")
    return out


def backup_db():
    os.makedirs(BKDIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BKDIR, "library.db.pre_rescan_%s" % ts)
    shutil.copy2(DB, dst)
    for suf in ("-wal", "-shm"):
        if os.path.exists(DB + suf):
            shutil.copy2(DB + suf, dst + suf)
    print("  备份 library.db -> %s (%d B)" % (dst, os.path.getsize(dst)))


def stop_task():
    t = task_info()
    tid = t.get("Id")
    print("  停止前 State=%s prog=%s Id=%s" % (t.get("State"), t.get("CurrentProgressPercentage"), tid))
    if t.get("State") != "Running":
        print("  当前非 Running，跳过 DELETE")
        return True
    st, _ = api("/ScheduledTasks/Running/%s" % tid, "DELETE", timeout=120)
    print("  DELETE /ScheduledTasks/Running/%s -> HTTP %s" % (tid, st))
    # 等 State 落到 Idle（Cancelling 是中间态）
    for i in range(30):
        time.sleep(2)
        cur = task_info()
        stt = cur.get("State")
        if stt == "Idle":
            print("  [%ds] State=Idle ✓ LastStatus=%s" %
                  (i * 2, (cur.get("LastExecutionResult") or {}).get("Status")))
            return True
        if i % 5 == 0:
            print("  [%ds] State=%s ..." % (i * 2, stt))
    cur = task_info()
    print("  ! 60s 后仍 State=%s（卡 Cancelling，见社区 139408）；需 docker restart emby 兜底" % cur.get("State"))
    return False


def refresh_lib(root):
    # MediaFolders 的 Id 是 JSON 字符串（不是 int），一律 %s，别用 %d
    p = ("/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=Default"
         "&ImageRefreshMode=Default&ReplaceAllMetadata=false&ReplaceAllImages=false" % str(root["id"]))
    st, _ = api(p, "POST", timeout=120)
    print("  POST /Items/%s/Refresh (%s) -> HTTP %s" % (root["id"], root["name"], st))
    return st in (200, 204)


def main():
    a = sys.argv[1:]
    roots = lib_roots()
    print("=== TV 库根 ===")
    for r in roots:
        print("  Id=%-5s %-10s %s" % (r["id"], r["name"], r["path"]))

    if "--status" in a:
        t = task_info()
        print("scan State=%s prog=%s" % (t.get("State"), t.get("CurrentProgressPercentage")))
        print("Episode 分库:", ep_by_lib())
        print("Series 分库:", series_by_lib())
        return

    if "--stop" in a:
        print("\n=== 停止全库扫描（先备份 DB） ===")
        backup_db()
        if not stop_task():
            print("  State 未落 Idle，本轮不刷库，退出")
            return

    print("\n=== 触发前基线 ===")
    ep0, se0 = ep_by_lib(), series_by_lib()
    print("  Episode:", ep0)
    print("  Series :", se0)
    fp0 = sh("grep -c \"ProcessRun 'ffprobe' Execute\" %s" % LOG)

    if "--all" in a:
        targets = roots
    elif "--lib" in a:
        name = a[a.index("--lib") + 1]
        targets = [r for r in roots if r["name"] == name]
        if not targets:
            print("  ! 找不到库", name)
            return
    else:
        print("  未指定 --lib/--all，只是停止，不刷库")
        return

    print("\n=== 分库 Recursive Refresh ===")
    for r in targets:
        refresh_lib(r)
        time.sleep(2)

    print("\n=== 90s 验证采样 ===")
    time.sleep(90)
    ep1, se1 = ep_by_lib(), series_by_lib()
    fp1 = sh("grep -c \"ProcessRun 'ffprobe' Execute\" %s" % LOG)
    print("  Episode:", ep0, "->", ep1)
    print("  Series :", se0, "->", se1)
    print("  ffprobe 累计 %s -> %s" % (fp0, fp1))
    for r in targets:
        n = r["name"]
        print("  【%s】Series %d->%d  Episode %d->%d" %
              (n, se0.get(n, 0), se1.get(n, 0), ep0.get(n, 0), ep1.get(n, 0)))
    print("  日志'日韩剧'命中 =", sh("grep -c '日韩剧' %s" % LOG))
    print("  近40条 ffprobe 日韩剧 =",
          sh("grep \"ProcessRun 'ffprobe' Execute\" %s | tail -40 | grep -c '日韩剧'" % LOG))
    print("  scan State:", [(t.get("State"), t.get("CurrentProgressPercentage"))
                            for t in (api("/ScheduledTasks")[1] or []) if t.get("Key") == "RefreshLibrary"])


if __name__ == "__main__":
    main()
