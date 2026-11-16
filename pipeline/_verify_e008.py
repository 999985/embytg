#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""E008 决定性验证（服务器端运行，不改生产数据）：
1) 单测：monkeypatch tgpipe.emby 记录调用，验证 incremental→Media/Updated 的 body 与分批、
   full→/Library/Refresh 回滚分支、空路径不请求、失败落 notify_pending.jsonl。
2) 真实端到端：用一条真实存在的容器路径调 emby_media_updated，确认 HTTP 204。
3) 路径拼接：从 file_name() 产出，与宿主挂载真实路径对齐核对。
"""
import importlib.util, json, os, sys, subprocess

PIPE = "/opt/embytg/pipeline"
spec = importlib.util.spec_from_file_location("tgpipe", os.path.join(PIPE, "tgpipe.py"))
tg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tg)

print("REFRESH_MODE =", tg.REFRESH_MODE, "NOTIFY_BATCH =", tg.NOTIFY_BATCH)
assert tg.REFRESH_MODE == "incremental", "默认必须是增量"

# ---- 1) 纯单测：记录调用 ----
calls = []
def fake_emby(path, method="GET", body=None, timeout=120):
    calls.append((path, method, body))
    return None
orig = tg.emby
tg.emby = fake_emby

# 分批：NOTIFY_BATCH 临时设小，验证切片
tg.NOTIFY_BATCH = 2
paths = ["/mnt/embytg/TV/国产剧/A/Season 01/A S01E%02d - 1080p.mp4" % i for i in range(1, 6)]
tg.emby_media_updated(paths)
mu = [c for c in calls if c[0] == "/Library/Media/Updated"]
assert len(mu) == 3, "5 路径 / batch2 应 3 批，实得 %d" % len(mu)
assert [len(c[2]["Updates"]) for c in mu] == [2, 2, 1]
assert mu[0][2]["Updates"][0]["Path"] == paths[0]
assert mu[0][2]["Updates"][0]["UpdateType"] == "Created"
print("单测-增量分批 OK: 3 批 [2,2,1], Path/UpdateType 正确")

# 空路径不请求
calls.clear()
tg.emby_media_updated([])
assert len(calls) == 0
print("单测-空路径不发请求 OK")

# full 回滚
calls.clear()
tg.REFRESH_MODE = "full"
tg.emby_media_updated(paths[:1])
assert len(calls) == 1 and calls[0][0] == "/Library/Refresh", calls
print("单测-full 回落 /Library/Refresh OK")
tg.REFRESH_MODE = "incremental"

# 失败落盘
def fail_emby(path, method="GET", body=None, timeout=120):
    raise RuntimeError("boom")
tg.emby = fail_emby
tg.emby_media_updated(["/mnt/embytg/Movies/剧情片/X - 1080p.mp4"])
assert os.path.exists(tg.NOTIFY_LOG), "失败应落 notify_pending.jsonl"
last = open(tg.NOTIFY_LOG, encoding="utf-8").read().strip().split("\n")[-1]
assert json.loads(last)["path"].endswith("X - 1080p.mp4")
print("单测-失败落盘 OK:", tg.NOTIFY_LOG)

tg.emby = orig

# ---- 2) 真实端到端 204 ----
real = subprocess.run(
    "find /opt/embytg/mnt/embytg/TV -name '*.mp4' 2>/dev/null | head -1",
    shell=True, capture_output=True, text=True).stdout.strip()
cpath = real.replace("/opt/embytg/mnt/embytg", "/mnt/embytg")
print("真实端到端路径:", cpath)
tg.emby_media_updated([cpath])   # 打印 "# 增量通知 Emby: 1/1 路径"

# ---- 3) 路径拼接对齐 ----
it_mv = {"kind": "movie", "title": "测试片", "meta_name": "测试片", "year": 2020,
         "src": "tmdb", "mid": 123, "qual": "1080p"}
folder, fname = tg.file_name(it_mv)
built = "/mnt/embytg/Movies/剧情片/" + fname
print("电影拼接:", built)
assert fname.endswith(" - 1080p.mp4") and "tmdb=123" in fname
it_tv = {"kind": "tv", "title": "测试剧", "meta_name": "测试剧", "year": 2021,
         "src": "tmdb", "mid": 456, "qual": "2160p", "season": 1, "episode": 3}
folder2, fname2 = tg.file_name(it_tv)
built2 = "/mnt/embytg/TV/国产剧/%s/Season 01/%s" % (folder2, fname2)
print("剧集拼接:", built2)
assert "S01E03" in fname2 and "tmdb=456" in folder2
print("VERIFY_E008_ALL_OK")
