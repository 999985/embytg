#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""只读就绪度审计（2026-11-10 校准版）：量化「十万~百万级批量入库」是否接得住。

【为什么重写】旧版拿 P0-1 之后已废弃的 4 个单文件 JSON
(tg_pending/tg_state/tg_meta_cache/tg_scan_raw, mtime 停在 11-09) 线性外推,
得出「100万条 604MB/3574MB、每次 apply 全量 load+dump」——这个瓶颈在
2026-11-09 22:55 P0-1 真切换(EMBYTG_STORE=sqlite, d593a30)后已不存在。
真源现在是 pipeline/tg.db (SQLite WAL)。旧结论会误导扩库决策, 必须校准。

【只读保证】tg.db 一律 mode=ro + query_only=ON；不写任何生产文件、
不触发扫描、不改配置、不联网。所有 docker/psql 均为只读查询。
"""
import json, os, re, sqlite3, subprocess, time

PIPE = "/opt/embytg/pipeline"
ROOT = "/opt/embytg"
TGDB = os.path.join(PIPE, "tg.db")
DB = "/var/lib/docker/volumes/embytg_embydata/_data/data/library.db"
LOG = "/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt"
SHIM_CACHE = "/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/cache"


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120).stdout.strip()


def sz(p):
    try:
        return os.path.getsize(p)
    except Exception:
        return -1


def ro():
    c = sqlite3.connect("file:%s?mode=ro" % TGDB, uri=True)
    c.execute("PRAGMA query_only=ON")
    return c


def line():
    print("-" * 72)


print("=" * 72)
print("EmbyTG 十万级入库就绪度审计 (校准版) | %s" % time.strftime("%F %T"))
print("=" * 72)

# ---------------------------------------------------------------- A 状态真源
line()
print("A) 状态真源：P0-1 后是 tg.db (SQLite WAL), 4 个 JSON 已废弃")
line()
try:
    c = ro()
    # 状态层(随本服入库增长) vs 清单层(TODB 全站, 不随我们入库增长)
    STATE_TABLES = ["state_key", "pending", "meta_cache", "scan_raw"]
    LIST_TABLES = ["todb_video", "todb_season", "todb_episode"]
    print("  [状态层] 随入库条目线性增长")
    tot_state = 0
    widths = {}
    for t in STATE_TABLES:
        n = c.execute("select count(*) from %s" % t).fetchone()[0]
        cols = [r[1] for r in c.execute("pragma table_info(%s)" % t)]
        expr = "+".join('coalesce(length(cast("%s" as text)),0)' % x for x in cols)
        b = c.execute("select coalesce(sum(%s),0) from %s" % (expr, t)).fetchone()[0]
        tot_state += b
        widths[t] = (n, b)
        print("    %-12s rows=%-8d payload=%9.2f MB  avg=%.1f B/row"
              % (t, n, b / 1048576, b / max(n, 1)))
    print("  [清单层] TODB 全站清单, 每天 04:00 增量几行, 规模无关")
    for t in LIST_TABLES:
        try:
            n = c.execute("select count(*) from %s" % t).fetchone()[0]
            print("    %-12s rows=%-8d" % (t, n))
        except Exception:
            pass
    print("  tg.db 文件 = %.1f MB | page_size=%s journal=%s | integrity=%s"
          % (sz(TGDB) / 1048576, c.execute("pragma page_size").fetchone()[0],
             c.execute("pragma journal_mode").fetchone()[0],
             c.execute("pragma integrity_check").fetchone()[0]))

    # 十万级外推：按「每媒体条目状态层字节」线性放大, 无全量 load+dump
    emb_ep = c.execute("select count(*) from state_key").fetchone()[0]
    per = tot_state / max(emb_ep, 1)
    print()
    print("  线性外推(状态层, SQLite 按 key 索引增量写, 不再全量序列化):")
    print("    实测基线: state_key %d 行 / 状态层 %.2f MB = %.1f B/条目"
          % (emb_ep, tot_state / 1048576, per))
    for target in (100000, 500000, 1000000):
        print("      %7d 条: 状态层约 %6.1f MB (+索引/WAL 上浮)  — 增量 upsert, O(k)"
              % (target, per * target / 1048576))
    print("    【结论】单文件 JSON 的 O(n) 全量 load+dump 瓶颈已由 P0-1 消除。")
except Exception as e:
    print("  ! tg.db 读取失败:", str(e)[:150])
finally:
    try:
        c.close()
    except Exception:
        pass

# 4 个死快照仅作对照, 明确标注废弃
print("  [废弃对照] 4 个 JSON 死快照 (P0-1 后不再自动更新, 不可用于外推):")
for f in ["tg_pending.json", "tg_state.json", "tg_meta_cache.json", "tg_scan_raw.json"]:
    p = os.path.join(PIPE, f)
    print("    %-24s %7.2f MB  mtime=%s"
          % (f, sz(p) / 1048576,
             time.strftime("%F %T", time.localtime(os.path.getmtime(p))) if sz(p) > 0 else "-"))

# ---------------------------------------------------------------- B 地基三项
line()
print("B) 地基三项 go / no-go (P0-1 / P0-2 / P0-3)")
line()
# P0-1: 默认后端（开关在 tgpipe.py:188, 不是 tgstore 模块）
try:
    src0 = open(os.path.join(PIPE, "tgpipe.py"), encoding="utf-8").read()
    m0 = re.search(r'os\.environ\.get\(\s*"EMBYTG_STORE"\s*,\s*"(\w+)"', src0)
    backend = m0.group(1) if m0 else "?"
    print("  P0-1 状态后端       : %s (tgpipe.py 缺省)  %s"
          % (backend, "GO" if backend == "sqlite" else "CHECK"))
except Exception as e:
    print("  P0-1 状态后端       : 探测失败 %s" % str(e)[:80])
# P0-2: 主路径是否还有全库 Refresh
try:
    src = open(os.path.join(PIPE, "tgpipe.py"), encoding="utf-8").read()
    full_refresh = len(re.findall(r'emby\("/Library/Refresh"|"/Library/Refresh"', src))
    media_updated = "emby_media_updated" in src and "/Library/Media/Updated" in src
    print("  P0-2 增量通知       : Media/Updated=%s  全库Refresh残留(含full回落)=%d  %s"
          % (media_updated, full_refresh, "GO" if media_updated else "CHECK"))
except Exception as e:
    print("  P0-2 增量通知       : 探测失败 %s" % str(e)[:80])
# P0-3: shim 在位 + 缓存命中率
shim_in = sh("docker exec emby head -1 /bin/ffprobe 2>/dev/null")
cache_n = len([x for x in os.listdir(SHIM_CACHE) if x.endswith(".json")]) if os.path.isdir(SHIM_CACHE) else -1
video_n = sh("docker exec -i teldrive-postgres psql -U teldrive -d teldrive -At -c "
             "\"select count(*) from files where status='active' and lower(name) ~ '\\.(mp4|mkv|avi|ts|mov)$'\"")
try:
    video_n = int(video_n)
except Exception:
    video_n = -1
print("  P0-3 ffprobe shim   : 头行=%r 缓存=%d 视频=%d 命中率=%.1f%%  %s"
      % (shim_in.strip(), cache_n, video_n,
         100.0 * cache_n / video_n if video_n > 0 else -1,
         "GO(重复探测已旁路)" if shim_in.strip() == "#!/bin/sh" else "CHECK"))

# ---------------------------------------------------------------- C 首次探测
line()
print("C) 剩余瓶颈：首次探测 (cache miss) 仍串行")
line()
cold = max(video_n - cache_n, 0) if video_n > 0 and cache_n >= 0 else -1
print("  冷文件(未缓存) ≈ %d 个；shim 命中 ~87ms/条, 冷探测 ~2-3s/文件"
      " (docs/ERRORS.md E011 实测口径)" % cold)
tail = sh("grep \"ProcessRun 'ffprobe' Execute\" %s | tail -400 | head -1" % LOG)
tail2 = sh("grep \"ProcessRun 'ffprobe' Execute\" %s | tail -1" % LOG)
def ts(l):
    m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", l)
    return m.group(1) if m else None
t0, t1 = ts(tail), ts(tail2)
total = sh("grep -c \"ProcessRun 'ffprobe' Execute\" %s" % LOG)
print("  日志 ffprobe Execute 累计 = %s | 窗口 %s -> %s" % (total, t0, t1))
if t0 and t1:
    fmt = "%Y-%m-%d %H:%M:%S"
    sec = time.mktime(time.strptime(t1, fmt)) - time.mktime(time.strptime(t0, fmt))
    if sec > 0:
        rate = 400 / (sec / 60.0)
        print("  Emby 被调吞吐(当前以缓存命中为主) ≈ %.1f 条/分 = %.0f 条/小时"
              % (rate, rate * 60))
        print("  ⚠ 该速率是『命中态』, 不能用于全冷外推(口径不可混用)。")
# 全冷首次探测：按 E011 实测 2-3s/文件、Emby 单线程串行
print("  首次全冷串行外推(2-3s/文件, 无 prewarm):")
for target in (100000, 1000000):
    print("    %7d 集: %.1f ~ %.1f 天" %
          (target, target * 2 / 86400.0, target * 3 / 86400.0))
print("  → 结论: P0-3 只把『重复探测』从每次全库降到 87ms;")
print("     『首次入库』对新增文件仍要真探, 这是唯一剩余硬瓶颈。")
print("  旁路手段: prewarm_ffprobe.sh (argv 与 Emby 完全一致, 已实证) 可离线并发预热,")
print("     把冷探测移出 Emby 串行链路; p0_3_rewarm.py 处理改名后 key 失效。")

# ---------------------------------------------------------------- D 本机水位
line()
print("D) 本机资源水位")
line()
print("  " + sh("df -m / | tail -1"))
print("  cache = " + (sh("du -sm /opt/embytg/cache 2>/dev/null | cut -f1") or "?") + " MB")
print("  load  = " + sh("cat /proc/loadavg"))
print("  containers = " + sh("docker ps --format '{{.Names}}' | tr '\\n' ' '"))

# ---------------------------------------------------------------- E 全量遍历点
line()
print("E) P1 待改：仍会全量遍历/全量拉内存的组件 (十万级下会退化)")
line()
print("  - nfo_gen.py --scan           : 全量遍历 FUSE 目录树收集清单")
print("  - finalize_meta.py paged()    : 全量拉 Movie/Series/Episode 进内存 list")
print("  - fix_nfo_tags.py os.walk    : 一次收集全量路径")
print("  - auto_finalize.py pg_dump   : 全量 dump(百万文件 PG 库数 GB)")
print("  改造路线见 docs/SCALING.md §2；本轮不擅改(并发 agent 占用项目文件)。")

line()
print("  判据一句话(SCALING §1): 增量优先、按库分片、状态落 DB、不硬编码规模常量。")
print("=" * 72)
print("AUDIT_DONE (只读, 未写任何生产文件)")
