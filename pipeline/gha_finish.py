#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gha_finish.py — GitHub Actions 上传完成后的服务器收尾（三线版）。

只调用项目现成工具：
  pc_register.py           写 teldrive PG + tg.db.state_key
  post_ingest.py           定向 NFO / Refresh / Emby Name / 文件验收
  resource_state.py        重建生命周期台账
  nfo_sidecar.py           洗版旧文件改名时同步 NFO/海报

同一时间只允许一个 finish 执行登记/刮削/全量台账；下载和上传仍可并行。
upgrade lane 在新文件通过 NFO/Emby 验证后，才把旧视频改成 ``.video.dup``，
并留下可回滚 journal。不会删除 TG 消息或旧视频字节。
"""
import argparse
import contextlib
import fcntl
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import autoingest as A          # noqa: E402
import gha_state as GS          # noqa: E402
import lane_queue as LQ         # noqa: E402
import nfo_sidecar as NS        # noqa: E402
import post_ingest as P         # noqa: E402

FINISH_LOCK = os.path.join(HERE, ".gha_finish.lock")
JOURNAL_DIR = os.path.join(HERE, ".gha_upgrade_journal")
# 2026-11-15（E131）：老身份名 Codex-Korea-Ingest 是韩国节点时代的遗留，
# 它让协作日志看起来「一直有个 Codex 在干活」，实际是这三条脚本的机械写入。
# 用户已确认现在只有 hermes-ops 在维护，改成中性名以免误导排查。
AGENT = "embytg-pipeline"
Q_RANK = {"2160p": 4, "1440p": 3, "1080p": 2, "720p": 1, "sd": 0}


def quality_from_dims(width, height):
    width, height = int(width or 0), int(height or 0)
    if height >= 1800 or width >= 3000:
        return "2160p"
    if height >= 1400 or width >= 2400:
        return "1440p"
    if height >= 1000 or width >= 1800:
        return "1080p"
    if height >= 700 or width >= 1200:
        return "720p"
    return "sd"


@contextlib.contextmanager
def finish_lock(wait_sec=1800):
    fd = os.open(FINISH_LOCK, os.O_CREAT | os.O_RDWR, 0o600)
    deadline = time.time() + max(0, int(wait_sec))
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() >= deadline:
                    raise RuntimeError("等待收尾锁超时（%ss）" % wait_sec)
                time.sleep(3)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def load_claim(run_id):
    path, job = GS.find_claim(run_id, include_failed=True)
    if not path:
        raise RuntimeError("找不到可恢复 claim：%s" % run_id)
    return path, job


def save_claim(path, job):
    GS.save_claim(path, job)


def log_agent(cmd, msg):
    try:
        subprocess.run([sys.executable, os.path.join(HERE, "agent_log.py"), cmd,
                        "--name", AGENT, "--task", "韩国双槽上片",
                        "--result", msg], capture_output=True, text=True, timeout=30)
    except Exception:                                      # noqa: BLE001
        pass


def run(cmd, data=None, timeout=1800):
    p = subprocess.run(cmd, input=data, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=timeout)
    if p.stdout:
        print(p.stdout[-3000:], file=sys.stderr)
    if p.stderr:
        print(p.stderr[-3000:], file=sys.stderr)
    return p


def td_file_id(rel, force=True):
    nodes, paths = A.td_snapshot(force=force)
    return next((nid for nid, p in paths.items()
                 if p == rel and nodes[nid].get("kind") == "file"), None)


def td_parts(fid):
    rows = A.psql_rows(
        "select part_no,channel_id,message_id,plain_size,stored_size "
        "from file_parts where file_id='%s' order by part_no;" % fid)
    out = []
    for line in rows.splitlines():
        f = line.split("|")
        if len(f) >= 5:
            out.append({"part_no": int(f[0]), "channel_id": int(f[1]),
                        "message_id": int(f[2]),
                        "plain_size": int(f[3]), "stored_size": int(f[4])})
    return out


def verify_td(fid, manifest):
    parts = td_parts(fid)
    want = sorted(manifest.get("parts") or [], key=lambda x: int(x["part_no"]))
    if len(parts) != len(want):
        raise RuntimeError("teldrive 片数不一致：实际 %d / 清单 %d" % (len(parts), len(want)))
    if sum(p["plain_size"] for p in parts) != sum(int(p["plain_size"]) for p in want):
        raise RuntimeError("teldrive 总字节与清单不一致")
    for got, expected in zip(parts, want):
        want_ch = int(expected.get("channel_id") or 0)
        if want_ch and int(got["channel_id"]) != want_ch:
            raise RuntimeError("teldrive part%02d 频道不一致：%s != %s" % (
                got["part_no"], got["channel_id"], want_ch))
    return parts


def target_abs(rel):
    return os.path.join(A.MNT_ROOT, rel.replace("/", os.sep))


RC_ADDR = os.environ.get("EMBYTG_RCLONE_RC", "127.0.0.1:5572")


def _rc_forget(dir_rel, timeout=15):
    """让 rclone mount 丢掉某个目录的 VFS 缓存条目（失败只留痕）。

    与 ``reconcile._vfs_forget`` 同一根因（E178）：``vfs/refresh`` 对**已缓存**的
    目录返回 OK 但不会失效，只有 ``vfs/forget`` + 重新 readdir 才会重新拉列表。
    """
    if not dir_rel:
        return False
    try:
        r = subprocess.run(["rclone", "rc", "vfs/forget", "dir=" + dir_rel,
                            "--rc-addr", RC_ADDR],
                           capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0
    except Exception:                                          # noqa: BLE001
        return False


def wait_mount_file(path, timeout=180):
    """等文件出现在挂载上（E180）。

    根因：mount 用 ``--dir-cache-time 10m``，被动 ``os.listdir`` 每 3s 读到的都是
    同一份缓存目录，180s 窗口内永远看不到新上传的文件 —— 2026-11-18 一天 128 单
    这样白等到超时（占当日中止 74%），文件其实早已在 teldrive 里。
    现在每 10s 主动 ``vfs/forget`` 文件所在目录（顺带其上一级，覆盖「季目录整个
    新建」的情况）再重新 readdir，等待窗口与目录缓存 TTL 彻底解耦。
    """
    parent = os.path.dirname(path)
    rel = os.path.relpath(parent, A.MNT_ROOT).replace(os.sep, "/")
    if rel == ".":
        rel = ""
    up = "/".join(rel.split("/")[:-1]) if rel and "/" in rel else ""
    deadline = time.time() + max(0, timeout)
    last = 0.0
    while True:
        if os.path.exists(path):
            return
        now = time.time()
        if now >= deadline:
            break
        if now - last >= 10:
            _rc_forget(rel)
            if up:
                _rc_forget(up)
            last = now
        try:
            os.listdir(parent)          # 清过缓存后这一下会真的重列远端
        except OSError:
            pass
        time.sleep(3)
    raise RuntimeError("rclone 挂载尚未出现文件（%ds）：%s" % (timeout, path))


def journal_path(run_id):
    os.makedirs(JOURNAL_DIR, mode=0o700, exist_ok=True)
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in str(run_id))
    return os.path.join(JOURNAL_DIR, safe + ".json")


def write_journal(job, stage, extra=None):
    old_rel = str(job.get("old_rel") or "")
    data = {
        "ts": time.strftime("%F %T"),
        "run_id": job.get("run_id"),
        "job_id": job.get("job_id"),
        "lane": job.get("lane"),
        "series": job.get("series"),
        "season": job.get("season"),
        "ep": job.get("ep"),
        "new_rel": job.get("target_rel"),
        "old_rel": old_rel,
        "old_dup_rel": old_rel + ".dup" if old_rel else "",
        "old_quality": job.get("old_quality"),
        "new_quality": job.get("quality"),
        "pre_dup": bool(job.get("pre_dup")),
        "stage": stage,
    }
    data.update(extra or {})
    path = journal_path(job.get("run_id") or job.get("job_id"))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
    return path


def rename_old_to_dup(job):
    old_rel = str(job.get("old_rel") or "")
    if not old_rel:
        raise RuntimeError("upgrade 缺少 old_rel")
    old = target_abs(old_rel)
    dup = old + ".dup"
    if not os.path.exists(old):
        if os.path.exists(dup):
            return dup, "already"
        raise RuntimeError("旧视频不存在：%s" % old_rel)
    if os.path.exists(dup):
        raise RuntimeError("旧视频 .dup 已存在，拒绝覆盖：%s.dup" % old_rel)
    moved, skipped = NS.rename_with_sidecars(old, dup)
    if skipped:
        print("  旧 sidecar 已存在，未覆盖：%s" % ", ".join(skipped), file=sys.stderr)
    return dup, "renamed:%d" % len(moved)


def restore_dup(job):
    """Restore a pre-dup old video when the replacement never landed."""
    old_rel = str(job.get("old_rel") or "")
    if not old_rel:
        return "no_old_rel"
    old = target_abs(old_rel)
    dup = old + ".dup"
    if not os.path.exists(dup):
        return "no_dup"
    if os.path.exists(old):
        return "target_exists"
    NS.rename_with_sidecars(dup, old)
    moved = []
    old_stem = os.path.splitext(old)[0]
    for suf in NS.SUFFIXES[1:]:
        src = dup + suf
        if not os.path.exists(src):
            continue
        dst = old_stem + suf
        if os.path.exists(dst):
            continue
        os.rename(src, dst)
        moved.append(os.path.basename(dst))
    return "restored:%d" % len(moved)


def drop_superseded_dup(job, dup_rel):
    """洗版成功后删掉旧版字节 + 旧版 sidecar。

    2026-11-23（E198）：改名成 .dup 只是「让 Emby 看不见」，不是「删掉」。
    upgrade lane 原来只改名不删除，实测积累 14 个 .dup（19.6 GB TG 配额），
    而且旧条目在 Emby 里要等下一次整库扫描才消失 —— 期间 Emby 集数 > 文件数，
    lib_reconcile 常驻 converged=False（daily_ops 天天红）。

    判据：新文件已在 Emby 里、旧路径已不在，才删。drop_old.py 自带
    「同季同集还有更大文件」的安全闸，这里不绕过它；拒绝就留着，不影响入库。
    """
    if os.environ.get("EMBYTG_KEEP_DUP") == "1" or not dup_rel:
        return ""
    try:
        r = run([sys.executable, os.path.join(HERE, "drop_old.py"),
                 "--path", dup_rel], timeout=900)
        if r.returncode != 0:
            print("  ⚠ 旧版未删除 rc=%d（保留，不影响入库）：%s"
                  % (r.returncode, dup_rel), file=sys.stderr)
            return ""
        # drop_old 只删视频本体，sidecar 后缀要自己收
        dup_abs = target_abs(dup_rel)
        stem = os.path.splitext(dup_abs)[0]
        for suf in NS.SUFFIXES[1:]:
            for cand in (stem + suf, dup_abs + suf):
                try:
                    if os.path.exists(cand):
                        os.remove(cand)
                except OSError:
                    pass
        write_journal(job, "old_dropped", {"dup_rel": dup_rel})
        return dup_rel
    except Exception as e:                                   # noqa: BLE001
        print("  ⚠ 删旧版异常（保留，不影响入库）：%s" % str(e)[:120],
              file=sys.stderr)
        return ""


def wait_td_path_gone(rel, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not td_file_id(rel, force=True):
            return True
        time.sleep(4)
    return False


def wait_td_path_present(rel, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if td_file_id(rel, force=True):
            return True
        time.sleep(4)
    return False


def wait_sidecars_in_td(job, timeout=180):
    """等 post_ingest 写的 sidecar（剧集 NFO / tvshow.nfo）真正落进 teldrive。

    2026-11-15 根因（slot1 run 20261115_133536 连跑 144 次都失败）：
    post_ingest 通过 FUSE 写 NFO，teldrive 是**异步上传** —— 挂载上 stat 立刻
    可见，但 Postgres `files` 行还没提交。紧接着 `resource_state.py --scan`
    查的是 Postgres 文件树，于是刚写的 NFO 不存在 → scraped_at=0 →
    下面那句 `resource_state.scraped_at 为空` 把整个收尾判失败 →
    runner 的 RESUME_FINISH 又从头跑一遍，永远好不了。
    这是**收尾流程内部的竞态**，不是数据问题：同一时刻手动重跑 scan 就 PASS。
    判据只能落在「扫描之前」：先确认 sidecar 进了 teldrive 文件树。
    """
    target_rel = str(job.get("target_rel") or "").lstrip("/")
    if not target_rel:
        return []
    stem = os.path.splitext(target_rel)[0]
    series_rel = os.path.dirname(os.path.dirname(target_rel))
    want = [stem + ".nfo", series_rel + "/tvshow.nfo"] if series_rel else [stem + ".nfo"]
    missing = []
    for rel in want:
        if not wait_td_path_present(rel, timeout):
            missing.append(rel)
    return missing


def refresh_episode(job):
    """扫描本集文件 + 只刷**这条集条目**（leaf）。绝不刷 Series/Season 目录。

    E115 第五根因（2026-11-23 02:12 / 04:15，DB 实证 171873/171874、171900/171901）：
    `Items/{SeriesId}/Refresh?Recursive=false` 在 Emby 4.10 里仍会刷新子季并枚举
    季目录，把「文件已落地、条目还没入库」的新集当新增插入。事故现场 02:12:19.424
    一条 Series Refresh，50ms 后本函数发 TargetedScan，两条同 guid 同 Path 同秒落库。
    叶子条目没有子项可枚举，机制上不可能再长出兄弟条目。
    """
    # E008/E028/E036：必须喂精确媒体文件，不能喂 Series 目录（整库 ffprobe）。
    # 2026-11-14 回归修复：这里曾退回裸 Media/Updated，而它**只能刷新已有条目**；
    # 新剧/新集（Series 尚不存在）会 204 假成功 → Emby Items 里查不到 →
    # gha_finish 判「未就位」把 claim 标 failed，runner 反复重试直到被 claim_reap 回收。
    # 正路是 TargetedScan 的 /Library/ScanPath：向上找最近已知祖先，<1 秒建出
    # Series/Season/Episode；插件不在时 emby_scan 自动降级回 Media/Updated（不会整库扫）。
    scan_path = "/mnt/embytg/" + job["target_rel"].lstrip("/")
    res = {}
    try:
        import emby_scan
        res = emby_scan.scan_paths([scan_path])
        if res.get("Created") or res.get("Refreshed"):
            print("  ScanPath %s: Created=%d Refreshed=%d"
                  % (res.get("mode"), res.get("Created", 0),
                     res.get("Refreshed", 0)), file=sys.stderr)
        elif res.get("PathNotFound") or res.get("ParentNotFound"):
            print("  ScanPath 未命中（路径/父目录不在挂载上）：%s" % scan_path[:120],
                  file=sys.stderr)
        else:
            print("  ScanPath 失败：%s" % str(res)[:120], file=sys.stderr)
    except Exception as e:                                 # noqa: BLE001
        print("  Emby ScanPath 异常：%s" % str(e)[:120], file=sys.stderr)
    hit, errors = P.refresh_episodes([scan_path])
    for x in errors[:3]:
        print("  Episode Refresh 失败：%s" % x, file=sys.stderr)
    return hit


def emby_episode_paths(job):
    items = P.emby("/Items?Recursive=true&IncludeItemTypes=Series&Fields=Path&Limit=5000")
    sid = None
    needle = "/" + job["dir"]
    for s in (items or {}).get("Items", []):
        if (s.get("Path") or "").endswith(needle):
            sid = s.get("Id")
            break
    if not sid:
        return None, []
    q = urllib.parse.urlencode({
        "ParentId": sid, "Recursive": "true", "IncludeItemTypes": "Episode",
        "Limit": "10000",
        "Fields": "Path,ParentIndexNumber,IndexNumber,MediaStreams,Size",
    })
    eps = (P.emby("/Items?" + q) or {}).get("Items", [])
    out = []
    host_root = A.MNT_ROOT.rstrip("/")
    for e in eps:
        path = e.get("Path") or ""
        if path.startswith("/mnt/embytg/"):
            path = path.replace("/mnt/embytg/", host_root + "/", 1)
        if path:
            out.append((path, e))
    return sid, out


def episode_rank(episode):
    for stream in (episode.get("MediaStreams") or []):
        if (stream.get("Type") or "").lower() != "video":
            continue
        width = int(stream.get("Width") or 0)
        height = int(stream.get("Height") or 0)
        if height >= 1800 or width >= 3000:
            return 4
        if height >= 1400 or width >= 2400:
            return 3
        if height >= 1000 or width >= 1800:
            return 2
        if height >= 700 or width >= 1200:
            return 1
        return 0
    return -1


def align_claim_with_manifest(job, manifest, claim_path):
    """Use ffprobe dimensions from the runner to correct a stale claim target.

    Source Emby metadata can advertise 4K while the actual stream is 1080p.
    The runner probes the downloaded file and writes actual_* plus the corrected
    path/key into the manifest.  Never accept a quality label that disagrees
    with those measured dimensions.
    """
    width = int(manifest.get("actual_width") or 0)
    height = int(manifest.get("actual_height") or 0)
    if width <= 0 or height <= 0:
        raise RuntimeError("manifest 缺少真实分辨率（runner 未执行 ffprobe）")
    actual = quality_from_dims(width, height)
    declared = str(manifest.get("quality") or "").lower()
    if declared != actual:
        raise RuntimeError("清单画质与实测不符：%s != %s (%dx%d)" % (
            declared or "-", actual, width, height))
    name = str(manifest.get("name") or "")
    target_rel = str(manifest.get("target_rel") or "")
    folders = str(manifest.get("folders") or "")
    key = str(manifest.get("key") or "")
    if not name or not target_rel or not folders or not key:
        raise RuntimeError("manifest 缺少 target_rel/folders/key/name")
    if os.path.basename(target_rel) != name:
        raise RuntimeError("manifest.name 与 target_rel 不一致")
    if os.path.dirname(target_rel).replace("\\", "/") != folders:
        raise RuntimeError("manifest.folders 与 target_rel 不一致")
    prefix = "TV/%s/%s/Season %02d/" % (
        job["lib"], job["dir"], int(job["season"]))
    if not target_rel.startswith(prefix):
        raise RuntimeError("manifest 目标越过已领取剧集目录：%s" % target_rel)
    want_key = A.state_key_for(job["tmdb"], job["season"], job["ep"], name)
    if key != want_key:
        raise RuntimeError("manifest.key 与目标文件名不一致：%s != %s" % (
            key, want_key))
    if job.get("upgrade"):
        old_rank = Q_RANK.get(str(job.get("old_quality") or "sd").lower(), 0)
        old_rel = str(job.get("old_rel") or "")
        if old_rel:
            try:
                con = sqlite3.connect(
                    "file:%s?mode=ro" % A.TGDB, uri=True, timeout=30)
                old_dims = con.execute(
                    "select width,height from resource_state where rel=?",
                    (old_rel,)).fetchone()
                con.close()
                if old_dims:
                    old_rank = Q_RANK.get(
                        quality_from_dims(old_dims[0], old_dims[1]), 0)
            except Exception:                              # noqa: BLE001
                pass
        if Q_RANK.get(actual, 0) <= old_rank:
            raise RuntimeError("洗版实测画质未提升：%s <= %s" % (
                actual, job.get("old_quality") or "sd"))
    elif str(job.get("lane") or "").startswith("new") and Q_RANK.get(actual, 0) < 2:
        raise RuntimeError("新增剧实测画质低于 1080p：%s" % actual)
    elif job.get("lane") == "fill" and Q_RANK.get(actual, 0) < 1:
        raise RuntimeError("补缺实测画质低于 720p：%s" % actual)

    changed = any((
        job.get("name") != name,
        job.get("target_rel") != target_rel,
        job.get("folders") != folders,
        job.get("key") != key,
        job.get("quality") != actual,
    ))
    job.update({
        "name": name,
        "target_rel": target_rel,
        "folders": folders,
        "key": key,
        "quality": actual,
        "actual_width": width,
        "actual_height": height,
        "actual_quality": actual,
    })
    if changed:
        save_claim(claim_path, job)
    return job


def verify_actual_emby(job, width, height, timeout=180):
    """Verify the exact indexed file reports the same dimensions as ffprobe."""
    target = target_abs(job["target_rel"])
    deadline = time.time() + timeout
    last = None
    refreshed = False
    while time.time() < deadline:
        _, rows = emby_episode_paths(job)
        episode = next((e for path, e in rows
                        if os.path.normpath(path) == os.path.normpath(target)), None)
        if episode:
            video = next((s for s in (episode.get("MediaStreams") or [])
                          if (s.get("Type") or "").lower() == "video"), {})
            got_w = int(video.get("Width") or 0)
            got_h = int(video.get("Height") or 0)
            last = "%sx%s" % (got_w, got_h)
            if got_w == int(width) and got_h == int(height):
                return episode
        if not refreshed and time.time() > deadline - max(30, timeout - 30):
            refresh_episode(job)
            refreshed = True
        time.sleep(10)
    raise RuntimeError("Emby 实测分辨率未收敛：want=%sx%s got=%s" % (
        width, height, last or "未收录"))


def forget_emby_items_at(path_abs, timeout=120):
    """摘掉「文件已经不在盘上」的旧集条目（只按精确 Path 匹配）。

    E199（2026-11-23）：洗版 pre-dup 把旧片改名成 ``.dup`` 后，旧 Emby 条目
    仍然指着旧 Path。实测 Emby 4.10：集条目 Refresh 只刷元数据、TargetedScan
    只建新条目，**两者都不会删掉缺文件的旧条目** —— 只有「递归刷新剧/季」在
    枚举目录时才会发现文件缺失并移除。而递归刷新正是 E115 造同 Path 重复条目
    的机制，不能为了收尾去发。

    事故现场：法警小队 S02E06（12:53 判 failed，13:12 才被别的进程的 Series
    递归刷新清掉）、仙逆 S01E156（13:40 判 failed，13:42 才清掉）—— 都只差
    一两次目录枚举，却把 upgrade lane 堵了 19~70 分钟。

    这里在「旧 Path 已不在挂载上」的前提下按精确 Path 删条目：Emby 的
    DELETE 会尝试删 Path 指向的文件，路径已不存在 → 无字节可删（与 drop_old
    第 ④ 步同一口径）。绝不按集号删，避免误伤新版条目。
    """
    host_root = A.MNT_ROOT.rstrip("/")
    want = os.path.normpath(path_abs)
    q = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                "Fields": "Path", "Limit": "20000"})
    items = (P.emby("/Items?" + q) or {}).get("Items") or []
    hit = []
    for e in items:
        p = e.get("Path") or ""
        if p.startswith("/mnt/embytg/"):
            p = host_root + p[len("/mnt/embytg"):]
        if os.path.normpath(p) == want:
            hit.append(str(e.get("Id")))
    for eid in hit:
        try:
            P.emby("/Items/%s" % eid, "DELETE", timeout=timeout)
        except Exception as exc:                               # noqa: BLE001
            print("  ⚠ 旧条目删除失败 %s：%s" % (eid, str(exc)[:80]),
                  file=sys.stderr)
    return len(hit)


def verify_upgrade_emby(job, timeout=300):
    """洗版收尾判据：新文件在、旧文件不在、该集只指向新文件、画质达标。

    2026-11-22（E178）：判据从「匹配条目数 == 1」改成「匹配**不同 Path 集合** == {新文件}」。
    事故：我是歌手 S01E03 / V世代 S01E08 洗版后 new=True old=False rank=4/4，
    但同 Path 重复条目让 len(matching)==2 → 连刷 300s 也永远不收敛（refresh 不合并重复，
    E125 已留证）→ claim 标 failed、堵住 upgrade lane。
    同 Path 重复是 E125 类库索引瑕疵（由 clean_dup_items.py 专门清），不是洗版失败。
    """
    new_abs = os.path.normpath(target_abs(job["target_rel"]))
    old_abs = os.path.normpath(target_abs(job["old_rel"]))
    same_name = os.path.abspath(new_abs) == os.path.abspath(old_abs)
    season, ep = int(job["season"]), int(job["ep"])
    want_rank = Q_RANK.get(str(job.get("quality") or "sd").lower(), 0)
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        sid, rows = emby_episode_paths(job)
        items = [(os.path.normpath(p), e) for p, e in rows
                 if int(e.get("ParentIndexNumber") or -1) == season
                 and int(e.get("IndexNumber") or -1) == ep]
        paths = {p for p, _ in items}
        saw_new = new_abs in paths
        saw_old = (not same_name) and old_abs in paths
        new_ep = next((e for p, e in items if p == new_abs), {})
        got_rank = episode_rank(new_ep)
        if (saw_new and not saw_old and paths == {new_abs}
                and got_rank >= want_rank):
            if len(items) > len(paths):
                print("  ⚠ %s：%d 条同 Path 重复条目（E125 口径，不计洗版失败）"
                      % (job.get("name"), len(items) - len(paths)),
                      file=sys.stderr)
            return sid
        last = "sid=%s new=%s old=%s paths=%d items=%d rank=%s/%s" % (
            sid, saw_new, saw_old, len(paths), len(items), got_rank, want_rank)
        # E199：改名后的旧条目不会被集级 Refresh 删掉，只有递归扫描才会 —— 不能等。
        # 判据收紧到「盘上确实没有旧文件」才摘条目，避免误删还有字节的版本。
        if saw_old and not os.path.exists(old_abs):
            n = forget_emby_items_at(old_abs)
            if n:
                print("  已摘掉缺文件的旧 Emby 条目 %d 条：%s"
                      % (n, os.path.basename(old_abs)), file=sys.stderr)
        refresh_episode(job)
        time.sleep(15)
    raise RuntimeError("洗版后 Emby 定向刷新未收敛：%s" % (last or "无数据"))


def register_from_manifest(job, manifest):
    rel = job["target_rel"]
    folders = job["folders"]
    existing = td_file_id(rel, force=True)
    if existing:
        verify_td(existing, manifest)
        cmd = [sys.executable, os.path.join(HERE, "pc_register.py"),
               "--folders", folders, "--file-id", existing, "--tgdb",
               "--ch", "source_github", "--tmdb", str(job["tmdb"]),
               "--lib", job["lib"]]
        r = run(cmd, data=json.dumps(manifest, ensure_ascii=False))
        if r.returncode != 0:
            raise RuntimeError("pc_register 补登记失败 rc=%d" % r.returncode)
        fid = existing
    else:
        cmd = [sys.executable, os.path.join(HERE, "pc_register.py"),
               "--folders", folders, "--tgdb", "--ch", "source_github",
               "--tmdb", str(job["tmdb"]), "--lib", job["lib"]]
        r = run(cmd, data=json.dumps(manifest, ensure_ascii=False))
        if r.returncode != 0:
            raise RuntimeError("pc_register 登记失败 rc=%d" % r.returncode)
        fid = td_file_id(rel, force=True)
        if not fid:
            raise RuntimeError("登记后仍找不到 teldrive 文件：%s" % rel)
        verify_td(fid, manifest)
    return fid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--mount-wait", type=int, default=180)
    ap.add_argument("--no-probe", action="store_true",
                    help="跳过 teldrive 直读探测（仅应急；正常入库必须探测，否则坏文件会让 Emby 的 ffprobe 挂死）")
    ap.add_argument("--lock-wait", type=int, default=1800)
    a = ap.parse_args()

    claim_path = None
    job = {}
    pre_dup_renamed = False
    try:
        claim_path, job = load_claim(a.run_id)
        if job.get("status") == "failed":
            job["status"] = "running"
            job["resumed_at"] = int(time.time())
            job["resume_count"] = int(job.get("resume_count") or 0) + 1
            save_claim(claim_path, job)
        manifest = json.load(open(a.manifest, encoding="utf-8"))
        job = align_claim_with_manifest(job, manifest, claim_path)
        total = sum(int(p["plain_size"]) for p in (manifest.get("parts") or []))
        if job.get("size") and abs(total - int(job["size"])) > max(
                1024 * 1024, int(job["size"]) * 0.01):
            raise RuntimeError("清单大小 %d 与源大小 %d 差异过大" % (
                total, int(job["size"])))

        with finish_lock(a.lock_wait):
            rel = job["target_rel"]
            target = target_abs(rel)
            old_rel = str(job.get("old_rel") or "")
            old_abs = target_abs(old_rel) if old_rel else ""
            same_name = bool(old_rel and os.path.abspath(target) == os.path.abspath(old_abs))

            # Different-name upgrades let the old SD/720p episode shadow the new
            # file in Emby. Rename it to .dup before NFO/Emby verification so
            # the new episode can become visible immediately; rollback restores
            # it if any later verification fails.
            if (job.get("upgrade") and old_rel and not same_name
                    and not job.get("pre_dup")):
                job["pre_dup"] = True
                save_claim(claim_path, job)

            if job.get("pre_dup"):
                write_journal(job, "before_pre_dup")
                _, action = rename_old_to_dup(job)
                pre_dup_renamed = True
                write_journal(job, "pre_dup_renamed", {"rename_action": action})
                if not wait_td_path_gone(old_rel, 180):
                    raise RuntimeError("pre_dup 后旧路径仍在 teldrive：%s" % old_rel)
                if not wait_td_path_present(old_rel + ".dup", 180):
                    raise RuntimeError("pre_dup 后 .dup 未进入 teldrive")

            fid = register_from_manifest(job, manifest)
            # 2026-11-13（E028 根因修复）：**通知 Emby 之前**先做 teldrive Range 直读探测。
            # wait_mount_file 只证明「文件出现在 rclone 挂载上」，不证明读得出来；
            # 坏 chunk 的文件被通知给 Emby 后，Emby 的 ffprobe 走 FUSE 读会永久挂起，
            # 进程进 D 状态、连 SIGKILL 都杀不掉，容器变成既不能 stop 也不能 rm 的僵尸。
            # 这里用 HTTP Range 直连 teldrive（不经 FUSE），代价固定 1MiB 头 + 2MiB 尾，
            # 与文件大小无关；读不出来就抛错，这个文件永远进不了 Emby 的视野。
            if not a.no_probe:
                import tdprobe
                _ok, _det = tdprobe.probe_file(
                    fid, ext=os.path.splitext(job.get("name") or "")[1])
                if not _ok:
                    _led = os.path.join(HERE, "logs", "td_dead_files.jsonl")
                    try:
                        os.makedirs(os.path.dirname(_led), exist_ok=True)
                        with open(_led, "a", encoding="utf-8") as _f:
                            _f.write(json.dumps({
                                "ts": time.strftime("%F %T"), "file_id": fid,
                                "name": job.get("name"), "target": target,
                                "tmdb": job.get("tmdb"), "season": job.get("season"),
                                "ep": job.get("ep"), "detail": _det},
                                ensure_ascii=False) + "\n")
                    except OSError:
                        pass
                    raise RuntimeError(
                        "teldrive 直读探测失败，未通知 Emby（防 ffprobe 挂死）：%s %s"
                        % (job.get("name"), _det))
            wait_mount_file(target, a.mount_wait)
            r = run([sys.executable, os.path.join(HERE, "post_ingest.py"),
                     "--tmdb", str(job["tmdb"]), "--wait-emby", "20",
                     "--sample", "5", "--verify-file", job["name"],
                     "--strict-emby"], timeout=1800)
            nfo = os.path.splitext(target)[0] + ".nfo"
            if r.returncode != 0 and not job.get("upgrade"):
                raise RuntimeError("post_ingest 失败 rc=%d" % r.returncode)
            if not os.path.exists(nfo):
                raise RuntimeError("NFO 仍不存在：%s" % nfo)
            if r.returncode != 0:
                print("  洗版首轮 post_ingest rc=%d；NFO 已生成，旧版改名后再复核"
                      % r.returncode, file=sys.stderr)
            verify_actual_emby(
                job, int(manifest["actual_width"]),
                int(manifest["actual_height"]), timeout=180)

            dup_rel = (old_rel + ".dup") if job.get("pre_dup") else ""
            if job.get("upgrade"):
                if not job.get("pre_dup"):
                    write_journal(job, "before_old_rename", {"new_file_id": fid})
                    _, action = rename_old_to_dup(job)
                    write_journal(job, "old_renamed", {
                        "new_file_id": fid, "rename_action": action,
                        "dup_rel": old_rel + ".dup"})
                    if not wait_td_path_gone(old_rel, 180):
                        raise RuntimeError("旧路径仍在 teldrive：%s" % old_rel)
                    if not wait_td_path_present(old_rel + ".dup", 180):
                        raise RuntimeError("旧文件 .dup 未进入 teldrive")
                    dup_rel = old_rel + ".dup"
                wait_mount_file(target, 120)
                refresh_episode(job)
                r2 = run([sys.executable, os.path.join(HERE, "post_ingest.py"),
                          "--tmdb", str(job["tmdb"]), "--wait-emby", "20",
                          "--sample", "5", "--verify-file", job["name"],
                          "--strict-emby"],
                         timeout=1800)
                if r2.returncode != 0:
                    raise RuntimeError("洗版改名后 post_ingest 失败 rc=%d"
                                       % r2.returncode)
                verify_actual_emby(
                    job, int(manifest["actual_width"]),
                    int(manifest["actual_height"]), timeout=180)
                verify_upgrade_emby(job, timeout=300)

            # 2026-11-15：扫描前先等 sidecar 进 teldrive 文件树（异步上传竞态，
            # 见 wait_sidecars_in_td 的说明）。等不到不是致命错误：只跳过等待，
            # 后面的重试扫描仍会兜一次，但把缺失项打进日志便于定位。
            _miss = wait_sidecars_in_td(job, timeout=180)
            if _miss:
                print("  ⚠ sidecar 未进 teldrive 文件树：%s" % ", ".join(_miss)[:160],
                      file=sys.stderr)
            r = run([sys.executable, os.path.join(HERE, "resource_state.py"), "--scan"],
                    timeout=1800)
            if r.returncode != 0:
                raise RuntimeError("resource_state 失败 rc=%d" % r.returncode)
            # 洗完就把旧版字节删掉（E198）：改名只是让 Emby 看不见，
            # 不删会一直占 TG 配额，还让 Emby 集数 > 文件数（对账常驻 False）。
            # 放在 resource_state --scan 之后：drop_old 的体积安全闸要读到新文件台账行。
            if job.get("upgrade"):
                drop_superseded_dup(job, dup_rel or
                                    ((str(job.get("old_rel") or "") + ".dup")
                                     if job.get("old_rel") else ""))

            con = sqlite3.connect("file:%s?mode=ro" % A.TGDB, uri=True, timeout=30)
            keyrow = con.execute("select k from state_key where k=?",
                                 (job["key"],)).fetchone()
            rs = con.execute(
                "select stage,scraped_at,emby_at from resource_state where rel=?",
                (rel,)).fetchone()
            old_rs = None
            if dup_rel and not same_name:
                old_rs = con.execute(
                    "select stage from resource_state where rel=?", (old_rel,)).fetchone()
            con.close()
            # 2026-11-15：sidecar 竞态兜底。scraped_at 为空时补等 sidecar 进
            # 文件树再扫一次；两次都为空才判失败（一次上传抖动不该判 FAIL，
            # 那会让 runner 的 RESUME_FINISH 无限重跑同一集）。
            if rs and not rs[1]:
                _m2 = wait_sidecars_in_td(job, timeout=120)
                if not _m2:
                    print("  ↻ scraped_at 为空：sidecar 已进树，补扫一次",
                          file=sys.stderr)
                    if run([sys.executable, os.path.join(HERE, "resource_state.py"),
                            "--scan"], timeout=1800).returncode == 0:
                        con = sqlite3.connect("file:%s?mode=ro" % A.TGDB, uri=True,
                                              timeout=30)
                        rs = con.execute(
                            "select stage,scraped_at,emby_at from resource_state "
                            "where rel=?", (rel,)).fetchone()
                        con.close()
                else:
                    print("  ⚠ 重试后 sidecar 仍不在文件树：%s" % ", ".join(_m2)[:160],
                          file=sys.stderr)
            if not keyrow:
                raise RuntimeError("tg.db.state_key 未写入：%s" % job["key"])
            if not rs:
                raise RuntimeError("resource_state 未写入：%s" % rel)
            if not rs[1]:
                raise RuntimeError("resource_state.scraped_at 为空")
            if old_rs:
                raise RuntimeError("旧视频仍在 resource_state：%s" % old_rel)

            if job.get("upgrade"):
                write_journal(job, "complete", {
                    "new_file_id": fid, "dup_rel": dup_rel})
            GS.record_source_success(job.get("source_label"))
            queue_state = None
            try:
                queue_state = LQ.prune_job(job)
            except Exception as qe:                     # noqa: BLE001
                print("queue prune warning: %s" % str(qe)[:300],
                      file=sys.stderr)
            # 2026-11-14（E074）：这一集成功了 —— 清掉它的失败退避记录。
            # 否则源站恢复后，曾被判失败的集会被永久跳过。
            try:
                GS.clear_episode_failure(job.get("key") or job.get("video_id"))
            except Exception:                           # noqa: BLE001
                pass
            # 2026-11-14（E075）：上传完成，归还 TG session 租约。
            try:
                _slot = job.get("tg_session_slot")
                if _slot:
                    GS.slot_release(int(_slot), job.get("run_id"))
            except Exception:                       # noqa: BLE001
                pass
            # 成功样本：喂给 worker 健康度（连续失败会让节点退出有效产能）
            try:
                GS.record_outcome(job.get("run_id"), ok=True)
            except Exception:                                 # noqa: BLE001
                pass
            GS.release_claim(claim_path)
            try:
                _wk = GS.item_key(job)
                GS.release_item(_wk)
            except Exception:                                 # noqa: BLE001
                pass
            log_agent("done", "完成[%s] %s S%02dE%02d；file_id=%s；state_key=%s；"
                      "stage=%s；NFO=%s%s" % (
                          job.get("lane"), job["series"][:20], job["season"],
                          job["ep"], fid, job["key"], rs[0],
                          os.path.basename(nfo),
                          ("；旧版→" + dup_rel) if dup_rel else ""))
            print(json.dumps({
                "ok": True, "lane": job.get("lane"), "job_id": job["job_id"],
                "file_id": fid, "key": job["key"], "rel": rel, "stage": rs[0],
                "nfo": nfo, "dup_rel": dup_rel,
                "queue_remaining": (queue_state or {}).get("remaining"),
                "lane_done": LQ.is_done(job.get("lane")),
                "size": sum(p["plain_size"] for p in td_parts(fid)),
            }, ensure_ascii=False))
            return 0
    except Exception as e:                                 # noqa: BLE001
        rollback = ""
        if pre_dup_renamed:
            try:
                target_landed = bool(td_file_id(job.get("target_rel"), force=True))
            except Exception:                              # noqa: BLE001
                target_landed = True                       # PG 不明时不冒险回滚
            if not target_landed:
                try:
                    rollback = restore_dup(job)
                    write_journal(job, "rolled_back", {
                        "rollback": rollback, "error": str(e)[:500]})
                except Exception as re:                    # noqa: BLE001
                    rollback = "rollback_failed:%s" % str(re)[:200]
        if claim_path:
            try:
                d = json.load(open(claim_path, encoding="utf-8"))
                d["status"] = "failed"
                d["error"] = str(e)[:500]
                if rollback:
                    d["rollback"] = rollback
                save_claim(claim_path, d)
                if d.get("upgrade"):
                    write_journal(d, "failed", {"error": str(e)[:500]})
            except Exception:                              # noqa: BLE001
                pass
        log_agent("abort", "失败：%s%s" % (
            str(e)[:300], ("；旧版回滚=" + rollback) if rollback else ""))
        print(json.dumps({"ok": False, "error": str(e)[:500],
                          "rollback": rollback}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    sys.exit(main() or 0)
