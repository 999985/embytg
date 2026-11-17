#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tdprobe.py —— 用 teldrive 的 HTTP Range 直读懂「这个文件到底能不能读」，不经过 FUSE。

为什么需要它（E028 的真根因）：
  入库流程是 `register_from_manifest()` → `wait_mount_file()` → `post_ingest.py`（通知 Emby）。
  `wait_mount_file` 只证明「文件出现在 rclone 挂载上」，**不证明读得出来**。
  于是坏文件（TG 侧 chunk 失效/限流）照样被通知给 Emby → Emby 的 ffprobe 去 FUSE 读
  → 读请求永久 pending → ffprobe 进 D 状态 → 连 SIGKILL 都杀不掉 → 容器变僵尸。
  实测过一次：`docker restart/kill/rm -f` 三种全报 "did not receive an exit event"。

  正确的拦截点在「通知 Emby 之前」。teldrive 有原生 Range 端点：
      GET /api/v1/files/{id}/content   Range: bytes=0-1048575
      → 206  Content-Range: bytes 0-1048575/10437185970
  走 HTTP 直连，不经过 FUSE，坏文件会在超时内失败（而不是挂死内核）。

探测策略（对齐 aetrix-emby-portal 的远程探测口径）：
  第一跳：头部 1 MiB（PROBE_HEAD）。8 MiB 反而会触发 ffprobe "File ended prematurely"，别调大。
  第二跳：尾部 2 MiB（PROBE_TAIL），只对 moov 在文件尾的 MP4/MOV 有意义；从 Content-Range
          拿到总大小后才发。两跳都不读整个文件，代价与文件大小无关。

用法（自检 / 批量体检）：
  python3 tdprobe.py --id <file_id>                  # 探一个
  python3 tdprobe.py --rel "TV/.../xxx.mp4"          # 按路径探（先查 id）
  python3 tdprobe.py --sweep --since-hours 48        # 扫最近登记的文件并隔离坏件
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
RCLONE_CONF = "/opt/embytg/config/rclone.conf"
TD_BASE = os.environ.get("TD_BASE", "http://127.0.0.1:8080")
PROBE_HEAD = int(os.environ.get("PROBE_HEAD", str(1 * 1024 * 1024)))
PROBE_TAIL = int(os.environ.get("PROBE_TAIL", str(2 * 1024 * 1024)))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "25"))
PROBE_RETRIES = int(os.environ.get("PROBE_RETRIES", "2"))


def api_key():
    """从 rclone.conf 读 teldrive 的 api_key（与 rclone 用同一份，避免双份凭证）。"""
    try:
        txt = open(RCLONE_CONF, encoding="utf-8").read()
        m = re.search(r"\[embytg\][^\[]*?api_key\s*=\s*(\S+)", txt, re.S)
        if m:
            return m.group(1).strip()
    except OSError:
        pass
    return os.environ.get("TD_API_KEY", "")


def _req(path, rng, timeout):
    h = {"X-Api-Key": api_key(), "User-Agent": "Mozilla/5.0"}
    if rng:
        h["Range"] = rng
    r = urllib.request.Request(TD_BASE + path, headers=h)
    with urllib.request.urlopen(r, timeout=timeout) as f:
        return f.getcode(), dict(f.headers), f.read(65536)


def _one_range(fid, rng, timeout):
    """返回 (ok, http_code, total_size or None, detail)。"""
    ok, code, total, detail, _body = _one_range_body(fid, rng, timeout)
    return ok, code, total, detail


def _one_range_body(fid, rng, timeout):
    """同 _one_range，但把 body 一起返回（结构体检要用前 64KB 的字节）。"""
    try:
        code, hdrs, body = _req("/api/v1/files/%s/content" % fid, rng, timeout)
    except urllib.error.HTTPError as e:
        return False, e.code, None, "HTTP %s" % e.code, b""
    except Exception as e:                                  # noqa: BLE001
        return False, "ERR", None, "%s: %s" % (type(e).__name__, str(e)[:60]), b""
    cr = hdrs.get("Content-Range") or hdrs.get("content-range") or ""
    total = None
    m = re.search(r"/(\d+)\s*$", cr)
    if m:
        total = int(m.group(1))
    if code not in (200, 206):
        return False, code, total, "unexpected code %s" % code, b""
    if not body:
        return False, code, total, "empty body", b""
    return True, code, total, "range=%s got=%d" % (cr or "-", len(body)), body


MP4_EXT = (".mp4", ".m4v", ".mov")


def mp4_index_status(head, total):
    """走查 mp4 顶层 box，判断索引（moov）是否存在。

    为什么需要（E169）：`probe_file` 只证明「头尾能读出字节」，不证明「容器结构完整」。
    实测 `当星光坠入花海 S01E07`：324 MB 文件、头尾都能读、teldrive 侧 ok=true，
    但顶层 box 是 `ftyp(28) + free(8) + 垃圾 4 字节 + mdat`——moov 索引根本不存在，
    ffprobe 报 `moov atom not found`，Emby 永远拿不到 MediaStreams（stage=emby_nofprobe）。
    这类坏文件重试探测一万次都不会好，只能在通知 Emby 之前就判死并重新下行。

    判据（保守，宁可 unknown 也不误杀）：
      - 命中顶层 moov            → ok
      - box 尺寸非法 / 超出文件尾 → dead（截断的上传就是这样）
      - 走完已知 box 恰好等于文件尾且无 moov → dead（索引缺失）
      - 其它                     → unknown（moov 可能在文件尾的普通 mp4，不做判定）
    """
    off = 0
    n = len(head)
    boxes = []
    while off + 8 <= n:
        size = int.from_bytes(head[off:off + 4], "big")
        typ = head[off + 4:off + 8]
        hdr = 8
        if size == 1:
            if off + 16 > n:
                return "unknown", "truncated_largesize@%d" % off
            size = int.from_bytes(head[off + 8:off + 16], "big")
            hdr = 16
        elif size == 0:
            size = total - off
        if typ == b"moov":
            return "ok", "moov@%d" % off
        if size < hdr or off + size > total:
            return "dead", "bad_box@%d type=%r size=%d total=%d boxes=%s" % (
                off, typ, size, total, boxes[:5])
        boxes.append((typ.decode("latin1", "replace"), size))
        off += size
        if len(boxes) > 64:
            return "unknown", "too_many_boxes"
    if off >= total:
        return "dead", "no_moov_at_eof boxes=%s" % (boxes[:6],)
    return "unknown", "moov_not_in_head boxes=%s" % (boxes[:6],)


def probe_file(fid, size=None, timeout=PROBE_TIMEOUT, retries=PROBE_RETRIES, ext=""):
    """探一个 teldrive 文件能不能读，并对 mp4 家族做容器结构体检。

    返回 (ok, detail_dict)。ext 为空时跳过结构体检（保持旧行为）。"""
    if not fid:
        return False, {"err": "no_file_id"}
    last = ""
    for attempt in range(retries + 1):
        ok, code, total, detail, body = _one_range_body(
            fid, "bytes=0-%d" % (PROBE_HEAD - 1), timeout)
        if ok:
            if ext.lower() in MP4_EXT and body:
                st, why = mp4_index_status(body, total or size or 0)
                if st == "dead":
                    return False, {"err": "mp4_index_missing", "why": why,
                                   "total": total, "head": detail,
                                   "attempt": attempt + 1}
            if total and total > PROBE_TAIL:
                ok2, code2, _t2, d2 = _one_range(
                    fid, "bytes=%d-%d" % (total - PROBE_TAIL, total - 1), timeout)
                if not ok2:
                    last = "tail_read_fail(%s)" % d2
                    time.sleep(3 * (attempt + 1))
                    continue
                return True, {"total": total, "head": detail, "tail": d2,
                              "attempt": attempt + 1}
            return True, {"total": total or size, "head": detail, "tail": "skip",
                          "attempt": attempt + 1}
        last = "%s(%s)" % (detail, code)
        if isinstance(code, int) and 400 <= code < 500 and code != 408:
            break                                  # 4xx 重试无意义
        time.sleep(3 * (attempt + 1))
    return False, {"err": last or "unknown"}


STATE_FILE = os.path.join(HERE, "logs", "td_bad_state.json")


def _load_state():
    try:
        return json.load(open(STATE_FILE, encoding="utf-8"))
    except Exception:                                       # noqa: BLE001
        return {}


def _save_state(st):
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        json.dump(st, open(tmp, "w", encoding="utf-8"))
        os.replace(tmp, STATE_FILE)
    except Exception:                                       # noqa: BLE001
        pass


def find_id_by_rel(rel):
    """按相对路径查 teldrive 的 file id（走 PG，比遍历 API 快）。"""
    import subprocess
    name = rel.rstrip("/").rsplit("/", 1)[-1]
    try:
        out = subprocess.run(
            ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive",
             "-d", "teldrive", "-tAc",
             "select id from files where name=%s limit 1" % _q(name)],
            capture_output=True, text=True, timeout=60)
        v = (out.stdout or "").strip()
        return v.splitlines()[0] if v else ""
    except Exception:                                       # noqa: BLE001
        return ""


def _q(s):
    return "'" + str(s).replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id")
    ap.add_argument("--rel")
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--since-hours", type=float, default=48)
    ap.add_argument("--min-mb", type=int, default=100,
                    help="只探大于该体积的文件（MB）")
    ap.add_argument("--quarantine", action="store_true",
                    help="把读不出来的文件在同目录改名成 .bad（Emby 不识别该扩展名）")
    ap.add_argument("--head", type=int)
    ap.add_argument("--tail", type=int)
    ap.add_argument("--timeout", type=float)
    a = ap.parse_args()
    global PROBE_HEAD, PROBE_TAIL, PROBE_TIMEOUT
    if a.head:
        PROBE_HEAD = a.head
    if a.tail:
        PROBE_TAIL = a.tail
    if a.timeout:
        PROBE_TIMEOUT = a.timeout

    if a.id:
        ok, d = probe_file(a.id, timeout=a.timeout or PROBE_TIMEOUT)
        print(json.dumps({"id": a.id, "ok": ok, **d}, ensure_ascii=False))
        return 0 if ok else 2

    if a.rel:
        fid = find_id_by_rel(a.rel)
        if not fid:
            print(json.dumps({"rel": a.rel, "ok": None, "err": "file_id_not_found"},
                             ensure_ascii=False))
            return 2
        ok, d = probe_file(fid, timeout=a.timeout or PROBE_TIMEOUT,
                           ext=os.path.splitext(a.rel)[1])
        print(json.dumps({"rel": a.rel, "id": fid, "ok": ok, **d}, ensure_ascii=False))
        return 0 if ok else 2

    if a.sweep:
        import subprocess
        cut = int(time.time() - a.since_hours * 3600)
        # 递归 CTE 拼完整路径（files 表只有 parent_id，没有 path 列）
        # 关键：只输出「目标文件」一行（distinct on id + max lvl），否则递归会把
        # 父目录也带进结果集 —— 探测文件夹 id 必然 4xx，会被误报成坏件。
        sql = ("with recursive targets as ("
               " select id from files where kind='file' and size>%d"
               " and (created_at is null or created_at > to_timestamp(%d))),"
               " up as ("
               " select f.id, f.parent_id, f.name::text as path, 0 as lvl"
               " from files f join targets t on f.id=t.id"
               " union all"
               " select f.id, f.parent_id, f.name||'/'||u.path, u.lvl+1"
               " from files f join up u on f.id=u.parent_id)"
               " select distinct on (u.id) u.id||'|'||u.path"
               " from up u join targets t on t.id=u.id"
               " order by u.id, u.lvl desc"
               % (a.min_mb * 1048576, cut))
        out = subprocess.run(
            ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive",
             "-d", "teldrive", "-tAc", sql],
            capture_output=True, text=True, timeout=600)
        rows = [l for l in (out.stdout or "").strip().splitlines() if "|" in l]
        print("候选 %d 个（>%dMB / 近 %.0f 小时）" % (len(rows), a.min_mb, a.since_hours),
              flush=True)
        bad = []
        for line in rows:
            fid, rel = line.split("|", 1)
            fid = fid.strip()
            ok, d = probe_file(fid, ext=os.path.splitext(rel)[1])
            tag = "OK " if ok else "BAD"
            print("  %-3s %s" % (tag, rel[:100]), flush=True)
            if ok:
                continue
            item = {"id": fid, "rel": rel, **d}
            if a.quarantine:
                # 2026-11-13：隔离是不可逆的（改名后 Emby 就不认这个条目了），
                # 所以**必须连续两轮体检都失败**才动手 —— 单轮可能只是 TG 侧抖动
                # 或限流。状态记在 logs/td_bad_state.json，成功一次即清零。
                st = _load_state()
                n = int(st.get(fid, 0)) + 1
                st[fid] = n
                _save_state(st)
                if n < 2:
                    item["quarantined"] = "pending(第 %d 次失败，需连续 2 次)" % n
                    print("       -> 首次失败，暂不隔离（下次体检再确认）", flush=True)
                else:
                    src = os.path.join("/opt/embytg/mnt/embytg", rel.lstrip("/"))
                    dst = src + ".bad"
                    try:
                        if os.path.exists(src):
                            os.rename(src, dst)      # rclone 挂载内 = 元数据操作，不搬字节
                            item["quarantined"] = dst
                            st.pop(fid, None)
                            _save_state(st)
                            print("       -> 连续 2 次失败，已隔离为 .bad", flush=True)
                        else:
                            item["quarantined"] = "mount_path_missing"
                    except Exception as e:           # noqa: BLE001
                        item["quarantined"] = "rename_fail:%s" % str(e)[:50]
            else:
                st = _load_state()
                st.pop(fid, None)
                _save_state(st)
            bad.append(item)
        print("\n扫描 %d 个，坏件 %d 个" % (len(rows), len(bad)))
        if bad:
            p = os.path.join(HERE, "logs", "td_bad_files_%s.json"
                             % time.strftime("%Y%m%d_%H%M%S"))
            os.makedirs(os.path.dirname(p), exist_ok=True)
            json.dump(bad, open(p, "w", encoding="utf-8"),
                      ensure_ascii=False, indent=1)
            print("清单 -> %s" % p)
        return 0
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
