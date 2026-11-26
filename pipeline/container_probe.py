"""container_probe.py —— 只读头尾 128KB，判断容器是否完整（零上行）。

为什么需要：频道转发（tgpipe）不下载文件，所以没有本地 ffprobe；而源站/频道
里确实存在「字节数对、但 mp4 没有 moov / mkv 没有 EBML」的坏文件
（实测 2026-11-26：16 条进库后 Emby 一直 Size=0，ffprobe 报 moov atom not found，
fragment 头和尾都没有 moov —— 文件本身坏了，不是慢）。

判据（只看两个 Range，约 128KB）：
  mp4/mov/m4v：头部 4 字节是 box 长度 + 'ftyp'；moov 在头 64KB 或尾 1MB 之一
  mkv/webm  ：头部 4 字节 == 1A 45 DF A3（EBML）
  其它容器  ：跳过（不误杀）

用法：
    python3 container_probe.py --fid <uuid>            # 单条
    python3 container_probe.py --recent-days 7         # 扫近 N 天新入库的
    python3 container_probe.py --recent-days 7 --json /tmp/bad.json
"""
import argparse
import json
import os
import subprocess
import sys
import urllib.request as ur

HERE = "/opt/embytg/pipeline"
API = "http://127.0.0.1:8080/v1/files/%s/content"
PSQL = ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive", "-t", "-A", "-F", "|", "-c"]


def token():
    r = subprocess.run(["bash", "/opt/embytg/api.sh"], capture_output=True, text=True)
    return (r.stdout or "").strip().splitlines()[-1].strip()


def rng(tok, fid, a, b, timeout=30):
    req = ur.Request(API % fid, headers={"Authorization": "Bearer " + tok, "Range": "bytes=%d-%d" % (a, b)})
    with ur.urlopen(req, timeout=timeout) as r:
        return r.read()


def check(tok, fid, size, name):
    ext = os.path.splitext(name)[1].lower()
    if ext not in (".mp4", ".mov", ".m4v", ".mkv", ".webm"):
        return None                                  # 不认识的容器不判
    # 头读 2MB 而不是 64KB：实测「红楼梦 S01E02」这类片 moov 在头 64KB 之外，
    # 只读 64KB 会把正常片判成 no_moov（2026-11-27 假阳性 165 条，ffprobe 本地
    # 明明能读出时长）。2MB 仍然很便宜，远小于一遍下载。
    try:
        head = rng(tok, fid, 0, 2 * 1024 * 1024 - 1, timeout=60)
    except Exception as e:                            # noqa: BLE001
        return ("head_read_fail", str(e)[:40])
    if ext in (".mkv", ".webm"):
        return None if head[:4] == b"\x1a\x45\xdf\xa3" else ("no_ebml", "头部不是 EBML")
    if len(head) < 12 or b"ftyp" not in head[:64]:
        return ("no_ftyp", "头部不是 ftyp box")
    if b"moov" in head:
        return None
    # 尾读只取 256KB，超时按 unknown 处理 —— 冷读 30kB/s 时 1MB 要 33 秒，
    # 拿超时当「坏文件」会得出 79% 全是坏的假结论（2026-11-26 实测 2133/2704）。
    try:
        # 尾读 4MB：moov 头可能落在离文件尾 1-4MB 的位置（实测红楼梦 S01E02
        # 读 256KB/1MB 都没有 moov，读 4MB 才有）。
        tail = rng(tok, fid, max(0, size - 4 * 1024 * 1024), size - 1, timeout=90)
    except Exception as e:                            # noqa: BLE001
        return ("unknown", "尾读超时/失败: %s" % str(e)[:30])
    if b"moov" in tail:
        return None
    return ("no_moov", "头 2MB 与尾 4MB 都没有 moov")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fid")
    ap.add_argument("--recent-days", type=int, default=0)
    ap.add_argument("--json")
    a = ap.parse_args()
    tok = token()
    rows = []
    if a.fid:
        rows = [x.split("|") for x in subprocess.run(
            PSQL + ["select id, size::bigint, name from files where id::text like '%s%%';" % a.fid],
            capture_output=True, text=True).stdout.strip().splitlines() if "|" in x]
    elif a.recent_days:
        rows = [x.split("|") for x in subprocess.run(
            PSQL + ["select id, size::bigint, name from files where kind='file' "
                    "and mime_type like 'video/%%' and created_at > now() - interval '%d days';" % a.recent_days],
            capture_output=True, text=True).stdout.strip().splitlines() if "|" in x]
    bad = []
    for fid, size, name in rows:
        res = check(tok, fid, int(size), name)
        if res:
            bad.append({"id": fid, "name": name, "size": int(size), "why": res[0], "detail": res[1]})
    from collections import Counter
    cnt = Counter(b["why"] for b in bad)
    print("  检查 %d 条，异常 %d 条：%s" % (len(rows), len(bad), dict(cnt)))
    real = [b for b in bad if b["why"] in ("no_ftyp", "no_moov", "no_ebml")]
    print("  其中确定坏容器 %d 条（no_ftyp/no_moov/no_ebml）" % len(real))
    for b in real[:15]:
        print("    %-10s %-44s %.2fGB" % (b["why"], b["name"][:44], b["size"] / 1e9))
    if a.json:
        json.dump(bad, open(a.json, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print("  明细: %s" % a.json)


if __name__ == "__main__":
    main()
