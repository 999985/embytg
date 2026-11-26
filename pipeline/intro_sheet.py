"""intro_sheet.py —— 给新入库电影生成「开头三帧」对照图，人工一眼看有没有片头广告。

为什么只做开头：用户口径（2026-11-27）——
  · 封面/开头几秒最容易挂广告，可以看
  · 中间插广告无法自动检测，只能人工发现后再换源
所以这里只抽 2s / 8s / 20s 三帧拼一张图，不做任何自动判定。

成本：每部电影读约 30MB（只解前 20 秒），电影每天十几部，可以忽略。

用法：
    python3 intro_sheet.py --days 1              # 近 1 天入库的电影
    python3 intro_sheet.py --path /mnt/embytg/Movies/动作片/xxx.mp4
输出：/opt/embytg/_intros/YYYY-MM-DD/<片名>.jpg（三帧横排，带时间标注）
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time

HERE = "/opt/embytg/pipeline"
OUT_ROOT = "/opt/embytg/_intros"
MOUNT = "/opt/embytg/mnt/embytg"


def grab(mp4, out_dir, stem):
    frames = []
    for t in (2, 8, 20):
        p = os.path.join(out_dir, "_f%d.jpg" % t)
        subprocess.run(["ffmpeg", "-v", "error", "-ss", str(t), "-i", mp4, "-frames:v", "1",
                        "-vf", "scale=480:-2", "-y", p], capture_output=True, timeout=120)
        if os.path.exists(p) and os.path.getsize(p) > 0:
            frames.append(p)
    if not frames:
        return None
    sheet = os.path.join(out_dir, stem + ".jpg")
    # 横向拼接；用 ffmpeg hstack（各帧同高）
    inputs = []
    for f in frames:
        inputs += ["-i", f]
    filt = "".join("[%d:v]" % i for i in range(len(frames))) + "hstack=inputs=%d" % len(frames)
    r = subprocess.run(["ffmpeg", "-v", "error"] + inputs + ["-filter_complex", filt, "-y", sheet],
                       capture_output=True, timeout=180)
    for f in frames:
        try:
            os.remove(f)
        except OSError:
            pass
    return sheet if os.path.exists(sheet) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--path")
    ap.add_argument("--limit", type=int, default=40)
    a = ap.parse_args()

    day = time.strftime("%Y-%m-%d")
    out_dir = os.path.join(OUT_ROOT, day)
    os.makedirs(out_dir, exist_ok=True)

    rows = []
    if a.path:
        rows = [a.path]
    else:
        con = sqlite3.connect("file:%s/tg.db?mode=ro" % HERE, uri=True, timeout=20)
        since = int(time.time()) - a.days * 86400
        rows = [r[0] for r in con.execute(
            "select rel from resource_state where kind='movie' and landed_at > ? order by landed_at desc limit ?",
            (since, a.limit))]
        con.close()

    ok = 0
    for rel in rows:
        p = rel if os.path.isabs(rel) else os.path.join(MOUNT, rel)
        if not os.path.exists(p):
            continue
        stem = os.path.splitext(os.path.basename(p))[0][:60]
        sheet = grab(p, out_dir, stem)
        if sheet:
            ok += 1
            print("  ✔ %s" % os.path.basename(sheet)[:60])
    print("  生成 %d 张，目录 %s" % (ok, out_dir))


if __name__ == "__main__":
    main()
