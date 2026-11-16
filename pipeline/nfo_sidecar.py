#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nfo_sidecar.py —— 视频改名时同步搬走同名 .nfo / 海报（防孤儿 NFO，E016 防复发 2）

背景：`todb_titles.py` / `fix_naming.py` / `renumber_todb.py` 都只 `os.rename(视频)`，
没有跟随同名 `.nfo`。改名后 Emby 看到的是「视频新名 + NFO 旧名」→ NFO 变成孤儿，
标题退回文件名（E016/E019 的复发路径）。本模块把「视频 + 同名 sidecar」当一个整体搬。

用法（在改名处替换 os.rename）：

    from nfo_sidecar import rename_with_sidecars
    rename_with_sidecars(src, dst)      # 失败抛异常，由调用方 try/except 记录

sidecar 规则：同目录同 basename 的 `.nfo`、`-poster.jpg`、`-thumb.jpg`、`.srt/.ass` 等字幕。
只动**存在**的文件；任一步失败会尽力回滚已搬的 sidecar，保证不留半成品。
"""
import os
import shutil

SUFFIXES = ("", ".nfo", "-poster.jpg", "-poster.png", "-thumb.jpg",
            ".srt", ".ass", ".ssa", ".vtt", ".zh.srt", ".chs.srt", ".eng.srt")


def sidecars(path):
    """返回该视频已存在的同名附属文件列表（不含视频本身）"""
    base = os.path.splitext(path)[0]
    out = []
    for suf in SUFFIXES[1:]:
        p = base + suf
        if os.path.exists(p):
            out.append(p)
    return out


def rename_with_sidecars(src, dst, dry=False):
    """把 src 及其同名 sidecar 一起改名为 dst（目标目录可不同）。

    返回 (moved_sidecars, skipped) —— skipped 是目标已存在而跳过的 sidecar。
    任一失败：把已搬的 sidecar 搬回、视频搬回，然后抛异常。
    """
    if not os.path.exists(src):
        raise FileNotFoundError(src)
    if os.path.exists(dst):
        raise FileExistsError(dst)
    src_base, src_ext = os.path.splitext(src)
    dst_base, dst_ext = os.path.splitext(dst)
    pairs = []
    dup = dst.endswith(".dup")          # fix_naming 的 multiver：视频改成 xxx.mp4.dup
    for p in sidecars(src):
        suf = p[len(src_base):]
        pairs.append((p, (dst + suf) if dup else (dst_base + suf)))
    if dry:
        return [d for _s, d in pairs], []
    moved, skipped = [], []
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        os.rename(src, dst)
        for s, d in pairs:
            if os.path.exists(d):
                skipped.append(d)          # 目标已存在，不覆盖
                continue
            os.makedirs(os.path.dirname(d), exist_ok=True)
            os.rename(s, d)
            moved.append(d)
    except Exception:
        for d in moved:                    # 回滚 sidecar
            s = src_base + d[len(dst_base):] if not dup else src_base + d[len(dst):]
            try:
                if os.path.exists(d) and not os.path.exists(s):
                    os.rename(d, s)
            except Exception:
                pass
        try:                               # 回滚视频
            if os.path.exists(dst) and not os.path.exists(src):
                os.rename(dst, src)
        except Exception:
            pass
        raise
    return moved, skipped


def move_video(src, dst):
    """跨目录移动（同 rename，语义更清楚）；同样带 sidecar。"""
    return rename_with_sidecars(src, dst)


def copy_sidecars_only(src, dst):
    """只复制 sidecar（视频已用别的方式搬过时用）"""
    src_base, _ = os.path.splitext(src)
    dst_base, _ = os.path.splitext(dst)
    out = []
    for p in sidecars(src):
        d = dst_base + p[len(src_base):]
        if not os.path.exists(d):
            os.makedirs(os.path.dirname(d), exist_ok=True)
            shutil.copy2(p, d)
            out.append(d)
    return out
