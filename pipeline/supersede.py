#!/usr/bin/env python3
"""supersede.py —— 把被替换的旧低码率文件移出媒体库（不删除）。

用户 2026-11-19 决策 B：
    升级成功后，旧文件挪到同季的 `.superseded/` 子目录 —— TG 里文件还在（可随时
    挪回），但 Emby 不再扫描它，客户端里同一集只留高画质那一条。

为什么不用 Emby 多版本：实测 4.10 对同 base 不同 version 的剧集文件不合并，
会变成两条独立条目（幻视任务 E1-E7 实测）。所以只能在「重复条目」和「移出旧版」
之间选，用户选后者。

用法：
    python3 supersede.py --path "TV/国产剧/xxx/Season 01/xxx S01E01 - 1080p.mp4"
    python3 supersede.py --path ... --restore        # 挪回原位置
"""
import argparse
import os
import re
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
MNT = "/opt/embytg/mnt/embytg"
DB = os.path.join(HERE, "tg.db")
SUP = ".superseded"


def _host(rel):
    return os.path.join(MNT, rel)


def _db_update(old_rel, new_rel):
    con = sqlite3.connect(DB, timeout=30)
    try:
        con.execute("update resource_state set rel=?, path=? where rel=?",
                    (new_rel, _host(new_rel), old_rel))
        con.commit()
    finally:
        con.close()


def move(rel, restore=False):
    """在挂载上移动文件（及其 .nfo）。返回 (ok, new_rel, msg)。"""
    src = _host(rel)
    if not os.path.exists(src):
        return False, rel, "源文件不存在"
    season_dir = os.path.dirname(rel)
    name = os.path.basename(rel)
    if restore:
        if not rel.split("/")[-2].startswith(SUP):
            return False, rel, "不在 .superseded 里，无需恢复"
        dst_rel = os.path.join(os.path.dirname(season_dir), name).replace("\\", "/")
    else:
        dst_rel = os.path.join(season_dir, SUP, name).replace("\\", "/")
    dst = _host(dst_rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
        os.replace(src, dst) if os.path.exists(dst) is False else None
        if os.path.exists(src):
            os.rename(src, dst)
    except Exception as e:                                   # noqa: BLE001
        return False, rel, "移动失败 %s" % str(e)[:60]
    # 同步 NFO
    for ext in (".nfo", "-thumb.jpg", ".srt", ".ass"):
        s2 = os.path.splitext(_host(rel))[0] + ext
        if os.path.exists(s2):
            d2 = os.path.splitext(dst)[0] + ext
            try:
                os.rename(s2, d2)
            except Exception:                                # noqa: BLE001
                pass
    _db_update(rel, dst_rel)
    return True, dst_rel, "ok"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    ap.add_argument("--restore", action="store_true")
    a = ap.parse_args()
    ok, new_rel, msg = move(a.path.strip("/"), a.restore)
    print("  %s → %s  (%s)" % (a.path, new_rel, msg))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
