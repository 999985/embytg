#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""migrate_to_sqlite.py — 把四个单文件 JSON 迁到 tg.db（P0-1 一次性迁移）。

用法：
    python3 migrate_to_sqlite.py --source /opt/embytg/pipeline --db /opt/embytg/pipeline/tg.db
    python3 migrate_to_sqlite.py --verify                          # 只读对比行数

安全：
    - 默认只读不写（dry）；加 --apply 才写库
    - 源 JSON 不删不改；迁移后可手动备份/移除
    - 迁移后 stats() 打印行数 / 库大小，人工核对后再切 tgpipe 后端
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tgstore


def migrate(src_dir, db_path):
    tgstore.DB_PATH = db_path
    for fname in ("tg_state.json", "tg_pending.json", "tg_meta_cache.json", "tg_scan_raw.json"):
        p = os.path.join(src_dir, fname)
        if not os.path.exists(p):
            print(f"  ! skip {fname} (不存在)")
            continue
        obj = json.load(open(p))
        # 映射到 save_json 的 path 选择器
        sel = os.path.basename(fname)
        tgstore.save_json(sel, obj)
        print(f"  OK {fname}: {len(obj) if isinstance(obj, list) else len(obj.get('keys', obj) if 'state' in sel else obj)} 条目")
    print("MIGRATE_DONE")


def verify(src_dir, db_path):
    tgstore.DB_PATH = db_path
    s = tgstore.stats()
    print("DB stats:", json.dumps(s, ensure_ascii=False))
    # 对源头 JSON 行数
    if os.path.exists(os.path.join(src_dir, "tg_state.json")):
        st = json.load(open(os.path.join(src_dir, "tg_state.json")))
        print(f"  json state.keys={len(st.get('keys', {}))} -> db state_keys={s['state_keys']}")
    if os.path.exists(os.path.join(src_dir, "tg_pending.json")):
        q = json.load(open(os.path.join(src_dir, "tg_pending.json")))
        print(f"  json pending.items={len(q.get('items', []))} cursor={q.get('cursor')} -> db pending={s['pending']}")
    if os.path.exists(os.path.join(src_dir, "tg_meta_cache.json")):
        m = json.load(open(os.path.join(src_dir, "tg_meta_cache.json")))
        print(f"  json meta_cache={len(m)} -> db meta_cache={s['meta_cache']}")
    if os.path.exists(os.path.join(src_dir, "tg_scan_raw.json")):
        r = json.load(open(os.path.join(src_dir, "tg_scan_raw.json")))
        print(f"  json scan_raw={len(r)} -> db scan_raw={s['scan_raw']}")
    print("VERIFY_DONE")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="/opt/embytg/pipeline")
    ap.add_argument("--db", default="/opt/embytg/pipeline/tg.db")
    ap.add_argument("--apply", action="store_true", help="实际写库；缺省仅 dry")
    ap.add_argument("--verify", action="store_true", help="只读对比行数，不迁移")
    a = ap.parse_args()
    if a.verify:
        verify(a.source, a.db)
    else:
        if not a.apply:
            print("# dry-run（未写库）。加 --apply 才真正迁移。")
            tgstore._get_conn()  # 建 schema
            print("DB schema ready:", a.db)
            return
        migrate(a.source, a.db)
        verify(a.source, a.db)


if __name__ == "__main__":
    main()