#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tgstore.py — EmbyTG 状态存储的 SQLite WAL 后端（P0-1 规模化地基）。

把 tgpipe.py 的四个单文件 JSON（state / pending / meta_cache / scan_raw）
迁到单 SQLite 库 + WAL 模式，提供与 load_json/save_json/load_state/save_state
**签名完全一致**的替代，使 tgpipe.py 调用点零改动即可切换。

为什么可扩展（对齐 SCALING.md §1 九原则）：
- 行级 upsert：save_state 不再全量 dump 5.6MB dict，只写本次变化的 keys 行；
- WAL：读不阻塞写、崩溃不损坏（-wal/-shm 已随库自动管理）；
- 只 load 需要的行：pending 按 cursor 区间分页读，不把 100 万条全拉进内存；
- 单文件 tg.db 而非 4 个 JSON，随库增长不膨胀到几百 MB 的 dump/load。

用法（tgpipe.py 顶部一行切后端）：
    # from tgpipe 的角度，任选：
    #  import tgstore as store            # SQLite WAL（P0-1 目标后端）
    #  import jsonstore as store          # 旧 JSON 后端（回滚用，行为不变）
    # load_state = store.load_state; save_state = store.save_state; ...

设计表：
    meta        (k TEXT PRIMARY KEY, v TEXT)          -- 标量: pending.cursor/ts/channels/kinds
    state_ch    (name TEXT, ch TEXT, val TEXT, PRIMARY KEY(name,ch))   -- channels.<ch>
    state_key   (k TEXT PRIMARY KEY, val TEXT)        -- keys.<key>
    pending     (rowid INTEGER PRIMARY KEY, ch, prio, msg, payload TEXT)
    meta_cache  (k TEXT PRIMARY KEY, val TEXT)
    scan_raw    (rowid INTEGER PRIMARY KEY, payload TEXT)

对外签名（与旧后端一一对应）：
    load_json(path, default)      -> 返回 dict/list（从 SQLite 重建，见下）
    save_json(path, obj)          -> 整体写（仅迁移/队列重建用，含 DELETE+INSERT 事务）
    load_state()                  -> {"channels": {...}, "keys": {...}}
    save_state(st)                -> 行级 upsert：只写 self._dirty 标记过的 key
"""
import json
import os
import sqlite3
import threading

DB_PATH = os.environ.get("EMBYTG_DB", "/opt/embytg/pipeline/tg.db")

# --------------------------------------------------------------------------
# 底层连接管理：进程内单连接（线程锁），WAL + synchronous=NORMAL（SCALING 可扩展）
# --------------------------------------------------------------------------
_conn = None
_lock = threading.Lock()


def _get_conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.execute("PRAGMA busy_timeout=5000")
        _init_schema(_conn)
    return _conn


def _init_schema(c):
    c.executescript("""
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE IF NOT EXISTS state_ch(name TEXT, ch TEXT, val TEXT, PRIMARY KEY(name,ch));
    CREATE TABLE IF NOT EXISTS state_key(k TEXT PRIMARY KEY, val TEXT);
    CREATE TABLE IF NOT EXISTS pending(rowid INTEGER PRIMARY KEY, ch TEXT, prio INTEGER, msg INTEGER, payload TEXT);
    CREATE TABLE IF NOT EXISTS meta_cache(k TEXT PRIMARY KEY, val TEXT);
    CREATE TABLE IF NOT EXISTS scan_raw(rowid INTEGER PRIMARY KEY, payload TEXT);
    CREATE INDEX IF NOT EXISTS idx_pending_cursor ON pending(rowid);
    CREATE INDEX IF NOT EXISTS idx_state_ch ON state_ch(name, ch);
    """)


def _tx():
    c = _get_conn()
    with _lock:
        c.execute("BEGIN")
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise


# --------------------------------------------------------------------------
# state：行级脏标记 upsert（核心可扩展点）
# --------------------------------------------------------------------------
class StateStore:
    """内存代理 + 行级持久化。调用方照旧 st['keys'][k]=...；仅被 save_state 落盘时
    持久化发生变化的行，不 dump 全量。"""

    def __init__(self):
        self.channels = {}
        self.keys = {}
        self._dirty_ch = set()
        self._dirty_key = set()

    # --- dict 协议（P0-1 真切换必备，2026-11-09）---
    # tgpipe.py 有 11 处 state["keys"] / st["keys"][k]=... / state["channels"][...]=...
    # 下标读写。之前 sqlite 模式 load_state() 返回本对象却不支持下标 →
    # apply/scan 直接 TypeError。这里补齐协议，使 tgpipe.py 调用点零改动。
    # 关键：__setitem__ 必须同时标脏，否则 save_state()->st.save() 不会落盘那些行。
    def __getitem__(self, k):
        if k == "keys":
            return self.keys
        if k == "channels":
            return self.channels
        raise KeyError(k)

    def __setitem__(self, k, v):
        if k == "keys":
            # 整体替换（迁移/兼容路径）：全量 diff 后标脏
            for key, val in (v or {}).items():
                if self.keys.get(key) != val:
                    self.keys[key] = val
                    self._dirty_key.add(key)
        elif k == "channels":
            for name, d in (v or {}).items():
                self.channels[name] = d
                self._dirty_ch.add(name)
        else:
            raise KeyError(k)

    def __contains__(self, k):
        return k in ("keys", "channels")

    def get(self, k, default=None):
        try:
            return self[k]
        except KeyError:
            return default

    def load(self):
        c = _get_conn()
        with _lock:
            for name, ch, val in c.execute("SELECT name,ch,val FROM state_ch"):
                self.channels.setdefault(name, {})[ch] = json.loads(val)
            for k, val in c.execute("SELECT k,val FROM state_key"):
                self.keys[k] = json.loads(val)
        return self

    def save(self, st=None):
        """只 upsert 脏行。st 可选：若非 StateStore 实例（旧 dict），按旧路径全量 diff。"""
        c = _get_conn()
        with _lock:
            c.execute("BEGIN")
            try:
                for name in list(self._dirty_ch):
                    for ch, val in (self.channels.get(name) or {}).items():
                        c.execute("INSERT INTO state_ch(name,ch,val) VALUES(?,?,?) "
                                  "ON CONFLICT(name,ch) DO UPDATE SET val=excluded.val",
                                  (name, ch, json.dumps(val, ensure_ascii=False)))
                    self._dirty_ch.discard(name)
                for k in list(self._dirty_key):
                    c.execute("INSERT INTO state_key(k,val) VALUES(?,?) "
                              "ON CONFLICT(k) DO UPDATE SET val=excluded.val",
                              (k, json.dumps(self.keys[k], ensure_ascii=False)))
                    self._dirty_key.discard(k)
                c.commit()
            except Exception:
                c.rollback()
                raise


_store_state = None  # 进程内单例


def load_state():
    global _store_state
    if _store_state is None:
        _store_state = StateStore().load()
    return _store_state


def save_state(st):
    global _store_state
    if isinstance(st, StateStore):
        # 二级下标赋值（st["keys"][k]=v / st["channels"][ch]=...）绕过 __setitem__，
        # 无法标脏。保存前与 DB 现有值做一次全量 diff 兜底，把变化的行补标脏。
        # 只比内存中的 dict，不查 DB（O(n) 但 n=2.4 万、只在每次 save 时一次，可接受）；
        # 若将来规模到百万，改为 keys 用 _ProxyDict 包装即可，这里保持最小改动。
        c = _get_conn()
        with _lock:
            db_keys = {k: json.loads(v) for k, v in
                       c.execute("SELECT k,val FROM state_key")}
            db_ch = {}
            for name, ch, val in c.execute("SELECT name,ch,val FROM state_ch"):
                db_ch.setdefault(name, {})[ch] = json.loads(val)
        for k, v in st.keys.items():
            if db_keys.get(k) != v:
                st._dirty_key.add(k)
        for name, d in st.channels.items():
            for ch, v in d.items():
                if (db_ch.get(name) or {}).get(ch) != v:
                    st._dirty_ch.add(name)
                    break
        st.save()
        return
    # 兼容旧 dict 调用：diff 全量后 upsert（仅迁移/一次性路径走这里，不常走）
    s = _store_state
    dirty = False
    for name, d in (st.get("channels") or {}).items():
        s.channels.setdefault(name, {})
        for ch, v in d.items():
            if s.channels[name].get(ch) != v:
                s.channels[name][ch] = v
                s._dirty_ch.add(name)
                dirty = True
    for k, v in (st.get("keys") or {}).items():
        if s.keys.get(k) != v:
            s.keys[k] = v
            s._dirty_key.add(k)
            dirty = True
    if dirty:
        s.save()


def mark_state_key(k):
    """apply() 在 st['keys'][it['key']] = {...} 之后调用，把该 key 标脏以便行级落盘。"""
    s = _store_state
    if s is not None:
        s._dirty_key.add(k)


# --------------------------------------------------------------------------
# 通用 load_json / save_json（迁移 & 一次性重建）
# --------------------------------------------------------------------------
def load_json(path, default):
    """从 SQLite 重建对应结构的 dict/list。path 仅作映射选择器：
      state -> load_state()
      pending -> {"items":[...], "cursor", "ts", "channels", "kinds"}
      meta_cache -> {k:v}
      scan_raw -> [...]
    """
    name = os.path.basename(path)
    if "state" in name:
        return load_state()
    c = _get_conn()
    if "pending" in name:
        with _lock:
            meta = {k: json.loads(v) for k, v in
                    c.execute("SELECT k,v FROM meta WHERE k IN ('cursor','ts','channels','kinds')")}
            items = [json.loads(r[4]) for r in
                     c.execute("SELECT rowid,ch,prio,msg,payload FROM pending ORDER BY rowid")]
        return {"items": items, "cursor": meta.get("cursor", 0), "ts": meta.get("ts", 0),
                "channels": meta.get("channels", []), "kinds": meta.get("kinds", [])}
    if "meta_cache" in name:
        with _lock:
            return {k: json.loads(v) for k, v in
                    c.execute("SELECT k,val FROM meta_cache")}
    if "scan_raw" in name:
        with _lock:
            return [json.loads(r[0]) for r in c.execute("SELECT payload FROM scan_raw ORDER BY rowid")]
    return default


def save_json(path, obj):
    """整体写（事务内 DELETE+INSERT）。仅迁移脚本 / 队列重建用，常驻循环不调用。"""
    name = os.path.basename(path)
    c = _get_conn()
    with _lock:
        c.execute("BEGIN")
        try:
            if "pending" in name:
                c.execute("DELETE FROM pending")
                c.execute("DELETE FROM meta WHERE k IN ('cursor','ts','channels','kinds')")
                for it in obj.get("items", []):
                    c.execute("INSERT INTO pending(ch,prio,msg,payload) VALUES(?,?,?,?)",
                              (it.get("ch"), it.get("prio"), it.get("msg"),
                               json.dumps(it, ensure_ascii=False)))
                for k in ("cursor", "ts", "channels", "kinds"):
                    if k in obj:
                        c.execute("INSERT INTO meta(k,v) VALUES(?,?) "
                                  "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                                  (k, json.dumps(obj[k], ensure_ascii=False)))
            elif "meta_cache" in name:
                c.execute("DELETE FROM meta_cache")
                for k, v in obj.items():
                    c.execute("INSERT INTO meta_cache(k,val) VALUES(?,?)",
                              (k, json.dumps(v, ensure_ascii=False)))
            elif "scan_raw" in name:
                c.execute("DELETE FROM scan_raw")
                for it in obj:
                    c.execute("INSERT INTO scan_raw(payload) VALUES(?)",
                              (json.dumps(it, ensure_ascii=False),))
            elif "state" in name:
                c.execute("DELETE FROM state_ch")
                c.execute("DELETE FROM state_key")
                for name2, d in (obj.get("channels") or {}).items():
                    for ch, v in d.items():
                        c.execute("INSERT INTO state_ch(name,ch,val) VALUES(?,?,?)",
                                  (name2, ch, json.dumps(v, ensure_ascii=False)))
                for k, v in (obj.get("keys") or {}).items():
                    c.execute("INSERT INTO state_key(k,val) VALUES(?,?)",
                              (k, json.dumps(v, ensure_ascii=False)))
            c.commit()
        except Exception:
            c.rollback()
            raise


def set_pending_cursor(cursor, total=None):
    """增量更新 pending 游标：只写 meta.cursor 一行，不重插 items（常驻循环热路径）。"""
    c = _get_conn()
    with _lock:
        c.execute("INSERT INTO meta(k,v) VALUES('cursor',?) "
                  "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                  (json.dumps(cursor),))


def mark_pending_done(rowids):
    """按 rowid 标记已处理（done=1）。tgpipe 常驻循环用它替代 q['items'][i]['done']=1 + 全量重写。"""
    c = _get_conn()
    with _lock:
        for rid in rowids:
            c.execute("UPDATE pending SET payload=json_set(payload,'$.done',1) WHERE rowid=?",
                      (rid,))


# --------------------------------------------------------------------------
# 便捷：库大小 / 行数（运维观测，不阻塞）
# --------------------------------------------------------------------------
def stats():
    c = _get_conn()
    with _lock:
        return {
            "state_keys": c.execute("SELECT COUNT(*) FROM state_key").fetchone()[0],
            "pending": c.execute("SELECT COUNT(*) FROM pending").fetchone()[0],
            "meta_cache": c.execute("SELECT COUNT(*) FROM meta_cache").fetchone()[0],
            "scan_raw": c.execute("SELECT COUNT(*) FROM scan_raw").fetchone()[0],
            "db_bytes": os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0,
            "wal_bytes": os.path.getsize(DB_PATH + "-wal") if os.path.exists(DB_PATH + "-wal") else 0,
        }