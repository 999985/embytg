#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""EmbyTG 统一配置读取模块（所有主力脚本共用）。

唯一明文真源：/opt/embytg/config.json（本地开发时为脚本同目录的 config.json）。
搜索顺序：环境变量 EMBYTG_CONFIG → /opt/embytg/config.json → 本模块同目录 config.json → 上级目录 config.json。
config.json 缺失/损坏时回落到本文件的 DEFAULTS（最后已知可用值），保证脚本永不因配置丢失而崩。

用法：
    from embytg_conf import cfg, C
    EMBY_TOKEN = cfg("emby_token")
    API_ID     = cfg("tg_api_id")

注意：业务规则（频道优先级 CHANNELS、GENRE_PRIO、库名清单、解析器）不放在这里，
      它们是代码逻辑，保留在 tgpipe.py 顶部；这里只放凭证 / 端点 / ID / 路径。
"""
import json, os

_HERE = os.path.dirname(os.path.abspath(__file__))
_PATHS = [
    os.environ.get("EMBYTG_CONFIG") or "",
    "/opt/embytg/config.json",
    os.path.join(_HERE, "config.json"),
    os.path.join(_HERE, "..", "config.json"),
]

# 最后已知可用值（config.json 缺失时的兜底；正常情况不会用到）
DEFAULTS = {
    "emby_url": "http://127.0.0.1:8096",
    "emby_token": "REPLACE_ME",
    "tg_api_id": 37496683,
    "tg_api_hash": "e068ab2154b7d206a6e284187674aa61",
    "tg_session": "/opt/embytg/.tg_string",
    "tg_session2": "/opt/embytg/.tg_string2",
    "tg_user_id": 7582337469,
    "store_channel_id": 4413117262,
    "tmdb_api": "https://api.themoviedb.org/3",
    "tmdb_token": "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyMzY5ZDFhMjM1ZGI3ODJkNjE4N2I4NTg2MmU3NTBiNyIsIm5iZiI6MTc4NjYyMjU3NS4xNjYwMDAxLCJzdWIiOiI2YTdkYjI2ZjliNjJkZjJiOGJjODkzYjUiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.dzzClKjNl7coF8osl0qiZRWPBuEkKP8edaPaGspBQ90",
    "todb_api": "https://theotherdb.org",
    "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "mount": "/opt/embytg/mnt/embytg",
    "movies_dir_id": "1e73f2db-11cd-4dd5-8983-9d18a21b63cb",
    "tv_dir_id": "505a7a0b-6e89-43c6-9233-c029de6c8ff5",
    "state_file": "/opt/embytg/pipeline/tg_state.json",
    "pending_file": "/opt/embytg/pipeline/tg_pending.json",
    "meta_cache_file": "/opt/embytg/pipeline/tg_meta_cache.json",
    "snapshot_file": "/opt/embytg/pipeline/tg_scan_raw.json",
    "lib_opts_before_nfo": "/opt/embytg/pipeline/lib_opts_before_nfo.json",
    "blocked_channels": ["tengxun4k"],
}


def _load():
    for p in _PATHS:
        if not p:
            continue
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and "emby_token" in d:
                return d
        except Exception:
            pass
    return {}


C = _load()


def cfg(key, default=None):
    """取配置：config.json → DEFAULTS → 调用方给的 default → None。"""
    v = C.get(key)
    if v is not None:
        return v
    if key in DEFAULTS:
        return DEFAULTS[key]
    return default


def store_channels():
    """TG 存储频道池，按优先级返回可用频道 id 列表。

    2026-11-15（E106）：原来 4413117262 散落在 embytg_conf / pc_register /
    tgpipe 三处写死。加一个频道要改三个文件，漏一个就是静默 bug。
    用户要求「不管账号、云电脑还是频道都要可扩展」。

    来源顺序：
      1. env TG_STORE_CH（逗号分隔）—— 节点机上的 runner.env 就是这么配的
      2. config/scale.json 的 store_channels.channels（enabled=true 的）
      3. config.json 的 store_channel_id（单值兜底）
    """
    import os as _os
    raw = (_os.environ.get("TG_STORE_CH") or "").strip()
    if raw:
        out = []
        for x in raw.replace(":", ",").split(","):
            x = x.strip()
            if x.lstrip("-").isdigit() and int(x) not in out:
                out.append(int(x))
        if out:
            return out
    try:
        import json as _json
        sp = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)),
                           "config", "scale.json")
        with open(sp, encoding="utf-8") as fh:
            sc = _json.load(fh) or {}
        chans = (sc.get("store_channels") or {}).get("channels") or []
        out = [int(c["id"]) for c in chans
               if isinstance(c, dict) and c.get("enabled", True) and c.get("id")]
        if out:
            return out
    except Exception:                                      # noqa: BLE001
        pass
    one = cfg("store_channel_id")
    return [int(one)] if one else []


def store_channel():
    """主频道（列表第一个）。需要单值的地方用它，不要写死 id。"""
    ch = store_channels()
    return ch[0] if ch else None


if __name__ == "__main__":
    print("config source:", next((p for p in _PATHS if p and os.path.exists(p)), "DEFAULTS(fallback)"))
    for k in ("emby_url", "emby_token", "tg_api_id", "store_channel_id", "tmdb_api"):
        v = cfg(k)
        print(k, "=", (v[:24] + "..." if isinstance(v, str) and len(v) > 40 else v))
