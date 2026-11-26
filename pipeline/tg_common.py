#!/usr/bin/env python3
"""tg_common.py —— 从 ysdq_import 抽出的公共工具（机器人下线后保留的部分）。

2026-11-20：@YSDQBOT 从项目移除，但这 5 个生产脚本一直在用它的工具函数：
fill_by_ids / repair_lowq / upgrade_episodes / upgrade_from_channels / drop_old。
把纯工具抽出来，机器人相关代码（BOT/fetch_item/parse_results/best_match）一起删掉。
"""
import json
import os
import re

STORE_CH = -1004413117262
MNT = '/opt/embytg/mnt/embytg'
GENRE_DIR = [('动画', 'Movies/动画片'), ('纪录', 'Movies/纪录片'),
             ('科幻', 'Movies/科幻片'), ('恐怖', 'Movies/惊悚片'),
             ('战争', 'Movies/战争片'), ('犯罪', 'Movies/犯罪片'),
             ('动作', 'Movies/动作片'), ('惊悚', 'Movies/惊悚片'),
             ('喜剧', 'Movies/喜剧片'), ('爱情', 'Movies/剧情片'),
             ('剧情', 'Movies/剧情片')]

# 2026-11-26（E215）：TMDB 类型 id → 中文库，**全项目唯一真源**。
# 以前 tgpipe.py 与 tools/fix_libs.py 各抄了一份，顺序还不一样（动作在恐怖前面），
# 于是「釜山行2：半岛」TMDB 首位类型是「恐怖」，却因为动作排在表里更靠前被归进动作片。
# 现在只在这里定义，其他脚本 import；顺序原则 = 具体类型优先、泛类型兜底，
# 同档内部按「更贴近内容」排序（恐怖在动作前，战争/犯罪也在动作前）。
GENRE_LIB_PRIO = [
    (16, '动画片'), (99, '纪录片'), (10402, '纪录片'),
    (878, '科幻片'), (14, '科幻片'),
    (27, '惊悚片'),            # 恐怖：只把它提到动作之前（2026-11-26 用户指出釜山行2 被归动作片）
    (28, '动作片'), (12, '动作片'), (37, '动作片'),
    (10752, '战争片'),
    (80, '犯罪片'),
    (53, '惊悚片'), (9648, '惊悚片'),
    (35, '喜剧片'),
    (18, '剧情片'), (10751, '剧情片'), (10749, '剧情片'),
    (36, '剧情片'), (10770, '剧情片'),
]
GENRE_LIB = dict(GENRE_LIB_PRIO)


def genre_dir(gids, default='其他片'):
    """按 GENRE_LIB_PRIO 取第一个命中的库名（gids 是 TMDB genre id 列表）。"""
    s = set(gids or [])
    for gid, lib in GENRE_LIB_PRIO:
        if gid in s:
            return lib
    return default


DEFAULT_DIR = 'Movies/其他片'
CAM_WORDS = ('抢先版', '枪版', '抢鲜版', 'TC版', 'TS版', 'HDTS', 'HDTC', '枪鲜', '预告片', '花絮')
def load_env():
    env = {}
    for p in ("/opt/embytg-runner/runner.env", "/opt/embytg/pipeline/.env"):
        if not os.path.exists(p):
            continue
        for line in open(p, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return env

def is_cam(text):
    """抢先版/枪版一律不收：画质和音质都不可用，进了库等于污染画质策略。"""
    t = text or ""
    return any(w in t for w in CAM_WORDS)

def pick_dir(desc):
    for kw, d in GENRE_DIR:
        if kw in desc:
            return d
    return DEFAULT_DIR

def norm_key(s):
    """比较用的归一化键：去掉标点/空格/版本词，只留中文与字母数字。

    实测必要性：库里叫「那夜凌晨，我坐上了旺角开往大埔的红VAN」，
    机器人给的是「那夜凌晨我坐上了旺角开往大埔的红VAN」——只差一个逗号，
    按原串比较就判不出重复，于是同一部片被转存了两遍（各 2.53GB）。
    """
    s = (s or "").strip()
    for w in ("原声版", "原声", "普通话", "国语", "粤语", "蓝光", "4K高清", "高清",
              "抢先版", "枪版", "加长版", "导演剪辑版"):
        s = s.replace(w, "")
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", s).lower()

def tmdb_lookup(title, kind="movie"):
    """按片名查 TMDB，返回 (年份, tmdb_id)；查不到返回 (0, 0)。

    为什么必须做：
      1) 目录/文件名不带年份时，Emby 会按名字模糊匹配到别的年份的片
         （实测「王牌 (2025)」被认成「王牌保镖 (2017)」）；
      2) 不带 `{tmdb=ID}` 锚点时，Emby 全靠猜。库里历史文件全是
         `片名 (年) {tmdb=ID} - 画质.ext`，所以新导入必须沿用同一格式。
    拿不到就返回 0 —— 调用方据此拒绝入库，宁缺毋错。
    """
    import urllib.parse
    import urllib.request
    try:
        from embytg_conf import cfg
        api = cfg("tmdb_api").rstrip("/")
        tk = cfg("tmdb_token")
    except Exception:                                          # noqa: BLE001
        return 0, 0
    q = urllib.parse.urlencode({"query": title, "language": "zh-CN", "include_adult": "false"})
    req = urllib.request.Request("%s/search/%s?%s" % (api, kind, q),
                                 headers={"Authorization": "Bearer %s" % tk,
                                          "accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
    except Exception:                                          # noqa: BLE001
        return 0, 0
    best = (0, 0)
    for it in (d.get("results") or [])[:8]:
        nm = (it.get("title") or it.get("name") or "").strip()
        if norm_key(nm) != norm_key(title):
            continue
        rd = it.get("release_date") or it.get("first_air_date") or ""
        y = int(rd[:4]) if rd[:4].isdigit() else 0
        if y and (y > best[0]):
            best = (y, int(it.get("id") or 0))
    return best

def tmdb_year(title, kind="movie"):
    return tmdb_lookup(title, kind)[0]

def safe_name(s):
    return re.sub(r'[\\/:*?"<>|]', " ", s).strip()

