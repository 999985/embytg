#!/usr/bin/env python3
"""TG 频道 → teldrive → Emby 上片管线（零流量引用式转存 + TMDB/TODB 双源准入）。

  python3 tgpipe.py scan  [--limit 200] [--channels yfqh06] [--why] [--no-meta]
  python3 tgpipe.py apply [--limit 200] [--max-items 20] [--kinds movie,tv] [--dry]
  python3 tgpipe.py pull  --want "谷雨" [--limit 40]        # 双源 ID 反查频道供货（倒着来）
  python3 tgpipe.py libs

规则（用户 2026-11-08 定稿）：
  1. 只上 TMDB 或 TODB(TheOtherDB) 能匹配到的片 —— 双源准入，无 ID 一律不上；
  2. 同一条目同一画质只上一次（画质可多版本，同画质不重复）；
  3. 中文命名；命中 ID 写进文件名做刮削锚点；
  4. 上片顺序：先查这些 TG 频道 → 没有再找其他 Emby 服 → 先转发到自己频道 → 再刮削；
  5. 电影按 TMDB genre / 剧集按地区归入中文分类库（参考源站 goemby 命名）。
"""
from embytg_conf import store_channel
import argparse, asyncio, json, os, re, subprocess, sys, time, unicodedata, urllib.request, urllib.error, urllib.parse, uuid

from embytg_conf import cfg  # 统一配置真源: /opt/embytg/config.json

API_ID = cfg("tg_api_id")
API_HASH = cfg("tg_api_hash")
SESS = cfg("tg_session")

# 2026-11-14（E081）：tgpipe 与上传侧共用同一个 TG 账号池，
# 必须像 gha_next3 一样**先拿租约再用账号**，否则同一账号从两个进程
# 同时连接 → TG 报 AuthKeyDuplicated 并永久作废授权（当天已发生两次）。
# 转存本身是 forwardMessages，1 秒就结束，所以租约是**短时借用**，
# 不长期占用上传槽位。
TGPIPE_SLOT = int(os.environ.get("TGPIPE_SLOT", "3"))
# 扫描期跳过统计（用于收尾打印，避免「静默丢弃」）
SKIP_STATS = {}


def _lease_or_die(wait_sec=None):
    """申请 tgpipe 专用账号的租约。拿不到就退出，绝不硬连。

    2026-11-15：**加等待**。原来拿不到租约立刻退出，而四条上传 lane 会
    长时间占满 4 个账号，结果 tgpipe 每轮都秒退——零流量转存这条唯一
    「不花流量、不担风控」的通道就这样一直闲置（实测连续多轮 0 条）。
    现在改成在 wait_sec 内轮询，抢到上传任务的间隙就干（转存是
    forwardMessages，1 秒级），本来就该短借而不是长期占号。
    """
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import gha_state as GS
    except Exception:                                      # noqa: BLE001
        return None
    if wait_sec is None:
        try:
            wait_sec = int(os.environ.get("TGPIPE_LEASE_WAIT", "900"))
        except ValueError:
            wait_sec = 900
    # 2026-11-15（E137）：**run_id 必须是 ps 里看得见的名字。**
    #
    # 原来是 `tgpipe-<pid>`，而 gha_state._orphan_evidence() 判活靠
    # 「ps 的某个 argv 元素基名 == run_id」；tgpipe 的 argv 里只有
    # `tgpipe.py`，永远匹配不到 `tgpipe-12345`，.gha_claims/ 里也没有
    # 这个 run_id → 被判孤儿 → 别人可以立刻 os.remove 掉活着的锁，
    # 3 秒内就能双连同一账号（AuthKeyDuplicated 事故的经典形状）。
    rid = "tgpipe.py"
    deadline = time.time() + max(0, wait_sec)
    # 先立预约旗，再等：上传 lane 收尾后不会再抢这个号，我们才拿得到。
    # 旗的 TTL 覆盖整个等待窗口，崩了也会自动过期，不会永久占号。
    try:
        GS.slot_reserve(TGPIPE_SLOT, rid, ttl=max(300, wait_sec + 300))
    except Exception:                                  # noqa: BLE001
        pass
    while True:
        if GS.slot_lease(TGPIPE_SLOT, rid, ttl=600):       # 10 分钟足够
            return (GS, rid)
        if time.time() >= deadline:
            try:
                GS.slot_unreserve(TGPIPE_SLOT, rid)
            except Exception:                          # noqa: BLE001
                pass
            return None
        # 2026-11-15：**轮询间隔必须小于上传任务的间隙。**
        # 上传 lane 是一集接一集连着领活的，slot 释放到下一次 claim 之间
        # 往往只有几秒；原来 20s 一轮，几乎必然错过，实测等满 15 分钟仍是 0 条。
        # 3s 一轮才能真的"捡缝"，代价只是每 3 秒一次 O_EXCL 尝试。
        time.sleep(3)


def _release(held):
    if not held:
        return
    GS, rid = held
    try:
        GS.slot_release(TGPIPE_SLOT, rid)
    except Exception:                                      # noqa: BLE001
        pass
    try:
        GS.slot_unreserve(TGPIPE_SLOT, rid)
    except Exception:                                      # noqa: BLE001
        pass

STORE_CH = cfg("store_channel_id")
USER_ID = cfg("tg_user_id")

EMBY = cfg("emby_url")
EMBY_TOKEN = cfg("emby_token")
MOVIES_DIR = cfg("movies_dir_id")
TV_DIR = cfg("tv_dir_id")
STATE = cfg("state_file")
META_CACHE = cfg("meta_cache_file")
PENDING = cfg("pending_file")  # 全量待上片队列（一次 scan 生成，分批 apply 消费）
SNAPSHOT = cfg("snapshot_file")  # 频道解析结果快照（重跑免再拉 TG 全历史）
ALLOW_MULTIPART = False
# 禁止转发的频道（ChatForwardsRestrictedError）：无法引用式转发，直接标记 done 跳过
BLOCKED_CHANNELS = set(cfg("blocked_channels", ["AE9527x"]))

# 双源凭证 / 端点
TMDB_TK = cfg("tmdb_token")
TMDB_API = cfg("tmdb_api")
TODB_API = cfg("todb_api")
UA = cfg("ua")

# 中文分类库（与 fix_libs.py 建库一致）
MOVIE_LIBS = ['剧情片', '喜剧片', '动作片', '惊悚片', '犯罪片', '科幻片', '动画片', '纪录片', '战争片', '其他片']
TV_LIBS = ['国产剧', '日韩剧', '欧美剧', '港台剧', '国漫', '日漫', '综艺']

# TMDB 电影 genre id -> 中文库（明确类型优先，泛类型兜底，避免 53/9648 吸走动作片）
GENRE_PRIO = [
    (16, '动画片'), (99, '纪录片'), (10402, '纪录片'),
    (878, '科幻片'), (14, '科幻片'),
    (28, '动作片'), (12, '动作片'), (37, '动作片'),
    (27, '惊悚片'), (80, '犯罪片'), (35, '喜剧片'), (10752, '战争片'),
    (53, '惊悚片'), (9648, '惊悚片'),
    (18, '剧情片'), (10751, '剧情片'), (10749, '剧情片'), (36, '剧情片'), (10770, '剧情片'),
]

# (用户名, 优先级, 解析器)
CHANNELS = [
    ("yfqh06", 1, "movie_hash"),      # 4K 电影，大文件切片成相册
    ("yingji", 2, "movie_douban"),    # 1080 电影，豆瓣信息块
    ("DYPD_3", 3, "movie_angle"),     # 1080 电影，老片库
    ("tengxun4k", 4, "tv_hash"),      # 剧集，同集 4K + 1080
    ("luolimeimei", 5, "tv_book"),    # 剧集，单集带剧名
    ("AE9527x", 6, "tv_album"),       # 剧集，相册打包 E21-30
    ("yingshi9999", 7, "tv_album"),   # 剧集，相册 + 独立标题
]

PG = ["docker", "exec", "-i", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive",
      "-v", "ON_ERROR_STOP=1", "-At"]


# ---------------- 基础 ----------------
def sql(q):
    p = subprocess.run(PG, input=q, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"psql: {p.stderr.strip()}")
    return p.stdout.strip()


def qq(s):
    return "'" + str(s).replace("'", "''") + "'"


def emby(path, method="GET", body=None, timeout=120):
    url = f"{EMBY}{path}{'&' if '?' in path else '?'}api_key={EMBY_TOKEN}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", "X-Emby-Token": EMBY_TOKEN})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return json.loads(raw) if raw.strip() else None


# E008 / SCALING.md P0-2: POST /Library/Media/Updated 按精确文件路径增量通知 Emby，
# 替代 O(全库) 的 /Library/Refresh。4.10.2.0 实测: 204 + 日志
# "LibraryMonitor: <path> will be refreshed."，且不触发全库 ScheduledTask。
# 回滚: EMBYTG_REFRESH_MODE=full（或 config.json emby_refresh_mode=full）即回落旧行为。
REFRESH_MODE = os.environ.get("EMBYTG_REFRESH_MODE") or cfg("emby_refresh_mode", "incremental")
NOTIFY_BATCH = int(os.environ.get("EMBYTG_NOTIFY_BATCH", "100"))
NOTIFY_LOG = os.path.join(os.path.dirname(PENDING), "notify_pending.jsonl")


def emby_media_updated(paths):
    """按精确文件路径增量通知 Emby，O(新增) 而非 O(全库)。
    失败只记日志 + 落 notify_pending.jsonl（append, 断点可续），不阻断上片（文件已登记）。"""
    paths = [p for p in paths if p]
    if not paths:
        return
    if REFRESH_MODE == "full":
        emby("/Library/Refresh", "POST", {})
        print(f"# [full 回落] /Library/Refresh（本批新增 {len(paths)} 路径）", flush=True)
        return
    ok, fail = 0, []
    # 2026-11-20 根因：`POST /Library/Media/Updated` **建不出新 Episode**。
    # 实测 20 条已登记文件在 Emby 全查不到（reconcile 报 episode_missing_emby），
    # 而 TargetedScan 插件（/Library/ScanPaths）按文件路径送扫能 Created=1。
    # 所以通知出口统一走 emby_scan.scan_paths —— 与收尾链路口径一致。
    try:
        import emby_scan
        res = emby_scan.scan_paths(paths)
        ok = int(res.get("Created", 0)) + int(res.get("Refreshed", 0))
        if res.get("Failed") or res.get("PathNotFound"):
            fail.extend(paths)
            print(f"  ! ScanPaths 部分失败: {res}", file=sys.stderr, flush=True)
        else:
            print(f"# 送扫 {len(paths)} 条 → Created={res.get('Created')} "
                  f"Refreshed={res.get('Refreshed')} skipped={res.get('skipped', 0)}", flush=True)
    except Exception as e:
        fail.extend(paths)
        print(f"  ! ScanPaths 失败 {type(e).__name__}: {str(e)[:120]}",
              file=sys.stderr, flush=True)
    if fail:
        try:
            with open(NOTIFY_LOG, "a", encoding="utf-8") as f:
                for p in fail:
                    f.write(json.dumps({"ts": int(time.time()), "path": p}) + "\n")
        except Exception as e:
            print(f"  ! 写 notify_pending 失败: {e}", file=sys.stderr, flush=True)
    print(f"# 增量通知 Emby: {ok}/{len(paths)} 路径"
          + (f"（失败 {len(fail)} 已落 {NOTIFY_LOG}）" if fail else ""), flush=True)


def http_json(url, bearer=None, timeout=25):
    h = {"accept": "application/json", "User-Agent": UA}
    if bearer:
        h["Authorization"] = "Bearer " + bearer
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"__err__": f"{type(e).__name__}: {e}"}


def load_json(path, default):
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except Exception:
            return default
    return default


def save_json(path, obj):
    tmp = path + ".tmp"
    json.dump(obj, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def load_state():
    st = load_json(STATE, {})
    st.setdefault("channels", {}); st.setdefault("keys", {})
    return st


def save_state(st):
    save_json(STATE, st)


# ---- P0-1 (US-004): 状态后端可切换，默认 json 与改动前完全一致 ----
# 上面四个函数是原始 JSON 实现，函数体保持字节级不变；此处不改其实现、不改
# STATE/PENDING/META_CACHE/SNAPSHOT 四个路径常量，也不改任何调用点的签名。
# 选择器在模块加载时只决定「四个入口绑定到哪个后端」：
#   - json（缺省）：四个名字仍指向上方原函数，缺省行为与改动前完全一致；
#   - sqlite：把 load_json/save_json/load_state/save_state 四个同名入口
#     重绑定为 tgstore 的同名函数（委托），传原路径常量即可，tgstore 内部
#     靠 os.path.basename(path) 选择表，无需改常量。
# import tgstore 严格放在 sqlite 分支内做延迟导入：json 模式不会加载它，
# 也就不会因 tgstore 的依赖 / 建 schema / _get_conn() 产生副作用或写库；
# 模块导入期亦不调用 tgstore._get_conn()。
# P0-1 真切换（2026-11-09 22:5x）：默认改为 sqlite。
# 前置已完成：4 个 JSON 权威内容回灌 tg.db（行数+内容逐项对齐、meta.cursor=1644、
# 业务表/水位未动、integrity ok）；tgstore.StateStore 补 dict 协议 + save_state 脏行兜底，
# json/sqlite 两模式 10 项内容 ALL_EQUAL=True，写入路径在隔离副本实测落盘。
# 一键回滚：EMBYTG_STORE=json（或恢复 .bak.p0_1_default_*）。
STORE_BACKEND = os.environ.get("EMBYTG_STORE", "sqlite").strip().lower()
if STORE_BACKEND not in ("json", "sqlite"):
    print("# WARN: 未知 EMBYTG_STORE=%r，回落 sqlite（P0-1 当前默认）" % STORE_BACKEND, flush=True)
    STORE_BACKEND = "sqlite"

if STORE_BACKEND == "sqlite":
    import tgstore
    load_json = tgstore.load_json        # (path, default)
    save_json = tgstore.save_json        # (path, obj)
    load_state = tgstore.load_state      # ()
    save_state = tgstore.save_state      # (st)


def norm_title(t):
    t = unicodedata.normalize("NFKC", t or "")
    return re.sub(r"[\s:：·・,，.。!！?？'\"“”‘’()（）\[\]【】<>《》/\\|~\-—_]+", "", t).strip().lower()


CN = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
NUM_RE = r"([0-9]{1,3}|[一二三四五六七八九十百]{1,5})"
AD_RE = re.compile(r"(注册|充值|首存|流水|彩金|娱乐城|博彩|电子游艺|棋牌|彩票|代理|返水|日赚|包赢|稳赚|"
                    r"押注|下注|视讯|捕鱼|极搜|JISOU|VPN|机场|翻墙|节点|订阅|广告|合作|客服)", re.I)
# 标题噪声：只删词本身，绝不吞相邻剧名（旧版 `#\S+` + `.*$` 会把「#折月亮」整串清空）
SHOW_NOISE_RE = re.compile(r"(4K[-－]?高码率|高码率|2160[pP]?|1080[pP]?|720[pP]?|"
                           r"4K|蓝光|全集|完结|已完结|抢先|HD|SD)", re.I)
# 导航尾巴（以锚点词开头，连同其后全部删除）
NAV_TAIL_RE = re.compile(r"\s*(需要|点击|其余|其他集数|持续|更新|更多|想看|私信|欢迎).*$")
# 已知话题标签（# + tag 词）整删；#剧名 不动
TAG_RE = re.compile(r"#\s*(电视剧|电影|综艺|动漫|美剧|韩剧|日剧|国产剧|连续剧|高清|"
                    r"4[kK]|1080|720|更新|全集|完结)\b", re.I)


def cn_num(s):
    if not s:
        return None
    if s.isdigit():
        return int(s)
    s = s.replace("零", "")
    total = 0
    if "百" in s:
        h, s = s.split("百", 1)
        total += (CN.get(h, 1) if h else 1) * 100
    if "十" in s:
        t, s = s.split("十", 1)
        total += (CN.get(t, 1) if t else 1) * 10
    if s:
        total += CN.get(s[0], 0)
    return total or None


def looks_ad(cap, dur, size):
    if size is not None and size < 120 * 1024 * 1024:
        return True
    if dur is not None and dur < 480:
        return True
    if cap and AD_RE.search(cap) and not re.search(r"第\s*\d+\s*集|S\d+E\d+|\bE\d{2}|《", cap):
        return True
    return False


def parse_quality(w, h, cap):
    """按真实分辨率判画质，**宽度优先**。

    2026-11-18：原来高度优先（`h>=1000 → 1080p`），于是 1920x800 这种
    2.40:1 的宽银幕 1080p 片被标成 720p（实测 @yingshi9999 有 7 条）。
    分档按宽度更稳：3840→2160p、1920→1080p、1280→720p，宽度缺失才用高度。
    """
    txt = (cap or "").lower()
    if w:
        if w >= 3000:
            return "2160p"
        if w >= 1700:
            return "1080p"
        if w >= 1200:
            return "720p"
    if h:
        if h >= 1800:
            return "2160p"
        if h >= 1000:
            return "1080p"
        if h >= 700:
            return "720p"
    if "#4k" in txt or "2160" in txt or "4k" in txt:
        return "2160p"
    if "#1080" in txt or "1080p" in txt:
        return "1080p"
    return "SD"


# ---------------- 电影解析 ----------------
def _year_from(cap, head):
    m = re.search(r"#\s*(\d{4})\s*年", cap)
    if m:
        return int(m.group(1))
    m = re.search(r"上映日期[:：]\s*(\d{4})", cap)
    if m:
        return int(m.group(1))
    m = re.search(r"[（(](\d{4})[)）]", head)
    if m:
        return int(m.group(1))
    return None


def _meta_fields(lines):
    meta = {}
    for ln in lines:
        for key, pat in (("plot_hdr", r"^(.*剧情简介.*)$"), ("genres", r"^类型[:：]\s*(.+)$"),
                         ("country", r"^制片国家/地区[:：]\s*(.+)$"), ("lang", r"^语言[:：]\s*(.+)$"),
                         ("release", r"^上映日期[:：]\s*(.+)$"), ("runtime", r"^片长[:：]\s*(.+)$"),
                         ("aka", r"^又名[:：]\s*(.+)$"), ("cast", r"^主演[:：]\s*(.+)$"),
                         ("director", r"^导演[:：]\s*(.+)$"), ("writer", r"^编剧[:：]\s*(.+)$"),
                         ("rating", r"^(?:豆瓣)?评分[:：]\s*(.+)$"), ("intro", r"^简介[:：]\s*(.+)$")):
            mm = re.match(pat, ln)
            if mm and key not in meta:
                meta[key] = mm.group(1).strip()
    return meta


def clean_movie_title(t):
    t = (t or "").strip(" #《》")
    t = re.sub(r"第\s*" + NUM_RE + r"\s*集.*$", "", t)
    t = NAV_TAIL_RE.sub("", t)
    t = TAG_RE.sub("", t)
    t = SHOW_NOISE_RE.sub("", t)
    t = re.sub(r"[（(][^)）]*[)）]", "", t)
    cut = re.split(r"[（(]\d{4}[)）]", t)[0]
    cut = re.split(r"\s{2,}|\s(?=[A-Za-z])", cut)[0]
    return cut.strip(" #《》-—")


def parse_movie(cap, style):
    lines = [l.strip() for l in (cap or "").split("\n") if l.strip()]
    if not lines:
        return None
    head = lines[0]
    title = None
    if style == "movie_angle":          # 《#狗狗的完美圣诞节 高清》#2026年加拿大最新电影
        m = re.search(r"《\s*#?\s*([^》]+?)\s*》", head)
        if m:
            title = re.sub(r"\s*(高清|4K|蓝光|HD)\s*$", "", m.group(1)).strip()
    if not title and style == "movie_hash":   # #生化危机爆发夜  #4K ...
        m = re.search(r"#\s*([^#]+?)\s{2,}", head)
        if m:
            title = m.group(1).strip()
    if not title:                        # 中文名 英文名 (2026)
        m = re.search(r"《\s*#?\s*([^》]+?)\s*》", head)
        if m:
            title = re.sub(r"\s*(高清|4K)\s*$", "", m.group(1)).strip()
        else:
            title = clean_movie_title(head)
    title = clean_movie_title(title) if title else None
    if not title or len(title) < 2:
        return None
    meta = _meta_fields(lines[1:])
    plot = meta.get("plot")
    if not plot:
        for i, ln in enumerate(lines):
            if "剧情简介" in ln:
                plot = " ".join(lines[i + 1:])[:1200].strip("· ").strip()
                break
    if not plot:
        plot = meta.get("intro", "")
    year = _year_from(cap, head)
    return {"title": title, "year": year, "genres": meta.get("genres"),
            "country": meta.get("country"), "lang": meta.get("lang"),
            "release": meta.get("release"), "runtime": meta.get("runtime"),
            "aka": meta.get("aka"), "cast": meta.get("cast"),
            "director": meta.get("director"), "writer": meta.get("writer"),
            "rating": meta.get("rating"), "plot": plot}


# ---------------- 剧集解析（清洗相册标题脏串）----------------
def parse_episode(cap):
    if not cap:
        return None
    t = cap.replace("\n", " ").strip()
    qual = "2160p" if re.search(r"#\s*4k|\b4K\b|2160p", t, re.I) else \
           ("1080p" if re.search(r"#\s*1080|1080p", t, re.I) else None)
    # 相册打包标题：《剧名》/剧名. E21-30
    m = re.search(r"^\s*([^.。]+)[.。]\s*E(\d+)\s*[-–~]\s*E?(\d+)", t, re.I)
    if m:
        title = clean_show_title(m.group(1))
        if not title:
            return None
        return {"title": title, "season": 1, "ep_start": int(m.group(2)),
                "ep_end": int(m.group(3)), "qual": qual}
    mr = re.search(r"第\s*" + NUM_RE + r"\s*[-–~至]\s*" + NUM_RE + r"\s*集", t)
    if mr:
        e1, e2 = cn_num(mr.group(1)), cn_num(mr.group(2))
    else:
        me = re.search(r"第\s*" + NUM_RE + r"\s*集(?:数)?", t)
        if not me:
            # 综艺/动漫分支：有「第X季」/「X季」+ 日期(8位)或「上/下」但无「第X集」
            # caption 例：《现在就出发第四季》20261108下 #4K #现在就出发 #综艺
            # 支持「第四季」「第4季」「第1季」「第一季」等中文数字序数
            season_match = re.search(r"(?:第\s*)?([一二三四五六七八九十]{1,3})\s*季", t)
            date_match = re.search(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)", t)  # YYYYMMDD；\b 在中文后失效，用 lookaround
            if season_match and (date_match or re.search(r"\b[上下]\b", t)):
                title = None
                mk = re.match(r"^\s*《([^》]+)》", t)
                if mk:
                    title = mk.group(1)
                else:
                    s = re.sub(r"第\s*[0-9一二三四五六七八九十]{1,3}\s*季", "", t)
                    s = NAV_TAIL_RE.sub("", s)
                    s = TAG_RE.sub("", s)
                    s = SHOW_NOISE_RE.sub("", s)
                    s = re.sub(r"[（(][^)）]*[)）]", "", s)
                    s = re.sub(r"^[#\s]+", "", s)
                    s = re.split(r"\s{2,}|\s+#", s)[0]
                    title = clean_show_title(s)
                if not title:
                    return None
                # 标题里若仍含第X季则去掉
                title = re.sub(r"第\s*[0-9一二三四五六七八九十]{1,3}\s*季", "", title)
                title = clean_show_title(title)
                if not title:
                    return None
                season = cn_num(season_match.group(1)) or 1
                # 2026-11-16（E136）：**日期编号的综艺不再入库，直接跳过。**
                #
                # 原来把 YYYYMMDD 的「月日」当集号：`《麻花特开心第二季》20250719上`
                # → `S02E719`，一晚上队列里就有 727 条这类条目（现在就出发/种地吧/
                # 脱口秀…）。它们会写进 teldrive 并建出 Emby 条目，编号与 TMDB
                # 完全对不上，属于「宁可没有，也不能错」的数据。
                # 日期→集号的正确做法要拿 TMDB 的分集播出日期反查，那是另一件事，
                # 在那之前一律跳过并计数，不写脏数据。
                if date_match:
                    SKIP_STATS["date_numbered"] = SKIP_STATS.get("date_numbered", 0) + 1
                    return None
                episode = 1 if "上" in t else 2
                half = "上" if "上" in t else ("下" if "下" in t else None)
                return {"title": title, "season": season, "ep_start": episode, "ep_end": episode,
                        "qual": qual, "air": None, "half": half}
            return None
        e1 = e2 = cn_num(me.group(1))
        # 兼容 luolimeimei「第五十三集53」：数字紧跟「集」且无空格才采信。
        # 避免「第09集 1080p」把 1080 当集号（曾产生 S01E108）。
        m2 = re.search(r"第\s*" + NUM_RE + r"\s*集([0-9]{1,3})(?!\d)", t)
        if m2:
            v = int(m2.group(2))
            if 1 <= v <= 200 and abs(v - e1) <= 1:
                e1 = e2 = v
    if e1 is None:
        return None
    # 剧名：优先《》，否则删掉集数范围描述 + 画质/状态噪声
    mk = re.match(r"^\s*《([^》]+)》", t)
    if mk:
        title = mk.group(1)
    else:
        s = re.sub(r"第\s*" + NUM_RE + r"\s*[-–~至]\s*" + NUM_RE + r"\s*集(?:数)?", "", t)
        s = re.sub(r"第\s*" + NUM_RE + r"\s*集.*$", "", s)
        s = NAV_TAIL_RE.sub("", s)
        s = TAG_RE.sub("", s)
        s = SHOW_NOISE_RE.sub("", s)
        s = re.sub(r"[（(][^)）]*[)）]", "", s)
        s = re.sub(r"^[#\s]+", "", s)
        s = re.split(r"\s{2,}|\s+#", s)[0]
        title = s
    title = clean_show_title(title)
    if not title:
        return None
    season, ms = 1, re.search(r"第\s*([0-9一二三四五六七八九十]{1,3})\s*季", title)
    if ms:
        season = cn_num(ms.group(1)) or 1
        title = re.sub(r"第\s*[0-9一二三四五六七八九十]{1,3}\s*季", "", title)
    title = clean_show_title(title)
    if len(title) < 2:
        return None
    return {"title": title, "season": season, "ep_start": e1, "ep_end": e2, "qual": qual}


def clean_show_title(t):
    t = (t or "").strip(" #《》-—.")
    t = NAV_TAIL_RE.sub("", t)
    t = TAG_RE.sub("", t)
    t = SHOW_NOISE_RE.sub("", t)
    t = re.sub(r"[（(][^)）]*[)）]", "", t)
    t = re.sub(r"\s*(电视剧|连续剧|剧集)\s*$", "", t)
    return t.strip(" #《》-—.")


# ---------------- 集号标准化：日期集号 → TMDB/TODB 标准 SxxExx ----------------
# 用户 m00001：综艺按日期编集，落盘成 S04E1001（10 月 01 日），Emby/TODB 都按标准
# 集号匹配元数据，1001 这种号谁都认不出。这里在双源准入拿到 TMDB ID 之后，
# 用 TMDB 季/集结构（TODB 与之逐条一致，实测 231620 五季 173/20/20/47/20 完全相同）
# 把集号换成标准 SxxExx；番外/先导片按 TODB 惯例落 S0。
_SEASON_EP_CACHE = {}
_TV_DETAIL_CACHE = {}


def tmdb_tv_detail(mid):
    if mid not in _TV_DETAIL_CACHE:
        d = http_json(f"{TMDB_API}/tv/{mid}?language=zh-CN", TMDB_TK)
        _TV_DETAIL_CACHE[mid] = d if isinstance(d, dict) and not d.get("__err__") else {}
    return _TV_DETAIL_CACHE[mid]


def tmdb_season_eps(mid, season):
    key = (mid, int(season))
    if key not in _SEASON_EP_CACHE:
        d = http_json(f"{TMDB_API}/tv/{mid}/season/{int(season)}?language=zh-CN", TMDB_TK)
        eps = []
        if isinstance(d, dict) and not d.get("__err__"):
            for e in d.get("episodes") or []:
                eps.append({"e": int(e.get("episode_number") or 0),
                            "air": e.get("air_date") or "", "name": e.get("name") or ""})
        _SEASON_EP_CACHE[key] = eps
    return _SEASON_EP_CACHE[key]


def _pick_ep(cands, half):
    if not cands:
        return None
    if half and len(cands) > 1:
        pref = [c for c in cands if half in (c["name"] or "")]
        if pref:
            cands = pref
    return sorted(cands, key=lambda c: (c.get("season") or 0, c["e"]))[0]


def normalize_tv_episode(it):
    """把「日期当集号」的条目换成标准 SxxExx。返回 True 表示改写过。"""
    ep = int(it.get("episode") or 0)
    season = int(it.get("season") or 1)
    air = it.get("air") or ""
    if not air and ep < 100:
        return False
    mid = it.get("mid") if it.get("src") == "tmdb" else None
    if not mid:
        return False
    cur = tmdb_season_eps(mid, season)
    if not air and any(e["e"] == ep for e in cur):
        return False                      # 本季真有这一集（长番 1000+ 集），不动
    cands = [dict(e, season=season) for e in cur if air and e["air"] == air]
    if not cands and ep >= 100:
        mmdd = "%04d" % ep
        cands = [dict(e, season=season) for e in cur
                 if e["air"][5:7] + e["air"][8:10] == mmdd]
    if not cands and air:
        # 先导片/特别篇：本季没有就按同年跨季找（TODB 惯例放 S0）
        detail = tmdb_tv_detail(mid)
        for s in sorted(detail.get("seasons") or [], key=lambda x: int(x.get("season_number") or 0)):
            sn = int(s.get("season_number") or 0)
            if sn == season:
                continue
            got = [dict(e, season=sn) for e in tmdb_season_eps(mid, sn) if e["air"] == air]
            if got:
                cands = got
                break
    hit = _pick_ep(cands, it.get("half"))
    if not hit:
        return False
    if hit["season"] != season or hit["e"] != ep:
        print(f'  # 集号标准化 {it.get("title")} {air or ep}: '
              f'S{season:02d}E{ep:02d} -> S{hit["season"]:02d}E{hit["e"]:02d}', flush=True)
        it["season"], it["episode"] = hit["season"], hit["e"]
        return True
    return False


# ---------------- 双源元数据（TMDB 官方 API + TODB）----------------
def name_ok(title, r):
    cand = norm_title(title)
    names = [norm_title(r.get("title") or r.get("name") or ""),
             norm_title(r.get("original_title") or r.get("original_name") or "")]
    for n in names:
        if not n:
            continue
        if cand == n or (len(cand) > 1 and (cand in n or n in cand)):
            return True
    return False


def tmdb_search(kind, title, year):
    ep = "movie" if kind == "movie" else "tv"
    q = urllib.parse.urlencode({"query": title, "language": "zh-CN", **({"year": year} if year else {})})
    d = http_json(f"{TMDB_API}/search/{ep}?{q}", TMDB_TK)
    if not isinstance(d, dict) or "__err__" in d:
        return {"err": str(d)[:120]}
    for r in (d.get("results") or [])[:6]:
        if not name_ok(title, r):
            continue
        date = r.get("release_date") or r.get("first_air_date") or ""
        y = int(date[:4]) if date[:4].isdigit() else None
        if year and y and abs(y - year) > 3:      # 明显错配（如 玫瑰战争→2011 玫瑰的战争）
            continue
        return {"src": "tmdb", "id": r.get("id"),
                "name": (r.get("title") or r.get("name") or r.get("original_title") or title).strip(),
                "year": y, "genres": r.get("genre_ids") or [],
                "country": r.get("origin_country") or [],
                "overview": (r.get("overview") or "")[:400]}
    return None


def todb_search(kind, title, year):
    q = urllib.parse.urlencode({"title": title, "page": 1, "page_size": 12,
                                **({"year": year} if year else {})})
    d = http_json(f"{TODB_API}/api/playback/video?{q}")
    if not isinstance(d, dict) or "__err__" in d:
        return None
    vt = "movie" if kind == "movie" else "tv"
    items = d.get("items") or []
    exact = [x for x in items if norm_title(x.get("video_title")) == norm_title(title) and x.get("video_type") == vt]
    hit = exact or [x for x in items if norm_title(x.get("video_title")) == norm_title(title)]
    if not hit:
        return None
    x = hit[0]
    air = x.get("date_air") or ""
    y = int(air[:4]) if air[:4].isdigit() else None
    if year and y and abs(y - year) > 3:
        return None
    return {"src": "todb", "id": x.get("video_id"), "name": x.get("video_title") or title,
            "year": y, "genres": [], "country": ["CN"], "overview": ""}


def meta_lookup(kind, title, year, cache):
    """双源准入：TMDB 优先，TODB 补漏；命中返回规范中文名+ID，未命中 None。"""
    ck = f"{kind}|{norm_title(title)}|{year or ''}"
    if ck in cache:
        return cache[ck]
    # 2026-11-16（E138）：**查询失败不能当「查无此片」写进永久缓存。**
    # 原来 tmdb_search 返回 {"err":...} 时直接降级成 None，随后 `cache[ck] = res`
    # 把 None 永久落盘；下次 scan 命中 `k not in cache` 就短路，直接判
    # 「无 TMDB/TODB ID，不上」——一次 429/断网就能让某个标题永远不再入库，
    # 而且无日志无告警，只能人工清 meta_cache。
    _tmdb_err = isinstance(res, dict) and res.get("err")
    if _tmdb_err:
        res = None
    if not res:
        res = todb_search(kind, title, year)
    if not res:
        # 一次清洗重试：去掉尾部数字/季号（重案六组4 → 重案六组 之类）
        t2 = re.sub(r"[0-9０-９]+$", "", title).strip()
        t2 = re.sub(r"第[0-9一二三四五六七八九十]+季$", "", t2).strip()
        if t2 and t2 != title and len(t2) >= 2:
            res = tmdb_search(kind, t2, year) or todb_search(kind, t2, year)
            if isinstance(res, dict) and res.get("err"):
                res = None
    # 只有「确实查过、确实没有」才落缓存；查询出错/超时不缓存，下次重试。
    if _tmdb_err:
        print("# 元数据查询出错，不写缓存（下次重试）: %s" % title, file=sys.stderr, flush=True)
    else:
        cache[ck] = res  # None 也缓存，避免反复查无结果的脏标题
    return res


# ---------------- 分类库 ----------------
def classify_lib(kind, meta):
    if kind == "movie":
        gids = meta.get("genres") or []
        for gid, lib in GENRE_PRIO:
            if gid in gids:
                return lib
        return "其他片"
    cs = meta.get("country") or []
    cs = [str(c).upper() for c in (cs if isinstance(cs, list) else [cs])]
    gids = meta.get("genres") or []
    # 2026-11-09 用户定：剧集新增「综艺 / 国漫 / 日漫」三个类型库，优先于国家库。
    #   10764=真人秀, 10767=脱口秀, 16=动画；其它地区动画（US/FR…）仍按国家分流。
    if 10764 in gids or 10767 in gids:
        return "综艺"
    if 16 in gids:
        if "CN" in cs:
            return "国漫"
        if "JP" in cs:
            return "日漫"
    if any(c in ("HK", "TW") for c in cs):
        return "港台剧"
    if any(c in ("JP", "KR") for c in cs):
        return "日韩剧"
    if any(c in ("US", "GB", "FR", "DE", "CA", "AU", "ES", "IT") for c in cs):
        return "欧美剧"
    return "国产剧"          # 5 频道以国产剧为主，TODB 补的也是国产剧


# ---------------- teldrive ----------------
def ensure_folder(name, parent_id=None):
    p = "parent_id=" + qq(parent_id) if parent_id else "parent_id is null"
    out = sql(f"select id from teldrive.files where kind='folder' and name={qq(name)} and {p} limit 1")
    if out:
        return out
    fid = str(uuid.uuid4())
    sql(f"""insert into teldrive.files(id,user_id,parent_id,name,kind,size,mod_time,generation)
            values('{fid}',{USER_ID},{qq(parent_id) if parent_id else 'null'},{qq(name)},'folder',0,now(),1)""")
    return fid


def register(parent_id, name, parts):
    """parts: [(message_id, size)]，按顺序作为 part_no 1..N。"""
    fid = str(uuid.uuid4())
    total = sum(s for _, s in parts)
    sql(f"""insert into teldrive.files(id,user_id,parent_id,name,kind,mime_type,size,
              hash_algorithm,hash_value,encryption,status,mod_time,generation)
            values('{fid}',{USER_ID},'{parent_id}',{qq(name)},'file','video/mp4',{total},
              'blake3','',false,'active',now(),1)""")
    rows = ",".join(f"('{fid}',{i},{STORE_CH},{mid},{sz},{sz},null,null)"
                    for i, (mid, sz) in enumerate(parts, 1))
    sql("insert into teldrive.file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size,checksum,salt) values "
        + rows)
    return fid, total



HOST_MNT = "/opt/embytg/mnt/embytg"


def _host_media_path(p):
    """容器口径 `/mnt/embytg/...` → 宿主机口径 `/opt/embytg/mnt/embytg/...`（H17/E141）。

    控制面进程跑在宿主机上，而有几处路径（`_landed`、Emby 通知）用的是**容器口径**。
    直接拿容器口径去 open() 会在宿主机根上新建影子目录，文件永远进不了真挂载 ——
    实测 tgpipe 的 NFO 就这么写了 24 个到 /mnt/embytg（192KB），Emby 一个都读不到。
    """
    if isinstance(p, str) and p.startswith("/mnt/embytg/"):
        return HOST_MNT + p[len("/mnt/embytg"):]
    return p


def _gen_episode_nfo(it, nfo_path):
    """给转存的集写最小可用 NFO：分集标题 + 剧集级基础信息。

    只写 Emby 需要的字段（title / season / episode / plot），
    完整刮削（演员、剧照、genre）仍由 nfo_gen / nfo_episodes_tmdb 负责。
    """
    import xml.sax.saxutils as _sx
    title = it.get("ep_title") or ""
    mid = it.get("mid")
    # 拿 TMDB 的分集名
    if not title and mid and it.get("season") is not None and it.get("episode") is not None:
        try:
            d = http_json("%s/tv/%s/season/%d?language=zh-CN"
                          % (TMDB_API, mid, int(it["season"])), TMDB_TK)
            for e in (d.get("episodes") or []):
                if int(e.get("episode_number") or 0) == int(it["episode"]):
                    title = e.get("name") or ""
                    break
        except Exception:                                  # noqa: BLE001
            pass
    # TMDB 占位名不要（「第 N 集」「Episode N」）
    if title and re.fullmatch(r"(第\s*\d+\s*集|Episode\s*\d+|短片\s*\d+)", title.strip()):
        title = ""
    if not title:
        title = it.get("title") or ""
    lines = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', "<episodedetails>"]
    lines.append("  <title>%s</title>" % _sx.escape(str(title or "")))
    if it.get("season") is not None:
        lines.append("  <season>%d</season>" % int(it["season"]))
    if it.get("episode") is not None:
        lines.append("  <episode>%d</episode>" % int(it["episode"]))
    if it.get("year"):
        lines.append("  <year>%s</year>" % _sx.escape(str(it["year"])))
    lines.append("</episodedetails>")
    os.makedirs(os.path.dirname(nfo_path), exist_ok=True)
    with open(nfo_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _file_qual(n):
    """从库内文件名解析画质；无法判定时返回 '*'（通配，宁可不上也不重复）。"""
    m = re.search(r"(2160p|1080p|720p)", n, re.I)
    if m:
        return m.group(1).lower()
    if re.search(r"2160|4K", n, re.I):
        return "2160p"
    if "1080" in n:
        return "1080p"
    if "720" in n:
        return "720p"
    return "*"


def _stream_qual(x):
    for s in (x.get("MediaStreams") or []):
        if s.get("Type") == "Video":
            h, w = s.get("Height") or 0, s.get("Width") or 0
            if h >= 1800 or w >= 3000:
                return "2160p"
            if h >= 1000:
                return "1080p"
            if h >= 700:
                return "720p"
            return "SD"
    return "*"


def existing_index():
    """一次性构建去重索引（避免每条 item 一次 psql / HTTP）。

    电影: set of (anchor, qual)；剧集: set of (anchor, season, ep, qual)。
    anchor 双轨：'id:<tmdb>' 抗中文名规范化/年份差异，'n:<归一标题>' 兜底。
    画质优先取文件名标注；无标注的老文件用 Emby MediaStreams 真实分辨率补全，
    避免 '*' 通配把「同片不同画质」也挡掉（用户允许多画质、只禁同画质两份）。
    """
    movies, eps = set(), set()
    emby_q = {}
    try:
        d = emby("/Items?Recursive=true&IncludeItemTypes=Movie,Episode"
                 "&Fields=Name,Path,ProviderIds,MediaStreams,ParentIndexNumber,IndexNumber,Type&Limit=3000")
        for x in (d or {}).get("Items", []):
            bn = os.path.basename(x.get("Path") or "")
            if bn:
                emby_q[bn] = _stream_qual(x)
    except Exception as e:
        print("# existing_index: Emby 查询失败，仅用文件名判定 " + str(e)[:80], file=sys.stderr)

    for n in sql("select name from teldrive.files where kind='file' and status='active'").splitlines():
        if n.lower().endswith(".nfo"):
            continue
        qual = _file_qual(n)
        if qual == "*":
            qual = emby_q.get(n, "*")
        mid = None
        mm = re.search(r"tmdb(?:id)?[-=](\d+)", n, re.I)
        if mm:
            mid = mm.group(1)
        title = re.split(r"\s*[（(]\d{4}|\s*[{[]tmdb|\s+S\d{2}E\d{2}", n)[0]
        nt = norm_title(title)
        anchors = []
        if mid:
            anchors.append("id:" + mid)
        if nt:
            anchors.append("n:" + nt)
        sem = re.search(r"S(\d{2})E(\d{2})", n)
        for a in anchors:
            if sem:
                eps.add((a, int(sem.group(1)), int(sem.group(2)), qual))
            else:
                movies.add((a, qual))
    return movies, eps


def _has(idx, anchors, qual, tail=()):
    for a in anchors:
        if (a,) + tail + (qual,) in idx or (a,) + tail + ("*",) in idx:
            return True
    return False


# ---------------- 扫描 ----------------
async def scan_channels(names, limit, state, full=False):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.types import DocumentAttributeVideo

    # 2026-11-14（E081）：先拿租约再连，避免与上传侧抢同一账号
    _held = _lease_or_die()
    if not _held:
        print("# 账号 %d 正被上传侧占用，tgpipe 本轮退出（不硬连）" % TGPIPE_SLOT,
              file=sys.stderr)
        return []
    c = TelegramClient(StringSession(open(SESS).read().strip()), API_ID, API_HASH)
    # E137：connect 及以下的任何异常都必须归还租约，否则锁留到 TTL，
    # 期间每轮白等，日志只打「正被上传侧占用」——像账号被别人占着，实际是自己的尸体。
    try:
        await c.connect()
    except BaseException:
        _release(_held)
        raise

    async def one(uname, prio, style):
        """单个频道的拉取+解析；返回该频道 parsed 条目列表。多频道由 gather 并发。"""
        try:
            ent = await c.get_entity(uname)
        except Exception as e:
            print(f"# {uname} 无法解析: {e}", file=sys.stderr)
            return []
        print(f"# [{uname}] 开始读取...", flush=True)
        last = 0 if full else int(state["channels"].get(uname, {}).get("last_msg") or 0)
        msgs = []
        try:
            if full:
                async for m in c.iter_messages(ent, reverse=True):
                    msgs.append(m)
            else:
                async for m in c.iter_messages(ent, limit=limit, reverse=False):
                    msgs.append(m)
        except Exception as e:
            print(f"# {uname} 读取失败: {e}", file=sys.stderr, flush=True)
            return []
        msgs = [m for m in msgs if m.id > last]
        msgs.sort(key=lambda m: m.id)
        print(f"# [{uname}] 拉到 {len(msgs)} 条（新于 last={last}, full={full}）", flush=True)
        gcap, gdocs = {}, {}
        for m in msgs:
            if not m.document:
                continue
            if m.grouped_id:
                gdocs.setdefault(m.grouped_id, []).append(m)
                if m.message:
                    gcap.setdefault(m.grouped_id, m.message)
        local = []
        for idx, m in enumerate(msgs):
            d = m.document
            if not d:
                continue
            w = h = dur = None
            for at in d.attributes or []:
                if isinstance(at, DocumentAttributeVideo):
                    w, h, dur = at.w, at.h, at.duration
            if w is None and not (d.mime_type or "").startswith("video"):
                continue
            cap = (m.message or "").strip()
            if not cap and m.grouped_id:
                cap = gcap.get(m.grouped_id, "")
            if not cap:
                for j in range(max(0, idx - 4), min(len(msgs), idx + 5)):
                    t = msgs[j].message or ""
                    if re.search(r"第\s*" + NUM_RE + r"\s*集|\.E\d+", t):
                        cap = t
                        break
            if looks_ad(cap, dur, d.size):
                continue
            qual = parse_quality(w, h, cap)
            base = {"ch": uname, "prio": prio, "msg": m.id, "size": d.size,
                    "w": w, "h": h, "dur": dur, "qual": qual, "cap": cap[:400]}
            if style.startswith("movie"):
                mv = parse_movie(cap, style)
                if not mv:
                    continue
                parts = sorted([x.id for x in gdocs.get(m.grouped_id, [])]) if m.grouped_id else [m.id]
                if m.id != parts[0]:
                    continue
                base.update(kind="movie", parts=parts, **mv)
                if len(parts) > 1 and not ALLOW_MULTIPART:
                    base["skip"] = f"多段切片({len(parts)}段)，跳过"
                local.append(base)
                continue
            ep = parse_episode(cap)
            if not ep:
                continue
            gm = sorted([x.id for x in gdocs.get(m.grouped_id, [])]) if m.grouped_id else [m.id]
            pos = gm.index(m.id) if m.grouped_id and m.id in gm else 0
            epno = ep["ep_start"] + pos
            if ep["ep_end"] and pos > (ep["ep_end"] - ep["ep_start"]):
                continue
            base.update(kind="tv", title=ep["title"], season=ep["season"], episode=epno, parts=[m.id],
                        air=ep.get("air"), half=ep.get("half"))
            if ep["qual"] == "2160p":
                base["qual"] = "2160p"
            local.append(base)
        return local

    chans = [(u, p, s) for u, p, s in CHANNELS if (not names or u in names) and u not in BLOCKED_CHANNELS]
    results = await asyncio.gather(*(one(u, p, s) for u, p, s in chans), return_exceptions=True)
    out = []
    for r in results:
        if isinstance(r, Exception):
            print(f"# 频道协程异常: {r}", file=sys.stderr)
        elif isinstance(r, list):
            out.extend(r)
    try:
        await c.disconnect()
    finally:
        _release(_held)      # E137: 放进 finally，异常也归还
    out.sort(key=lambda r: (r["prio"], r["msg"]))
    if SKIP_STATS:
        print("# 跳过统计: " + ", ".join("%s=%d" % kv for kv in sorted(SKIP_STATS.items())),
              flush=True)
    return out


# ---------------- 去重/命名/入库 ----------------
def item_anchors(it):
    """同一条目的多重锚点：TMDB ID 最可靠（抗中文名规范化/年份差异），归一标题兜底。"""
    out = []
    if it.get("src") == "tmdb" and it.get("mid"):
        out.append("id:" + str(it["mid"]))
    for t in (it.get("raw_title"), it.get("title"), it.get("meta_name")):
        if t:
            a = "n:" + norm_title(t)
            if a not in out:
                out.append(a)
    return out


def dedupe(items, state, idx=None):
    """同集唯一：剧集同集只保留最高画质，电影允许多画质版本；同画质不允许两份。
    判重锚点 = TMDB ID 优先 + state keys + 库内已有文件 + 本批次 seen。"""
    if idx is None:
        idx = existing_index()
    mv_idx, ep_idx = idx
    # state 里已登记条目的锚点集合：即使库内文件被删/改名也不会重复上片
    st_mv, st_ep = set(), set()
    for v in state["keys"].values():
        sa = []
        if v.get("src") == "tmdb" and v.get("mid"):
            sa.append("id:" + str(v["mid"]))
        nm = re.split(r"\s*[（(]\d{4}|\s*[{[]tmdb|\s+S\d{2}E\d{2}", v.get("name") or "")[0]
        if norm_title(nm):
            sa.append("n:" + norm_title(nm))
        q = _file_qual(v.get("name") or "")
        sem = re.search(r"S(\d{2})E(\d{2})", v.get("name") or "")
        for a in sa:
            if sem:
                st_ep.add((a, int(sem.group(1)), int(sem.group(2)), q))
            else:
                st_mv.add((a, q))
    # 同集只留最高画质（根治 Emby 同集出现两条条目；2026-11-09 修）
    _qr = {"2160p": 4, "4k": 4, "1440p": 3, "1080p": 2, "720p": 1, "sd": 0}
    _best = {}
    for _it in items:
        if _it.get("kind") == "movie":
            continue
        _k = (tuple(item_anchors(_it)), int(_it.get("season") or 0), int(_it.get("episode") or 0))
        _sc = (_qr.get((_it.get("qual") or "SD").lower(), 0), int(_it.get("size") or 0))
        _cur = _best.get(_k)
        if _cur is None:
            _best[_k] = (_sc, _it)
        elif _sc > _cur[0]:
            _cur[1]["skip"] = "同集低画质"
            _best[_k] = (_sc, _it)
        else:
            _it["skip"] = "同集低画质"
    kept, seen = [], set()
    for it in items:
        if it.get("skip"):
            continue
        anchors = item_anchors(it)
        tail = () if it["kind"] == "movie" else (int(it["season"]), int(it["episode"]))
        qual = (it.get("qual") or "SD").lower()
        cur = mv_idx if it["kind"] == "movie" else ep_idx
        st_cur = st_mv if it["kind"] == "movie" else st_ep
        # 主 key 用最强锚点（TMDB ID 优先），保证跨批次/跨年/跨改名也判重
        key = f'{it["kind"]}|{anchors[0]}|{tail}|{qual}'
        it["key"] = key
        if key in state["keys"] or key in seen:
            it["skip"] = "本管线已有"
            continue
        if _has(cur, anchors, qual, tail) or _has(st_cur, anchors, qual, tail):
            it["skip"] = "库内已有同画质"
            continue
        seen.add(key)
        kept.append(it)
    return kept


def _real_qual(it):
    """按真实分辨率复算画质，别信频道自称。

    2026-11-18（用户反馈 @luolimeimei 质量忽高忽低）实测：
    队列里 18 条标 `2160p`，其中 8 条 DocumentAttributeVideo 的 w/h 是
    1920x1080 / 1920x800 —— 频道把 1080p 片子写成 2160p 发出来。
    `parse_quality(w,h,cap)` 本来优先信 w/h，但扫描时若消息没带 video 属性
    就退回 caption（含 "4K" 字样），qual 就此被写歪并落进队列，之后再没人复算。

    这里在**落地命名前**用队列里的 w/h 再算一次：名字写真实画质，
    upgrade 车道才不会被"假 4K"骗住而不再升级。
    """
    w, h = it.get("w") or 0, it.get("h") or 0
    if w >= 1200 and h >= 600:
        real = parse_quality(w, h, "")
        if real and real != it.get("qual"):
            print("# 画质订正 %s m%s: 标签 %s → 实际 %s (%dx%d)"
                  % (it.get("ch"), it.get("msg"), it.get("qual"), real, w, h),
                  file=sys.stderr, flush=True)
            try:
                # 留痕给巡检看：频道自称 vs 实际分辨率，谁在虚标一目了然
                with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       "logs", "tgpipe_quality.jsonl"), "a",
                          encoding="utf-8") as _f:
                    _f.write(json.dumps({"ts": int(time.time()), "ch": it.get("ch"),
                                         "msg": it.get("msg"), "label": it.get("qual"),
                                         "real": real, "w": w, "h": h},
                                        ensure_ascii=False) + "\n")
            except Exception:                                # noqa: BLE001
                pass
            it["qual"] = real
            it["qual_corrected"] = "标签%s→实际%s(%dx%d)" % (
                it.get("qual_tag") or it.get("qual"), real, w, h)
    return it["qual"]


def file_name(it):
    """中文规范名 + TMDB ID 锚点；剧集 ID 写在文件夹名（Emby 精确识别 Series），文件名按 SxxExx。"""
    _real_qual(it)
    name = it.get("meta_name") or it["title"]
    year = it.get("year") or "0000"
    anchor = f' {{tmdb={it["mid"]}}}' if (it.get("src") == "tmdb" and it.get("mid")) else ""
    if it["kind"] == "movie":
        base = f"{name} ({year}){anchor}"
        return base, f'{base} - {it["qual"]}.mp4'
    folder = f"{name} ({year}){anchor}"
    return folder, f'{name} S{it["season"]:02d}E{it["episode"]:02d} - {it["qual"]}.mp4'


async def apply(items, state, dry=False, c=None, dst=None):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import FloodWaitError

    # 常驻循环模式：外部传入 c/dst，复用一条 Telethon 连接跑完全部批次
    own_client = c is None
    _held_apply = None
    if own_client:
        # 2026-11-15（E137）：**apply 也必须拿租约。** E081 只给 scan 加了租约，
        # 而真正连 TG 发消息的是 apply —— `ingest_sop.sh` 跑的就是
        # `apply --from-queue --loop --acct2 ...`。上传 lane 一旦此刻持有同一账号，
        # 两边同时建连 = AuthKeyDuplicated，账号当场作废。scan 拿了锁也没用。
        _held_apply = _lease_or_die()
        if not _held_apply:
            print("# 账号正被上传侧占用，apply 本轮放弃（不硬连）", file=sys.stderr, flush=True)
            return 0
        try:
            c = TelegramClient(StringSession(open(SESS).read().strip()), API_ID, API_HASH)
            await c.connect()
            async for d in c.iter_dialogs():
                if getattr(d.entity, "id", None) == STORE_CH:
                    dst = d.entity
                    break
        except BaseException:
            _release(_held_apply)
            raise

    async def send_fwd(m, nm):
        # 引用式转发（TG 服务器端复制，零本地字节）；遇限流按 TG 要求等待后重试
        delay = 0.35
        for attempt in range(6):
            try:
                return await c.send_message(dst, nm, file=m.document)
            except FloodWaitError as e:
                w = int(getattr(e, "seconds", 5)) + 2
                print(f'  ~ FloodWait {w}s（第 {attempt + 1} 次）', flush=True)
                await asyncio.sleep(w)
            except Exception:
                if attempt >= 2:
                    raise
                await asyncio.sleep(delay)
                delay *= 2
        raise RuntimeError("send_message 重试耗尽")

    ents, done = {}, []
    for it in items:
        try:
            folder, fname = file_name(it)
            parts = it.get("parts") or [it["msg"]]
            lib = it.get("lib") or ("其他片" if it["kind"] == "movie" else "国产剧")
        except Exception as e:
            print(f'  ! 跳过脏条目 {it.get("ch")}/m{it.get("msg")}: {e}', file=sys.stderr, flush=True)
            continue
        if dry:
            print(f'[dry] {it["ch"]}/m{it["msg"]} {it["size"]/1e9:5.2f}GB {it["qual"]:>6} '
                  f'{it["src"]}={it.get("mid")} lib={lib} parts={len(parts)} -> '
                  f'{"Movies" if it["kind"]=="movie" else "TV"}/{lib}/{folder}/{fname}')
            continue
        # 单条容错：任何一条限流耗尽 / 源消息失效 / 登记异常只跳过该条，
        # 不中断 4 小时无人值守常驻循环（cursor 断点续传 + existing_index 保证不重不漏）
        try:
            if it["kind"] == "movie":
                base_dir = ensure_folder(lib, MOVIES_DIR)
                parent = base_dir if len(parts) == 1 else ensure_folder(folder, base_dir)
            else:
                lib_dir = ensure_folder(lib, TV_DIR)
                show = ensure_folder(folder, lib_dir)
                parent = ensure_folder(f"Season {it['season']:02d}", show)
            if it["ch"] not in ents:
                ents[it["ch"]] = await c.get_entity(it["ch"])
            src = ents[it["ch"]]
            msgs = await c.get_messages(src, ids=parts)
            got = []
            sent_names = []
            for i, m in enumerate(msgs, 1):
                if not m or not m.document:
                    continue
                nm = fname if len(parts) == 1 else f"{folder} - cd{i}.mp4"
                sent = await send_fwd(m, nm)
                got.append((sent.id, sent.document.size))
                sent_names.append(nm)
                if len(parts) > 1:
                    register(parent, nm, [(sent.id, sent.document.size)])
                await asyncio.sleep(0.8)      # 温和节流：TG 对非 Premium 高频转发递增限流(848→1642s)，
                                               # 0.2s/条过急触发翻倍等待；提到 0.8s 平滑速率
            if not got:
                print(f'  ! {it["ch"]}/m{it["msg"]} 源消息失效', file=sys.stderr, flush=True)
                continue
            if len(parts) == 1:
                fid, total = register(parent, fname, got)
            else:
                fid, total = None, sum(s for _, s in got)
            state["keys"][it["key"]] = {"ch": it["ch"], "msg": it["msg"], "name": fname,
                                        "file_id": fid, "parts": [g[0] for g in got],
                                        "src": it.get("src"), "mid": it.get("mid"),
                                        "lib": lib, "ts": int(time.time())}
            if it["msg"] > int(state["channels"].get(it["ch"], {}).get("last_msg") or 0):
                state["channels"][it["ch"]] = {"last_msg": it["msg"]}
            save_state(state)
            print(f'  + {lib}/{fname}  {total/1e9:.2f}GB  parts={len(got)}  {it["src"]}={it.get("mid")}')
            # E008/P0-2: 记录精确落地路径（容器内口径 /mnt/embytg/...），供增量通知
            if it["kind"] == "movie":
                if len(parts) == 1:
                    it["_landed"] = [f"/mnt/embytg/Movies/{lib}/{fname}"]
                else:
                    it["_landed"] = [f"/mnt/embytg/Movies/{lib}/{folder}/{nm}"
                                     for nm in sent_names]
            else:
                it["_landed"] = [f"/mnt/embytg/TV/{lib}/{folder}/Season {it['season']:02d}/{fname}"]
            # 2026-11-14（E084）：转存必须**同步写 NFO**，否则 Emby 拿文件名当分集标题。
            # 实测：转存的《百年孤独 S02E01》在客户端显示「百年孤独 S02E01 - 2160p」，
            # 而下载上传那条路显示真实分集名。原因是 tgpipe 全程不生成 NFO，
            # Emby 只能从文件名推名称。海报不受影响（Series 级统一），但分集名很难看。
            try:
                # 2026-11-16（E141）：**路径口径要翻译。**
                # it["_landed"] 是给 Emby 通知用的**容器口径** `/mnt/embytg/...`，
                # 而本进程跑在宿主机上 —— 直接拿它去 open() 会在宿主机根上
                # 新建一个影子目录 /mnt/embytg（实测攒了 24 个 NFO、192KB），
                # NFO 永远到不了真挂载 /opt/embytg/mnt/embytg，Emby 也就永远读不到。
                # 这正是 E084「转存写 NFO」看起来生效、实际一次都没生效的真因。
                _lp = _host_media_path(it["_landed"][0])
                _p = os.path.dirname(_lp)
                _stem = os.path.splitext(os.path.basename(_lp))[0]
                _nfo = os.path.join(_p, _stem + ".nfo")
                if not os.path.exists(_nfo):
                    _gen_episode_nfo(it, _nfo)
            except Exception as _e:                        # noqa: BLE001
                print(f"  ! NFO 生成失败 {it.get('title')}: {type(_e).__name__}",
                      file=sys.stderr, flush=True)
            done.append(it)
        except Exception as e:
            print(f'  ! 登记失败 {it.get("ch")}/m{it.get("msg")}: {type(e).__name__}: {str(e)[:160]}',
                  file=sys.stderr, flush=True)
            continue
    if own_client:
        await c.disconnect()
        if _held_apply:
            _release(_held_apply)    # E137：apply 自建连接时必须归还租约
    return done


# ---------------- 倒着来：拿双源 ID 反查频道供货 ----------------
async def pull(want, kind, limit, cache):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    m = meta_lookup(kind, want, None, cache)
    if not m:
        print(f"# 「{want}」双源均未命中，不上片")
        return
    print(f'# 锚点 {m["src"]}={m["id"]} 中文名={m["name"]} 年份={m["year"]} lib={classify_lib(kind, m)}')
    c = TelegramClient(StringSession(open(SESS).read().strip()), API_ID, API_HASH)
    await c.connect()
    for uname, prio, style in CHANNELS:
        try:
            ent = await c.get_entity(uname)
        except Exception:
            continue
        hits = []
        async for msg in c.iter_messages(ent, search=m["name"], limit=limit):
            cap = (msg.message or "").strip()
            if not msg.document or not cap:
                continue
            # 严格复核：caption 首行/《》须含规范中文名
            head = cap.split("\n")[0]
            if norm_title(m["name"]) not in norm_title(head):
                continue
            hits.append((msg.id, msg.document.size, head[:60]))
        if hits:
            print(f'  [{uname}] {len(hits)} 条:')
            for mid, sz, h in hits[:12]:
                print(f'    m{mid} {sz/1e9:.2f}GB | {h}')
    await c.disconnect()


# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["scan", "apply", "libs", "pull"])
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--max-items", type=int, default=20)
    ap.add_argument("--kinds", default="movie,tv")
    ap.add_argument("--channels", default="")
    ap.add_argument("--why", action="store_true")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--no-meta", action="store_true")
    ap.add_argument("--want", default="")
    ap.add_argument("--allow-multipart", action="store_true")
    ap.add_argument("--full", action="store_true", help="忽略 last_msg 游标，拉全频道历史（配合去重索引安全回填）")
    ap.add_argument("--queue", action="store_true", help="全量 scan 生成待上片队列到 tg_pending.json")
    ap.add_argument("--from-queue", action="store_true", help="apply 从 tg_pending.json 消费（分批、断点续传）")
    ap.add_argument("--from-snapshot", action="store_true", help="scan 复用 tg_scan_raw.json 快照，跳过 TG 全历史拉取")
    ap.add_argument("--loop", action="store_true", help="--from-queue 常驻循环：单连接跑完整个队列，末尾统一刷新一次")
    ap.add_argument("--refresh-every", type=int, default=200, help="loop 模式每登记 N 条增量通知 Emby 一次（默认 200；E008 防 pending_paths 无界累积；0=仅末尾通知，仅小规模调试用）")
    ap.add_argument("--acct2", default="", help="第二账号 StringSession 文件路径，启用双账号并行转存（奇偶分片）")
    a = ap.parse_args()
    global ALLOW_MULTIPART
    ALLOW_MULTIPART = a.allow_multipart

    if a.cmd == "libs":
        for f in emby("/Library/VirtualFolders"):
            o = f.get("LibraryOptions") or {}
            print(f'{f["Name"]:<10} | {f["CollectionType"]:<8} | {f["ItemId"]:>4} | '
                  f'{",".join(f["Locations"])} | {o.get("PreferredMetadataLanguage")}/{o.get("MetadataCountryCode")}')
        return

    cache = load_json(META_CACHE, {})
    if a.cmd == "pull":
        kind = "movie" if "movie" in a.kinds and a.kinds == "movie" else "tv"
        asyncio.run(pull(a.want, kind, a.limit, cache))
        save_json(META_CACHE, cache)
        return

    if a.from_queue:
        if a.cmd != "apply":
            print("# --from-queue 只用于 apply"); return
        if not a.loop:
            # 单批模式：小步验证用（独立进程、算一次索引、末尾刷新一次）
            st = load_state()
            q = load_json(PENDING, {"items": [], "cursor": 0})
            todo = q["items"][q["cursor"]:]
            attempted = len(todo[:a.max_items])
            batch = todo[:attempted]
            for it in batch:
                it.setdefault("raw_title", it.get("title"))
            batch = dedupe(batch, st, existing_index())
            print(f'# 队列: 共 {len(q["items"])} 条, 已完成 {q["cursor"]}, '
                  f'本批取 {attempted} 条 / 去重后 {len(batch)} 条', flush=True)
            done = asyncio.run(apply(batch, st, dry=a.dry))
            if not a.dry:
                q["cursor"] += attempted
                save_json(PENDING, q)
                if done:
                    print(f"# 本批登记 {len(done)} 部/集，队列进度 {q['cursor']}/{len(q['items'])}；增量通知 Emby")
                    emby_media_updated([p for it in done for p in it.get("_landed", [])])
            return

        # ---- 常驻循环：一条 Telethon 连接跑完整个队列，库内索引只算一次，末尾统一刷新一次 ----
        from telethon import TelegramClient
        from telethon.sessions import StringSession

        async def run_loop():
            st = load_state()
            q = load_json(PENDING, {"items": [], "cursor": 0})
            total = len(q["items"])
            idx = existing_index()          # 库内已有文件索引，仅算一次
            print(f'# 常驻循环: 队列 {total} 条, 起点 cursor={q["cursor"]}, '
                  f'库内已有 movies={len(idx[0])} eps={len(idx[1])}', flush=True)

            # ---- 账号列表：主号 + 可选第二账号（--acct2）----
            accounts = [{"sess": SESS, "name": "acct1", "step": 0}]
            if a.acct2:
                accounts.append({"sess": a.acct2, "name": "acct2", "step": 1})
            # step: 该账号负责的队列 index 取模。单号 step=0 → 全量；双号 step 0/1 → 奇偶分片，
            #        同一 Series 连续集尽量落在同一账号，减少并发去重竞争。

            clients = []
            for acc in accounts:
                c = TelegramClient(StringSession(open(acc["sess"]).read().strip()), API_ID, API_HASH)
                await c.connect()
                dst = None
                async for d in c.iter_dialogs(limit=None, offset_id=0):
                    if getattr(d.entity, "id", None) == STORE_CH:
                        dst = d.entity
                        break
                if dst is None:
                    await c.disconnect()
                    raise RuntimeError(f'{acc["name"]} 找不到存储频道 {store_channel()}')
                clients.append((acc, c, dst))

            since_refresh = 0
            pending_paths = []
            registered = 0
            stop = asyncio.Event()

            async def worker(acc, c, dst):
                nonlocal since_refresh, registered
                local_reg = 0
                my_global = q["cursor"]   # 从持久化游标续跑（重启/断点安全），非 0
                try:
                    # 每个账号独立扫 [cursor, total) 中属于自己 parity 的条目；st 去重保证不重复登记
                    while not stop.is_set():
                        my_idx = [i for i in range(my_global, total)
                                  if i % len(accounts) == acc["step"] and q["items"][i].get("done") is None]
                        if not my_idx:
                            if my_global >= total:
                                break
                            await asyncio.sleep(0.5)
                            continue
                        take = my_idx[:a.max_items]
                        batch = [q["items"][i] for i in take]
                        # 跳过禁止转发的频道：不试转发，直接标记 done（记录到 st 防重试）
                        live = []
                        for i in take:
                            it = q["items"][i]
                            if it.get("ch") in BLOCKED_CHANNELS:
                                if it["key"] not in st["keys"]:
                                    st["keys"][it["key"]] = {"ch": it["ch"], "msg": it["msg"],
                                                             "name": it.get("title") or it.get("raw_title") or "",
                                                             "skip": "禁止转发频道"}
                                it["done"] = 1
                            else:
                                live.append(i)
                        if not live:
                            # 本批全是禁转频道，仅推进游标
                            newcur = max(take) + 1
                            my_global = max(my_global, newcur)
                            cursor = q["cursor"]
                            for j in range(cursor, total):
                                if q["items"][j].get("done") is None:
                                    break
                                cursor = j + 1
                            q["cursor"] = cursor
                            save_json(PENDING, q)
                            print(f'# [{acc["name"]}] 跳过禁转段 cursor={q["cursor"]}/{total} '
                                  f'（本批 {len(take)} 条全为禁转频道）', flush=True)
                            if a.dry:
                                stop.set()
                                return
                            continue
                        batch = [q["items"][i] for i in live]
                        for it in batch:
                            it.setdefault("raw_title", it.get("title"))
                        batch = dedupe(batch, st, idx)
                        done = await apply(batch, st, dry=a.dry, c=c, dst=dst)
                        pending_paths.extend(p for it in done for p in it.get("_landed", []))
                        local_reg += len(done)
                        registered += len(done)
                        # 标记本次处理过的条目 done；游标推进到已覆盖区间最大值
                        for i in take:
                            q["items"][i]["done"] = 1
                        newcur = max(take) + 1
                        my_global = max(my_global, newcur)
                        # 游标 = 从持久化起点向后数第一个未 done 的位置（只增不减）
                        cursor = q["cursor"]
                        for j in range(cursor, total):
                            if q["items"][j].get("done") is None:
                                break
                            cursor = j + 1
                        q["cursor"] = cursor
                        save_json(PENDING, q)
                        print(f'# [{acc["name"]}] 进度 cursor={q["cursor"]}/{total} '
                              f'本批登记 {len(done)} / 该账号累计 {local_reg}', flush=True)
                        if a.dry:
                            stop.set()
                            return
                        since_refresh += len(done)
                        if a.refresh_every > 0 and since_refresh >= a.refresh_every:
                            print(f'# 中途增量通知 Emby（累计 {since_refresh} 条）', flush=True)
                            emby_media_updated(pending_paths)
                            pending_paths[:] = []
                            since_refresh = 0
                finally:
                    await c.disconnect()

            try:
                await asyncio.gather(*(worker(acc, c, dst) for acc, c, dst in clients))
                if not a.dry and registered:
                    print(f'# 队列消费完成，增量通知 Emby（登记 {registered} 条 / {len(pending_paths)} 路径）', flush=True)
                    emby_media_updated(pending_paths)
            finally:
                for _, c, _ in clients:
                    try:
                        await c.disconnect()
                    except Exception:
                        pass
        asyncio.run(run_loop())
        return

    names = [x.strip() for x in a.channels.split(",") if x.strip()]
    kinds = {x.strip() for x in a.kinds.split(",") if x.strip()}
    st = load_state()
    if a.from_snapshot and os.path.exists(SNAPSHOT):
        items = load_json(SNAPSHOT, [])
        print(f"# 复用快照 {SNAPSHOT}: {len(items)} 条", flush=True)
    else:
        items = asyncio.run(scan_channels(names, a.limit, st, full=a.full))
        if a.full:
            save_json(SNAPSHOT, items)
            print(f"# 全历史快照已落盘 {SNAPSHOT}: {len(items)} 条", flush=True)
    items = [i for i in items if i["kind"] in kinds]
    print(f"# 频道解析 {len(items)} 条，开始双源准入（TMDB+TODB，16 并发）...", flush=True)

    # 双源准入：按 (kind, 归一标题, year) 去重后并发查询（16 线程，命中约 0.4s / 未命中约 2.8s）
    from concurrent.futures import ThreadPoolExecutor
    tasks = {}
    for it in items:
        if it.get("skip"):
            continue
        ck = f'{it["kind"]}|{norm_title(it["title"])}|{it.get("year") or ""}'
        tasks.setdefault(ck, (it["kind"], it["title"], it.get("year")))
    todo = [k for k in tasks if k not in cache]
    print(f"# 唯一标题 {len(tasks)} 个，待查 {len(todo)} 个（缓存命中 {len(tasks)-len(todo)}）", flush=True)
    if todo and not a.no_meta:
        def _q(ck):
            kind, title, year = tasks[ck]
            try:
                return ck, meta_lookup(kind, title, year, {})
            except Exception as e:
                return ck, {"err": str(e)[:120]}
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=16) as ex:
            for n, (ck, res) in enumerate(ex.map(_q, todo), 1):
                cache[ck] = res if not (isinstance(res, dict) and res.get("err")) else None
                if n % 200 == 0:
                    print(f"  # 双源进度 {n}/{len(todo)}  {time.time()-t0:.0f}s", flush=True)
        print(f"# 双源查询完成 {len(todo)} 个 / {time.time()-t0:.0f}s", flush=True)
    for it in items:
        if it.get("skip"):
            continue
        ck = f'{it["kind"]}|{norm_title(it["title"])}|{it.get("year") or ""}'
        if a.no_meta:
            m = {"src": "-", "id": None, "name": it["title"], "year": it.get("year"),
                 "genres": [], "country": []}
        else:
            m = cache.get(ck)
        if not m:
            # 2026-11-14（E083）：双源准入失败时，用搜索 API 反查 TMDB id 兜底。
            # 频道里的中文译名（「老千4别西卜之歌」「爱的修学旅行第二季」）
            # 在 TMDB 中文库里常搜不到，原来的处理是直接淘汰 —— 准入率 97%，
            # 剩下 3% 就这么丢了。web_lookup 用 Brave/Firecrawl 搜到 TMDB 页面 URL
            # 抠出 id，再回 TMDB 官方 API 确认标题能对上，才采纳。
            # 开关：GHA_WEB_LOOKUP=0 可关闭（防止搜索额度耗光时拖慢整轮）。
            if os.getenv("GHA_WEB_LOOKUP", "1") == "1":
                try:
                    import web_lookup as WL
                    _hit = WL.lookup(it.get("title") or "",
                                     it.get("kind") or None)
                except Exception as _e:                    # noqa: BLE001
                    _hit = None
                    print(f"  # web_lookup 异常: {type(_e).__name__}", flush=True)
                if _hit:
                    m = {"src": "web", "id": _hit["tmdb_id"],
                         "name": _hit.get("name") or it.get("title"),
                         "year": _hit.get("year") or it.get("year"),
                         "_web": True}
                    print(f"  # 搜索兜底 {it.get('title')} -> "
                          f"tmdb/{_hit['tmdb_id']} {_hit.get('name')}",
                          flush=True)
        if not m:
            it["skip"] = "无 TMDB/TODB ID，不上"
            continue
        it["src"] = m["src"]; it["mid"] = m.get("id")
        it["raw_title"] = it["title"]                  # 保留频道原名，供去重兜底
        it["meta_name"] = m["name"]; it["title"] = m["name"]   # 规范中文名用于命名 + 去重
        if not it.get("year"):
            it["year"] = m.get("year")
        it["lib"] = classify_lib(it["kind"], m)
        if it["kind"] == "tv":
            normalize_tv_episode(it)      # 日期集号 → 标准 SxxExx（用户 m00001）
    save_json(META_CACHE, cache)

    eligible = [i for i in items if not i.get("skip")]
    for i in eligible:
        i.setdefault("raw_title", i["title"])
    idx = existing_index()
    kept = dedupe(eligible, st, idx)
    dropped = len(items) - len(eligible)
    print(f"# 双源准入: {len(eligible)} 条命中 / {dropped} 条无 ID 淘汰；去重后 {len(kept)} 条待上片")

    if a.queue:
        save_json(PENDING, {"items": kept, "cursor": 0, "ts": int(time.time()),
                            "channels": names, "kinds": sorted(kinds)})
        print(f"# 队列已写入 {PENDING}：{len(kept)} 条待上片（按频道优先级排序）")
        by_kind = {}
        by_lib = {}
        for it in kept:
            by_kind[it["kind"]] = by_kind.get(it["kind"], 0) + 1
            by_lib[it.get("lib") or "?"] = by_lib.get(it.get("lib") or "?", 0) + 1
        print("# 类型分布:", by_kind)
        print("# 分类分布:", by_lib)
        print("# 前 15 条预览:")
        for it in kept[:15]:
            print(f'  {it["ch"]:>12}/m{it["msg"]:<7} {it["size"]/1e9:5.2f}GB {it["qual"]:>6} '
                  f'{it["src"]}={str(it.get("mid")):>9} {it["lib"]:<5} {it["title"]} ({it["year"]})'
                  + (f' S{it["season"]:02d}E{it["episode"]:02d}' if it["kind"] == "tv" else ""))
        return

    if a.why:
        for it in items:
            if it.get("skip"):
                print(f'  - {it["ch"]}/m{it["msg"]} {it["title"][:34]} {it["qual"]} :: {it["skip"]}')
    if a.cmd == "scan" or a.dry:
        for it in kept:
            print(f'{it["ch"]:>12}/m{it["msg"]:<7} {it["size"]/1e9:5.2f}GB {it["qual"]:>6} '
                  f'{it["src"]}={str(it.get("mid")):>9} {it["lib"]:<5} '
                  f'{it["title"]} ({it["year"]})'
                  + (f' S{it["season"]:02d}E{it["episode"]:02d}' if it["kind"] == "tv" else ""))
        return
    kept = kept[:a.max_items]
    done = asyncio.run(apply(kept, st, dry=False))
    if done:
        print(f"# 已登记 {len(done)} 部/集，增量通知 Emby")
        emby_media_updated([p for it in done for p in it.get("_landed", [])])
    else:
        print("# 无新增")


if __name__ == "__main__":
    main()
