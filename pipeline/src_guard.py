#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""src_guard.py —— 第三方 Emby 源服的**统一访问闸门**（伪装 + 节流 + 负载均衡 + 审计）

为什么要有它
------------
用户 2026-11-10 指示：「从其他服务器拿好资源，但要遵守 ① 伪装好 UA ② 不要对某一个服大量异常请求，
负载均衡这些 emby 服」。项目原有 `probe_source_servers.py` 只做了「Hills UA + 1.5s + ≤12 次/服」，
缺 **UA 轮换、跨源负载均衡、熔断退避、审计留痕**。本模块是这些规则的**唯一执行点**：
所有访问第三方源服的脚本都必须走这里，规则文本见 `docs/SOURCE_SERVERS.md`。

六条硬规则（代码强制，不是建议）
--------------------------------
1. 只 GET。登录/写端点直接抛异常（`/Users/AuthenticateByName`、`POST/PUT/DELETE`）。
2. UA 固定一个 `Hills Windows/1.5.1`（用户 2026-11-10 定：不轮换）。禁止 python-urllib/curl 默认 UA。
3. 单 host：最小间隔 `MIN_INTERVAL`(1.6s，搜索 2.5s)、单会话请求数 ≤ `MAX_PER_HOST`(12)、并发 1。
4. 跨源轮询 + 按成功率加权；连续 2 次失败 → 该源冷却 10 分钟（熔断）。
5. 禁止任何形式的 web 登录（茶百道被封就是这条）；只用已存 token。
6. 每次请求写 `logs/source_requests.log`（JSONL）；会话结束打印汇总。

用法
----
    python3 src_guard.py --list                      # 列出可用源（不请求）
    python3 src_guard.py --search "小丑回魂"          # 跨源轮询搜索（每源 ≤3 次）
    python3 src_guard.py --plan "乡村爱情" --want 7   # 搜索 + 给候选清单（入库前评估）
    python3 src_guard.py --stats                      # 今日各源请求数/成功率
"""
import argparse
import json
import os
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.join(HERE, "_archive/sources_probe.json")
# 2026-11-14：从 _archive/ 挪到 config/。
# 这个文件存的是 14 个源站的明文账号，却一直放在名字叫"归档"的目录里，
# 而 src_guard 每次请求都要读它 —— "归档目录在承担生产职责"是结构上的语义错乱，
# 下一个接手的人清理 _archive/ 就会把生产断掉。
# 保留回退路径：config/ 不存在时仍读 _archive/（避免部署顺序出错时断服）。
SOURCES = os.path.join(HERE, "config", "sources.json")
if not os.path.exists(SOURCES):
    SOURCES = os.path.join(HERE, "_archive", "sources.json")
LOG = os.path.join(HERE, "logs", "source_requests.log")

BLACKLIST = set()          # 2026-11-10 用户已解封茶百道：封号原因是 web 登录被禁，不是频率/UA
# 2026-11-11 新增：持久隔离表（人工/无人值守判定"配置已死"的源），格式 {label 或 hostname: 理由}
# 与以下跳过项的区别：BLACKLIST=账号被封；TOKEN_STALE=401 待换 token；SKIP=端点本身 404/不可用
SKIP_FILE = os.path.join(HERE, ".src_skip.json")
# 账号本身可用但**本地存的 token 已失效**（401）→ 先跳过，等用户给新 token；绝不自己登录换 token
TOKEN_STALE = {}
# 个别源反代只代理了带 /Users/Me 前缀的路由（如 emos.200038.xyz：裸 /Items 内部 30 跳 500，
# /Users/Me/Items 正常）。按 label 覆写搜索/取数前缀，未列出的源走默认路径。
ITEMS_PREFIX = {
    "video.emos.best": "/Users/Me",
    "emos.enic65.eu.org": "/Users/Me",
}
MIN_INTERVAL = 1.6          # 元数据 GET 的最小间隔
SEARCH_INTERVAL = 2.5       # 搜索类接口更贵，间隔更大
# 2026-11-22 用户口径：**源站不设量子上限，只要求不频繁请求。**
# 原来的 12 次/会话把「枚举源站目录」直接掐死：lite 有 28716 部剧，
# 每页 200 → 需要 144 次请求，12 次只能拿到 8%。
# 现在默认 400（可由 EMBYTG_SRC_MAX_PER_HOST 覆盖），
# 真正的节流靠下面的 MIN_INTERVAL / SEARCH_INTERVAL（1.6s / 2.5s）。
MAX_PER_HOST = int(os.environ.get("EMBYTG_SRC_MAX_PER_HOST") or 400)
COOLDOWN_FAIL = 600         # 熔断 10 分钟
COOLDOWN_RATE = 1800        # 429（限流/封禁前兆）熔断 30 分钟
MAX_CONSEC_FAIL = 2
# 2026-11-23（自省）：源排序必须基于**可观测的历史**，不是每个进程从零开始的内存。
# 事故：yuntv 自 11-17 起 742 次请求只有 10 次成功（1.3%，其余是连接失败/429），
# 但每个 runner 都是新建 Guard，`score` 的 ok/fails 从 0/0 起算 0.5 —— 死源和
# 健康源在排序里完全等价，于是它每 10 分钟被重新选中一次，6 天烧掉 ~500 次配额。
HIST_LINES = int(os.getenv("EMBYTG_SRC_HIST_LINES", "20000"))   # 历史上限（行）
HIST_TAIL_BYTES = int(os.getenv("EMBYTG_SRC_HIST_BYTES", str(4 * 1024 * 1024)))
HIST_MIN_N = int(os.getenv("EMBYTG_SRC_HIST_MIN_N", "30"))      # 样本 >= 30 才用历史成功率
# 成功率只看**最近**这个窗口：10 天的平均值会把「上周还好、这周全挂」的源判成健康。
# 实测 yuntv 20 天窗口 19.6%（历史上有过好日子）、最近 48h **0/62**。
HIST_RECENT_H = float(os.getenv("EMBYTG_SRC_HIST_RECENT_H", "48"))
DEAD_SCORE = float(os.getenv("EMBYTG_SRC_DEAD_SCORE", "0.05"))  # <5% 视为死源，排到最后
# 2026-11-21：熔断状态原先只活在 Guard 实例里。每个 runner 进程都是新 Guard，
# fails/cool_until 归零 → 「连续 2 次失败熔断」在跨进程场景下**等于不存在**。
# 实测 7 天：yuntv 1451 次请求里 713 次 status=0 + 217 次 429（今天 46 次全部失败），
# lite 568 次 400，合计 2176 次白烧（占全部请求 17.4%，≈286 集的配额）。
# 现在把失败计数/冷却落盘，所有进程共享同一份「这个源现在不能用」的记忆。
HEALTH_FILE = os.getenv("EMBYTG_SRC_HEALTH_FILE") or os.path.join(HERE, "cache", "src_health.json")
HEALTH_TTL = int(os.getenv("EMBYTG_SRC_HEALTH_TTL", str(12 * 3600)))   # 记录 12h 后自然遗忘
FAIL_WINDOW = int(os.getenv("EMBYTG_SRC_FAIL_WINDOW", "600"))          # 10min 内才算「连续」
# 2026-11-22：**200 + 非 JSON 不是成功。**
# `39.96.113.82-mirror` 的第一个 url 是 `emby.taotu.ink`，它对 `/Items?SearchTerm=` 与
# `/System/Info/Public` 都返回 **200 + Emby 登录页 HTML**（不是这台源，是另一个站）。
# 旧 `get()` 把「200 且 body 不以 { [ 开头」原样 return 字符串，于是：
#   ① 调用方 `d.get("Items")` AttributeError（search_series 当场崩）；
#   ② 崩溃前已经执行了 `st.ok += 1; st.cool_until = 0; _health_ok()` ——
#      **用一个假成功把真镜像的 401 冷却清掉了**，这个源永远出不了轮转；
#   ③ 异常被 `resolve_candidates` 吞成「这个源没有这部剧」→ 按集 6h 冷却
#      （今日 151 次请求全是 HTML、77 次是同题重复搜索）。
# 现在：坏应答 = 该 host 记坏（跨进程 6h）+ 立刻换下一个镜像；全部坏才抛错。
HOST_BAD_FILE = os.getenv("EMBYTG_SRC_HOSTBAD_FILE") or os.path.join(HERE, "cache", "src_hosts_bad.json")
HOST_BAD_TTL = int(os.getenv("EMBYTG_SRC_HOSTBAD_TTL", str(6 * 3600)))
# 401 = 我们存的 token 被拒，重试一万次也不会成功（只有人能换 token）。
# 按「待换 token」长冷却，别再每 10 分钟重撞一次。
TOKEN_STALE_TTL = int(os.getenv("EMBYTG_SRC_TOKEN_STALE_TTL", str(12 * 3600)))

UA = "Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)"   # 用户 2026-11-10：UA 固定一个即可

FORBIDDEN_PATHS = (
    "/Users/AuthenticateByName",
    "/Users/New",
    "/Users/Delete",
    "/Sessions/Logout",
)

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE

# 2026-11-14：部分源站按**出口地区**封禁（E072/E073）。
# yuntv 系列在东京出口 403、香港出口 200 —— 实测确认。
# 这里维护「源 label → 本地 SOCKS5 代理」的映射，由 `get()` 按源选用。
# 代理端口来自 systemd 服务（embytg-xray-hk 提供 127.0.0.1:11082）。
# 配置放独立文件，避免和凭证混在一起；不存在则不启用任何代理（行为不变）。
PROXY_MAP = os.path.join(HERE, "config", "source_proxy.json")


def _load_proxy_map():
    """读 `config/source_proxy.json`：{"源label": "socks5h://127.0.0.1:PORT"}。

    文件不存在 → 返回空 dict（全部直连，与原行为一致）。
    """
    try:
        d = json.load(open(PROXY_MAP, encoding="utf-8"))
        return {str(k): str(v) for k, v in (d or {}).items() if v}
    except Exception:                                          # noqa: BLE001
        return {}


def _socks5_socket(proxy_host, proxy_port, dest_host, dest_port, timeout):
    """建立一条经 SOCKS5 代理到 dest 的 TCP 连接，返回已连通的 socket。

    只实现 CONNECT（无认证 / 用户名密码），够本项目用。
    `socks5h` 语义：域名交给代理解析（避免本地 DNS 泄露 + 保证走远端出口）。
    """
    s = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    s.settimeout(timeout)
    try:
        # 握手：VER=5, NMETHODS=1, METHOD=0(no auth)
        s.sendall(b"\x05\x01\x00")
        rep = s.recv(2)
        if len(rep) < 2 or rep[0] != 5:
            raise RuntimeError("SOCKS5 握手失败: %r" % (rep,))
        if rep[1] != 0:
            raise RuntimeError("SOCKS5 需要认证(method=%d)，当前不支持" % rep[1])
        # CONNECT：VER CMD RSV ATYP DST.ADDR DST.PORT
        host_b = dest_host.encode("idna")
        req = b"\x05\x01\x00\x03" + bytes([len(host_b)]) + host_b + dest_port.to_bytes(2, "big")
        s.sendall(req)
        res = s.recv(4)
        if len(res) < 4 or res[1] != 0:
            raise RuntimeError("SOCKS5 CONNECT 被拒: code=%d" % (res[1] if len(res) > 1 else -1))
        atyp = res[3]
        if atyp == 1:
            s.recv(4 + 2)
        elif atyp == 3:
            n = s.recv(1)[0]
            s.recv(n + 2)
        elif atyp == 4:
            s.recv(16 + 2)
        return s
    except Exception:
        s.close()
        raise


def _https_get_via_socks5(proxy_url, url, headers, timeout):
    """经 SOCKS5 代理发一个 HTTPS GET，返回 (status, body_bytes)。

    用标准 `ssl` 包裹代理 socket，再手工拼 HTTP/1.1 请求。
    只用于**元数据 GET**（本项目对源站只做只读 GET），够用且零依赖。
    """
    p = urllib.parse.urlparse(proxy_url)
    proxy_host, proxy_port = p.hostname, p.port or 1080
    u = urllib.parse.urlparse(url)
    host, port = u.hostname, u.port or (443 if u.scheme == "https" else 80)
    path = (u.path or "/") + ("?" + u.query if u.query else "")

    raw = _socks5_socket(proxy_host, proxy_port, host, port, timeout)
    try:
        if u.scheme == "https":
            raw = _ctx.wrap_socket(raw, server_hostname=host)
        lines = ["GET %s HTTP/1.1" % path, "Host: %s" % host]
        for k, v in (headers or {}).items():
            lines.append("%s: %s" % (k, v))
        lines.append("Connection: close")
        req = ("\r\n".join(lines) + "\r\n\r\n").encode()
        raw.sendall(req)

        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = raw.recv(4096)
            if not chunk:
                break
            buf += chunk
        head, _, rest = buf.partition(b"\r\n\r\n")
        status_line = head.split(b"\r\n", 1)[0].decode("latin1", "ignore")
        try:
            status = int(status_line.split()[1])
        except Exception:                                      # noqa: BLE001
            status = 0
        body = rest
        # 简单处理 chunked（源站元数据响应基本都是 identity，chunked 兜一下）
        hl = head.decode("latin1", "ignore").lower()
        if "transfer-encoding: chunked" in hl:
            out, data = b"", body
            while True:
                if b"\r\n" not in data:
                    more = raw.recv(4096)
                    if not more:
                        break
                    data += more
                    continue
                sz_line, _, data = data.partition(b"\r\n")
                try:
                    sz = int(sz_line.strip().split(b";")[0], 16)
                except Exception:                              # noqa: BLE001
                    break
                if sz == 0:
                    break
                while len(data) < sz + 2:
                    more = raw.recv(4096)
                    if not more:
                        break
                    data += more
                out += data[:sz]
                data = data[sz + 2:]
            body = out
        else:
            while True:
                chunk = raw.recv(65536)
                if not chunk:
                    break
                body += chunk
        return status, body
    finally:
        try:
            raw.close()
        except Exception:                                      # noqa: BLE001
            pass



def _skip_labels():
    """读持久隔离表；缺失/损坏一律当作空表，绝不让源服闸门因此挂掉"""
    try:
        with open(SKIP_FILE, encoding="utf-8") as fh:
            d = json.load(fh)
        return set(d) if isinstance(d, dict) else set(d)
    except Exception:                                              # noqa: BLE001
        return set()


def _health_load():
    """读跨进程源健康表（label -> {fails,ts,cool_until,last_status}）。

    缺失/损坏一律当空表：源服闸门不能因为状态文件坏了就整条断掉。
    """
    try:
        with open(HEALTH_FILE, encoding="utf-8") as fh:
            d = json.load(fh)
        return d if isinstance(d, dict) else {}
    except Exception:                                          # noqa: BLE001
        return {}


def _health_rmw(label, mut):
    """在 flock 下对单源记录做「读-改-写」，返回改后的记录。

    为什么必须加锁：source_pull / gha_next3 / daily_ops 会并发起 Guard，
    无锁 read-modify-write 会让两边的 fails 互相覆盖，熔断永远到不了阈值。
    写盘用 tmp + os.replace（同目录原子替换），读方永远看不到半截 JSON。
    """
    rec = {}
    try:
        d = _health_load()
        now = time.time()
        rec = dict(d.get(label) or {})
        rec = mut(rec, now) or rec
        rec["ts"] = now
        d[label] = rec
        for k in list(d):
            r = d[k] or {}
            # 12h 没再出过事且不在冷却中 → 遗忘，避免一次抖动变成终身惩罚
            if now - float(r.get("ts") or 0) > HEALTH_TTL and float(r.get("cool_until") or 0) <= now:
                d.pop(k, None)
        os.makedirs(os.path.dirname(HEALTH_FILE) or ".", exist_ok=True)
        tmp = "%s.tmp.%d" % (HEALTH_FILE, os.getpid())
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False)
        try:
            import fcntl
            lock_path = HEALTH_FILE + ".lock"
            with open(lock_path, "a+") as lk:
                fcntl.flock(lk, fcntl.LOCK_EX)
                os.replace(tmp, HEALTH_FILE)
                fcntl.flock(lk, fcntl.LOCK_UN)
        except Exception:                                      # noqa: BLE001
            os.replace(tmp, HEALTH_FILE)
    except Exception:                                          # noqa: BLE001
        return rec
    return rec


def _health_fail(label, status):
    """记一次失败（跨进程累计）；返回最新记录，调用方据此同步 cool_until。"""
    def _mut(rec, now):
        if now - float(rec.get("ts") or 0) > FAIL_WINDOW:
            rec["fails"] = 0          # 隔了 10min 以上不算「连续」
        rec["fails"] = int(rec.get("fails") or 0) + 1
        rec["last_status"] = int(status or 0)
        if int(status or 0) == 401:
            # 2026-11-22：401 = 本地 token 被源站拒绝。换进程、等 10 分钟、
            # 换镜像都不会让它变好 —— 只有人能重新登录换 token。
            # 实测 mirror 8 天 1012 次请求里 401 之外还有一堆「首个镜像返回
            # 登录页」，每次假成功都把冷却清掉 → 这个源永远出不了轮转。
            rec["token_stale_until"] = now + TOKEN_STALE_TTL
            rec["cool_until"] = max(float(rec.get("cool_until") or 0),
                                    rec["token_stale_until"])
            return rec
        if rec["fails"] >= MAX_CONSEC_FAIL:
            cool = COOLDOWN_RATE if int(status or 0) == 429 else COOLDOWN_FAIL
            rec["cool_until"] = now + cool
        return rec
    return _health_rmw(label, _mut)


def _health_ok(label, status=200):
    """一次成功 → 清零失败计数、解除冷却（源站恢复后一次请求就能重新入选）。"""
    def _mut(rec, now):
        rec["fails"] = 0
        rec["cool_until"] = 0
        rec["token_stale_until"] = 0
        rec["last_status"] = int(status)
        rec["ok_streak"] = int(rec.get("ok_streak") or 0) + 1
        return rec
    return _health_rmw(label, _mut)


def _host_bad_load():
    """跨进程的「这个镜像 url 是坏的」表：``{label\\x1fhost: {ts, why}}``。

    与源级健康表分开：一个 label 有 4 个镜像 url 时，坏的是 **url**，
    不是整个源。冷却整个 label 会把 3 个能用的镜像一起关掉。
    """
    try:
        with open(HOST_BAD_FILE, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            return {}
        now = time.time()
        return {k: v for k, v in d.items()
                if isinstance(v, dict) and now - float(v.get("ts") or 0) <= HOST_BAD_TTL}
    except Exception:                                          # noqa: BLE001
        return {}


def _host_bad_mark(label, host, why="non_json"):
    """记一个坏镜像（跨进程 6h）；返回写后的表。flock + 原子替换。"""
    key = "%s\x1f%s" % (label, str(host).rstrip("/"))
    now = time.time()
    d = _host_bad_load()
    d[key] = {"ts": now, "why": str(why)[:40]}
    try:
        os.makedirs(os.path.dirname(HOST_BAD_FILE) or ".", exist_ok=True)
        tmp = "%s.tmp.%d" % (HOST_BAD_FILE, os.getpid())
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False)
        try:
            import fcntl
            with open(HOST_BAD_FILE + ".lock", "a+") as lk:
                fcntl.flock(lk, fcntl.LOCK_EX)
                os.replace(tmp, HOST_BAD_FILE)
                fcntl.flock(lk, fcntl.LOCK_UN)
        except Exception:                                      # noqa: BLE001
            os.replace(tmp, HOST_BAD_FILE)
    except Exception:                                          # noqa: BLE001
        pass
    return d


def load_url_map():
    """Read the existing multi-URL source inventory.

    ``sources.json`` has always carried several URLs for some labels (e.g.
    ``39.96.113.82-mirror``).  Keep that as the source of truth for mirror
    rotation instead of creating several logical sources with independent
    request counters.
    """
    try:
        rows = json.load(open(SOURCES, encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        return {}
    out = {}
    for row in rows if isinstance(rows, list) else []:
        label = str(row.get("label") or "").strip()
        urls = row.get("urls") or []
        if not label or not isinstance(urls, list):
            continue
        clean = []
        for url in urls:
            url = str(url or "").strip().rstrip("/")
            if url.startswith(("http://", "https://")) and url not in clean:
                clean.append(url)
        if clean:
            out[label] = clean
    return out


def load_sources():
    d = json.load(open(PROBE, encoding="utf-8"))
    by = d.get("by_label") or d
    skip = _skip_labels()
    mirrors = load_url_map()
    out = {}
    for label, v in by.items():
        host = (v.get("url") or v.get("base") or "").rstrip("/")
        hostname = urllib.parse.urlparse(host).hostname or label
        if hostname in BLACKLIST or label in BLACKLIST:
            continue
        if hostname in TOKEN_STALE or label in TOKEN_STALE:
            continue
        if label in skip or hostname in skip:
            continue
        if not v.get("alive") or not v.get("auth"):
            continue
        tok = v.get("token") or v.get("AccessToken")
        if not tok:
            continue
        hosts = []
        for url in [host] + mirrors.get(label, []):
            url = str(url or "").strip().rstrip("/")
            if url.startswith(("http://", "https://")) and url not in hosts:
                hosts.append(url)
        out[label] = {"label": label, "host": host, "hostname": hostname,
                      "hosts": hosts or [host], "token": tok}
    return out


class SourceBadAnswer(RuntimeError):
    """源站回了 200，但 body 不是 JSON（登录页 / WAF / 反代首页 / HTML 错误页）。

    这**不是**「源站没有这部剧」，也**不是**「这条路径不存在」。把它当成两者之一
    都会造成静默损失：前者会给整部剧落 6h 冷却，后者会永久记住一条假的能力。
    """

    bad_answer = True


class SourceCoolingDown(RuntimeError):
    """源级熔断中（跨进程共享）。**不是**这条路径/这部剧的问题。

    调用方（source_pull._ep_get）必须把它与「400 端点不支持」区分开：
    前者只影响现在这 10~30 分钟，不该写成路径负缓存或能力记忆。
    """
    src_cooling = True


class HostState:
    def __init__(self, label, host, hostname, token, ua, hosts=None):
        self.label, self.host, self.hostname, self.token, self.ua = label, host, hostname, token, ua
        self.hosts = [str(x).rstrip("/") for x in (hosts or [host]) if x]
        if not self.hosts:
            self.hosts = [host]
        self.host = self.hosts[0]
        self.hostname = urllib.parse.urlparse(self.host).hostname or hostname
        self._host_rr = 0
        self.count = 0
        # answered：源站**真正应答过**的次数（拿到 HTTP 响应，无论状态码）。
        # count 是"尝试次数"，包含连接失败和多镜像重试。预算该按 answered 记，
        # 否则网络抖动会把当天配额白烧掉（实测 2300 次里 323 次 status=0）。
        self.answered = 0
        self.last = 0.0
        self.fails = 0
        self.ok = 0
        self.cool_until = 0.0
        self.avg_ms = 0.0          # 自审改进#2：按历史延迟排序，慢源后置
        # 2026-11-23：历史成功率（来自审计日志，跨进程）。样本不足时回落到会话内统计。
        self.hist_n = 0
        self.hist_ok = 0

    @property
    def usable(self):
        return self.count < MAX_PER_HOST and time.time() >= self.cool_until

    @property
    def score(self):
        """成功率：优先用**历史**（>= HIST_MIN_N 样本），否则用本会话。

        为什么不能用本会话：每个 runner 周期都是新进程，ok/fails 从 0/0 起算，
        所有源的 score 都是 0.5 —— 排序退化成「按延迟/按请求数」，死了 6 天的源
        与健康源等价。历史来自 logs/source_requests.log，跨进程、跨重启都一致。
        """
        if self.hist_n >= HIST_MIN_N:
            return self.hist_ok / float(self.hist_n)
        tot = self.ok + self.fails
        return (self.ok / tot) if tot else 0.5

    def take_hosts(self):
        """Return the mirror order for one request, rotating the start."""
        if not self.hosts:
            return [self.host]
        start = self._host_rr % len(self.hosts)
        self._host_rr += 1
        return self.hosts[start:] + self.hosts[:start]

    def select_host(self, host):
        self.host = str(host).rstrip("/")
        self.hostname = urllib.parse.urlparse(self.host).hostname or self.label
        return self.host


class Guard:
    """一个会话 = 一个 Guard 实例（UA 固定、计数与审计独立）"""

    def __init__(self, verbose=True):
        self.sources = load_sources()
        self.ua = UA                              # 固定 UA（不轮换）
        # 2026-11-14：按源选代理（E073）。空映射 = 全直连，行为与原版一致。
        self.proxies = _load_proxy_map()
        self.states = {k: HostState(k, v["host"], v["hostname"], v["token"], self.ua,
                                    v.get("hosts"))
                       for k, v in self.sources.items()}
        self.verbose = verbose
        self.total = 0
        self._seed_latency()
        self._seed_health()
        self._drop_bad_hosts()
        self._rr = 0
        os.makedirs(os.path.dirname(LOG), exist_ok=True)

    def _drop_bad_hosts(self):
        """把别的进程记下的「这个镜像 url 是登录页/HTML」从轮转里摘掉。"""
        bad = _host_bad_load()
        if not bad:
            return
        for st in self.states.values():
            good = [h for h in st.hosts
                    if ("%s\x1f%s" % (st.label, str(h).rstrip("/"))) not in bad]
            if good and len(good) < len(st.hosts):
                if self.verbose:
                    print("    ! %s 跳过坏镜像 %s" % (st.label,
                          ",".join(urllib.parse.urlparse(h).hostname or h
                                   for h in st.hosts if h not in good)))
                st.hosts = good
                st.select_host(good[0])

    def _seed_latency(self):
        """从审计日志取每源历史（平均耗时 + 成功率），作为调度权重。

        2026-11-23：原来只读最后 2000 行算 avg_ms，成功率仍用会话内存（=0.5），
        于是「1.3% 成功率的死源」和「99.8% 的健康源」在 order() 里完全等价。
        现在同一趟把 ok/n 也算出来。读取用**tail 定量字节**（HIST_TAIL_BYTES），
        不 readlines 整个文件 —— 审计日志会一直增长，O(文件大小) 的读法在百万行时
        会拖慢每一次 Guard 初始化；O(窗口) 才与规模无关。
        """
        agg = defaultdict(lambda: [0, 0, 0.0])      # label -> [n, ok, sum_ms]
        # 时间戳是定宽字符串（%F %T），直接做**字典序**比较即可，不 strptime（20000 行
        # strptime 要 60ms+，而定宽字符串比较是免费的）。只统计最近 HIST_RECENT_H 小时。
        cutoff = time.strftime("%F %T", time.localtime(time.time() - HIST_RECENT_H * 3600))
        try:
            size = os.path.getsize(LOG)
            with open(LOG, "rb") as f:
                if size > HIST_TAIL_BYTES:
                    f.seek(size - HIST_TAIL_BYTES)
                    f.readline()                     # 丢掉被截断的半行
                blob = f.read().decode("utf-8", "ignore")
            lines = blob.splitlines()[-HIST_LINES:]
        except Exception:                                      # noqa: BLE001
            return
        for line in lines:
            if not line.strip():
                continue
            try:
                r = json.loads(line)
            except Exception:                                  # noqa: BLE001
                continue
            label = r.get("label")
            if label not in self.states:
                continue
            if str(r.get("ts") or "") < cutoff:      # 只认最近窗口，定宽字符串字典序
                continue
            slot = agg[label]
            slot[0] += 1
            try:
                status = int(r.get("status") or 0)
            except (TypeError, ValueError):
                status = 0
            # status<=0 = 连应答都没有（连接失败/被墙）：这在配额上同样是白烧，
            # 必须计成失败，否则「打不通的源」会拿到 100% 的假成功率。
            if 200 <= status < 300:
                slot[1] += 1
            try:
                if r.get("dur_ms"):
                    slot[2] += float(r["dur_ms"])
            except (TypeError, ValueError):
                pass
        for label, (n, ok, ms) in agg.items():
            st = self.states[label]
            st.hist_n = int(n)
            st.hist_ok = int(ok)
            if n:
                st.avg_ms = float(ms) / n

    def _seed_health(self):
        """把别的进程记下的「这个源正在熔断」搬进本会话。

        这是本轮修复的核心：原来 fails/cool_until 只活在 `self.states`，
        每起一个 runner 就归零 → yuntv 这种「整源不可用」每天被几十个进程
        各撞一次（今天 46 次请求 0 次成功）。现在同一份状态被所有进程共享。
        """
        now = time.time()
        for label, rec in _health_load().items():
            st = self.states.get(label)
            if not st or not isinstance(rec, dict):
                continue
            if now - float(rec.get("ts") or 0) > HEALTH_TTL:
                continue
            cu = float(rec.get("cool_until") or 0)
            ts_until = float(rec.get("token_stale_until") or 0)
            cu = max(cu, ts_until)
            if cu > now:
                st.cool_until = cu
                st.fails = max(st.fails, int(rec.get("fails") or MAX_CONSEC_FAIL))

    # ---------- 调度 ----------
    def order(self):
        """负载均衡：可用源按 (连续失败, -历史成功率, 历史延迟, 本会话请求数) 排序。

        2026-11-23：**死源不参与轮转起点偏移**。原来整个可用列表一起轮转，
        8 个源里那个 1.3% 成功率的死源每个进程会被排到第一位 1/8 次 —— 等于每天
        主动把配额喂给一个 6 天没成功的源。现在分两段：健康段轮转保公平，
        死源段（score < DEAD_SCORE）追加在最后，只有健康源都用不了时才会碰它，
        下一次任何一次成功都会把它拉回健康段（自愈不留人工）。
        """
        usable = [s for s in self.states.values() if s.usable]
        usable.sort(key=lambda s: (s.fails, -s.score, s.avg_ms, s.count))
        alive = [s for s in usable if s.score >= DEAD_SCORE]
        dead = [s for s in usable if s.score < DEAD_SCORE]
        if alive:
            k = self._rr % len(alive)
            alive = alive[k:] + alive[:k]
            self._rr += 1
        return alive + dead

    # ---------- 请求 ----------
    def get(self, st, path, search=False, timeout=25):
        if any(p in path for p in FORBIDDEN_PATHS):
            raise RuntimeError("规则禁止访问登录端点：%s" % path)
        # 2026-11-21：熔断在**唯一入口**强制执行。原来只有 `order()` 会跳过冷却中的源，
        # 任何直接拿 `g.states[label]` 调 get() 的路径都能绕过，于是 yuntv 白天被
        # 40 多个进程各撞一次。这里挡住 = 入口挡住（比事后统计更早一层）。
        if time.time() < st.cool_until and not os.getenv("EMBYTG_SRC_FORCE"):
            raise SourceCoolingDown("源 %s 熔断中（到 %s），本轮不请求"
                                    % (st.label, time.strftime("%H:%M:%S",
                                                               time.localtime(st.cool_until))))
        if st.count >= MAX_PER_HOST:
            raise RuntimeError("源 %s 已达单会话上限 %d 次" % (st.label, MAX_PER_HOST))
        gap = SEARCH_INTERVAL if search else MIN_INTERVAL
        last_error = None
        # 2026-11-14（E073）：走代理时所有镜像共用同一条出口，
        # 代理挂了换镜像也没用 —— 只试第一个，避免一次请求白跑 3 遍。
        _hosts = st.take_hosts()
        if self.proxies.get(st.label):
            _hosts = _hosts[:1]
        for host in _hosts:
            if st.count >= MAX_PER_HOST:
                break
            wait = gap - (time.time() - st.last)
            if wait > 0:
                time.sleep(wait)
            st.select_host(host)
            url = st.host + path
            req = urllib.request.Request(url, headers={
                "User-Agent": st.ua, "X-Emby-Token": st.token,
                "Accept": "application/json", "Accept-Language": "zh-CN",
            })
            t0 = time.time()
            st.count += 1
            self.total += 1
            status = 0
            # 2026-11-14（E073）：在 try 之外取，保证 except 分支一定能引用到。
            proxy = self.proxies.get(st.label) if self.proxies else None
            try:
                if proxy:                    # 2026-11-14：该源按出口地区封禁，走本地 SOCKS5（E073）。
                    status, raw_b = _https_get_via_socks5(
                        proxy, url, {"User-Agent": st.ua, "X-Emby-Token": st.token,
                                     "Accept": "application/json",
                                     "Accept-Language": "zh-CN"}, timeout)
                    raw = raw_b.decode("utf-8", "ignore")
                    if status >= 400:
                        raise urllib.error.HTTPError(url, status, "proxied %d" % status,
                                                     {}, None)
                else:
                    with urllib.request.urlopen(req, timeout=timeout, context=_ctx) as r:
                        raw = r.read().decode("utf-8", "ignore")
                        status = r.status
                st.last = time.time()
                st.answered += 1
                body = raw.lstrip()
                if body and not body.startswith(("{", "[")):
                    # 2026-11-22（根因）：**200 + 非 JSON 不是成功，是这个 url 走错了。**
                    # 实测 mirror 的首个 url（emby.taotu.ink）对任何路径都回 200+登录页
                    # HTML；旧代码在这里 `st.ok += 1; _health_ok()` 并 return 字符串，
                    # 于是真镜像的 401 冷却被假成功清掉、search_series 当场 AttributeError、
                    # 上层再把异常当成「源站没这部剧」→ 按集 6h 冷却（今日 151 次全白烧）。
                    hostname = urllib.parse.urlparse(st.host).hostname or st.host
                    st.fails += 1
                    last_error = SourceBadAnswer(
                        "源 %s 镜像 %s 回 200 但非 JSON（%s）"
                        % (st.label, hostname, body[:40].replace("\n", " ")))
                    _host_bad_mark(st.label, st.host, "non_json")
                    if len(st.hosts) > 1 and st.host in st.hosts:
                        st.hosts = [h for h in st.hosts if h != st.host]
                        st.select_host(st.hosts[0])
                    if self.verbose:
                        print("    ! %s 镜像 %s 回 HTML/非 JSON，换下一个（已记坏 6h）"
                              % (st.label, hostname))
                    status = -200          # 审计里与真 200 区分：HTTP 成功但内容不是 API
                    continue
                st.ok += 1
                st.fails = 0
                st.cool_until = 0.0
                st.avg_ms = (st.avg_ms * 0.7 + (time.time() - t0) * 1000 * 0.3) if st.avg_ms else (time.time() - t0) * 1000
                data = json.loads(raw) if body else {}
                _health_ok(st.label, status)
                return data
            except urllib.error.HTTPError as e:
                status = e.code
                st.last = time.time()
                st.answered += 1          # 服务器回了状态码 = 它确实处理了这次请求
                st.fails += 1
                last_error = e
                # 2026-11-21：401/403/429/5xx 记进**跨进程**健康表。
                # 429 是限流/封禁前兆 → 冷却 30min（原来只是本进程 10min，
                # 换个进程立刻又去撞，7 天撞了 217 次）。404 是缺片、400 是调用方
                # 能力问题，都不算源故障，不进表。
                if e.code in (401, 403, 429) or e.code >= 500:
                    self._mark_fail(st, e.code)
                if e.code in (401, 403, 429):
                    raise
                if e.code < 500:
                    raise
            except Exception as e:                             # noqa: BLE001
                st.last = time.time()
                st.fails += 1
                last_error = e
                # status==0 = 源站压根没应答（连接被拒/超时/TLS/代理挂）。
                # 与 429 同罪：这一次请求的配额已经白烧，必须让后面的进程知道。
                if status == 0:
                    self._mark_fail(st, 0)
            finally:
                self._audit(st, path, status, time.time() - t0)
        if last_error:
            if isinstance(last_error, SourceBadAnswer):
                # 全部镜像都回 HTML → 这个源今天不能用于取数，按 5xx 级冷却。
                self._mark_fail(st, 502)
            raise last_error
        raise RuntimeError("源 %s 没有可用镜像" % st.label)

    def _mark_fail(self, st, status):
        """把一次失败写进跨进程健康表，并把冷却/计数同步回本会话。

        熔断判据不再是「本实例连续 2 次」，而是「10 分钟内全网累计 2 次」——
        因为浪费配额的是「多个进程各撞一次」，不是单个进程连撞两次。
        """
        rec = _health_fail(st.label, status)
        st.fails = max(st.fails, int(rec.get("fails") or 0))
        cu = float(rec.get("cool_until") or 0)
        if cu > time.time():
            st.cool_until = cu
            if self.verbose:
                print("    ! %s HTTP/%s 累计 %d 次失败，熔断到 %s"
                      % (st.label, status or "无应答", st.fails,
                         time.strftime("%H:%M:%S", time.localtime(cu))))

    def _audit(self, st, path, status, dur):
        rec = {"ts": time.strftime("%F %T"), "session_ua": self.ua[:24], "host": st.hostname,
               "label": st.label, "path": path[:120], "status": status, "dur_ms": int(dur * 1000)}
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # ---------- 业务 ----------
    def search(self, query, per_host=2, take=8):
        """跨源搜索，返回 [{label, id, name, year, type}]"""
        hits = []
        q = urllib.parse.urlencode({"SearchTerm": query, "Recursive": "true",
                                    "IncludeItemTypes": "Series,Movie", "Limit": take})
        for st in self.order():
            if len(hits) >= take:
                break
            try:
                d = self.get(st, ITEMS_PREFIX.get(st.label, "") + "/Items?" + q, search=True)
            except Exception as e:                                  # noqa: BLE001
                if self.verbose:
                    print("    × %-22s %s" % (st.label, str(e)[:40]))
                continue
            # 2026-11-21：源站有时返回字符串（错误页/HTML/纯文本），原来直接
            # d.get() 会 AttributeError 崩掉整个搜索 —— 一个坏源把整轮探测带崩。
            # 这里按「坏应答」处理并继续，不许让一个源拖垮全部探测。
            if not isinstance(d, dict):
                if self.verbose:
                    print("    × %-22s 非 JSON 应答: %s" % (st.label, str(d)[:40]))
                continue
            for it in (d.get("Items") or [])[:per_host]:
                hits.append({"label": st.label, "id": it.get("Id"), "name": it.get("Name"),
                             "year": (it.get("ProductionYear") or ""), "type": it.get("Type")})
            if self.verbose:
                print("    ✓ %-22s 命中 %d" % (st.label, len(d.get("Items") or [])))
        return hits

    def summary(self):
        rows = []
        for s in sorted(self.states.values(), key=lambda x: -x.count):
            if s.count:
                rows.append("%s=%d(ok%d/fail%d)" % (s.label, s.count, s.ok, s.fails))
        return "本会话 %d 次请求 | UA=%s | %s" % (self.total, self.ua[:20], "  ".join(rows) or "-")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--search")
    ap.add_argument("--plan")
    ap.add_argument("--want", type=int, default=0)
    ap.add_argument("--stats", action="store_true")
    a = ap.parse_args()

    if a.stats:
        if not os.path.exists(LOG):
            print("还没有审计记录")
            return
        agg = defaultdict(lambda: [0, 0])
        today = time.strftime("%F")
        for line in open(LOG, encoding="utf-8"):
            try:
                r = json.loads(line)
            except Exception:                                  # noqa: BLE001
                continue
            if not str(r.get("ts", "")).startswith(today):
                continue
            agg[r["label"]][0] += 1
            agg[r["label"]][1] += 1 if 200 <= int(r.get("status") or 0) < 300 else 0
        print("今日各源请求：")
        for k, (n, ok) in sorted(agg.items(), key=lambda x: -x[1][0]):
            print("  %-24s %3d 次  成功率 %.0f%%" % (k, n, ok * 100.0 / n))
        # 2026-11-23：调度实际用的权重（历史窗口）+ 死源判定必须能一眼看到，
        # 否则「谁被排到最后」只能靠读代码猜。
        g = Guard(verbose=False)
        print("\n调度权重（审计窗口 %d 行 / 最近 %.0fh，样本 >= %d 才生效）："
              % (HIST_LINES, HIST_RECENT_H, HIST_MIN_N))
        rows = sorted(g.states.values(), key=lambda s: -s.score)
        for st in rows:
            tag = "死源→排最后" if st.score < DEAD_SCORE else ""
            print("  %-24s score=%.3f (n=%d) avg=%5.0fms %s"
                  % (st.label, st.score, st.hist_n, st.avg_ms, tag))
        print("\n本轮探测顺序：%s" % " → ".join(s.label for s in g.order()))
        now = time.time()
        cool = {k: v for k, v in _health_load().items()
                if isinstance(v, dict) and float(v.get("cool_until") or 0) > now}
        if cool:
            print("熔断中（跨进程共享）：")
            for k, v in sorted(cool.items(), key=lambda x: -float(x[1]["cool_until"])):
                print("  %-24s 到 %s  last=%s 累计 %s 次"
                      % (k, time.strftime("%H:%M:%S", time.localtime(float(v["cool_until"]))),
                         v.get("last_status"), v.get("fails")))
        return

    g = Guard()
    if a.list:
        print("可用源 %d 个（黑名单 %d / token 失效 %d 已剔除）:" % (len(g.states), len(BLACKLIST), len(TOKEN_STALE)))
        for s in g.states.values():
            print("  %-24s %-34s 上限 %d 次/会话" % (s.label, s.hostname, MAX_PER_HOST))
        print("\n会话 UA：%s" % g.ua)
        return

    q = a.search or a.plan
    if not q:
        ap.error("需要 --list / --search / --plan / --stats")
    print("搜索「%s」（UA=%s，单源≤%d 次，间隔≥%.1fs）" % (q, g.ua[:28], MAX_PER_HOST, SEARCH_INTERVAL))
    hits = g.search(q)
    print()
    if not hits:
        print("  无命中")
    for h in hits:
        print("  [%s] %s (%s) %s  id=%s" % (h["label"][:20], h["name"], h["year"], h["type"], (h["id"] or "")[:8]))
    if a.plan:
        print("\n负载分布：%s" % g.summary())
        if a.want:
            print("目标片数 %d → 需要确认各源可提供；下一步按 docs/SOURCE_SERVERS.md §5 走" % a.want)
    else:
        print("\n%s" % g.summary())


if __name__ == "__main__":
    main()
