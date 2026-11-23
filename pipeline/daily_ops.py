#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""daily_ops.py —— 运维守护每日一巡检（只读，不改任何东西）

对应 `docs/OPS_HANDBOOK.md §1.1`：开工 5 分钟内回答「今天能不能开工」。

    python3 daily_ops.py            # 全量巡检（约 20–40s）
    python3 daily_ops.py --quick    # 跳过 naming_health / lib_reconcile（各约 10s）
输出：分区块 PASS/FAIL + 末尾「今日建议」，可直接贴到 AgentCommunication.log。
"""
import argparse
import glob
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def find_phantom_db():
    """项目根目录的 tg.db 是幻影台账 —— 真源只有 `pipeline/tg.db`。

    2026-11-16 实测：根目录反复出现 0 字节 `tg.db`（00:04 一份、07:03 又一份），
    来源是从 `cd /opt/embytg` 用相对路径开库的命令（脚本里 TGDB 全是绝对路径，
    所以这是人工/临时命令留下的空壳）。危害不是占几 KB：任何人从根目录
    `sqlite3 tg.db "select count(*) ..."` 都会读到 0 条，然后误判「台账全丢」。
    """
    root = os.path.dirname(HERE)
    out = []
    for p in [os.path.join(root, "tg.db")] + sorted(glob.glob(os.path.join(root, "tg.db.stray-*"))):
        if os.path.exists(p):
            out.append((p, os.path.getsize(p)))
    return out
CFG = json.load(open("/opt/embytg/config.json", encoding="utf-8"))
EMBY = (CFG.get("emby_url") or "http://127.0.0.1:8096").rstrip("/")
TOKEN = CFG["emby_token"]
MNT = "/opt/embytg/mnt/embytg"

rows = []


def gap_objects(gaps):
    """把对账差异行折叠成「对象」数（E182 附带修的判据单位问题）。

    一集刚落地的中间态会同时产出 3 行：unregistered / episode_missing_emby /
    series_missing_emby。按行数比 claim 数就是拿行比集，1 个 claim 必然误报。
    同一集的三行算 1 个对象，series 级缺剧单独算 1。
    """
    objs = set()
    for g in gaps or []:
        k = str(g.get("kind") or "")
        if k == "series_missing_emby":
            objs.add(("series", g.get("lib"), g.get("series")))
        elif k in ("unregistered", "episode_missing_emby"):
            objs.add(("episode", g.get("lib"), g.get("series"),
                      g.get("season"), g.get("ep")))
        else:
            objs.add((k, g.get("lib"), g.get("series"), g.get("detail")))
    return len(objs)


def line(ok, name, detail=""):
    rows.append((ok, name, detail))
    print("  %s %-26s %s" % ("✔" if ok is True else ("✘" if ok is False else "•"), name, detail))


def emby(path, method="GET", body=None, timeout=120):
    req = urllib.request.Request(EMBY + path,
                                data=json.dumps(body).encode() if body is not None else None,
                                method=method, headers={"X-Emby-Token": TOKEN, "Content-Type": "application/json"})
    raw = urllib.request.urlopen(req, timeout=timeout).read().decode()
    return json.loads(raw) if raw.strip().startswith(("{", "[")) else raw


def sh(cmd, timeout=300):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "") + (r.stderr or "")
    except Exception as e:                                          # noqa: BLE001
        return 1, str(e)


def sec(title):
    print("\n== %s ==" % title)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    a = ap.parse_args()

    # 1) 基础设施
    sec("1) 基础设施")
    rc, out = sh("mount | grep -c 'mnt/embytg' ")
    line(rc == 0 and out.strip().isdigit() and int(out.strip()) > 0, "rclone 挂载", "挂载点 %s" % out.strip())
    rc, out = sh("docker ps --format '{{.Names}} {{.State}} {{.Status}}' | wc -l")
    line(rc == 0, "容器数", out.strip())
    rc, out = sh("docker ps --filter health=unhealthy --format '{{.Names}}'")
    line(out.strip() == "", "无 unhealthy 容器", out.strip() or "全部健康")
    du = shutil.disk_usage("/")
    free_g = du.free / 1024 ** 3
    line(free_g > 15, "磁盘余量", "%.0f GB 可用" % free_g)

    # 2) 数据库
    sec("2) 数据库")
    try:
        con = sqlite3.connect(os.path.join(HERE, "tg.db"))
        ok = con.execute("pragma integrity_check").fetchone()[0] == "ok"
        pend = con.execute("select count(*) from pending").fetchone()[0]
        line(ok, "tg.db integrity", "pending=%d" % pend)
    except Exception as e:                                          # noqa: BLE001
        line(False, "tg.db", str(e)[:50])

    # 3) Emby
    sec("3) Emby")
    try:
        counts = {}
        for t in ("Movie", "Series", "Episode"):
            d = emby("/Items?" + urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": t,
                                                         "Limit": "1", "EnableTotalRecordCount": "true"}))
            counts[t] = d.get("TotalRecordCount")
        line(True, "Movie/Series/Episode", "%s / %s / %s" % (counts["Movie"], counts["Series"], counts["Episode"]))
        tasks = emby("/ScheduledTasks")
        scan = [t for t in tasks if "Scan media" in (t.get("Name") or "")][0]
        running = scan.get("State") == "Running"
        line(None if running else True, "扫描状态",
             ("%s %.1f%%（运维任务入库中，不影响巡检）" % (scan.get("State"), scan.get("CurrentProgressPercentage") or 0))
             if running else "Idle")
    except Exception as e:                                          # noqa: BLE001
        line(False, "Emby API", str(e)[:60])
        running = False

    # 4) 库一致性 / 命名
    sec("4) 库一致性与命名")
    if not a.quick:
        rc, out = sh("cd %s && python3 lib_reconcile.py 2>&1 | tail -3" % HERE, timeout=600)
        ok = "converged = True" in out
        # 2026-11-13：缺口 ≤ 活动 claim 数时是「正在下载/待索引」，不是失配。
        # 与 status_now / improve_guard 的 gap_le_claims 同一口径，
        # 避免五路都在跑时 daily_ops 天天报 FAIL（观测到缺口 2 / claim 5）。
        if not ok:
            try:
                _gap = 0
                for _l in out.splitlines():
                    if "期望区间" in _l:
                        import re as _re
                        _m = _re.search(r"\[(\d+)", _l)
                        _m2 = _re.search(r"总数\s*(\d+)", _l)
                        if _m and _m2:
                            _gap = int(_m.group(1)) - int(_m2.group(1))
                import glob as _g
                _ncl = len(_g.glob(os.path.join(HERE, ".gha_claims", "*.json")))
                if 0 < _gap <= _ncl:
                    ok = True
                    out = out + "\n缺口 %d 集 ≤ 活动 claim %d 个，属正在下载/待索引" % (_gap, _ncl)
            except Exception:                                  # noqa: BLE001
                pass
        line(ok, "lib_reconcile", out.strip().splitlines()[-1][:70] if out.strip() else "")
        rc, out = sh("cd %s && python3 naming_health.py --no-name 2>&1 | tail -6" % HERE, timeout=600)
        fails = [l.strip() for l in out.splitlines() if "FAIL" in l]
        gaps = [l.strip() for l in out.splitlines() if "缺 NFO" in l]
        # 2026-11-13：区分「故障」和「已知待决策」。
        # 剩下的 NFO 缺口经 renumber_plan.py 逐条验证，**没有一条能安全机械重编号**
        # （盘侧季号冲突、绝对/日期集号、或 TMDB 自己季划分有歧义）。这类不是脚本故障，
        # 是数据源层面的歧义，必须人工定夺。让它天天红只会让真故障被淹没。
        # 判据不靠"我觉得"：直接看 renumber_plan 的可执行移动数是否为 0。
        _pending = 0
        try:
            import renumber_plan as _rp
            _plans = [_rp.plan_one(lib, name.split("{tmdb=")[0].strip(), miss)
                      for (lib, name), miss in _rp.gap_series().items()]
            _pending = sum(len(p["moves"]) for p in _plans)
        except Exception:                                          # noqa: BLE001
            _pending = -1
        if fails and not _pending:
            line(None, "naming_health",
                 (gaps[0] if gaps else "") +
                 " —— 已逐条验证：无安全重编号方案，属已知待决策（见 docs/NFO_GAPS_*.md）")
        else:
            line(not fails, "naming_health",
                 gaps[0] if gaps else ("FAIL: " + "; ".join(fails)[:60] if fails else ""))
    else:
        line(None, "lib_reconcile / naming_health", "已跳过（--quick）")

    # 4.5) 布局（一次性脚本必须放 /tmp）
    sec("4.5) 项目布局")
    try:
        import check_layout as CL
        stray = CL.find_stray()
        # 自愈：*.bak* 是编辑器备份，移到 .attic（可逆、不删），只对真正的临时脚本报错
        attic = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".attic")
        moved = 0
        keep = []
        for f in stray:
            if ".bak" in os.path.basename(f):
                try:
                    os.makedirs(attic, exist_ok=True)
                    shutil.move(f, os.path.join(attic, os.path.basename(f)))
                    moved += 1
                    continue
                except Exception:                            # noqa: BLE001
                    pass
            keep.append(f)
        if moved:
            print("   ↳ 已把 %d 个 *.bak 移进 .attic" % moved)
        stray = keep
        line(not stray, "一次性脚本", ("%d 个残留: %s" % (len(stray), ", ".join(os.path.basename(x) for x in stray[:4])))
             if stray else "无（规则：放 /tmp）")
    except Exception as e:                                          # noqa: BLE001
        line(False, "布局检查", str(e)[:50])

    # 4.505) 转存画质虚标（2026-11-18 用户反馈 @luolimeimei 质量忽高忽低）
    # tgpipe 落地命名前会用真实分辨率复算画质，并把「标签 vs 实际」写进
    # logs/tgpipe_quality.jsonl。这里只统计 24h 内的订正条数：
    # 少量订正 = 频道偶发虚标（正常）；持续 >10 条 = 那条频道系统性虚标，该降权。
    try:
        qlog = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "logs", "tgpipe_quality.jsonl")
        n24 = 0
        per = {}
        if os.path.exists(qlog):
            cut = time.time() - 86400
            for ln in open(qlog, encoding="utf-8"):
                try:
                    j = json.loads(ln)
                except Exception:                            # noqa: BLE001
                    continue
                if float(j.get("ts") or 0) < cut:
                    continue
                n24 += 1
                per[j.get("ch") or "?"] = per.get(j.get("ch") or "?", 0) + 1
        detail = ("24h 订正 %d 条%s" % (n24, ("：" + ", ".join("%s=%d" % kv for kv in per.items())) if per else ""))
        # 命中率比绝对条数更能说明问题：luolimeimei 今日 20 条里 10 条标错 = 50%，
        # 说明它的 2160p 标签不可信（落地名前 tgpipe 已按真实分辨率订正，故不影响画质，
        # 只影响「这频道到底有多少 4K」的判断）。>10 条或占本批 >30% 都判 FAIL。
        try:
            _q = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "tgpipe.log")
        except Exception:                                            # noqa: BLE001
            _q = ""
        # 真正该 FAIL 的是「名字标的画质与实测不符」——源站虚标由 tgpipe 落地前订正，
        # 只要订正成功就不算故障，只作参考信息（占比高 = 该频道 4K 不可信）。
        try:
            _c2 = sqlite3.connect("file:%s?mode=ro" % os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "tg.db"), uri=True, timeout=10)
            _rows = _c2.execute("select rel, width, height from resource_state "
                                "where landed_at>=? and width is not null", (int(time.time()) - 86400,)).fetchall()
            _c2.close()
        except Exception:                                            # noqa: BLE001
            _rows = []

        def _real(w, h):
            w, h = w or 0, h or 0
            if w >= 3000 or h >= 1800:
                return "2160p"
            if w >= 1700 or h >= 1000:
                return "1080p"
            if w >= 1200 or h >= 700:
                return "720p"
            return "SD"

        _bad = []
        for _rel, _w, _h in _rows:
            if not (_w or _h):
                continue                     # 还没探测到分辨率，不参与判定
            _nm = _rel.split("/")[-1]
            _tag = ("2160p" if "2160p" in _nm else "1080p" if "1080p" in _nm
                    else "720p" if "720p" in _nm else None)
            if _tag and _tag != _real(_w, _h):
                _bad.append("%s(标%s实%s)" % (_nm[:20], _tag, _real(_w, _h)))
        _info = ("源站虚标 %d 条" % n24) if n24 else "源站无虚标"
        line(not _bad, "落地画质与实测一致",
             "%d 条不符：%s｜%s" % (len(_bad), "; ".join(_bad[:3]), _info) if _bad
             else "近24h %d 条已探测全部一致｜%s" % (len(_rows), _info))
    except Exception as e:                                          # noqa: BLE001
        line(False, "转存画质虚标", str(e)[:50])

    # 4.51) 新入库码率达标率（2026-11-19 目标口径）
    # 判据不是「文件名标了什么」，而是 size×8/dur_sec 的真实码率：
    # 2160p ≥2.0Mbps、1080p ≥0.8Mbps、其余 ≥0.4Mbps。
    # 存量 1463 集低码率是历史遗留（门禁上线前入库的），这里只看**新入库**，
    # 因为新入库能不能达标才是可控的那一半。
    try:
        _c3 = sqlite3.connect("file:%s?mode=ro" % os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tg.db"), uri=True, timeout=10)
        _r3 = _c3.execute("select rel, size, dur_sec, width, height from resource_state "
                          "where kind='episode' and landed_at>=? and dur_sec>0 and size>0",
                          (int(time.time()) - 86400,)).fetchall()
        _c3.close()
        _bad3 = []
        for _rel3, _sz3, _d3, _w3, _h3 in _r3:
            _mbps = _sz3 * 8 / _d3 / 1e6
            _floor3 = 2.0 if ((_w3 or 0) >= 3000 or (_h3 or 0) >= 1800) else (
                0.8 if ((_w3 or 0) >= 1700 or (_h3 or 0) >= 1000) else 0.4)
            if _mbps < _floor3:
                _bad3.append("%s(%.1fMbps)" % (_rel3.split("/")[-1][:22], _mbps))
        if not _r3:
            # 台账探测滞后（新集 dur_sec 还没填）时，改问 Emby 要 RunTimeTicks ——
            # 否则会出现「近24h 0 集，达标率 100%」这种假通过。
            try:
                import urllib.parse as _up
                import urllib.request as _ur
                from embytg_conf import cfg as _cfg
                _b = _cfg("emby_url").rstrip("/")
                _tk = _cfg("emby_token")
                _since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400))
                _q = _up.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode",
                                    "MinDateCreated": _since, "Fields": "Path,MediaSources,RunTimeTicks",
                                    "Limit": "5000"})
                with _ur.urlopen("%s/Items?%s&api_key=%s" % (_b, _q, _tk), timeout=120) as _f:
                    _items = (json.loads(_f.read()) or {}).get("Items") or []
                for _it in _items:
                    _ms = (_it.get("MediaSources") or [{}])[0]
                    _d3 = (_it.get("RunTimeTicks") or 0) / 1e7
                    _sz3 = _ms.get("Size") or 0
                    if not _d3 or not _sz3:
                        continue
                    _nm = (_it.get("Path") or "").split("/")[-1]
                    _mbps = _sz3 * 8 / _d3 / 1e6
                    _floor3 = 2.0 if "2160p" in _nm else (0.8 if "1080p" in _nm else 0.4)
                    _r3.append((_nm, _sz3, _d3, 0, 0))
                    if _mbps < _floor3:
                        _bad3.append("%s(%.1fMbps)" % (_nm[:22], _mbps))
            except Exception:                                       # noqa: BLE001
                pass
        _rate = (1 - len(_bad3) / len(_r3)) * 100 if _r3 else 100.0
        line(_rate >= 95, "新入库码率达标",
             "近24h %d 集，达标率 %.1f%%%s" % (len(_r3), _rate,
              ("，不达标 %d：%s" % (len(_bad3), "; ".join(_bad3[:3]))) if _bad3 else ""))
    except Exception as e:                                          # noqa: BLE001
        line(False, "新入库码率达标", str(e)[:50])

    # 4.52) 同片同画质重复入库（2026-11-20 封神第一部事故沉淀）
    # 用户策略是「低画质删不掉就并存，高画质并列」，所以同片名同一年份出现两条**不同画质**
    # 是设计内的；真正要抓的是「同片名 + 同一年份 + 同一画质」两条 —— 那是重复导入。
    try:
        import urllib.parse as _up
        import urllib.request as _ur
        _b = EMBY
        _t = TOKEN
        _q = _up.urlencode({"IncludeItemTypes": "Movie", "Recursive": "true",
                            "Fields": "Path,ProductionYear", "Limit": "5000"})
        _d = json.loads(_ur.urlopen(_b + "/Items?" + _q + "&api_key=" + _t, timeout=40).read())
        _seen = {}
        for _it in _d.get("Items") or []:
            _p = _it.get("Path") or ""
            _base = os.path.basename(_p)
            # 用**文件名**做键，不用 Emby 的 Name：Emby 会把「王牌 (2025)」误认成
            # 「王牌保镖 (2017)」，按 Name+年份分组会误报（真重复 = 同名文件出现两份）
            _key = re.sub(r"\s*-\s*(2160p|1080p|720p|SD).*$", "", os.path.splitext(_base)[0])
            _qm = re.search(r"-\s*(2160p|1080p|720p|SD)", _base)
            _ql = _qm.group(1) if _qm else "?"
            _seen.setdefault((_key.strip(), _ql), []).append(_p)
        _dups = {k: v for k, v in _seen.items() if len(v) > 1}
        if _dups:
            _s = "; ".join("%s(%s,%s)×%d" % (k[0][:12], k[1], k[2], len(v))
                           for k, v in list(_dups.items())[:3])
            line(False, "同片同画质重复", "%d 组：%s" % (len(_dups), _s))
        else:
            line(True, "同片同画质重复", "无")
    except Exception as e:                                          # noqa: BLE001
        line(False, "同片同画质重复", str(e)[:50])


    # 4.56) 低码率假高清（2026-11-19 谷雨事故：标 2160p 单集 345-435MB，实为 1.0-1.3Mbps）
    # 必须用**真实时长**算码率：只看体积会把 20 分钟的国漫误杀。
    # 时长取 Emby 的 RunTimeTicks（resource_state.dur_sec 目前是空的）。
    try:
        import urllib.parse as _up2
        _since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400))
        _q2 = _up2.urlencode({"Recursive": "true", "IncludeItemTypes": "Episode,Movie",
                              "MinDateCreated": _since,
                              "Fields": "Path,MediaSources,RunTimeTicks", "Limit": "3000"})
        _d2 = json.loads(urllib.request.urlopen(
            EMBY + "/Items?" + _q2 + "&api_key=" + TOKEN, timeout=60).read())
        _low, _fresh = [], []
        for _it in _d2.get("Items") or []:
            _ms = (_it.get("MediaSources") or [{}])[0]
            _dur = (_it.get("RunTimeTicks") or 0) / 1e7
            _sz = _ms.get("Size") or 0
            if not _dur or not _sz:
                continue
            _m = _sz * 8 / _dur / 1e6
            _nm = (_it.get("Path") or "").split("/")[-1]
            _floor = 2.0 if "2160p" in _nm else 0.8 if "1080p" in _nm else 0.4
            if _m < _floor:
                _low.append("%s(%.1fMbps/%.0fMB)" % (_nm[:24], _m, _sz / 1e6))
                # 闸门上线后仍进来的才算回归；历史存量只报数不算失败
                _ticks = _it.get("DateCreated") or ""
                try:
                    _age_h = (time.time() - time.mktime(time.strptime(_ticks[:19], "%Y-%m-%dT%H:%M:%S"))) / 3600
                except Exception:                                # noqa: BLE001
                    _age_h = 999
                if _age_h <= 6:
                    _fresh.append("%s(%.1fMbps)" % (_nm[:24], _m))
        line(not _fresh, "低码率假高清",
             "近6h 新增 %d 条：%s｜近24h 共 %d 条（含历史）"
             % (len(_fresh), "; ".join(_fresh[:3]), len(_low)) if _fresh
             else "近6h 无新增｜近24h 存量 %d 条（闸门已拦截新片）" % len(_low))
    except Exception as e:                                          # noqa: BLE001
        line(False, "低码率假高清", str(e)[:50])
    # 4.55) 机器人补片：2026-11-20 起 @YSDQBOT 已从项目移除（用户决定），
    # 这里只做存在性断言，防止有人又把它拉起来却没人知道。
    _ysdq_units = __import__("glob").glob("/etc/systemd/system/embytg-ysdq-auto.*")
    line(not _ysdq_units, "机器人补片", "已下线（无残留 unit）" if not _ysdq_units
         else "发现残留 unit: %s" % len(_ysdq_units))

    # 4.54) 转存链路停摆（2026-11-20 事故：cursor 走满后 pending 行数不减，
    # 队列闸永远判「够用」，转存静默停摆，定时器每分钟"成功"退出）
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import tgpipe as _T
        _q = _T.load_json(_T.PENDING, {"items": [], "cursor": 0})
        _remain = max(0, len(_q.get("items") or []) - int(_q.get("cursor") or 0))
        _con = sqlite3.connect("file:%s?mode=ro" % os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tg.db"), uri=True, timeout=10)
        _cut = int(time.time()) - 6 * 3600
        _recent = _con.execute("select count(*) from resource_state where landed_at>=?",
                               (_cut,)).fetchone()[0]
        _con.close()
        if _remain == 0 and _recent == 0:
            line(False, "转存队列", "队列未消费=0 且近6h零入库：补扫闸可能又卡住了")
        else:
            line(True, "转存队列", "未消费 %d 条，近6h 入库 %d 集" % (_remain, _recent))
    except Exception as e:                                          # noqa: BLE001
        line(False, "转存队列", str(e)[:50])

    # 4.53) 电影缺识别锚点（2026-11-20 王牌误认事故沉淀）
    # Emby 认片靠「年份 + {tmdb=ID}」。文件名两者都没有时它会瞎猜
    # （「王牌 (2025)」被认成「王牌保镖 (2017)」）。新导入必须两个锚点之一，
    # 这条检查是「以后别再犯」的兜底：0 = 全部有锚点。
    try:
        _q = urllib.parse.urlencode({"IncludeItemTypes": "Movie", "Recursive": "true",
                                     "Fields": "Path", "Limit": "6000"})
        _d = json.loads(urllib.request.urlopen(EMBY + "/Items?" + _q + "&api_key=" + TOKEN,
                                              timeout=40).read())
        _bad = []
        for _it in _d.get("Items") or []:
            _base = (_it.get("Path") or "").split("/")[-1]
            if not re.search(r"\((\d{4})\)", _base) and "{tmdb=" not in _base:
                _bad.append(_base)
        line(not _bad, "电影识别锚点",
             "0 条无锚点" if not _bad else "%d 条无年份无 tmdb：%s" % (len(_bad), " / ".join(_bad[:3])))
    except Exception as e:                                          # noqa: BLE001
        line(False, "电影识别锚点", str(e)[:50])

    # 4.51) emos 夜间 4K 补版窗口是否真在产出（2026-11-20 新增）
    # 定时器 02:10 跑 emos_night.py，日志 /opt/embytg/logs/emos_night.log。
    # 判据只看「最近一次运行」：跑了但 0 集成功 = 有问题；没跑 = 定时器挂了。
    try:
        elog = "/opt/embytg/logs/emos_night.log"
        if not os.path.exists(elog):
            line(True, "夜间4K补版", "尚未运行（定时器 02:10 首跑）")
        else:
            age_h = (time.time() - os.path.getmtime(elog)) / 3600.0
            tail = open(elog, encoding="utf-8", errors="replace").read()[-4000:]
            last = None
            for ln in tail.splitlines():
                if ln.startswith("  完成 "):
                    last = ln.strip()
            if age_h > 26:                     # 超过一天没动 = 定时器/服务挂了
                line(False, "夜间4K补版", "日志 %.0f 小时未更新，定时器可能失效" % age_h)
            elif last and "完成 0/" in last:
                line(False, "夜间4K补版", "最近一轮 0 集成功：%s" % last)
            else:
                line(True, "夜间4K补版", "%s（%.0f 小时前）" % (last or "有日志无小结", age_h))
    except Exception as e:                                          # noqa: BLE001
        line(False, "夜间4K补版", str(e)[:50])

    except Exception as e:                                          # noqa: BLE001
        line(False, "转存画质虚标", str(e)[:50])

    # 4.51) 幻影台账：根目录 tg.db 空壳（2026-11-16 新增）
    # 真源只有 pipeline/tg.db。根目录那份被查询会读到 0 条 → 误判「台账全丢」。
    try:
        ghost = find_phantom_db()
        # 自愈：0 字节空壳是 sqlite 在错误目录下被查询时自动建的，删掉不留痕；
        # 非空的不动（可能是别人真正要的数据），继续报错让人看。
        healed = 0
        for gp, gsz in ghost:
            if gsz == 0:
                try:
                    os.remove(gp)
                    healed += 1
                except Exception:                            # noqa: BLE001
                    pass
        if healed:
            ghost = [(gp, gsz) for gp, gsz in ghost if gsz != 0]
            print("   ↳ 已自动删除 %d 份 0 字节空壳" % healed)
        line(not ghost, "幻影 tg.db",
             ("根目录有 %d 份空壳: %s —— 真源只有 pipeline/tg.db"
              % (len(ghost), ", ".join("%s(%dB)" % (os.path.basename(p), s) for p, s in ghost[:3])))
             if ghost else "无（真源只有 pipeline/tg.db）")
    except Exception as e:                                          # noqa: BLE001
        line(False, "幻影 tg.db", str(e)[:50])

    # 4.6) 韩国侧留言检查 —— 2026-11-13 移除（韩国 VPS 已退役、qwenpaw 已停，
    # 这条 md5 比对只会把 KOREA_RUNNER.md 的任何编辑报成"新留言"；文档已归档）。
    # 跨机沟通统一走 AgentCommunication.log。

    # 4.65) 文档与布局不变量（2026-11-13 新增）
    # 存在理由：项目曾有 4 份 ERRORS.md / 2 份 AGENTS.md、同一条规则散落 37 个文档，
    # 导致同一个事故被独立诊断两次。规则只有能被执行检查时才算规则。
    sec("4.65) 文档一致性")
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "docs_lint.py"), "--json"],
                           capture_output=True, text=True, timeout=180)
        dl = json.loads((r.stdout or "{}").strip() or "{}")
        line(dl.get("verdict") == "PASS", "docs_lint",
             "真源唯一/链接可达/编号唯一/无死链" if dl.get("verdict") == "PASS"
             else "%d 条问题" % len(dl.get("problems") or []))
        for p_ in (dl.get("problems") or [])[:3]:
            line(False, "文档问题", "%s: %s" % (p_.get("kind"), p_.get("detail"))[:80])
    except Exception as e:                                          # noqa: BLE001
        line(False, "docs_lint", str(e)[:60])

    # 4.68) 五账对账（2026-11-13 新增，架构级）
    # 之前只有 lib_reconcile 的**计数级**对比（td 文件数 vs Emby 集数），
    # 总数相等也会错位，总数不等时只说"缺口≈N 集"——正因如此 2 个孤儿剧
    # 存在了很久却被解释成"新片/改名尾部"。这条是条目级：每个 (库,剧,季,集)
    # 具体缺在哪本账里都列出来，并写进 tg.db 的 reconcile_gaps 表。
    sec("4.68) 五账对账（teldrive/tg.db/Emby）")
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "reconcile.py"), "--json"],
                           capture_output=True, text=True, timeout=600)
        rc = json.loads((r.stdout or "{}").strip() or "{}")
        st = rc.get("stats") or {}
        cnt = rc.get("counts") or {}
        total = sum(cnt.values())
        # 在途容忍：刚落地、Emby 还没索引的那几集是正常中间态（差异数 ≤ 活跃 claim 数）。
        # 与 status_now / improve_guard 的 gap_le_claims 同一口径，避免每次落地都报 FAIL。
        #
        # 2026-11-19：**单位要对齐。** 一集刚落地的状态会同时产出 3 行
        # （unregistered + episode_missing_emby + series_missing_emby），
        # 而容忍度是按「claim 数」算的 → 只有 1 个 claim 时 3 > max(2,1)，
        # 每次单集落地都会假 FAIL 两三分钟（实测 06:12 律政英雄 S01E01）。
        # 判据改成按「对象」计数：同一集的 unregistered/episode_missing 合并为 1，
        # series 级缺剧单独算 1。
        import glob as _g
        _ncl = len(_g.glob(os.path.join(HERE, ".gha_claims", "*.json")))
        _objs = gap_objects(rc.get("gaps") or [])
        _in_flight = _objs > 0 and _objs <= max(2, _ncl)
        line(total == 0 or _in_flight,
             "对账差异",
             "teldrive=%s tgdb=%s emby=%s | 差异 %s 条 / %d 个对象%s"
             % (st.get("teldrive"), st.get("tgdb"), st.get("emby_eps"), total, _objs,
                "（≤活动 claim %d，属在途）" % _ncl if _in_flight and total else ""))
        if not _in_flight:
            for k, n in sorted(cnt.items(), key=lambda x: -x[1])[:4]:
                line(False, "对账差异", "%s: %d" % (k, n))
    except Exception as e:                                          # noqa: BLE001
        line(False, "对账", str(e)[:60])

    # 4.70) 回归测试（2026-11-14 新增）
    #
    # 2026-11-14 一天修了 15 个真实 bug，几乎全是"线上出问题才发现"的。
    # 之前所有检查（五账对账、docs_lint、stall_watch）都是**事后体检**，
    # 没有一个能在**改动前**把人拦住。tests/test_regressions.py 补的就是这一层。
    #
    # 这里必须真跑：测试写了不跑等于没写。实测 test_ingest_breaker
    # 已经红了很久而无人发现，就是"没有人跑"的代价。
    sec("4.70) 回归测试")
    try:
        _rc, _out = sh("%s -m unittest discover -s %s"
                       % (sys.executable, os.path.join(HERE, "tests")), timeout=180)
        _tail = [l for l in (_out or "").splitlines() if l.startswith(("OK", "FAILED", "Ran "))]
        _ok = _rc == 0 and any(l.startswith("OK") for l in _tail)
        line(_ok, "回归测试", " | ".join(_tail[-2:]) or "无输出")
        if not _ok:
            for l in (_out or "").splitlines():
                if l.startswith(("FAIL:", "ERROR:")):
                    line(False, "失败用例", l[:80])
    except Exception as e:                                          # noqa: BLE001
        line(False, "回归测试", str(e)[:60])

    # 4.695) 配额效率历史（2026-11-14 新增）
    # 核心指标 = 每出一集花多少次源站请求。它是 10 万集目标的唯一硬约束，
    # 而以前是靠临时脚本拍快照、口径还各写一套。这里每天落一条，形成可比曲线。
    sec("4.695) 配额效率（请求/集）")
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "quota_history.py")],
                           capture_output=True, text=True, timeout=300)
        out = (r.stdout or "").strip()
        row = [l for l in out.splitlines() if l.strip().startswith("2026-")
               or re.match(r"\s+20\d\d-\d\d-\d\d", l)]
        trend = [l for l in out.splitlines() if "趋势" in l]
        if row:
            line(None, "今日配额效率", row[-1].strip()[:76])
        if trend:
            line(None, "配额趋势", trend[0].strip()[:76])
    except Exception as e:                                          # noqa: BLE001
        line(None, "配额效率", str(e)[:60])

    # 4.69) 每源产能归因（2026-11-13 新增）
    # 每个源每天 300 次请求是硬顶，8 源 = 2400 次/天，实际出片约 250 集 —— 命中率 10%。
    # 以前预算文件只记「打了多少枪」不记「打中多少」，谁在白烧配额只能靠感觉。
    # 这里是 notes 而不是 FAIL：源产出低可能是它今天确实没新片，不是故障；
    # 但连续多天 ok=0 且 day 打满，就该降权了（改 .gha_src_budget.json 的 limit 或移出源列表）。
    sec("4.69) 每源产能归因")
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "source_yield.py"), "--json"],
                           capture_output=True, text=True, timeout=120)
        y = json.loads((r.stdout or "{}").strip() or "{}")
        srcs = y.get("sources") or []
        tot_day = sum(x.get("day") or 0 for x in srcs)
        tot_ok = sum(x.get("ok") or 0 for x in srcs)
        zero = [x for x in srcs if x.get("verdict") == "zero"]
        line(None, "源命中率",
             "%d 源 / 请求 %d / 出片 %d / 命中 %.1f%%"
             % (len(srcs), tot_day, tot_ok,
                (tot_ok * 100.0 / tot_day) if tot_day else 0))
        if zero and tot_ok:
            # 判据跟着动作走：额度已经压到地板（20）的源，不该每天再报一次
            # 「建议降 limit」——2026-11-18 日终发现 rebalance 根本没被调度，
            # 结果这条建议在「从不执行的建议」与「已经执行完的旧话」之间反复刷屏。
            try:
                with open(os.path.join(HERE, ".gha_src_budget.json"),
                          encoding="utf-8") as f:
                    caps = {k: v.get("daily_cap")
                            for k, v in (json.load(f).get("sources") or {}).items()
                            if isinstance(v, dict)}
            except Exception:                                      # noqa: BLE001
                caps = {}

            def _pending(x):
                c = caps.get(x["label"])
                # 2026-11-23：策略已改为「源站配额不设上限，只保留频率约束」
                # （src_budget_rebalance.tier_of 恒返回 0）。cap=0 是**已执行**的
                # 目标状态，不是「还没降」。再报「建议降 limit」就是让人去改一个
                # 按策略永远不该改的值 —— 改成只报告，不再提建议。
                return c is None

            todo = [x for x in zero if _pending(x)]
            names = ",".join(x["label"][:16] for x in zero[:4])
            days = ",".join(str(x.get("day") or 0) for x in zero[:4])
            line(None, "零产出源",
                 "%s（各烧 %s 次，无 cap；未在源列表）" % (names, days) if todo
                 else "%s（各烧 %s 次；额度策略=不限，按 download_fails 降权）" % (names, days))
    except Exception as e:                                          # noqa: BLE001
        line(None, "源命中率", str(e)[:60])

    # 4.7) 定时任务自检：prompt 里引用的脚本是否还在（防改名后静默失效）
    sec("4.7) 定时任务自检")
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "cron_lint.py"), "--json"],
                           capture_output=True, text=True, timeout=180)
        cl = json.loads((r.stdout or "{}").strip() or "{}")
        line(cl.get("verdict") == "PASS", "cron_lint",
             "%s 个任务，问题 %s 条" % (cl.get("jobs"), len(cl.get("problems") or [])))
        for p_ in (cl.get("problems") or [])[:3]:
            line(False, "任务问题", "%s: %s" % (p_.get("kind"), p_.get("detail")))
    except Exception as e:                                          # noqa: BLE001
        line(False, "cron_lint", str(e)[:60])

    # 4.72) 刮削缺口报告新鲜度（2026-11-14）
    # status_now 读 docs/NFO_GAPS_<date>.md，超过 12h 就标 STALE、**不计入结论**，
    # 等于刮削缺口变成盲区。原来没有任何定时任务运行 gen_nfo_gap_report.py，
    # 全靠人手跑一次，跑完 9 小时后报表就瞎了。
    # 接进每日巡检：超过 12h 就重跑一次（34 秒），保证结论里始终有真实缺口数。
    sec("4.72) 刮削缺口报告")
    try:
        import glob as _glob
        import time as _time
        _docs = os.path.join(os.path.dirname(HERE), "docs")
        _reps = sorted(_glob.glob(os.path.join(_docs, "NFO_GAPS_*.md")))
        _age_h = None
        if _reps:
            _age_h = (_time.time() - os.path.getmtime(_reps[-1])) / 3600.0
        if _age_h is None or _age_h > 12:
            r = subprocess.run([sys.executable, os.path.join(HERE, "gen_nfo_gap_report.py")],
                               capture_output=True, text=True, timeout=600)
            _ok = r.returncode == 0
            _tail = (r.stdout or "").strip().splitlines()[-1:] or [""]
            line(_ok, "缺口报告", ("已重建（原 %.1fh 前）：%s" % (_age_h, _tail[0]))
                 if _age_h else ("已重建：%s" % _tail[0]))
        else:
            line(True, "缺口报告", "新鲜（%.1fh 前）" % _age_h)
    except Exception as e:                                          # noqa: BLE001
        line(False, "缺口报告", str(e)[:60])

    # 4.73) Series 级 NFO 覆盖（逐集 NFO 齐 ≠ 剧级 tvshow.nfo 齐）
    # 2026-11-16 加：三部新剧（十万狂花入梦来/青春共体/重生密码）逐集 NFO 与 Emby Name 全对，
    # 但 Series 根目录没有 tvshow.nfo —— 缺口报告只看逐集，post_ingest 只查当天剧，谁都看不见。
    # 用 teldrive 库一条 SQL 数全库（O(1) 次查询），不 walk FUSE（800 部剧 stat 要 3 分钟以上）。
    sec("4.73) Series 级 NFO 覆盖")
    try:
        pg = CFG.get("pg_query") or "docker exec -i teldrive-postgres psql -U teldrive -d teldrive -At -F'|'"
        sql = (
            "with dirs as (select id,parent_id,name from files where kind='folder' and deleted_at is null),"
            " tv as (select id from dirs where name='TV' and parent_id is null),"
            " lv1 as (select d.id,d.name lib from dirs d join tv on d.parent_id=tv.id),"
            " lv2 as (select d.id,d.name n2,lv1.lib from dirs d join lv1 on d.parent_id=lv1.id),"
            " season as (select distinct parent_id from dirs where name ~ '^Season'),"
            " has_nfo as (select distinct parent_id from files where name='tvshow.nfo' and deleted_at is null)"
            " select count(*) filter (where lv2.id in (select parent_id from season)),"
            " count(*) filter (where lv2.id in (select parent_id from season)"
            " and lv2.id not in (select parent_id from has_nfo)) from lv2"
        )
        r = subprocess.run(pg + " -c " + json.dumps(sql), shell=True,
                           capture_output=True, text=True, timeout=120)
        out = (r.stdout or "").strip().splitlines()
        total, miss = ([int(x) for x in out[-1].split("|")] if out and "|" in out[-1] else (0, 0))
        if not total:
            line(None, "Series NFO", "查不到 teldrive 文件树，跳过")
        elif miss == 0:
            line(True, "Series NFO", "%d/%d 部有 tvshow.nfo" % (total, total))
        else:
            names = ""
            if miss <= 5:
                sql2 = sql.replace("select count(*) filter (where lv2.id in (select parent_id from season)),"
                                   " count(*) filter (where lv2.id in (select parent_id from season)"
                                   " and lv2.id not in (select parent_id from has_nfo)) from lv2",
                                   "select lv2.lib||'/'||lv2.n2 from lv2 where lv2.id in"
                                   " (select parent_id from season) and lv2.id not in"
                                   " (select parent_id from has_nfo) order by 1")
                r2 = subprocess.run(pg + " -c " + json.dumps(sql2), shell=True,
                                    capture_output=True, text=True, timeout=120)
                names = "；".join((r2.stdout or "").strip().splitlines()[:5])
            line(miss <= 2, "Series NFO", "缺 %d/%d 部：%s（修：post_ingest.py --tmdb <id>）"
                 % (miss, total, names or ""))
    except Exception as e:                                          # noqa: BLE001
        line(None, "Series NFO", str(e)[:60])

    # 4.75) NFO 内容完整性（2026-11-19 判据重写）
    # 旧判据「占位标题占比 >5% 判 FAIL」把「上游没有数据」当成「我们出错」：
    # 1045 集占位里有 1007 集在 TODB 与 TMDB **两边都是占位**（三源确实没有真名，
    # 谷雨 E28-E30 连 TMDB 自己都填「第 28 集」），占比 7.2% 永远压不到 5%
    # → 每天必 FAIL → 这条告警会被无视。**永远报警的检查等于没有检查。**
    #
    # 新判据只守一条不变式：**任一权威源有真名时，库里不许还是占位**。
    # 量尺是 nfo_src_audit.py（TMDB 中文优先/英文补/自动翻译 + TODB 兜底），
    # 结果落 .nfo_src_audit.json；actionable>0 才算我们的错。
    # 缓存超过 NFO_AUDIT_TTL_H（默认 20h）才重算，日常调用不重复打外部接口。
    try:
        _aud_p = os.path.join(HERE, ".nfo_src_audit.json")
        _ttl = float(os.environ.get("NFO_AUDIT_TTL_H") or 20) * 3600
        _aud = {}
        try:
            _aud = json.load(open(_aud_p, encoding="utf-8"))
        except Exception:                                          # noqa: BLE001
            _aud = {}
        if (not _aud) or (time.time() - float(_aud.get("ts") or 0) > _ttl):
            # 日常巡检只看近 7 天新增（全量 1.4 万集要几分钟，且历史缺口源本来就没有）
            _r = subprocess.run([sys.executable, os.path.join(HERE, "nfo_src_audit.py"),
                                 "--days", "7", "--json"],
                                capture_output=True, text=True, timeout=1800)
            try:
                _aud = json.load(open(_aud_p, encoding="utf-8"))
            except Exception:                                      # noqa: BLE001
                _aud = {}
            if not _aud:
                raise RuntimeError("audit 未产出结果：%s" % (str(_r.stderr or _r.stdout)[-80:]))
        _act = int(_aud.get("actionable") or 0)
        _ph = int(_aud.get("placeholder") or 0)
        _tot = max(1, int(_aud.get("episodes") or 0))
        _noph = int(_aud.get("no_plot") or 0)
        _pg = int(_aud.get("plot_gain") or 0)
        _age = (time.time() - float(_aud.get("ts") or 0)) / 3600.0
        line(_act == 0 and _age <= 36, "NFO 内容完整性",
             "可修复占位 %d（源有真名而库里没有）｜占位 %d/%d（%.1f%%，其余源无数据）"
             "｜无简介 %d（可补 %d）｜审计 %.1fh 前"
             % (_act, _ph, _tot, 100.0 * _ph / _tot, _noph, _pg, _age))
        if _act:
            line(False, "可修复占位明细",
                 "先 nfo_src_audit.py --apply 写 NFO，再 nfo_name_to_emby.py --apply --series-dir <目录>")
    except Exception as e:                                         # noqa: BLE001
        line(None, "NFO 内容完整性", str(e)[:60])

    # 4.8) 阈值自适应（磁盘按 7 天斜率重算；claim 阈值固定并写明原因）
    sec("4.8) 阈值自适应")
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "adapt_thresholds.py")],
                           capture_output=True, text=True, timeout=120)
        tail = [l.strip() for l in (r.stdout or "").strip().splitlines() if "→" in l]
        line(r.returncode == 0, "阈值", tail[-1] if tail else "已刷新")
    except Exception as e:                                          # noqa: BLE001
        line(False, "阈值", str(e)[:60])
    # 源优先级按真实产出重排（静态文件会腐烂：死源压着出片源，每轮先撞墙再熔断）
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "reprioritize_sources.py")],
                           capture_output=True, text=True, timeout=60)
        tail = [l.strip() for l in (r.stdout or "").strip().splitlines() if l.strip()]
        line(r.returncode == 0, "源优先级", tail[0] if tail else "已按真实产出对齐")
    except Exception as e:                                          # noqa: BLE001
        line(False, "源优先级", str(e)[:60])

    # 5) 入库（GHA，2026-11-11 起媒体数据面移交 GitHub Actions）
    sec("5) 入库（韩国 runner）")
    try:
        import subprocess as _sp
        r = _sp.run([sys.executable, os.path.join(HERE, "runner_report.py"), "--json"],
                    capture_output=True, text=True, timeout=180)
        # runner_report 会在 stdout 先打一行人类可读的 [queue] 摘要，
        # 直接 json.loads 整段会 "Expecting value: line 1 column 2"。
        # 从第一个 '{' 开始取，兼容前后夹带任何提示行。
        out = r.stdout or ""
        start = out.find("{")
        g = json.loads(out[start:].strip() if start >= 0 else "{}")
        q = g.get("queues") or {}
        line(g.get("verdict") == "PASS", "远程上片",
             "今日 %s 条 / 近3h %s 条 | 队列 new %s/fill %s/upgrade %s | 卡单 %s"
             % (g.get("done_today"), g.get("done_3h"), q.get("new"), q.get("fill"), q.get("upgrade"),
                len(g.get("stale_claims") or [])))
        for p_ in (g.get("problems") or [])[:3]:
            line(False, "上片问题", p_)
    except Exception as e:                                          # noqa: BLE001
        line(False, "远程上片", str(e)[:60])

    # 5.5) 流量（本机已停用，仅手工兜底时参考）
    sec("5.5) 本机流量闸门")
    # 2026-11-13：本机（Oracle 控制面）已不再承担上传，ingest_quota 的计数器
    # 停在停用那天的残值上（今日 439.5/300、余量 -139.5 GB），天天报 FAIL 是假信号。
    # 按「是否还有本地上传进程」判断，没有就跳过这项，不参与结论。
    _local_ingest = os.path.exists(os.path.join(HERE, ".local_ingest_enabled"))
    if not _local_ingest:
        line(True, "本机流量闸门", "已停用（本机不再上传，此项不参与结论）")
    else:
        try:
            import ingest_quota as Q
            c = Q.cfg()
            d_gb, d_n, m_gb, m_n = Q.usage()
            left = min(c["daily_gb"] - d_gb, c["safety_gb"] - m_gb)
            line(left > 0, "今日余量", "%.1f GB（今日 %.1f/%.0f，本月 %.1f/%.0f）"
                 % (left, d_gb, c["daily_gb"], m_gb, c["safety_gb"]))
        except Exception as e:                                          # noqa: BLE001
            left = 0
            line(False, "预算", str(e)[:60])

    # 5.9) teldrive part 完整性（2026-11-24 升级后新增）
    # plain_size 缺失会让 teldrive 的偏移计算错位 → 播放到某个位置就断。
    # 2.0.14 提供了 backfill-part-sizes 命令，这里只做体检，不自动改数据。
    sec("5.9) teldrive 分片完整性")
    try:
        import subprocess as _sp
        _q = ("select count(*) from file_parts where plain_size is null or plain_size=0;")
        _r = _sp.run(["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive",
                      "-d", "teldrive", "-t", "-A", "-c", _q],
                     capture_output=True, text=True, timeout=30)
        _bad = int((_r.stdout or "0").strip() or 0)
        if _bad:
            line(False, "part 尺寸缺失", "%d 条（跑 teldrive backfill-part-sizes）" % _bad)
        else:
            line(True, "part 尺寸", "全部完整")
    except Exception as e:                                          # noqa: BLE001
        line(False, "part 尺寸", str(e)[:60])

    # 6) 源服
    sec("6) 源服可用性")
    try:
        import src_guard as SG
        g = SG.Guard(verbose=False)
        line(len(g.states) > 0, "可用源", "%d 个（黑名单 %d / token 失效 %d）"
             % (len(g.states), len(SG.BLACKLIST), len(SG.TOKEN_STALE)))
        line(True, "UA", g.ua)
    except Exception as e:                                          # noqa: BLE001
        line(False, "源服", str(e)[:60])

    # 7) 建议
    sec("7) 今日建议")
    fails = [r for r in rows if r[0] is False]
    if fails:
        print("  ⛔ 先修这些问题再入库：")
        for _, n, d in fails:
            print("     - %s %s" % (n, d))
    else:
        print("  ✅ 基础设施健康，可以开工")
    if 'left' in dir() and left > 0:
        print("  📦 今日可入库额度 %.1f GB（目标 ≤300GB/天）" % left)
        print("  ▶ 建议批次：docs/NFO_GAPS_*.md 里「盘侧季号/集号错」优先，"
              "其次截断件、整季缺失")
    if 'running' in dir() and running:
        print("  ⏳ Emby 正在扫描：先等它跑完，避免与扫描抢 IO")

    ok_all = not fails
    print("\n小结：%s（%d 项检查，%d 项不通过）" % ("PASS" if ok_all else "FAIL", len(rows), len(fails)))
    return 0 if ok_all else 1


if __name__ == "__main__":
    import urllib.parse                                              # noqa: E402
    sys.exit(main())
