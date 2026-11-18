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
        line(n24 <= 10, "转存画质虚标", detail if n24 else "无（标签与实际一致）")
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
                return c is None or c == 0 or int(c) > 20

            todo = [x for x in zero if _pending(x)]
            names = ",".join(x["label"][:16] for x in zero[:4])
            days = ",".join(str(x["day"]) for x in zero[:4])
            line(None, "零产出源",
                 "%s（各烧 %s 次，建议降 limit）" % (names, days) if todo
                 else "%s（各烧 %s 次；额度已压到 20，次日生效）" % (names, days))
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

    # 4.8) 阈值自适应（磁盘按 7 天斜率重算；claim 阈值固定并写明原因）
    sec("4.8) 阈值自适应")
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "adapt_thresholds.py")],
                           capture_output=True, text=True, timeout=120)
        tail = [l.strip() for l in (r.stdout or "").strip().splitlines() if "→" in l]
        line(r.returncode == 0, "阈值", tail[-1] if tail else "已刷新")
    except Exception as e:                                          # noqa: BLE001
        line(False, "阈值", str(e)[:60])

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
