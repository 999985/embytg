#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gha_cancel.py — 按 run_key 释放三线 claim；可选记录源站失败。"""
import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gha_state as GS          # noqa: E402

# 2026-11-15（E131）：老身份名 Codex-Korea-Ingest 是韩国节点时代的遗留，
# 它让协作日志看起来「一直有个 Codex 在干活」，实际是这三条脚本的机械写入。
# 用户已确认现在只有 hermes-ops 在维护，改成中性名以免误导排查。
AGENT = "embytg-pipeline"


SCAN_FAIL_WINDOW = 20 * 60      # 统计窗口：20 分钟
SCAN_FAIL_TRIP = 5              # 窗口内 5 次 → 熔断


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--reason", default="cancelled")
    ap.add_argument("--source-failed", action="store_true",
                    help="源直链下载失败；累计连续失败并在两次后冷却 10 分钟")
    a = ap.parse_args()

    try:
        # 2026-11-13：find_claim 默认只认 status=running，失败态的 claim 取不到，
        # 于是「下载失败」的 claim 永远没人释放，一直占着 lane（观测到 new4
        # 的加油吧威基基 S1E6 以 [Errno 32] Broken pipe 失败后卡了 8.5 小时）。
        # 取消操作本来就是「把这个 claim 关掉」，所以必须包含 failed。
        path, job = GS.find_claim(a.run_id, include_failed=True)
        if not path:
            print(json.dumps({"ok": True, "reason": "no_claim"},
                             ensure_ascii=False))
            return 0
        # 2026-11-15（E109）：**TG 授权类失败要自动落闸门，不能等人。**
        #
        # 原来闸门只由人工写入（告警脚本），代码里没有落闸门的路径 ——
        # 于是 box2 失败一百次也不会被隔离，每轮都白烧一次下载配额。
        # 实测：box2 session 已作废（AuthKeyDuplicated）+ 网络退化
        # （Network is unreachable），但它照样领活、照样下完整集、照样传失败。
        #
        # 判据：失败原因里出现 TG 授权/网络关键字 → 给该 lane 落闸门。
        # 闸门带时间戳，6 小时后自动重试（E107），修好了自己回来。
        _why = str(a.reason or "")
        # E116：错误必须分两类，不能混一个闸门、一个策略：
        #   永久类（TG 吊销 session，只有人工重登能解）→ 闸门写 permanent，永不放行；
        #   网络类（可能自愈）→ 闸门带时间戳，GATE_RETRY_SEC 后放行重试。
        _perm_kw = ("AuthKeyDuplicated", "AuthKeyUnregistered", "AuthKeyInvalid",
                    "SessionRevoked", "SESSION_REVOKED")
        _net_kw = ("Network is unreachable", "Connection to Telegram failed",
                   "ConnectionError", "Timeout")
        _is_perm = any(k in _why for k in _perm_kw)
        if _is_perm or any(k in _why for k in _net_kw):
            try:
                import time as _t
                _d = os.path.join(HERE, ".gha_lane_auth_dead")
                os.makedirs(_d, exist_ok=True)
                _lane = str(job.get("lane") or "")
                if _lane:
                    with open(os.path.join(_d, _lane), "w", encoding="utf-8") as fh:
                        if _is_perm:
                            fh.write("permanent: %s %s TG 授权被吊销（%s），需人工重登后 rm 本文件\n%s\n"
                                     % (_t.strftime("%Y-%m-%d %H:%M"), _lane,
                                        "AuthKeyDuplicated/SessionRevoked", _why[:300]))
                        else:
                            fh.write("%s %s 自动落闸（网络类失败，%ss 后自动重试）\n%s\n"
                                     % (_t.strftime("%Y-%m-%d %H:%M"), _lane,
                                        os.environ.get("GATE_RETRY_SEC", "21600"), _why[:300]))
                    print(json.dumps({"ok": True, "lane_auth_dead": _lane},
                                     ensure_ascii=False))
            except Exception:                          # noqa: BLE001
                pass

        # 2026-11-15（E129）：**resume_scan 类失败要熔断，不能一直占 claim 槽。**
        #
        # 实测：远端 PC（lane=new，120.229.68.6）每分钟 claim 一单，下完发现要
        # 走严格 resume scan 而本机没那份 .part → exit 4 → 释放 claim，
        # 今天 826 次（昨日 350 / 13 日 59）。它不烧源站配额（实测 75s 内各源 day 不变），
        # 但**每轮白占一个 claim 槽**，还把协作日志刷满。
        #
        # 真正的修法在那台机器上设 TG_RESUME_SCAN_MODE=skip（我够不着它的 shell），
        # 控制面能做的是：连续 5 次（20 分钟窗口）就熔断 1 小时，到期自动再试 ——
        # 修好了它自己回来，不用人工 rm（E107 的同一条纪律）。
        if "resume_scan" in _why:
            try:
                import time as _t
                _lane = str(job.get("lane") or "")
                if _lane:
                    _d = os.path.join(HERE, ".gha_lane_scan_fail")
                    os.makedirs(_d, exist_ok=True)
                    _p = os.path.join(_d, _lane + ".json")
                    try:
                        rec = json.load(open(_p, encoding="utf-8"))
                        rec = [float(x) for x in rec if isinstance(x, (int, float))]
                    except Exception:                      # noqa: BLE001
                        rec = []
                    now = _t.time()
                    rec = [x for x in rec if now - x < SCAN_FAIL_WINDOW] + [now]
                    with open(_p, "w", encoding="utf-8") as fh:
                        json.dump(rec, fh)
                    if len(rec) >= SCAN_FAIL_TRIP:
                        _b = os.path.join(HERE, ".gha_lane_scan_broken")
                        os.makedirs(_b, exist_ok=True)
                        with open(os.path.join(_b, _lane), "w", encoding="utf-8") as fh:
                            fh.write("%s %s resume_scan 连续 %d 次失败（%d 分钟内），熔断 %ss 后自动重试\n"
                                     "修法：在那台机器上设 TG_RESUME_SCAN_MODE=skip\n%s\n"
                                     % (_t.strftime("%Y-%m-%d %H:%M"), _lane, len(rec),
                                        SCAN_FAIL_WINDOW // 60,
                                        os.environ.get("SCAN_GATE_RETRY_SEC", "3600"),
                                        _why[:300]))
                        print(json.dumps({"ok": True, "lane_scan_broken": _lane,
                                          "fails": len(rec)}, ensure_ascii=False))
            except Exception:                          # noqa: BLE001
                pass

        if a.source_failed:
            GS.record_source_failure(job.get("source_label"), a.reason)

        # 2026-11-16（E152）：**按集退避不能只看 source_failed。**
        #
        # E074 的「第 3 次起冷却一天」原来包在 `if a.source_failed:` 里面，
        # 而真正反复吃 lane 的是**上传超时**这类失败（run_one 取消 claim 时不带
        # --source-failed）。实测 `现在就出发 S03E09` 8 小时内被领了 **10 次**、
        # `.gha_ep_fail` 里一条都没有 —— 冷却机制写好了却从没被触发过。
        #
        # 现在：只要不是「主动让路」类原因（no_session / paused / manual /
        # shutdown 之类），就记一次按集失败，第 3 次起进 24h 冷却。
        _skip = ("no_session", "paused", "manual", "shutdown", "restart", "owner")
        _reason_l = str(a.reason or "").lower()
        if not any(k in _reason_l for k in _skip):
            _vid = job.get("key") or job.get("video_id")
            if _vid:
                _n, _tripped = GS.record_episode_failure(_vid, a.reason)
                if _tripped:
                    print(json.dumps({"ep_backoff": str(_vid), "fails": _n,
                                      "cls": GS.ep_fail_class(a.reason),
                                      "hours": round(GS.ep_cool_seconds(_n, a.reason) / 3600.0, 2)},
                                     ensure_ascii=False))
        # 2026-11-14（E075）：归还 TG session 租约，否则要等 3h TTL 才释放。
        try:
            _slot = job.get("tg_session_slot")
            if _slot:
                GS.slot_release(int(_slot), a.run_id)
        except Exception:                             # noqa: BLE001
            pass
        GS.release_claim(path)
    except Exception as e:                                # noqa: BLE001
        print(json.dumps({"ok": False, "error": str(e)[:300]},
                         ensure_ascii=False))
        return 2
    try:
        subprocess.run([sys.executable, os.path.join(HERE, "agent_log.py"), "abort",
                        "--name", AGENT, "--task", "韩国双槽上片",
                        "--result", "释放[%s] claim（%s）：%s%s" % (
                            job.get("lane"), a.reason, job.get("job_id", ""),
                            "；源站失败" if a.source_failed else "")],
                       capture_output=True, text=True, timeout=30)
    except Exception:                                     # noqa: BLE001
        pass
    print(json.dumps({"ok": True, "lane": job.get("lane"),
                      "released": job.get("job_id"),
                      "reason": a.reason}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
