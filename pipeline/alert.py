#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""alert.py —— 统一告警出口（本地留痕优先，外发默认关闭）

为什么需要它：现在的巡检/自愈只把结论写进日志与会话，**人不在就没人知道**。
本工具是唯一告警出口：所有 P0/P1 都走它，保证"有记录、可追溯、可选外发"。

三级行为
--------
1. **一定做**：追加 `logs/alerts.jsonl`（结构化）+ 打印到 stdout
2. **P0 额外做**：在 `AgentCommunication.log` 写一条广播（别的 agent 也能看到）
3. **可选外发**：若配置了 `ALERT_TG_BOT` + `ALERT_TG_CHAT`（环境变量）或
   `pipeline/.alert_channel.json`（{"bot_token":"...","chat_id":"..."}），才发 Telegram。
   **默认不发**——外发消息需用户明确同意后配置。

用法
----
    python3 alert.py --level P0 --code disk_full --msg "磁盘仅剩 3.2GB"
    python3 alert.py --level P1 --code runner_stall --msg "近 3h 无上片完成" --json '{"lane":"new"}'
    python3 alert.py --tail 10          # 看最近 10 条
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ALERTS = os.path.join(HERE, "logs", "alerts.jsonl")
COMM = "/opt/embytg/AgentCommunication.log"
CHANNEL = os.path.join(HERE, ".alert_channel.json")


STATE = os.path.join(HERE, ".alert_state.json")
DEDUPE = {"P0": 0, "P1": 6 * 3600, "P2": 24 * 3600}


def _dedupe(level, code):
    """同一 code 在窗口内只发一次（P0 不去重）"""
    win = DEDUPE.get(level, 0)
    if not win:
        return True
    try:
        st = json.load(open(STATE, encoding="utf-8"))
    except Exception:                                              # noqa: BLE001
        st = {}
    last = st.get("%s|%s" % (level, code), 0)
    if time.time() - last < win:
        return False
    st["%s|%s" % (level, code)] = time.time()
    json.dump(st, open(STATE, "w", encoding="utf-8"), ensure_ascii=False)
    return True


def _mail_send(level, code, msg, extra):
    """SMTP_SSL 发邮件（QQ 邮箱授权码）；未配置则跳过"""
    cfg = {}
    if os.path.exists(CHANNEL):
        try:
            cfg = json.load(open(CHANNEL, encoding="utf-8")).get("smtp") or {}
        except Exception:                                          # noqa: BLE001
            cfg = {}
    if not cfg.get("server") or not cfg.get("password"):
        return "skip(未配置邮件)"
    import smtplib
    from email.header import Header
    from email.mime.text import MIMEText
    import socket
    host = socket.gethostname()
    body = ("[%s] %s\n\n告警码: %s\n主机: %s\n时间: %s\n\n详情: %s\n\n附加: %s\n"
            % (level, msg, code, host, time.strftime("%F %T"),
               msg, json.dumps(extra or {}, ensure_ascii=False, indent=1)))
    m = MIMEText(body, "plain", "utf-8")
    m["Subject"] = Header("[EmbyTG][%s] %s" % (level, code), "utf-8")
    m["From"] = cfg["user"]
    m["To"] = cfg.get("receiver") or cfg["user"]
    try:
        with smtplib.SMTP_SSL(cfg["server"], int(cfg.get("port", 465)), timeout=25) as srv:
            srv.login(cfg["user"], cfg["password"])
            srv.sendmail(cfg["user"], [m["To"]], m.as_string())
        return "mail:ok"
    except Exception as e:                                         # noqa: BLE001
        return "mail_fail:%s" % str(e)[:60]


def _tg_conf():
    """TG 出口配置：`.alert_channel.json` 的 tg 段 > 环境变量 > 默认。

    chat 缺省时自动取 `config.json` 的 `tg_user_id`（用户本人），
    这样「加一个机器人就生效」，不用手填 chat_id（2026-11-16）。
    """
    try:
        tg = json.load(open(CHANNEL, encoding="utf-8")).get("tg") or {}
    except Exception:                                              # noqa: BLE001
        tg = {}
    bot = os.environ.get("ALERT_TG_BOT_NAME") or tg.get("bot") or "ops"
    chat = os.environ.get("ALERT_TG_CHAT") or tg.get("chat_id") or ""
    if not chat:
        # 2026-11-16：该 bot 已是 Hermes profile butler2 的管家入口，用户先在 TG 里
        # 跟它说过话 → channel_directory 里的 DM id 才是 Bot API 认得的 chat_id。
        try:
            d = json.load(open("/root/.hermes/channel_directory.json", encoding="utf-8"))
            chat = str(((d.get("platforms") or {}).get("telegram") or [{}])[0].get("id") or "")
        except Exception:                                          # noqa: BLE001
            chat = ""
    if not chat:
        try:
            chat = str(json.load(open("/opt/embytg/config.json", encoding="utf-8")).get("tg_user_id") or "")
        except Exception:                                          # noqa: BLE001
            chat = ""
    off = os.environ.get("ALERT_TG", "").strip().lower() in ("0", "false", "no")
    return {"bot": bot, "chat": chat, "enabled": bool(tg.get("enabled", True)) and not off}


def _tg_send(text):
    """经 tgbot.py 外发（token 在 `pipeline/.bot_tokens.json`，业务脚本不写死）。"""
    c = _tg_conf()
    if not c["enabled"]:
        return "skip(tg 已关闭)"
    if not c["chat"]:
        return "skip(无 chat_id)"
    try:
        sys.path.insert(0, HERE)
        import tgbot
        mid = tgbot.send(c["chat"], text, bot=c["bot"], silent=True)
        return "tg:ok id=%s" % mid
    except Exception as e:                                         # noqa: BLE001
        s = str(e)
        if "can't initiate conversation" in s:
            return "tg_fail:用户还没给该 bot 发过 /start"
        return "tg_fail:%s" % s[:60]


# 2026-11-12 用户「现在根本不让gha上片，请消除噪音，以后不要被误会」：
# 已退役链路的告警码/文案，任何脚本都不许再外发（邮件/TG），只本地留痕。
# 本地留痕保留 → 可追溯「谁还想报」，但不骚扰用户。
RETIRED_CODES = {"gha_stall", "runner_stall", "gha_paused"}
RETIRED_WORDS = ("GHA 疑似停摆", "GHA 已暂停", "GHA 停摆")


AGENT_QUEUE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "logs", "alerts_agent_queue.jsonl")


def _agent_queue(rec):
    """把告警投给 agent（Hermes cron 每 30 分钟消费一次）。

    2026-11-15 用户：「又发了一个邮件，能不能改成通知你而不是我」。
    之前的出口是邮件直发用户，而绝大多数告警是**我自己能修的**——
    发给人只增加了他的负担，还让真正的 P0 淹没在噪音里。

    改法：告警照旧写 alerts.jsonl（全量留痕），另写一份 agent 队列；
    由 hermes cron 的「告警自愈」任务消费：能修的自己修，
    修不了才升级到 TG 找用户。邮件通道默认关闭（smtp.enabled=false）。
    """
    try:
        os.makedirs(os.path.dirname(AGENT_QUEUE), exist_ok=True)
        with open(AGENT_QUEUE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return "agent:queued"
    except Exception as e:                                     # noqa: BLE001
        return "agent_fail:%s" % str(e)[:60]


def _mail_enabled():
    """邮件默认关闭；只有配置里显式 smtp.enabled=true 才发。"""
    if os.environ.get("ALERT_MAIL", "").strip() in ("0", "false", "no"):
        return False
    try:
        cfg = json.load(open(CHANNEL, encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        return False
    smtp = cfg.get("smtp") or {}
    return bool(smtp.get("enabled"))


def _retired(code, msg):
    return code in RETIRED_CODES or any(w in (msg or "") for w in RETIRED_WORDS)


def alert(level, code, msg, extra=None):
    rec = {"ts": time.strftime("%F %T"), "level": level, "code": code, "msg": msg, "extra": extra or {}}
    os.makedirs(os.path.dirname(ALERTS), exist_ok=True)
    line = "[%s] %s %s: %s" % (level, rec["ts"], code, msg)
    print(line, flush=True)
    if _retired(code, msg):
        rec["channels"] = {"suppressed": "退役链路告警（GHA已退役/上片归韩国阿里云），按用户 2026-11-12 指令消除噪音；仅本地留痕不外发"}
        rec["suppressed"] = True
        with open(ALERTS, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return rec
    if level == "P0":
        try:
            with open(COMM, "a", encoding="utf-8") as f:
                f.write("\n## [%s] hermes-ops | 告警 | P0 %s\n- **结果**：%s（%s）\n"
                        % (rec["ts"][:16], code, msg, json.dumps(extra, ensure_ascii=False)[:200]))
        except Exception:                                          # noqa: BLE001
            pass
    rec["channels"] = {}
    if _dedupe(level, code):
        # 2026-11-15：主出口改成 agent 队列（见 _agent_queue 说明）
        rec["channels"]["agent"] = _agent_queue(rec)
        if _mail_enabled():
            rec["channels"]["mail"] = _mail_send(level, code, msg, extra)
        # TG 仍是兜底：P0 且 agent 队列写失败时才用
        if level == "P0":
            # 2026-11-16 管理2号 接入：P0 一律推 TG（不再只在队列写失败时兜底）。
            # 队列是给我自己消费的，P0 是用户该知道的；P1/P2 仍不发，避免噪音。
            rec["channels"]["tg"] = _tg_send(line)
    else:
        rec["channels"]["skipped"] = "dedupe"
    with open(ALERTS, "a", encoding="utf-8") as f:      # 每事件只落一行（含 channels）
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--level", default="P1", choices=["P0", "P1", "P2"])
    ap.add_argument("--code", required=False)
    ap.add_argument("--msg", required=False)
    ap.add_argument("--json", dest="extra")
    ap.add_argument("--tail", type=int, default=0)
    a = ap.parse_args()
    if a.tail:
        if not os.path.exists(ALERTS):
            print("暂无告警")
            return 0
        for l in open(ALERTS, encoding="utf-8").read().strip().splitlines()[-a.tail:]:
            r = json.loads(l)
            print("%s %-3s %-22s %s" % (r["ts"], r["level"], r["code"], r["msg"]))
        return 0
    if not (a.code and a.msg):
        ap.error("需要 --code 与 --msg")
    extra = json.loads(a.extra) if a.extra else {}
    alert(a.level, a.code, a.msg, extra)
    return 0


if __name__ == "__main__":
    sys.exit(main())
