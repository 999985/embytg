#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tgbot.py —— 多机器人统一出口（只发消息，**不轮询**，免得跟别的消费者抢 getUpdates）

为什么要有它
------------
项目里所有 Telegram 机器人必须「加一个就自动生效」，不许把 token / chat_id 写死在
业务脚本里（用户 2026-11-11：讨厌写死配置+人工调参）。所以凭据只放
`pipeline/.bot_tokens.json`（600，已在 .gitignore 与三个备份清单白名单里，换机能恢复）。

硬约束（写在这里省得下次再踩）
------------------------------
1. **一个 bot token 同一时刻只能有一个 getUpdates 消费者** → 本工具不轮询；
   要收指令就另起独立守护进程，且别跟本工具抢。
2. Bot API 拉文件上限 20MB（下载）；但 `copyMessage` / 频道内引用转发是服务端动作，
   不受此限——这是 bots 唯一能参与「入库」的口子。
3. 私聊推送要求用户先 /start 过该 bot，否则 sendMessage 报 chat not found。
4. 频道推送要求 bot 是管理员（can_post_messages）。

用法
----
    /usr/bin/python3 tgbot.py whoami
    /usr/bin/python3 tgbot.py bots
    /usr/bin/python3 tgbot.py chats
    /usr/bin/python3 tgbot.py admins                 # 扫配置里的频道，报 bot 身份
    /usr/bin/python3 tgbot.py send --to store --text "hi"
    /usr/bin/python3 tgbot.py send --to -1004413117262 --text "hi" --bot ops
    /usr/bin/python3 tgbot.py sendfile --to store --path /tmp/a.log --caption "log"
    /usr/bin/python3 tgbot.py copy --to store --from -1004413117262 --message 12345
    /usr/bin/python3 tgbot.py drop                   # 清 getUpdates backlog（只清不读）
    /usr/bin/python3 tgbot.py chatid                 # 从 backlog 末条反查 chat_id（不确认）

作为库：
    from tgbot import send, send_file, bot_conf
    send("store", "文本")
"""
import argparse
import json
import mimetypes
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REG = os.path.join(HERE, ".bot_tokens.json")
API = "https://api.telegram.org/bot%s/%s"


# ---------------------------------------------------------------- 配置
HERMES_HOME = os.environ.get("HERMES_HOME") or "/root/.hermes"


def _served_by_hermes(conf):
    """token 是否已被某个 Hermes profile 接管（gateway 在长轮询）。

    2026-11-16 起 ops 这条 bot 成了 butler2 profile 的管家入口，gateway 是它唯一的
    getUpdates 消费者。此时再调 getUpdates 会被 TG 判 409 Conflict —— 所以 drop/chatid
    这两个需要轮询的命令必须让路，只保留「发」的能力。
    """
    tok = (conf or {}).get("token") or ""
    if not tok:
        return None
    cands = [os.path.join(HERMES_HOME, ".env")]
    pdir = os.path.join(HERMES_HOME, "profiles")
    if os.path.isdir(pdir):
        cands += [os.path.join(pdir, d, ".env") for d in os.listdir(pdir)]
    for p in cands:
        try:
            txt = open(p, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        if tok in txt:
            return os.path.basename(os.path.dirname(p)) if "/profiles/" in p else "default"
    return None


def _load():
    if not os.path.exists(REG):
        sys.exit("缺少 %s（机器人登记表）" % REG)
    with open(REG, encoding="utf-8") as f:
        return json.load(f)


def bot_conf(name=None):
    """取某个机器人的配置；name 为空时用登记表 default 或 env EMBYTG_BOT。"""
    reg = _load()
    bots = reg.get("bots") or {}
    name = name or os.environ.get("EMBYTG_BOT") or reg.get("default")
    if not name:
        sys.exit("登记表没有 default，也没有 --bot")
    if name not in bots:
        # 允许直接传 bot token / 数字 id
        for k, v in bots.items():
            if name in (v.get("token"), str(v.get("id")), v.get("username")):
                return k, v
        sys.exit("未知机器人 %r，已登记：%s" % (name, ", ".join(bots)))
    return name, bots[name]


def _chat(conf, to):
    """--to 可以是别名（store）、chat_id、@username。"""
    return (conf.get("chats") or {}).get(to) or to


# ---------------------------------------------------------------- 调用
def call(method, bot=None, **params):
    """调 Bot API，429/5xx 退避重试；返回 result（失败抛 RuntimeError）。"""
    _, conf = bot_conf(bot)
    url = API % (conf["token"], method)
    data = None
    files = None
    for k, v in list(params.items()):
        if hasattr(v, "read"):
            files = files or {}
            files[k] = v
    if files:
        boundary = "----tgbot%d" % int(time.time() * 1000)
        body = b""
        for k, v in params.items():
            if k in files:
                continue
            body += ("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n"
                     % (boundary, k, v)).encode()
        for k, fh in files.items():
            fname = getattr(fh, "name", "upload.bin")
            ctype = mimetypes.guess_type(fname)[0] or "application/octet-stream"
            body += ("--%s\r\nContent-Disposition: form-data; name=\"%s\"; filename=\"%s\"\r\n"
                     "Content-Type: %s\r\n\r\n" % (boundary, k, os.path.basename(fname), ctype)).encode()
            body += fh.read() + b"\r\n"
        body += ("--%s--\r\n" % boundary).encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "multipart/form-data; boundary=%s" % boundary})
    else:
        payload = {k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in params.items()}
        req = urllib.request.Request(url, data=urllib.parse.urlencode(payload).encode()) if payload else urllib.request.Request(url)

    last = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode()).get("result")
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")[:400]
            try:
                j = json.loads(raw)
            except Exception:                                       # noqa: BLE001
                j = {}
            if e.code == 429:
                wait = int((j.get("parameters") or {}).get("retry_after", 3) or 3)
                time.sleep(min(wait, 30))
                last = "429 retry_after=%s" % wait
                continue
            if e.code >= 500:
                time.sleep(2 * (attempt + 1))
                last = "%s %s" % (e.code, raw)
                continue
            raise RuntimeError("%s -> %s %s" % (method, e.code, j.get("description") or raw))
        except Exception as e:                                      # noqa: BLE001
            last = str(e)
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("%s 失败：%s" % (method, last))


# ---------------------------------------------------------------- 高层
def send(to, text, bot=None, silent=False, preview=False):
    """发文本。返回 message_id。"""
    _, conf = bot_conf(bot)
    r = call("sendMessage", bot=bot, chat_id=_chat(conf, to), text=text[:4000],
             disable_notification=bool(silent),
             disable_web_page_preview="true" if not preview else "false")
    return r.get("message_id")


def send_file(to, path, caption="", bot=None, silent=False):
    _, conf = bot_conf(bot)
    with open(path, "rb") as fh:
        r = call("sendDocument", bot=bot, chat_id=_chat(conf, to), document=fh,
                 caption=caption, disable_notification=bool(silent))
    return r.get("message_id")


def admins(bot=None):
    """扫登记表里的频道，报 bot 在其中的身份。"""
    _, conf = bot_conf(bot)
    out = []
    for alias, cid in (conf.get("chats") or {}).items():
        try:
            m = call("getChatMember", bot=bot, chat_id=cid, user_id=conf["id"])
            st = m.get("status")
            out.append((alias, cid, st, m.get("can_post_messages")))
        except RuntimeError as e:
            out.append((alias, cid, "ERR:%s" % str(e)[:60], None))
    return out


# ---------------------------------------------------------------- CLI
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["whoami", "bots", "chats", "admins", "send", "sendfile",
                                    "copy", "drop", "chatid"])
    ap.add_argument("--bot")
    ap.add_argument("--to")
    ap.add_argument("--text")
    ap.add_argument("--path")
    ap.add_argument("--caption", default="")
    ap.add_argument("--from", dest="src")
    ap.add_argument("--message", type=int)
    ap.add_argument("--silent", action="store_true")
    a = ap.parse_args()

    if a.cmd == "bots":
        reg = _load()
        for k, v in (reg.get("bots") or {}).items():
            print("%-6s %-8s @%-18s id=%s %s" % (k, v.get("label"), v.get("username"),
                                                 v.get("id"), v.get("role", "")))
        return 0

    if a.cmd == "whoami":
        name, conf = bot_conf(a.bot)
        me = call("getMe", bot=name)
        print(json.dumps({"profile": name, "id": me["id"], "label": me.get("first_name"),
                          "username": me.get("username")}, ensure_ascii=False))
        return 0

    if a.cmd == "chats":
        name, conf = bot_conf(a.bot)
        print(json.dumps(conf.get("chats") or {}, ensure_ascii=False))
        return 0

    if a.cmd == "admins":
        for alias, cid, st, post in admins(a.bot):
            print("%-8s %-18s %-14s can_post=%s" % (alias, cid, st, post))
        return 0

    if a.cmd == "send":
        if not (a.to and a.text):
            ap.error("需要 --to 与 --text")
        print("sent message_id=%s" % send(a.to, a.text, bot=a.bot, silent=a.silent))
        return 0

    if a.cmd == "sendfile":
        if not (a.to and a.path):
            ap.error("需要 --to 与 --path")
        print("sent message_id=%s" % send_file(a.to, a.path, a.caption, bot=a.bot, silent=a.silent))
        return 0

    if a.cmd == "copy":
        if not (a.to and a.src and a.message):
            ap.error("需要 --to --from --message")
        _, conf = bot_conf(a.bot)
        r = call("copyMessage", bot=a.bot, chat_id=_chat(conf, a.to),
                 from_chat_id=a.src, message_id=a.message)
        print("copied message_id=%s" % r.get("message_id"))
        return 0

    if a.cmd == "drop":
        _, conf = bot_conf(a.bot)
        served = _served_by_hermes(conf)
        if served:
            print("该 bot 已由 Hermes profile %r 的 gateway 长轮询接管，"
                  "这里不抢 getUpdates（会 409 Conflict）。backlog 由 gateway 消费。" % served)
            return 0
        info = call("getWebhookInfo", bot=a.bot)
        before = info.get("pending_update_count", 0)
        if info.get("url"):
            print("有 webhook 在收（%s），不发 getUpdates 清 backlog" % info["url"])
            return 0
        if not before:
            print("backlog 已是 0")
            return 0
        last = call("getUpdates", bot=a.bot, offset=-1, limit=1)
        if not last:
            print("backlog=%s 但取不到末条" % before)
            return 0
        call("getUpdates", bot=a.bot, offset=last[-1]["update_id"] + 1, limit=1, timeout=0)
        after = call("getWebhookInfo", bot=a.bot).get("pending_update_count", 0)
        print("dropped %s -> %s（update_id 水位 %s）" % (before, after, last[-1]["update_id"] + 1))
        return 0

    if a.cmd == "chatid":
        _, conf = bot_conf(a.bot)
        served = _served_by_hermes(conf)
        if served:
            print("该 bot 由 profile %r 的 gateway 消费，本命令不抢 getUpdates；"
                  "要查 chat_id 看 ~/.hermes/channel_directory.json" % served)
            return 0
        last = call("getUpdates", bot=a.bot, offset=-1, limit=1)
        for u in last or []:
            m = u.get("message") or u.get("channel_post") or {}
            ch = m.get("chat") or {}
            print("update=%s chat_id=%s type=%s title=%s text=%r"
                  % (u.get("update_id"), ch.get("id"), ch.get("type"),
                     ch.get("title") or ch.get("username"), (m.get("text") or "")[:40]))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
