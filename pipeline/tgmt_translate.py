#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tgmt_translate.py — 用 B站 Index-Translate 把英文分集标题译成中文。

为什么（2026-11-15，用户要求）：
    用户明确「不想英文」。TMDB 中文缺标题时，用英文回退会在客户端看到
    "London Calling" 这种名字 —— 对中文观影是噪音。

接口（用户提供，B站播放器 ai 字幕用的公开端点）：
    POST https://index-translate.bilibili.com/v1/chat/completions
    model: Index-Translate-35B-A3B
    密钥随意填，不验证
    OpenAI 兼容格式

注意：
    · 必须带 UA / Referer，否则 412（实测）
    · 批量翻译：一次请求译多行，比逐条快几十倍（实测 3 条 0.9s）
    · 译文要校验条数一致，对不上就整批丢弃（宁可不译，不能错位）
"""
import json
import os
import urllib.request

URL = "https://index-translate.bilibili.com/v1/chat/completions"
MODEL = "Index-Translate-35B-A3B"
HDR = {
    "Content-Type": "application/json",
    "Authorization": "Bearer sk-bilibili-index-translate",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0.0.0 Safari/537.36"),
    "Referer": "https://www.bilibili.com/",
    "Origin": "https://www.bilibili.com",
}

BATCH = 20          # 单次最多译多少条：太多容易串行错位
TIMEOUT = 60


def translate(lines, retries=2):
    """把英文标题列表译成中文，返回同长度列表（失败位置保持原文）。

    校验原则：**行数对不上就整批放弃**。错位的标题比英文原文更糟 ——
    E1 显示 E2 的名字，用户无从发现。
    """
    out = list(lines)
    if not lines:
        return out
    for i in range(0, len(out), BATCH):
        chunk = out[i:i + BATCH]
        todo = [(j, t) for j, t in enumerate(chunk) if t and t.strip()]
        if not todo:
            continue
        texts = [t for _, t in todo]
        prompt = ("把下面每行英文剧集标题翻译成简洁的中文。"
                  "逐行输出对应译文，行数必须与输入相同。"
                  "不要解释、不要编号、不要保留英文原词：\n"
                  + "\n".join(texts))
        body = json.dumps({
            "model": MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 200 * len(texts),
        }).encode("utf-8")
        got = None
        for _ in range(max(1, retries)):
            try:
                req = urllib.request.Request(URL, data=body, headers=HDR)
                d = json.loads(urllib.request.urlopen(req, timeout=TIMEOUT).read())
                txt = (d["choices"][0]["message"]["content"] or "").strip()
                rows = [x.strip() for x in txt.split("\n") if x.strip()]
                if len(rows) == len(texts):
                    got = rows
                    break
            except Exception:                              # noqa: BLE001
                continue
        if not got:
            continue                       # 整批放弃，保留英文原文
        for (j, _src), dst in zip(todo, got):
            if dst and dst != _src:
                out[i + j] = dst
    return out


if __name__ == "__main__":
    import sys
    src = sys.argv[1:] or ["London Calling", "Planet of the Men and Women",
                           "Nixon's Presidential Library is a Bookmobile"]
    for a, b in zip(src, translate(src)):
        print("%-44s → %s" % (a[:42], b))
