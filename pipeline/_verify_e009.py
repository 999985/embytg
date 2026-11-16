#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""E009 只读验证：确认服务器上的 ffprobe_window() 在真实日志上给出规模无关信号。"""
import importlib.util
import json

spec = importlib.util.spec_from_file_location("af", "/opt/embytg/pipeline/auto_finalize.py")
af = importlib.util.module_from_spec(spec)
spec.loader.exec_module(af)

pw = af.ffprobe_window()
print("PW =", json.dumps(pw, ensure_ascii=False))
print("repeat_ok(==0) =", pw["repeat"] == 0)
print("rate_ok(0<=r<=0.05) =", 0 <= pw["rate"] <= 0.05)
print("has_repeat_check =", "ffprobe无同路径反复超时" in open(
    "/opt/embytg/pipeline/auto_finalize.py", encoding="utf-8").read())
# 全窗口对比：旧口径（绝对条数）
print("legacy_timeout_total(recent30) =", af.ffprobe_timeout())
print("probe_killed =", af.probe_killed())
print("VERIFY_E009_OK")
