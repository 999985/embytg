#!/bin/bash
# ⚠️ 已停用（2026-11-22 00:5x）：GHA 已于 2026-11-11 永久退役，本机没有任何进程
# 触发 transport.yml（最后成功运行 11-21 18:54），于是本脚本每 15 分钟只是
# 「gha_next3 落 claim → 没人执行 → claim 当 running 占满 max_active」。
# 实测 4 条 slot6 孤儿把 active 顶到 6/6，embytg-gha-stock 每轮 no_job。
# timer 已 disable；gha_queue.publish_auto 也补了「派单失败当场释放 claim」。
# 要重新启用必须同时接上触发端（gha_dispatch/transport.yml），否则就是在漏 claim。
# 队列常备：空了就补一单，保证外部 worker（GHA / PC）随时有事可做。
# 2026-11-21：原来把 python 直接塞进 systemd 的 ExecStart heredoc，systemd 报
# "Missing '='"，服务每次都「成功退出」但什么也没做 —— 队列一直是空的。
set -uo pipefail
Q=/opt/embytg/pipeline/queue/assign.json
N=$(python3 - <<'PY'
import json
try:
    d = json.load(open("/opt/embytg/pipeline/queue/assign.json", encoding="utf-8"))
    print(len(d.get("items") or []))
except Exception:
    print(0)
PY
)
if [ "${N:-0}" -gt 0 ]; then
    echo "队列已有 ${N} 单，跳过"
    exit 0
fi
cd /opt/embytg/pipeline || exit 1
EMBYTG_EXTERNAL=1 EMBYTG_WORKER=gha timeout 600 /usr/bin/python3 gha_queue.py publish-auto
