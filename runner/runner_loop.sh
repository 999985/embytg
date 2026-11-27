#!/bin/bash
# runner_loop.sh —— 常驻 worker 循环（架构修正 2026-11-23）
#
# 原来的形状：systemd 里 Restart=always + run_one.sh 一次性退出
#   → 每跑完一单就「进程退出 → 重启」，没活时每 15 秒空转一次重启，
#     四条 lane 一天两万多次无效启动，还在每次重启时丢掉进程内缓存
#     （tmdb 缓存、源站会话）。
# 把「重启」当成工作循环，是拿系统的故障恢复机制当调度器用 —— 架构问题。
#
# 现在的形状：一个长驻进程内部循环取活
#   · 领到活 → 干完 → 立刻取下一单
#   · 没活   → 指数退避 20s → 40s → 80s → … 上限 300s
#   · 只有进程真的崩溃（非 0 且非「无活」）才交给 systemd 重启
set -uo pipefail
BASE="${EMBYTG_RUNNER_BASE:-/opt/embytg-runner}"
LOG="$BASE/logs/loop.log"
IDLE_MIN="${LOOP_IDLE_MIN:-20}"
IDLE_MAX="${LOOP_IDLE_MAX:-300}"
sleep_s="$IDLE_MIN"

mkdir -p "$BASE/logs"
echo "$(date '+%F %T') [loop] start pid=$$ slot=${RUNNER_SLOT:-?} lane=${RUNNER_LANE:-auto}" >>"$LOG"

while true; do
    t0=$(date +%s)
    bash "$BASE/run_one.sh" >>"$LOG" 2>&1
    rc=$?
    dur=$(( $(date +%s) - t0 ))

    # 判断这一轮是「干到活了」还是「没活」
    if [ "$dur" -ge 20 ] && [ "$rc" -eq 0 ]; then
        sleep_s="$IDLE_MIN"          # 干到了活，下一轮立刻可跑
        continue
    fi
    # 2026-11-23：**rc=0 且很快退出 = 没活，不是故障**，不该指数退避到 300 秒。
    # 实测 23:44 整条 lane 睡 300s，而队列里明明有 900 条 new —— 四条 lane 里
    # 两条在睡觉，看起来就像「四个号没同时工作」。没活就 20 秒后再看。
    # 2026-11-27（E220）：**rc=1 且几秒就退出 = 没活（NOJOB/IDLE_WITH_ERRORS），
    # 不是故障。** 之前只把 rc=0 当「没活」，于是 slot4 每轮 rc=1 都走指数退避
    # 到 300s —— 日志里 22:40 → 22:45 隔 5 分钟才试一次，下午产量就是这么腰斩的。
    if [ "$rc" -eq 0 ] || { [ "$rc" -eq 1 ] && [ "$dur" -lt 20 ]; }; then
        sleep_s="$IDLE_MIN"
        sleep "$IDLE_MIN"
        continue
    fi
    # 没活 / 失败退避：指数增长，上限 IDLE_MAX
    echo "$(date '+%F %T') [loop] idle rc=$rc dur=${dur}s → sleep ${sleep_s}s" >>"$LOG"
    sleep "$sleep_s"
    sleep_s=$(( sleep_s * 2 ))
    [ "$sleep_s" -gt "$IDLE_MAX" ] && sleep_s="$IDLE_MAX"
done
