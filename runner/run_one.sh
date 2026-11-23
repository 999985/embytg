#!/bin/bash
set -u
BASE="${EMBYTG_RUNNER_BASE:-/opt/embytg-runner}"
PIPELINE_DIR="${EMBYTG_PIPELINE_DIR:-/opt/embytg/pipeline}"
SYSPY="${EMBYTG_SYSPY:-/usr/bin/python3}"
ENV="$BASE/runner.env"
LOG="$BASE/logs/runner.log"
VENV="$BASE/venv/bin/python"
POLICY="$BASE/lane_policy.py"
SLOT="${RUNNER_SLOT:-1}"
SYSTEM_LANE="${RUNNER_LANE:-}"

case "$SLOT" in
  # 2026-11-14：原来写死 1|2|3。加了第 4 个 TG 账号（Carlis）后，
  # 本机想再起一个 runner 实例会被这里挡回去 —— 账号空着用不上。
  # 槽位上限必须跟 TG_SESSION_STRING_N 的数量对齐，不能再写死。
  1|2|3|4|5|6) ;;
  *) echo "bad RUNNER_SLOT=$SLOT"; exit 2 ;;
esac

mkdir -p "$BASE/logs" "$BASE/work"
set -a
. "$ENV"
set +a

MODE="${SYSTEM_LANE:-${LANE:-auto}}"
# 2026-11-15（E105）：默认交给池子决定，不再写死 3。
# gha_next3 会用 min(传入值, 可用账号槽位数) 作为真正的并发上限 ——
# 加账号自动扩容、减账号自动收敛。这里给一个大数当"天花板"，
# 即等于告诉它「按池子容量来」；要人工保护时再在 runner.env 设 TG_MAX_ACTIVE。
MAX_ACTIVE="${TG_MAX_ACTIVE:-99}"
SOURCE_ALLOW="${GHA_SOURCE_ALLOW:-}"
export PYTHONUNBUFFERED=1
export TG_CHUNK_MIB=128
if [ "$SLOT" = "3" ]; then
  export TG_UPLOAD_JOBS="${TG_UPLOAD_JOBS_SLOT3:-1}"
else
  export TG_UPLOAD_JOBS="${TG_UPLOAD_JOBS:-2}"
fi
# 2026-11-14（E075）：只有一个 TG 账号时必须单会话——
# 多会话模式下 gha_upload 不会传 --session-slot，pc_upload 就用「全部 session」，
# 而 runner.env 里 TG_SESSION_STRING_2 是空值，等于把同一个账号发两次。
# 用实际账号数决定，不要写死。
_n_sess=0
while IFS= read -r _l; do
  case "$_l" in
    TG_SESSION_STRING=*)
      _v="${_l#TG_SESSION_STRING=}"
      [ -n "$_v" ] && _n_sess=$((_n_sess+1)) ;;
  esac
done < "$ENV"
if [ "$_n_sess" -ge 2 ]; then
  export TG_UPLOAD_MULTI_SESSION=1
else
  export TG_UPLOAD_MULTI_SESSION=0
fi
unset _l _v _n_sess
export TG_SESSION_WEIGHTS="${TG_SESSION_WEIGHTS:-1,7}"
export TG_WORKER_TIMEOUT="${TG_WORKER_TIMEOUT:-3600}"
export TG_UPLOAD_TIMEOUT="${TG_UPLOAD_TIMEOUT:-7200}"
export TG_DOWNLOAD_TIMEOUT="${TG_DOWNLOAD_TIMEOUT:-7200}"
UPLOAD_OUTER_TIMEOUT=$((TG_UPLOAD_TIMEOUT + 1800))
export TG_DOWNLOAD_RETRIES="${TG_DOWNLOAD_RETRIES:-3}"
export TG_MIN_DOWNLOAD_MBPS="${TG_MIN_DOWNLOAD_MBPS:-1.5}"
export TG_SPEED_SAMPLE_MB="${TG_SPEED_SAMPLE_MB:-32}"
export TG_MAX_DOWNLOAD_ETA_SEC="${TG_MAX_DOWNLOAD_ETA_SEC:-7200}"
export TG_PARTIAL_DIR="${TG_PARTIAL_DIR_SLOT:-$BASE/_partials.slot$SLOT}"
export TG_MANIFEST_DIR="${TG_MANIFEST_DIR_SLOT:-$BASE/_manifests.slot$SLOT}"
MEDIA_DIR="$BASE/media.slot$SLOT"
# 2026-11-16（E157）：上传失败后的源文件缓存（硬链接复用，见下方 KEEP_HIT/KEEP_SAVE）
KEEP_DIR="$BASE/_keep"
RUN_KEY=""
# 2026-11-23：**只租不放会让号被锁死。** gha_state 的 slot 租约原本只有
# TTL（3h）或孤儿判定才回收 —— 3 次上传之后三个号全占满，之后每次领单都报
# no_session（近 2 小时 37 次，是最大的错误源）。退出时统一释放。
RUN_KEYS=()
release_leases() {
  local k
  for k in "${RUN_KEYS[@]:-}"; do
    [ -n "$k" ] || continue
    # 已经落地 manifest = 上传成功，只是登记/收尾失败 → **保留 claim** 交给下一轮
    # FINISH 重试（PC 今天 7 次 ssh 超时都靠这条恢复）。此时只还 session 租约，
    # 不释放条目 claim，否则同一集会被重新领走、重复上传一份。
    if [ -n "${WORK:-}" ] && [ -f "$WORK/manifest.path" ]; then
      ssh_o "cd /opt/embytg/pipeline && $SYSPY -c \"import gha_state as G; G.release_slot_by_run('$k')\"" >/dev/null 2>&1 || true
    else
      ssh_o "cd /opt/embytg/pipeline && $SYSPY -c \"import gha_state as G; G.abandon_run('$k')\"" >/dev/null 2>&1 || true
    fi
    rm -f "$BASE/run/$k.hb" 2>/dev/null || true
  kill "${RUN_HB_PID:-0}" 2>/dev/null || true
  done
}
trap release_leases EXIT
WORK=""
JOB=""
ACTIVE_LANE=""
CLAIM_ACTIVE=0
KEEP_CLAIM=0
PAUSED=0
CAPACITY=0
ERRORS=0

mkdir -p "$BASE/work" "$TG_MANIFEST_DIR" "$TG_PARTIAL_DIR" "$MEDIA_DIR"

exec 9>"/run/lock/embytg-runner-slot${SLOT}.lock"
flock -n 9 || { echo "runner slot $SLOT already active"; exit 0; }

# 2026-11-14（E082）：自动判定「是否在控制面本机运行」。
# 是 → ssh_o 直接本地执行，消除 SSH 失败吞错的僵尸 claim。
_my_ips="$(hostname -I 2>/dev/null) $(hostname -f 2>/dev/null) $(hostname 2>/dev/null)"
case " $_my_ips " in
  *" $ORACLE_HOST "*) export EMBYTG_LOCAL_CONTROL=1 ;;
esac
[ "$ORACLE_HOST" = "127.0.0.1" ] && export EMBYTG_LOCAL_CONTROL=1
[ "$ORACLE_HOST" = "localhost" ] && export EMBYTG_LOCAL_CONTROL=1

# 2026-11-15：**job.json 可能有多行 JSON。**
# gha_next3 在「扫描闸门重试」时先 emit 一条 {"ok": true, "scan_gate_retry": ...}
# 再继续领活，于是同一份 stdout 里会有两条对象 —— 而下面用 json.load() 读整个文件，
# 直接抛 "Extra data"，被 || 兜成 bad_json，看起来像「队列坏了」。
# 实测新起的 slot2/slot3 就在这个分支里空转。判据取**最后一条**能解析的对象。
json_last() {
  "$VENV" - "$1" <<'PYJ'
import json, sys
last = {}
for line in open(sys.argv[1], encoding="utf-8", errors="ignore"):
    line = line.strip()
    if not line:
        continue
    try:
        o = json.loads(line)
    except Exception:
        continue
    if isinstance(o, dict):
        last = o
print(json.dumps(last, ensure_ascii=False))
PYJ
}

log() { printf '%s [slot%s] %s\n' "$(date '+%F %T')" "$SLOT" "$*" | tee -a "$LOG"; }
ssh_o() {
  # 2026-11-14（E082）：**根因修复** —— 在 Oracle 本机运行时直接执行，不绕 SSH。
  #
  # 原来无条件 ssh 回自己（ORACLE_HOST=132.145.125.171），于是：
  #   · 每次领片/收尾/释放 claim 都要过一次 SSH 握手
  #   · ssh 失败时调用方是 `... || true`，错误被吞掉
  #   · 结果 claim 没人释放 → 变成僵尸占着 slot → run_one 领不到新活空转
  #     （实测两个 claim 挂了 9~12 分钟、进程早已不在，run_one 在 sleep 180 里循环）
  #
  # 判据：本机 hostname / IP 与 ORACLE_HOST 一致就直接 bash -c 执行。
  # 非本机（云盒）仍走 SSH，行为不变。
  if [ "$EMBYTG_LOCAL_CONTROL" = "1" ]; then
    bash -c "$*"
    return $?
  fi
  ssh -i "$ORACLE_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -o ConnectTimeout=15 -o ServerAliveInterval=10 -o ServerAliveCountMax=12 \
      -o ConnectionAttempts=3 -o TCPKeepAlive=yes \
      -p "$ORACLE_PORT" "$ORACLE_USER@$ORACLE_HOST" "$@"
}
cancel_claim() {
  local reason="$1"; shift || true
  # 2026-11-14（E082）：**不能再 `|| true` 静默吞错** ——
  # 释放失败 = claim 变僵尸 = 占着 session 槽 = 整条 lane 空转。
  # 重试 3 次（间隔递增），仍失败就留下显式证据到日志和磁盘，
  # 让 claim_reap / 巡检能查到。
  local _try
  for _try in 1 2 3; do
    if ssh_o "cd /opt/embytg/pipeline && python3 gha_cancel.py --run-id '$RUN_KEY' --reason '$reason' $*" >>"$BASE/logs/cancel.log" 2>&1; then
      return 0
    fi
    log "CANCEL_RETRY $_try/3 failed (reason=$reason)"
    sleep $((_try * 2))
  done
  log "CANCEL_FAILED run=$RUN_KEY reason=$reason —— claim 可能变僵尸"
  printf '%s %s %s\n' "$(date '+%F %T')" "$RUN_KEY" "$reason" >>"$BASE/logs/cancel_failed.log" 2>/dev/null || true
  return 1
}
release_failed_claim() {
  local run="$1"
  ssh_o "cd /opt/embytg/pipeline && python3 -c \"import glob,json,os;[os.remove(p) for p in glob.glob('.gha_claims/*.json') if json.load(open(p)).get('run_id')=='$run']\"" >/dev/null 2>&1 || true
}
policy() {
  "$VENV" "$POLICY" "$@" >/dev/null 2>&1 || true
}
policy_lane() {
  case "$1" in
    upgrade2) echo upgrade ;;
    new2|new3|new4|new5) echo new ;;
    *) echo "$1" ;;
  esac
}
mark_lane_done() {
  local lane="$1" reason="$2"
  lane="$(policy_lane "$lane")"
  ssh_o "touch /opt/embytg/pipeline/.gha_lane_done_$lane" >/dev/null 2>&1 || true
  policy mark-done --lane "$lane" --reason "$reason"
}
cleanup() {
  local rc=$?
  if [ "$CLAIM_ACTIVE" = "1" ] && [ "$KEEP_CLAIM" != "1" ]; then
    cancel_claim "runner_exit_rc_${rc}"
  fi
  log "EXIT ${RUN_KEY:-none} rc=$rc"
}
trap cleanup EXIT

case "$MODE" in
  auto|new|new2|new3|new4|new5|fill|upgrade|upgrade2) ;;
  *) log "BAD_LANE $MODE"; exit 2 ;;
esac

# Only slot 1 owns failed-claim recovery. A failed finish must never be
# attempted by two runners at the same time.
if [ "$SLOT" = "1" ]; then
  CLAIMS_FILE="$BASE/logs/claims.$$.json"
  if ssh_o "cd /opt/embytg/pipeline && python3 -c \"import glob,json; print(json.dumps([json.load(open(p)) for p in sorted(glob.glob('.gha_claims/*.json'))]))\"" >"$CLAIMS_FILE" 2>"$BASE/logs/claims.err"; then
    chmod 600 "$CLAIMS_FILE" 2>/dev/null || true
    read -r CLAIM_KIND CLAIM_LANE CLAIM_RUN <<<"$("$VENV" "$POLICY" select-claim --file "$CLAIMS_FILE" 2>/dev/null || echo none)"
  else
    CLAIM_KIND="error"; CLAIM_LANE="-"; CLAIM_RUN=""
  fi
  rm -f "$CLAIMS_FILE"

  if [ "$CLAIM_KIND" = "error" ]; then
    log "CLAIM_SCAN_FAILED"
    exit 1
  fi
  if [ "$CLAIM_KIND" = "running" ]; then
    log "REMOTE_RUNNING lane=$CLAIM_LANE run=$CLAIM_RUN"
  fi
  # With the five-way split, slot 1 must only recover claims owned by its
  # own lane.  A failed claim from box2/box3 has its manifest on that box,
  # not on the Oracle control plane; trying to resume it here would fail
  # every timer run and leave new2 permanently idle.
  if [ "$CLAIM_KIND" = "failed" ] && [ -n "$CLAIM_RUN" ]; then
    OWN_LANE="$MODE"
    [ "$MODE" = "upgrade2" ] && OWN_LANE="upgrade"
    if [ "$CLAIM_LANE" != "$MODE" ] && [ "$CLAIM_LANE" != "$OWN_LANE" ]; then
      log "REMOTE_FAILED_SKIP lane=$CLAIM_LANE run=$CLAIM_RUN (owned by another runner)"
      CLAIM_KIND="none"
      CLAIM_RUN=""
    fi
  fi
  if [ "$CLAIM_KIND" = "failed" ] && [ -n "$CLAIM_RUN" ]; then
    exec 8>/run/lock/embytg-runner-recovery.lock
    if ! flock -n 8; then
      log "RECOVERY_BUSY run=$CLAIM_RUN"
      exit 0
    fi
    RUN_KEY="$CLAIM_RUN"
    RUN_KEYS+=("$RUN_KEY")
    if ssh_o "test -f /opt/embytg/pipeline/.gha_paused"; then
      log "PAUSED; failed claim kept: $CLAIM_RUN"
      exit 0
    fi
    MANIFEST="/opt/embytg/pipeline/_gha_manifests/$CLAIM_RUN.json"
    if ! ssh_o "test -f '$MANIFEST'"; then
      log "RESUME manifest missing for failed claim $CLAIM_RUN; left for manual recovery"
      exit 1
    fi
    log "RESUME_FINISH $CLAIM_RUN lane=$CLAIM_LANE"
    FINISH_OK=0
    mkdir -p "$BASE/work/$CLAIM_RUN"
    # 2026-11-15：**收尾重试必须有上限。**
    # 原来失败就 exit 1 + 保留 claim，靠 systemd/timer 再拉起来重跑 —— 于是
    # 一个「永远收不掉」的 claim 能无限重跑。实测 slot1 的
    # 20261115_133536-slot1-new2-625c44 因 TMDB 锚点写错（295123 不存在）
    # 连跑 144 次（21:35→22:27，近一小时零产出），每次都在同一行 FAIL。
    # 安全恢复的本意是「别丢已上传的字节」，不是「把 lane 赔进去」；
    # 到了一个明确的次数就应当**隔离该 claim 并让 lane 去干别的活**。
    CYCLES_FILE="$BASE/work/$CLAIM_RUN/.resume_cycles"
    RESUME_CYCLES=$(cat "$CYCLES_FILE" 2>/dev/null || echo 0)
    RESUME_CYCLES=$((RESUME_CYCLES + 1))
    echo "$RESUME_CYCLES" >"$CYCLES_FILE"
    MAX_RESUME_CYCLES="${TG_MAX_RESUME_CYCLES:-4}"
    if [ "$RESUME_CYCLES" -gt "$MAX_RESUME_CYCLES" ]; then
      QDIR=/opt/embytg/pipeline/.gha_claims_quarantine
      log "RESUME_GIVE_UP $CLAIM_RUN cycles=$RESUME_CYCLES > $MAX_RESUME_CYCLES; quarantining claim"
      # 2026-11-21：claim 自 11-20 起按 run 分文件（<lane>.<run_id>.json）。
      # 旧的按 lane 拼文件名的 mv 永远找不到源、失败又被 || true 吞掉，
      # 于是「隔离」是空操作：claim 留在活跃集，slot1 每 15 秒重选一次，
      # .resume_cycles 从 5 涨到 260+，每轮一条 P1。必须按 run_id 搬。
      ssh_o "cd /opt/embytg/pipeline && python3 claim_quarantine.py --run-id '$CLAIM_RUN'" >/dev/null 2>&1 || true
      ssh_o "cd /opt/embytg/pipeline && python3 alert.py --level P1 --code resume_give_up --msg \"claim $CLAIM_RUN 连续 $RESUME_CYCLES 轮收尾失败，已隔离（工作目录 $BASE/work/$CLAIM_RUN/resume.log）；lane $CLAIM_LANE 已释放\"" >/dev/null 2>&1 || true
      policy note --lane "$(policy_lane "$CLAIM_LANE")" --result failed
      CLAIM_ACTIVE=0
      exit 0
    fi
    log "RESUME_FINISH cycle $RESUME_CYCLES/$MAX_RESUME_CYCLES"
    : >"$BASE/work/$CLAIM_RUN/resume.log"
    for attempt in 1 2; do
      printf '\n=== resume finish attempt %s ===\n' "$attempt" >>"$BASE/work/$CLAIM_RUN/resume.log"
      if ssh_o "cd /opt/embytg/pipeline && timeout 3600 python3 gha_finish.py --run-id '$CLAIM_RUN' --manifest '$MANIFEST'" >>"$BASE/work/$CLAIM_RUN/resume.log" 2>&1; then
        FINISH_OK=1
        break
      fi
      log "RESUME_FINISH attempt $attempt failed; see $BASE/work/$CLAIM_RUN/resume.log"
      [ "$attempt" -eq 1 ] && sleep 30
    done
    if [ "$FINISH_OK" = "1" ]; then
      policy note --lane "$(policy_lane "$CLAIM_LANE")" --result done
      log "RESUME_FINISH DONE $CLAIM_RUN"
      rm -rf -- "$MEDIA_DIR"
      exit 0
    fi
    if [ "${TG_FINISH_EMBY_PENDING_OK:-0}" = "1" ] \
       && grep -q '"tgdb": "ok"' "$BASE/work/$CLAIM_RUN/resume.log" 2>/dev/null \
       && grep -q 'emby_missing' "$BASE/work/$CLAIM_RUN/resume.log" 2>/dev/null; then
      log "RESUME_FINISH_PARTIAL_EMBY_PENDING $CLAIM_RUN (registered+NFO ok; released)"
      release_failed_claim "$CLAIM_RUN"
      CLAIM_ACTIVE=0
      rm -rf -- "$MEDIA_DIR"
      exit 0
    fi
    log "RESUME_FINISH failed; claim kept for safe recovery"
    exit 1
  fi
fi

if [ "$MODE" = "auto" ]; then
  LANES_ORDER="$("$VENV" "$POLICY" next 2>/dev/null || echo new)"
  # 2026-11-19（E183）：policy.next 返回的是**全局**推荐窗口（当前固定两格 new/fill），
  # 四条 lane 拿到同一份候选 → 前两条被别人 claim 走后，剩下的一律 LANE_BUSY → IDLE_NO_JOB。
  # 实测 slot2 连续 1389 次重启全是 `LANE_BUSY lane=new/fill` 后 exit 0。
  #
  # 但 upgrade **暂时不能**放进兜底：实测启用后第一条 upgrade claim 就卡死在收尾——
  # `verify_upgrade_emby()` 要求 `len(matching)==1 且 old 消失`，而本项目按
  # 「多版本共存、不删旧文件」执行，新旧两个文件同时在 Emby 里就是 2 条，永远不可能收敛。
  # 等收尾判据支持多版本（或多版本命名被 Emby 合并成单条）后再打开。
  for _L in new fill; do
    case " $LANES_ORDER " in *" $_L "*) ;; *) LANES_ORDER="$LANES_ORDER $_L" ;; esac
  done
else
  # 2026-11-17（E163）：**固定车道的 runner 在 NOJOB 之后必须能换道，不能空转。**
  #
  # 事故形状：slot4 用 LANE=fill 固定，而 fill 队列只有 12 条、大部分集已冷却 →
  # 实测今日 24 次 `NOJOB lane=fill`，每 15.5 分钟一次（03:44→06:04 连续），
  # 这一条 lane 整天基本在 IDLE_NO_JOB；同时 upgrade 队列压着 2000 条、
  # new 队列实际待扫 2706 条。五条 lane 里白扔一条 ≈ 20% 产能。
  #
  # 原因不是「fill 没事干」这件事本身，而是 `LANES_ORDER="$MODE"` 把这个事实
  # 变成了「这条 lane 不许干别的」。固定车道的**优先级**要保留（fill 永远先试
  # fill），但**独占性**没有理由 —— claim 是全局加锁的，多条 lane 抢同一队列
  # 不会重复领片（同一 state_key 只能被一个 claim 拿走）。
  LANES_ORDER="$MODE"
  for L in upgrade new fill; do
    [ "$L" = "$MODE" ] && continue
    LANES_ORDER="$LANES_ORDER $L"
  done
fi
log "START mode=$MODE candidates=$LANES_ORDER"

# 2026-11-19（E182）：**上传断流闸门**，必须在领 claim 之前问。
# 03:42–05:41 那两小时：每条 claim 都先把整集从源站下完（配额硬顶），
# 上传 100% 零进展失败 → 释放 → 下一条 lane 再领一集。产出 0，白烧 ~12GB 配额。
# 判据只看「零进展失败的连续次数 + 涉及几条 lane」，不预测速度（速度是噪声：
# 同一账号 03:44 是 0.12MB/s、05:41 是 6MB/s）。
# 关掉：TG_UPLOAD_BREAKER=0（记账照旧）。
if [ "${TG_UPLOAD_BREAKER:-1}" != "0" ] && [ -f "$PIPELINE_DIR/upload_health.py" ]; then
  UB_JSON="$("$SYSPY" "$PIPELINE_DIR/upload_health.py" --gate --slot "$SLOT" 2>>"$BASE/logs/upload_health.err")"
  UB_RC=$?
  if [ "$UB_RC" = "75" ]; then
    UB_WAIT=$(printf '%s' "$UB_JSON" | sed -n 's/.*"wait_sec": *\([0-9]*\).*/\1/p' | head -1)
    case "$UB_WAIT" in ''|*[!0-9]*) UB_WAIT=600 ;; esac
    [ "$UB_WAIT" -lt 30 ] && UB_WAIT=30
    [ "$UB_WAIT" -gt 600 ] && UB_WAIT=600
    log "UPLOAD_HOLD slot=$SLOT wait=${UB_WAIT}s $UB_JSON"
    sleep "$UB_WAIT"
    exit 0
  fi
fi

SELECTED=0
BUDGET_PAUSE=0
BUDGET_WAIT=600
for L in $LANES_ORDER; do
  RUN_KEY="$(date -u +%Y%m%d_%H%M%S)-slot${SLOT}-${L}-$(openssl rand -hex 3)"
  RUN_KEYS+=("$RUN_KEY")
  # 心跳：上传进程 argv 里没有 run_id，判活只能靠它（gha_state._hb_fresh）
  mkdir -p "$BASE/run"
  ( while :; do date +%s >"$BASE/run/$RUN_KEY.hb" 2>/dev/null || exit 0; sleep 30; done ) &
  RUN_HB_PID=$!
  WORK="$BASE/work/$RUN_KEY"
  JOB="$WORK/job.json"
  mkdir -p "$WORK"
  MAX_ATTEMPTS=8
  [ "$L" = "new" ] && MAX_ATTEMPTS=30
  [ "$L" = "new2" ] && MAX_ATTEMPTS=30
  [ "$L" = "new3" ] && MAX_ATTEMPTS=30
  [ "$L" = "new4" ] && MAX_ATTEMPTS=30
  [ "$L" = "new5" ] && MAX_ATTEMPTS=30
  [ "$L" = "fill" ] && MAX_ATTEMPTS=12
  [ "$L" = "upgrade2" ] && MAX_ATTEMPTS=300
  SOURCE_ARG=""
  [ -n "$SOURCE_ALLOW" ] && SOURCE_ARG="--source-allow '$SOURCE_ALLOW'"
  # 2026-11-16（E148/E151）：**日字节预算 + 匀速闸门**。
  # 必须在领 claim 之前问：超速就等、预算见底就停 —— 不要「先领再挂着」，
  # 那会让 claim_reap 把正常节流误判成僵尸 claim（判据是「有 claim 无进程」）。
  # 闸门是全局的（四个 lane 共用同一个 pace），所以这里只按 lane 的优先级放行。
  if [ "${TG_BUDGET_GATE:-1}" != "0" ]; then
    GATE_OUT="$("$VENV" "$BASE/budget_gate.py" --lane "$L" 2>>"$BASE/logs/budget_gate.err")"
    GATE_RC=$?
    if [ "$GATE_RC" != "0" ]; then
      BUDGET_PAUSE=1
      BUDGET_WAIT=$(printf '%s' "$GATE_OUT" | sed -n 's/.*"wait_sec": *\([0-9]*\).*/\1/p' | head -1)
      BUDGET_REASON=$(printf '%s' "$GATE_OUT" | sed -n 's/.*"reason": *"\([^"]*\)".*/\1/p' | head -1)
      case "$BUDGET_WAIT" in ''|*[!0-9]*) BUDGET_WAIT=600 ;; esac
      [ "$BUDGET_WAIT" -lt 60 ] && BUDGET_WAIT=60
      log "BUDGET_WAIT lane=$L rc=$GATE_RC reason=${BUDGET_REASON:-unknown} wait=${BUDGET_WAIT}s $GATE_OUT"
      break
    fi
  fi
  # 2026-11-23：**必须带 worker 身份。** 原来这里不传 EMBYTG_WORKER，
  # gha_next3 落到默认值 "local" —— job_claim 里所有本机记录都是同一个
  # worker，worker_stat 的成功率/连续失败就分不清是哪条 lane，
  # 不健康剔除也就无从谈起（PC 之所以能看到 slot5，是因为它另外传了）。
  if ! ssh_o "cd /opt/embytg/pipeline && EMBYTG_WORKER=slot$SLOT EMBYTG_RUN_SLOT=$SLOT python3 gha_next3.py --lane '$L' --run-id '$RUN_KEY' --max-attempts $MAX_ATTEMPTS --max-active $MAX_ACTIVE --lock-wait 900 $SOURCE_ARG" >"$JOB" 2>"$WORK/next.err"; then
    log "CLAIM_TRANSPORT_FAILED lane=$L"
    sleep 2
    if ! ssh_o "pgrep -af 'gha_next3.py.*$RUN_KEY' >/dev/null 2>&1"; then
      cancel_claim claim_transport_failed
    else
      log "CLAIM_TRANSPORT_STILL_RUNNING run=$RUN_KEY"
    fi
    ERRORS=1
    continue
  fi
  JOB_JSON="$(json_last "$JOB" 2>/dev/null || echo '{}')"
  OK=$($VENV -c 'import json,sys; print(1 if json.loads(sys.argv[1]).get("ok") else 0)' "$JOB_JSON" 2>/dev/null || echo 0)
  if [ "$OK" = "1" ]; then
    # 2026-11-15：**job.json 必须只留一条对象。**
    # gha_next3 在扫描闸门重试时先 emit 一条 {"scan_gate_retry": ...} 再 emit 真正的 job，
    # 直接重定向到 job.json 就是两行 JSON —— 下游 gha_download.py 用 json.load()
    # 读它会抛 `Extra data: line 2 column 1`，表现为「刚 CLAIMED 就 DOWNLOAD failed rc=1」，
    # 而 claim 随即被取消，白跑一轮（实测 slot2 龙珠超 E3）。
    printf '%s\n' "$JOB_JSON" >"$JOB"
    CLAIM_ACTIVE=1
    SELECTED=1
    ACTIVE_LANE="$L"
    SUMMARY=$($VENV -c 'import json,sys; d=json.loads(sys.argv[1])["job"]; print("%s S%02dE%02d %s %s" % (d.get("series"), int(d.get("season",0)), int(d.get("ep",0)), d.get("quality"), d.get("lane")))' "$JOB_JSON" 2>/dev/null || echo claimed)
    log "CLAIMED $SUMMARY"
    break
  fi
  REASON=$($VENV -c 'import json,sys; d=json.loads(sys.argv[1]); print(d.get("reason") or d.get("error") or "no_job")' "$JOB_JSON" 2>/dev/null || echo bad_json)
  if [ "$L" != "upgrade2" ]; then
    policy note --lane "$(policy_lane "$L")" --result "$REASON"
  fi
  case "$REASON" in
    lane_done)
      log "LANE_DONE lane=$L reason=$REASON"
      if [ "$L" != "upgrade2" ]; then
        policy mark-done --lane "$(policy_lane "$L")" --reason "$REASON"
      fi
      ;;
    empty_queue)
      case "$L" in
        new|new2|new3|new4|new5)
          log "NEW_QUEUE_EMPTY"
          ;;
        *)
          mark_lane_done "$L" "$REASON"
          log "LANE_DONE lane=$L reason=$REASON"
          ;;
      esac
      ;;
    no_queue)
      log "LANE_NO_QUEUE lane=$L"
      ERRORS=1
      ;;
    paused)
      log "GLOBAL_STOP lane=$L reason=$REASON"
      PAUSED=1
      break
      ;;
    claimed)
      log "LANE_BUSY lane=$L"
      ;;
    busy|autoingest_busy)
      log "GLOBAL_STOP lane=$L reason=$REASON"
      ERRORS=1
      break
      ;;
    max_active)
      log "CAPACITY_FULL lane=$L active=$($VENV -c 'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8")).get("active","?"))' "$JOB" 2>/dev/null || echo ?)"
      CAPACITY=1
      break
      ;;
    no_job)
      log "NOJOB lane=$L reason=$REASON"
      ;;
    *)
      log "NOJOB lane=$L reason=$REASON"
      ERRORS=1
      ;;
  esac
done

if [ "$MODE" = "auto" ]; then
  policy advance
fi

if [ "$SELECTED" != "1" ]; then
  # 预算闸门要求等待：直接按闸门给的秒数睡，别落进「no_job 睡 180/900」那套
  # —— 那会把节流周期和预算节奏错开，等回来时窗口已经又被打满。
  if [ "$BUDGET_PAUSE" = "1" ]; then
    log "IDLE_BUDGET_PAUSE wait=${BUDGET_WAIT}s"
    sleep "$BUDGET_WAIT"
    exit 0
  fi
  if [ "$PAUSED" = "1" ]; then
    log "IDLE_PAUSED"
    exit 0
  fi
  if [ "$MODE" = "new" ] || [ "$MODE" = "new2" ] || [ "$MODE" = "new3" ] || [ "$MODE" = "new4" ] || [ "$MODE" = "new5" ] || [ "$MODE" = "fill" ]; then
    log "IDLE_NO_JOB"
    # 2026-11-14：按**原因**决定睡多久，不要一律 600 秒。
    # 实测：配额节流（source_budget）时 lane 拿 NOJOB，然后睡 600 秒 ——
    # 10 分钟后才重试，直接错过两个节流槽位（槽宽 5 分钟），
    # 节流器「按槽放行」的设计被这个 sleep 完全架空。
    # source_budget 是可恢复的短暂状态，60 秒后重试即可；
    # 真正的"没活可干"（no_job / 队列空）才值得睡久一点。
    if [ "$CAPACITY" = "1" ]; then
      sleep 20
    elif [ "$REASON" = "source_budget" ]; then
      # 2026-11-14：**必须加抖动**。
      # 原来固定 sleep 60，而节流器的槽位周期也是整数分钟 ——
      # 固定重试周期会与槽位**阶段锁定**：要么每次都撞开放，
      # 要么每次都撞关闭（实测连续 40 分钟 0 集就是这么来的）。
      # 60~119 秒随机，几轮内就会错开。
      sleep $((60 + RANDOM % 60))
    else
      # 2026-11-14：原来这里写的是
      #   elif [ "$MODE" = "new" ] || [ "new3" ] || [ "new4" ] || [ "new5" ]; then sleep 600
      #   else sleep 900
      # **漏了 new2** —— 而 new2 正是 Oracle 本机这条 lane，于是它每次都掉进
      # else 空等 900 秒（15 分钟）。这是"列表式匹配漏一项"的经典写法，
      # 每加一条 lane 就得记得改这里。
      # 改成 case 前缀匹配，new/new2/new3/new4/new5 一次覆盖；
      # 且 no_job 多半是临时状态（series_active / cooldown），180 秒就够。
      case "$MODE" in
        new|new2|new3|new4|new5) sleep 180 ;;
        *) sleep 900 ;;
      esac
    fi
    exit 0
  fi
  if [ "$MODE" = "upgrade2" ]; then
    log "IDLE_NO_JOB"
    [ "$CAPACITY" = "1" ] && sleep 20 || sleep 60
    exit 0
  fi
  if [ "$ERRORS" = "1" ]; then
    log "IDLE_WITH_ERRORS"
    exit 1
  fi
  log "IDLE_NO_JOB"
  exit 0
fi

JOB_BYTES=$($VENV -c 'import json,sys; d=json.load(open(sys.argv[1],encoding="utf-8")); print(int((d.get("job") or d).get("size") or 0))' "$JOB" 2>/dev/null || echo 0)
FREE_KB=$(df -Pk "$BASE" | awk 'NR==2 {print $4}')
FREE_KB="${FREE_KB:-0}"
NEED_KB=$(( JOB_BYTES / 1024 + 8 * 1024 * 1024 ))
if [ "$FREE_KB" -lt "$NEED_KB" ]; then
  log "DISK_GUARD free=$((FREE_KB / 1024 / 1024))GiB need=$((NEED_KB / 1024 / 1024))GiB"
  cancel_claim disk_guard
  CLAIM_ACTIVE=0
  exit 1
fi

rm -rf -- "$MEDIA_DIR"
mkdir -p "$MEDIA_DIR"
RESUME_ARGS="--resume"
UPLOAD_LEDGER="$BASE/logs/uploaded_names.txt"
MEDIA_NAME=$($VENV -c 'import json,sys; d=json.load(open(sys.argv[1],encoding="utf-8")); print((d.get("job") if d.get("ok") else d).get("name") or "")' "$JOB" 2>/dev/null || echo "")
if [ -n "$MEDIA_NAME" ] && [ -f "$KEEP_DIR/$MEDIA_NAME" ]; then
  # E157：上传失败留下的源文件按 name+size 命中就硬链接回来，
  # gha_download.py 的「本地完整文件已存在」分支会直接跳过下载（省源站配额）。
  EXPECT_SIZE=$($VENV -c 'import json,sys; d=json.load(open(sys.argv[1],encoding="utf-8")); j=d.get("job") if d.get("ok") else d; print(int(j.get("size") or 0))' "$JOB" 2>/dev/null || echo 0)
  HAVE_SIZE=$(stat -c %s "$KEEP_DIR/$MEDIA_NAME" 2>/dev/null || echo 0)
  if [ "${EXPECT_SIZE:-0}" != "0" ] && [ "$HAVE_SIZE" = "$EXPECT_SIZE" ]; then
    ln -f "$KEEP_DIR/$MEDIA_NAME" "$MEDIA_DIR/$MEDIA_NAME" 2>/dev/null || cp -f "$KEEP_DIR/$MEDIA_NAME" "$MEDIA_DIR/$MEDIA_NAME"
    log "KEEP_HIT name=$MEDIA_NAME bytes=$HAVE_SIZE (skip source download)"
  else
    log "KEEP_SIZE_MISMATCH name=$MEDIA_NAME keep=$HAVE_SIZE expect=${EXPECT_SIZE:-0}"
  fi
fi
if [ "${TG_RESUME_SCAN_MODE:-strict}" = "skip" ]; then
  # Bot 会话没有 GetHistory 权限，无法做断点扫描。只允许“本机从未上传过
  # 这个名字”的新任务跳过扫描；台账命中说明可能有半成品分片，必须交给
  # 有用户会话的数据面（PC/韩国）处理，避免重复占 TG 配额。
  if [ -n "$MEDIA_NAME" ] && grep -Fxq -- "$MEDIA_NAME" "$UPLOAD_LEDGER" 2>/dev/null; then
    log "RESUME_SCAN_REQUIRED name=$MEDIA_NAME (ledger hit; needs user session)"
    cancel_claim resume_scan_needs_user_session
    CLAIM_ACTIVE=0
    exit 1
  fi
  RESUME_ARGS="--resume --skip-resume-scan"
  log "RESUME_SCAN_SKIP name=$MEDIA_NAME (bot session, fresh name)"
fi
timeout "$TG_DOWNLOAD_TIMEOUT" "$VENV" "$BASE/gha_download.py" --job "$JOB" --out-dir "$MEDIA_DIR" $RESUME_ARGS --chunk-mib 128 >"$WORK/download.log" 2>&1
DOWNLOAD_RC=$?
if [ "$DOWNLOAD_RC" -ne 0 ]; then
  if [ "$DOWNLOAD_RC" -eq 3 ] || [ "$DOWNLOAD_RC" -eq 124 ]; then
    log "DOWNLOAD too slow; see $WORK/download.log"
    cancel_claim source_too_slow --source-failed
  elif [ "$DOWNLOAD_RC" -eq 4 ]; then
    log "DOWNLOAD resume scan failed; see $WORK/download.log"
    cancel_claim resume_scan_failed
  elif [ "$DOWNLOAD_RC" -eq 5 ]; then
    log "DOWNLOAD rejected by real-quality probe; see $WORK/download.log"
    cancel_claim quality_rejected
  else
    log "DOWNLOAD failed rc=$DOWNLOAD_RC; see $WORK/download.log"
    cancel_claim source_download_failed --source-failed
  fi
  CLAIM_ACTIVE=0
  exit 1
fi

UPLOAD_OK=0
: >"$WORK/upload.log"
UP_BYTES=0
if [ -n "${MEDIA_NAME:-}" ] && [ -f "$MEDIA_DIR/$MEDIA_NAME" ]; then
  UP_BYTES=$(stat -c %s "$MEDIA_DIR/$MEDIA_NAME" 2>/dev/null || echo 0)
fi
for attempt in 1 2; do
  printf '\n=== upload attempt %s ===\n' "$attempt" >>"$WORK/upload.log"
  timeout "$UPLOAD_OUTER_TIMEOUT" "$VENV" "$BASE/gha_upload.py" --job "$JOB" --media-dir "$MEDIA_DIR" --manifest-out "$WORK/manifest.path" >>"$WORK/upload.log" 2>&1
  UPLOAD_RC=$?
  if [ "$UPLOAD_RC" -eq 0 ]; then
    UPLOAD_OK=1
    break
  fi
  log "UPLOAD attempt $attempt failed rc=$UPLOAD_RC; resuming from manifest"
  # E164：两种情况重试第二次没有意义，直接走失败收尾（claim 释放、文件留 _keep）：
  #   rc=6 零进展：一个分片都没落地（实测两次 attempt 结果完全相同，白占 30 分钟）
  #   rc=3 QuotaRest：TG 要求按秒休息，立刻重试只会再撞一次额度
  # 关掉本策略：TG_UPLOAD_SKIP_RETRY_ZERO_PARTS=0
  if [ "${TG_UPLOAD_SKIP_RETRY_ZERO_PARTS:-1}" = "1" ] && { [ "$UPLOAD_RC" -eq 6 ] || [ "$UPLOAD_RC" -eq 3 ]; }; then
    log "UPLOAD_SKIP_RETRY rc=$UPLOAD_RC（零进展/额度休息）；不再重试，文件留给下一次 claim 复用"
    break
  fi
  [ "$attempt" -eq 1 ] && sleep 15
done
# E182：把上传结果记进台账（断流闸门的唯一输入）。成功/失败都要记，
# 否则闸门只看得到失败、看不到恢复，会一直按着不放。
if [ "$UPLOAD_OK" != "1" ]; then
  # 2026-11-23：失败必须留一条**带原因**的日志行。原来只有 "attempt N failed rc=X"，
  # 事后查日志得翻 upload.log 才知道是超时、额度还是分片全丢。日检只看这一行。
  _last_err="$(grep -iE "error|failed|timeout|FloodWait|limit" "$WORK/upload.log" 2>/dev/null | tail -1 | cut -c1-140)"
  log "UPLOAD_FAILED run=$RUN_KEY rc=${UPLOAD_RC:-?} reason=${_last_err:-unknown}"
fi
if [ "$UPLOAD_OK" = "1" ]; then
  "$SYSPY" "$PIPELINE_DIR/upload_health.py" --record ok --rc 0 --bytes "$UP_BYTES" \
    --claim "$RUN_KEY" --lane "${ACTIVE_LANE:-$MODE}" --err="" \
    >>"$BASE/logs/upload_health.err" 2>&1 || true
fi
if [ "$UPLOAD_OK" != "1" ]; then
  log "UPLOAD failed after retry; see $WORK/upload.log"
  UP_ERR=$(grep -a -E "ZERO_PROGRESS|worker 超时|FloodWait|RPC 超时|QuotaRest|no manifest" "$WORK/upload.log" \
            | tail -1 | tr -d '\r' | cut -c1-180)
  "$SYSPY" "$PIPELINE_DIR/upload_health.py" --record fail --rc "$UPLOAD_RC" --bytes "$UP_BYTES" \
    --claim "$RUN_KEY" --lane "${ACTIVE_LANE:-$MODE}" --err="$UP_ERR" \
    >>"$BASE/logs/upload_health.err" 2>&1 || true
  # E157：源文件不要跟着 media.slotN 一起被 rm 掉。留到 _keep，下一单硬链接复用；
  # 同一份字节第二次仍失败 → 判为坏件，删掉不再复用（否则会无限重传同一坏文件）。
  if [ -n "$MEDIA_NAME" ] && [ -f "$MEDIA_DIR/$MEDIA_NAME" ]; then
    mkdir -p "$KEEP_DIR"
    if [ -f "$KEEP_DIR/$MEDIA_NAME" ]; then
      rm -f "$KEEP_DIR/$MEDIA_NAME"
      log "KEEP_DROP name=$MEDIA_NAME (same bytes failed twice)"
    else
      mv -f "$MEDIA_DIR/$MEDIA_NAME" "$KEEP_DIR/$MEDIA_NAME" 2>/dev/null \
        && log "KEEP_SAVE name=$MEDIA_NAME bytes=$(stat -c %s "$KEEP_DIR/$MEDIA_NAME" 2>/dev/null || echo 0)"
    fi
  fi
  cancel_claim upload_failed
  CLAIM_ACTIVE=0
  exit 1
fi
MANIFEST=$(cat "$WORK/manifest.path" 2>/dev/null || true)
if [ -z "$MANIFEST" ] || [ ! -f "$MANIFEST" ]; then
  log "UPLOAD produced no manifest"
  cancel_claim upload_manifest_missing
  CLAIM_ACTIVE=0
  exit 1
fi
if [ -n "$MEDIA_NAME" ]; then
  mkdir -p "$BASE/logs"
  printf '%s\t%s\n' "$(date -Is)" "$MEDIA_NAME" >>"$UPLOAD_LEDGER"
fi
ssh_o 'mkdir -p /opt/embytg/pipeline/_gha_manifests && chmod 700 /opt/embytg/pipeline/_gha_manifests' >/dev/null 2>&1 || true
if ! scp -i "$ORACLE_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -P "$ORACLE_PORT" "$MANIFEST" "$ORACLE_USER@$ORACLE_HOST:/opt/embytg/pipeline/_gha_manifests/$RUN_KEY.json" >"$WORK/scp.log" 2>&1; then
  log "SCP failed; see $WORK/scp.log"
  cancel_claim manifest_transfer_failed
  CLAIM_ACTIVE=0
  exit 1
fi

: >"$WORK/finish.log"
FINISH_OK=0
for attempt in 1 2; do
  printf '\n=== finish attempt %s ===\n' "$attempt" >>"$WORK/finish.log"
  if ssh_o "cd /opt/embytg/pipeline && timeout 3600 python3 gha_finish.py --run-id '$RUN_KEY' --manifest /opt/embytg/pipeline/_gha_manifests/$RUN_KEY.json" >>"$WORK/finish.log" 2>&1; then
    FINISH_OK=1
    break
  fi
  log "FINISH attempt $attempt failed; see $WORK/finish.log"
  [ "$attempt" -eq 1 ] && sleep 30
done
if [ "$FINISH_OK" != "1" ]; then
  # 登记与 NFO 已完成，只差 Emby 扫描把新文件枚举出来时，不能一直占着
  # claim（全库扫描可能几十分钟到几小时，会把整条 lane 卡死）。释放
  # claim 继续上片，由后置扫描补可见性；台账里已有 key，不会重复领取。
  if [ "${TG_FINISH_EMBY_PENDING_OK:-0}" = "1" ] \
     && grep -q '"tgdb": "ok"' "$WORK/finish.log" 2>/dev/null \
     && grep -q 'emby_missing' "$WORK/finish.log" 2>/dev/null; then
    log "FINISH_PARTIAL_EMBY_PENDING $RUN_KEY (registered+NFO ok; scan will catch up)"
    CLAIM_ACTIVE=0
    rm -rf -- "$MEDIA_DIR"
    exit 0
  fi
  log "FINISH failed; claim kept for safe recovery"
  KEEP_CLAIM=1
  exit 1
fi

CLAIM_ACTIVE=0
if [ "$ACTIVE_LANE" != "upgrade2" ]; then
  policy note --lane "$(policy_lane "$ACTIVE_LANE")" --result done
  case "$ACTIVE_LANE" in
    new|new2|new3|new4|new5) ;;
    *)
      if ssh_o "test -f /opt/embytg/pipeline/.gha_lane_done_$(policy_lane "$ACTIVE_LANE")" >/dev/null 2>&1; then
        policy mark-done --lane "$ACTIVE_LANE" --reason queue_empty_after_finish
        log "LANE_DONE lane=$ACTIVE_LANE reason=queue_empty_after_finish"
      fi
      ;;
  esac
fi
log "DONE $(tail -c 400 "$WORK/finish.log" | tr '\n' ' ')"
rm -rf -- "$MEDIA_DIR"
exit 0
