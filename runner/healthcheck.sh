#!/bin/bash
set -u
BASE=/opt/embytg-runner
ENV="$BASE/runner.env"
LOG="$BASE/logs/health.jsonl"
ALERT="$BASE/logs/ALERT"
mkdir -p "$BASE/logs"
set -a
. "$ENV"
set +a

ssh_o() {
  ssh -i "$ORACLE_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 \
      -p "$ORACLE_PORT" "$ORACLE_USER@$ORACLE_HOST" "$@"
}

now="$(date +%s)"
issues=()
notes=()
timer1="$(systemctl is-active embytg-runner@1.timer 2>/dev/null || true)"
timer2="$(systemctl is-active embytg-runner@2.timer 2>/dev/null || true)"
timer3="$(systemctl is-active embytg-runner@3.timer 2>/dev/null || true)"
service1="$(systemctl is-active embytg-runner@1.service 2>/dev/null || true)"
service2="$(systemctl is-active embytg-runner@2.service 2>/dev/null || true)"
service3="$(systemctl is-active embytg-runner@3.service 2>/dev/null || true)"
[ "$timer1" = "active" ] || issues+=("timer1=$timer1")
[ "$timer2" = "active" ] || issues+=("timer2=$timer2")
[ "$timer3" = "active" ] || issues+=("timer3=$timer3")

avail_kb="$(df -Pk / | awk 'NR==2 {print $4}')"
if [ -n "$avail_kb" ] && [ "$avail_kb" -lt $((8 * 1024 * 1024)) ]; then
  issues+=("disk_free=$((avail_kb / 1024 / 1024))GiB")
fi

procs="$(pgrep -af 'run_one.sh|gha_next3.py|gha_download.py|gha_upload.py|pc_upload.py|pc_worker.py|gha_finish.py' 2>/dev/null || true)"
any_active=0
for s in "$service1" "$service2" "$service3"; do
  if [ "$s" = "activating" ] || [ "$s" = "active" ]; then
    any_active=1
  fi
done
if [ "$any_active" = "1" ] && [ -z "$procs" ]; then
  notes+=("service_active_without_child")
fi

paused=no
paused_mtime="$(
  ssh_o "stat -c %Y /opt/embytg/pipeline/.gha_paused 2>/dev/null || true" \
    2>/dev/null | tail -n 1 | tr -dc '0-9'
)"
if [ -n "$paused_mtime" ]; then
  paused=yes
  paused_age=$((now - paused_mtime))
  # 维护暂停不超过 6 小时是正常状态；超过才说明闸门可能忘记撤。
  if [ "$paused_age" -gt 21600 ]; then
    issues+=("oracle_paused_age=$((paused_age / 60))m")
  else
    notes+=("paused_maintenance")
  fi
fi

claim_count="$(
  ssh_o "ls /opt/embytg/pipeline/.gha_claims/*.json 2>/dev/null | wc -l" \
    2>/dev/null | tail -n 1 | tr -dc '0-9'
)"
claim_count="${claim_count:-0}"
if [ "$claim_count" -gt 3 ]; then
  issues+=("claim_count=$claim_count")
fi
claim_mtime="$(
  ssh_o "stat -c %Y /opt/embytg/pipeline/.gha_claims/*.json 2>/dev/null | sort -nr | head -1" \
    2>/dev/null | tail -n 1 | tr -dc '0-9'
)"
claim_age=""
if [ -n "$claim_mtime" ]; then
  claim_age=$((now - claim_mtime))
  # Must match gha_state.py CLAIM_TTL (24h); a long upload is not an orphan.
  if [ "$claim_age" -gt 86400 ]; then
    issues+=("claim_age=$((claim_age / 60))m")
  elif [ "$any_active" = "0" ]; then
    issues+=("orphan_claim_age=$((claim_age / 60))m")
  fi
else
  if [ "$paused" = "yes" ]; then
    notes+=("paused_no_claim")
  elif [ -n "$procs" ]; then
    notes+=("selecting_no_claim")
  elif [ "$service1" = "inactive" ] && [ "$service2" = "inactive" ]; then
    notes+=("idle_no_claim")
  else
    notes+=("no_claim")
  fi
fi

sourceing="$(ssh_o "systemctl is-active embytg-sourceing.service 2>/dev/null || true" 2>/dev/null | tail -n 1)"
[ "$sourceing" = "inactive" ] || issues+=("oracle_sourceing=$sourceing")

slow_recent="$(find "$BASE/work" -type f -name download.log -mmin -30 -print0 2>/dev/null | xargs -0 -r grep -l 'SOURCE_TOO_SLOW' 2>/dev/null | wc -l)"
slow_recent="${slow_recent:-0}"
if [ "$claim_count" -eq 0 ] && [ -z "$procs" ] && [ "$slow_recent" -ge 3 ]; then
  issues+=("source_too_slow_recent=$slow_recent")
fi

partials_bytes="$(find "$BASE" -maxdepth 1 -type d -name '_partials*' -print0 2>/dev/null | xargs -0 -r du -sk 2>/dev/null | awk '{s+=$1} END{print s+0}')"
partials_bytes="${partials_bytes:-0}"
if [ "$partials_bytes" -gt $((20 * 1024 * 1024)) ]; then
  issues+=("partials=$((partials_bytes / 1024 / 1024))GiB")
fi

status=ok
if [ "${#notes[@]}" -gt 0 ]; then
  status=warn
  bad_notes=0
  for note in "${notes[@]}"; do
    case "$note" in
      idle_no_claim|paused_maintenance|paused_no_claim|selecting_no_claim) ;;
      *) bad_notes=1 ;;
    esac
  done
  # 正常空闲/维护暂停不是故障。
  [ "$bad_notes" -eq 0 ] && status=ok
fi
[ "${#issues[@]}" -gt 0 ] && status=fail
line="$(printf '{"ts":"%s","status":"%s","timer_1":"%s","timer_2":"%s","timer_3":"%s","service_1":"%s","service_2":"%s","service_3":"%s","claim_count":%s,"claim_age_sec":%s,"child":%s,"disk_free_kb":%s,"partials_kb":%s,"issues":"%s","notes":"%s"}' \
  "$(date '+%F %T %z')" "$status" "$timer1" "$timer2" "$timer3" "$service1" "$service2" "$service3" \
  "$claim_count" "${claim_age:-null}" \
  "$([ -n "$procs" ] && echo true || echo false)" "${avail_kb:-0}" "$partials_bytes" \
  "$(IFS='|'; echo "${issues[*]:-}")" "$(IFS='|'; echo "${notes[*]:-}")")"
printf '%s\n' "$line" >>"$LOG"
if [ "$status" = "fail" ]; then
  printf '%s %s\n' "$(date '+%F %T')" "$line" >"$ALERT"
  logger -t embytg-runner-health "$line"
  exit 1
fi
rm -f "$ALERT"
exit 0
