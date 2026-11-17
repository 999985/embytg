#!/bin/bash
# 2026-11-15：**实例数必须由账号数推导，不能由人写 unit。**
# 现状：4 个 TG 账号全部 is_user_authorized=True，本机却只跑 slot1/slot4
# （另两个是给已下线的云盒写的一次性 unit），于是 2 个账号长期空转，
# 产能只有基线 23%。用户的原话是「加一个账号就该自动生效」——
# 那这里的判据就是：runner.env 里非空 TG_SESSION_STRING_N 有几个，
# 就跑几个实例；slot1/slot4 已由专属 unit 覆盖，其余用模板实例补齐。
set -u
ENV=/opt/embytg-runner/runner.env
LOG=/opt/embytg-runner/logs/ensure_lanes.log
mkdir -p "$(dirname "$LOG")"

N=0
for k in $(sed -n 's/^\(TG_SESSION_STRING[_0-9]*\)=.*/\1/p' "$ENV" 2>/dev/null); do
  v=$(sed -n "s/^$k=//p" "$ENV" | head -1 | tr -d '"'"'"' ')
  [ -n "$v" ] || continue
  case "$k" in
    TG_SESSION_STRING) n=1 ;;
    *) n=${k##*_} ;;
  esac
  case "$n" in ''|*[!0-9]*) continue ;; esac
  # slot1 / slot4 有专属 unit（带固定 lane），不再起模板实例
  case "$n" in 1|4) continue ;; esac
  systemctl enable --now "embytg-runner@$n.service" >/dev/null 2>&1
  echo "$(date '+%F %T') ensured slot$n $(systemctl is-active embytg-runner@$n.service)" >> "$LOG"
  N=$((N+1))
done
echo "$(date '+%F %T') templated instances=$N" >> "$LOG"

# E157：上传失败留下的 _keep 源文件缓存必须有界（TTL 24h / 上限 16GiB 且不超过「余量−20GB」）
/usr/bin/python3 /opt/embytg-runner/keep_cleanup.py >> "$LOG" 2>&1 || true
