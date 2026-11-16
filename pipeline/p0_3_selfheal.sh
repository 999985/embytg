#!/bin/sh
# p0_3_selfheal.sh — 容器重建 / 镜像升级后自动恢复 ffprobe shim（幂等、零数据风险）
# 背景(E011 S4)：image 是 emby/embyserver:latest，docker compose pull/up 会把
# /bin/ffprobe 覆盖回 ELF 真身，shim 静默失效（只是变慢、不损数据）。
# 本脚本由 root crontab 每 10 分钟调一次，检测到非 shim 就重跑幂等 install.sh。
C=emby
LOG=/opt/embytg/backup/p03_selfheal.log
line=$(docker exec "$C" head -1 /bin/ffprobe 2>/dev/null || true)
case "$line" in
  '#!/bin/sh'*) exit 0 ;;
esac
echo "$(date '+%F %T') shim missing (head='$line'), re-running p0_3_install.sh" >> "$LOG"
/opt/embytg/pipeline/p0_3_install.sh >> "$LOG" 2>&1
rc=$?
echo "$(date '+%F %T') selfheal rc=$rc" >> "$LOG"
exit "$rc"
