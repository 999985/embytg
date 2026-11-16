#!/bin/sh
# oncall_check.sh — 上电自检（AGENTS.md §3 的可执行版，只读，不改任何东西）
#
# 存在理由：AGENTS.md §3 列了 7 条命令散在文档里，agent 常跳过或凭印象说"正常"。
# 固化成一条命令，输出可直接贴进 OPS_JOURNAL.md。
#
# 用法: bash pipeline/oncall_check.sh
set -u
cd /opt/embytg || exit 1
T=$(/usr/bin/python3 -c "import json;print(json.load(open('config.json'))['emby_token'])")

echo "=== EmbyTG 上电自检 | $(date '+%F %T %Z') ==="
echo "[挂载] (唯一可信判据是 mountpoint，不是 systemctl is-active — E013)"
printf '  embytg-mount.service : '; systemctl is-active embytg-mount.service 2>&1
printf '  mountpoint           : '; mountpoint -q mnt/embytg && echo OK || echo DEAD
printf '  rclone 参数          : '; ps -eo args --no-headers | grep '[r]clone mount embytg' | grep -v '/bin/sh -c' | head -1 | grep -oE '\-\-vfs-cache-mode [^ ]+' || echo '未找到'
printf '  vfs 待传文件数       : '; find cache/vfs -type f 2>/dev/null | wc -l
printf '  注：待传非 0 时禁止重启 mount (E013)\n'

echo "[容器]"
docker ps --format '  {{.Names}}\t{{.Status}}' 2>&1 | head -10

echo "[服务]"
printf '  Emby /System/Info    : '; curl -s -o /dev/null -w '%{http_code}\n' --max-time 5 http://127.0.0.1:8096/System/Info -H "X-Emby-Token: $T" 2>&1
printf '  teldrive /health     : '; curl -s -o /dev/null -w '%{http_code}\n' --max-time 5 http://127.0.0.1:8080/health 2>&1
printf '  Emby 全库(不带UserId): '; for TY in Movie Series Episode; do
  printf '%s=' "$TY"
  curl -s --max-time 15 "http://127.0.0.1:8096/Items?Recursive=true&IncludeItemTypes=$TY&Limit=1" -H "X-Emby-Token: $T" \
    | /usr/bin/python3 -c 'import sys,json;print(json.load(sys.stdin).get("TotalRecordCount","ERR"),end=" ")' 2>/dev/null
done; echo
printf '  Scan media library   : '; curl -s --max-time 10 "http://127.0.0.1:8096/ScheduledTasks" -H "X-Emby-Token: $T" \
  | /usr/bin/python3 -c 'import sys,json;print([(t.get("State"),round(t.get("CurrentProgressPercentage") or 0,1)) for t in json.load(sys.stdin) if t.get("Name")=="Scan media library"][0])' 2>/dev/null
printf '  若长期 Running 不推进 → 查 library.db NamedValues 行5 是否 Cancelled（START_HERE.md §4）\n'

echo "[磁盘 / 状态库]"
df -h / | tail -1 | awk '{print "  磁盘: 用 "$3" 余 "$4" ("$5")"}'
printf '  tg.db integrity      : '; /usr/bin/python3 -c "import sqlite3;print(sqlite3.connect('file:/opt/embytg/pipeline/tg.db?mode=ro',uri=True).execute('pragma integrity_check').fetchone()[0])" 2>&1
printf '  tg.db 状态表         : '; /usr/bin/python3 -c "
import sqlite3;c=sqlite3.connect('file:/opt/embytg/pipeline/tg.db?mode=ro',uri=True)
print('state_key=%s pending=%s cursor=%s todb_video=%s'%(c.execute('select count(*) from state_key').fetchone()[0],c.execute('select count(*) from pending').fetchone()[0],c.execute(\"select v from meta where k='cursor'\").fetchone()[0],c.execute('select count(*) from todb_video').fetchone()[0]))" 2>&1
printf '  每日同步水位         : '; cd pipeline && /usr/bin/python3 tgdb_daily_sync.py --status 2>/dev/null | grep last_at | sed 's/^ *//'

echo "[常驻]"
printf '  auto_finalize --loop : '; ps -eo pid,args --no-headers | grep '[a]uto_finalize.py --loop' | grep -v '/bin/sh -c' | head -1 || echo '未运行'
printf '  cron 04:00 同步      : '; grep -hE '^[0-9]' /etc/cron.d/embytg_tgdb_daily 2>/dev/null | cut -c1-12 || echo '缺失'

echo "=== 结束。判收敛请另跑: /usr/bin/python3 pipeline/lib_reconcile.py --json ==="
