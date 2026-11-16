#!/bin/bash
# P0-3 / E011 状态看板: shim 在位 / 缓存规模 / 超时增量 / 进程 / Emby 健康
C=emby
PIPE=/opt/embytg/pipeline
LOG=/var/lib/docker/volumes/embytg_embydata/_data/logs/embyserver.txt
MI=/var/lib/docker/volumes/embytg_embydata/_data/mediainfo
echo "=== P0-3 状态 $(date '+%F %T') ==="
echo "shim 头行 : $(docker exec $C sh -c 'head -1 /bin/ffprobe' 2>/dev/null)"
echo "真身备份 : $([ -s $MI/ffprobe.real.bak ] && echo OK || echo MISSING)"
echo "缓存文件 : $(ls $MI/cache/*.json 2>/dev/null | wc -l) 个 (1 个 = 1 次成功探测; 目标约 18515)"
echo "缓存体积 : $(du -sh $MI/cache 2>/dev/null | awk '{print $1}')"
echo "ffprobe 进程: $(ps -eo args | grep -c '[f]fprobe -i')"
echo "超时累计行 : $(grep -c 'Timeout while running Ffprobe' $LOG || true)"
echo "Emby API  : $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8096/System/Info/Public)"
TOK=$(cd $PIPE && python3 -c 'import embytg_conf as c;print(c.cfg("emby_token",""))')
curl -s http://127.0.0.1:8096/ScheduledTasks/REPLACE_ME -H "X-Emby-Token: $TOK" | python3 -c 'import json,sys;d=json.load(sys.stdin);print("Scan media library:",d["State"],d.get("CurrentProgressPercentage"),"triggers=",d.get("Triggers"))'
echo "回滚      : bash $PIPE/p0_3_rollback.sh"
