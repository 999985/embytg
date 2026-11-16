#!/bin/bash
# P0-3 / E011 一键回滚: /bin/ffprobe 恢复为真身 ELF
set -eu
C=emby
HOST_BAK=/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/ffprobe.real.bak
[ -s "$HOST_BAK" ] || { echo "FATAL 真身备份缺失"; exit 1; }
docker cp "$HOST_BAK" "$C":/bin/ffprobe
docker exec "$C" chmod 755 /bin/ffprobe
docker exec "$C" sh -c 'head -c 4 /bin/ffprobe | grep -aq ELF' || { echo "FATAL 回滚后不是 ELF"; exit 1; }
docker exec "$C" /bin/ffprobe -version 2>&1 | head -1
echo "[rollback] OK: /bin/ffprobe 已恢复真身 ELF"
echo "[rollback] 缓存 /config/mediainfo/cache 保留(纯旁路产物, 不影响已入库数据)"
echo "[rollback] 从未改 ScheduledTask Triggers, 无需恢复"
