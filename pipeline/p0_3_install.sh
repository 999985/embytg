#!/bin/bash
# P0-3 / E011 安装: 仅替换 /bin/ffprobe, 不动 ScheduledTask Triggers / 不改库配置
# 理由: E008 notify_pending.jsonl 无自动消费方, 12h 扫描是失败通知的最后兜底;
#       shim 后全库重扫已从真探变为命中缓存, 正是低并发养缓存过程, 无需关。
set -eu
PIPE=/opt/embytg/pipeline
C=emby
HOST_BAK=/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/ffprobe.real.bak
SHIM="$PIPE/p0_3_ffprobe_shim.sh"
STAMP=$(date +%Y%m%d_%H%M%S)
BK=/opt/embytg/backup

[ -s "$SHIM" ] || { echo "FATAL shim 缺失"; exit 1; }
[ -s "$HOST_BAK" ] || { echo "FATAL 持久卷真身缺失 $HOST_BAK"; exit 1; }
mkdir -p "$BK"

echo "[install] 1 真身落到 /bin/ffprobe.real (同目录, 动态库路径不变; 持久卷黄金副本不动)"
docker cp "$HOST_BAK" "$C":/bin/ffprobe.real
docker exec "$C" chmod 755 /bin/ffprobe.real
docker exec "$C" /bin/ffprobe.real -version 2>&1 | head -1

echo "[install] 2 改前留档 $BK/p03_before_$STAMP.txt"
{ docker exec "$C" sh -c 'ls -la /bin/ffprobe; sha256sum /bin/ffprobe'; } > "$BK/p03_before_$STAMP.txt" 2>&1
cat "$BK/p03_before_$STAMP.txt"

echo "[install] 3 原子替换 (先 .new 再 mv, 避开运行中二进制 ETXTBSY)"
docker cp "$SHIM" "$C":/bin/ffprobe.new
docker exec "$C" sh -c 'chmod 755 /bin/ffprobe.new && mv -f /bin/ffprobe.new /bin/ffprobe'

echo "[install] 4 校验 shim 在位, 否则自动回滚"
if ! docker exec "$C" sh -c 'head -1 /bin/ffprobe | grep -q "^#!/bin/sh"'; then
  echo "FATAL shim 未生效, 自动回滚"
  docker cp "$HOST_BAK" "$C":/bin/ffprobe
  docker exec "$C" chmod 755 /bin/ffprobe
  exit 1
fi
docker exec "$C" sh -c 'ls -la /bin/ffprobe; head -1 /bin/ffprobe'
echo "[install] OK — 未改 ScheduledTask, 未 stop 当前扫描"
echo "[install] 回滚: bash $PIPE/p0_3_rollback.sh"
