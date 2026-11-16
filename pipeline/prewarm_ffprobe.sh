#!/bin/bash
# prewarm_ffprobe.sh —— 预热 E011 ffprobe shim 缓存，让 Emby 后续扫描变成缓存命中（~75ms/文件）
# 用法: ./prewarm_ffprobe.sh <目录> [并发]
# 关键：argv 必须与 Emby MediaProbeManager 完全一致，否则 key 对不上（缓存不命中）
set -u
DIR="${1:?需要目录}"
W="${2:-8}"
CACHE=/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/cache
before=$(ls "$CACHE" 2>/dev/null | wc -l)
echo "[prewarm] 目标=$DIR 并发=$W 起始缓存=$before"
find "$DIR" -type f \( -name '*.mp4' -o -name '*.mkv' -o -name '*.ts' \) -print0 \
| xargs -0 -P "$W" -I{} sh -c '
  f="$1"
  c="${f/\/opt\/embytg\/mnt\/embytg/\/mnt\/embytg}"
  timeout 120 docker exec emby /bin/ffprobe -i "file:$c" -threads 0 -v info -print_format json -show_streams -show_chapters -show_format -show_data >/dev/null 2>&1
  echo "."
' _ {} | tr -d '\n'
echo
after=$(ls "$CACHE" 2>/dev/null | wc -l)
echo "[prewarm] 完成：缓存 $before -> $after (+$((after-before)))"
