#!/bin/bash
# P0-3 / E011 验证: 传 /tmp/xxx = 隔离测; 不传 = 测已部署的 /bin/ffprobe
set -u
PIPE=/opt/embytg/pipeline
C=emby
SHIM="$PIPE/p0_3_ffprobe_shim.sh"
HOST_BAK=/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/ffprobe.real.bak
TARGET="${1:-/bin/ffprobe}"
CACHE_HOST=/var/lib/docker/volumes/embytg_embydata/_data/mediainfo/cache

echo "[verify] target=$TARGET"
if ! docker exec "$C" sh -c 'test -x /bin/ffprobe.real' 2>/dev/null; then
  docker cp "$HOST_BAK" "$C":/bin/ffprobe.real
  docker exec "$C" chmod 755 /bin/ffprobe.real
fi
if [ "$TARGET" != "/bin/ffprobe" ]; then
  docker cp "$SHIM" "$C":"$TARGET"
  docker exec "$C" chmod +x "$TARGET"
fi
docker exec "$C" mkdir -p /config/mediainfo/cache

SAMPLE='/mnt/embytg/TV/欧美剧/纸牌屋 (2013) {tmdb=1425}/Season 04/纸牌屋 S04E06 - 2160p.mp4'
if ! docker exec "$C" test -f "$SAMPLE" 2>/dev/null; then
  SAMPLE=$(docker exec "$C" sh -c 'find /mnt/embytg/TV -name "*.mp4" -type f 2>/dev/null | head -1')
fi
echo "[verify] sample=$SAMPLE"
[ -n "$SAMPLE" ] || { echo "FATAL 无样本"; exit 1; }

cnt0=$(ls "$CACHE_HOST"/*.json 2>/dev/null | wc -l)
ARGS=(-i "file:$SAMPLE" -threads 0 -v info -print_format json -show_streams -show_chapters -show_format -show_data)
T0=$(date +%s%N)
docker exec "$C" "$TARGET" "${ARGS[@]}" > /tmp/p03_v_cold.json 2>/tmp/p03_v_cold.err
RC_C=$?
T1=$(date +%s%N)
echo "[verify] 冷启 rc=$RC_C bytes=$(wc -c </tmp/p03_v_cold.json) ms=$(( (T1-T0)/1000000 ))"
cnt1=$(ls "$CACHE_HOST"/*.json 2>/dev/null | wc -l)
if [ "$RC_C" -ne 0 ] || [ ! -s /tmp/p03_v_cold.json ]; then
  echo "FATAL 冷启失败 (真身在新位置无法探测?)"; head -10 /tmp/p03_v_cold.err; exit 1
fi
T0=$(date +%s%N)
docker exec "$C" "$TARGET" "${ARGS[@]}" > /tmp/p03_v_hot.json 2>/tmp/p03_v_hot.err
RC_H=$?
T1=$(date +%s%N)
echo "[verify] 热启 rc=$RC_H bytes=$(wc -c </tmp/p03_v_hot.json) ms=$(( (T1-T0)/1000000 ))"
cnt2=$(ls "$CACHE_HOST"/*.json 2>/dev/null | wc -l)
cmp -s /tmp/p03_v_cold.json /tmp/p03_v_hot.json && echo "[verify] 冷热字节 IDENTICAL" || { echo "FATAL 字节不一致"; exit 1; }
python3 -c 'import json;d=json.load(open("/tmp/p03_v_hot.json"));print("[verify] JSON streams=%d chapters=%d duration=%s"%(len(d["streams"]),len(d.get("chapters",[])),d["format"].get("duration")))'
echo "[verify] 缓存数 前=$cnt0 冷后=$cnt1 热后=$cnt2 (热启不应新增)"

echo "[verify] 不同 argv 应生成新 key"
docker exec "$C" "$TARGET" -i "file:$SAMPLE" -v info -print_format json -show_streams -show_format > /tmp/p03_v_alt.json 2>/dev/null
cnt3=$(ls "$CACHE_HOST"/*.json 2>/dev/null | wc -l)
echo "[verify] alt bytes=$(wc -c </tmp/p03_v_alt.json) 缓存数=$cnt3 (应比热后+1)"

echo "[verify] -version 透传"
docker exec "$C" "$TARGET" -version 2>&1 | head -1
echo "[verify] Emby API /System/Info/Public = $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8096/System/Info/Public)"
echo "[verify] DONE"
exit 0
