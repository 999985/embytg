#!/bin/sh
# P0-3 / E011 ffprobe 透明缓存 shim (EmbyTG). 详见 docs/ERRORS.md E011
# 不写库 / 不碰 Emby 源码: 只在 ffprobe stdout 边界缓存并回放原始 JSON,
# Emby 自己的 FFProbeProvider 照常解析并写 MediaStreams2。
# 失败安全: 非标准 argv / 无 -i / URL / stat 失败 => exec 真身;
#           rc!=0 或 stdout 不含 streams+format+codec_type => 不落缓存; rc/stderr 透传。
# key = sha256(path | size | 归一化argv): 换片失效, argv 变化不串味; 绝不算全文件内容 hash(E001)。
REAL=/bin/ffprobe.real
CACHE=/config/mediainfo/cache

case " $* " in
  *" -print_format json "*) : ;;
  *) exec "$REAL" "$@" ;;
esac

p=""; prev=""
for a in "$@"; do
  if [ "$prev" = "-i" ]; then p="$a"; break; fi
  prev="$a"
done
[ -n "$p" ] || exec "$REAL" "$@"

path=$(printf '%s' "$p" | sed 's/^file://; s/^"//; s/"$//')
[ -n "$path" ] || exec "$REAL" "$@"
case "$path" in
  http:*|https:*) exec "$REAL" "$@" ;;
esac
sz=$(stat -c %s "$path" 2>/dev/null) || exec "$REAL" "$@"

argv=""; skip=0
for a in "$@"; do
  if [ "$skip" = "1" ]; then skip=0; continue; fi
  if [ "$a" = "-i" ]; then skip=1; continue; fi
  argv="$argv|$a"
done
key=$(printf '%s|%s|%s' "$path" "$sz" "$argv" | sha256sum | awk '{print $1}')
[ -n "$key" ] || exec "$REAL" "$@"

mkdir -p "$CACHE" 2>/dev/null
f="$CACHE/$key.json"
if [ -s "$f" ]; then
  cat "$f"
  exit 0
fi

tmp="$CACHE/.$key.$$.tmp"
trap 'rm -f "$tmp"' 0 HUP INT TERM
"$REAL" "$@" > "$tmp"
rc=$?
if [ "$rc" -eq 0 ] && [ -s "$tmp" ] && grep -q '"streams"' "$tmp" && grep -q '"format"' "$tmp" && grep -q '"codec_type"' "$tmp"; then
  if mv -f "$tmp" "$f" 2>/dev/null; then cat "$f"; else cat "$tmp"; fi
else
  cat "$tmp"
fi
rm -f "$tmp"
exit "$rc"
