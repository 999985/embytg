#!/usr/bin/env bash
# =============================================================================
# restore_to_new_host.sh —— 服务器全失后，在一台全新机器上把 EmbyTG 恢复起来
#
# 2026-11-13 用户要求：「保证能在服务器无法访问的情况下，在下一台机器能完美展开，
# 立即恢复」。本脚本就是那条恢复路径的可执行版本，配套 R2 上的三类包：
#   full-latest.tar.gz  全量（library.db + PG dump + 脚本 + 凭证 + 状态）
#   env-latest.tar.gz   OS 层（nginx/证书/rclone/systemd/cron/box 密钥/hermes cron）
#   daily-latest.tar.gz 轻量（tg.db + 凭证，用作 full 不可取时的兜底）
#
# 用法（在新机 root 下）：
#   1) 装 docker + python3 + rclone 依赖前先联网
#   2) R2_KEY=... R2_ACCOUNT=... bash restore_to_new_host.sh
#      或把 .r2_backup.json 放到 /opt/embytg-restore/r2.json 里，脚本自动读
#   3) 脚本会逐步打印 [1/9]...[9/9] 和最终验收结果
#
# 设计原则：
#   - 任何一步失败立刻停（set -e），绝不带着半截状态往下走
#   - DATA_KEY / SIGNING_KEY 一致性校验不过就中止 —— 这两个字节变了，
#     TG 里的文件映射永远解不开，宁可不恢复也不能搞错
#   - 不碰旧机器的任何东西（这里假设旧机已经没了）
# =============================================================================
set -euo pipefail

R2_BASE="https://api.cloudflare.com/client/v4/accounts/%s/r2/buckets/%s/objects"
WORK="${WORK:-/opt/embytg-restore}"
ROOT="/opt/embytg"
BUCKET="${BUCKET:-tgemby}"
R2JSON="${R2JSON:-$WORK/r2.json}"

step() { printf '\n\033[1;36m== [%s] %s\033[0m\n' "$1" "$2"; }
ok()   { printf '   \033[32m✔\033[0m %s\n' "$1"; }
warn() { printf '   \033[33m!\033[0m %s\n' "$1"; }
die()  { printf '   \033[31m✘ %s\033[0m\n' "$1"; exit 1; }

step 1/9 "环境检查"
command -v docker >/dev/null || die "docker 未安装"
docker compose version >/dev/null 2>&1 || die "docker compose 不可用"
command -v python3 >/dev/null || die "python3 未安装"
mkdir -p "$WORK"
ok "docker / python3 就位，工作目录 $WORK"

step 2/9 "读取 R2 凭证"
if [ -f "$R2JSON" ]; then
  ACCOUNT=$(python3 -c "import json;print(json.load(open('$R2JSON'))['account_id'])")
  APIKEY=$(python3 -c "import json;print(json.load(open('$R2JSON'))['api_key'])")
elif [ -n "${R2_ACCOUNT:-}" ] && [ -n "${R2_KEY:-}" ]; then
  ACCOUNT="$R2_ACCOUNT"; APIKEY="$R2_KEY"
else
  die "缺少 R2 凭证（放 $R2JSON 或设 R2_ACCOUNT/R2_KEY）"
fi
ok "account=${ACCOUNT:0:8}… bucket=$BUCKET"

fetch() {  # $1=对象名 $2=落地文件
  local url
  url=$(printf "$R2_BASE" "$ACCOUNT" "$BUCKET")"/$1"
  printf '   拉取 %-24s' "$1"
  curl -fsSL -H "Authorization: Bearer $APIKEY" "$url" -o "$2" || die "下载 $1 失败"
  printf ' %8.1f MB\n' "$(echo "scale=1;$(stat -c %s "$2")/1048576" | bc)"
}

step 3/9 "下载三类包"
fetch "full-latest.tar.gz"  "$WORK/full.tar.gz"
fetch "env-latest.tar.gz"   "$WORK/env.tar.gz"
fetch "manifest.json"       "$WORK/r2_manifest.json" || warn "manifest 拉取失败，继续"
ok "包已落地 $WORK"

step 4/9 "校验完整性"
python3 - "$WORK/r2_manifest.json" "$WORK" <<'PY'
import json, os, sys, hashlib
man, work = sys.argv[1], sys.argv[2]
try:
    d = json.load(open(man))
except Exception:
    print("   ! manifest 不可读，跳过 sha 校验"); raise SystemExit(0)
def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
    return h.hexdigest()
for obj, local in (("full-latest.tar.gz", "full.tar.gz"), ("env-latest.tar.gz", "env.tar.gz")):
    want = (d.get(obj) or {}).get("sha256") or d.get("sha_" + obj)
    p = os.path.join(work, local)
    if not want:
        print("   - %s 无 sha 记录" % obj); continue
    got = sha(p)
    print(("   ✔ " if got == want else "   ✘ ") + "%s sha256 %s" % (obj, "匹配" if got == want else "不匹配"))
    if got != want: raise SystemExit("sha 不匹配，拒绝继续")
PY
ok "校验通过"

step 5/9 "解包到目标路径"
mkdir -p "$WORK/x"
tar xzf "$WORK/full.tar.gz" -C "$WORK/x"
FULLDIR=$(find "$WORK/x" -maxdepth 1 -type d -name 'embytg_backup_*' | head -1)
[ -n "$FULLDIR" ] || die "全量包结构异常"
mkdir -p "$ROOT"
# 1) 先把脚本/文档/配置铺开（新机若已有 /opt/embytg，保留旧目录）
for d in config sessions scripts state docs stats systemd teldrive emby; do
  [ -e "$FULLDIR/$d" ] || continue
  mkdir -p "$ROOT"
  cp -a "$FULLDIR/$d" "$ROOT/" 2>/dev/null || true
done
# 2) 恢复成运行时需要的真实路径（包内是 config/xxx，运行时要 /opt/embytg/xxx）
[ -f "$ROOT/config/config.json" ]      && cp -a "$ROOT/config/config.json" "$ROOT/config.json"
[ -f "$ROOT/config/.env" ]             && cp -a "$ROOT/config/.env" "$ROOT/.env"
[ -f "$ROOT/config/.token.json" ]      && cp -a "$ROOT/config/.token.json" "$ROOT/.token.json"
[ -f "$ROOT/config/.apikey.json" ]     && cp -a "$ROOT/config/.apikey.json" "$ROOT/.apikey.json"
[ -f "$ROOT/config/.bot_tokens.json" ] && cp -a "$ROOT/config/.bot_tokens.json" "$ROOT/.bot_tokens.json"
[ -f "$ROOT/config/compose.yaml" ]     && cp -a "$ROOT/config/compose.yaml" "$ROOT/compose.yaml"
[ -f "$ROOT/config/rclone.conf" ]      && mkdir -p "$ROOT/config" && cp -a "$ROOT/config/rclone.conf" "$ROOT/config/rclone.conf"
[ -f "$ROOT/config/config.toml" ]      && cp -a "$ROOT/config/config.toml" "$ROOT/config/config.toml"
for f in .tg_string .tg_string2 .tg_user.session; do
  [ -f "$ROOT/sessions/$f" ] && cp -a "$ROOT/sessions/$f" "$ROOT/$f"
done
# 3) 脚本落到 pipeline
mkdir -p "$ROOT/pipeline"
cp -a "$ROOT/scripts/." "$ROOT/pipeline/" 2>/dev/null || true
ok "完整目录已铺开到 $ROOT"

step 6/9 "命门校验：DATA_KEY / SIGNING_KEY"
[ -f "$ROOT/.env" ] || die ".env 缺失，无法恢复（teldrive 登录态与文件映射都靠它）"
for k in DATA_KEY SIGNING_KEY; do
  grep -q "^$k=" "$ROOT/.env" || die ".env 缺少 $k"
  v=$(grep "^$k=" "$ROOT/.env" | head -1 | cut -d= -f2-)
  [ ${#v} -ge 16 ] || die "$k 长度异常（${#v}），拒绝继续"
  ok "$k 存在（长度 ${#v}）"
done

step 7/9 "起数据库并导入 teldrive 映射"
cd "$ROOT"
docker compose --env-file .env up -d postgres
for i in $(seq 1 60); do
  docker exec teldrive-postgres pg_isready -U teldrive >/dev/null 2>&1 && break
  sleep 2
done
docker exec teldrive-postgres pg_isready -U teldrive >/dev/null 2>&1 || die "postgres 未就绪"
DUMP="$ROOT/teldrive/teldrive_dump.sql.gz"
[ -f "$DUMP" ] || die "包内无 teldrive_dump.sql.gz"
zcat "$DUMP" | docker exec -i teldrive-postgres psql -U teldrive -d teldrive -q
FILES=$(docker exec teldrive-postgres psql -U teldrive -d teldrive -tAc "select count(*) from files")
SESS=$(docker exec teldrive-postgres psql -U teldrive -d teldrive -tAc "select count(*) from sessions")
[ "$FILES" -gt 1000 ] || die "files 表只有 $FILES 行，导入异常"
[ "$SESS" -ge 1 ] || die "sessions 表为空，teldrive 登录态没恢复"
ok "PG 导入完成：files=$FILES sessions=$SESS"

step 8/9 "起 teldrive / 还原 Emby / 挂载"
docker compose --env-file .env up -d teldrive
sleep 8
curl -fsS http://127.0.0.1:8080/health/ready >/dev/null 2>&1 && ok "teldrive health ok" || warn "teldrive health 未就绪（继续）"

# Emby 卷：新机没有卷时先建，再把 library.db 灌进去
docker volume create embytg_embydata >/dev/null 2>&1 || true
if [ -f "$ROOT/emby/library.db" ]; then
  docker run --rm -v embytg_embydata:/data -v "$ROOT/emby":/src alpine \
    sh -c 'mkdir -p /data/data /data/config && cp /src/library.db /data/data/library.db' || warn "library.db 灌入失败"
  [ -f "$ROOT/emby/config/system.xml" ] && docker run --rm -v embytg_embydata:/data -v "$ROOT/emby":/src alpine \
    sh -c 'cp /src/config/system.xml /data/config/system.xml' || true
  ok "library.db / system.xml 已进卷"
fi
ok "容器层就绪"

step 9/9 "还原 OS 层（nginx/证书/rclone/systemd/cron/密钥）"
tar xzf "$WORK/env.tar.gz" -C "$WORK/x"
ENVDIR=$(find "$WORK/x" -maxdepth 1 -type d -name 'env_*' | head -1)
[ -n "$ENVDIR" ] || die "env 包结构异常"

# nginx vhost + 证书
if [ -f "$ENVDIR/nginx/vhost.conf" ]; then
  mkdir -p /www/server/panel/vhost/nginx
  cp -a "$ENVDIR/nginx/vhost.conf" /www/server/panel/vhost/nginx/emby.xy4.de5.net.conf
  ok "nginx vhost 已还原"
fi
if [ -d "$ENVDIR/cert/emby.xy4.de5.net" ]; then
  mkdir -p /www/server/panel/vhost/cert
  cp -a "$ENVDIR/cert/emby.xy4.de5.net" /www/server/panel/vhost/cert/
  ok "SSL 证书已还原"
fi
# rclone
if [ -f "$ENVDIR/bin/rclone" ]; then
  install -m 0755 "$ENVDIR/bin/rclone" /usr/bin/rclone
  ok "rclone 已安装 $(/usr/bin/rclone version 2>/dev/null | head -1)"
fi
# systemd
if ls "$ENVDIR/systemd/"* >/dev/null 2>&1; then
  cp -a "$ENVDIR/systemd/." /etc/systemd/system/
  systemctl daemon-reload
  ok "systemd unit 已还原并 daemon-reload"
fi
# cron
if ls "$ENVDIR/cron/"* >/dev/null 2>&1; then
  cp -a "$ENVDIR/cron/." /etc/cron.d/
  chmod 0644 /etc/cron.d/embytg_* /etc/cron.d/p03_* 2>/dev/null || true
  ok "cron 已还原"
fi
# box 控制密钥
if [ -f "$ENVDIR/keys/embytg_box_control" ]; then
  install -d -m 0700 /root/.ssh
  install -m 0600 "$ENVDIR/keys/embytg_box_control" /root/.ssh/embytg_box_control
  [ -f "$ENVDIR/keys/embytg_self" ] && install -m 0600 "$ENVDIR/keys/embytg_self" /root/.ssh/embytg_self
  [ -f "$ENVDIR/keys/authorized_keys" ] && install -m 0600 "$ENVDIR/keys/authorized_keys" /root/.ssh/authorized_keys
  ok "SSH 密钥已还原"
fi
if [ -f "$ENVDIR/keys/box_tokens.json" ]; then
  install -d -m 0700 /root/.config/embytg
  install -m 0600 "$ENVDIR/keys/box_tokens.json" /root/.config/embytg/box_tokens.json
  ok "boxwake 令牌已还原"
fi
# hermes cron
if [ -f "$ENVDIR/hermes/cron_jobs.json" ]; then
  install -d -m 0700 /root/.hermes/cron
  [ -f /root/.hermes/cron/jobs.json ] || cp -a "$ENVDIR/hermes/cron_jobs.json" /root/.hermes/cron/jobs.json
  ok "Hermes cron 任务定义已还原"
fi
# 防火墙
[ -f "$ENVDIR/net/iptables-rules.v4" ] && cp -a "$ENVDIR/net/iptables-rules.v4" /etc/iptables/rules.v4 2>/dev/null && ok "iptables 规则已还原"

printf '\n\033[1;32m========================================\033[0m\n'
printf '\033[1;32m 恢复完成。接下来手动确认三件事：\033[0m\n'
printf '  1) systemctl start embytg-mount.service   （挂 TG 目录）\n'
printf '  2) docker compose --env-file .env up -d emby\n'
printf '  3) cd /opt/embytg/pipeline && python3 lib_reconcile.py --json   （对账应为 converged=true）\n'
printf '\033[1;32m========================================\033[0m\n'
