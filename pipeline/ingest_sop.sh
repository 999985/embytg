#!/bin/sh
# ingest_sop.sh — 入库完整 SOP（PROJECT.md §十 / START_HERE.md §2 的可执行版）
#
# 存在理由：2026-11-09 一个 agent 只跑了 scan+apply，漏掉 nfo_gen / restore_fetchers
# / 刷新 / audit，还手搓 Emby 端点，被用户连续质问两次。把 6 步固化成脚本，
# 让"照抄"取代"凭印象"。
#
# 用法：
#   bash pipeline/ingest_sop.sh                 # 全 7 频道（PROJECT.md §十 原样）
#   bash pipeline/ingest_sop.sh yingji         # 单频道，从第一条消息开始
#   bash pipeline/ingest_sop.sh yingji --dry   # 只预演，不转存不写 NFO
#
# 安全：
#   - 每一步失败即停（set -e 语义用显式 check），不带着半成品往下走
#   - 用 /usr/bin/python3（agent 会话 python3 可能是 uv 3.12 无 Telethon）
#   - 不改任何凭证；不碰改名 agent 的文件；不重复 apply（dedupe 幂等）
#   - 执行前后都提示去看 docs/OPS_JOURNAL.md（多 agent 协作）

set -u
PY=/usr/bin/python3
PIPE=/opt/embytg/pipeline
cd "$PIPE" || exit 1

CH="${1:-}"
DRY="${2:-}"
[ -n "$CH" ] && CH_ARG="--channels $CH" || CH_ARG=""
[ -n "$DRY" ] && DRY_ARG="--dry" || DRY_ARG=""

say() { printf '\n===== %s =====\n' "$1"; }
ok()  { printf '  [OK] %s\n' "$1"; }
die() { printf '  [FAIL] %s\n' "$1" >&2; exit 1; }

say "入库 SOP 开始 | 频道=${CH:-全部7频道} | dry=${DRY:-否} | $(date '+%F %T')"
echo "  提醒：动手前/完成后都要在 docs/OPS_JOURNAL.md 追加一条（多 agent 协作）"
echo "  SOP 原文：PROJECT.md §十 / START_HERE.md §2"

# ---------------- 步骤 1：生成队列 ----------------
if [ -n "$DRY_ARG" ]; then
  # dry 模式不能带 --queue：scan --queue 会整体替换 pending 表，那不是"只读预演"
  say "1/6 scan --full（DRY：只扫描预览，不写队列）"
  $PY tgpipe.py scan --full $CH_ARG --kinds movie,tv --dry || die "scan dry 失败"
  ok "扫描预览完成（pending 未被覆盖）"
else
  say "1/6 scan --full --queue ${CH_ARG}"
  $PY tgpipe.py scan --full --queue $CH_ARG --kinds movie,tv || die "scan 失败"
  ok "队列已生成"
fi

# ---------------- 步骤 2：引用转发登记 ----------------
if [ -n "$DRY_ARG" ]; then
  say "2/6 apply（DRY 完全跳过，不读也不写队列）"
  ok "预演跳过（步骤 1 已给出待上片预览）"
else
  say "2/6 apply --from-queue --loop --max-items 50"
  # --loop 常驻单连接跑完整队列；第二账号存在才加 --acct2
  ACCT2=""
  [ -f /opt/embytg/.tg_string2 ] && ACCT2="--acct2 /opt/embytg/.tg_string2"
  $PY tgpipe.py apply --from-queue --loop --max-items 50 $ACCT2 || die "apply 失败"
  ok "登记完成（apply 内部已发 E008 增量通知）"
fi

# ---------------- 步骤 3：自主刮削（最容易漏！）----------------
if [ -n "$DRY_ARG" ]; then
  say "3/6 nfo_gen.py（DRY 只列清单）"
  $PY nfo_gen.py --scan --kind all || die "nfo_gen --scan 失败"
else
  say "3/6 nfo_gen.py --kind all --workers 24  ← 自己刮削，不靠 Emby"
  $PY nfo_gen.py --kind all --workers 24 || die "nfo_gen 失败"
  ok "NFO + poster 已写进 TG 挂载（<plot> 标签）"
fi

# ---------------- 步骤 4：复核 NFO-only 配置 ----------------
say "4/6 restore_fetchers.py --verify"
$PY restore_fetchers.py --verify || die "restore_fetchers 复核失败"
ok "Series 文字 fetcher 在、四类型图片全关"

# ---------------- 步骤 5：让 Emby 读到本地 NFO ----------------
if [ -n "$DRY_ARG" ]; then
  say "5/6 _scan_now.py（DRY 跳过）"
else
  say "5/6 _scan_now.py  ← 现成脚本，别手搓 /Library/Refresh 或 Media/Updated"
  $PY _scan_now.py || die "_scan_now 失败"
  ok "全库扫描已触发（或已在运行，不重复触发）"
  echo "  注意：NFO-only 下 EnableRealtimeMonitor=false，204 ≠ 条目已建。"
  echo "  扫描是全库串行，新片排在队尾；用 tail 日志看真实进度，别盯 26.1% 的数字。"
fi

# ---------------- 步骤 6：体检 ----------------
say "6/6 finalize_meta.py --audit（只读）"
$PY finalize_meta.py --audit || die "audit 失败"
ok "体检完成"

say "SOP 结束 | $(date '+%F %T')"
echo "  收尾必做："
echo "   1) python3 lib_reconcile.py --json   # 判收敛只认按库对账"
echo "   2) 在 docs/OPS_JOURNAL.md 追加一条（做了什么/实测数字/给其他 agent）"
echo "   3) 等扫描扫到 Movies/<分类> 后复核 /Items?SearchTerm=<片名>"
