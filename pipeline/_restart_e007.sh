#!/bin/bash
# E007 部署自检：语法 -> 全量 NFO 直写 -> 审计 -> 清 HOLD 状态 -> 重启 --loop
cd /opt/embytg/pipeline || exit 1
BK=/opt/embytg/backup

echo "=== 1) REMOTE SYNTAX ==="
python3 -c "import ast,io; [ast.parse(io.open(f,encoding='utf-8').read()) for f in ['auto_finalize.py','nfo_to_emby.py']]; print('REMOTE_SYNTAX_OK')" || exit 1

echo "=== 2) FULL NFO DIRECT-WRITE (self-heal 3 rounds) ==="
python3 -u nfo_to_emby.py --apply --workers 16 --rounds 3

echo "=== 3) AUDIT AFTER WRITE ==="
python3 finalize_meta.py --audit

echo "=== 4) RESET STATE + RESTART LOOP ==="
if [ -f "$BK/finalize_state.json" ]; then
  cp "$BK/finalize_state.json" "$BK/finalize_state.pre_e007_$(date +%s).json"
fi
rm -f "$BK/finalize_state.json" "$BK/LATEST.json"
pkill -f "auto_finalize.py --loop" || true
sleep 2
nohup python3 -u auto_finalize.py --loop >> "$BK/auto_finalize.stdout.log" 2>&1 < /dev/null &
echo "NEW_PID=$!"
sleep 3

echo "=== 5) PROC + STATE ==="
ps -eo pid,args | grep "[a]uto_finalize" || true
echo "--- tail log ---"
tail -n 12 "$BK/auto_finalize.log"
echo "=== RESTART_E007_DONE ==="
