#!/usr/bin/env bash
echo "===== docker ps -a (全部) ====="
docker ps -a --format 'NAME={{.Names}} IMAGE={{.Image}} STATUS={{.Status}}'
echo
echo "===== systemd 相关服务 ====="
systemctl list-units --type=service --all 2>/dev/null | grep -iE 'new-api|embykeeper|wb|workbuddy|tgdb|embytg' || echo "(无匹配 systemd 单元)"
echo
echo "===== /etc/cron.d 列表 ====="
ls -la /etc/cron.d/ 2>/dev/null || echo "(无 /etc/cron.d)"
echo
echo "===== 三旁挂服务的 inspect ====="
for c in new-api embykeeper wb-proxy workbuddy2api-hub; do
  echo "----- $c -----"
  if docker inspect "$c" >/dev/null 2>&1; then
    docker inspect "$c" --format 'RESTART={{.HostConfig.RestartPolicy.Name}}'
    docker inspect "$c" --format 'CMD={{json .Config.Cmd}}'
    docker inspect "$c" --format 'ENV={{json .Config.Env}}'
    docker inspect "$c" --format 'PORTS={{json .NetworkSettings.Ports}}'
    docker inspect "$c" --format 'MOUNTS={{json .Mounts}}'
  else
    echo "(容器不存在)"
  fi
done
echo
echo "===== 旁挂数据目录大小 ====="
for d in /opt/new-api /root/embykeeper /root/workbuddy2api-hub; do
  if [ -d "$d" ]; then echo "$d -> $(du -sh "$d" 2>/dev/null | cut -f1)"; else echo "$d (不存在)"; fi
done
echo
echo "===== tg.db 现状 ====="
ls -la /opt/embytg/pipeline/tg.db* 2>/dev/null || echo "(无 tg.db)"
echo
echo "===== embydata 子目录(决策排除) ====="
du -sh /var/lib/docker/volumes/embytg_embydata/_data/* 2>/dev/null
echo
echo "===== 是否有 tgdb 每日同步脚本/定时 ====="
ls -la /opt/embytg/pipeline/tgdb_daily_sync.py 2>/dev/null || echo "(无 tgdb_daily_sync.py)"
grep -nE 'tgdb_daily|embytg_tgdb' /etc/cron.d/* 2>/dev/null || echo "(cron 未引用)"
