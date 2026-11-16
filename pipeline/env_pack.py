#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""env_pack.py —— 打「OS 层环境包」，补上全量备份包从不包含的那一层。

为什么需要它（2026-11-13，用户要求「服务器全失，新机完美展开」）：
全量备份包只含 /opt/embytg 里的状态与脚本，恢复出来的机器能起 Emby，
但 nginx vhost / SSL 证书 / rclone 二进制 / systemd unit / cron / box 控制密钥
全都不在包里 —— MIGRATION.md 早就把这条列为 R6，一直没做。没有这些，
新机起来也没有对外入口、挂不上 TG、唤不醒云电脑。

产出：/opt/embytg/backup/env_<ts>.tar.gz + env_LATEST.json
用法：
    python3 env_pack.py            # 打包
    python3 env_pack.py --verify   # 只打印会收什么/缺什么
"""
import glob
import hashlib
import json
import os
import subprocess
import sys
import tarfile
from datetime import datetime

ROOT = "/opt/embytg"
BK = os.path.join(ROOT, "backup")
LATEST = os.path.join(BK, "env_LATEST.json")

# (绝对路径, 包内相对路径)；glob 用 GLOB: 前缀
ENTRIES = [
    # --- 对外入口：没有这两项，新机起来外界访问不到 ---
    ("/www/server/panel/vhost/nginx/emby.xy4.de5.net.conf", "nginx/vhost.conf"),
    ("/www/server/panel/vhost/cert/emby.xy4.de5.net", "cert/emby.xy4.de5.net"),
    # --- rclone（带 teldrive backend 的自编译版，新机装不到同款）---
    ("/usr/bin/rclone", "bin/rclone"),
    # --- systemd：mount / autoops / finalize / runner / boxwake / tunnel ---
    ("GLOB:/etc/systemd/system/embytg-*", "systemd/"),
    ("GLOB:/etc/systemd/system/hermes-gateway.service", "systemd/"),
    # --- cron：全部 embytg 相关 + ffprobe shim ---
    ("GLOB:/etc/cron.d/embytg_*", "cron/"),
    ("GLOB:/etc/cron.d/p03_*", "cron/"),
    # --- 云电脑控制密钥与令牌（没有它，恢复后唤不醒三台 box）---
    ("/root/.ssh/embytg_box_control", "keys/embytg_box_control"),
    ("/root/.config/embytg/box_tokens.json", "keys/box_tokens.json"),
    ("/root/.ssh/embytg_self", "keys/embytg_self"),
    ("/root/.ssh/authorized_keys", "keys/authorized_keys"),
    # --- Hermes 侧：任务定义 + 配置（cron 层不能丢）---
    ("/root/.hermes/cron/jobs.json", "hermes/cron_jobs.json"),
    ("/root/.hermes/config.yaml", "hermes/config.yaml"),
    ("/root/.hermes/.env", "hermes/env"),
    # --- 项目全部文本（脚本/文档/配置真源）---
    ("/opt/embytg/AGENTS.md", "project/AGENTS.md"),
    ("/opt/embytg/START_HERE.md", "project/START_HERE.md"),
    ("/opt/embytg/docs/CREDENTIALS.txt", "project/CREDENTIALS.txt"),
    ("/opt/embytg/.r2_backup.json", "project/.r2_backup.json"),
    # 2026-11-14：源站账号。**前面一直没收进包里**，
    # 而它被 gitignore（pipeline/**/*.json），也不在 git 历史里 ——
    # 等于说：空机恢复后没有任何源，整个上片流程起不来。
    # 从 _archive/ 挪到 config/ 的同时补上这一行。
    ("/opt/embytg/pipeline/config/sources.json", "project/pipeline/config/sources.json"),
    # 2026-11-14（E073）：按出口地区封禁的源要走代理，这份映射是**运行必需**，
    # 且被 pipeline/**/*.json 规则 gitignore 掉了 —— 不进包的话，
    # 空机恢复后 yuntv 这类源会静默退回直连（然后 403）。
    ("/opt/embytg/pipeline/config/source_proxy.json",
     "project/pipeline/config/source_proxy.json"),
    # 2026-11-14（E079）：源优先级（实测下载速度）。被 pipeline/**/*.json
    # 规则 gitignore 掉，不进包的话空机恢复后队列会退回按集数排 ——
    # 也就是把 97% 的内容排在传不动的源上。
    ("/opt/embytg/pipeline/config/source_priority.json",
     "project/pipeline/config/source_priority.json"),
    # 2026-11-16：**可扩展性真源**（lanes / store_channels / TG 账号归属）。
    # 它同样被 pipeline/**/*.json 规则 gitignore 掉，而且此前不在包清单里 ——
    # 空机恢复后 gha_next3 会退回写死的 7 lane 兜底表（含已下线的
    # new3/new4/new5），store_channels 也会退回 config.json。
    # 「加机器/停机器只改这一份配置」这句话只有在它随包走时才成立。
    ("/opt/embytg/pipeline/config/scale.json",
     "project/pipeline/config/scale.json"),
    # 2026-11-14：源日额度与冷却账本。被 gitignore 且从不在包清单里 ——
    # 空机恢复后所有源都会退回默认 300/天，产能直接砍半。
    # 注意：这是**运行时账本**（含当日已用次数），恢复后 day 计数会被重置，
    # 但 daily_cap / cap_note 这些配置项必须留下。
    ("/opt/embytg/pipeline/.gha_src_budget.json",
     "project/pipeline/.gha_src_budget.json"),
    # 2026-11-14：TG 账号池的实测吞吐账本。丢了不影响正确性（会重新测），
    # 但恢复后前几集分片分配会退化成均分，慢号拖长尾 → worker 超时。
    ("/opt/embytg-runner/config/tg_pool.json",
     "runner/config/tg_pool.json"),
    # 扫描冷却台账（86 KB）：丢了会把 6000 部剧重扫一遍（约 3 万次请求）。
    ("/opt/embytg/pipeline/.gha_new_done.json", "project/pipeline/.gha_new_done.json"),
    ("/opt/embytg/config/r2_backup.conf", "project/r2_backup.conf"),
    # --- 数据面节点侧脚本（云电脑/PC 用的，不在 pipeline/ 下，但缺了新节点跑不起来）---
    # 同时以 runner/ 和 scripts/ 两份落地：runner/ 给节点机用，
    # scripts/ 保证「解 full 包得到一套完整 scripts」这条断言对新机也成立。
    ("/opt/embytg-runner/tg_pool.py", "runner/tg_pool.py"),
    ("/opt/embytg-runner/pc_upload.py", "runner/pc_upload.py"),
    ("/opt/embytg-runner/pc_worker.py", "runner/pc_worker.py"),
    ("/opt/embytg-runner/run_one.sh", "runner/run_one.sh"),
    ("/opt/embytg-runner/lane_policy.py", "runner/lane_policy.py"),
    ("/opt/embytg-runner/healthcheck.sh", "runner/healthcheck.sh"),
    ("/opt/embytg-runner/mint_bot_session.py", "runner/mint_bot_session.py"),
    ("/opt/embytg-runner/check_session_file.py", "runner/check_session_file.py"),
    ("/opt/embytg-runner/gha_download.py", "runner/gha_download.py"),
    ("/opt/embytg-runner/gha_upload.py", "runner/gha_upload.py"),
    ("/opt/embytg-runner/runner.env", "runner/runner.env"),
    ("/usr/local/bin/boxctl", "bin/boxctl"),
    ("/usr/local/bin/boxwake", "bin/boxwake"),
    # --- 自检脚本不能丢：它们在空机上就是恢复后的第一批工具 ---
    ("/opt/embytg/pipeline/restore_to_new_host.sh", "bin/restore_to_new_host.sh"),
    ("/opt/embytg/pipeline/env_pack.py", "bin/env_pack.py"),
    # --- Emby 插件（缺了功能就退化，且不随 git 走）---
    # TargetedScan：POST /Library/ScanPath，建新条目 <1s，取代整库 ffprobe（E036）。
    # 空机恢复时如果不带这个 dll，新剧只能靠库级刷新，20 万集会当场拖死。
    ("PLUGINS:EmbyTargetedScan.dll", "emby-plugins/EmbyTargetedScan.dll"),
    # --- 防火墙 ---
    ("/etc/iptables/rules.v4", "net/iptables-rules.v4"),
]

# 明确不收（体积大且可再生）
EXCLUDE_SIZE_MB = 200


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def expand():
    out, missing = [], []
    for src, dst in ENTRIES:
        if src.startswith("PLUGINS:"):
            # Emby 插件在 docker volume 里，路径不是仓库内相对路径
            src = os.path.join("/var/lib/docker/volumes/embytg_embydata/_data/plugins",
                               src.split(":", 1)[1])
        if src.startswith("GLOB:"):
            pat = src[5:]
            hits = sorted(glob.glob(pat))
            if not hits:
                missing.append(pat)
            for h in hits:
                if os.path.isfile(h):
                    out.append((h, dst + os.path.basename(h)))
        elif os.path.isdir(src):
            for dirpath, _dn, fn in os.walk(src):
                for f in fn:
                    ap = os.path.join(dirpath, f)
                    rel = os.path.relpath(ap, src)
                    out.append((ap, dst + "/" + rel))
        elif os.path.isfile(src):
            out.append((src, dst))
        else:
            missing.append(src)
    return out, missing


def verify():
    out, missing = expand()
    total = sum(os.path.getsize(p) for p, _ in out if os.path.getsize(p) < EXCLUDE_SIZE_MB * 1048576)
    print("将收录 %d 个文件，合计 %.1f MB" % (len(out), total / 1048576.0))
    for p, d in out:
        sz = os.path.getsize(p)
        flag = "  " if sz < EXCLUDE_SIZE_MB * 1048576 else "!!"
        print("  %s %-58s %8.2f MB" % (flag, d, sz / 1048576.0))
    if missing:
        print("\n缺失（新机上要补齐或忽略）：")
        for m in missing:
            print("  - " + m)
    return 0


def build():
    out, missing = expand()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = "env_%s" % ts
    path = os.path.join(BK, name + ".tar.gz")
    files = []
    with tarfile.open(path, "w:gz", compresslevel=6) as tf:
        for src, dst in out:
            if not os.path.isfile(src):
                continue
            if os.path.getsize(src) >= EXCLUDE_SIZE_MB * 1048576:
                missing.append("%s (超 %dMB 跳过)" % (src, EXCLUDE_SIZE_MB))
                continue
            tf.add(src, arcname=os.path.join(name, dst))
            files.append({"path": dst, "size": os.path.getsize(src), "sha256": sha256(src)})
        man = {
            "kind": "env",
            "created": datetime.now().strftime("%F %T %z"),
            "note": "OS 层环境包：nginx/证书/rclone/systemd/cron/box 密钥/hermes cron/项目文本",
            "files": files,
            "missing": missing,
            "restore": "tar xzf 到 / 之前先读 project/CREDENTIALS.txt；"
                       "systemd 文件放 /etc/systemd/system/ 后 daemon-reload",
        }
        b = json.dumps(man, ensure_ascii=False, indent=2).encode("utf-8")
        info = tarfile.TarInfo(os.path.join(name, "ENV_MANIFEST.json"))
        info.size = len(b)
        import io as _io
        tf.addfile(info, _io.BytesIO(b))
    size = os.path.getsize(path)
    latest = {
        "state": "DONE",
        "created": datetime.now().strftime("%F %T"),
        "archive": path,
        "archive_name": name + ".tar.gz",
        "sha256": sha256(path),
        "size_bytes": size,
        "files": len(files),
        "missing": missing,
    }
    tmp = LATEST + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)
    os.replace(tmp, LATEST)
    print("ENV PACK DONE %s / %.1f MB / %d 文件 / 缺失 %d / sha256=%s"
          % (latest["archive_name"], size / 1048576.0, len(files), len(missing), latest["sha256"][:16]))
    # 只留最近 2 个
    packs = sorted(glob.glob(os.path.join(BK, "env_*.tar.gz")), key=os.path.getmtime, reverse=True)
    for old in packs[2:]:
        os.remove(old)
        print("  清理旧 env 包 %s" % os.path.basename(old))
    return 0


if __name__ == "__main__":
    if "--verify" in sys.argv:
        sys.exit(verify())
    sys.exit(build())
