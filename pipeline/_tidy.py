# -*- coding: utf-8 -*-
"""EmbyTG 项目整理器（本地 F:\\emby 与服务器 /opt/embytg 共用同一套逻辑）。

四层结构：
  根目录   = 当前主力脚本 + 统一配置真源 + 接手手册 + 运行时凭证/状态
  tools/   = 历史但可复现的运维脚本（建库、播放链路调优、部署副本），有长期价值
  docs/    = 历史文档（背景价值，数字已过时，以 PROJECT.md 为准）
  _archive/= 一次性探针 / 中间产物 / 已被取代的旧链路脚本（确认无用后整目录删除）
  真删     = 日志 / 截图 / pyc / 缓存目录

只处理指定目录下的「一级文件」；除 __pycache__ 外的目录一律整体保留。
用法:
  python _tidy.py DIR [DIR ...]              # dry-run，只打印分类报告
  python _tidy.py --apply DIR [DIR ...]      # 真正执行
"""
import os, sys, fnmatch, shutil

args = sys.argv[1:]
APPLY = '--apply' in args
DIRS = [a for a in args if a != '--apply'] or ['.']

# ---- 留根目录：当前在跑的 / 接手第一入口 / 断点续传依赖 ----
KEEP = set("""
config.json embytg_conf.py _tidy.py
tgpipe.py nfo_gen.py nfo_only.py finalize_meta.py disk_watch.sh
PROJECT.md README.md AGENTS.md
worker-tgemby.js compose.yaml rebind-emby.sh api.sh
.tg_string .tg_string2 .tg_user.session .bot_tokens.json .token.json .apikey.json
.env .flow .tg_state.json .login_flow
tg_state.json tg_pending.json tg_meta_cache.json tg_scan_raw.json
lib_opts_before_nfo.json
""".split())

# ---- 移到 tools/：历史但可复现的运维能力 ----
TOOLS = set("""
fix_libs.py fix_fetchers.py
apply_play_fix.sh fix_chunksize.py fix_readahead.py optimize_play.py verify_play_fix.py
audit_queue.py bench_meta.py consume_queue.sh snapshot_now.py verify_all.py repair_library.py
gen_map.py tg_crossclone.py
server_config.toml server_rclone.conf
""".split())

# ---- 移到 docs/：历史文档 ----
DOCS = set("""
HANDOFF.md 可行性报告.md 部署完成报告.md 转存资产清洗技术简报.md
fix-2026-11-07-range.md 部署步骤.md CREDENTIALS.txt
""".split())

# ---- 真删 ----
DELETE_GLOB = ['*.log', '*.pyc', '*.bak', '*.bak.*', 'screen*.png', '*_prev.log', '*.tmp']
DELETE_EXACT = set(['_t.py', '_refactor.py', 'thumbs.db', 'desktop.ini'])


def classify(name):
    if name in KEEP:
        return 'KEEP'
    if name in TOOLS:
        return 'TOOLS'
    if name in DOCS:
        return 'DOCS'
    if name in DELETE_EXACT or any(fnmatch.fnmatch(name, g) for g in DELETE_GLOB):
        return 'DELETE'
    return 'ARCHIVE'


def tidy(d):
    d = os.path.abspath(d)
    if not os.path.isdir(d):
        print('SKIP not a dir:', d); return
    tools = os.path.join(d, 'tools'); docs = os.path.join(d, 'docs'); arc = os.path.join(d, '_archive')
    buckets = {'KEEP': [], 'TOOLS': [], 'DOCS': [], 'ARCHIVE': [], 'DELETE': []}
    for name in sorted(os.listdir(d)):
        p = os.path.join(d, name)
        if os.path.isdir(p):
            if name == '__pycache__':
                buckets['DELETE'].append(name + '/ (pycache)')
                if APPLY:
                    shutil.rmtree(p, ignore_errors=True)
            # 其余目录（emby-mcp/ deploy/ config/ mnt/ cache/ pgdata/ backup/
            #           tgvenv/ docs/ _archive/ tools/ logs/ staging/ ...）一律保留
            continue
        buckets[classify(name)].append(name)
    if APPLY:
        for sub in (tools, docs, arc):
            os.makedirs(sub, exist_ok=True)
        for n in buckets['TOOLS']:
            shutil.move(os.path.join(d, n), os.path.join(tools, n))
        for n in buckets['DOCS']:
            shutil.move(os.path.join(d, n), os.path.join(docs, n))
        for n in buckets['ARCHIVE']:
            shutil.move(os.path.join(d, n), os.path.join(arc, n))
        for n in buckets['DELETE']:
            fp = os.path.join(d, n)
            if os.path.isfile(fp): os.remove(fp)
    print('\n=== %s %s ===' % (d, '[APPLY]' if APPLY else '[DRY-RUN]'))
    for k in ('KEEP', 'TOOLS', 'DOCS', 'ARCHIVE', 'DELETE'):
        v = buckets[k]
        print('  %-8s %3d  %s' % (k, len(v), ', '.join(v[:12]) + (' ...' if len(v) > 12 else '')))


for d in DIRS:
    tidy(d)
print('\n' + ('DONE' if APPLY else 'DRY-RUN only; add --apply to execute'))
