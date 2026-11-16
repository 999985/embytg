#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""restore_drill.py —— 备份恢复演练（每月一次，G2 缺口）

为什么：备份每天打、R2 也有异地副本，但**从没验证过能不能恢复**。
本脚本只做**只读演练**：把最新备份包解到 `/tmp`，验证
① 包完整性（manifest 里的文件数与 sha256）
② `tg.db` 能打开且 `integrity_check=ok`、关键表行数合理
③ `emby/library.db` 能打开
④ 脚本/文档类文件齐全（抽样）
演练完删除临时目录，**绝不写生产目录**。

用法：
    python3 restore_drill.py            # 演练并出报告
    python3 restore_drill.py --keep     # 保留解包目录（排障用）
"""
import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BACKUP = "/opt/embytg/backup"
LATEST = os.path.join(BACKUP, "LATEST.json")
REPORT_DIR = os.path.join(HERE, "logs")


def md5(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def tf_manifest_handle(pack, member_name):
    """取出包内 manifest.json 的字节（2026-11-14：pg_verifybackup 式规则校验用）。

    先找与包同目录的 `<包名>_manifest.json` 旁挂文件（auto_finalize 会写），
    没有再回包内读 —— 两者都读不到就抛，让调用方记 FAIL，不静默当「没有清单」。
    """
    side = os.path.join(os.path.dirname(pack),
                        os.path.basename(pack).replace(".tar.gz", "_manifest.json"))
    if os.path.isfile(side):
        return open(side, "rb").read()
    with tarfile.open(pack, "r:gz") as tf:
        f = tf.extractfile(member_name)
        if f is None:
            raise RuntimeError("manifest.json 在包内不可读")
        return f.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()

    t0 = time.time()
    res = {"ts": time.strftime("%F %T"), "checks": [], "ok": True}

    def check(ok, name, detail=""):
        res["checks"].append({"ok": bool(ok), "name": name, "detail": detail})
        if not ok:
            res["ok"] = False
        print("  %s %-30s %s" % ("✔" if ok else "✘", name, detail))

    if not os.path.exists(LATEST):
        check(False, "LATEST.json", "不存在")
        return finish(res, t0)
    latest = json.load(open(LATEST, encoding="utf-8"))
    pack = latest.get("archive")
    check(bool(pack) and os.path.exists(pack), "备份包存在", os.path.basename(pack or ""))
    if not res["ok"]:
        return finish(res, t0)
    size_mb = os.path.getsize(pack) / 1e6
    check(size_mb > 50, "包大小合理", "%.1f MB" % size_mb)
    if latest.get("sha256"):
        got = md5(pack)
        check(got == latest["sha256"], "sha256 与 LATEST 一致", got[:16])

    tmp = tempfile.mkdtemp(prefix="drill_", dir="/tmp")
    try:
        with tarfile.open(pack, "r:gz") as tf:
            names = tf.getnames()
            check(len(names) > 80, "包内文件数", str(len(names)))
            tf.extractall(tmp, filter="data")
        root = os.path.join(tmp, names[0].split("/")[0])

        # 1) tg.db
        tg = None
        for dp, _, fs in os.walk(root):
            if "tg.db" in fs:
                tg = os.path.join(dp, "tg.db")
                break
        if tg:
            con = sqlite3.connect(tg)
            ok = con.execute("pragma integrity_check").fetchone()[0] == "ok"
            check(ok, "tg.db integrity_check", "ok" if ok else "FAIL")
            rows = con.execute("select count(*) from resource_state").fetchone()[0]
            check(rows > 1000, "tg.db resource_state 行数", str(rows))
            keys = con.execute("select count(*) from state_key").fetchone()[0]
            check(keys > 1000, "tg.db state_key 行数", str(keys))
            con.close()
        else:
            check(False, "tg.db 在包内", "未找到")

        # 2) Emby library.db
        ldb = None
        for dp, _, fs in os.walk(root):
            if "library.db" in fs:
                ldb = os.path.join(dp, "library.db")
                break
        if ldb:
            try:
                con = sqlite3.connect(ldb)
                n = con.execute("select count(*) from MediaItems").fetchone()[0]
                check(n > 10000, "Emby library.db 可读", "MediaItems=%d" % n)
                con.close()
            except Exception as e:                                 # noqa: BLE001
                check(False, "Emby library.db 可读", str(e)[:60])
        else:
            check(False, "library.db 在包内", "未找到")

        # 3) 关键脚本/文档抽样
        # 2026-11-13 灾备改造：抽样从 3 个扩到上片链路必需的那批 —— 旧版本只查
        # post_ingest/autoops/手册，结果「包缺 gha_next3/gha_finish/pc_upload」这类
        # 致命缺口永远演练不出来。
        #
        # 2026-11-14 自省（借鉴 PostgreSQL pg_verifybackup 的模型）：
        # PG 的做法是「把 manifest 当**意图合约**，逐条比对实际文件」——missing 与 extra 都报。
        # 我们原来只有手写抽样清单（人想得到的才查），漏掉的永远查不出来：
        # 今天真事就是 docs/archive/ 三份运维审计文档整整一天不在任何包里，抽样没抽到它。
        # 现在换成：**以 manifest.json 的 files 列表为准逐条比对实际 tar 条目**，
        # 抽样清单只作为「就算 manifest 没列也必须存在」的底线补充。
        mf_names = [n for n in names if n.endswith("/manifest.json")]
        declared, mf_missing = [], []
        if mf_names:
            try:
                mf = json.loads(tf_manifest_handle(pack, mf_names[0]).decode("utf-8", "ignore"))
                # files 条目形如 "docs/archive/OPS_JOURNAL.md" 或
                # "emby/library.db (sqlite backup snapshot)" / "teldrive/... (1234 B)"，
                # 统一剥掉括号备注再比对。
                _raw = mf.get("files") or []
                declared = [str(x).split(" (")[0].strip() for x in _raw if x]
                mf_missing = mf.get("files_missing") or []
            except Exception as e:                                 # noqa: BLE001
                check(False, "manifest.json 可解析", str(e)[:60])
        if mf_missing:
            check(False, "manifest 自报 files_missing 为空", "缺 %d 项: %s"
                  % (len(mf_missing), ", ".join(str(x) for x in mf_missing[:5])))
        else:
            check(True, "manifest 自报 files_missing 为空", "")
        if declared:
            absent = [d for d in declared
                      if not any(n == d or n.endswith("/" + d) for n in names)]
            check(not absent, "manifest 声明的 %d 个文件全部在包内" % len(declared),
                  "缺 %d: %s" % (len(absent), ", ".join(absent[:4])) if absent else "")

        for f in ("scripts/post_ingest.py", "scripts/autoops.py", "docs/OPS_HANDBOOK.md",
                  "scripts/gha_next3.py", "scripts/gha_finish.py", "scripts/gha_state.py",
                  "scripts/pc_upload.py", "scripts/run_one.sh", "scripts/lane_policy.py",
                  "scripts/nfo_gen.py", "scripts/lib_reconcile.py", "scripts/daily_ops.py",
                  "scripts/env_pack.py", "scripts/restore_to_new_host.sh",
                  "teldrive/teldrive_dump.sql.gz", "state/tg.db", "emby/config/system.xml",
                  # 2026-11-14：运维审计链（换机后解释历史决策的唯一证据）
                  "docs/archive/OPS_JOURNAL.md", "docs/archive/OPS1_MANUAL.md"):
            hit = any(n.endswith(f) for n in names)
            check(hit, "包内含 %s" % f, "")

        # 3.5) PG dump 结构：files/sessions 行数（决定 TG 文件能否重新对上）
        import gzip as _gz
        dump = None
        for dp, _, fs in os.walk(root):
            if "teldrive_dump.sql.gz" in fs:
                dump = os.path.join(dp, "teldrive_dump.sql.gz")
                break
        if dump:
            counts, cur = {}, None
            with _gz.open(dump, "rt", errors="replace") as f:
                for line in f:
                    if line.startswith("COPY teldrive."):
                        cur = line.split()[1].split(".")[1]
                        counts.setdefault(cur, 0)
                    elif line.strip() == "\\.":
                        cur = None
                    elif cur:
                        counts[cur] += 1
            check(counts.get("files", 0) > 1000, "PG dump files 行数", str(counts.get("files", 0)))
            check(counts.get("sessions", 0) >= 1, "PG dump sessions 行数",
                  str(counts.get("sessions", 0)))
        else:
            check(False, "PG dump 在包内", "未找到")

        # 3.6) 命门：DATA_KEY / SIGNING_KEY 必须在（丢了 TG 文件永久解不开）
        envf = None
        for dp, _, fs in os.walk(root):
            if ".env" in fs and os.path.basename(dp) == "config":
                envf = os.path.join(dp, ".env")
                break
        if envf:
            txt = open(envf, encoding="utf-8", errors="replace").read()
            for k in ("DATA_KEY", "SIGNING_KEY"):
                check(("%s=" % k) in txt, "包内 %s" % k, "")
        else:
            check(False, "包内 config/.env", "未找到")

        # 4) 配置
        for f in ("config/config.json",):
            hit = any(n.endswith(f) for n in names)
            check(hit, "包内含 %s" % f, "")
    finally:
        if a.keep:
            res["kept_at"] = tmp
            print("  保留解包目录：%s" % tmp)
        else:
            shutil.rmtree(tmp, ignore_errors=True)
    return finish(res, t0)


def finish(res, t0):
    res["dur_s"] = round(time.time() - t0, 1)
    res["verdict"] = "PASS" if res["ok"] else "FAIL"
    os.makedirs(REPORT_DIR, exist_ok=True)
    p = os.path.join(REPORT_DIR, "restore_drill_%s.md" % time.strftime("%Y%m%d"))
    md = ["# 备份恢复演练 %s —— %s" % (res["ts"], res["verdict"]), ""]
    for c in res["checks"]:
        md.append("- %s %s %s" % ("✔" if c["ok"] else "✘", c["name"], c["detail"]))
    md.append("")
    md.append("耗时 %.1fs" % res["dur_s"])
    open(p, "w", encoding="utf-8").write("\n".join(md) + "\n")
    print("\n== 演练结论：%s（%d 项，耗时 %.1fs）==" % (res["verdict"], len(res["checks"]), res["dur_s"]))
    print("  报告：%s" % p)
    if not res["ok"]:
        try:
            import alert
            alert.alert("P1", "restore_drill_failed", "备份恢复演练失败，见 %s" % p,
                        {"failed": [c["name"] for c in res["checks"] if not c["ok"]]})
        except Exception:                                          # noqa: BLE001
            pass
    return 0 if res["ok"] else 2


if __name__ == "__main__":
    sys.exit(main())
