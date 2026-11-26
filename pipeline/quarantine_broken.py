"""把确认播不了的片子隔离成 .bad（不删字节，只从库和 Emby 里摘掉）。

判定依据必须是本地 ffprobe 明确报错（moov atom not found / Invalid data），
不是超时——超时是读得慢，见 recheck_readfail.py。

做法：读 parts → 删行 → 用「原名 + .bad」重新 register → 删 Emby 条目 → 删台账。
"""
import json
import subprocess
import sys
import urllib.parse as up
import urllib.request as ur

sys.path.insert(0, "/opt/embytg/pipeline")
import tgpipe as TP                                       # noqa: E402
from embytg_conf import cfg                               # noqa: E402

NAMES = [
    # ① 容器坏：本地 ffprobe 报 moov atom not found
    "当星光坠入花海 S01E02 - 2160p.mp4",
    "当星光坠入花海 S01E04 - 2160p.mp4",
    "当星光坠入花海 S01E07 - 2160p.mp4",
    "当星光坠入花海 S01E08 - 2160p.mp4",
    "当星光坠入花海 S01E10 - 2160p.mp4",
    "当星光坠入花海 S01E14 - 2160p.mp4",
    "当星光坠入花海 S01E16 - 2160p.mp4",
    # ② TG 层就读不出来：连自己的 session 下载都报
    #    "Request was unsuccessful 6 time(s)"，teldrive 重启也不恢复
    "黑吃黑 S03E05 - 部落 - 2160p.mp4",
    "权欲 S05E01 - 每个人都涉嫌 - 1080p.mp4",
    "权欲 S05E04 - 第二次机会 - 1080p.mp4",
    "欢乐家长群 S01E39 - 果宝找到小树许下生日愿望 - 2160p.mp4",
    "钢铁森林 S01E22 - 蒋诚锁定七叔据点 - 2160p.mp4",
    "紧急呼救 S01E03 - 近亲 - 2160p.mp4",
    "隐锋 S01E08 - 野火燎原险中生 - 2160p.mp4",
]
PSQL = ["docker", "exec", "teldrive-postgres", "psql", "-U", "teldrive", "-d", "teldrive", "-t", "-A", "-F", "|", "-c"]


def q(sql):
    r = subprocess.run(PSQL + [sql], capture_output=True, text=True)
    return [l.split("|") for l in r.stdout.strip().splitlines() if l.strip()]


def main():
    apply = "--apply" in sys.argv
    b = cfg("emby_url").rstrip("/")
    tk = cfg("emby_token")
    for nm in NAMES:
        rows = q("select id, parent_id from files where name='%s';" % nm)
        if not rows:
            print("  ? 找不到 %s" % nm[:36])
            continue
        fid, pid = rows[0]
        parts = [(int(a), int(b_)) for a, b_ in q("select message_id, plain_size from file_parts where file_id='%s' order by part_no;" % fid)]
        print("  %-40s parts=%d -> %s" % (nm[:40], len(parts), "隔离" if apply else "dry"))
        if not apply:
            continue
        subprocess.run(PSQL + ["delete from file_parts where file_id='%s'; delete from files where id='%s';" % (fid, fid)], capture_output=True)
        TP.register(pid, nm + ".bad", parts)
        # 删 Emby 条目
        qy = up.urlencode({"SearchTerm": nm.split(" S0")[0], "Recursive": "true",
                           "IncludeItemTypes": "Episode", "Fields": "Path", "api_key": tk})
        try:
            d = json.loads(ur.urlopen("%s/Items?%s" % (b, qy), timeout=30).read())
            for it in d.get("Items") or []:
                if (it.get("Path") or "").endswith(nm):
                    ur.urlopen(ur.Request("%s/Items/%s?api_key=%s" % (b, it["Id"], tk), method="DELETE"), timeout=30).read()
        except Exception as e:                            # noqa: BLE001
            print("    删条目失败:", str(e)[:40])
        import sqlite3 as _sq
        try:
            con = _sq.connect("/opt/embytg/pipeline/tg.db", timeout=30)
            cur = con.execute("delete from resource_state where rel like ?", ("%" + nm.split(" - ")[0] + "%",))
            con.commit(); con.close()
            print("    台账删除 %d 行" % cur.rowcount)
        except Exception as e:                            # noqa: BLE001
            print("    台账删除失败:", str(e)[:40])


if __name__ == "__main__":
    main()
