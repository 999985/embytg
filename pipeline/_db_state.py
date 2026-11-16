#!/usr/bin/env python3
import sqlite3
import sys

db = "/opt/embytg/pipeline/tg.db"
c = sqlite3.connect(db, timeout=60)
if "--checkpoint" in sys.argv:
    print("checkpoint", c.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall())
print("state_key", c.execute("select count(*) from state_key").fetchone()[0])
print("pending", c.execute("select count(*) from pending").fetchone()[0])
c.close()
