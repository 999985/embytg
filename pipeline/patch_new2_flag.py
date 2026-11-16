#!/usr/bin/env python3
p = "/opt/embytg/pipeline/gha_next3.py"
s = open(p).read()
old = '    if a.lane in ("new", "new2"):\n        shard = 1 if a.lane == "new2" else 0'
new = ('    if a.lane in ("new", "new2") and os.path.exists(\n'
       '            os.path.join(HERE, ".gha_new2_enabled")):\n'
       '        shard = 1 if a.lane == "new2" else 0')
if old in s:
    s = s.replace(old, new, 1)
open(p, "w").write(s)
print("ok")
