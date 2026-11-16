#!/usr/bin/env python3
import json
import subprocess
import sys

sys.path.insert(0, "/opt/embytg/pipeline")
import resource_state as RS


def psql(sql):
    p = subprocess.run(
        ["docker", "exec", "-i", "teldrive-postgres", "psql",
         "-U", "teldrive", "-d", "teldrive", "-AtF", "|"],
        input=sql.encode("utf-8"), stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=True,
    )
    return p.stdout.decode("utf-8", "replace")


nodes = RS.td_tree()
paths = RS.build_paths(nodes)
for nid, path in sorted(paths.items(), key=lambda kv: kv[1]):
    if "当星光坠入花海" not in path or not path.lower().endswith((".mp4", ".mkv")):
        continue
    node = nodes[nid]
    parts = psql(
        "select part_no,channel_id,message_id,plain_size,stored_size "
        "from file_parts where file_id='%s' order by part_no;" % nid
    ).strip()
    print(json.dumps({"path": path, "id": nid, "size": node["size"],
                      "parts": parts}, ensure_ascii=False))
