"""Load the shared table list and client list."""

import json
from pathlib import Path


def load_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def load_tables(config_dir):
    data = load_json(Path(config_dir) / "common_tables.json")
    if isinstance(data, list):
        return list(data)
    return list(data.get("tables") or [])


def load_clients(config_dir):
    rows = load_json(Path(config_dir) / "client.json")
    return [
        {
            "client_name": row["client_name"],
            "is_active": bool(row.get("is_active", True)),
            "job_group": int(row.get("job_group", 1)),
        }
        for row in rows
    ]
