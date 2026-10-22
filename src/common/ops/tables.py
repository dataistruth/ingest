"""Load the shared table list, client list, and per-client overrides."""

import json
from pathlib import Path


def load_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


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


def active_clients(config_dir, job_group=None):
    clients = [c for c in load_clients(config_dir) if c["is_active"]]
    if job_group is None:
        return clients
    group = int(job_group)
    return [c for c in clients if c["job_group"] == group]


def client_database_name(conn, client_name):
    pattern = conn.get("database_pattern") or "{client_name}"
    return pattern.format(env=conn.get("env", ""), client_name=client_name)


def uc_schema_name(database_name, suffix=""):
    return f"{str(database_name).lower()}{suffix or ''}"


def uc_table_path(catalog, database_name, suffix, table_nm):
    return f"{catalog}.{uc_schema_name(database_name, suffix)}.{table_nm}"


def uc_schema_exists(spark, catalog, schema_name):
    rows = spark.sql(
        f"""
        SELECT 1
        FROM system.information_schema.schemata
        WHERE catalog_name = '{catalog}'
          AND schema_name = '{schema_name}'
        """
    ).collect()
    return bool(rows)


def ensure_uc_schema(spark, catalog, schema_name):
    if uc_schema_exists(spark, catalog, schema_name):
        return False
    spark.sql(f"CREATE SCHEMA `{catalog}`.`{schema_name}`")
    return True


def _row(item):
    if isinstance(item, str):
        return {"table_nm": item, "is_active": True}
    row = dict(item)
    row["table_nm"] = row.get("table_nm") or row.get("table") or row.get("table_name")
    row.setdefault("is_active", True)
    return row


def load_tables(config_dir, client_name=None):
    data = load_json(Path(config_dir) / "common_tables.json")
    common = data if isinstance(data, list) else list(data.get("tables") or [])
    by_name = {r["table_nm"]: r for r in (_row(t) for t in common)}

    if client_name:
        override_path = Path(config_dir) / "client_overrides" / f"{client_name}.json"
        if override_path.exists():
            override = load_json(override_path)
            if not override.get("include_common", True):
                by_name = {}
            for item in override.get("ignore") or []:
                name = item["table_nm"] if isinstance(item, dict) else item
                if name in by_name:
                    by_name[name]["is_active"] = False
            for item in override.get("extra") or []:
                row = _row(item)
                by_name[row["table_nm"]] = row

    return list(by_name.values())


def active_tables(config_dir, client_name=None):
    return [t for t in load_tables(config_dir, client_name) if t.get("is_active", True)]
