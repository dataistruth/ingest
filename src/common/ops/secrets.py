"""Read Databricks secrets for SQL Server."""

import json
from pathlib import Path


def get_secret(dbutils, scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def load_sql_server_conn(config_dir):
    with open(Path(config_dir) / "sql_server_conn.json", encoding="utf-8") as fh:
        return json.load(fh)


def sql_secret_keys(conn, env=None):
    env = env or conn["env"]
    return {
        "username": conn["username_key"].format(env=env),
        "password": conn["password_key"].format(env=env),
        "server": conn["server_key"].format(env=env),
    }


def get_sql_server_login(dbutils, conn, env=None):
    keys = sql_secret_keys(conn, env)
    scope = conn["secret_scope"]
    return {
        "username": get_secret(dbutils, scope, keys["username"]),
        "password": get_secret(dbutils, scope, keys["password"]),
        "server": get_secret(dbutils, scope, keys["server"]),
        "port": int(conn.get("port", 1433)),
    }
