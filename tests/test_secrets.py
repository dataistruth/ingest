from pathlib import Path

from common.ops.secrets import (
    get_secret,
    get_sql_server_login,
    load_sql_server_conn,
    sql_secret_keys,
)

CONFIG = Path(__file__).parents[1] / "config" / "common"


class FakeDbutils:
    class secrets:
        @staticmethod
        def get(scope: str, key: str) -> str:
            return f"{scope}:{key}:value"


def test_get_secret():
    assert (
        get_secret(FakeDbutils, "scope_ipacs_audit", "dev_sql_db_username")
        == "scope_ipacs_audit:dev_sql_db_username:value"
    )


def test_sql_server_conn_keys():
    conn = load_sql_server_conn(CONFIG)
    assert conn["secret_scope"] == "scope_ipacs_audit"
    assert conn["env"] == "dev"
    keys = sql_secret_keys(conn)
    assert keys == {
        "username": "dev_sql_db_username",
        "password": "dev_sql_db_password",
        "server": "dev_sql_server",
    }


def test_get_sql_server_login():
    conn = load_sql_server_conn(CONFIG)
    login = get_sql_server_login(FakeDbutils, conn)
    assert login["username"] == "scope_ipacs_audit:dev_sql_db_username:value"
    assert login["password"] == "scope_ipacs_audit:dev_sql_db_password:value"
    assert login["server"] == "scope_ipacs_audit:dev_sql_server:value"
    assert login["port"] == 1433
    assert conn["database_pattern"] == "iPC_2025_Dev7_{client_name}"
