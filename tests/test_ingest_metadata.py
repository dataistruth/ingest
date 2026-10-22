from pathlib import Path

from common.ops.mssql import (
    connect_sql_server,
    ensure_mssql_python,
    sql_server_jdbc_url,
    sql_server_odbc,
)
from common.ops.recon import compare_counts


def test_compare_counts():
    assert compare_counts(10, 10, 100, 100)["status"] == "PASS"
    assert compare_counts(10, 9, 100, 99)["status"] == "FAIL"
    assert compare_counts(10, 10, 100, 99)["count_match"] is False
    assert compare_counts(10, 10)["status"] == "PASS"


def test_sql_server_jdbc_url():
    url = sql_server_jdbc_url({"server": "sql.example", "port": 1433}, "db1")
    assert url.startswith("jdbc:sqlserver://sql.example:1433;databaseName=db1")


def test_connect_sql_server_is_function():
    assert callable(connect_sql_server)
    assert callable(ensure_mssql_python)


def test_sql_server_odbc_omits_driver():
    conn = sql_server_odbc(
        {"server": "sql.example", "port": 1433, "username": "u", "password": "p"},
        "iPC_2025_Dev7_15347",
    )
    assert "Driver=" not in conn
    assert "Server=sql.example,1433" in conn
    assert "Database=iPC_2025_Dev7_15347" in conn


def test_sql_server_ddl_set_exists():
    root = Path(__file__).parents[1] / "src" / "sql" / "sql_server"
    names = sorted(path.name for path in root.glob("*.sql"))
    assert "01_create_database.sql" in names
    assert "02_ingest_runtime.sql" in names
    assert "03_watermark_completeness.sql" in names
    assert "04_grants.sql" in names
    assert "05_watermark_count_cache.sql" in names
    assert "004_ingest_metadata_procedures.sql" not in names
