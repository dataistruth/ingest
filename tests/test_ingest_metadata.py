from pathlib import Path

from common.ops.ingest_metadata import compare_counts
from common.ops.mssql import connect_sql_server


def test_compare_counts_type_2():
    ok = compare_counts(2, 10, 10, 100, 100)
    assert ok["status"] == "PASS"
    bad = compare_counts(2, 10, 9, 100, 99)
    assert bad["status"] == "FAIL"


def test_compare_counts_type_1_skips_match():
    assert compare_counts(1, 1, 2)["status"] == "PASS"


def test_connect_sql_server_is_function():
    assert callable(connect_sql_server)


def test_sql_server_ddl_set_exists():
    root = Path(__file__).parents[1] / "src" / "sql" / "sql_server"
    names = sorted(path.name for path in root.glob("*.sql"))
    assert "001_create_database.sql" in names
    assert "002_ingest_metadata_tables.sql" in names
