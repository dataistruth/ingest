from pathlib import Path

from common.ops.secrets import load_sql_server_conn
from common.ops.tables import (
    active_clients,
    active_tables,
    client_database_name,
    load_clients,
    load_tables,
    uc_schema_name,
    uc_table_path,
)

CONFIG = Path(__file__).parents[1] / "config" / "common"


def test_load_tables():
    tables = load_tables(CONFIG)
    names = [t["table_nm"] for t in tables]
    assert "Entity" in names
    assert "Form926Package" in names
    assert "ALLENTITY" not in names
    assert all("recon_type" in t for t in tables)
    assert "YearlyByInvestment" in names
    assert names == list(dict.fromkeys(names))


def test_client_override_disables_extra_tables():
    all_rows = load_tables(CONFIG, "15347")
    by_name = {t["table_nm"]: t for t in all_rows}
    assert by_name["ALLENTITY"]["is_active"] is False
    assert by_name["VW_DF_Financial_Archive"]["is_active"] is False
    active_names = [t["table_nm"] for t in active_tables(CONFIG, "15347")]
    assert "Entity" in active_names
    assert "ALLENTITY" not in active_names
    assert "DF_DEALS_ARCHIVE" not in active_names


def test_load_clients_defaults_job_group():
    clients = load_clients(CONFIG)
    assert [c["client_name"] for c in clients] == ["15347", "15447"]
    assert all(c["is_active"] for c in clients)
    assert all(c["job_group"] == 1 for c in clients)


def test_client_database_name_from_pattern():
    conn = load_sql_server_conn(CONFIG)
    assert client_database_name(conn, "15347") == "iPC_2025_Dev7_15347"
    assert [c["client_name"] for c in active_clients(CONFIG)] == ["15347", "15447"]


def test_uc_schema_is_lower_database_plus_suffix():
    assert uc_schema_name("iPC_2025_Dev7_15347", "_custom") == "ipc_2025_dev7_15347_custom"
    assert (
        uc_table_path("dev7", "iPC_2025_Dev7_15447", "_custom", "Entity")
        == "dev7.ipc_2025_dev7_15447_custom.Entity"
    )
