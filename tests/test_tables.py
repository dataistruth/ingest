from pathlib import Path

from common.ops.tables import load_clients, load_tables

CONFIG = Path(__file__).parents[1] / "config" / "common"


def test_load_tables():
    tables = load_tables(CONFIG)
    names = [t["table_nm"] for t in tables]
    assert "Entity" in names
    assert "Form926Package" in names
    assert all("recon_type" in t for t in tables)
    assert "YearlyByInvestment" in names
    assert names == list(dict.fromkeys(names))


def test_load_clients_defaults_job_group():
    clients = load_clients(CONFIG)
    assert [c["client_name"] for c in clients] == [
        "ipc_2025_dev7_15347",
        "ipc_2025_dev7_15353",
    ]
    assert all(c["is_active"] for c in clients)
    assert all(c["job_group"] == 1 for c in clients)
