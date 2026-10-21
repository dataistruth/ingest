from pathlib import Path
from types import SimpleNamespace

from common.ops import cdc, ingest
from common.ops.jdbc_extract import build_ct_change_sql

CONFIG_DIR = Path(__file__).parents[1] / "config" / "common"


def test_build_ct_change_sql():
    sql = build_ct_change_sql("dbo", "Entity", ["EntityID"], 100, 200)
    assert "CHANGETABLE(CHANGES [dbo].[Entity], 100)" in sql
    assert "ct.[EntityID] AS [__ct_EntityID]" in sql
    assert "SYS_CHANGE_VERSION <= 200" in sql
    assert "src.[EntityID] = ct.[EntityID]" in sql


def test_split_and_cluster_keys():
    keys = cdc.split_keys("A, B,C,D,E")
    assert keys == ["A", "B", "C", "D", "E"]
    assert cdc.cluster_cols(keys) == ["A", "B", "C", "D"]
    assert cdc.split_keys("") == []
    assert cdc.split_keys(None) == []


def test_quote_name_and_unique_views():
    assert cdc.quote_name("dev7.db_custom.Entity") == "`dev7`.`db_custom`.`Entity`"
    assert cdc.view_name("cdc_src") != cdc.view_name("cdc_src")


def _spark(existing):
    return SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda name: name in existing))


def _ctx(existing=()):
    ctx = ingest.new_context(
        _spark(set(existing)),
        {"server": "s", "port": 1433, "username": "u", "password": "p"},
        "15347",
        "iPC_2025_Dev7_15347",
        "dbo",
        "dev7",
        "_custom",
        "ipac_ingest_metadata",
        str(CONFIG_DIR),
    )
    ctx["src_conn"] = object()
    ctx["meta_conn"] = object()
    return ctx


def test_plan_tables_snapshot_cdc_and_skip(monkeypatch):
    tables = [
        {"table_nm": "NewT", "primary_key_cols": "ID", "lq_key": "ID", "is_active": True},
        {"table_nm": "OldT", "primary_key_cols": "ID", "lq_key": "ID", "is_active": True},
        {"table_nm": "Quiet", "primary_key_cols": "ID", "lq_key": "ID", "is_active": True},
        {"table_nm": "NoKey", "primary_key_cols": "", "lq_key": "", "is_active": True},
        {"table_nm": "AView", "primary_key_cols": "ID", "lq_key": "ID", "is_active": True},
    ]
    status = {
        name: {"type_desc": "USER_TABLE", "ct_enabled": True, "min_valid_version": 10}
        for name in ("NewT", "OldT", "Quiet", "NoKey")
    }
    status["AView"] = {"type_desc": "VIEW", "ct_enabled": False, "min_valid_version": None}
    monkeypatch.setattr(ingest, "active_tables", lambda *a: tables)
    monkeypatch.setattr(ingest, "fetch_ct_status", lambda *a: status)
    monkeypatch.setattr(ingest, "fetch_table_watermarks", lambda *a: {"OldT": 50, "Quiet": 50})

    def pending(conn, schema, table, wm, head):
        total = 3 if table == "OldT" else 0
        return {
            "pending_inserts": total, "pending_updates": 0,
            "pending_deletes": 0, "pending_total": total,
        }

    monkeypatch.setattr(ingest, "fetch_pending_ct_counts", pending)
    targets = {
        "dev7.ipc_2025_dev7_15347_custom.OldT",
        "dev7.ipc_2025_dev7_15347_custom.Quiet",
    }
    ctx = _ctx(targets)
    work = {w["table_nm"]: w for w in ingest.plan_tables(ctx, 100)}

    assert set(work) == {"NewT", "OldT"}
    assert work["NewT"]["mode"] == "snapshot"
    assert work["OldT"]["mode"] == "cdc"
    assert work["OldT"]["watermark_before"] == 50
    assert ctx["probed_head"]["Quiet"] == 100
    assert {"NoKey", "AView"} <= ctx["skip_logged"]


def test_plan_tables_stale_watermark_snapshots(monkeypatch):
    tables = [{"table_nm": "T", "primary_key_cols": "ID", "lq_key": "ID", "is_active": True}]
    status = {"T": {"type_desc": "USER_TABLE", "ct_enabled": True, "min_valid_version": 80}}
    monkeypatch.setattr(ingest, "active_tables", lambda *a: tables)
    monkeypatch.setattr(ingest, "fetch_ct_status", lambda *a: status)
    monkeypatch.setattr(ingest, "fetch_table_watermarks", lambda *a: {"T": 50})
    ctx = _ctx({"dev7.ipc_2025_dev7_15347_custom.T"})
    work = ingest.plan_tables(ctx, 100)
    assert work[0]["mode"] == "snapshot"
    assert "min_valid" in work[0]["reason"]
