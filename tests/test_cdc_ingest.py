import threading
from pathlib import Path
from types import SimpleNamespace

from common.ops import cdc, completeness, ingest
from common.ops.jdbc_extract import build_ct_change_sql
from common.ops.target import ordered_columns

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


def test_lq_columns_are_moved_first_case_insensitively():
    columns = ["Other1", "lq_two", "Other2", "LQ_ONE"]
    assert ordered_columns(columns, ["lq_one", "LQ_TWO"]) == [
        "LQ_ONE",
        "lq_two",
        "Other1",
        "Other2",
    ]


def test_table_status_loaded_and_ready():
    missing = completeness.table_status(None, 1, True, 0, None, None)
    assert missing["loaded"] is False
    assert missing["ready"] is False
    pending = completeness.table_status(10, 1, True, 5, None, None)
    assert pending["loaded"] is False
    stale = completeness.table_status(5, 10, True, 0, 1, 1)
    assert stale["loaded"] is False
    ok = completeness.table_status(10, 1, True, 0, 100, 100)
    assert ok["loaded"] is True
    assert ok["ready"] is True
    pending_count = completeness.table_status(10, 1, True, 0, None, None)
    assert pending_count["loaded"] is True
    assert pending_count["ready"] is False
    assert "count pending" in pending_count["reason"]
    mismatch = completeness.table_status(10, 1, True, 0, 100, 99)
    assert mismatch["loaded"] is True
    assert mismatch["ready"] is False


def test_due_full_recon_uses_3600_interval():
    ctx = {"full_recon_sec": 3600, "last_full_recon_at": None}
    assert completeness.due_full_recon(ctx, now=1.0) is True
    ctx["last_full_recon_at"] = 100.0
    assert completeness.due_full_recon(ctx, now=3700.0) is True
    assert completeness.due_full_recon(ctx, now=3699.0) is False


def test_check_one_table_cached_counts_do_not_scan_delta(monkeypatch):
    monkeypatch.setattr(
        completeness,
        "fetch_pending_ct_counts",
        lambda *a, **k: {"pending_total": 0},
    )
    counts = {"n": 0}

    class _Table:
        def count(self):
            counts["n"] += 1
            return 99

    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda name: True),
        table=lambda name: _Table(),
    )
    item = {"table_nm": "Entity", "target": "dev7.db.Entity"}
    status = {"Entity": {"ct_enabled": True, "min_valid_version": 1}}
    cached = completeness.check_one_table(
        spark, None, threading.Lock(), "dbo", item, 20, {"Entity": 10},
        status, {"Entity": {"sql_row_count": 5, "delta_row_count": 5}}, "cached",
    )
    assert cached["ready"] is True
    assert cached["sql_row_count"] == 5
    assert counts["n"] == 0
    monkeypatch.setattr(completeness, "source_table_count", lambda *a, **k: 7)
    monkeypatch.setattr(completeness, "upsert_table_counts", lambda *a, **k: None)
    full = completeness.check_one_table(
        spark, None, threading.Lock(), "dbo", item, 20, {"Entity": 10},
        status, {}, "full",
    )
    assert full["sql_row_count"] == 7
    assert full["delta_row_count"] == 99
    assert counts["n"] == 1


def test_no_lq_keeps_original_column_order():
    columns = ["ID", "Value", "Other"]
    assert ordered_columns(columns, []) == columns
    assert ordered_columns(columns, None) == columns


def test_missing_lq_column_fails_clearly():
    try:
        ordered_columns(["ID", "Value"], ["PK_ID"])
    except ValueError as exc:
        assert str(exc) == "configured LQ column(s) not found in source: PK_ID"
    else:
        raise AssertionError("missing LQ column must fail")


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
        "ipac_common_metadata",
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
        {"table_nm": "NoKey", "primary_key_cols": "", "lq_key": "ID", "is_active": True},
        {"table_nm": "NoLq", "primary_key_cols": "ID", "lq_key": "", "is_active": True},
        {"table_nm": "AView", "primary_key_cols": "ID", "lq_key": "ID", "is_active": True},
    ]
    status = {
        name: {"type_desc": "USER_TABLE", "ct_enabled": True, "min_valid_version": 10}
        for name in ("NewT", "OldT", "Quiet", "NoKey", "NoLq")
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

    assert set(work) == {"NewT", "OldT", "NoLq"}
    assert work["NewT"]["mode"] == "snapshot"
    assert work["OldT"]["mode"] == "cdc"
    assert work["OldT"]["watermark_before"] == 50
    assert work["NoLq"]["mode"] == "snapshot"
    assert work["NoLq"]["lq_keys"] == []
    assert work["NoLq"]["cluster"] == []
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
