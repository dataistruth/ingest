import json

from common.ops.ct_discover import (
    ChangedTable,
    build_database_group,
    groups_to_task_value_json,
    parse_changed_tables_json,
)
from common.ops.jdbc_extract import build_ct_change_sql


def test_build_database_group_payload():
    group = build_database_group(
        client_nm="client_a",
        database_name="client_a_db",
        ct_head=65830,
        ct_watermark_before=65800,
        changed_tables=[
            ChangedTable(table="Entity", watermark_before=65800, pending_rows=12),
            ChangedTable(table="Partner", watermark_before=65800, pending_rows=3),
        ],
    )
    payload = group.to_task_value()
    assert payload["client_nm"] == "client_a"
    assert payload["ct_head"] == "65830"
    tables = json.loads(payload["changed_tables_json"])
    assert len(tables) == 2
    assert tables[0]["table"] == "Entity"


def test_groups_to_task_value_json_empty():
    assert groups_to_task_value_json([]) == "[]"


def test_parse_changed_tables_json():
    raw = '[{"table":"Entity","watermark_before":1}]'
    rows = parse_changed_tables_json(raw)
    assert rows[0]["table"] == "Entity"


def test_build_ct_change_sql_window():
    sql = build_ct_change_sql("dbo", "Entity", ["EntityId"], 100, 200)
    assert "CHANGETABLE(CHANGES [dbo].[Entity], 100)" in sql
    assert "SYS_CHANGE_VERSION <= 200" in sql
    assert "src.[EntityId] = ct.[EntityId]" in sql
