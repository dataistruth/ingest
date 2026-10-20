import json

from common.ops.ct_discover import (
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
            {"table": "Entity", "watermark_before": 65800, "pending_rows": 12},
        ],
    )
    assert group["client_nm"] == "client_a"
    assert group["ct_head"] == "65830"
    assert json.loads(group["changed_tables_json"])[0]["table"] == "Entity"


def test_groups_to_task_value_json_empty():
    assert groups_to_task_value_json([]) == "[]"


def test_parse_changed_tables_json():
    rows = parse_changed_tables_json('[{"table":"Entity","watermark_before":1}]')
    assert rows[0]["table"] == "Entity"


def test_build_ct_change_sql_window():
    sql = build_ct_change_sql("dbo", "Entity", ["EntityId"], 100, 200)
    assert "CHANGETABLE(CHANGES [dbo].[Entity], 100)" in sql
    assert "SYS_CHANGE_VERSION <= 200" in sql
