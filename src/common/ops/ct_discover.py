"""Build for_each payloads for changed databases/tables."""

import json
import uuid


def build_database_group(
    client_nm,
    database_name,
    ct_head,
    ct_watermark_before,
    changed_tables,
):
    return {
        "group_id": str(uuid.uuid4()),
        "client_nm": client_nm,
        "database_name": database_name,
        "ct_head": str(ct_head),
        "ct_watermark_before": str(ct_watermark_before),
        "changed_tables_json": json.dumps(changed_tables),
    }


def groups_to_task_value_json(groups):
    return json.dumps(groups)


def parse_changed_tables_json(raw):
    return json.loads(raw or "[]")
