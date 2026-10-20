"""Discover SQL Server Change Tracking groups for JDBC fan-out.

One group = one source database + one fixed ct_head + changed tables.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class ChangedTable:
    table: str
    watermark_before: int
    pending_rows: int = 0


@dataclass
class DatabaseGroup:
    group_id: str
    client_nm: str
    database_name: str
    ct_head: int
    ct_watermark_before: int
    changed_tables_json: str

    def to_task_value(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "client_nm": self.client_nm,
            "database_name": self.database_name,
            "ct_head": str(self.ct_head),
            "ct_watermark_before": str(self.ct_watermark_before),
            "changed_tables_json": self.changed_tables_json,
        }


def build_database_group(
    client_nm: str,
    database_name: str,
    ct_head: int,
    ct_watermark_before: int,
    changed_tables: list[ChangedTable],
) -> DatabaseGroup:
    tables_payload = [
        {
            "table": t.table,
            "watermark_before": t.watermark_before,
            "pending_rows": t.pending_rows,
        }
        for t in changed_tables
    ]
    return DatabaseGroup(
        group_id=str(uuid.uuid4()),
        client_nm=client_nm,
        database_name=database_name,
        ct_head=ct_head,
        ct_watermark_before=ct_watermark_before,
        changed_tables_json=json.dumps(tables_payload),
    )


def groups_to_task_value_json(groups: list[DatabaseGroup]) -> str:
    """JSON array for Databricks for_each_task inputs."""
    return json.dumps([g.to_task_value() for g in groups])


def parse_changed_tables_json(raw: str) -> list[dict[str, Any]]:
    data = json.loads(raw or "[]")
    if not isinstance(data, list):
        raise ValueError("changed_tables_json must be a JSON array")
    return data
