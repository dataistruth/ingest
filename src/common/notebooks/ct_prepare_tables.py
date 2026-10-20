# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare table fan-out
# MAGIC
# MAGIC Parse `changed_tables_json` and set task value `tables` for for_each (max 5 concurrent).

# COMMAND ----------

dbutils.widgets.text("group_id", "")
dbutils.widgets.text("client_nm", "")
dbutils.widgets.text("database_name", "")
dbutils.widgets.text("ct_head", "0")
dbutils.widgets.text("changed_tables_json", "[]")

group_id = dbutils.widgets.get("group_id").strip()
client_nm = dbutils.widgets.get("client_nm").strip()
database_name = dbutils.widgets.get("database_name").strip()
ct_head = dbutils.widgets.get("ct_head").strip()
changed_tables_json = dbutils.widgets.get("changed_tables_json").strip() or "[]"

print(f"group_id={group_id} client={client_nm} db={database_name} ct_head={ct_head}")

# COMMAND ----------

import json
import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.ct_discover import parse_changed_tables_json

tables = parse_changed_tables_json(changed_tables_json)
# Normalize keys for for_each_task templates: {{input.table}}, {{input.watermark_before}}
normalized = [
    {
        "table": str(t.get("table") or t.get("table_name") or t.get("table_nm") or ""),
        "watermark_before": str(t.get("watermark_before", 0)),
        "pending_rows": str(t.get("pending_rows", 0)),
    }
    for t in tables
    if (t.get("table") or t.get("table_nm"))
]

print(f"tables for fan-out: {len(normalized)}")
for row in normalized:
    print(row)

dbutils.jobs.taskValues.set(key="tables", value=json.dumps(normalized))
