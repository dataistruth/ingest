# Databricks notebook source
# MAGIC %md
# MAGIC # JDBC CT discover
# MAGIC
# MAGIC Snapshot `ct_head` per active client DB, find tables with pending CT, emit
# MAGIC `changed_databases` task value for for_each fan-out.

# COMMAND ----------

dbutils.widgets.text("poll_interval_sec", "10")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata")
dbutils.widgets.text("dest_schema_suffix", "_raw")

poll_interval_sec = int(dbutils.widgets.get("poll_interval_sec") or "10")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
ipac_metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip()
dest_schema_suffix = dbutils.widgets.get("dest_schema_suffix").strip()

print(f"uc_catalog={uc_catalog} metadata={ipac_metadata_schema} poll={poll_interval_sec}s")

# COMMAND ----------

import json
import sys
from pathlib import Path

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.ct_discover import build_database_group, groups_to_task_value_json

# TODO: load config/common/client.json + common_tables.json
# TODO: for each active client, connect via JDBC / mssql-python
# TODO: CHANGE_TRACKING_CURRENT_VERSION() once per DB → ct_head
# TODO: CHANGETABLE vs ct_table_watermark → ChangedTable list
# Stub: empty fan-out until wired to SQL Server.

groups = []
payload = groups_to_task_value_json(groups)
print(f"changed database groups: {len(groups)}")
print(payload)

dbutils.jobs.taskValues.set(key="changed_databases", value=payload)
