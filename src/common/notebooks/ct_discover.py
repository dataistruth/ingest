# Databricks notebook source
# MAGIC %md
# MAGIC # CT discover
# MAGIC
# MAGIC Snapshot `ct_head` per active client DB, find tables with pending CT, emit
# MAGIC `changed_databases` task value for for_each fan-out.
# MAGIC
# MAGIC **Connection:** mssql-python only (CT head, CHANGETABLE counts, watermarks).
# MAGIC Spark JDBC is **not** used here.

# COMMAND ----------

dbutils.widgets.text("poll_interval_sec", "10")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("ingest_metadata_database", "ipac_ingest_metadata")
dbutils.widgets.text("dest_schema_suffix", "_raw")

poll_interval_sec = int(dbutils.widgets.get("poll_interval_sec") or "10")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
ingest_metadata_database = dbutils.widgets.get("ingest_metadata_database").strip()
dest_schema_suffix = dbutils.widgets.get("dest_schema_suffix").strip()

print(
    f"uc_catalog={uc_catalog} "
    f"sql_metadata_db={ingest_metadata_database} poll={poll_interval_sec}s"
)

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

# TODO: load_clients + load_tables from config/common
# TODO: login = get_sql_server_login(dbutils, load_sql_server_conn(config_dir))
# TODO: conn = connect_sql_server(conn_str)
# TODO: fetch_ct_head(conn)
# TODO: fetch_pending_ct_counts(...) then create_group(conn, ...)
# Stub: empty fan-out until wired to SQL Server.

groups = []
payload = groups_to_task_value_json(groups)
print(f"changed database groups: {len(groups)}")
print(payload)

dbutils.jobs.taskValues.set(key="changed_databases", value=payload)
