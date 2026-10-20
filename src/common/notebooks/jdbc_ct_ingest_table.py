# Databricks notebook source
# MAGIC %md
# MAGIC # JDBC CT ingest one table
# MAGIC
# MAGIC Extract CHANGETABLE window for fixed `ct_head`, MERGE into Delta (SCD1).

# COMMAND ----------

dbutils.widgets.text("group_id", "")
dbutils.widgets.text("client_nm", "")
dbutils.widgets.text("database_name", "")
dbutils.widgets.text("ct_head", "0")
dbutils.widgets.text("table_nm", "")
dbutils.widgets.text("watermark_before", "0")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata")
dbutils.widgets.text("dest_schema_suffix", "_raw")

group_id = dbutils.widgets.get("group_id").strip()
client_nm = dbutils.widgets.get("client_nm").strip()
database_name = dbutils.widgets.get("database_name").strip()
ct_head = int(dbutils.widgets.get("ct_head") or "0")
table_nm = dbutils.widgets.get("table_nm").strip()
watermark_before = int(dbutils.widgets.get("watermark_before") or "0")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
dest_schema_suffix = dbutils.widgets.get("dest_schema_suffix").strip()

target = f"{uc_catalog}.{client_nm}{dest_schema_suffix}.{table_nm}"
print(
    f"group={group_id} db={database_name} table={table_nm} "
    f"wm={watermark_before}..{ct_head} target={target}"
)

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.jdbc_extract import build_ct_change_sql

# TODO: resolve PK cols from common_tables.json / overrides
# TODO: load JDBC secrets from client.json scope
# TODO: read_ct_changes(...) then merge_ct_changes_scd1(...)
# TODO: write table-level result keyed by group_id + ct_head + table

sql = build_ct_change_sql("dbo", table_nm, ["Id"], watermark_before, ct_head)
print("CT extract SQL (preview):")
print(sql)
print("STUB — wire JDBC + MERGE next")
