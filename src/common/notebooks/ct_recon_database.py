# Databricks notebook source
# MAGIC %md
# MAGIC # Recon database group
# MAGIC
# MAGIC After all table tasks complete for a fixed `database + ct_head`:
# MAGIC - Verify table results PASS
# MAGIC - Publish one `recon_ready` row (`recon_id` + database + ct_head)
# MAGIC - Advance watermarks **only** on PASS
# MAGIC
# MAGIC **Connection:** mssql-python only. No Spark JDBC.

# COMMAND ----------

dbutils.widgets.text("group_id", "")
dbutils.widgets.text("client_nm", "")
dbutils.widgets.text("database_name", "")
dbutils.widgets.text("ct_head", "0")
dbutils.widgets.text("ct_watermark_before", "0")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("ingest_metadata_database", "ipac_metadata")

group_id = dbutils.widgets.get("group_id").strip()
client_nm = dbutils.widgets.get("client_nm").strip()
database_name = dbutils.widgets.get("database_name").strip()
ct_head = int(dbutils.widgets.get("ct_head") or "0")
ct_watermark_before = int(dbutils.widgets.get("ct_watermark_before") or "0")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
ingest_metadata_database = dbutils.widgets.get("ingest_metadata_database").strip()

print(
    f"recon group={group_id} client={client_nm} db={database_name} "
    f"ct={ct_watermark_before}..{ct_head} "
    f"gate={ingest_metadata_database}.dbo.recon_ready"
)

# COMMAND ----------

# TODO: complete_group(conn, group_id)

print("STUB — wire mssql-python complete_group")
dbutils.jobs.taskValues.set(key="recon_status", value="STUB")
