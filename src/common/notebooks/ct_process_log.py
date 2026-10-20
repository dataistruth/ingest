# Databricks notebook source
# MAGIC %md
# MAGIC # Write process_log for database group
# MAGIC
# MAGIC **Connection:** mssql-python to `ipac_ingest_metadata.dbo.process_log`. No JDBC.

# COMMAND ----------

dbutils.widgets.text("group_id", "")
dbutils.widgets.text("client_nm", "")
dbutils.widgets.text("database_name", "")
dbutils.widgets.text("ct_head", "0")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("ingest_metadata_database", "ipac_ingest_metadata")

group_id = dbutils.widgets.get("group_id").strip()
client_nm = dbutils.widgets.get("client_nm").strip()
database_name = dbutils.widgets.get("database_name").strip()
ct_head = dbutils.widgets.get("ct_head").strip()
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
ingest_metadata_database = dbutils.widgets.get("ingest_metadata_database").strip()

print(
    f"process_log group={group_id} client={client_nm} db={database_name} "
    f"ct_head={ct_head} table={ingest_metadata_database}.dbo.process_log"
)

# COMMAND ----------

# TODO: append process_log row using mssql-python
print("STUB — wire SQL Server process_log write")
