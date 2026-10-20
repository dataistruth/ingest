# Databricks notebook source
# MAGIC %md
# MAGIC # Write process_log for database group

# COMMAND ----------

dbutils.widgets.text("group_id", "")
dbutils.widgets.text("client_nm", "")
dbutils.widgets.text("database_name", "")
dbutils.widgets.text("ct_head", "0")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata")

group_id = dbutils.widgets.get("group_id").strip()
client_nm = dbutils.widgets.get("client_nm").strip()
database_name = dbutils.widgets.get("database_name").strip()
ct_head = dbutils.widgets.get("ct_head").strip()
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
ipac_metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip()

print(
    f"process_log group={group_id} client={client_nm} db={database_name} "
    f"ct_head={ct_head} table={uc_catalog}.{ipac_metadata_schema}.process_log"
)

# COMMAND ----------

# TODO: append process_log row process_type=ingest status=SUCCESS|FAILED
print("STUB — wire process_log write")
