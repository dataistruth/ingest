# Databricks notebook source
# MAGIC %md
# MAGIC # Recon database group
# MAGIC
# MAGIC After all table tasks complete for a fixed `database + ct_head`:
# MAGIC - Verify table results PASS
# MAGIC - Publish one `recon_ready` row (`recon_id` + database + ct_head)
# MAGIC - Advance watermarks **only** on PASS

# COMMAND ----------

dbutils.widgets.text("group_id", "")
dbutils.widgets.text("client_nm", "")
dbutils.widgets.text("database_name", "")
dbutils.widgets.text("ct_head", "0")
dbutils.widgets.text("ct_watermark_before", "0")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("ipac_metadata_schema", "ipac_metadata")

group_id = dbutils.widgets.get("group_id").strip()
client_nm = dbutils.widgets.get("client_nm").strip()
database_name = dbutils.widgets.get("database_name").strip()
ct_head = int(dbutils.widgets.get("ct_head") or "0")
ct_watermark_before = int(dbutils.widgets.get("ct_watermark_before") or "0")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
ipac_metadata_schema = dbutils.widgets.get("ipac_metadata_schema").strip()

print(
    f"recon group={group_id} client={client_nm} db={database_name} "
    f"ct={ct_watermark_before}..{ct_head} "
    f"gate={uc_catalog}.{ipac_metadata_schema}.recon_ready"
)

# COMMAND ----------

# TODO: read per-table results for group_id + ct_head
# TODO: if all PASS → INSERT recon_ready (ready_for_calc=true)
# TODO: advance ct_table_watermark / ct_db_watermark to ct_head
# TODO: if any FAIL → no recon_ready, no watermark advance

print("STUB — wire recon_ready publish + watermark advance")
dbutils.jobs.taskValues.set(key="recon_status", value="STUB")
