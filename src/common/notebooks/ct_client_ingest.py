# Databricks notebook source
# MAGIC %md
# MAGIC # Client CT ingest (runs forever)
# MAGIC
# MAGIC One notebook per client task. Every `freq_run` seconds:
# MAGIC
# MAGIC 1. mssql-python: `CHANGE_TRACKING_CURRENT_VERSION()` + pending I/U/D per table
# MAGIC 2. up to `parallel_table_run` threads: JDBC extract, then `cdc.cdc_merge`
# MAGIC    (SCD1 MERGE into Delta; `CLUSTER BY` lq_key / PK). Tables without a
# MAGIC    watermark get a full snapshot.
# MAGIC 3. advance the per-table watermark in `ipac_common_metadata`, then recon counts
# MAGIC
# MAGIC Stops only when the job run is cancelled.

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
config_dir = f"{repo_root}/config/common"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.mssql import ensure_mssql_python

ensure_mssql_python()

# COMMAND ----------

dbutils.widgets.text("client_name", "15347")
dbutils.widgets.text("freq_run", "10")
dbutils.widgets.text("parallel_table_run", "5")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("uc_schema_suffix", "_custom")
dbutils.widgets.text("ingest_metadata_database", "ipac_common_metadata")

client_name = dbutils.widgets.get("client_name").strip()
freq_run = int(dbutils.widgets.get("freq_run") or "10")
parallel_table_run = int(dbutils.widgets.get("parallel_table_run") or "5")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
uc_schema_suffix = dbutils.widgets.get("uc_schema_suffix").strip()
ingest_metadata_database = dbutils.widgets.get("ingest_metadata_database").strip()
for name, value in {
    "client_name": client_name,
    "uc_catalog": uc_catalog,
    "uc_schema_suffix": uc_schema_suffix,
    "ingest_metadata_database": ingest_metadata_database,
}.items():
    if not value:
        raise ValueError(f"{name} is required")

# COMMAND ----------

from common.ops.ingest import new_context, run_forever
from common.ops.secrets import get_sql_server_login, load_sql_server_conn
from common.ops.tables import client_database_name, ensure_uc_schema, uc_schema_name

sql_conn = load_sql_server_conn(config_dir)
login = get_sql_server_login(dbutils, sql_conn)
database_name = client_database_name(sql_conn, client_name)
source_schema = sql_conn.get("source_schema", "dbo")
target_schema = uc_schema_name(database_name, uc_schema_suffix)
if ensure_uc_schema(spark, uc_catalog, target_schema):
    print(f"created {uc_catalog}.{target_schema}")

print(
    f"client={client_name} database={database_name} target={uc_catalog}.{target_schema} "
    f"metadata={ingest_metadata_database} freq_run={freq_run}s "
    f"parallel_table_run={parallel_table_run}"
)

# COMMAND ----------

ctx = new_context(
    spark,
    login,
    client_name,
    database_name,
    source_schema,
    uc_catalog,
    uc_schema_suffix,
    ingest_metadata_database,
    config_dir,
    freq_run=freq_run,
    parallel_table_run=parallel_table_run,
)
run_forever(ctx)
