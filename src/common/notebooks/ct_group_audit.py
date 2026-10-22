# Databricks notebook source
# MAGIC %md
# MAGIC # Group completeness audit (runs forever)
# MAGIC
# MAGIC One task for the whole `job_group`. Every `freq_run` seconds, for each active
# MAGIC client in the group:
# MAGIC
# MAGIC 1. Read `CHANGE_TRACKING_CURRENT_VERSION()` (detected CT head)
# MAGIC 2. Count active tables that are loaded (watermark + no pending CT + target exists)
# MAGIC 3. Compare SQL `COUNT(*)` vs Delta `COUNT(*)` on loaded tables
# MAGIC 4. Publish READY / NOT_READY with elapsed time
# MAGIC
# MAGIC Results go to the job log, `dbo.ingest_completeness` (latest row per database),
# MAGIC and `dbo.process_log`. Stops only when the job run is cancelled.

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

dbutils.widgets.text("job_group", "1")
dbutils.widgets.text("freq_run", "10")
dbutils.widgets.text("parallel_table_run", "5")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("uc_schema_suffix", "_custom")
dbutils.widgets.text("ingest_metadata_database", "ipac_common_metdata")

job_group = int(dbutils.widgets.get("job_group") or "1")
freq_run = int(dbutils.widgets.get("freq_run") or "10")
parallel_table_run = int(dbutils.widgets.get("parallel_table_run") or "5")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
uc_schema_suffix = dbutils.widgets.get("uc_schema_suffix").strip()
ingest_metadata_database = dbutils.widgets.get("ingest_metadata_database").strip()
for name, value in {
    "uc_catalog": uc_catalog,
    "uc_schema_suffix": uc_schema_suffix,
    "ingest_metadata_database": ingest_metadata_database,
}.items():
    if not value:
        raise ValueError(f"{name} is required")

# COMMAND ----------

from common.ops.completeness import new_audit_context, run_forever
from common.ops.secrets import get_sql_server_login, load_sql_server_conn
from common.ops.tables import active_clients

sql_conn = load_sql_server_conn(config_dir)
login = get_sql_server_login(dbutils, sql_conn)
clients = active_clients(config_dir, job_group)
print(
    f"job_group={job_group} clients={[c['client_name'] for c in clients]} "
    f"freq_run={freq_run}s parallel_table_run={parallel_table_run} "
    f"catalog={uc_catalog} suffix={uc_schema_suffix} "
    f"metadata={ingest_metadata_database}"
)

# COMMAND ----------

ctx = new_audit_context(
    spark,
    login,
    sql_conn,
    uc_catalog,
    uc_schema_suffix,
    ingest_metadata_database,
    config_dir,
    job_group=job_group,
    freq_run=freq_run,
    parallel_table_run=parallel_table_run,
)
run_forever(ctx)
