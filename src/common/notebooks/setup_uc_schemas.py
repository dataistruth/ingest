# Databricks notebook source
# MAGIC %md
# MAGIC # One-time UC schema setup
# MAGIC
# MAGIC Reads active clients from `config/common/client.json` and creates
# MAGIC `{uc_catalog}.{lower(database_name)}{uc_schema_suffix}` if it does not exist.
# MAGIC
# MAGIC Pass `uc_catalog` and `uc_schema_suffix` from the job (`databricks.yml`).

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "")
dbutils.widgets.text("uc_schema_suffix", "")

uc_catalog = dbutils.widgets.get("uc_catalog").strip()
uc_schema_suffix = dbutils.widgets.get("uc_schema_suffix").strip()
if not uc_catalog:
    raise ValueError("uc_catalog is required")
if not uc_schema_suffix:
    raise ValueError("uc_schema_suffix is required")

print(f"uc_catalog={uc_catalog} uc_schema_suffix={uc_schema_suffix}")

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
config_dir = f"{repo_root}/config/common"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.secrets import load_sql_server_conn
from common.ops.tables import (
    active_clients,
    client_database_name,
    ensure_uc_schema,
    uc_schema_exists,
    uc_schema_name,
)

sql_conn = load_sql_server_conn(config_dir)
clients = active_clients(config_dir)
print(f"active clients: {len(clients)}")

for client in clients:
    client_name = client["client_name"]
    database_name = client_database_name(sql_conn, client_name)
    schema_name = uc_schema_name(database_name, uc_schema_suffix)
    full_name = f"{uc_catalog}.{schema_name}"
    if uc_schema_exists(spark, uc_catalog, schema_name):
        print(f"  {client_name}: {full_name} already exists")
        continue
    ensure_uc_schema(spark, uc_catalog, schema_name)
    print(f"  {client_name}: created {full_name}")

print("UC schema setup done")
