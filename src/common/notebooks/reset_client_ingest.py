# Databricks notebook source
# MAGIC %md
# MAGIC # Reset one client ingest
# MAGIC
# MAGIC Stop the continuous ingest task before running this notebook.
# MAGIC The default is a dry run. To execute, set `dry_run=false` and enter
# MAGIC `RESET_<client_name>` in `confirmation`.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("client_name", "15347")
dbutils.widgets.text("uc_catalog", "dev7")
dbutils.widgets.text("uc_schema_suffix", "_custom")
dbutils.widgets.text("ingest_metadata_database", "ipac_common_metadata")
dbutils.widgets.dropdown("drop_targets", "true", ["true", "false"])
dbutils.widgets.dropdown("reset_metadata", "true", ["true", "false"])
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("confirmation", "")

client_name = dbutils.widgets.get("client_name").strip()
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
uc_schema_suffix = dbutils.widgets.get("uc_schema_suffix").strip()
ingest_metadata_database = dbutils.widgets.get("ingest_metadata_database").strip()
drop_targets = dbutils.widgets.get("drop_targets") == "true"
reset_metadata = dbutils.widgets.get("reset_metadata") == "true"
dry_run = dbutils.widgets.get("dry_run") == "true"
confirmation = dbutils.widgets.get("confirmation").strip()

for name, value in {
    "client_name": client_name,
    "uc_catalog": uc_catalog,
    "uc_schema_suffix": uc_schema_suffix,
    "ingest_metadata_database": ingest_metadata_database,
}.items():
    if not value:
        raise ValueError(f"{name} is required")

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
config_dir = f"{repo_root}/config/common"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

import importlib

from common.ops import reset as reset_mod
from common.ops import target as target_mod
from common.ops.mssql import connect_sql_server, ensure_mssql_python, sql_server_odbc
from common.ops.secrets import get_sql_server_login, load_sql_server_conn
from common.ops.tables import client_database_name, load_tables, uc_table_path

importlib.reload(reset_mod)
importlib.reload(target_mod)
reset_client_metadata = reset_mod.reset_client_metadata
drop_target_tables = target_mod.drop_target_tables

ensure_mssql_python()
sql_conn = load_sql_server_conn(config_dir)
login = get_sql_server_login(dbutils, sql_conn)
database_name = client_database_name(sql_conn, client_name)
targets = [
    uc_table_path(
        uc_catalog,
        database_name,
        uc_schema_suffix,
        table["table_nm"],
    )
    for table in load_tables(config_dir, client_name)
    if table.get("table_nm")
]

print(f"client={client_name} database={database_name}")
print(f"drop_targets={drop_targets} ({len(targets)} configured tables)")
print(f"reset_metadata={reset_metadata} metadata_database={ingest_metadata_database}")
print("The continuous ingest task must be stopped before reset.")

if dry_run:
    print("DRY RUN: no changes made")
    dbutils.notebook.exit("DRY RUN")

expected = f"RESET_{client_name}"
if confirmation != expected:
    raise ValueError(f"confirmation must be exactly {expected}")

# COMMAND ----------

if drop_targets:
    print("Unity Catalog target reset SQL:")
    dropped = drop_target_tables(spark, targets)
    print(f"dropped {len(dropped)} configured target tables")

if reset_metadata:
    print("SQL Server metadata reset SQL:")
    conn = connect_sql_server(sql_server_odbc(login, ingest_metadata_database))
    try:
        deleted = reset_client_metadata(conn, database_name)
    finally:
        conn.close()
    print(f"metadata reset: {deleted}")

print(f"reset complete for {client_name}; restart the continuous ingest task")
