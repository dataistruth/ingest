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
dbutils.widgets.text("ingest_metadata_database", "")
dbutils.widgets.text("uc_schema_suffix", "")

poll_interval_sec = int(dbutils.widgets.get("poll_interval_sec") or "10")
uc_catalog = dbutils.widgets.get("uc_catalog").strip()
ingest_metadata_database = dbutils.widgets.get("ingest_metadata_database").strip()
uc_schema_suffix = dbutils.widgets.get("uc_schema_suffix").strip()

print(
    f"uc_catalog={uc_catalog} uc_schema_suffix={uc_schema_suffix} "
    f"sql_metadata_db={ingest_metadata_database} poll={poll_interval_sec}s"
)

# COMMAND ----------

import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
config_dir = f"{repo_root}/config/common"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.ops.ct_discover import build_database_group, groups_to_task_value_json
from common.ops.ct_source import (
    fetch_ct_head,
    fetch_ct_status,
    fetch_pending_ct_counts,
    fetch_table_watermark,
)
from common.ops.ingest_metadata import create_group
from common.ops.mssql import connect_sql_server, sql_server_odbc
from common.ops.secrets import get_sql_server_login, load_sql_server_conn, sql_secret_keys
from common.ops.tables import active_clients, active_tables, client_database_name, load_tables

sql_conn = load_sql_server_conn(config_dir)
schema = sql_conn.get("source_schema") or "dbo"
clients = active_clients(config_dir)
common_count = len(load_tables(config_dir))
keys = sql_secret_keys(sql_conn)

print(f"env={sql_conn['env']} scope={sql_conn['secret_scope']}")
print(f"secret keys: {keys['username']}, {keys['password']}, {keys['server']}")
print(f"database_pattern={sql_conn.get('database_pattern')}")
print(f"active clients: {len(clients)}")
for client in clients:
    print(f"  {client['client_name']} -> {client_database_name(sql_conn, client['client_name'])}")
print(f"common tables: {common_count}")
print(f"now looking for CT of {len(clients)} databases")

login = get_sql_server_login(dbutils, sql_conn)
print(f"secrets loaded; server={login['server']} port={login['port']}")

meta_conn = None
try:
    meta_conn = connect_sql_server(sql_server_odbc(login, ingest_metadata_database))
    print(f"connection succeeded: metadata database {ingest_metadata_database}")
except Exception as exc:
    print(f"metadata database {ingest_metadata_database} not reachable: {exc}")

groups = []
for client in clients:
    client_name = client["client_name"]
    database_name = client_database_name(sql_conn, client_name)
    print(f"connecting to client DB {database_name} ...")
    try:
        src_conn = connect_sql_server(sql_server_odbc(login, database_name))
    except Exception as exc:
        print(f"connection failed: {database_name}: {exc}")
        continue
    print(f"connection succeeded: {database_name}")
    try:
        ct_head = fetch_ct_head(src_conn)
        print(f"  ct_head={ct_head}")
        if ct_head is None:
            print(f"  change tracking not enabled on {database_name}; skip")
            continue
        tables = active_tables(config_dir, client_name)
        print(f"  active tables for {client_name}: {len(tables)}")
        changed = []
        skipped_not_table = []
        skipped_no_ct = []
        skipped_missing = []
        ct_status = fetch_ct_status(src_conn, schema)
        for table in tables:
            table_nm = table["table_nm"]
            status = ct_status.get(table_nm)
            if status is None:
                skipped_missing.append(table_nm)
                continue
            if status["type_desc"] != "USER_TABLE":
                skipped_not_table.append(table_nm)
                continue
            if not status["ct_enabled"]:
                skipped_no_ct.append(table_nm)
                continue
            watermark = 0
            if meta_conn is not None:
                try:
                    watermark = fetch_table_watermark(
                        meta_conn, database_name, schema, table_nm
                    )
                except Exception as exc:
                    print(f"  watermark read failed {table_nm}: {exc}")
                    watermark = 0
            try:
                pending = fetch_pending_ct_counts(
                    src_conn, schema, table_nm, watermark, ct_head
                )
            except Exception as exc:
                print(f"  CT lookup failed {schema}.{table_nm}: {exc}")
                continue
            if pending["pending_total"] > 0:
                print(
                    f"  {table_nm}: pending={pending['pending_total']} "
                    f"(I={pending['pending_inserts']} U={pending['pending_updates']} "
                    f"D={pending['pending_deletes']}) wm={watermark}..{ct_head}"
                )
                changed.append(pending)
        if skipped_not_table:
            print(f"  skip (view/synonym, CT not allowed): {', '.join(skipped_not_table)}")
        if skipped_no_ct:
            print(f"  skip (CT not enabled): {', '.join(skipped_no_ct)}")
        if skipped_missing:
            print(f"  skip (missing object): {', '.join(skipped_missing)}")
        print(f"  {database_name}: {len(changed)} tables with pending CT")
        if not changed:
            continue
        group_id = None
        if meta_conn is not None:
            try:
                group_id = create_group(
                    meta_conn,
                    client_name,
                    database_name,
                    schema,
                    min(int(t["watermark_before"]) for t in changed),
                    ct_head,
                    changed,
                )
                print(f"  ingest_group {group_id}")
            except Exception as exc:
                print(f"  create_group skipped: {exc}")
        group = build_database_group(
            client_nm=client_name,
            database_name=database_name,
            ct_head=ct_head,
            ct_watermark_before=min(int(t["watermark_before"]) for t in changed),
            changed_tables=[
                {
                    "table": t["table_name"],
                    "watermark_before": t["watermark_before"],
                    "pending_rows": t["pending_total"],
                }
                for t in changed
            ],
        )
        if group_id:
            group["group_id"] = group_id
        groups.append(group)
    finally:
        src_conn.close()

if meta_conn is not None:
    meta_conn.close()

payload = groups_to_task_value_json(groups)
print(f"changed database groups: {len(groups)}")
print(payload)

dbutils.jobs.taskValues.set(key="changed_databases", value=payload)
