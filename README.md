# ingest

JDBC + SQL Server **Change Tracking (CT)** ingestion for iPAC.

**Connections:** Spark JDBC is used only to extract change **payload** for MERGE.
CT head, pending counts, COUNT(*), watermarks, and audit writes use **mssql-python**.

One **continuous** job per client `job_group`. Each client is a parallel notebook task
(`ct_client_ingest.py`) that loops forever on a shared job cluster: poll CT every
`freq_run` seconds, then snapshot or SCD1-MERGE changed tables in `parallel_table_run`
threads. No task hand-off, no fan-out.

## Flow

```
j_ipac_ingest_group_1 (continuous)
  ├── ingest_15347 ─┐
  ├── ingest_15447 ─┼─ every freq_run s: CT poll + snapshot/MERGE
  └── audit_group_1 ┘ every freq_run s: both clients
                         ct_head, tables loaded, SQL vs Delta count,
                         duration, READY / NOT_READY
                         → dbo.ingest_completeness + process_log
```

See [docs/JOB_FLOW.md](docs/JOB_FLOW.md).

## Layout

```
config/common/          client + table registry
resources/jobs/         Databricks job YAML
src/common/notebooks/   Job notebook tasks
src/common/ops/         ingest loop, CDC merge, recon, JDBC extract, metadata
docs/                   Design notes
```

## Onboard a client

1. Add `{ "client_name", "is_active", "job_group": 1 }` to `config/common/client.json`
2. Shared CT tables live in `config/common/common_tables.json`
3. Optional `config/common/client_overrides/<client_name>.json` (`ignore` / `extra`, including disabled tables)
4. Ensure CT is enabled on PK tables and grants exist
5. SQL Server login is `config/common/sql_server_conn.json` (`scope_ipacs_audit` / `{env}_sql_db_username`, `{env}_sql_db_password`, `{env}_sql_server`)
6. Run `src/common/notebooks/setup_uc_schemas.py` once (creates `{catalog}.{lower(database)}{uc_schema_suffix}`)
7. Add a task for the client to `resources/jobs/ingest_group_jobs.yml` (same `job_group`)
8. Deploy bundle and unpause `j_ipac_ingest_group_1`

## Reset a client

Stop the client's continuous task, then run
`src/common/notebooks/reset_client_ingest.py`. It defaults to `dry_run=true`.
For an actual reset, use `dry_run=false` and set `confirmation` to
`RESET_<client_name>`. The notebook can drop all configured client target tables
and delete only that source database's metadata and watermarks. Restarting the
task then performs a clean snapshot.

At initial target creation, configured LQ columns are moved to the beginning of
the Delta schema so they stay within Delta's first 32 statistics columns. If
`lq_key` is blank, the table is created with no liquid clustering. A missing
configured LQ column fails with a short, explicit validation error. MERGE still
requires `primary_key_cols`.

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Jobs

| Job | Role |
|-----|------|
| `j_ipac_ingest_group_1` | Continuous; one ingest task per client plus one group audit task |

## Completeness (calc gate)

```text
ingest_completeness.ready = 1
AND database_name + ct_head_version = that client's last audit snapshot
```

Do **not** gate calc on “job finished” or “last update time.”

## SQL Server audit database (`ipac_common_metdata`)

JDBC ingest does not use LakeFlow event logs. Its operational metadata is kept
in the SQL Server OLTP database `ipac_common_metdata` and accessed with
`mssql-python`. Unity Catalog contains only the Delta target tables.

| Table | Purpose |
|-------|---------|
| `ingest_group` / `ingest_table_change` | Identify CT-changed tables at a frozen `ct_head` |
| `ingest_table_apply` | Track MERGE I/U/D into Delta |
| `ingest_table_compare` | Compare CT vs apply, and SQL `COUNT(*)` vs Delta |
| `ct_table_watermark` | Per-table resume pointer; advanced after that table's MERGE |
| `ingest_completeness` | Latest READY / NOT_READY per client |
| `process_log` | Audit history |

See [docs/INGEST_METADATA.md](docs/INGEST_METADATA.md).

Run the DDL in order after dropping the old metadata database:

```text
src/sql/sql_server/01_create_database.sql
src/sql/sql_server/02_ingest_runtime.sql
src/sql/sql_server/03_watermark_completeness.sql
src/sql/sql_server/04_grants.sql
```
