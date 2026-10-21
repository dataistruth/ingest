# ingest

JDBC + SQL Server **Change Tracking (CT)** ingestion for iPAC.

**Connections:** Spark JDBC is used only to extract change **payload** for MERGE.
Discover, COUNT(*), watermarks, recon, and process_log use **mssql-python**.

On-demand job clusters (not 24×7 LakeFlow pipeline clusters). Two-level fan-out:

1. **Controller** discovers changed databases at a fixed `ct_head`
2. **Database worker** fans out tables (max concurrency **5**), merges to Delta, then publishes **one recon group** per `database + ct_head`

## Why this repo

LakeFlow Connect is viable functionally, but each continuous pipeline needs a dedicated job cluster. JDBC CT ingest keeps the same recon contract (`recon_id` + database + `ct_head`) while using **job clusters only when work exists**.

## Flow

```
discover (scheduled, max 1 concurrent run)
  snapshot ct_head per DB
  CHANGETABLE vs watermark → changed tables
        │
        ▼
for_each database  (concurrency 5)
        │
        ▼
for_each table     (concurrency 5)
  JDBC extract + Delta MERGE
        │
        ▼
recon_ready PASS → advance watermarks
```

## Layout

```
config/common/          client + table registry
resources/jobs/         Databricks job YAML
src/common/notebooks/   Job notebook tasks
src/common/ops/         CT discover, JDBC extract, recon helpers
docs/                   Design notes
```

## Onboard a client

1. Add `{ "client_name", "is_active", "job_group": 1 }` to `config/common/client.json`
2. Shared CT tables live in `config/common/common_tables.json`
3. Optional `config/common/client_overrides/<client_name>.json` (`ignore` / `extra`, including disabled tables)
4. Ensure CT is enabled on PK tables and grants exist
5. SQL Server login is `config/common/sql_server_conn.json` (`scope_ipacs_audit` / `{env}_sql_db_username`, `{env}_sql_db_password`, `{env}_sql_server`)
6. Run `src/common/notebooks/setup_uc_schemas.py` once (creates `{catalog}.{lower(database)}{uc_schema_suffix}`)
7. Deploy bundle and start `j_ipac_jdbc_ct_controller`

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Jobs

| Job | Role |
|-----|------|
| `j_ipac_jdbc_ct_controller` | Discover CT groups; fan out 1 worker per changed DB |
| `j_ipac_jdbc_ct_database_worker` | Fan out tables (max 5); recon; watermark advance |

## Recon contract (calc gate)

```text
recon_ready.ready_for_calc = true
AND database_name + ct_head_version = group identity
```

Do **not** gate calc on “job finished” or “last update time.”

## SQL Server audit database (`ipac_ingest_metadata`)

JDBC ingest does not use LakeFlow event logs. Its operational metadata is kept
in the SQL Server OLTP database `ipac_ingest_metadata` and accessed with
`mssql-python`. Unity Catalog contains only the Delta target tables.

| Table | Purpose |
|-------|---------|
| `ingest_group` / `ingest_table_change` | Identify CT-changed tables at a frozen `ct_head` |
| `ingest_table_apply` | Track MERGE I/U/D into Delta |
| `ingest_table_compare` | Compare CT vs apply, and SQL `COUNT(*)` vs Delta |
| `recon_ready` | Calc gate (PASS only) |
| `ct_*_watermark` | Resume pointer; advance only after group PASS |

See [docs/INGEST_METADATA.md](docs/INGEST_METADATA.md).

Run the DDL in order:

```text
src/sql/sql_server/001_create_database.sql
src/sql/sql_server/002_ingest_metadata_tables.sql
src/sql/sql_server/003_ingest_metadata_views.sql
src/sql/sql_server/004_ingest_metadata_procedures.sql
src/sql/sql_server/005_grants.sql
```
