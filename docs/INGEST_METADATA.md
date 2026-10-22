# SQL Server `ipac_common_metdata`

LakeFlow recon reads pipeline event logs. The custom path has no LakeFlow
event log. Operational metadata lives in SQL Server **`ipac_common_metdata`**.

**Drivers**

- **Spark JDBC:** ingest payload only (`read_ct_changes` → MERGE)
- **mssql-python:** CT head, pending I/U/D counts, SQL `COUNT(*)`, watermarks,
  apply/compare rows, completeness, `process_log`

Unity Catalog stores the target Delta tables only.

## Recreate (drop old DB first)

```sql
USE master;
DROP DATABASE IF EXISTS ipac_ingest_metadata;
DROP DATABASE IF EXISTS ipac_common_metdata;
```

Then run in order:

1. `src/sql/sql_server/01_create_database.sql`
2. `src/sql/sql_server/02_ingest_runtime.sql`
3. `src/sql/sql_server/03_watermark_completeness.sql`
4. `src/sql/sql_server/04_grants.sql` (replace `<KEEP_USER_ID>`)

Source-database CT enablement (not metadata):
`src/sql/source/enable_ct_on_tables.sql`

## Tables

| Table | Role |
|-------|------|
| `ingest_group` | One batch = one SQL DB + frozen `CHANGE_TRACKING_CURRENT_VERSION()` |
| `ingest_table_change` | Identify CHANGETABLE I/U/D since the table watermark |
| `ingest_table_apply` | Track SCD1 MERGE applied to Delta |
| `ingest_table_compare` | Compare CT vs apply and SQL `COUNT(*)` vs Delta |
| `ct_table_watermark` | Per-table last ingested CT version |
| `ingest_completeness` | Latest READY / NOT_READY per client (audit task) |
| `process_log` | Cycle and audit operational log |

Views:

- `v_ingest_group_compare` — group totals and pass/fail counts
- `v_failed_ingest_tables` — apply and compare failures
- `v_latest_completeness` — current completeness snapshot

## Completeness

A client is READY when every **active** table has a watermark, no pending CT
up to the captured head, a Delta target, and matching SQL vs Delta counts.
The audit notebook upserts `dbo.ingest_completeness` every `freq_run`.

```sql
USE ipac_common_metdata;
SELECT * FROM dbo.ingest_completeness ORDER BY client_nm;
```

## Secrets (Databricks scope)

Config: `config/common/sql_server_conn.json`. Scope `scope_ipacs_audit`, keys from `{env}`:

- `{env}_sql_db_username`
- `{env}_sql_db_password`
- `{env}_sql_server`

The metadata database name is the bundle variable `ingest_metadata_database`
(`ipac_common_metdata`). Client source DB names come from `database_pattern`.
