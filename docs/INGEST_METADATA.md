# SQL Server `ipac_metadata`

LakeFlow recon reads pipeline event logs. The custom path has no LakeFlow
event log. Operational metadata lives in SQL Server **`ipac_metadata`**.

**Drivers**

- **Spark JDBC:** ingest payload only (`read_ct_changes` → MERGE)
- **mssql-python:** CT head, pending I/U/D counts, SQL `COUNT(*)`, watermarks,
  apply/compare rows, `recon_ready`, `process_log`

Unity Catalog stores the target Delta tables only. Calc gates on
**database + `ct_head_version`**, not “job finished.”

## DDL order

1. `src/sql/sql_server/001_create_database.sql`
2. `src/sql/sql_server/002_ingest_metadata_tables.sql`
3. `src/sql/sql_server/003_ingest_metadata_views.sql`
4. `src/sql/sql_server/004_ingest_metadata_procedures.sql`
5. `src/sql/sql_server/005_grants.sql` (replace `<KEEP_USER_ID>`)

## Tables

| Table | Role |
|-------|------|
| `ingest_group` | One batch = one SQL DB + frozen `CHANGE_TRACKING_CURRENT_VERSION()` |
| `ingest_table_change` | Identify CHANGETABLE I/U/D since the table watermark |
| `ingest_table_apply` | Track Auto CDC–style SCD1 MERGE applied to Delta |
| `ingest_table_compare` | Compare CT vs apply and SQL `COUNT(*)` vs Delta |
| `recon_ready` | Calc gate — PASS only, database + CT-head grain |
| `ct_db_watermark` / `ct_table_watermark` | Last PASS version used as the next `ct_start` |
| `process_log` | Controller/worker/table-task operational log |
| `controller_lease` | Prevent concurrent controller ownership |

Views:

- `v_ingest_group_compare` — group totals and pass/fail counts
- `v_latest_client_recon_ready` — newest calc-ready group per client
- `v_failed_ingest_tables` — apply and reconciliation failures

## Compare rules

| recon_type | Change sync | Count sync |
|------------|-------------|------------|
| 1 | Apply completed | Skip |
| 2 | `pending I+U+D = applied I+U+D` | `sql_row_count = delta_row_count` |
| 3 | `pending I+U = applied I+U` | Skip |

After every table has a PASS compare row, call
`dbo.usp_complete_ingest_group`. In one SQL transaction it:

1. verifies expected table count and zero failures;
2. inserts `recon_ready`;
3. advances database and table watermarks to the frozen `ct_head`;
4. marks the group PASS.

On FAIL, no `recon_ready` row is published and no watermark advances.

## Secrets (Databricks scope)

Config: `config/common/sql_server_conn.json`. Scope `scope_ipacs_audit`, keys from `{env}`:

- `{env}_sql_db_username`
- `{env}_sql_db_password`
- `{env}_sql_server`

Database name comes from the client (`client_name`), not from the secret.
