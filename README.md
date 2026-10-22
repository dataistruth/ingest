# ingest

JDBC + SQL Server **Change Tracking (CT)** ingestion for iPAC.

**Connections:** Spark JDBC is used only to extract change **payload** for MERGE.
CT head, pending counts, COUNT(*), watermarks, and audit writes use **mssql-python**.

One **continuous** job per client `job_group`. Each client is a parallel notebook task
(`ct_client_ingest.py`) that loops forever on a shared job cluster: poll CT every
`freq_run` seconds (default 10), then snapshot or SCD1-MERGE changed tables in
`parallel_table_run` threads. Completeness audit is a separate interval
(`audit_freq`, default 15s). No task hand-off, no fan-out.

## Flow

```
j_ipac_ingest_group_1 (continuous)
  ├── ingest_15347 ─┐ every freq_run s (10): CT poll + snapshot/MERGE
  ├── ingest_15447 ─┤   then SQL vs Delta COUNT(*) for those tables only
  └── audit_group_1 ┘ every audit_freq s (15): CT/watermark/pending
                         full SQL vs Delta COUNT(*) every full_recon s (3600)
                         READY / NOT_READY → dbo.ingest_completeness
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

## How a cycle finds changed tables

Candidate tables are **not** discovered from SQL Server. They come from
`config/common/common_tables.json` plus `client_overrides/<client>.json`
(`ignore` / `extra`). Only `is_active = true` tables are considered.

Each ingest cycle on a client notebook:

1. **Freeze the CT head once** from the **source** client database:
   `SELECT CHANGE_TRACKING_CURRENT_VERSION()`. Every table in that cycle is
   compared against this same `ct_head`. If the head is unchanged from the
   last idle cycle, the notebook sleeps `freq_run` seconds and polls again.
2. **Load resume pointers** from metadata
   `ipac_common_metadata.dbo.ct_table_watermark` (`last_version` per
   `database_name` + schema + table).
3. **Probe each active table** (mssql-python, cheap counts — not the payload):
   - skip if the object is missing, not a user table, CT is off, or
     `primary_key_cols` is blank
   - **snapshot** (full JDBC read + `CREATE OR REPLACE TABLE`) if there is no
     watermark, the watermark is below `CHANGE_TRACKING_MIN_VALID_VERSION`,
     or the Unity Catalog target is missing
   - **CDC** if watermark `< ct_head` **and** CHANGETABLE has pending rows
   - **idle** if watermark `>= ct_head` or pending total is `0` (that table
     is marked probed to this head and is not written to `ingest_table_change`)
4. **Pending I/U/D** (this is how “changed” is defined):

   ```sql
   SELECT SYS_CHANGE_OPERATION, COUNT_BIG(*)
   FROM CHANGETABLE(CHANGES [dbo].[TableName], @watermark) AS ct
   WHERE ct.SYS_CHANGE_OPERATION IN ('I', 'U', 'D')
     AND ct.SYS_CHANGE_VERSION <= @ct_head
   GROUP BY SYS_CHANGE_OPERATION
   ```

   `GROUP BY` returns only operations that occurred. One row with `I=40000`
   means 40,000 inserts and no updates or deletes in that window. Counts are
   per table, not one database-wide total.
5. If any table needs snapshot or CDC, insert **`ingest_group`** (one row for
   this client DB + frozen `ct_head`) and one **`ingest_table_change`** row per
   planned table (I/U/D counts; snapshot rows have zeros and
   `watermark_before = 0`).
6. Threads (`parallel_table_run`) **extract payload with Spark JDBC**
   (`CHANGETABLE` join to the source table, or a full snapshot) and SCD1 MERGE
   into Delta. After a **successful** apply, upsert
   `ct_table_watermark.last_version = ct_head` for **that table only**.
7. Write **`ingest_table_apply`** (applied I/U/D + SUCCESS/FAIL) and
   **`ingest_table_compare`** (CT vs apply, SQL `COUNT(*)` vs Delta). Set
   `ingest_group.group_status` to PASS or FAIL.

A failed table keeps its old watermark and is retried next cycle. MERGE is
idempotent.

There is no T-SQL cursor or stored procedure on this detect path. The job
does not use `sys.dm_tran_commit_table` or a database-wide watermark table.

## SQL Server audit database (`ipac_common_metadata`)

Operational metadata lives here (mssql-python). Unity Catalog holds only
Delta targets. Payload extract uses Spark JDBC.

| Table | Grain | What it records |
|-------|--------|-----------------|
| `ingest_group` | one cycle = `database_name` + `ct_head_version` | Frozen head, table count planned, PASS/FAIL |
| `ingest_table_change` | group + table | Identify window: `watermark_before` → `ct_head`, pending I/U/D |
| `ingest_table_apply` | group + table | MERGE/snapshot result: applied I/U/D, SUCCESS/FAIL, duration |
| `ingest_table_compare` | group + table | CT pending vs applied; SQL `COUNT(*)` vs Delta `COUNT(*)` |
| `ct_table_watermark` | source DB + schema + table | Last ingested CT version plus last SQL/Delta counts |
| `ingest_completeness` | one row per client DB | Latest READY / NOT_READY from the audit task |
| `process_log` | append-only | Cycle and audit messages |

**Views:** `v_ingest_group_compare`, `v_failed_ingest_tables`,
`v_latest_completeness`.

**Completeness (audit task, not ingest):** READY when every **active** table
has a watermark, pending CT to the captured head is 0, the Delta table exists,
and last SQL count equals last Delta count. Ignore-list tables are excluded.

The 15s audit does **not** `COUNT(*)` every Delta table. Counts are taken:

1. Immediately after that table’s snapshot / MERGE
2. For a full safety pass every `full_recon` seconds (3600)
3. Otherwise from `ct_table_watermark.sql_row_count` / `delta_row_count`

```sql
USE ipac_common_metadata;
SELECT * FROM dbo.ingest_completeness ORDER BY client_nm;
SELECT * FROM dbo.v_failed_ingest_tables;
SELECT * FROM dbo.ct_table_watermark WHERE database_name = N'...';
```

See [docs/INGEST_METADATA.md](docs/INGEST_METADATA.md).

Run the DDL in order after dropping the old metadata database:

```text
src/sql/sql_server/01_create_database.sql
src/sql/sql_server/02_ingest_runtime.sql
src/sql/sql_server/03_watermark_completeness.sql
src/sql/sql_server/04_grants.sql
src/sql/sql_server/05_watermark_count_cache.sql   # only if 01–04 already ran
```

