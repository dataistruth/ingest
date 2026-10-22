# JDBC CT ingest — job flow

## Diagram

```
j_ipac_ingest_group_1   (continuous, max_concurrent_runs=1, FAIR scheduler)
  ├── ingest_15347   ct_client_ingest.py  (client_name=15347)
  ├── ingest_15447   ct_client_ingest.py  (client_name=15447)
  └── audit_group_1  ct_group_audit.py    (both clients, every freq_run)
        │   tasks run in parallel on one job cluster; no depends_on
        ▼
while True:                                   # never stops unless cancelled
  ct_head = CHANGE_TRACKING_CURRENT_VERSION() # mssql-python
  if ct_head unchanged since last clean cycle → sleep
  plan per active table:
    no watermark / watermark < CT min valid / target missing → snapshot
    else pending = CHANGETABLE(watermark .. ct_head)  → cdc if pending > 0
  ThreadPool(parallel_table_run):
    snapshot: JDBC SELECT * → if lq_key set, LQ columns first + CLUSTER BY; else no LQ
    cdc:      JDBC CHANGETABLE payload → SCD1 MERGE (delete on D)
    upsert ct_table_watermark = ct_head         # per table, after success
    recon: CT rows vs applied, SQL COUNT(*) vs Delta count (logged, no gate)
  sleep(freq_run − elapsed)
```

## Code layout (one notebook, separate modules)

| Module | Role |
|--------|------|
| `notebooks/ct_client_ingest.py` | Widgets, secrets, UC schema, then `run_forever` |
| `ops/ingest.py` | Cycle: probe CT, plan tables, thread pool, logging |
| `ops/cdc.py` | Snapshot create (`CLUSTER BY`) and SCD1 MERGE with per-call unique temp views |
| `ops/recon.py` | Per-table count compare |
| `ops/jdbc_extract.py` | Spark JDBC payload reads (CT changes, snapshot) |
| `ops/ct_source.py` | mssql-python CT head, CT status, pending counts, watermarks |
| `ops/ingest_metadata.py` | mssql-python writes to `ipac_ingest_metadata` |
| `ops/target.py` | LQ-first target schema and configured target deletion |
| `ops/reset.py` | Transactional per-client metadata reset |
| `notebooks/reset_client_ingest.py` | Guarded dry-run/reset process |
| `ops/completeness.py` | Per-client CT completeness: loaded tables, counts, ready |
| `notebooks/ct_group_audit.py` | Forever audit loop for every client in the job_group |

## Connections

| Path | Driver | Why |
|------|--------|-----|
| Payload extract (CT changes, bootstrap snapshot) | Spark JDBC | Row images into a DataFrame for MERGE |
| CT head, CT status, pending counts, `COUNT(*)` | mssql-python | Cheap scalars / aggregates |
| Watermarks, group, apply, compare | mssql-python | OLTP in `ipac_ingest_metadata` |

## Parameters (`databricks.yml`)

| Variable | Default | Used as |
|----------|---------|---------|
| `freq_run_sec` | 10 | Notebook `freq_run`: seconds between polls |
| `parallel_table_run` | 5 | Notebook thread pool size per client |
| `uc_catalog`, `uc_schema_suffix` | dev7, `_custom` | Target `{catalog}.{lower(db)}{suffix}.{table}` |
| `ingest_metadata_database` | ipac_ingest_metadata | SQL Server audit DB |

## Metadata writes (SQL Server `ipac_ingest_metadata`)

1. Cycle with work → `ingest_group` + `ingest_table_change` for the planned tables
2. Each table → `ct_table_watermark` upsert to `ct_head`, then `ingest_table_apply`
3. Recon → `ingest_table_compare`
4. Cycle end → `ingest_group.group_status` PASS / FAIL

Audit writes are best-effort (logged on failure). The watermark upsert is not:
if it fails, the table is reported FAIL and is re-applied next cycle (MERGE is idempotent).

## Invariants

1. `ct_head` is read once per cycle; every table in the cycle reads `CHANGETABLE` up to it.
2. A table's watermark advances only after its own MERGE / snapshot succeeds.
3. A snapshot sets the watermark to the `ct_head` captured before the read; later
   CT replays over the snapshot are idempotent.
4. Cycle errors are logged, connections reopened next cycle; the loop never exits.
5. Group membership is hard-coded per job: tasks must match `client.json` `job_group`.

## Completeness audit

`audit_group_1` polls every `freq_run` seconds for every active client in the
job group. A client is READY when every active table has a watermark, no pending
CT changes up to the captured `ct_head`, a Delta target, and matching SQL vs
Delta `COUNT(*)`. Latest status is upserted to `dbo.ingest_completeness`; each
cycle also appends `dbo.process_log`.

## Reset

Stop the continuous client task before running `reset_client_ingest.py`. The
notebook is dry-run by default and requires `RESET_<client_name>` confirmation
to execute. A full reset drops configured target tables first, then transactionally
deletes that database's watermarks and audit groups. Restarting ingest triggers
a clean snapshot.
