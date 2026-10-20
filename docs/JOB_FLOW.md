# JDBC CT ingest — job flow

## Diagram

```
j_ipac_jdbc_ct_controller   (max_concurrent_runs=1)
        │
        ▼
discover_ct_groups
  for each active client DB:
    ct_head = CHANGE_TRACKING_CURRENT_VERSION()   # fixed for the group
    pending = CHANGETABLE vs ct_table_watermark
    if pending: emit DatabaseGroup payload
        │
        ▼
fanout_databases   for_each  concurrency=5
        │
        └── run_job → j_ipac_jdbc_ct_database_worker
                │
                ▼
        prepare_table_fanout
                │
                ▼
        fanout_tables   for_each  concurrency=5
                └── ingest_one_table  (JDBC + MERGE)
                │
                ▼
        recon_database_group
          PASS → recon_ready (recon_id + database + ct_head)
               → advance watermarks to ct_head
          FAIL → no recon_ready, no watermark advance
                │
                ▼
        write_process_log
```

## Connections

| Path | Driver | Why |
|------|--------|-----|
| Ingest payload (`jdbc_ct_ingest_table`) | Spark JDBC | Pull I/U/D row images into a DataFrame for MERGE |
| Discover CT head + pending counts | mssql-python | Cheap scalars / aggregates, no Spark cluster I/O |
| SQL `COUNT(*)` for recon_type 2 | mssql-python | Same |
| Watermarks, apply, compare, recon_ready, process_log | mssql-python | OLTP in `ipac_ingest_metadata` |

## Concurrency (example: 5 DBs, 100 tables)

| Level | Concurrency | Notes |
|-------|-------------|--------|
| Database workers | 5 | Reusable job, not 5 definitions |
| Tables per DB | 5 | YAML `for_each_task.concurrency` |
| Global JDBC | start ~10 | Payload extract only; raise toward 25 after load test |

## Metadata writes (SQL Server `ipac_ingest_metadata`)

1. Discover → `ingest_group` + `ingest_table_change` (identify)
2. Each table MERGE → `ingest_table_apply` (track)
3. After tables → `ingest_table_compare` (CT vs apply, SQL vs Delta COUNT)
4. All PASS → call `usp_complete_ingest_group`, which atomically writes
   `recon_ready` and advances `ct_*_watermark`
5. Any FAIL → no `recon_ready`, watermarks unchanged

All reads and writes use `mssql-python`. UC stores only the target Delta tables.

## Invariants

1. `ct_head` is captured once in discover and never re-read in table tasks.
2. Watermarks advance only after database-group recon PASS.
3. Calc gates on `recon_ready`, not job SUCCESS alone.
