# ingest

JDBC + SQL Server **Change Tracking (CT)** ingestion for iPAC.

On-demand job clusters (not 24×7 LakeFlow pipeline clusters). Two-level fan-out:

1. **Controller** discovers changed databases at a fixed `ct_head`
2. **Database worker** fans out tables (max concurrency **5**), merges to Delta, then publishes **one recon group** per `database + ct_head`

## Why this repo

LakeFlow Connect is viable functionally, but each continuous pipeline needs a dedicated job cluster. JDBC CT ingest keeps the same recon contract (`recon_id` + database + `ct_head`) while using **job clusters only when work exists**.

## Flow

```
discover (10s / continuous, lease)
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

1. Add a row to `config/common/client.json` (`is_active: true`)
2. Optional: `config/common/client_overrides/<client_nm>.json`
3. Ensure CT is enabled on PK tables and grants exist
4. Deploy bundle and start `j_ipac_jdbc_ct_controller`

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
