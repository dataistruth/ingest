/*
  Operational and reconciliation views.
*/
USE ipac_ingest_metadata;
GO

CREATE OR ALTER VIEW dbo.v_ingest_group_compare
AS
SELECT
    g.group_id,
    g.client_nm,
    g.database_name,
    g.ct_watermark_before,
    g.ct_head_version,
    g.group_status,
    g.expected_table_count,
    COUNT(c.table_name) AS discovered_table_count,
    COALESCE(SUM(c.pending_total), 0) AS source_change_rows,
    COALESCE(SUM(a.applied_total), 0) AS ingest_change_rows,
    COALESCE(SUM(CASE WHEN cmp.compare_status = 'PASS' THEN 1 ELSE 0 END), 0)
        AS tables_pass,
    COALESCE(SUM(CASE WHEN cmp.compare_status = 'FAIL' THEN 1 ELSE 0 END), 0)
        AS tables_fail,
    COALESCE(SUM(CASE WHEN cmp.change_match = 1 THEN 1 ELSE 0 END), 0)
        AS tables_change_match,
    COALESCE(SUM(CASE WHEN cmp.count_match = 1 THEN 1 ELSE 0 END), 0)
        AS tables_count_match,
    g.discovered_at,
    g.started_at,
    g.completed_at
FROM dbo.ingest_group AS g
LEFT JOIN dbo.ingest_table_change AS c
    ON c.group_id = g.group_id
LEFT JOIN dbo.ingest_table_apply AS a
    ON a.group_id = c.group_id
   AND a.source_schema = c.source_schema
   AND a.table_name = c.table_name
LEFT JOIN dbo.ingest_table_compare AS cmp
    ON cmp.group_id = c.group_id
   AND cmp.source_schema = c.source_schema
   AND cmp.table_name = c.table_name
GROUP BY
    g.group_id,
    g.client_nm,
    g.database_name,
    g.ct_watermark_before,
    g.ct_head_version,
    g.group_status,
    g.expected_table_count,
    g.discovered_at,
    g.started_at,
    g.completed_at;
GO

CREATE OR ALTER VIEW dbo.v_latest_client_recon_ready
AS
WITH ranked AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.client_nm
            ORDER BY r.completed_at DESC, r.ct_head_version DESC
        ) AS rn
    FROM dbo.recon_ready AS r
    WHERE r.ready_for_calc = 1
)
SELECT
    recon_id,
    group_id,
    ingest_method,
    client_nm,
    database_name,
    ct_watermark_before,
    ct_head_version,
    table_count,
    batch_total_volume,
    source_change_rows,
    ingest_change_rows,
    tables_json,
    artifact_type,
    artifact_id,
    artifact_run_id,
    loop_started_at,
    completed_at,
    total_ingestion_ms
FROM ranked
WHERE rn = 1;
GO

CREATE OR ALTER VIEW dbo.v_failed_ingest_tables
AS
SELECT
    g.client_nm,
    g.database_name,
    g.ct_head_version,
    c.source_schema,
    c.table_name,
    c.pending_total,
    a.applied_total,
    a.apply_status,
    cmp.source_change_rows,
    cmp.ingest_change_rows,
    cmp.sql_row_count,
    cmp.delta_row_count,
    cmp.change_match,
    cmp.count_match,
    cmp.compare_status,
    COALESCE(cmp.compare_message, a.error_message) AS error_message,
    cmp.compared_at
FROM dbo.ingest_group AS g
INNER JOIN dbo.ingest_table_change AS c
    ON c.group_id = g.group_id
LEFT JOIN dbo.ingest_table_apply AS a
    ON a.group_id = c.group_id
   AND a.source_schema = c.source_schema
   AND a.table_name = c.table_name
LEFT JOIN dbo.ingest_table_compare AS cmp
    ON cmp.group_id = c.group_id
   AND cmp.source_schema = c.source_schema
   AND cmp.table_name = c.table_name
WHERE
    a.apply_status = 'FAIL'
    OR cmp.compare_status = 'FAIL';
GO

