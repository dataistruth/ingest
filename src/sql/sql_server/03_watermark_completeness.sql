/*
  Resume pointers and latest completeness snapshot (audit notebook).
*/
USE ipac_common_metadata;
GO

CREATE TABLE dbo.ct_table_watermark (
    database_name       sysname          NOT NULL,
    source_schema       sysname          NOT NULL,
    table_name          sysname          NOT NULL,
    client_nm           sysname          NULL,
    last_version        bigint           NOT NULL,
    last_group_id       uniqueidentifier NULL,
    updated_at          datetime2(3)     NOT NULL
        CONSTRAINT DF_ct_table_wm_updated DEFAULT SYSUTCDATETIME(),
    row_version         rowversion       NOT NULL,
    CONSTRAINT PK_ct_table_watermark PRIMARY KEY CLUSTERED
        (database_name, source_schema, table_name),
    CONSTRAINT FK_ct_table_wm_group FOREIGN KEY (last_group_id)
        REFERENCES dbo.ingest_group(group_id)
);
GO

CREATE TABLE dbo.ingest_completeness (
    database_name            sysname          NOT NULL,
    client_nm                sysname          NOT NULL,
    ct_head_version          bigint           NULL,
    expected_table_count     int              NOT NULL,
    loaded_table_count       int              NOT NULL,
    ready_table_count        int              NOT NULL,
    sql_row_count            bigint           NULL,
    delta_row_count          bigint           NULL,
    duration_ms              bigint           NULL,
    ready                    bit              NOT NULL,
    not_ready_tables         nvarchar(4000)   NULL,
    checked_at               datetime2(3)     NOT NULL
        CONSTRAINT DF_ingest_completeness_checked DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_ingest_completeness PRIMARY KEY CLUSTERED (database_name)
);
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
    cmp.sql_row_count,
    cmp.delta_row_count,
    cmp.compare_status,
    COALESCE(cmp.compare_message, a.error_message) AS error_message
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

CREATE OR ALTER VIEW dbo.v_latest_completeness
AS
SELECT
    client_nm,
    database_name,
    ct_head_version,
    expected_table_count,
    loaded_table_count,
    ready_table_count,
    sql_row_count,
    delta_row_count,
    duration_ms,
    ready,
    not_ready_tables,
    checked_at
FROM dbo.ingest_completeness;
GO
