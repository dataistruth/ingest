/*
  Ingest cycle OLTP: one group per client DB + frozen CT head, then per-table
  change / apply / compare rows, plus process_log.
*/
USE ipac_common_metdata;
GO

CREATE TABLE dbo.ingest_group (
    group_id                 uniqueidentifier NOT NULL,
    client_nm                sysname          NOT NULL,
    database_name            sysname          NOT NULL,
    source_schema            sysname          NOT NULL
        CONSTRAINT DF_ingest_group_schema DEFAULT N'dbo',
    ct_watermark_before      bigint           NOT NULL,
    ct_head_version          bigint           NOT NULL,
    expected_table_count     int              NOT NULL,
    batch_total_volume       bigint           NOT NULL
        CONSTRAINT DF_ingest_group_volume DEFAULT 0,
    group_status             varchar(20)      NOT NULL,
    controller_job_run_id    nvarchar(128)    NULL,
    worker_job_run_id        nvarchar(128)    NULL,
    discovered_at            datetime2(3)     NOT NULL
        CONSTRAINT DF_ingest_group_discovered DEFAULT SYSUTCDATETIME(),
    started_at               datetime2(3)     NULL,
    completed_at             datetime2(3)     NULL,
    error_message            nvarchar(4000)   NULL,
    created_at               datetime2(3)     NOT NULL
        CONSTRAINT DF_ingest_group_created DEFAULT SYSUTCDATETIME(),
    updated_at               datetime2(3)     NOT NULL
        CONSTRAINT DF_ingest_group_updated DEFAULT SYSUTCDATETIME(),
    row_version              rowversion       NOT NULL,
    CONSTRAINT PK_ingest_group PRIMARY KEY CLUSTERED (group_id),
    CONSTRAINT UQ_ingest_group_db_head UNIQUE (database_name, ct_head_version),
    CONSTRAINT CK_ingest_group_window
        CHECK (ct_head_version >= ct_watermark_before),
    CONSTRAINT CK_ingest_group_table_count
        CHECK (expected_table_count >= 0),
    CONSTRAINT CK_ingest_group_status
        CHECK (group_status IN
            ('DISCOVERED', 'RUNNING', 'PASS', 'FAIL', 'CANCELLED'))
);

CREATE INDEX IX_ingest_group_client_status
    ON dbo.ingest_group (client_nm, group_status, discovered_at DESC)
    INCLUDE (database_name, ct_head_version, expected_table_count);
GO

CREATE TABLE dbo.ingest_table_change (
    group_id              uniqueidentifier NOT NULL,
    source_schema         sysname          NOT NULL,
    table_name            sysname          NOT NULL,
    watermark_before      bigint           NOT NULL,
    ct_head_version       bigint           NOT NULL,
    pending_inserts       bigint           NOT NULL
        CONSTRAINT DF_table_change_i DEFAULT 0,
    pending_updates       bigint           NOT NULL
        CONSTRAINT DF_table_change_u DEFAULT 0,
    pending_deletes       bigint           NOT NULL
        CONSTRAINT DF_table_change_d DEFAULT 0,
    pending_total         AS
        (pending_inserts + pending_updates + pending_deletes) PERSISTED,
    detected_at           datetime2(3)     NOT NULL
        CONSTRAINT DF_table_change_detected DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_ingest_table_change
        PRIMARY KEY CLUSTERED (group_id, source_schema, table_name),
    CONSTRAINT FK_table_change_group FOREIGN KEY (group_id)
        REFERENCES dbo.ingest_group(group_id),
    CONSTRAINT CK_table_change_window
        CHECK (ct_head_version >= watermark_before),
    CONSTRAINT CK_table_change_counts
        CHECK (
            pending_inserts >= 0
            AND pending_updates >= 0
            AND pending_deletes >= 0
        )
);
GO

CREATE TABLE dbo.ingest_table_apply (
    group_id              uniqueidentifier NOT NULL,
    source_schema         sysname          NOT NULL,
    table_name            sysname          NOT NULL,
    target_catalog        sysname          NULL,
    target_schema         sysname          NULL,
    target_table          sysname          NULL,
    sequence_column       sysname          NOT NULL
        CONSTRAINT DF_table_apply_sequence DEFAULT N'sys_change_version',
    applied_inserts       bigint           NOT NULL
        CONSTRAINT DF_table_apply_i DEFAULT 0,
    applied_updates       bigint           NOT NULL
        CONSTRAINT DF_table_apply_u DEFAULT 0,
    applied_deletes       bigint           NOT NULL
        CONSTRAINT DF_table_apply_d DEFAULT 0,
    applied_total         AS
        (applied_inserts + applied_updates + applied_deletes) PERSISTED,
    apply_status          varchar(20)      NOT NULL,
    task_run_id           nvarchar(128)    NULL,
    started_at            datetime2(3)     NULL,
    completed_at          datetime2(3)     NULL,
    duration_ms           bigint           NULL,
    error_message         nvarchar(4000)   NULL,
    recorded_at           datetime2(3)     NOT NULL
        CONSTRAINT DF_table_apply_recorded DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_ingest_table_apply
        PRIMARY KEY CLUSTERED (group_id, source_schema, table_name),
    CONSTRAINT FK_table_apply_change FOREIGN KEY
        (group_id, source_schema, table_name)
        REFERENCES dbo.ingest_table_change
            (group_id, source_schema, table_name),
    CONSTRAINT CK_table_apply_counts
        CHECK (
            applied_inserts >= 0
            AND applied_updates >= 0
            AND applied_deletes >= 0
        ),
    CONSTRAINT CK_table_apply_status
        CHECK (apply_status IN ('RUNNING', 'SUCCESS', 'FAIL', 'SKIPPED'))
);
GO

CREATE TABLE dbo.ingest_table_compare (
    group_id              uniqueidentifier NOT NULL,
    source_schema         sysname          NOT NULL,
    table_name            sysname          NOT NULL,
    recon_type            tinyint          NOT NULL,
    source_change_rows    bigint           NULL,
    ingest_change_rows    bigint           NULL,
    change_match          bit              NULL,
    sql_row_count         bigint           NULL,
    delta_row_count       bigint           NULL,
    count_match           bit              NULL,
    compare_status        varchar(20)      NOT NULL,
    compare_message       nvarchar(4000)   NULL,
    compared_at           datetime2(3)     NOT NULL
        CONSTRAINT DF_table_compare_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_ingest_table_compare
        PRIMARY KEY CLUSTERED (group_id, source_schema, table_name),
    CONSTRAINT FK_table_compare_change FOREIGN KEY
        (group_id, source_schema, table_name)
        REFERENCES dbo.ingest_table_change
            (group_id, source_schema, table_name),
    CONSTRAINT CK_table_compare_recon_type
        CHECK (recon_type IN (1, 2, 3)),
    CONSTRAINT CK_table_compare_status
        CHECK (compare_status IN ('PASS', 'FAIL', 'SKIPPED'))
);

CREATE INDEX IX_table_compare_fail
    ON dbo.ingest_table_compare (compare_status, compared_at DESC)
    INCLUDE (group_id, source_schema, table_name, compare_message);
GO

CREATE TABLE dbo.process_log (
    log_id               bigint           NOT NULL IDENTITY(1,1),
    process_type         varchar(20)      NOT NULL,
    process_name         nvarchar(128)    NOT NULL,
    artifact_type        varchar(20)      NULL,
    artifact_id          nvarchar(128)    NULL,
    artifact_run_id      nvarchar(128)    NULL,
    group_id             uniqueidentifier NULL,
    client_nm            sysname          NULL,
    database_name        sysname          NULL,
    object_name          sysname          NULL,
    current_status       varchar(20)      NOT NULL,
    detail_status        nvarchar(128)    NULL,
    rows_read            bigint           NULL,
    rows_written         bigint           NULL,
    rows_deleted         bigint           NULL,
    start_time           datetime2(3)     NULL,
    end_time             datetime2(3)     NULL,
    log_message          nvarchar(4000)   NULL,
    recorded_at          datetime2(3)     NOT NULL
        CONSTRAINT DF_process_log_recorded DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_process_log PRIMARY KEY CLUSTERED (log_id),
    CONSTRAINT FK_process_log_group FOREIGN KEY (group_id)
        REFERENCES dbo.ingest_group(group_id)
);

CREATE INDEX IX_process_log_group
    ON dbo.process_log (group_id, recorded_at DESC);

CREATE INDEX IX_process_log_status
    ON dbo.process_log (process_type, current_status, recorded_at DESC)
    INCLUDE (client_nm, database_name, object_name, log_message);
GO
