/*
  Latest completeness snapshot per source database.
  Audit notebook upserts one row per client every freq_run.
*/
USE ipac_ingest_metadata;
GO

IF OBJECT_ID(N'dbo.ingest_completeness', N'U') IS NULL
BEGIN
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
END;
GO
