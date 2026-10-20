/*
  Atomic group completion.

  Called by mssql-python after all table tasks write apply + compare rows.
  On PASS, recon_ready and both watermark levels commit in one transaction.
  On any mismatch, nothing advances.
*/
USE ipac_metadata;
GO

CREATE OR ALTER PROCEDURE dbo.usp_complete_ingest_group
    @group_id            uniqueidentifier,
    @artifact_id         nvarchar(128) = N'j_ipac_jdbc_ct_database_worker',
    @artifact_run_id     nvarchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @client_nm              sysname,
        @database_name          sysname,
        @ct_watermark_before    bigint,
        @ct_head_version        bigint,
        @expected_table_count   int,
        @batch_total_volume     bigint,
        @discovered_at          datetime2(3),
        @compare_count          int,
        @failed_count           int,
        @source_change_rows     bigint,
        @ingest_change_rows     bigint,
        @tables_json            nvarchar(max);

    BEGIN TRANSACTION;

    SELECT
        @client_nm = g.client_nm,
        @database_name = g.database_name,
        @ct_watermark_before = g.ct_watermark_before,
        @ct_head_version = g.ct_head_version,
        @expected_table_count = g.expected_table_count,
        @batch_total_volume = g.batch_total_volume,
        @discovered_at = g.discovered_at
    FROM dbo.ingest_group AS g WITH (UPDLOCK, HOLDLOCK)
    WHERE g.group_id = @group_id;

    IF @database_name IS NULL
        THROW 51000, 'Unknown ingest group.', 1;

    IF EXISTS (
        SELECT 1
        FROM dbo.recon_ready
        WHERE group_id = @group_id
    )
    BEGIN
        COMMIT TRANSACTION;
        SELECT CAST(1 AS bit) AS ready_for_calc, N'ALREADY_COMPLETE' AS result;
        RETURN;
    END;

    SELECT
        @compare_count = COUNT(*),
        @failed_count = COALESCE(SUM(
            CASE WHEN compare_status <> 'PASS' THEN 1 ELSE 0 END
        ), 0),
        @source_change_rows = COALESCE(SUM(source_change_rows), 0),
        @ingest_change_rows = COALESCE(SUM(ingest_change_rows), 0)
    FROM dbo.ingest_table_compare
    WHERE group_id = @group_id;

    IF @compare_count <> @expected_table_count OR @failed_count > 0
    BEGIN
        UPDATE dbo.ingest_group
        SET
            group_status = 'FAIL',
            completed_at = SYSUTCDATETIME(),
            error_message = CONCAT(
                N'Recon incomplete or failed: expected=',
                @expected_table_count,
                N', compared=',
                COALESCE(@compare_count, 0),
                N', failed=',
                COALESCE(@failed_count, 0)
            ),
            updated_at = SYSUTCDATETIME()
        WHERE group_id = @group_id;

        COMMIT TRANSACTION;
        SELECT CAST(0 AS bit) AS ready_for_calc, N'RECON_FAILED' AS result;
        RETURN;
    END;

    SELECT @tables_json = (
        SELECT
            c.source_schema,
            c.table_name,
            c.watermark_before,
            c.ct_head_version,
            c.pending_inserts,
            c.pending_updates,
            c.pending_deletes,
            c.pending_total,
            a.applied_inserts,
            a.applied_updates,
            a.applied_deletes,
            cmp.sql_row_count,
            cmp.delta_row_count,
            cmp.compare_status
        FROM dbo.ingest_table_change AS c
        INNER JOIN dbo.ingest_table_apply AS a
            ON a.group_id = c.group_id
           AND a.source_schema = c.source_schema
           AND a.table_name = c.table_name
        INNER JOIN dbo.ingest_table_compare AS cmp
            ON cmp.group_id = c.group_id
           AND cmp.source_schema = c.source_schema
           AND cmp.table_name = c.table_name
        WHERE c.group_id = @group_id
        ORDER BY c.source_schema, c.table_name
        FOR JSON PATH
    );

    INSERT dbo.recon_ready (
        group_id,
        client_nm,
        database_name,
        ct_watermark_before,
        ct_head_version,
        table_count,
        batch_total_volume,
        source_change_rows,
        ingest_change_rows,
        tables_json,
        ready_for_calc,
        artifact_id,
        artifact_run_id,
        loop_started_at,
        completed_at,
        total_ingestion_ms
    )
    VALUES (
        @group_id,
        @client_nm,
        @database_name,
        @ct_watermark_before,
        @ct_head_version,
        @expected_table_count,
        @batch_total_volume,
        @source_change_rows,
        @ingest_change_rows,
        @tables_json,
        1,
        @artifact_id,
        COALESCE(@artifact_run_id, CONVERT(nvarchar(36), @group_id)),
        @discovered_at,
        SYSUTCDATETIME(),
        DATEDIFF_BIG(millisecond, @discovered_at, SYSUTCDATETIME())
    );

    UPDATE dbo.ct_db_watermark WITH (UPDLOCK, SERIALIZABLE)
    SET
        client_nm = @client_nm,
        last_version = @ct_head_version,
        last_group_id = @group_id,
        updated_at = SYSUTCDATETIME()
    WHERE database_name = @database_name;

    IF @@ROWCOUNT = 0
    BEGIN
        INSERT dbo.ct_db_watermark (
            database_name, client_nm, last_version, last_group_id
        )
        VALUES (
            @database_name, @client_nm, @ct_head_version, @group_id
        );
    END;

    UPDATE wm WITH (UPDLOCK, SERIALIZABLE)
    SET
        wm.client_nm = @client_nm,
        wm.last_version = c.ct_head_version,
        wm.last_group_id = @group_id,
        wm.updated_at = SYSUTCDATETIME()
    FROM dbo.ct_table_watermark AS wm
    INNER JOIN dbo.ingest_table_change AS c
        ON c.group_id = @group_id
       AND c.source_schema = wm.source_schema
       AND c.table_name = wm.table_name
    WHERE wm.database_name = @database_name;

    INSERT dbo.ct_table_watermark (
        database_name,
        source_schema,
        table_name,
        client_nm,
        last_version,
        last_group_id
    )
    SELECT
        @database_name,
        c.source_schema,
        c.table_name,
        @client_nm,
        c.ct_head_version,
        @group_id
    FROM dbo.ingest_table_change AS c
    WHERE c.group_id = @group_id
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.ct_table_watermark AS wm WITH (UPDLOCK, HOLDLOCK)
          WHERE wm.database_name = @database_name
            AND wm.source_schema = c.source_schema
            AND wm.table_name = c.table_name
      );

    UPDATE dbo.ingest_group
    SET
        group_status = 'PASS',
        completed_at = SYSUTCDATETIME(),
        error_message = NULL,
        updated_at = SYSUTCDATETIME()
    WHERE group_id = @group_id;

    COMMIT TRANSACTION;
    SELECT CAST(1 AS bit) AS ready_for_calc, N'PASS' AS result;
END;
GO

