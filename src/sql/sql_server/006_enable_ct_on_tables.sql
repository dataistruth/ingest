/*
  Enable Change Tracking on user tables that have a primary key.
  Views and synonyms cannot use CT — leave them is_active=false in common_tables.json.

  Run on each client database (example: iPC_2025_Dev7_15347).
  Database-level CT must already be ON.
*/
USE [iPC_2025_Dev7_15347];
GO

SET NOCOUNT ON;

DECLARE @schema sysname = N'dbo';
DECLARE @table  sysname;
DECLARE @sql    nvarchar(max);
DECLARE @oid    int;

IF NOT EXISTS (
    SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()
)
BEGIN
    RAISERROR('Database-level CHANGE_TRACKING is OFF. Enable it before table CT.', 16, 1);
    RETURN;
END;

DECLARE table_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT t.name
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
    WHERE s.name = @schema
      AND t.name IN (
          N'DF_DEALS_ARCHIVE',
          N'Entity_InactiveInvestments',
          N'FootNoteTrialBalanceAdjustments',
          N'K1LineItemGroup'
      );

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @table;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @oid = OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table));

    IF @oid IS NULL
        PRINT N'SKIP (missing): ' + @schema + N'.' + @table;
    ELSE IF NOT EXISTS (
        SELECT 1 FROM sys.indexes WHERE object_id = @oid AND is_primary_key = 1
    )
        PRINT N'SKIP (no PK — cannot enable CT): ' + @schema + N'.' + @table;
    ELSE IF EXISTS (
        SELECT 1 FROM sys.change_tracking_tables WHERE object_id = @oid
    )
        PRINT N'CT already enabled: ' + @schema + N'.' + @table;
    ELSE
    BEGIN
        SET @sql = N'ALTER TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table)
            + N' ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);';
        EXEC sys.sp_executesql @sql;
        PRINT N'CT enabled: ' + @schema + N'.' + @table;
    END

    FETCH NEXT FROM table_cursor INTO @table;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;
GO
