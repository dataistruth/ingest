/*
  Seed rows into CT-enabled snapshot tables so CT discover finds pending I/U/D.
  Safe to re-run: each run appends @RowsToInsert rows per table.
  Change the USE line to target another client database.
*/
USE [iPC_2025_Dev7_15347];
GO

SET NOCOUNT ON;

DECLARE @RowsToInsert int = 25;
DECLARE @ClientID     int = 15347;
DECLARE @TaxPeriodID  int = 2025;
DECLARE @CtBefore     bigint = CHANGE_TRACKING_CURRENT_VERSION();

PRINT CONCAT('ct_head before insert: ', @CtBefore);

BEGIN TRANSACTION;

;WITH tally AS (
    SELECT TOP (@RowsToInsert) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT INTO dbo.K1Input_Snapshot (
    WorkflowID, K1PackageID, LineID, Amount, Adjustment,
    TextValue, TotalAmount, ClientID, TaxPeriodID
)
SELECT
    (rn % 1000) + 1,
    (rn % 500) + 1,
    (rn % 10000) + 1,
    CAST(rn % 100000 AS float) / 100.0,
    CAST(rn % 1000 AS float) / 10.0,
    LEFT(N'k1_' + CAST(rn AS varchar(20)), 60),
    CAST(rn % 200000 AS float) / 100.0,
    @ClientID,
    @TaxPeriodID
FROM tally;
PRINT CONCAT('K1Input_Snapshot: ', @@ROWCOUNT, ' rows');

;WITH tally AS (
    SELECT TOP (@RowsToInsert) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT INTO dbo.LookthroughAdjustments_Snapshot (
    WorkflowID, TransactionID, EntityID, ClientID, TaxPeriodID,
    UnderlyingEntityID, SourceID, FootNoteID, LineID,
    LookthroughAmount, AdjustmentDescription, AdjustmentAmount,
    TrackingKey, ParentEntityID, SuperParentEntityID, Tag,
    SourceEntityID, BoxJKLBox
)
SELECT
    (rn % 1000) + 1,
    rn,
    (rn % 500) + 1,
    @ClientID,
    @TaxPeriodID,
    (rn % 300) + 1,
    (rn % 50) + 1,
    (rn % 20) + 1,
    (rn % 10000) + 1,
    CAST(rn % 10000 AS varchar(200)),
    LEFT(N'adj_' + CAST(rn AS varchar(20)), 200),
    CAST(rn % 5000 AS varchar(200)),
    LEFT(N'trk_' + CAST(rn AS varchar(20)), 4000),
    (rn % 400) + 1,
    (rn % 200) + 1,
    LEFT(N'tag_' + CAST(rn AS varchar(20)), 5000),
    (rn % 350) + 1,
    CHAR(65 + (rn % 3))
FROM tally;
PRINT CONCAT('LookthroughAdjustments_Snapshot: ', @@ROWCOUNT, ' rows');

;WITH tally AS (
    SELECT TOP (@RowsToInsert) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT INTO dbo.PFICFootnoteInput_Snapshot (
    WorkflowID, PFICFootnoteID, LineID, Amount, TextValue,
    ClientID, TaxPeriodID
)
SELECT
    (rn % 1000) + 1,
    (rn % 500) + 1,
    (rn % 10000) + 1,
    CAST(rn % 100000 AS float) / 100.0,
    LEFT(N'pfic_' + CAST(rn AS varchar(20)), 100),
    @ClientID,
    @TaxPeriodID
FROM tally;
PRINT CONCAT('PFICFootnoteInput_Snapshot: ', @@ROWCOUNT, ' rows');

COMMIT TRANSACTION;

DECLARE @CtAfter bigint = CHANGE_TRACKING_CURRENT_VERSION();
PRINT CONCAT('ct_head after insert: ', @CtAfter);

SELECT 'K1Input_Snapshot' AS table_name, ct.SYS_CHANGE_OPERATION AS op, COUNT_BIG(*) AS pending
FROM CHANGETABLE(CHANGES dbo.K1Input_Snapshot, @CtBefore) AS ct
GROUP BY ct.SYS_CHANGE_OPERATION
UNION ALL
SELECT 'LookthroughAdjustments_Snapshot', ct.SYS_CHANGE_OPERATION, COUNT_BIG(*)
FROM CHANGETABLE(CHANGES dbo.LookthroughAdjustments_Snapshot, @CtBefore) AS ct
GROUP BY ct.SYS_CHANGE_OPERATION
UNION ALL
SELECT 'PFICFootnoteInput_Snapshot', ct.SYS_CHANGE_OPERATION, COUNT_BIG(*)
FROM CHANGETABLE(CHANGES dbo.PFICFootnoteInput_Snapshot, @CtBefore) AS ct
GROUP BY ct.SYS_CHANGE_OPERATION;
GO
