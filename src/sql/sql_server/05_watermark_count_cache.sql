/*
  If ipac_common_metadata already exists from 01–04, run this once to add
  cached SQL vs Delta counts on ct_table_watermark.
*/
USE ipac_common_metadata;
GO

IF COL_LENGTH('dbo.ct_table_watermark', 'sql_row_count') IS NULL
    ALTER TABLE dbo.ct_table_watermark ADD sql_row_count bigint NULL;
GO

IF COL_LENGTH('dbo.ct_table_watermark', 'delta_row_count') IS NULL
    ALTER TABLE dbo.ct_table_watermark ADD delta_row_count bigint NULL;
GO

IF COL_LENGTH('dbo.ct_table_watermark', 'count_checked_at') IS NULL
    ALTER TABLE dbo.ct_table_watermark ADD count_checked_at datetime2(3) NULL;
GO
