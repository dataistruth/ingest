/*
  Replace the principal before execution.
  The same service principal/login can read source CT databases and write this
  central metadata database, or separate read/write principals can be used.
*/
USE ipac_ingest_metadata;
GO

DECLARE @Principal sysname = N'<KEEP_USER_ID>';

IF @Principal = N'<KEEP_USER_ID>'
    THROW 51000, 'Replace <KEEP_USER_ID> before running grants.', 1;

DECLARE @sql nvarchar(max);

IF DATABASE_PRINCIPAL_ID(@Principal) IS NULL
BEGIN
    SET @sql = N'CREATE USER ' + QUOTENAME(@Principal)
        + N' FROM LOGIN ' + QUOTENAME(@Principal) + N';';
    EXEC sys.sp_executesql @sql;
END;

SET @sql =
    N'GRANT SELECT, INSERT, UPDATE ON SCHEMA::dbo TO '
    + QUOTENAME(@Principal) + N';'
    + N' GRANT EXECUTE ON SCHEMA::dbo TO '
    + QUOTENAME(@Principal) + N';';
EXEC sys.sp_executesql @sql;
GO

