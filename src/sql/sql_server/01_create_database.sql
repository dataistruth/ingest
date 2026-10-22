/*
  Central metadata database for JDBC CT ingest.

  Drop the old ipac_ingest_metadata database first if it still exists, then run
  this file and 02–04 in order.
*/
USE master;
GO

IF DB_ID(N'ipac_common_metdata') IS NULL
BEGIN
    CREATE DATABASE ipac_common_metdata;
END;
GO

ALTER DATABASE ipac_common_metdata SET RECOVERY SIMPLE;
ALTER DATABASE ipac_common_metdata SET READ_COMMITTED_SNAPSHOT ON;
GO
