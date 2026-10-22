/*
  Central metadata database for JDBC CT ingest.

  Drop ipac_ingest_metadata or ipac_common_metdata first if they still exist,
  then run this file and 02–04 in order.
*/
USE master;
GO

IF DB_ID(N'ipac_common_metadata') IS NULL
BEGIN
    CREATE DATABASE ipac_common_metadata;
END;
GO

ALTER DATABASE ipac_common_metadata SET RECOVERY SIMPLE;
ALTER DATABASE ipac_common_metadata SET READ_COMMITTED_SNAPSHOT ON;
GO
