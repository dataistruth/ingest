/*
  Create the SQL Server OLTP metadata database for custom JDBC CT ingest.
  Run as a login with CREATE DATABASE permission.
*/
USE master;
GO

IF DB_ID(N'ipac_metadata') IS NULL
BEGIN
    CREATE DATABASE ipac_metadata;
END;
GO

ALTER DATABASE ipac_metadata SET RECOVERY SIMPLE;
ALTER DATABASE ipac_metadata SET READ_COMMITTED_SNAPSHOT ON;
GO

