"""Spark JDBC extract for SQL Server Change Tracking windows."""

from __future__ import annotations

from typing import Any


def build_ct_change_sql(
    schema: str,
    table_nm: str,
    pk_cols: list[str],
    watermark_before: int,
    ct_head: int,
) -> str:
    """
    Pull CT change keys + current row image for I/U; key-only for D.

    Watermark window: watermark_before < version <= ct_head
    (CHANGETABLE(CHANGES t, watermark) returns versions > watermark).
    """
    schema = (schema or "dbo").strip()
    join_pred = " AND ".join(f"src.[{c}] = ct.[{c}]" for c in pk_cols)
    pk_select = ", ".join(f"ct.[{c}]" for c in pk_cols)
    return f"""
SELECT
  {pk_select},
  ct.SYS_CHANGE_OPERATION AS sys_change_operation,
  ct.SYS_CHANGE_VERSION AS sys_change_version,
  src.*
FROM CHANGETABLE(CHANGES [{schema}].[{table_nm}], {int(watermark_before)}) AS ct
LEFT JOIN [{schema}].[{table_nm}] AS src
  ON {join_pred}
WHERE ct.SYS_CHANGE_VERSION <= {int(ct_head)}
  AND ct.SYS_CHANGE_OPERATION IN ('I', 'U', 'D')
""".strip()


def jdbc_options(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    *,
    encrypt: bool = True,
    trust_server_certificate: bool = True,
) -> dict[str, str]:
    encrypt_flag = "true" if encrypt else "false"
    trust_flag = "true" if trust_server_certificate else "false"
    url = (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={database};"
        f"encrypt={encrypt_flag};"
        f"trustServerCertificate={trust_flag}"
    )
    return {
        "url": url,
        "user": user,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def read_ct_changes(
    spark: Any,
    jdbc_opts: dict[str, str],
    schema: str,
    table_nm: str,
    pk_cols: list[str],
    watermark_before: int,
    ct_head: int,
):
    """Return a Spark DataFrame of CT changes for one table window."""
    sql = build_ct_change_sql(schema, table_nm, pk_cols, watermark_before, ct_head)
    return (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", sql)
        .load()
    )
