"""Spark JDBC payload extract only."""


def build_ct_change_sql(schema, table_nm, pk_cols, watermark_before, ct_head):
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


def read_ct_changes(
    spark,
    jdbc_url,
    user,
    password,
    schema,
    table_nm,
    pk_cols,
    watermark_before,
    ct_head,
):
    sql = build_ct_change_sql(schema, table_nm, pk_cols, watermark_before, ct_head)
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("query", sql)
        .load()
    )
