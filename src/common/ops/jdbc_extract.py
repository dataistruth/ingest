"""Spark JDBC payload extract only."""

CT_PK_PREFIX = "__ct_"


def build_ct_change_sql(schema, table_nm, pk_cols, watermark_before, ct_head):
    join_pred = " AND ".join(f"src.[{c}] = ct.[{c}]" for c in pk_cols)
    pk_select = ", ".join(f"ct.[{c}] AS [{CT_PK_PREFIX}{c}]" for c in pk_cols)
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


def _jdbc_reader(spark, jdbc_url, user, password, fetch_size):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", str(fetch_size))
    )


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
    fetch_size=10000,
):
    from pyspark.sql import functions as F

    sql = build_ct_change_sql(schema, table_nm, pk_cols, watermark_before, ct_head)
    df = _jdbc_reader(spark, jdbc_url, user, password, fetch_size).option("query", sql).load()
    # Deleted rows have no src.* image; take the key from CHANGETABLE.
    for col in pk_cols:
        ct_col = f"{CT_PK_PREFIX}{col}"
        df = df.withColumn(col, F.coalesce(F.col(f"`{col}`"), F.col(f"`{ct_col}`"))).drop(ct_col)
    return df


def read_table_snapshot(spark, jdbc_url, user, password, schema, table_nm, fetch_size=10000):
    return (
        _jdbc_reader(spark, jdbc_url, user, password, fetch_size)
        .option("dbtable", f"[{schema}].[{table_nm}]")
        .load()
    )
