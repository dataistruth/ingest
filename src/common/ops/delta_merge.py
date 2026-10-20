"""Apply JDBC CT change sets into Delta (SCD1 default)."""

from __future__ import annotations

from typing import Any, Sequence


def merge_ct_changes_scd1(
    spark: Any,
    changes_df: Any,
    target_table: str,
    pk_cols: Sequence[str],
    op_col: str = "sys_change_operation",
) -> dict[str, int]:
    """
    MERGE I/U upserts and D deletes into target_table.

    Expects changes_df to include PK columns + op_col + business columns.
    """
    if not pk_cols:
        raise ValueError("pk_cols required for MERGE")

    changes_df.createOrReplaceTempView("_ct_changes")
    on_pred = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pk_cols)

    # Prefer latest SYS_CHANGE_VERSION per PK if duplicates in the window.
    dedupe_sql = f"""
    SELECT * FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY {", ".join(f"`{c}`" for c in pk_cols)}
               ORDER BY sys_change_version DESC
             ) AS rn
      FROM _ct_changes
    ) WHERE rn = 1
    """
    spark.sql(dedupe_sql).createOrReplaceTempView("_ct_latest")

    merge_sql = f"""
    MERGE INTO {target_table} AS t
    USING _ct_latest AS s
    ON {on_pred}
    WHEN MATCHED AND s.{op_col} = 'D' THEN DELETE
    WHEN MATCHED AND s.{op_col} IN ('I', 'U') THEN UPDATE SET *
    WHEN NOT MATCHED AND s.{op_col} IN ('I', 'U') THEN INSERT *
    """
    spark.sql(merge_sql)

    counts = spark.sql(
        f"""
        SELECT
          SUM(CASE WHEN {op_col} = 'I' THEN 1 ELSE 0 END) AS inserts,
          SUM(CASE WHEN {op_col} = 'U' THEN 1 ELSE 0 END) AS updates,
          SUM(CASE WHEN {op_col} = 'D' THEN 1 ELSE 0 END) AS deletes
        FROM _ct_latest
        """
    ).collect()[0]
    return {
        "inserts": int(counts["inserts"] or 0),
        "updates": int(counts["updates"] or 0),
        "deletes": int(counts["deletes"] or 0),
    }
