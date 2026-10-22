"""SCD1 apply into Delta: latest CT change per key, delete on D."""

import uuid

from common.ops.target import lq_first, quote_name

# Liquid clustering accepts at most 4 columns.
MAX_CLUSTER_COLS = 4
CT_COLS = ("sys_change_operation", "sys_change_version")


def split_keys(value):
    return [k.strip() for k in str(value or "").split(",") if k.strip()]


def cluster_cols(keys):
    return list(keys)[:MAX_CLUSTER_COLS]


def view_name(prefix):
    return f"_{prefix}_{uuid.uuid4().hex}"


def create_table_from_df(spark, target, df, cluster_keys, lq_keys=None):
    lq_keys = list(lq_keys if lq_keys is not None else cluster_keys or [])
    if lq_keys:
        df = lq_first(df, lq_keys)
    view = view_name("snap")
    df.createOrReplaceTempView(view)
    cluster_sql = ""
    if cluster_keys:
        cluster_sql = " CLUSTER BY (" + ", ".join(f"`{c}`" for c in cluster_keys) + ")"
    try:
        spark.sql(
            f"CREATE OR REPLACE TABLE {quote_name(target)}{cluster_sql} "
            f"AS SELECT * FROM {view}"
        )
    finally:
        spark.catalog.dropTempView(view)


def create_auto_cdc_flow(
    spark,
    target,
    source,
    keys,
    sequence_by="sys_change_version",
    apply_as_deletes="s.sys_change_operation = 'D'",
):
    skip = {c.casefold() for c in list(keys) + list(CT_COLS) + [sequence_by, "rn"]}
    data_cols = [c for c in source.columns if c.casefold() not in skip]
    key_sql = ", ".join(f"`{k}`" for k in keys)
    on_sql = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    set_sql = ", ".join(f"t.`{c}` = s.`{c}`" for c in data_cols)
    insert_cols = list(keys) + data_cols
    insert_list = ", ".join(f"`{c}`" for c in insert_cols)
    insert_vals = ", ".join(f"s.`{c}`" for c in insert_cols)
    update_sql = (
        f"WHEN MATCHED AND NOT ({apply_as_deletes}) THEN UPDATE SET {set_sql}"
        if data_cols
        else ""
    )

    src_view = view_name("cdc_src")
    latest_view = view_name("cdc_latest")
    source.createOrReplaceTempView(src_view)
    try:
        spark.sql(
            f"""
            CREATE OR REPLACE TEMP VIEW {latest_view} AS
            SELECT * FROM (
              SELECT *,
                     ROW_NUMBER() OVER (
                       PARTITION BY {key_sql}
                       ORDER BY `{sequence_by}` DESC
                     ) AS rn
              FROM {src_view}
            )
            WHERE rn = 1
            """
        )
        spark.sql(
            f"""
            MERGE INTO {quote_name(target)} AS t
            USING {latest_view} AS s
            ON {on_sql}
            WHEN MATCHED AND ({apply_as_deletes}) THEN DELETE
            {update_sql}
            WHEN NOT MATCHED AND NOT ({apply_as_deletes}) THEN
              INSERT ({insert_list}) VALUES ({insert_vals})
            """
        )
    finally:
        spark.catalog.dropTempView(latest_view)
        spark.catalog.dropTempView(src_view)
