"""SCD1 MERGE for CT ingest (latest version per key, delete on D)."""


def create_auto_cdc_flow(
    spark,
    target,
    source,
    keys,
    sequence_by="sys_change_version",
    apply_as_deletes="s.sys_change_operation = 'D'",
):
    skip = {c.casefold() for c in list(keys) + [sequence_by, "sys_change_operation", "__cdc_rn", "rn"]}
    data_cols = [c for c in source.columns if c.casefold() not in skip]
    key_sql = ", ".join(f"`{k}`" for k in keys)
    on_sql = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    set_sql = ", ".join(f"t.`{c}` = s.`{c}`" for c in data_cols)
    insert_cols = list(keys) + data_cols
    insert_list = ", ".join(f"`{c}`" for c in insert_cols)
    insert_vals = ", ".join(f"s.`{c}`" for c in insert_cols)

    source.createOrReplaceTempView("_cdc_src")
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW _cdc_latest AS
        SELECT * FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY {key_sql}
                   ORDER BY `{sequence_by}` DESC
                 ) AS rn
          FROM _cdc_src
        )
        WHERE rn = 1
        """
    )
    spark.sql(
        f"""
        MERGE INTO {target} AS t
        USING _cdc_latest AS s
        ON {on_sql}
        WHEN MATCHED AND ({apply_as_deletes}) THEN DELETE
        WHEN MATCHED AND NOT ({apply_as_deletes}) THEN UPDATE SET {set_sql}
        WHEN NOT MATCHED AND NOT ({apply_as_deletes}) THEN
          INSERT ({insert_list}) VALUES ({insert_vals})
        """
    )
