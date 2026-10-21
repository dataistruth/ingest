"""Per-table recon after apply: CT change rows vs applied, SQL COUNT(*) vs Delta."""

from common.ops.ct_source import source_table_count


def compare_counts(
    source_change_rows,
    ingest_change_rows,
    sql_row_count=None,
    delta_row_count=None,
):
    change_match = source_change_rows == ingest_change_rows
    count_match = None if sql_row_count is None else sql_row_count == delta_row_count
    ok = change_match and count_match is not False
    return {
        "status": "PASS" if ok else "FAIL",
        "change_match": change_match,
        "count_match": count_match,
    }


def reconcile_table(
    spark,
    src_conn,
    src_lock,
    schema,
    table_nm,
    target,
    source_change_rows,
    ingest_change_rows,
):
    with src_lock:
        sql_count = source_table_count(src_conn, schema, table_nm)
    delta_count = spark.table(target).count()
    result = compare_counts(source_change_rows, ingest_change_rows, sql_count, delta_count)
    result.update(
        source_change_rows=source_change_rows,
        ingest_change_rows=ingest_change_rows,
        sql_row_count=sql_count,
        delta_row_count=delta_count,
    )
    if result["status"] != "PASS":
        result["message"] = (
            f"ct={source_change_rows} applied={ingest_change_rows} "
            f"sql={sql_count} delta={delta_count}"
        )
    return result
