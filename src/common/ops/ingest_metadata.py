"""mssql-python writes to ipac_ingest_metadata."""

import uuid


def compare_counts(
    recon_type,
    source_change_rows,
    ingest_change_rows,
    sql_row_count=None,
    delta_row_count=None,
):
    if recon_type == 1:
        return {"status": "PASS", "change_match": None, "count_match": None}
    change_match = source_change_rows == ingest_change_rows
    count_match = None
    if recon_type == 2:
        count_match = sql_row_count == delta_row_count
        ok = change_match and count_match
    else:
        ok = change_match
    return {
        "status": "PASS" if ok else "FAIL",
        "change_match": change_match,
        "count_match": count_match,
    }


def create_group(
    conn,
    client_nm,
    database_name,
    source_schema,
    ct_watermark_before,
    ct_head_version,
    changed_tables,
    controller_job_run_id="",
):
    group_id = str(uuid.uuid4())
    total = sum(int(t.get("pending_total") or 0) for t in changed_tables)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT group_id FROM dbo.ingest_group
            WHERE database_name = ? AND ct_head_version = ?;
            """,
            (database_name, int(ct_head_version)),
        )
        existing = cursor.fetchone()
        if existing:
            conn.commit()
            return str(existing[0])
        cursor.execute(
            """
            INSERT dbo.ingest_group (
                group_id, client_nm, database_name, source_schema,
                ct_watermark_before, ct_head_version,
                expected_table_count, batch_total_volume, group_status,
                controller_job_run_id, discovered_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'DISCOVERED', ?, SYSUTCDATETIME());
            """,
            (
                group_id,
                client_nm,
                database_name,
                source_schema,
                int(ct_watermark_before),
                int(ct_head_version),
                len(changed_tables),
                total,
                controller_job_run_id or None,
            ),
        )
        for table in changed_tables:
            cursor.execute(
                """
                INSERT dbo.ingest_table_change (
                    group_id, source_schema, table_name,
                    watermark_before, ct_head_version,
                    pending_inserts, pending_updates, pending_deletes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    group_id,
                    table["source_schema"],
                    table["table_name"],
                    int(table["watermark_before"]),
                    int(table["ct_head_version"]),
                    int(table["pending_inserts"]),
                    int(table["pending_updates"]),
                    int(table["pending_deletes"]),
                ),
            )
        conn.commit()
        return group_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def record_table_apply(
    conn,
    group_id,
    source_schema,
    table_name,
    inserts,
    updates,
    deletes,
    status,
    target_table="",
):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT dbo.ingest_table_apply (
                group_id, source_schema, table_name, target_table,
                applied_inserts, applied_updates, applied_deletes,
                apply_status, started_at, completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
            (
                group_id,
                source_schema,
                table_name,
                target_table or None,
                int(inserts),
                int(updates),
                int(deletes),
                status,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def record_table_compare(
    conn,
    group_id,
    source_schema,
    table_name,
    recon_type,
    result,
):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT dbo.ingest_table_compare (
                group_id, source_schema, table_name, recon_type,
                source_change_rows, ingest_change_rows, change_match,
                sql_row_count, delta_row_count, count_match,
                compare_status, compare_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                group_id,
                source_schema,
                table_name,
                int(recon_type),
                result.get("source_change_rows"),
                result.get("ingest_change_rows"),
                result.get("change_match"),
                result.get("sql_row_count"),
                result.get("delta_row_count"),
                result.get("count_match"),
                result["status"],
                result.get("message"),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def complete_group(conn, group_id):
    cursor = conn.cursor()
    try:
        cursor.execute("EXEC dbo.usp_complete_ingest_group @group_id = ?;", (group_id,))
        row = cursor.fetchone()
        conn.commit()
        return bool(row and row[0])
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
