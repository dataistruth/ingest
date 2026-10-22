"""mssql-python writes to ipac_ingest_metadata."""

import uuid


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
    duration_ms=None,
    error_message=None,
):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE dbo.ingest_table_apply
            SET target_table = ?,
                applied_inserts = ?,
                applied_updates = ?,
                applied_deletes = ?,
                apply_status = ?,
                started_at = SYSUTCDATETIME(),
                completed_at = SYSUTCDATETIME(),
                duration_ms = ?,
                error_message = ?,
                recorded_at = SYSUTCDATETIME()
            WHERE group_id = ? AND source_schema = ? AND table_name = ?;
            """,
            (
                target_table or None,
                int(inserts),
                int(updates),
                int(deletes),
                status,
                duration_ms,
                (error_message or "")[:4000] or None,
                group_id,
                source_schema,
                table_name,
            ),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT dbo.ingest_table_apply (
                    group_id, source_schema, table_name, target_table,
                    applied_inserts, applied_updates, applied_deletes,
                    apply_status, started_at, completed_at, duration_ms, error_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME(), ?, ?);
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
                    duration_ms,
                    (error_message or "")[:4000] or None,
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
    result,
    recon_type=2,
):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE dbo.ingest_table_compare
            SET recon_type = ?,
                source_change_rows = ?,
                ingest_change_rows = ?,
                change_match = ?,
                sql_row_count = ?,
                delta_row_count = ?,
                count_match = ?,
                compare_status = ?,
                compare_message = ?,
                compared_at = SYSUTCDATETIME()
            WHERE group_id = ? AND source_schema = ? AND table_name = ?;
            """,
            (
                int(recon_type),
                result.get("source_change_rows"),
                result.get("ingest_change_rows"),
                result.get("change_match"),
                result.get("sql_row_count"),
                result.get("delta_row_count"),
                result.get("count_match"),
                result["status"],
                result.get("message"),
                group_id,
                source_schema,
                table_name,
            ),
        )
        if cursor.rowcount == 0:
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


def upsert_table_watermark(
    conn,
    database_name,
    source_schema,
    table_name,
    client_nm,
    last_version,
    group_id=None,
):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            MERGE dbo.ct_table_watermark WITH (HOLDLOCK) AS t
            USING (SELECT ? AS database_name, ? AS source_schema, ? AS table_name) AS s
              ON t.database_name = s.database_name
             AND t.source_schema = s.source_schema
             AND t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET
                last_version = ?, last_group_id = ?, client_nm = ?,
                updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                database_name, source_schema, table_name, client_nm,
                last_version, last_group_id
            ) VALUES (s.database_name, s.source_schema, s.table_name, ?, ?, ?);
            """,
            (
                database_name,
                source_schema,
                table_name,
                int(last_version),
                group_id,
                client_nm,
                client_nm,
                int(last_version),
                group_id,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def set_group_status(conn, group_id, status, error_message=None):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE dbo.ingest_group
            SET group_status = ?,
                error_message = ?,
                completed_at = CASE WHEN ? IN ('PASS', 'FAIL', 'CANCELLED')
                                    THEN SYSUTCDATETIME() END,
                updated_at = SYSUTCDATETIME()
            WHERE group_id = ?;
            """,
            (status, (error_message or "")[:4000] or None, status, group_id),
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
