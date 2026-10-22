"""mssql-python writes to ipac_common_metdata."""

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


def ensure_completeness_table(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            IF OBJECT_ID(N'dbo.ingest_completeness', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.ingest_completeness (
                    database_name            sysname          NOT NULL,
                    client_nm                sysname          NOT NULL,
                    ct_head_version          bigint           NULL,
                    expected_table_count     int              NOT NULL,
                    loaded_table_count       int              NOT NULL,
                    ready_table_count        int              NOT NULL,
                    sql_row_count            bigint           NULL,
                    delta_row_count          bigint           NULL,
                    duration_ms              bigint           NULL,
                    ready                    bit              NOT NULL,
                    not_ready_tables         nvarchar(4000)   NULL,
                    checked_at               datetime2(3)     NOT NULL
                        CONSTRAINT DF_ingest_completeness_checked DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT PK_ingest_completeness PRIMARY KEY CLUSTERED (database_name)
                );
            END;
            """
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def upsert_completeness(conn, row):
    ensure_completeness_table(conn)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            MERGE dbo.ingest_completeness WITH (HOLDLOCK) AS t
            USING (SELECT ? AS database_name) AS s
              ON t.database_name = s.database_name
            WHEN MATCHED THEN UPDATE SET
                client_nm = ?, ct_head_version = ?,
                expected_table_count = ?, loaded_table_count = ?,
                ready_table_count = ?, sql_row_count = ?, delta_row_count = ?,
                duration_ms = ?, ready = ?, not_ready_tables = ?,
                checked_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                database_name, client_nm, ct_head_version,
                expected_table_count, loaded_table_count, ready_table_count,
                sql_row_count, delta_row_count, duration_ms, ready, not_ready_tables
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                row["database_name"],
                row["client_nm"],
                row.get("ct_head_version"),
                int(row["expected_table_count"]),
                int(row["loaded_table_count"]),
                int(row["ready_table_count"]),
                row.get("sql_row_count"),
                row.get("delta_row_count"),
                row.get("duration_ms"),
                1 if row["ready"] else 0,
                (row.get("not_ready_tables") or "")[:4000] or None,
                row["database_name"],
                row["client_nm"],
                row.get("ct_head_version"),
                int(row["expected_table_count"]),
                int(row["loaded_table_count"]),
                int(row["ready_table_count"]),
                row.get("sql_row_count"),
                row.get("delta_row_count"),
                row.get("duration_ms"),
                1 if row["ready"] else 0,
                (row.get("not_ready_tables") or "")[:4000] or None,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def record_process_log(
    conn,
    process_type,
    process_name,
    current_status,
    client_nm="",
    database_name="",
    object_name="",
    detail_status="",
    rows_read=None,
    rows_written=None,
    start_time=None,
    end_time=None,
    log_message="",
):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT dbo.process_log (
                process_type, process_name, artifact_type,
                client_nm, database_name, object_name,
                current_status, detail_status,
                rows_read, rows_written,
                start_time, end_time, log_message
            )
            VALUES (?, ?, 'job', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                process_type[:20],
                process_name[:128],
                client_nm or None,
                database_name or None,
                object_name or None,
                current_status[:20],
                (detail_status or "")[:128] or None,
                rows_read,
                rows_written,
                start_time,
                end_time,
                (log_message or "")[:4000] or None,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

