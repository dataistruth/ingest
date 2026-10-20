"""mssql-python CT head and pending I/U/D counts. Not payload extract."""


def fetch_ct_head(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION();")
        row = cursor.fetchone()
        return None if row is None or row[0] is None else int(row[0])
    finally:
        cursor.close()


def fetch_pending_ct_counts(conn, schema, table_name, watermark_before, ct_head):
    sql = f"""
    SELECT SYS_CHANGE_OPERATION, COUNT_BIG(*)
    FROM CHANGETABLE(CHANGES [{schema}].[{table_name}], {int(watermark_before)}) AS ct
    WHERE ct.SYS_CHANGE_OPERATION IN ('I', 'U', 'D')
      AND ct.SYS_CHANGE_VERSION <= {int(ct_head)}
    GROUP BY SYS_CHANGE_OPERATION
    """
    cursor = conn.cursor()
    inserts = updates = deletes = 0
    try:
        cursor.execute(sql)
        for op, cnt in cursor.fetchall():
            code = str(op or "").strip().upper()
            n = int(cnt or 0)
            if code == "I":
                inserts = n
            elif code == "U":
                updates = n
            elif code == "D":
                deletes = n
    finally:
        cursor.close()
    return {
        "source_schema": schema,
        "table_name": table_name,
        "watermark_before": int(watermark_before),
        "ct_head_version": int(ct_head),
        "pending_inserts": inserts,
        "pending_updates": updates,
        "pending_deletes": deletes,
        "pending_total": inserts + updates + deletes,
    }


def fetch_table_watermark(conn, database_name, schema, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT last_version
            FROM dbo.ct_table_watermark
            WHERE database_name = ? AND source_schema = ? AND table_name = ?;
            """,
            (database_name, schema, table_name),
        )
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        cursor.close()


def source_table_count(conn, schema, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT_BIG(*) FROM [{schema}].[{table_name}];")
        return int(cursor.fetchone()[0] or 0)
    finally:
        cursor.close()
