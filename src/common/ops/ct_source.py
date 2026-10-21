"""mssql-python CT head and pending I/U/D counts. Not payload extract."""


def fetch_ct_status(conn, schema):
    """Map object name -> {type_desc, ct_enabled, min_valid_version}."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT o.name, o.type_desc,
                   CASE WHEN ctt.object_id IS NULL THEN 0 ELSE 1 END,
                   CASE WHEN ctt.object_id IS NULL THEN NULL
                        ELSE CHANGE_TRACKING_MIN_VALID_VERSION(o.object_id) END
            FROM sys.objects AS o
            INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            LEFT JOIN sys.change_tracking_tables AS ctt
              ON ctt.object_id = o.object_id
            WHERE s.name = ?
              AND o.type IN ('U', 'V', 'SN');
            """,
            (schema,),
        )
        out = {}
        for name, type_desc, ct_on, min_valid in cursor.fetchall():
            out[str(name)] = {
                "type_desc": str(type_desc or ""),
                "ct_enabled": bool(ct_on),
                "min_valid_version": None if min_valid is None else int(min_valid),
            }
        return out
    finally:
        cursor.close()


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


def fetch_table_watermarks(conn, database_name, schema):
    """Map table name -> last_version from ipac_ingest_metadata."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT table_name, last_version
            FROM dbo.ct_table_watermark
            WHERE database_name = ? AND source_schema = ?;
            """,
            (database_name, schema),
        )
        return {str(name): int(version) for name, version in cursor.fetchall()}
    finally:
        cursor.close()


def source_table_count(conn, schema, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT_BIG(*) FROM [{schema}].[{table_name}];")
        return int(cursor.fetchone()[0] or 0)
    finally:
        cursor.close()
