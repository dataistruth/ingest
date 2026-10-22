"""Guarded reset helpers for a single client ingest."""


def reset_client_metadata(conn, database_name):
    """Delete one database's ingest history and watermarks in FK-safe order."""
    statements = [
        (
            "process_log",
            """
            DELETE p
            FROM dbo.process_log AS p
            WHERE p.database_name = ?
               OR p.group_id IN (
                   SELECT group_id FROM dbo.ingest_group WHERE database_name = ?
               );
            """,
            (database_name, database_name),
        ),
        (
            "ct_table_watermark",
            "DELETE FROM dbo.ct_table_watermark WHERE database_name = ?;",
            (database_name,),
        ),
        (
            "ct_db_watermark",
            "DELETE FROM dbo.ct_db_watermark WHERE database_name = ?;",
            (database_name,),
        ),
        (
            "recon_ready",
            "DELETE FROM dbo.recon_ready WHERE database_name = ?;",
            (database_name,),
        ),
        (
            "ingest_table_compare",
            """
            DELETE c
            FROM dbo.ingest_table_compare AS c
            INNER JOIN dbo.ingest_group AS g ON g.group_id = c.group_id
            WHERE g.database_name = ?;
            """,
            (database_name,),
        ),
        (
            "ingest_table_apply",
            """
            DELETE a
            FROM dbo.ingest_table_apply AS a
            INNER JOIN dbo.ingest_group AS g ON g.group_id = a.group_id
            WHERE g.database_name = ?;
            """,
            (database_name,),
        ),
        (
            "ingest_table_change",
            """
            DELETE c
            FROM dbo.ingest_table_change AS c
            INNER JOIN dbo.ingest_group AS g ON g.group_id = c.group_id
            WHERE g.database_name = ?;
            """,
            (database_name,),
        ),
        (
            "ingest_group",
            "DELETE FROM dbo.ingest_group WHERE database_name = ?;",
            (database_name,),
        ),
    ]
    deleted = {}
    cursor = conn.cursor()
    try:
        for name, sql, params in statements:
            print(
                f"METADATA SQL [{name}]: {' '.join(sql.split())} PARAMS={params}",
                flush=True,
            )
            cursor.execute(sql, params)
            deleted[name] = max(0, int(cursor.rowcount or 0))
            print(
                f"METADATA RESULT [{name}]: deleted_rows={deleted[name]}",
                flush=True,
            )
        conn.commit()
        print("METADATA TRANSACTION: COMMIT", flush=True)
        return deleted
    except Exception:
        conn.rollback()
        print("METADATA TRANSACTION: ROLLBACK", flush=True)
        raise
    finally:
        cursor.close()
