"""Client completeness audit: CT head, loaded tables, counts, ready flag."""

import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from common.ops.ct_source import (
    fetch_ct_head,
    fetch_ct_status,
    fetch_pending_ct_counts,
    fetch_table_count_cache,
    fetch_table_watermarks,
    source_table_count,
)
from common.ops.ingest_metadata import (
    ensure_watermark_count_columns,
    record_process_log,
    upsert_completeness,
    upsert_table_counts,
)
from common.ops.mssql import connect_sql_server, sql_server_odbc
from common.ops.tables import (
    active_clients,
    active_tables,
    client_database_name,
    uc_table_path,
)


def log(message, client_name="audit"):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{client_name}] {message}", flush=True)


def table_status(watermark, min_valid, target_exists, pending_total, sql_count, delta_count):
    reasons = []
    if watermark is None:
        reasons.append("no watermark")
    elif min_valid is not None and watermark < min_valid:
        reasons.append(f"watermark {watermark} < min_valid {min_valid}")
    if not target_exists:
        reasons.append("target missing")
    if watermark is not None and int(pending_total or 0) > 0:
        reasons.append(f"pending={int(pending_total)}")
    loaded = not reasons
    if loaded and sql_count is None:
        reasons.append("count pending")
        count_ok = False
    else:
        count_ok = sql_count is not None and sql_count == delta_count
        if loaded and not count_ok:
            reasons.append(f"sql={sql_count} delta={delta_count}")
    return {
        "loaded": loaded,
        "ready": loaded and count_ok,
        "reason": "; ".join(reasons) or "ok",
    }


def new_audit_context(
    spark,
    login,
    sql_conn,
    uc_catalog,
    uc_schema_suffix,
    metadata_database,
    config_dir,
    job_group=1,
    freq_run=15,
    parallel_table_run=5,
    full_recon_sec=3600,
):
    return {
        "spark": spark,
        "login": login,
        "sql_conn": sql_conn,
        "schema": sql_conn.get("source_schema", "dbo"),
        "uc_catalog": uc_catalog,
        "uc_schema_suffix": uc_schema_suffix,
        "metadata_database": metadata_database,
        "config_dir": config_dir,
        "job_group": int(job_group),
        "freq_run": int(freq_run),
        "parallel_table_run": max(1, int(parallel_table_run)),
        "full_recon_sec": max(1, int(full_recon_sec)),
        "last_full_recon_at": None,
        "meta_conn": None,
        "meta_lock": threading.Lock(),
    }


def open_meta(ctx):
    if ctx["meta_conn"] is None:
        ctx["meta_conn"] = connect_sql_server(
            sql_server_odbc(ctx["login"], ctx["metadata_database"])
        )
        log(f"connected metadata {ctx['metadata_database']}")


def close_meta(ctx):
    conn = ctx.get("meta_conn")
    ctx["meta_conn"] = None
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def check_one_table(
    spark,
    src_conn,
    src_lock,
    schema,
    item,
    ct_head,
    watermarks,
    status,
    count_cache,
    count_mode,
    meta_conn=None,
    meta_lock=None,
    database_name=None,
):
    table_nm = item["table_nm"]
    info = status.get(table_nm) or {}
    watermark = watermarks.get(table_nm)
    min_valid = info.get("min_valid_version")
    target = item["target"]
    exists = spark.catalog.tableExists(target)
    pending_total = 0
    sql_count = None
    delta_count = None
    if not info.get("ct_enabled"):
        result = {
            "loaded": False,
            "ready": False,
            "reason": "change tracking disabled",
            "table_nm": table_nm,
            "watermark": watermark,
            "pending_total": None,
            "sql_row_count": None,
            "delta_row_count": None,
            "target": target,
        }
        return result
    if watermark is not None:
        with src_lock:
            pending = fetch_pending_ct_counts(
                src_conn, schema, table_nm, watermark, ct_head
            )
        pending_total = pending["pending_total"]
    result = table_status(watermark, min_valid, exists, pending_total, None, None)
    if result["loaded"] and count_mode == "full":
        with src_lock:
            sql_count = source_table_count(src_conn, schema, table_nm)
        delta_count = spark.table(target).count()
        if meta_conn is not None:
            lock = meta_lock or threading.Lock()
            with lock:
                upsert_table_counts(
                    meta_conn, database_name, schema, table_nm, sql_count, delta_count
                )
    elif result["loaded"]:
        cached = count_cache.get(table_nm) or {}
        sql_count = cached.get("sql_row_count")
        delta_count = cached.get("delta_row_count")
    result = table_status(
        watermark, min_valid, exists, pending_total, sql_count, delta_count
    )
    result.update(
        table_nm=table_nm,
        watermark=watermark,
        pending_total=pending_total,
        sql_row_count=sql_count,
        delta_row_count=delta_count,
        target=target,
    )
    return result


def audit_client(ctx, client, count_mode="cached"):
    started = time.time()
    client_name = client["client_name"]
    database_name = client_database_name(ctx["sql_conn"], client_name)
    schema = ctx["schema"]
    tables = active_tables(ctx["config_dir"], client_name)
    expected = len(tables)
    src_conn = connect_sql_server(sql_server_odbc(ctx["login"], database_name))
    src_lock = threading.Lock()
    try:
        ct_head = fetch_ct_head(src_conn)
        if ct_head is None:
            row = {
                "client_nm": client_name,
                "database_name": database_name,
                "ct_head_version": None,
                "expected_table_count": expected,
                "loaded_table_count": 0,
                "ready_table_count": 0,
                "sql_row_count": None,
                "delta_row_count": None,
                "duration_ms": int((time.time() - started) * 1000),
                "ready": False,
                "not_ready_tables": "change tracking is not enabled on the database",
                "count_mode": count_mode,
            }
            return row
        status = fetch_ct_status(src_conn, schema)
        with ctx["meta_lock"]:
            ensure_watermark_count_columns(ctx["meta_conn"])
            watermarks = fetch_table_watermarks(ctx["meta_conn"], database_name, schema)
            count_cache = fetch_table_count_cache(
                ctx["meta_conn"], database_name, schema
            )
        work = []
        for table in tables:
            table_nm = table["table_nm"]
            work.append(
                {
                    "table_nm": table_nm,
                    "target": uc_table_path(
                        ctx["uc_catalog"],
                        database_name,
                        ctx["uc_schema_suffix"],
                        table_nm,
                    ),
                }
            )

        def _run(item):
            try:
                ctx["spark"].sparkContext.setLocalProperty(
                    "spark.scheduler.pool", f"audit_{client_name}_{item['table_nm']}"
                )
            except Exception:
                pass
            return check_one_table(
                ctx["spark"],
                src_conn,
                src_lock,
                schema,
                item,
                ct_head,
                watermarks,
                status,
                count_cache,
                count_mode,
                meta_conn=ctx["meta_conn"],
                meta_lock=ctx["meta_lock"],
                database_name=database_name,
            )

        with ThreadPoolExecutor(max_workers=ctx["parallel_table_run"]) as pool:
            results = list(pool.map(_run, work))
    finally:
        try:
            src_conn.close()
        except Exception:
            pass

    loaded = [r for r in results if r["loaded"]]
    ready_tables = [r for r in results if r["ready"]]
    not_ready = [r for r in results if not r["ready"]]
    counted = [r for r in loaded if r["sql_row_count"] is not None]
    sql_total = sum(r["sql_row_count"] or 0 for r in counted)
    delta_total = sum(r["delta_row_count"] or 0 for r in counted)
    reasons = [f"{r['table_nm']}:{r['reason']}" for r in not_ready[:20]]
    if len(not_ready) > 20:
        reasons.append(f"... +{len(not_ready) - 20} more")
    return {
        "client_nm": client_name,
        "database_name": database_name,
        "ct_head_version": ct_head,
        "expected_table_count": expected,
        "loaded_table_count": len(loaded),
        "ready_table_count": len(ready_tables),
        "sql_row_count": sql_total if counted else None,
        "delta_row_count": delta_total if counted else None,
        "duration_ms": int((time.time() - started) * 1000),
        "ready": expected > 0 and len(ready_tables) == expected,
        "not_ready_tables": "; ".join(reasons) or None,
        "count_mode": count_mode,
    }


def publish(ctx, row):
    ready_label = "READY" if row["ready"] else "NOT_READY"
    mode = row.get("count_mode") or "cached"
    log(
        f"mode={mode} ct_head={row['ct_head_version']} "
        f"tables={row['loaded_table_count']}/{row['expected_table_count']} loaded "
        f"ready_tables={row['ready_table_count']} "
        f"sql_count={row['sql_row_count']} delta_count={row['delta_row_count']} "
        f"duration_ms={row['duration_ms']} ready={ready_label}",
        client_name=row["client_nm"],
    )
    if row.get("not_ready_tables"):
        log(f"not ready: {row['not_ready_tables']}", client_name=row["client_nm"])
    try:
        with ctx["meta_lock"]:
            upsert_completeness(ctx["meta_conn"], row)
            record_process_log(
                ctx["meta_conn"],
                "completeness",
                "ct_group_audit",
                ready_label,
                client_nm=row["client_nm"],
                database_name=row["database_name"],
                object_name=str(row["ct_head_version"] or ""),
                detail_status=(
                    f"{mode} {row['loaded_table_count']}/{row['expected_table_count']} loaded"
                ),
                rows_read=row.get("sql_row_count"),
                rows_written=row.get("delta_row_count"),
                log_message=row.get("not_ready_tables") or "ready",
            )
    except Exception as exc:
        log(f"publish failed: {exc}", client_name=row["client_nm"])


def due_full_recon(ctx, now=None):
    now = time.time() if now is None else now
    last = ctx.get("last_full_recon_at")
    if last is None:
        return True
    return now - float(last) >= ctx["full_recon_sec"]


def run_audit_cycle(ctx):
    open_meta(ctx)
    clients = active_clients(ctx["config_dir"], ctx["job_group"])
    if not clients:
        log(f"no active clients in job_group={ctx['job_group']}")
        return
    now = time.time()
    due = due_full_recon(ctx, now)
    count_mode = "full" if due else "cached"
    if due:
        ctx["last_full_recon_at"] = now
        log(f"full SQL vs Delta count recon (every {ctx['full_recon_sec']}s)")
    for client in clients:
        try:
            row = audit_client(ctx, client, count_mode=count_mode)
            publish(ctx, row)
        except Exception:
            log(
                f"audit error for {client['client_name']}\n{traceback.format_exc()}",
                client_name=client["client_name"],
            )


def run_forever(ctx):
    log(
        f"start job_group={ctx['job_group']} audit_freq={ctx['freq_run']}s "
        f"full_recon={ctx['full_recon_sec']}s "
        f"parallel_table_run={ctx['parallel_table_run']}"
    )
    while True:
        started = time.time()
        try:
            run_audit_cycle(ctx)
        except Exception:
            log(f"cycle error; reconnecting next cycle\n{traceback.format_exc()}")
            close_meta(ctx)
        time.sleep(max(0.0, ctx["freq_run"] - (time.time() - started)))
