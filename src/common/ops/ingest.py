"""Single-client CT ingest loop: probe CT, then snapshot or SCD1-merge changed tables."""

import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from common.ops import cdc, recon
from common.ops.ct_source import (
    fetch_ct_head,
    fetch_ct_status,
    fetch_pending_ct_counts,
    fetch_table_watermarks,
)
from common.ops.ingest_metadata import (
    create_group,
    record_table_apply,
    record_table_compare,
    set_group_status,
    upsert_table_counts,
    upsert_table_watermark,
)
from common.ops.jdbc_extract import read_ct_changes, read_table_snapshot
from common.ops.mssql import connect_sql_server, sql_server_jdbc_url, sql_server_odbc
from common.ops.tables import active_tables, uc_table_path


def log(ctx, message):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{ctx['client_name']}] {message}", flush=True)


def new_context(
    spark,
    login,
    client_name,
    database_name,
    schema,
    uc_catalog,
    uc_schema_suffix,
    metadata_database,
    config_dir,
    freq_run=10,
    parallel_table_run=5,
):
    return {
        "spark": spark,
        "login": login,
        "client_name": client_name,
        "database_name": database_name,
        "schema": schema,
        "uc_catalog": uc_catalog,
        "uc_schema_suffix": uc_schema_suffix,
        "metadata_database": metadata_database,
        "config_dir": config_dir,
        "freq_run": int(freq_run),
        "parallel_table_run": max(1, int(parallel_table_run)),
        "jdbc_url": sql_server_jdbc_url(login, database_name),
        # mssql-python connections are not thread-safe; workers share them under locks.
        "src_conn": None,
        "meta_conn": None,
        "src_lock": threading.Lock(),
        "meta_lock": threading.Lock(),
        "known_targets": set(),
        "probed_head": {},
        "idle_head": None,
        "skip_logged": set(),
    }


def open_connections(ctx):
    if ctx["src_conn"] is None:
        ctx["src_conn"] = connect_sql_server(
            sql_server_odbc(ctx["login"], ctx["database_name"])
        )
        log(ctx, f"connected source {ctx['database_name']}")
    if ctx["meta_conn"] is None:
        ctx["meta_conn"] = connect_sql_server(
            sql_server_odbc(ctx["login"], ctx["metadata_database"])
        )
        log(ctx, f"connected metadata {ctx['metadata_database']}")


def close_connections(ctx):
    for key in ("src_conn", "meta_conn"):
        conn = ctx.get(key)
        ctx[key] = None
        if conn is None:
            continue
        try:
            conn.close()
        except Exception:
            pass


def target_exists(ctx, target):
    if target in ctx["known_targets"]:
        return True
    if ctx["spark"].catalog.tableExists(target):
        ctx["known_targets"].add(target)
        return True
    return False


def skip_once(ctx, table_nm, reason):
    if table_nm not in ctx["skip_logged"]:
        ctx["skip_logged"].add(table_nm)
        log(ctx, f"skip {table_nm}: {reason}")


def plan_tables(ctx, ct_head):
    """Return tables to snapshot (no watermark / stale / no target) or merge (pending CT)."""
    schema = ctx["schema"]
    status = fetch_ct_status(ctx["src_conn"], schema)
    watermarks = fetch_table_watermarks(ctx["meta_conn"], ctx["database_name"], schema)
    work = []
    for table in active_tables(ctx["config_dir"], ctx["client_name"]):
        table_nm = table["table_nm"]
        info = status.get(table_nm)
        if info is None:
            skip_once(ctx, table_nm, "not found in source")
            continue
        if info["type_desc"] != "USER_TABLE":
            skip_once(ctx, table_nm, f"{info['type_desc']} is not a table")
            continue
        if not info["ct_enabled"]:
            skip_once(ctx, table_nm, "change tracking disabled")
            continue
        keys = cdc.split_keys(table.get("primary_key_cols"))
        if not keys:
            skip_once(ctx, table_nm, "no primary_key_cols")
            continue

        target = uc_table_path(
            ctx["uc_catalog"], ctx["database_name"], ctx["uc_schema_suffix"], table_nm
        )
        lq_keys = cdc.split_keys(table.get("lq_key"))
        cluster = cdc.cluster_cols(lq_keys)
        item = {
            "table_nm": table_nm,
            "keys": keys,
            "lq_keys": lq_keys,
            "cluster": cluster,
            "target": target,
        }

        stored = watermarks.get(table_nm)
        min_valid = info["min_valid_version"]
        if stored is None:
            reason = "no watermark"
        elif min_valid is not None and stored < min_valid:
            reason = f"watermark {stored} < min_valid {min_valid}"
        elif not target_exists(ctx, target):
            reason = "target missing"
        else:
            reason = None
        if reason:
            work.append({**item, "mode": "snapshot", "reason": reason, "watermark_before": 0})
            continue

        watermark = max(stored, ctx["probed_head"].get(table_nm, stored))
        if watermark >= ct_head:
            continue
        pending = fetch_pending_ct_counts(ctx["src_conn"], schema, table_nm, watermark, ct_head)
        if pending["pending_total"] == 0:
            ctx["probed_head"][table_nm] = ct_head
            continue
        work.append({**item, "mode": "cdc", "watermark_before": watermark, "pending": pending})
    return work


def change_row(ctx, item, ct_head):
    pending = item.get("pending") or {}
    return {
        "source_schema": ctx["schema"],
        "table_name": item["table_nm"],
        "watermark_before": item["watermark_before"],
        "ct_head_version": ct_head,
        "pending_inserts": pending.get("pending_inserts", 0),
        "pending_updates": pending.get("pending_updates", 0),
        "pending_deletes": pending.get("pending_deletes", 0),
        "pending_total": pending.get("pending_total", 0),
    }


def safe_meta(ctx, what, fn, *args, **kwargs):
    """Audit writes never block ingest; failures are logged."""
    try:
        with ctx["meta_lock"]:
            return fn(ctx["meta_conn"], *args, **kwargs)
    except Exception as exc:
        log(ctx, f"metadata {what} failed: {exc}")
        return None


def apply_snapshot(ctx, item):
    spark = ctx["spark"]
    login = ctx["login"]
    df = read_table_snapshot(
        spark, ctx["jdbc_url"], login["username"], login["password"],
        ctx["schema"], item["table_nm"],
    )
    cdc.create_table_from_df(
        spark,
        item["target"],
        df,
        item["cluster"],
        item["lq_keys"],
    )
    ctx["known_targets"].add(item["target"])
    rows = spark.table(item["target"]).count()
    return {"I": rows, "U": 0, "D": 0}


def apply_cdc(ctx, item, ct_head):
    spark = ctx["spark"]
    login = ctx["login"]
    df = read_ct_changes(
        spark, ctx["jdbc_url"], login["username"], login["password"],
        ctx["schema"], item["table_nm"], item["keys"], item["watermark_before"], ct_head,
    ).persist()
    try:
        counts = {"I": 0, "U": 0, "D": 0}
        for row in df.groupBy("sys_change_operation").count().collect():
            counts[str(row[0]).strip().upper()] = int(row[1])
        cdc.cdc_merge(spark, item["target"], df, item["keys"])
        return counts
    finally:
        df.unpersist()


def run_table(ctx, item, ct_head, group_id):
    table_nm = item["table_nm"]
    started = time.time()
    try:
        ctx["spark"].sparkContext.setLocalProperty(
            "spark.scheduler.pool", f"{ctx['client_name']}_{table_nm}"
        )
    except Exception:
        pass

    result = {"table_nm": table_nm, "mode": item["mode"], "status": "SUCCESS", "error": None}
    try:
        if item["mode"] == "snapshot":
            counts = apply_snapshot(ctx, item)
            expected = counts["I"]
        else:
            counts = apply_cdc(ctx, item, ct_head)
            expected = item["pending"]["pending_total"]
        with ctx["meta_lock"]:
            upsert_table_watermark(
                ctx["meta_conn"], ctx["database_name"], ctx["schema"], table_nm,
                ctx["client_name"], ct_head, group_id,
            )
        ctx["probed_head"][table_nm] = ct_head
        result["counts"] = counts
    except Exception as exc:
        result["status"] = "FAIL"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["counts"] = {"I": 0, "U": 0, "D": 0}

    if result["status"] == "SUCCESS":
        try:
            result["recon"] = recon.reconcile_table(
                ctx["spark"], ctx["src_conn"], ctx["src_lock"], ctx["schema"], table_nm,
                item["target"], expected, sum(counts.values()),
            )
        except Exception as exc:
            result["recon"] = {"status": "FAIL", "message": f"recon error: {exc}"}
        rec = result.get("recon") or {}
        if rec.get("sql_row_count") is not None:
            with ctx["meta_lock"]:
                upsert_table_counts(
                    ctx["meta_conn"],
                    ctx["database_name"],
                    ctx["schema"],
                    table_nm,
                    rec.get("sql_row_count"),
                    rec.get("delta_row_count"),
                )

    result["duration_ms"] = int((time.time() - started) * 1000)
    if group_id:
        c = result["counts"]
        safe_meta(
            ctx, f"apply {table_nm}", record_table_apply,
            group_id, ctx["schema"], table_nm, c["I"], c["U"], c["D"], result["status"],
            target_table=item["target"], duration_ms=result["duration_ms"],
            error_message=result["error"],
        )
        if result.get("recon"):
            safe_meta(
                ctx, f"compare {table_nm}", record_table_compare,
                group_id, ctx["schema"], table_nm, result["recon"],
            )
    return result


def describe(item):
    if item["mode"] == "snapshot":
        return f"{item['table_nm']} snapshot ({item['reason']})"
    p = item["pending"]
    return (
        f"{item['table_nm']} cdc wm={item['watermark_before']} "
        f"I={p['pending_inserts']} U={p['pending_updates']} D={p['pending_deletes']}"
    )


def run_cycle(ctx):
    open_connections(ctx)
    with ctx["src_lock"]:
        ct_head = fetch_ct_head(ctx["src_conn"])
    if ct_head is None:
        log(ctx, "change tracking is not enabled on the database")
        return 0
    if ct_head == ctx["idle_head"]:
        return 0

    with ctx["src_lock"], ctx["meta_lock"]:
        work = plan_tables(ctx, ct_head)
    if not work:
        ctx["idle_head"] = ct_head
        log(ctx, f"ct_head={ct_head} no pending changes")
        return 0

    log(ctx, f"ct_head={ct_head} tables to apply={len(work)}")
    for item in work:
        log(ctx, f"  {describe(item)}")

    group_id = safe_meta(
        ctx, "create_group", create_group,
        ctx["client_name"], ctx["database_name"], ctx["schema"],
        min(item["watermark_before"] for item in work), ct_head,
        [change_row(ctx, item, ct_head) for item in work],
    )
    if group_id:
        safe_meta(ctx, "group RUNNING", set_group_status, group_id, "RUNNING")

    with ThreadPoolExecutor(max_workers=ctx["parallel_table_run"]) as pool:
        results = list(pool.map(lambda item: run_table(ctx, item, ct_head, group_id), work))

    failed = [r for r in results if r["status"] != "SUCCESS"]
    for r in results:
        c = r["counts"]
        rec = r.get("recon") or {}
        line = (
            f"  {r['status']} {r['table_nm']} {r['mode']} "
            f"I={c['I']} U={c['U']} D={c['D']} {r['duration_ms']}ms"
        )
        if rec:
            line += f" recon={rec['status']}"
            if rec.get("message"):
                line += f" ({rec['message']})"
        if r["error"]:
            line += f" error={r['error']}"
        log(ctx, line)

    if group_id:
        error = "; ".join(f"{r['table_nm']}: {r['error']}" for r in failed) or None
        safe_meta(
            ctx, "group status", set_group_status,
            group_id, "FAIL" if failed else "PASS", error,
        )
    if not failed:
        ctx["idle_head"] = ct_head
    log(ctx, f"cycle done ct_head={ct_head} ok={len(results) - len(failed)} failed={len(failed)}")
    return len(results)


def run_forever(ctx):
    """Poll every freq_run seconds until the job is cancelled."""
    log(
        ctx,
        f"start db={ctx['database_name']} freq_run={ctx['freq_run']}s "
        f"parallel_table_run={ctx['parallel_table_run']}",
    )
    while True:
        started = time.time()
        try:
            run_cycle(ctx)
        except Exception:
            log(ctx, f"cycle error; reconnecting next cycle\n{traceback.format_exc()}")
            close_connections(ctx)
        time.sleep(max(0.0, ctx["freq_run"] - (time.time() - started)))
