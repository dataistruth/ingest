"""Prepare and reset Unity Catalog Delta targets."""


def quote_name(name):
    return ".".join(f"`{part}`" for part in str(name).split("."))


def ordered_columns(columns, first_columns):
    """Return existing column names with configured LQ columns first."""
    first_columns = list(first_columns or [])
    if not first_columns:
        return [str(column) for column in columns]
    actual = {str(column).casefold(): str(column) for column in columns}
    first = []
    missing = []
    for column in first_columns:
        match = actual.get(str(column).casefold())
        if match is None:
            missing.append(str(column))
        elif match not in first:
            first.append(match)
    if missing:
        raise ValueError(
            "configured LQ column(s) not found in source: "
            + ", ".join(missing)
        )
    first_names = {column.casefold() for column in first}
    return first + [
        str(column)
        for column in columns
        if str(column).casefold() not in first_names
    ]


def lq_first(df, lq_columns):
    """Move LQ columns to the front so Delta collects their statistics."""
    if not lq_columns:
        return df
    return df.select(*ordered_columns(df.columns, lq_columns))


def drop_target_tables(spark, targets, verbose=False):
    dropped = []
    for target in targets:
        sql = f"DROP TABLE IF EXISTS {quote_name(target)}"
        if verbose:
            print(f"UC SQL: {sql}", flush=True)
        spark.sql(sql)
        dropped.append(target)
    return dropped
