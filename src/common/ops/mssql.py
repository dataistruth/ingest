"""mssql-python connection. JDBC is not used here."""

import importlib.util
import subprocess
import sys


def connect_sql_server(connection_string: str):
    if importlib.util.find_spec("mssql_python") is None:
        print("mssql_python not found; installing mssql-python>=1.13.0")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-q", "mssql-python>=1.13.0"]
        )
        importlib.invalidate_caches()
    from mssql_python import connect

    return connect(connection_string)


def sql_server_odbc(login, database):
    # mssql-python owns the driver; do not pass Driver=
    return (
        f"Server={login['server']},{login['port']};"
        f"Database={database};"
        f"UID={login['username']};PWD={login['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )


def sql_server_jdbc_url(login, database):
    return (
        f"jdbc:sqlserver://{login['server']}:{login['port']};"
        f"databaseName={database}"
    )
