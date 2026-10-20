"""mssql-python connection. JDBC is not used here."""


def connect_sql_server(connection_string: str):
    from mssql_python import connect

    return connect(connection_string)


def sql_server_odbc(login, database):
    return (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
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
