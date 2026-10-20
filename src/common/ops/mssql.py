"""mssql-python connection. JDBC is not used here."""


def connect_sql_server(connection_string: str):
    from mssql_python import connect

    return connect(connection_string)
