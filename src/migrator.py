import pandas as pd
import pyodbc
import mysql.connector


MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Chandan@199171",
    "database": "migration_test"
}


# -------------------------
# LOADERS
# -------------------------
def load_csv(csv_path):
    return pd.read_csv(csv_path)


def load_mssql(server, database, query, auth_type, username=None, password=None):
    if auth_type == "Windows":
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )

    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# -------------------------
# TYPE INFERENCE
# -------------------------
def infer_mysql_type(series):
    if pd.api.types.is_integer_dtype(series):
        return "INT"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT"
    if pd.to_datetime(series, errors="coerce").notna().any():
        return "DATETIME"
    return "VARCHAR(255)"


# -------------------------
# SQL GENERATION
# -------------------------
def generate_create_table(df, table):
    cols = []
    for col in df.columns:
        col_type = infer_mysql_type(df[col])
        nullable = "NULL" if df[col].isnull().any() else "NOT NULL"
        cols.append(f"`{col}` {col_type} {nullable}")

    return f"""CREATE TABLE IF NOT EXISTS `{table}` (
{', '.join(cols)}
);"""


def format_value(v):
    if pd.isna(v):
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def generate_insert_sql(df, table):
    columns = ", ".join(f"`{c}`" for c in df.columns)
    rows = [
        "(" + ", ".join(format_value(v) for v in row) + ")"
        for _, row in df.iterrows()
    ]

    return f"""INSERT INTO `{table}` ({columns})
VALUES
{', '.join(rows)};"""


# -------------------------
# MYSQL EXECUTION
# -------------------------
def execute_mysql(sql):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    for stmt in sql.split(";"):
        if stmt.strip():
            cursor.execute(stmt)

    conn.commit()
    cursor.close()
    conn.close()


# -------------------------
# MAIN ENTRY
# -------------------------
def migrate_dataframe(df, table_name):
    create_sql = generate_create_table(df, table_name)
    insert_sql = generate_insert_sql(df, table_name)

    # Execute
    execute_mysql(create_sql)
    execute_mysql(insert_sql)

    # Return EVERYTHING needed
    return {
        "table": table_name,
        "rows": len(df),
        "columns": len(df.columns),
        "sql": create_sql + "\n\n" + insert_sql
    }
