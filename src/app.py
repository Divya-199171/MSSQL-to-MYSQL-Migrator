import streamlit as st
import pandas as pd
from pathlib import Path
from migrator import load_csv, load_mssql, migrate_dataframe


st.set_page_config(page_title="Data Migration Tool", layout="centered")

st.title("📦 Data Migration Tool")
st.subheader("CSV / MSSQL → MySQL")
st.divider()

source = st.radio("Select Data Source", ["CSV File", "MSSQL Database"], horizontal=True)

df = None
table_name = None
migration_result = None


# ---------------- CSV ----------------
if source == "CSV File":
    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        table_name = Path(uploaded_file.name).stem

        st.success("CSV loaded")
        st.metric("Rows", len(df))
        st.metric("Columns", len(df.columns))
        st.dataframe(df.head(10), use_container_width=True)


# ---------------- MSSQL ----------------
else:
    st.subheader("MSSQL Connection")

    server = st.text_input("Server", "localhost")
    database = st.text_input("Database")
    table = st.text_input("Table Name")

    auth = st.radio("Authentication", ["Windows", "SQL Login"], horizontal=True)

    username = password = None
    if auth == "SQL Login":
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

    if st.button("Fetch Data"):
        query = f"SELECT * FROM dbo.[{table}]"
        df = load_mssql(server, database, query, auth, username, password)
        table_name = table

        st.success("Data fetched")
        st.metric("Rows", len(df))
        st.metric("Columns", len(df.columns))
        st.dataframe(df.head(10), use_container_width=True)


# ---------------- MIGRATION ----------------
if df is not None and table_name:
    st.divider()

    if st.button("🚀 Migrate to MySQL"):
        try:
            migration_result = migrate_dataframe(df, table_name)
            st.success("Migration completed successfully")
        except Exception as e:
            st.error(f"MySQL Migration Failed: {e}")


# ---------------- DOWNLOAD ----------------
if migration_result:
    st.download_button(
        label="⬇️ Download MySQL SQL File",
        data=migration_result["sql"],
        file_name=f"{migration_result['table']}.sql",
        mime="text/sql"
    )

    st.json({
        "table": migration_result["table"],
        "rows": migration_result["rows"],
        "columns": migration_result["columns"]
    })
