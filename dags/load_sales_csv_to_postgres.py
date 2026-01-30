from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def load_csv_df_to_postgres_no_sqlalchemy(
    postgres_conn_id: str,
    table_fqn: str,
    csv_relative_path: str,
    truncate_before_load: bool = False,
    chunk_size: int = 5000,
) -> None:
    """
    Read CSV into pandas DataFrame and insert into Postgres using psycopg2 cursor.
    """

    # Resolve CSV path relative to this DAG file
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_dir, csv_relative_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    schema, table = table_fqn.split(".", 1)

    # Read CSV (keep strings as-is to avoid surprises)
    df = pd.read_csv(csv_path)

    if df.empty:
        print("CSV is empty. Nothing to load.")
        return
    
    df.columns = df.columns.str.strip().str.lower()

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Replace NaN with None so psycopg2 inserts NULL
    df = df.where(pd.notnull(df), None)

    cols = list(df.columns)
    col_list_sql = ", ".join([f'"{c}"' for c in cols])  # quote column names safely

    insert_sql = f'INSERT INTO "{schema}"."{table}" ({col_list_sql}) VALUES %s'

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Optional truncate
    if truncate_before_load:
        hook.run(f'TRUNCATE TABLE "{schema}"."{table}";')

    print("CSV columns:", list(df.columns))

    rows_total = 0
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            # chunked insert
            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start : start + chunk_size]
                values = [tuple(x) for x in chunk.to_numpy()]
                execute_values(cur, insert_sql, values, page_size=min(chunk_size, 10000))
                rows_total += len(chunk)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # Log row count
    cnt = hook.get_first(f'SELECT COUNT(*) FROM "{schema}"."{table}";')[0]
    print(f"Loaded {rows_total} rows into {schema}.{table}. Total rows now: {cnt}")


with DAG(
    dag_id="load_sales_csv_to_postgres",
    start_date=datetime(2025, 1, 30),
    schedule=None,
    catchup=False,
    tags=["postgres", "csv", "pandas"],
    description="Load CSV into demo.sales_data_test using pandas DataFrame (no SQLAlchemy)",
) as dag:

    load_task = PythonOperator(
        task_id="load_csv_via_dataframe",
        python_callable=load_csv_df_to_postgres_no_sqlalchemy,
        op_kwargs={
            "postgres_conn_id": "postgres_k8s",
            "table_fqn": "demo.sales_data_test",   # убедись что схема demo существует
            "csv_relative_path": "data/sales_data_sample.csv",
            "truncate_before_load": True,
            "chunk_size": 5000,
        },
    )
