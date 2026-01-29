from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_csv_df_to_postgres(
    postgres_conn_id: str,
    table_fqn: str,
    csv_relative_path: str,
    truncate_before_load: bool = False,
) -> None:
    """
    Read CSV into pandas DataFrame and insert into Postgres.
    No COPY, no STDIN, no Postgres filesystem access.
    """

    # Resolve CSV path relative to this DAG file
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_dir, csv_relative_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    # 1) Read CSV
    df = pd.read_csv(csv_path)

    if df.empty:
        print("CSV is empty. Nothing to load.")
        return

    # 2) Get SQLAlchemy engine from Airflow connection
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()

    schema, table = table_fqn.split(".")

    # 3) Optional truncate
    if truncate_before_load:
        hook.run(f"TRUNCATE TABLE {table_fqn};")

    # 4) Insert data
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",      # batches INSERTs
        chunksize=1000,      # tune if needed
    )

    # 5) Log row count
    cnt = hook.get_first(f"SELECT COUNT(*) FROM {table_fqn};")[0]
    print(f"Loaded {len(df)} rows into {table_fqn}. Total rows now: {cnt}")


with DAG(
    dag_id="load_sales_csv_to_postgres",
    start_date=datetime(2025, 1, 30),
    schedule=None,
    catchup=False,
    tags=["postgres", "csv", "pandas"],
    description="Load CSV into demo.sales_data_test using pandas DataFrame",
) as dag:

    load_task = PythonOperator(
        task_id="load_csv_via_dataframe",
        python_callable=load_csv_df_to_postgres,
        op_kwargs={
            "postgres_conn_id": "postgres_k8s",
            "table_fqn": "demo.sales_data_test",
            "csv_relative_path": "data/sales_data_sample.csv",
            "truncate_before_load": True,
        },
    )
