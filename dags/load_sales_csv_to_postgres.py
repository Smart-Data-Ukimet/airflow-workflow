from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_csv_to_postgres(
    postgres_conn_id: str,
    table_fqn: str,
    csv_relative_path: str,
    delimiter: str = ",",
    has_header: bool = True,
    truncate_before_load: bool = False,
) -> None:

    dag_dir = os.path.dirname(os.path.abspath(__file__))  # /opt/airflow/dags (usually)
    csv_path = os.path.join(dag_dir, csv_relative_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    if truncate_before_load:
        hook.run(f"TRUNCATE TABLE {table_fqn};")

    copy_sql = f"""
        COPY {table_fqn}
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER '{delimiter}',
            HEADER {'true' if has_header else 'false'}
        );
    """

    # COPY FROM STDIN reads bytes, so open in binary mode
    hook.copy_expert(sql=copy_sql, filename=csv_path)

    # Optional: log count
    cnt = hook.get_first(f"SELECT COUNT(*) FROM {table_fqn};")[0]
    print(f"Loaded CSV into {table_fqn}. Current row count: {cnt}")


with DAG(
    dag_id="load_sales_csv_to_postgres",
    start_date=datetime(2025, 1, 30),
    schedule=None,  # run manually (safer for CSV loads)
    catchup=False,
    tags=["postgres", "csv", "demo"],
    description="Load CSV from dags/data into demo.sales_data_test using Airflow Postgres connection",
) as dag:

    load_task = PythonOperator(
        task_id="load_csv_into_demo_sales_data_test",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "postgres_conn_id": "postgres_k8s",
            "table_fqn": "demo.sales_data_test",
            "csv_relative_path": "data/sales_data_sample.csv",
            "delimiter": ",",
            "has_header": True,
            "truncate_before_load": True,              # set True if you want full refresh
        },
    )
