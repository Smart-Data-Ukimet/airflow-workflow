from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def copy_table_rows(
    src_table: str,
    dst_table: str,
    postgres_conn_id: str,
    batch_size: int = 1000,
):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # 1) Read from source
    select_sql = f"SELECT * FROM {src_table};"
    rows = hook.get_records(select_sql)

    if not rows:
        print(f"No rows found in {src_table}. Nothing to insert.")
        return

    # 2) Get column names (to build INSERT)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {src_table} LIMIT 0;")
            colnames = [desc[0] for desc in cur.description]

    cols = ", ".join(colnames)
    placeholders = ", ".join(["%s"] * len(colnames))
    insert_sql = f"INSERT INTO {dst_table} ({cols}) VALUES ({placeholders})"

    # 3) Insert into destination in batches
    total = len(rows)
    print(f"Copying {total} rows from {src_table} -> {dst_table}")

    for i in range(0, total, batch_size):
        chunk = rows[i : i + batch_size]
        hook.insert_rows(
            table=dst_table,
            rows=chunk,
            target_fields=colnames,
            commit_every=batch_size,
            replace=False,
        )
        print(f"Inserted {min(i + batch_size, total)}/{total} rows")

    print("Done.")


with DAG(
    dag_id="copy_table_postgres",
    start_date=datetime(2025, 1, 29),
    schedule="0 22 * * *",
    description="Select data from one table and insert into another using Airflow Connection",
    tags=["test", "postgres"],
    catchup=False,
) as dag:

    copy_task = PythonOperator(
        task_id="copy_src_to_dst",
        python_callable=copy_table_rows,
        op_kwargs={
            "src_table": "public.sales_data_test",
            "dst_table": "public.sales_data_test_hist",
            "postgres_conn_id": "postgres_114_20",
            "batch_size": 1000,
        },
    )
