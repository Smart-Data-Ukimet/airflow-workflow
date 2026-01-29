from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to run
def test_func():
    print("Test output")

# Define the DAG
with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 29),
    schedule="0 22 * * *",
    description="Test description",
    tags=["test"],
    catchup=False,
) as dag:

    test_func_task = PythonOperator(
        task_id="say_hello",
        python_callable=test_func
    )

    # t1