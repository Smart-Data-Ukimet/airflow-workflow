from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to run
def hello_airflow():
    print("Hello Airflow 3!")

# Define the DAG
with DAG(
    dag_id="hello_airflow_3",
    start_date=datetime(2025, 8, 16),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    t1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello_airflow
    )

    t1