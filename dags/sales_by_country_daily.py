from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# from datahub_airflow_plugin.entities import Urn

# IN_TABLE_URN = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,test.sales_data_test,PROD)"
# OUT_TABLE_URN = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,test.sales_by_country,PROD)"

with DAG(
    dag_id="sales_by_country_daily",
    start_date=datetime(2026, 1, 29),
    schedule="0 22 * * *", # daily 22:00
    description="Update data in table sales_by_country daily at 22:00",
    tags=["dbt", "postgres", "sales"],
    catchup=False,
) as dag:

    run_sales_by_country = BashOperator(
        task_id="sales_by_country",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run -s sales_by_country --profiles-dir /opt/airflow/dbt"
        ),
        # inlets=[Urn(IN_TABLE_URN)],
        # outlets=[Urn(OUT_TABLE_URN)],
    )