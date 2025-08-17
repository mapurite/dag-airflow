from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id='example_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
) as dag:
    task = DummyOperator(task_id='dummy_task')
