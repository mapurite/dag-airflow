from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests


def obtener_api():
    r = requests.get("https://api.exchangerate.host/latest?base=USD")
    data = r.json()
    print("Cotizaciones del USD:")
    print(data)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="cotizacion_api_dag",
    default_args=default_args,
    description="Ejemplo DAG que consulta una API",
    schedule_interval="@daily",    # corre todos los días
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["api", "ejemplo"],
) as dag:

    consulta_api = PythonOperator(
        task_id="cotizacion_usd",
        python_callable=obtener_api,
    )

