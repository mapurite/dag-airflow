from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

def obtener_api(**context):
    """
    Llama a la API y guarda el JSON en XCom
    """
    r = requests.get("https://api.exchangerate.host/latest?base=USD")
    data = r.json()
    context['ti'].xcom_push(key="api_data", value=data)
    print("Cotizaciones obtenidas:", data)


def guardar_en_db(**context):
    """
    Inserta los datos de la API en la tabla 'cotizaciones'
    """
    data = context['ti'].xcom_pull(task_ids="consulta_api", key="api_data")
    fecha = data["date"]
    base = data["base"]
    rates = data["rates"]   # diccionario { "EUR": 0.92, "ARS": 875.0, ... }

    pg_hook = PostgresHook(postgres_conn_id="mi_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for moneda, valor in rates.items():
        cursor.execute(
            "INSERT INTO cotizaciones (fecha, base, moneda, valor) VALUES (%s, %s, %s, %s)",
            (fecha, base, moneda, valor)
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(rates)} registros insertados en la base de datos")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="cotizacion_api_to_db",
    default_args=default_args,
    description="Consulta API de cotizaciones y guarda en Postgres",
    schedule_interval="@daily",   # corre todos los días
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["api", "etl"],
) as dag:

    consulta_api = PythonOperator(
        task_id="consulta_api",
        python_callable=obtener_api,
        provide_context=True,
    )

    guardar_db = PythonOperator(
        task_id="guardar_db",
        python_callable=guardar_en_db,
        provide_context=True,
    )

    consulta_api >> guardar_db
