from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# Funciones simples
def start_task():
    print("👉 Inicio del DAG")

def wait_task():
    print("😴 Esperando 5 segundos...")
    time.sleep(5)

def end_task():
    print("✅ DAG finalizado")

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='simple_dag',
    default_args=default_args,
    description='Ejemplo sencillo de DAG',
    schedule_interval=None,  # Solo se ejecuta manualmente
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ejemplo', 'simple'],
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=start_task
    )

    wait = PythonOperator(
        task_id='wait',
        python_callable=wait_task
    )

    end = PythonOperator(
        task_id='end',
        python_callable=end_task
    )

    # Definir dependencias
    start >> wait >> end
