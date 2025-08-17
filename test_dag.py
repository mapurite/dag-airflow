from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['tu_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='Un DAG de prueba simple',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['prueba'],
) as dag:

    def print_hello():
        print("¡Hola Mundo desde Airflow!")
        return "¡Hola Mundo!"

    t1 = PythonOperator(
        task_id='imprimir_hola',
        python_callable=print_hello,
    )

    t2 = BashOperator(
        task_id='fecha_actual',
        bash_command='date',
    )

    t3 = BashOperator(
        task_id='echo_prueba',
        bash_command='echo "Airflow funciona correctamente"',
    )

    t1 >> [t2, t3]
