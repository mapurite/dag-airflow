from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta
import pandas as pd
import io

# ------------------------------
# Funciones de cada etapa
# ------------------------------

def extract_data(**context):
    """
    Conecta a un servidor SFTP y baja el archivo de logs.
    """
    sftp_hook = SFTPHook(ssh_conn_id="sftp_legacy")  # definida en Airflow Connections
    remote_file = "/logs/sistema_legacy.log"
    local_file = f"/opt/airflow/files/legacy_{context['ds']}.log"

    sftp_hook.retrieve_file(remote_file, local_file)
    print(f"Archivo descargado: {local_file}")
    context['ti'].xcom_push(key="local_file", value=local_file)


def transform_data(**context):
    """
    Limpieza y normalización con Pandas.
    """
    local_file = context['ti'].xcom_pull(task_ids="extract_data", key="local_file")
    df = pd.read_csv(local_file, sep="|", names=["fecha", "usuario", "accion", "detalle"])

    # Normalizamos
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.dropna()  # quitamos registros inválidos
    df["accion"] = df["accion"].str.upper()

    # Guardamos CSV transformado
    transformed_file = f"/opt/airflow/files/legacy_transformed_{context['ds']}.csv"
    df.to_csv(transformed_file, index=False)
    print(f"Archivo transformado: {transformed_file}")
    context['ti'].xcom_push(key="transformed_file", value=transformed_file)


def load_data(**context):
    """
    Inserta en un Data Warehouse (ejemplo: Postgres).
    """
    transformed_file = context['ti'].xcom_pull(task_ids="transform_data", key="transformed_file")
    df = pd.read_csv(transformed_file)

    # Conexión a Postgres (definida en Airflow Connections)
    pg_hook = PostgresHook(postgres_conn_id="dw_postgres")
    engine = pg_hook.get_sqlalchemy_engine()

    # Inserta en tabla destino
    df.to_sql("logs_legacy", engine, if_exists="append", index=False)
    print(f"{len(df)} registros insertados en logs_legacy")


# ------------------------------
# Definición del DAG
# ------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline_empresa",
    default_args=default_args,
    description="Pipeline ETL completo: Extract -> Transform -> Load -> Notify",
    schedule_interval="@daily",   # corre todos los días
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "empresa"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
    )

    t4 = EmailOperator(
        task_id="notify_team",
        to="equipo@empresa.com",
        subject="✅ Pipeline ETL finalizado",
        html_content="El pipeline ETL se ejecutó correctamente y los datos ya están en el Data Warehouse.",
    )

    # Definición del flujo
    t1 >> t2 >> t3 >> t4
