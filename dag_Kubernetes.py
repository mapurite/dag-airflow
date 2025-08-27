from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
}

with DAG(
    'kubernetes_job_from_yaml',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_job_from_yaml = KubernetesJobOperator(
        task_id='run_job_from_yaml',
        namespace='default',
        config_file=os.path.expanduser('~/.kube/config'),  # Ruta al kubeconfig
        job_template_file='path/to/your/job_template.yaml',  # Archivo YAML del Job
        in_cluster=False,  # False si usas config file externo
    )
