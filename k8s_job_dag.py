from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id="remote_k8s_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    remote_job = KubernetesPodOperator(
        task_id="remote_pod",
        namespace="default",
        name="remote-test-pod",
        image="busybox",
        cmds=["sh", "-c"],
        arguments=["echo 'Hola desde el cluster remoto' && sleep 5"],
        get_logs=True,
        is_delete_operator_pod=True,
        cluster_context="airflow-context"  # opcional si tenés varios clusters en el kubeconfig
    )
