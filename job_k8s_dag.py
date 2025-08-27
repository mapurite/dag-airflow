from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
import yaml
import os

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'execute_external_yaml_job',
    default_args=default_args,
    description='Ejecuta Job desde archivo YAML externo',
    schedule_interval=None,
    catchup=False,
    tags=['kubernetes', 'yaml', 'external-file']
)

# Task que ejecuta kubectl apply desde un archivo
apply_yaml_job = KubernetesPodOperator(
    task_id='apply_yaml_job',
    name='kubectl-apply-job',
    namespace='data-jobs',
    image='bitnami/kubectl:latest',
    cmds=["/bin/bash", "-c"],
    arguments=["""
# Ruta donde está tu archivo YAML (monta un volumen o ConfigMap)
YAML_FILE="/config/data-processing-job.yaml"

# Si el archivo viene de un ConfigMap o Secret
if [ ! -f "$YAML_FILE" ]; then
    echo "Archivo YAML no encontrado en $YAML_FILE"
    echo "Creando archivo temporal con la definición..."
    
    cat > /tmp/job.yaml << 'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: data-processing-job-{{ run_id | replace(':', '') | replace('.', '') }}
  namespace: data-jobs
spec:
  template:
    spec:
      containers:
      - name: processor
        image: python:3.9-slim
        command: ["python", "-c"]
        args:
        - |
          import pandas as pd
          print("Procesando datos desde YAML externo...")
          print("Parámetros: {{ dag_run.conf }}")
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
      restartPolicy: Never
  backoffLimit: 3
EOF
    YAML_FILE="/tmp/job.yaml"
fi

echo "Aplicando Job desde: $YAML_FILE"
kubectl apply -f $YAML_FILE

# Obtener nombre del Job
JOB_NAME=$(grep -E "^\s*name:" $YAML_FILE | head -1 | awk '{print $2}')
echo "Job creado: $JOB_NAME"

# Esperar completitud
echo "Esperando que el Job complete..."
kubectl wait --for=condition=complete --timeout=900s job/$JOB_NAME -n data-jobs

# Mostrar logs
echo "=== LOGS DEL JOB ==="
kubectl logs -l job-name=$JOB_NAME -n data-jobs --tail=100

# Verificar estado final
JOB_STATUS=$(kubectl get job $JOB_NAME -n data-jobs -o jsonpath='{.status.conditions[0].type}')
echo "Estado final del Job: $JOB_STATUS"

if [ "$JOB_STATUS" = "Complete" ]; then
    echo "✅ Job completado exitosamente"
    exit 0
else
    echo "❌ Job falló o no se completó"
    kubectl describe job $JOB_NAME -n data-jobs
    exit 1
fi
    """],
    volumes=[],  # Aquí montarías tu ConfigMap o volumen con el YAML
    volume_mounts=[],
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=True,
)

# Task para limpiar Jobs antiguos (opcional)
cleanup_old_jobs = KubernetesPodOperator(
    task_id='cleanup_old_jobs',
    name='cleanup-jobs',
    namespace='data-jobs',
    image='bitnami/kubectl:latest',
    cmds=["/bin/bash", "-c"],
    arguments=["""
echo "Limpiando Jobs antiguos..."

# Eliminar Jobs completados con más de 1 hora
kubectl get jobs -n data-jobs --field-selector status.successful=1 \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.creationTimestamp}{"\n"}{end}' | \
  while read job_name created_time; do
    if [ ! -z "$job_name" ]; then
      # Lógica para eliminar Jobs antiguos
      echo "Revisando Job: $job_name creado en: $created_time"
      # kubectl delete job $job_name -n data-jobs
    fi
  done

echo "Limpieza completada"
    """],
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=True,
    trigger_rule='all_done'  # Ejecuta sin importar si la tarea anterior falló
)

# Configurar dependencias
apply_yaml_job >> cleanup_old_jobs
