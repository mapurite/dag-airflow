from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'debug_k8s_permissions',
    default_args=default_args,
    description='Debug de permisos de Kubernetes',
    schedule_interval=None,
    catchup=False,
    tags=['debug', 'kubernetes']
)

# Task 1: Verificar conexión básica a Kubernetes
test_connection = KubernetesPodOperator(
    task_id='test_k8s_connection',
    name='test-connection',
    namespace='data-ai-desa',  # Usar el mismo namespace de Airflow
    image='bitnami/kubectl:latest',
    cmds=["/bin/bash", "-c"],
    arguments=["""
echo "=== DEBUGGING KUBERNETES CONNECTION ==="
echo "1. Verificando conexión a Kubernetes..."
kubectl version --client

echo "2. Verificando ServiceAccount actual..."
cat /var/run/secrets/kubernetes.io/serviceaccount/namespace
whoami

echo "3. Verificando permisos actuales..."
kubectl auth can-i '*' '*' --all-namespaces || true
kubectl auth can-i create pods --namespace=data-ai-desa || true
kubectl auth can-i create jobs --namespace=data-jobs || true

echo "4. Listando namespaces disponibles..."
kubectl get namespaces || true

echo "5. Verificando si namespace data-jobs existe..."
kubectl get namespace data-jobs || echo "Namespace data-jobs NO existe"

echo "6. Intentando crear un pod simple..."
kubectl run test-pod --image=busybox --restart=Never --dry-run=client -o yaml || true

echo "=== FIN DEL DEBUG ==="
    """],
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=True,
)

# Task 2: Crear namespace si no existe
create_namespace = KubernetesPodOperator(
    task_id='create_namespace_if_needed',
    name='create-namespace',
    namespace='data-ai-desa',
    image='bitnami/kubectl:latest',
    cmds=["/bin/bash", "-c"],
    arguments=["""
echo "Verificando si namespace data-jobs existe..."
if kubectl get namespace data-jobs; then
    echo "Namespace data-jobs ya existe"
else
    echo "Creando namespace data-jobs..."
    kubectl create namespace data-jobs || echo "No se pudo crear el namespace"
fi

echo "Verificando estado final..."
kubectl get namespace data-jobs || echo "Namespace aún no existe"
    """],
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=True,
)

# Task 3: Test simple de creación de Job
test_simple_job = KubernetesPodOperator(
    task_id='test_simple_job_creation',
    name='test-simple-job',
    namespace='data-ai-desa',
    image='bitnami/kubectl:latest',
    cmds=["/bin/bash", "-c"],
    arguments=["""
echo "=== TESTING JOB CREATION ==="

# Crear un Job muy simple
cat > /tmp/simple-job.yaml << 'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: simple-test-job
  namespace: data-jobs
spec:
  template:
    spec:
      containers:
      - name: test
        image: busybox:latest
        command: ["echo", "Hello from Kubernetes Job!"]
      restartPolicy: Never
  backoffLimit: 1
EOF

echo "Contenido del Job:"
cat /tmp/simple-job.yaml

echo "Intentando aplicar el Job..."
kubectl apply -f /tmp/simple-job.yaml

echo "Verificando creación del Job..."
kubectl get jobs -n data-jobs

echo "Esperando completitud del Job..."
sleep 30

echo "Obteniendo logs del Job..."
kubectl logs -l job-name=simple-test-job -n data-jobs || true

echo "Limpiando Job de prueba..."
kubectl delete -f /tmp/simple-job.yaml || true

echo "=== FIN DEL TEST ==="
    """],
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=True,
)

# Configurar dependencias
test_connection >> create_namespace >> test_simple_job
