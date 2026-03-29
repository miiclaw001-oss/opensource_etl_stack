# Use Case 2: Kubernetes with kind

**Best for:** testing production-like deployments, CI/CD, learning k8s.

**RAM needed:** ~12 GB (kind cluster overhead + all services)

---

## Prerequisites Check

```bash
kind version    # should show v0.20+
kubectl version --client
helm version
```

---

## Step 1 — Start the kind Cluster and Deploy

```bash
./scripts/setup.sh --mode k8s
```

This creates a kind cluster named `etl-stack`, applies all manifests from `k8s/`, waits for deployments, and uploads Airflow DAGs.

## Step 2 — Verify All Pods Are Running

```bash
kubectl get pods --all-namespaces
# Expected: all pods in Running or Completed state
```

## Step 3 — Port-Forward to Access Services

```bash
# MinIO
kubectl port-forward -n minio svc/minio 9000:9000 9001:9001 &

# Nessie
kubectl port-forward -n polaris svc/polaris 8181:8181 &

# Airflow
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080 &

# Spark
kubectl port-forward -n spark svc/spark-master-ui 8090:8090 &
```

!!! note
    The k8s manifests use `NodePort` for kind. If port-forwards don't work, check NodePort assignments with `kubectl get svc --all-namespaces`.

## Step 4 — Trigger the Pipeline

```bash
AIRFLOW_POD=$(kubectl get pod -n airflow -l component=webserver -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n airflow $AIRFLOW_POD -- airflow dags trigger etl_iceberg_pipeline
```

## Step 5 — Watch Logs

```bash
kubectl logs -n airflow deployment/airflow-scheduler -f
kubectl logs -n spark deployment/spark-master -f
kubectl logs -n polaris deployment/nessie -f
```

---

## Tear Down

```bash
kind delete cluster --name etl-stack
```
