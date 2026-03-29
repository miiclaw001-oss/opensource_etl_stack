# Prerequisites

---

## Required for All Modes

- **Docker** 24+ with Docker Compose v2 (`docker compose` — note: no hyphen)
- **Python 3.10+** (for synthetic data generation and local scripts)
- **8 GB RAM** minimum (16 GB recommended for full stack)
- **20 GB disk space** free

## Required for Kubernetes Mode Only

- **kind** v0.20+ — `go install sigs.k8s.io/kind@v0.20.0` or [kind releases](https://github.com/kubernetes-sigs/kind/releases)
- **kubectl** — [install guide](https://kubernetes.io/docs/tasks/tools/)
- **helm** v3 — `brew install helm` or [helm.sh](https://helm.sh)

## Optional but Useful

- **AWS CLI v2** — for MinIO bucket inspection (`brew install awscli`)
- **mc (MinIO Client)** — `brew install minio/stable/mc`
- **dbt-spark** — `pip install dbt-spark==1.7.2` (for running dbt locally)

## Check Your Setup

```bash
docker --version          # ≥ 24.0
docker compose version    # ≥ 2.0
python3 --version         # ≥ 3.10
# Kubernetes only:
kind version
kubectl version --client
helm version
```
