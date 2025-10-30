# Orchestration Infrastructure Specification

This document outlines the infrastructure requirements and recommended practices for running the Salam ingestion framework under managed orchestration, with special focus on Temporal-based deployments.

## 1. Environment Layout

| Component | DEV | UAT | PROD |
|-----------|-----|-----|------|
| Temporal Namespace | `ingestion-dev` | `ingestion-uat` | `ingestion-prod` |
| Task Queues | `spark-ingest-dev`, `temporal-maintenance` | `spark-ingest-uat` | `spark-ingest-prod` |
| Spark Cluster | Shared QA cluster | Staging autoscale cluster | Dedicated prod cluster with autoscale |
| Artifact Registry | Dev ECR/ACR/GCR repo | UAT repo (immutable tags) | Prod repo (signed images) |
| Secrets Store | Vault.dev / AWS Secrets Manager (dev path) | Vault.uat | Vault.prod with RBAC |

Each environment mirrors the same configuration surface (`runtime.orchestration.temporal`), differing only in namespace, task queue, and credential scopes.

## 2. CI/CD Requirements

1. **Build & Test Pipeline**
   - Run `pytest` and `python ingestion.py --dump-orchestration-plan --config <cfg>` for every target config.
   - Lint orchestration helpers (`orchestration/**/*.py`) with the project’s standard tools.
   - Package ingestion artifacts (zip & Docker images) with versioned tags.

2. **Artifact Promotion**
   - Promote Docker images and plan JSON between DEV → UAT → PROD using immutable tags.
   - Store generated orchestration plans as build artifacts (`orchestration_plans/<env>/<build>.json`) for visibility and audits.

3. **Temporal Schedule Deployment**
   - Use the helper `orchestration/temporal/register_schedule.py` (or a custom tool) in the deployment pipeline.
   - Inject environment-specific overrides (`--namespace`, `--task-queue`) via CI/CD variables.
   - Validate schedules with Temporal’s CLI (`tctl schedule list`) post-deployment.

4. **Rollback Strategy**
   - Maintain previous plan JSON and image tags; allow the deployment pipeline to reapply a prior schedule definition.
   - Temporal supports versioned schedules; keep history for quick revert.

## 3. Temporal Cluster & Worker Infrastructure

### Temporal Cluster

| Element | Specification |
|---------|---------------|
| Cluster | Managed service (Temporal Cloud) or self-hosted k8s deployment |
| Namespaces | One per environment (`ingestion-dev`, `ingestion-uat`, `ingestion-prod`) |
| Authentication | mTLS certificates or OIDC (depending on provider) |
| Persistence | MySQL/PostgreSQL or Cassandra (follow Temporal sizing guide) |
| Visibility | Prometheus metrics & Grafana dashboards |

### Worker Runbook

| Concern | Implementation |
|---------|----------------|
| Runtime | Docker container running `orchestration/temporal/workflow_template.py` (after customization) |
| Deployment | Helm chart / ArgoCD / ECS service with auto-scaling |
| Scaling | Horizontal Pod Autoscaler based on queue depth & workflow concurrency |
| Health Checks | Liveness & readiness endpoints; Temporal worker heartbeat |
| Logging | Structured logs to ELK/CloudWatch/Loki; include run IDs and plan metadata |

## 4. Secrets & Configuration Management

| Asset | Storage | Rotation |
|-------|---------|----------|
| JDBC credentials | Vault / Secrets Manager by environment | Automated rotation with notification hooks in Temporal |
| Temporal certificates | Vault or KMS-backed store | Rotate quarterly; deploy via CI pipeline |
| Spark configs | Config maps or S3/gcs buckets with restricted IAM | Validate checksums in CI/CD |
| Alert endpoints | Secrets store (Slack webhook, PagerDuty key) | Masked in logs; rotate when team changes |

Best Practices:
- Use short-lived tokens or IAM roles for worker pods to fetch secrets.
- Keep orchestration plan JSON free of secrets; the workflow fetches them at runtime.

## 5. Monitoring & Observability

| Signal | Tooling | Notes |
|--------|---------|-------|
| Temporal Metrics | Prometheus (Worker & Server) | Track workflow success, retry counts, schedule lag |
| Spark Job Metrics | Spark History Server, custom metrics to Prometheus | Correlate with run IDs from orchestration plan |
| Logs | Centralized logging (ELK/Loki/CloudWatch) | Include namespace, workflow ID, plan version |
| Alerts | PagerDuty/Slack integration | Hook into Temporal failure hooks or workflow activity exceptions |

### Recommended Dashboards
- **Schedule Health**: per-namespace schedule lag, missed catches, next run time.
- **Workflow Outcomes**: success vs. retries vs. failures.
- **Resource Utilization**: worker CPU/memory, Spark executor metrics, queue depth.

## 6. Operational Runbooks

1. **Registering/Updating a Schedule**
   - Generate plan: `python ingestion.py --config conf/<env>.json --dump-orchestration-plan > plan.json`
   - Apply: `python orchestration/temporal/register_schedule.py --plan plan.json --namespace ingestion-<env>`
   - Verify: `tctl schedule describe --namespace ingestion-<env> <workflow_id>`

2. **Triggering Ad-hoc Runs**
   - Use Temporal CLI `workflow start` with the plan payload if you need to bypass schedule cadence.

3. **Disaster Recovery**
   - Ensure Temporal persistence layer is replicated (multi-AZ DB or managed service).
   - Backup orchestration plans and configs in version control.
   - Run periodic failover tests (switch worker queue, restore from backup).

4. **Audit & Compliance**
   - Log every schedule change (store plan JSON + CI/CD metadata).
   - Implement approvals in CI/CD for PROD schedule deployments.

## 7. Future Enhancements

- Add automated drift detection to compare plan JSON against deployed schedule specs.
- Integrate policy-as-code (e.g., OPA) to validate orchestration configs before merge.
- Extend monitoring with synthetic checks (trigger canary workflows daily).

By standardizing infrastructure across Dev/UAT/Prod and embedding orchestration plans into CI/CD pipelines, teams can safely promote ingestion workloads while leveraging Temporal’s workflow guarantees.
