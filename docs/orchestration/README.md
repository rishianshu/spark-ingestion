# Orchestration Guide

The ingestion framework can be promoted into different scheduling stacks without touching the core code. This guide explains the configuration for each mode, references helper utilities, and shows how to generate an orchestration plan that other systems can consume.

Run the CLI with `--dump-orchestration-plan` to preview the derived plan before wiring it into a scheduler:

```bash
python ingestion.py --config conf/miniboss_bss.json --dump-orchestration-plan
```

The JSON payload contains the Spark command, schedule metadata, retry/alert hints, and bookkeeping fields.

## Plain Cron

Lightweight deployments can rely on the host crontab. Configure the runtime block as follows:

```json
{
  "runtime": {
    "orchestration": {
      "mode": "plain_cron",
      "cron": {
        "expression": "0 * * * *",
        "timezone": "Asia/Kolkata",
        "log_file": "/var/log/salam_ingest.log"
      }
    }
  }
}
```

The generated plan includes an `execution_command` array and an example crontab line. Install the command using the system’s `crontab -e` and ensure Spark binaries, configuration JSON, and any virtualenv activation happen in the wrapper script you place in cron.

## External Scheduler (Airflow / Informatica)

When Salam runs under Airflow, Informatica, or another enterprise scheduler, set `mode` to `external_scheduler`, `airflow`, or `informatica`. The plan captures the command, cron expression, and retry/alert hints for the orchestrator:

```json
{
  "runtime": {
    "orchestration": {
      "mode": "airflow",
      "external": {
        "provider": "airflow",
        "dag_id": "ingest_miniboss",
        "expression": "0 2 * * *",
        "command": [
          "python",
          "ingestion.py",
          "--config",
          "conf/miniboss_bss.json"
        ],
        "retries": {
          "count": 3,
          "delay_minutes": 10
        },
        "alerts": {
          "email": ["datalake-ops@example.com"]
        }
      }
    }
  }
}
```

Embed the command in a BashOperator (Airflow), a command task (Informatica), or the equivalent construct for your scheduler.

## Temporal

Temporal offers workflow templates, built-in retries, and managed scheduling. Declare the Temporal options in the config:

```json
{
  "runtime": {
    "orchestration": {
      "mode": "temporal",
      "temporal": {
        "namespace": "ingestion-prod",
        "task_queue": "spark-ingest",
        "workflow": "IngestionWorkflow",
        "workflow_id": "miniboss_ingest",
        "command": [
          "python",
          "ingestion.py",
          "--config",
          "conf/miniboss_bss.json"
        ],
        "retry_policy": {
          "initial_interval_seconds": 60,
          "maximum_interval_seconds": 900,
          "maximum_attempts": 5
        },
        "schedule": {
          "cron": "0 0 * * *",
          "timezone": "Asia/Kolkata",
          "catchup_window": "24h"
        },
        "alerts": {
          "webhook": "https://hooks.slack.com/services/..."
        }
      }
    }
  }
}
```

The plan is meant to feed into the Temporal SDK when you register a schedule or launch a manual run. Sample helpers and workflow templates live under the repository’s `orchestration/` directory.

### Bringing Temporal to Production

1. **Implement a workflow** that accepts the plan (or a subset) and spawns the Spark job. Skeleton code is available under `orchestration/temporal/workflow_template.py`.
2. **Deploy a worker** container/service listening on the plan’s `task_queue` within the specified Temporal namespace.
3. **Register schedules** using the helper script `orchestration/temporal/register_schedule.py` (reference it or roll your own). Feed the plan JSON into the script so it sets up cron triggers and retry policies.
4. **Wire alerts** by calling Slack/email/webhook targets inside workflow failure handlers or Temporal’s notification hooks.
5. **Monitor** Temporal’s UI, Prometheus metrics, and Salam’s ingestion events to ensure retries and alerts behave as expected.

## Next Steps

- Check `orchestration/README.md` for environment-specific templates and scripts.
- Add CI guardrails that run `--dump-orchestration-plan` against your production config to catch accidental changes.
- Keep secrets (JDBC credentials, API tokens) out of the plan: rely on Temporal activities or the external scheduler to inject them securely at runtime.
- Review `docs/orchestration/infra_spec.md` for CI/CD, secrets, and monitoring expectations, and consult the sample helpers in `orchestration/` when wiring Temporal workers and schedules.
