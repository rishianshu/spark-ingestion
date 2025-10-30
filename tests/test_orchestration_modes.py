import json

from salam_ingest.orchestration import build_orchestration_plan


def test_plain_cron_plan_defaults():
    cfg = {
        "runtime": {
            "timezone": "UTC",
            "orchestration": {
                "mode": "plain_cron",
                "cron": {"expression": "*/15 * * * *"},
            },
        }
    }

    plan = build_orchestration_plan(cfg, ["--config", "conf/demo.json"])

    assert plan.mode == "plain_cron"
    assert plan.schedule["expression"] == "*/15 * * * *"
    assert "spark-submit" in plan.execution_command[0]
    json.loads(plan.to_json())


def test_external_scheduler_plan_with_overrides():
    cfg = {
        "runtime": {
            "orchestration": {
                "mode": "airflow",
                "external": {
                    "provider": "airflow",
                    "dag_id": "ingest_demo",
                    "expression": "0 3 * * *",
                    "command": ["python", "ingestion.py", "--config", "conf/demo.json"],
                    "retries": {"count": 2},
                },
            }
        }
    }

    plan = build_orchestration_plan(cfg)

    assert plan.mode == "external_scheduler"
    assert plan.schedule["expression"] == "0 3 * * *"
    assert plan.execution_command == ["python", "ingestion.py", "--config", "conf/demo.json"]
    assert plan.retries["count"] == 2


def test_temporal_plan_requires_fields():
    cfg = {
        "runtime": {
            "timezone": "UTC",
            "orchestration": {
                "mode": "temporal",
                "temporal": {
                    "namespace": "test",
                    "task_queue": "queue",
                    "workflow": "Workflow",
                    "schedule": {"cron": "0 0 * * *"},
                },
            }
        }
    }

    plan = build_orchestration_plan(cfg)

    assert plan.mode == "temporal"
    assert plan.schedule["cron"] == "0 0 * * *"
