from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from salam_ingest.common import RUN_ID


@dataclass
class OrchestrationPlan:
    """Structured representation of how a job should be orchestrated."""

    mode: str
    schedule: Dict[str, Any] = field(default_factory=dict)
    execution_command: List[str] = field(default_factory=list)
    retries: Optional[Dict[str, Any]] = None
    alerts: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        data = {
            "mode": self.mode,
            "schedule": self.schedule,
            "execution_command": self.execution_command,
            "retries": self.retries,
            "alerts": self.alerts,
            "metadata": self.metadata,
            "run_id": RUN_ID,
        }
        return {key: value for key, value in data.items() if value is not None}

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)


def _default_exec_command(args: Optional[Iterable[str]] = None) -> List[str]:
    base = ["spark-submit", "--py-files", "dist/salam_ingest_bundle.zip", "ingestion.py"]
    if args:
        base.extend([str(arg) for arg in args if arg is not None])
    return base


def _build_plain_cron(cfg: Dict[str, Any], args: Optional[Iterable[str]]) -> OrchestrationPlan:
    cron_cfg = cfg.get("cron", {})
    schedule = {
        "expression": cron_cfg.get("expression", "0 * * * *"),
        "timezone": cron_cfg.get("timezone", cfg.get("timezone", "UTC")),
        "notes": cron_cfg.get("notes", "Run via crontab on ingestion host"),
    }
    metadata = {
        "example_crontab": f"{schedule['expression']} salam_ingest_runner.sh",
        "log_file": cron_cfg.get("log_file"),
    }
    if cron_cfg.get("env"):
        metadata["env"] = cron_cfg["env"]
    return OrchestrationPlan(
        mode="plain_cron",
        schedule=schedule,
        execution_command=_default_exec_command(args),
        metadata=metadata,
    )


def _build_external_scheduler(cfg: Dict[str, Any], args: Optional[Iterable[str]]) -> OrchestrationPlan:
    ext_cfg = cfg.get("external", {})
    provider = ext_cfg.get("provider", "airflow")
    schedule = {
        "type": ext_cfg.get("schedule_type", "cron"),
        "expression": ext_cfg.get("expression", ext_cfg.get("cron")),
        "timezone": ext_cfg.get("timezone", cfg.get("timezone", "UTC")),
    }
    metadata = {
        "provider": provider,
        "job_name": ext_cfg.get("job_name"),
        "dag_id": ext_cfg.get("dag_id"),
        "pipeline_id": ext_cfg.get("pipeline_id"),
        "notes": ext_cfg.get("notes"),
    }
    execution_command = ext_cfg.get("command") or _default_exec_command(args)
    retries = ext_cfg.get("retries")
    alerts = ext_cfg.get("alerts")
    return OrchestrationPlan(
        mode="external_scheduler",
        schedule=schedule,
        execution_command=execution_command,
        retries=retries,
        alerts=alerts,
        metadata={k: v for k, v in metadata.items() if v},
    )


def _build_temporal(cfg: Dict[str, Any], args: Optional[Iterable[str]]) -> OrchestrationPlan:
    temporal_cfg = cfg.get("temporal", {})
    schedule_cfg = temporal_cfg.get("schedule", {})
    retries = temporal_cfg.get("retry_policy", {})
    alerts = temporal_cfg.get("alerts", {})
    metadata = {
        "namespace": temporal_cfg.get("namespace"),
        "task_queue": temporal_cfg.get("task_queue"),
        "workflow": temporal_cfg.get("workflow"),
        "cron_schedule": schedule_cfg.get("cron"),
        "start_to_close_timeout": temporal_cfg.get("start_to_close_timeout"),
        "workflow_id": temporal_cfg.get("workflow_id"),
        "notes": temporal_cfg.get("notes"),
    }
    command = temporal_cfg.get("command") or _default_exec_command(args)
    schedule = {
        "type": "temporal_schedule",
        "cron": schedule_cfg.get("cron"),
        "timezone": schedule_cfg.get("timezone", cfg.get("timezone", "UTC")),
        "catchup_window": schedule_cfg.get("catchup_window"),
    }
    return OrchestrationPlan(
        mode="temporal",
        schedule=schedule,
        execution_command=command,
        retries=retries or None,
        alerts=alerts or None,
        metadata={k: v for k, v in metadata.items() if v},
    )


def build_orchestration_plan(cfg: Dict[str, Any], args: Optional[Iterable[str]] = None) -> OrchestrationPlan:
    """Return an orchestration plan based on runtime configuration."""

    runtime_cfg = cfg.get("runtime", {})
    orch_cfg = runtime_cfg.get("orchestration", {}) or {}
    mode = orch_cfg.get("mode", "plain_cron").lower()
    mode_switch = {
        "plain_cron": _build_plain_cron,
        "external_scheduler": _build_external_scheduler,
        "informatica": _build_external_scheduler,
        "airflow": _build_external_scheduler,
        "temporal": _build_temporal,
    }
    builder = mode_switch.get(mode)
    if builder is None:
        raise ValueError(f"Unsupported orchestration mode: {mode}")
    effective_cfg = dict(orch_cfg)
    effective_cfg.setdefault("timezone", runtime_cfg.get("timezone", "UTC"))
    plan = builder(effective_cfg, args)
    plan.metadata.setdefault("mode_display", mode)
    return plan
