"""
Sample utility to create or update a Temporal schedule using the orchestration plan.

Usage:
    python register_schedule.py --plan plan.json

Dependencies:
    pip install temporalio

This script is intentionally lightweight—adapt it to your deployment standards
(better logging, secrets management, dry-run support, etc.) before using in production.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:  # pragma: no cover - only used when Temporal SDK available
    from temporalio import client, schedule
except Exception:  # pragma: no cover
    client = schedule = None  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register or update a Temporal schedule from a plan JSON.")
    parser.add_argument("--plan", required=True, type=Path, help="Path to the orchestration plan JSON")
    parser.add_argument("--namespace", help="Temporal namespace override")
    parser.add_argument("--task-queue", help="Task queue override")
    parser.add_argument("--workflow-id", help="Workflow ID override")
    return parser.parse_args()


async def main() -> None:  # pragma: no cover - Temporal runtime
    if client is None or schedule is None:
        raise SystemExit(
            "Temporal SDK not available. Install with `pip install temporalio` before using this helper."
        )
    args = parse_args()
    plan = json.loads(args.plan.read_text())
    temporal_meta = plan.get("metadata", {})

    namespace = args.namespace or temporal_meta.get("namespace") or "default"
    task_queue = args.task_queue or temporal_meta.get("task_queue")
    workflow_name = temporal_meta.get("workflow") or "IngestionWorkflow"
    workflow_id = args.workflow_id or temporal_meta.get("workflow_id") or f"{workflow_name}-schedule"

    if not task_queue:
        raise SystemExit("Task queue missing; ensure the plan contains metadata.task_queue or pass --task-queue.")

    schedule_spec = schedule.ScheduleSpec(
        cron_expressions=[plan["schedule"].get("cron")] if plan["schedule"].get("cron") else [],
        timezone=plan["schedule"].get("timezone"),
    )

    payload = {
        "execution_command": plan["execution_command"],
        "environment": plan["metadata"].get("env") if plan.get("metadata") else None,
        "schedule_info": plan.get("schedule"),
        "retries": plan.get("retries"),
        "alerts": plan.get("alerts"),
    }

    action = schedule.ScheduleActionStartWorkflow(
        workflow=workflow_name,
        task_queue=task_queue,
        args=[payload],
        id=workflow_id,
    )

    temporal_client = await client.Client.connect(f"{namespace}.temporal:7233", namespace=namespace)
    handle = temporal_client.get_schedule_handle(workflow_id)
    try:
        await handle.describe()
        await handle.update(schedule.Schedule(actions=[action], spec=schedule_spec))
        print(f"Updated existing schedule '{workflow_id}' in namespace '{namespace}'.")
    except schedule.ScheduleNotFoundError:
        await temporal_client.create_schedule(workflow_id, schedule.Schedule(actions=[action], spec=schedule_spec))
        print(f"Created new schedule '{workflow_id}' in namespace '{namespace}'.")


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())
