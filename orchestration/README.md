# Orchestration Toolkit

This directory hosts samples and helper utilities for integrating Salam ingestion with different orchestrators. Use the files as reference implementations—copy or adapt them into your deployment repos rather than importing them directly.

- `temporal/workflow_template.py` – A minimal Temporal workflow/activity pair that executes the ingestion command from an orchestration plan.
- `temporal/register_schedule.py` – Utility for registering or updating a Temporal schedule using the JSON emitted by `--dump-orchestration-plan`.

See `docs/orchestration/README.md` for configuration guidance and the CLI commands that produce orchestration plans.
