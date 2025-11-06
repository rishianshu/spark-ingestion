"""
Recon subsystem for configurable source vs destination validation.

This package coordinates execution of reconciliation checks that compare
metrics between upstream systems (typically JDBC sources) and downstream
targets (e.g. Iceberg tables).  The design mirrors the ingestion stack so the
same tooling (Spark session, endpoint factory, logging) can be reused.
"""

from .cli import run_cli

__all__ = ["run_cli"]

