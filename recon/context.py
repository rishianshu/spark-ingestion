from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from typing import Optional

from pyspark.sql import SparkSession

from salam_ingest.common import PrintLogger
from salam_ingest.endpoints.base import SourceEndpoint, SupportsQueryExecution


@dataclass
class ReconContext:
    """Lightweight execution context passed to reconciliation checks."""

    spark: Optional[SparkSession]
    tool: Any
    cfg: Dict[str, Any]
    table_cfg: Dict[str, Any]
    logger: PrintLogger
    source: SourceEndpoint
    target: Optional[SupportsQueryExecution] = None
