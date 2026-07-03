from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any, Dict, Optional


@dataclass
class StageMetric:
    stage: str
    duration_seconds: float
    input_records: Optional[int]
    output_records: Optional[int]
    throughput_rps: Optional[float]
    extra: Dict[str, Any]


class StageBenchmarkLogger:
    """Lightweight benchmark logger for Spark pipeline stages."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id

    def start(self) -> float:
        return perf_counter()

    def finish(
        self,
        *,
        stage: str,
        started_at: float,
        input_records: Optional[int] = None,
        output_records: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> StageMetric:
        duration = perf_counter() - started_at
        throughput: Optional[float] = None
        if output_records is not None and duration > 0:
            throughput = round(float(output_records) / duration, 2)

        metric = StageMetric(
            stage=stage,
            duration_seconds=round(duration, 3),
            input_records=input_records,
            output_records=output_records,
            throughput_rps=throughput,
            extra=extra or {},
        )
        self._print_metric(metric)
        return metric

    def _print_metric(self, metric: StageMetric) -> None:
        payload = {
            "run_id": self.run_id,
            "stage": metric.stage,
            "duration_seconds": metric.duration_seconds,
            "input_records": metric.input_records,
            "output_records": metric.output_records,
            "throughput_rps": metric.throughput_rps,
            "extra": metric.extra,
        }
        print(f"[Silver Benchmark] {payload}")
