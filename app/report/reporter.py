from __future__ import annotations

from pathlib import Path
from typing import Any

from app.report.stats import build_batch_summary
from app.storage.paths import PathResolver
from app.storage.writers import write_json


class BatchReporter:
    def __init__(self, paths: PathResolver) -> None:
        self.paths = paths
        self.paths.ensure_base_dirs()

    def write_batch_results(
        self,
        *,
        input_value: str,
        results: list[dict[str, Any]],
        filename: str = "batch_results.json",
    ) -> Path:
        summary = build_batch_summary(results)
        payload = {
            "input": input_value,
            "summary": summary,
            "results": results,
        }
        output_path = self.paths.report_path(filename)
        write_json(output_path, payload)
        return output_path

    def write_unresolved_results(
        self,
        *,
        results: list[dict[str, Any]],
        filename: str = "unresolved_results.json",
    ) -> Path:
        unresolved = [result for result in results if str(result.get("status")) == "failed"]
        output_path = self.paths.report_path(filename)
        write_json(output_path, unresolved)
        return output_path