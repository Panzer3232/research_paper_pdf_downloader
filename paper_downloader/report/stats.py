from __future__ import annotations

from typing import Any


def build_batch_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    source_type_counts: dict[str, int] = {}
    source_name_counts: dict[str, int] = {}

    for result in results:
        status = str(result.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

        selected_source = result.get("selected_source") or {}
        if isinstance(selected_source, dict):
            version_type = str(selected_source.get("version_type") or "").strip()
            source_name = str(selected_source.get("source_name") or "").strip()

            if version_type:
                source_type_counts[version_type] = source_type_counts.get(version_type, 0) + 1

            if source_name:
                source_name_counts[source_name] = source_name_counts.get(source_name, 0) + 1

    total = len(results)
    failed = status_counts.get("failed", 0)

    return {
        "total": total,
        "failed": failed,
        "succeeded": total - failed,
        "status_counts": status_counts,
        "source_type_counts": source_type_counts,
        "source_name_counts": source_name_counts,
    }