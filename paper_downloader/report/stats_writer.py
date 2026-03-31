from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from paper_downloader.pipeline.orchestrator import DownloadPipelineResult

logger = logging.getLogger("paper_downloader.stats")


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _build_summary(results: list[DownloadPipelineResult]) -> dict[str, Any]:
    total = len(results)
    freshly_downloaded = sum(1 for r in results if r.downloaded and not r.reused_existing)
    already_existed = sum(1 for r in results if r.downloaded and r.reused_existing)
    failed = sum(1 for r in results if not r.downloaded)

    status_counts: dict[str, int] = {}
    for r in results:
        status_counts[r.status] = status_counts.get(r.status, 0) + 1

    return {
        "total": total,
        "freshly_downloaded": freshly_downloaded,
        "already_existed": already_existed,
        "failed": failed,
        "status_counts": status_counts,
    }


def _short_records(results: list[DownloadPipelineResult]) -> list[dict[str, Any]]:
    return [
        {
            "paper_id": r.semantic_scholar_id or r.paper_key,
            "title": r.title,
            "downloaded": r.downloaded,
            "reused_existing": r.reused_existing,
            "status": r.status,
            "pdf_path": r.pdf_path,
        }
        for r in results
    ]


def write_download_stats(
    results: list[DownloadPipelineResult],
    stats_dir: Path,
    *,
    run_label: str | None = None,
) -> dict[str, Path]:
    
    stats_dir.mkdir(parents=True, exist_ok=True)

    ts = _utc_timestamp()
    label = f"_{run_label}" if run_label else ""
    prefix = f"download_stats{label}_{ts}"

    full_path = stats_dir / f"{prefix}_full.json"
    short_json_path = stats_dir / f"{prefix}_short.json"
    csv_path = stats_dir / f"{prefix}_short.csv"

    summary = _build_summary(results)
    short = _short_records(results)

    _write_json(full_path, {
        "run_timestamp": ts,
        "run_label": run_label,
        "summary": summary,
        "results": [r.to_dict() for r in results],
    })

    _write_json(short_json_path, {
        "run_timestamp": ts,
        "run_label": run_label,
        "summary": summary,
        "results": short,
    })

    _write_csv(csv_path, short)

    logger.info(
        "Stats written | full=%s | short_json=%s | csv=%s",
        full_path, short_json_path, csv_path,
    )

    return {"full_json": full_path, "short_json": short_json_path, "csv": csv_path}


def _write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, records: list[dict[str, Any]]) -> None:
    fieldnames = ["paper_id", "title", "downloaded", "reused_existing", "status", "pdf_path"]
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        if records:
            writer.writerows(records)
    tmp.replace(path)