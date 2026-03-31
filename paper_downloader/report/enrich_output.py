from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from paper_downloader.pipeline.orchestrator import DownloadPipelineResult
from paper_downloader.storage.writers import write_json

logger = logging.getLogger("paper_downloader.enrich")


def _build_result_index(
    results: list[DownloadPipelineResult],
) -> dict[str, DownloadPipelineResult]:
    
    index: dict[str, DownloadPipelineResult] = {}
    for result in results:
        for key in (result.semantic_scholar_id, result.paper_key, result.input_value):
            if key:
                index.setdefault(key.lower(), result)
    return index


def _resolve_result_for_record(
    record: dict[str, Any],
    index: dict[str, DownloadPipelineResult],
) -> DownloadPipelineResult | None:
   
    candidates: list[str] = []

    paper_id = record.get("paperId")
    if paper_id:
        candidates.append(str(paper_id).lower())

    external_ids: dict[str, Any] = record.get("externalIds") or {}

    corpus_id = external_ids.get("CorpusId")
    if corpus_id is not None:
        candidates.append(f"corpus__{corpus_id}".lower())

    arxiv_id = external_ids.get("ArXiv")
    if arxiv_id:
        candidates.append(f"arxiv__{arxiv_id}".lower())

    doi = external_ids.get("DOI")
    if doi:
        normalized = doi.lower().strip()
        candidates.append(f"doi__{normalized}".replace("/", "_").replace(".", "_"))
        candidates.append(normalized)

    for candidate in candidates:
        result = index.get(candidate)
        if result is not None:
            return result

    return None


def _enrich_record(
    record: dict[str, Any],
    result: DownloadPipelineResult | None,
) -> dict[str, Any]:
    
    enriched = dict(record)
    if result is None:
        enriched["pdf_path"] = None
        enriched["download_status"] = "not_attempted"
        enriched["downloaded"] = False
    else:
        enriched["pdf_path"] = result.pdf_path
        enriched["download_status"] = result.status
        enriched["downloaded"] = result.downloaded
    return enriched


def _has_metadata_records(records: list[Any]) -> bool:
    
    return any(isinstance(r, dict) for r in records)


def derive_output_path(input_path: Path) -> Path:
    
    return input_path.parent / f"{input_path.stem}_enriched{input_path.suffix}"


def enrich_metadata_with_results(
    input_path: str | Path,
    results: list[DownloadPipelineResult],
    output_path: str | Path | None = None,
) -> Path | None:
    
    input_path = Path(input_path)

    raw: Any = json.loads(input_path.read_text(encoding="utf-8"))

    wrapper_key: str | None = None
    if isinstance(raw, dict):
        if "papers" in raw and isinstance(raw["papers"], list):
            wrapper_key = "papers"
            records = raw["papers"]
        else:
            records = [raw]
    elif isinstance(raw, list):
        records = raw
    else:
        raise ValueError(f"Unsupported JSON structure in input file: {input_path}")

    if not _has_metadata_records(records):
        logger.info(
            "Skipping enriched output: input contains only bare identifiers, "
            "no metadata records to enrich | input=%s",
            input_path,
        )
        return None

    if output_path is None:
        output_path = derive_output_path(input_path)
    else:
        output_path = Path(output_path)

    index = _build_result_index(results)

    enriched_records: list[Any] = []
    for record in records:
        if not isinstance(record, dict):
            enriched_records.append(record)
            continue
        result = _resolve_result_for_record(record, index)
        enriched_records.append(_enrich_record(record, result))

    if wrapper_key:
        payload: Any = dict(raw)
        payload[wrapper_key] = enriched_records
    else:
        payload = enriched_records

    written = write_json(output_path, payload)
    logger.info(
        "Enriched metadata written | path=%s | records=%d",
        written,
        len(enriched_records),
    )
    return written