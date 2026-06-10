from __future__ import annotations

import json
import re
import logging
import time
from pathlib import Path

import requests

from ..config.models import MetadataConfig
from ..deduplication.id_dedup import deduplicate_intra

logger = logging.getLogger(__name__)

def load_search_queries(path: str | Path) -> dict[str, str]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def fetch_all_categories(
    config: MetadataConfig,
    raw_old_dir: Path,
    raw_new_dir: Path,
) -> dict[str, dict]:
 
    queries = load_search_queries(config.search_queries_path)
    results: dict[str, dict] = {}

    for category_name, query in queries.items():
        papers_old, papers_new = _fetch_category(config, category_name, query)

        _save_json(papers_old, raw_old_dir / f"{category_name}.json")
        logger.info("[%s] Raw old saved (%d papers)", category_name, len(papers_old))

        _save_json(papers_new, raw_new_dir / f"{category_name}.json")
        logger.info("[%s] Raw new saved (%d papers)", category_name, len(papers_new))

        combined = papers_old + papers_new
        unique, dupe_count = deduplicate_intra(combined)

        logger.info(
            "[%s] Raw: %d old + %d new = %d | Intra dupes removed: %d | After intra: %d",
            category_name,
            len(papers_old),
            len(papers_new),
            len(combined),
            dupe_count,
            len(unique),
        )

        results[category_name] = {
            "papers": unique,
            "raw_old_count": len(papers_old),
            "raw_new_count": len(papers_new),
            "intra_dupes": dupe_count,
        }

    return results


def _fetch_category(
    config: MetadataConfig,
    category_name: str,
    query: str,
) -> tuple[list[dict], list[dict]]:
    ss = config.semantic_scholar
    papers_old = _run_paginated_query(
        config=config,
        query=query,
        date_filter=ss.date_filter_old,
        min_citation=ss.min_citation_old,
        category_name=category_name,
        date_label="old",
    )
    papers_new = _run_paginated_query(
        config=config,
        query=query,
        date_filter=ss.date_filter_new,
        min_citation=ss.min_citation_new,
        category_name=category_name,
        date_label="new",
    )
    return papers_old, papers_new


def _run_paginated_query(
    config: MetadataConfig,
    query: str,
    date_filter: str,
    min_citation: int,
    category_name: str,
    date_label: str,
) -> list[dict]:
    ss = config.semantic_scholar
    headers = {"x-api-key": config.ss_api_key} if config.ss_api_key else {}

    params: dict = {
        "query": query,
        "publicationDateOrYear": date_filter,
        "publicationTypes": ss.publication_types,
        "fields": ss.fields,
        "limit": 1000,
    }
    if min_citation > 0:
        params["minCitationCount"] = min_citation

    papers: list[dict] = []
    batch_count = 0

    while True:
        batch_count += 1
        logger.info(
            "[%s][%s] Batch %d — fetched so far: %d",
            category_name,
            date_label,
            batch_count,
            len(papers),
        )

        try:
            response = requests.get(
                ss.bulk_search_url, headers=headers, params=params, timeout=30
            )
        except requests.RequestException as exc:
            logger.error("[%s][%s] Request failed: %s", category_name, date_label, exc)
            break

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            logger.warning("Rate limited. Sleeping %ds.", retry_after)
            time.sleep(retry_after)
            continue

        if response.status_code != 200:
            logger.error(
                "[%s][%s] API error %d: %s",
                category_name,
                date_label,
                response.status_code,
                response.text[:300],
            )
            break

        data = response.json()
        batch = data.get("data", [])
        if not batch:
            break

        papers.extend(batch)

        token = data.get("token")
        if not token:
            break

        params["token"] = token
        time.sleep(1)

    return papers


def _save_json(data: list | dict, filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


_BATCH_LOOKUP_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
_BATCH_SIZE       = 500  # SS-enforced hard maximum per request
 
_SS_PAPER_ID_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
_ARXIV_ID_RE    = re.compile(r"^\d{4}\.\d{4,5}(v\d+)?$")
 
# Evaluated case-insensitively against the uppercased raw input.
_KNOWN_PREFIXES = frozenset({
    "DOI:", "ARXIV:", "MAG:", "ACL:", "PMID:", "PMCID:", "CORPUSID:",
})
 
 
def _normalise_paper_id(raw_id: str) -> str:
    """
    Resolve a raw identifier string to the prefix format required by the SS
    paper lookup and batch endpoints.

    """
    raw_id = raw_id.strip()
    if not raw_id:
        raise ValueError("Paper ID must not be empty.")
 
    upper = raw_id.upper()
    for prefix in _KNOWN_PREFIXES:
        if upper.startswith(prefix):
            return raw_id  
 
    if raw_id.startswith("10.") and "/" in raw_id:
        return f"DOI:{raw_id}"
 
    if _ARXIV_ID_RE.match(raw_id):
        return f"ARXIV:{raw_id}"
 
    if _SS_PAPER_ID_RE.match(raw_id):
        return raw_id  # bare 40-char hex SS paperId needs no prefix
 
    if raw_id.isdigit():
        raise ValueError(
            f"Ambiguous numeric ID '{raw_id}'. "
            f"Provide an explicit prefix: CorpusId:{raw_id}, "
            f"PMID:{raw_id}, or MAG:{raw_id}."
        )
 
    raise ValueError(
        f"Unrecognised ID format: '{raw_id}'. "
    )
 
 
def _fetch_batch_chunk(
    ids: list[str],
    fields: str,
    headers: dict,
    max_retries: int = 3,
) -> list[dict | None]:
    """
    POST one chunk of normalised IDs (len ≤ _BATCH_SIZE) to the SS batch endpoint.
 
    Returns a list parallel to `ids`.  Not-found entries are None.  On
    unrecoverable failure the entire chunk returns a None-filled list so the
    caller can map failures back to their original positions without index drift.
    """
    for attempt in range(max_retries):
        try:
            response = requests.post(
                _BATCH_LOOKUP_URL,
                headers={**headers, "Content-Type": "application/json"},
                params={"fields": fields},
                json={"ids": ids},
                timeout=30,
            )
        except requests.RequestException as exc:
            logger.error("Batch chunk network error: %s", exc)
            return [None] * len(ids)
 
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 10))
            logger.warning(
                "Rate limited on batch chunk — sleeping %ds (attempt %d/%d)",
                wait, attempt + 1, max_retries,
            )
            time.sleep(wait)
            continue
 
        if response.status_code == 200:
            data = response.json()
            if not isinstance(data, list):
                logger.error(
                    "Unexpected batch response type: expected list, got %s",
                    type(data).__name__,
                )
                return [None] * len(ids)
            # Defensive: pad if the API returns fewer entries than requested
            if len(data) < len(ids):
                data.extend([None] * (len(ids) - len(data)))
            return data[: len(ids)]
 
        logger.error(
            "Batch chunk failed | status=%d | body=%s",
            response.status_code,
            response.text[:300],
        )
        return [None] * len(ids)
 
    logger.error("Batch chunk exhausted %d retries.", max_retries)
    return [None] * len(ids)
 
 
def _log_fetch_summary(results: list[dict]) -> None:
    counts: dict[str, int] = {}
    for r in results:
        key = r.get("_fetch_status", "unknown")
        counts[key] = counts.get(key, 0) + 1
    logger.info(
        "fetch_papers_by_ids complete | total=%d | %s",
        len(results),
        "  ".join(f"{k}={v}" for k, v in sorted(counts.items())),
    )
 
 
def fetch_papers_by_ids(
    ids: list[str],
    config: MetadataConfig,
) -> list[dict]:
    """
    Fetch full metadata for one or more paper identifiers using the Semantic
    Scholar batch endpoint.  Accepts any mix of SS paperIds, DOIs, ArXiv IDs,
    or explicitly prefixed identifiers (ARXIV:, DOI:, ACL:, MAG:, PMID:,
    PMCID:, CorpusId:).
 
    The returned list is parallel to the input list and preserves input order.
    Each dict carries two diagnostic fields injected by this function:
 
        _input_id     – the original identifier string supplied by the caller
        _fetch_status – one of: 'found' | 'not_found' | 'invalid_id'
 
    Papers with a status other than 'found' are returned as minimal stubs so
    callers can iterate the result list uniformly without index arithmetic.
    Normalisation failures are logged as warnings and do not abort the batch.
    """
    if not ids:
        return []
 
    headers = {"x-api-key": config.ss_api_key} if config.ss_api_key else {}
    fields  = config.semantic_scholar.fields
    results: list[dict] = []
 
    normalised: list[str | None] = []
    for raw in ids:
        try:
            normalised.append(_normalise_paper_id(raw))
        except ValueError as exc:
            logger.warning("ID normalisation failed — skipping '%s': %s", raw, exc)
            normalised.append(None)
 
    total_batches = (len(ids) + _BATCH_SIZE - 1) // _BATCH_SIZE
 
    for batch_idx, start in enumerate(range(0, len(ids), _BATCH_SIZE), start=1):
        end        = min(start + _BATCH_SIZE, len(ids))
        raw_chunk  = ids[start:end]
        norm_chunk = normalised[start:end]
 
        valid_pairs = [(i, n) for i, n in enumerate(norm_chunk) if n is not None]
        valid_ids   = [n for _, n in valid_pairs]
 
        logger.info(
            "Batch %d/%d | IDs %d–%d | valid=%d invalid=%d",
            batch_idx, total_batches,
            start + 1, end,
            len(valid_ids),
            len(norm_chunk) - len(valid_ids),
        )
 
        if valid_ids:
            api_rows = _fetch_batch_chunk(valid_ids, fields, headers)
            # Map each API row back to its position within this chunk
            pos_to_row: dict[int, dict | None] = {
                chunk_pos: row
                for (chunk_pos, _), row in zip(valid_pairs, api_rows)
            }
        else:
            pos_to_row = {}
 
        for i, raw in enumerate(raw_chunk):
            if norm_chunk[i] is None:
                results.append({"_input_id": raw, "_fetch_status": "invalid_id"})
                continue
 
            row = pos_to_row.get(i)
            if not isinstance(row, dict):
                results.append({"_input_id": raw, "_fetch_status": "not_found"})
                continue
 
            row["_input_id"]     = raw
            row["_fetch_status"] = "found"
            results.append(row)
 
        # Respect rate limits between chunks; no sleep needed after the last one.
        if end < len(ids):
            time.sleep(1)
 
    _log_fetch_summary(results)
    return results