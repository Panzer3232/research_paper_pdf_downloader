from __future__ import annotations

import json
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