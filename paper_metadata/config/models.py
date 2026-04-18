from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class SemanticScholarConfig:
    date_filter_old: str
    date_filter_new: str
    min_citation_old: int
    min_citation_new: int
    publication_types: str
    fields: str
    bulk_search_url: str = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"


@dataclass
class RecoveryConfig:
    similarity_threshold: float
    min_abstract_len: int
    request_delay: float
    scrape_timeout: tuple[int, int]
    scrape_max_retries: int
    api_sleep_between_papers: float


@dataclass
class OutputConfig:
    base_dir: str


@dataclass
class MetadataConfig:
    semantic_scholar: SemanticScholarConfig
    recovery: RecoveryConfig
    output: OutputConfig
    search_queries_path: str
    ss_api_key: str = ""
    core_api_key: str = ""


@dataclass
class RunConfig:
    run_api_recovery: bool = False
    run_scrape_recovery: bool = False
    input_dir: Path | None = None