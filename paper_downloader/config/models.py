from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RuntimeConfig:
    input_path: str | None = None


@dataclass(slots=True)
class ApiConfig:
    semantic_scholar_api_key: str | None = None
    openalex_api_key: str | None = None
    unpaywall_email: str | None = None
    core_api_key: str | None = None
    crossref_email: str | None = None


@dataclass(slots=True)
class ResolutionConfig:
    prefer_publisher_version: bool = True
    allow_preprints: bool = True
    allow_title_fallback: bool = True
    title_similarity_threshold: float = 0.90

    source_priority: list[str] = field(
        default_factory=lambda: [
            "metadata_open_access",
            "venue_exact",
            "arxiv",
            "acl",
            "cvf",
            "openalex",
            "unpaywall",
            "europepmc",
            "crossref",
            "core",
            "zenodo",
            "doaj",
            "broad_search",
        ]
    )

    trusted_domains: list[str] = field(
        default_factory=lambda: [
            "arxiv.org",
            "biorxiv.org",
            "medrxiv.org",
            "aclanthology.org",
            "openaccess.thecvf.com",
            "api.openalex.org",
            "content.openalex.org",
            "doi.org",
            "dl.acm.org",
            "ieeexplore.ieee.org",
            "link.springer.com",
            "nature.com",
            "sciencedirect.com",
            "pmc.ncbi.nlm.nih.gov",
            "europepmc.org",
            "zenodo.org",
            "doaj.org",
            "osf.io",
            "hal.science",
            "ssrn.com",
            "core.ac.uk",
            "mdpi.com",
            "frontiersin.org",
            "journals.plos.org",
        ]
    )


@dataclass(slots=True)
class DownloadConfig:
    connect_timeout_seconds: int = 10
    read_timeout_seconds: int = 60
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    verify_ssl: bool = True
    max_redirects: int = 5
    min_pdf_bytes: int = 1024
    max_pdf_bytes: int = 250_000_000
    user_agent: str = "paper-downloader/1.0"
    allowed_content_types: list[str] = field(
        default_factory=lambda: [
            "application/pdf",
            "application/x-pdf",
            "binary/octet-stream",
        ]
    )


@dataclass(slots=True)
class ResumeConfig:
    enabled: bool = True
    skip_completed_stages: bool = True
    verify_existing_files: bool = True
    retry_failed_stage_only: bool = True


@dataclass(slots=True)
class OutputConfig:
    root_dir: str = "data"
    input_dir_name: str = "input"
    metadata_dir_name: str = "metadata"
    manifests_dir_name: str = "manifests"
    pdfs_dir_name: str = "pdfs"
    reports_dir_name: str = "reports"


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    log_file: str | None = None


@dataclass(slots=True)
class PipelineConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    apis: ApiConfig = field(default_factory=ApiConfig)
    resolution: ResolutionConfig = field(default_factory=ResolutionConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)
    resume: ResumeConfig = field(default_factory=ResumeConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineConfig":
        return cls(
            runtime=RuntimeConfig(**dict(data.get("runtime") or {})),
            apis=ApiConfig(**dict(data.get("apis") or {})),
            resolution=ResolutionConfig(**dict(data.get("resolution") or {})),
            download=DownloadConfig(**dict(data.get("download") or {})),
            resume=ResumeConfig(**dict(data.get("resume") or {})),
            output=OutputConfig(**dict(data.get("output") or {})),
            logging=LoggingConfig(**dict(data.get("logging") or {})),
        )