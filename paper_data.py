from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).parent.resolve()
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

from paper_downloader.config.loader import load_config as _dl_load_config
from paper_downloader.config.models import PipelineConfig
from paper_downloader.config.validator import validate_config
from paper_downloader.pipeline.orchestrator import DownloadOrchestrator, DownloadPipelineResult

from paper_metadata.config.loader import load_config as _md_load_config
from paper_metadata.config.models import MetadataConfig, RunConfig
from paper_metadata.pipeline.orchestrator import MetadataOrchestrator
from paper_metadata.acquisition.semantic_scholar import fetch_papers_by_ids as _fetch_papers_by_ids
from paper_metadata.recovery.api_recovery import ApiRecoveryProvider
from paper_metadata.recovery.scrape_recovery import ScrapeRecoveryProvider

logging.getLogger("paper_downloader").addHandler(logging.NullHandler())
logging.getLogger("paper_metadata").addHandler(logging.NullHandler())

_DEFAULT_DOWNLOAD_CONFIG = Path(__file__).parent / "config.json"
_DEFAULT_METADATA_CONFIG = Path(__file__).parent / "paper_metadata" / "config.json"


def download(
    ids: str | list[str] | Path,
    *,
    config: PipelineConfig | None = None,
    config_path: str | Path | None = None,
) -> list[DownloadPipelineResult]:
    """
    Download open-access PDFs for one or more paper identifiers.

    Accepts a single identifier string (Semantic Scholar ID, DOI, ArXiv ID, etc.),
    a list of identifier strings, or a path to a JSON file containing a list of
    identifiers or full metadata records. Returns one DownloadPipelineResult
    per input paper; check result.downloaded and result.pdf_path on each.
    """
    resolved_ids = _resolve_ids(ids)
    resolved_config = _resolve_download_config(config, config_path)
    orchestrator = DownloadOrchestrator(resolved_config)
    return orchestrator.process_inputs(resolved_ids)


def _resolve_ids(ids: str | list[str] | Path) -> list[str]:
    """
    Normalise the ids argument into a plain list of identifier strings.

    If ids is already a list it is returned as-is. If it is a path to an
    existing .json file, the file is read and its contents returned as a
    list. Any other string is treated as a single identifier and wrapped in a
    one-element list.
    """
    if isinstance(ids, list):
        return ids

    path = Path(ids)

    if path.suffix.lower() == ".json" and path.exists():
        return _load_ids_from_json(path)

    return [str(ids)]


def _load_ids_from_json(path: Path) -> list[str]:
    """
    Read a JSON file and return its contents as a list of identifier strings.

    The file must contain a JSON array of strings. Blank entries are silently
    dropped. Raises ValueError if the file is not valid JSON or does not
    contain a non-empty array.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in file {path}: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError(
            f"JSON file must contain a list of paper ID strings. "
            f"Got {type(data).__name__} in: {path}"
        )

    ids = [str(item).strip() for item in data if str(item).strip()]

    if not ids:
        raise ValueError(f"No paper IDs found in JSON file: {path}")

    return ids


def _resolve_download_config(
    config: PipelineConfig | None,
    config_path: str | Path | None,
) -> PipelineConfig:
    """
    Return the PipelineConfig to use for a download run.

    If a pre-built config object is supplied it is used directly. Otherwise
    the config is loaded and validated from config_path, falling back to the
    default config.json that ships alongside this file.
    """
    if config is not None:
        return config
    path = Path(config_path) if config_path is not None else _DEFAULT_DOWNLOAD_CONFIG
    loaded = _dl_load_config(path)
    validate_config(loaded)
    return loaded

def _is_missing_abstract(paper: dict) -> bool:
    abstract = paper.get("abstract")
    return not abstract or not str(abstract).strip()


def fetch_metadata(
    *,
    config: MetadataConfig | None = None,
    config_path: str | Path | None = None,
    base_dir: str | Path | None = None,
    search_queries_path: str | Path | None = None,
    api_recovery: bool = True,
    scrape_recovery: bool = True,
) -> None:
    resolved_config = _resolve_metadata_config(config, config_path, base_dir, search_queries_path)
    run_config = RunConfig(
        run_api_recovery=api_recovery,
        run_scrape_recovery=scrape_recovery,
    )
    MetadataOrchestrator(resolved_config, run_config).run()


def recover_abstracts(
    input_dir: str | Path,
    *,
    config: MetadataConfig | None = None,
    config_path: str | Path | None = None,
    base_dir: str | Path | None = None,
    api_recovery: bool = True,
    scrape_recovery: bool = True,
) -> None:
    input_path = Path(input_dir).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"input_dir does not exist: {input_path}")

    resolved_config = _resolve_metadata_config(config, config_path, base_dir, None)
    run_config = RunConfig(
        run_api_recovery=api_recovery,
        run_scrape_recovery=scrape_recovery,
        input_dir=input_path,
    )
    MetadataOrchestrator(resolved_config, run_config).run()

def fetch_papers_by_id(
    ids: str | list[str],
    *,
    config: MetadataConfig | None = None,
    config_path: str | Path | None = None,
    api_recovery: bool = False,
    scrape_recovery: bool = False,
) -> list[dict]:
    """
    Fetch full metadata for one or more paper identifiers directly from
    Semantic Scholar, without running the keyword search pipeline.

    Accepts a single identifier string or a list. Each identifier can be a
    Semantic Scholar paper ID, ArXiv ID, or any recognised by SS
    (ARXIV:, DOI:, ACL:, MAG:, PMID:, PMCID:, CorpusId:).

    Returns a list parallel to the input. Each dict contains the full paper
    metadata in the same schema used throughout the pipeline, plus two
    diagnostic fields:
        _input_id     – the original identifier supplied by the caller
        _fetch_status – 'found' | 'not_found' | 'invalid_id'

    When api_recovery or scrape_recovery is True, papers found in SS but
    missing an abstract are passed through the respective recovery providers
    in-memory. No disk I/O or pipeline orchestration is involved.
    """
    resolved_ids    = [ids] if isinstance(ids, str) else list(ids)
    resolved_config = _resolve_metadata_config(config, config_path, None, None)
    results         = _fetch_papers_by_ids(resolved_ids, resolved_config)

    if not (api_recovery or scrape_recovery):
        return results

    found_papers    = [r for r in results if r.get("_fetch_status") == "found"]
    missing_papers  = [p for p in found_papers if _is_missing_abstract(p)]

    if not missing_papers:
        return results

    if api_recovery:
        provider = ApiRecoveryProvider(
            config       = resolved_config.recovery,
            ss_api_key   = resolved_config.ss_api_key,
            core_api_key = resolved_config.core_api_key,
        )
        for paper in missing_papers:
            result = provider.recover(paper)
            if result:
                paper["abstract"] = result.abstract

    if scrape_recovery:
        # Re-evaluate which papers still need recovery after the API pass.
        still_missing = [p for p in missing_papers if _is_missing_abstract(p)]
        provider = ScrapeRecoveryProvider(config=resolved_config.recovery)
        for paper in still_missing:
            result = provider.recover(paper)
            if result:
                paper["abstract"] = result.abstract

    return results


def _resolve_metadata_config(
    config: MetadataConfig | None,
    config_path: str | Path | None,
    base_dir: str | Path | None,
    search_queries_path: str | Path | None,
) -> MetadataConfig:
    if config is not None:
        resolved = config
    else:
        path = Path(config_path) if config_path is not None else _DEFAULT_METADATA_CONFIG
        resolved = _md_load_config(path)

    if base_dir is not None:
        resolved.output.base_dir = str(Path(base_dir).resolve())

    if search_queries_path is not None:
        resolved.search_queries_path = str(Path(search_queries_path).resolve())

    return resolved