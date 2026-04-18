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

    resolved_ids = _resolve_ids(ids)
    resolved_config = _resolve_download_config(config, config_path)
    orchestrator = DownloadOrchestrator(resolved_config)
    return orchestrator.process_inputs(resolved_ids)


def _resolve_ids(ids: str | list[str] | Path) -> list[str]:

    if isinstance(ids, list):
        return ids

    path = Path(ids)

    if path.suffix.lower() == ".json" and path.exists():
        return _load_ids_from_json(path)

    return [str(ids)]


def _load_ids_from_json(path: Path) -> list[str]:
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
    if config is not None:
        return config
    path = Path(config_path) if config_path is not None else _DEFAULT_DOWNLOAD_CONFIG
    loaded = _dl_load_config(path)
    validate_config(loaded)
    return loaded


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