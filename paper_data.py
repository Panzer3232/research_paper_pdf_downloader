from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).parent.resolve()
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

from paper_downloader.config.loader import load_config
from paper_downloader.config.models import PipelineConfig
from paper_downloader.config.validator import validate_config
from paper_downloader.pipeline.orchestrator import DownloadOrchestrator, DownloadPipelineResult

logging.getLogger("paper_downloader").addHandler(logging.NullHandler())

_DEFAULT_CONFIG = Path(__file__).parent / "config.json"


def download(
    ids: str | list[str] | Path,
    *,
    config: PipelineConfig | None = None,
    config_path: str | Path | None = None,
) -> list[DownloadPipelineResult]:
    
    resolved_ids = _resolve_ids(ids)
    resolved_config = _resolve_config(config, config_path)
    orchestrator = DownloadOrchestrator(resolved_config)
    return orchestrator.process_inputs(resolved_ids)


def _resolve_ids(ids: str | list[str] | Path) -> list[str]:
    
    if isinstance(ids, list):
        return ids

    path = Path(ids)

    if path.suffix.lower() == ".json" and path.exists():
        return _load_ids_from_json(path)

    # Plain string that is not a JSON file — treat as a single paper ID
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


def _resolve_config(
    config: PipelineConfig | None,
    config_path: str | Path | None,
) -> PipelineConfig:
    if config is not None:
        return config
    path = Path(config_path) if config_path is not None else _DEFAULT_CONFIG
    loaded = load_config(path)
    validate_config(loaded)
    return loaded