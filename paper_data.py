from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

_PACKAGE_ROOT = Path(__file__).parent.resolve()
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

from app.config.loader import load_config
from app.config.models import PipelineConfig
from app.config.validator import validate_config
from app.pipeline.orchestrator import DownloadOrchestrator, DownloadPipelineResult

logging.getLogger("paper_downloader").addHandler(logging.NullHandler())

_DEFAULT_CONFIG = Path(__file__).parent / "config.json"


def download(
    ids: str | list[str],
    *,
    config: PipelineConfig | None = None,
    config_path: str | Path | None = None,
) -> list[DownloadPipelineResult]:
    """
    Download PDFs for one or more Semantic Scholar paper IDs.
    """
    resolved_config = _resolve_config(config, config_path)
    orchestrator = DownloadOrchestrator(resolved_config)
    inputs: str | list[str] = ids if isinstance(ids, list) else [ids]
    return orchestrator.process_inputs(inputs)


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