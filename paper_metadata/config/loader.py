from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

from .models import MetadataConfig, OutputConfig, RecoveryConfig, SemanticScholarConfig

_REQUIRED_SS_FIELDS = [
    "date_filter_old",
    "date_filter_new",
    "min_citation_old",
    "min_citation_new",
    "publication_types",
    "fields",
]

_REQUIRED_RECOVERY_FIELDS = [
    "similarity_threshold",
    "min_abstract_len",
    "request_delay",
    "scrape_timeout",
    "scrape_max_retries",
    "api_sleep_between_papers",
]


def load_config(config_path: str | Path) -> MetadataConfig:
    config_path = Path(config_path).resolve()

    env_path = config_path.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    with open(config_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    _validate_raw(raw, config_path)

    ss_raw = raw["semantic_scholar"]
    recovery_raw = raw["recovery"]
    output_raw = raw["output"]

    scrape_timeout = recovery_raw["scrape_timeout"]
    if isinstance(scrape_timeout, list):
        scrape_timeout = tuple(scrape_timeout)

    ss_key_env = ss_raw.get("api_key_env", "SEMANTIC_SCHOLAR_API_KEY")
    core_key_env = recovery_raw.get("core_api_key_env", "CORE_API_KEY")

    return MetadataConfig(
        semantic_scholar=SemanticScholarConfig(
            date_filter_old=ss_raw["date_filter_old"],
            date_filter_new=ss_raw["date_filter_new"],
            min_citation_old=int(ss_raw["min_citation_old"]),
            min_citation_new=int(ss_raw["min_citation_new"]),
            publication_types=ss_raw["publication_types"],
            fields=ss_raw["fields"],
        ),
        recovery=RecoveryConfig(
            similarity_threshold=float(recovery_raw["similarity_threshold"]),
            min_abstract_len=int(recovery_raw["min_abstract_len"]),
            request_delay=float(recovery_raw["request_delay"]),
            scrape_timeout=scrape_timeout,
            scrape_max_retries=int(recovery_raw["scrape_max_retries"]),
            api_sleep_between_papers=float(recovery_raw["api_sleep_between_papers"]),
        ),
        output=OutputConfig(
            base_dir=output_raw["base_dir"],
        ),
        search_queries_path=raw.get(
            "search_queries_path",
            str(config_path.parent / "search_queries.json"),
        ),
        ss_api_key=os.environ.get(ss_key_env, ""),
        core_api_key=os.environ.get(core_key_env, ""),
    )


def _validate_raw(raw: dict, config_path: Path) -> None:
    for section, required_fields in [
        ("semantic_scholar", _REQUIRED_SS_FIELDS),
        ("recovery", _REQUIRED_RECOVERY_FIELDS),
    ]:
        if section not in raw:
            raise ValueError(
                f"config.json missing required section '{section}' (path: {config_path})"
            )
        for key in required_fields:
            if key not in raw[section]:
                raise ValueError(
                    f"config.json missing required key '{section}.{key}' (path: {config_path})"
                )

    if "output" not in raw or "base_dir" not in raw["output"]:
        raise ValueError(
            f"config.json missing required key 'output.base_dir' (path: {config_path})"
        )