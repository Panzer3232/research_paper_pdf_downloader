from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from paper_downloader.config.loader import load_config
from paper_downloader.config.validator import validate_config
from paper_downloader.pipeline.orchestrator import DownloadOrchestrator
from paper_downloader.report.enrich_output import enrich_metadata_with_results
from paper_downloader.report.stats_writer import write_download_stats

_DEFAULT_CONFIG = Path(__file__).parent / "config.json"


def _setup_logging(level: str, log_file: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )


def run(
    input_path: str | Path,
    *,
    config_path: str | Path = _DEFAULT_CONFIG,
    stats_dir: str | Path | None = None,
    run_label: str | None = None,
    output_path: str | Path | None = None,
) -> int:
    
    config = load_config(config_path)
    validate_config(config)
    _setup_logging(config.logging.level, config.logging.log_file)

    logger = logging.getLogger("paper_downloader")
    logger.info("paper_downloader starting | input=%s", input_path)

    orchestrator = DownloadOrchestrator(config)
    results = orchestrator.process_inputs(input_path)

    resolved_stats_dir = (
        Path(stats_dir)
        if stats_dir
        else Path(config.output.root_dir) / "download_stats"
    )
    written = write_download_stats(results, resolved_stats_dir, run_label=run_label)

    input_as_path = Path(input_path)
    if input_as_path.exists() and input_as_path.suffix.lower() == ".json":
        enriched = enrich_metadata_with_results(input_as_path, results, output_path)
        if enriched is not None:
            logger.info("Enriched metadata written | output=%s", enriched)
    else:
        logger.warning(
            "Skipping enriched output: --input is not a JSON file path | input=%s",
            input_path,
        )

    downloaded = sum(1 for r in results if r.downloaded)
    freshly_downloaded = sum(1 for r in results if r.downloaded and not r.reused_existing)
    already_existed = sum(1 for r in results if r.downloaded and r.reused_existing)
    total = len(results)

    for r in results:
        if not r.downloaded:
            logger.error(
                "FAILED | %s | %s | %s",
                r.input_value or r.paper_key,
                r.status,
                r.error,
            )

    logger.info(
        "paper_downloader finished | %d/%d downloaded (%d new, %d already existed) | stats=%s",
        downloaded,
        total,
        freshly_downloaded,
        already_existed,
        written["full_json"],
    )

    return 0 if downloaded == total else 1


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download PDFs for a batch of paper identifiers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to JSON input file or a single identifier string.",
    )
    parser.add_argument(
        "--config",
        default=str(_DEFAULT_CONFIG),
        help="Path to config.json.",
    )
    parser.add_argument(
        "--stats-dir",
        default=None,
        help="Directory for timestamped stats files. "
             "Defaults to <output.root_dir>/download_stats.",
    )
    parser.add_argument(
        "--run-label",
        default=None,
        help="Optional label appended to stats filenames.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path for the enriched output JSON. Defaults to "
             "<input_stem>_enriched.json in the input file's directory.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    sys.exit(
        run(
            args.input,
            config_path=args.config,
            stats_dir=args.stats_dir,
            run_label=args.run_label,
            output_path=args.output,
        )
    )


if __name__ == "__main__":
    main()