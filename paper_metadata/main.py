from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config.loader import load_config
from .config.models import RunConfig
from .pipeline.orchestrator import MetadataOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("arxiv").setLevel(logging.WARNING)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="paper_metadata: fetch, deduplicate and recover abstracts for academic papers.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "config.json",
        help="Path to config.json (default: paper_metadata/config.json)",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "Override output.base_dir from config.json.\n"
            "All search_results/ subdirectories will be created inside this directory."
        ),
    )
    parser.add_argument(
        "--search-queries-path",
        type=Path,
        default=None,
        metavar="FILE",
        help="Override search_queries_path from config.json.",
    )

    recovery_group = parser.add_mutually_exclusive_group()
    recovery_group.add_argument(
        "--api-recovery",
        action="store_true",
        default=False,
        help=(
            "Run API-based abstract recovery only\n"
            "(ArXiv, OpenAlex, PubMed, ACL, EuropePMC, Crossref, CORE, SemanticScholar).\n"
            "Skips scrape recovery."
        ),
    )
    recovery_group.add_argument(
        "--scrape-recovery",
        action="store_true",
        default=False,
        help=(
            "Run publisher HTML scrape recovery only (Springer, IEEE, Wiley, ACM, etc.).\n"
            "Skips API recovery."
        ),
    )
    recovery_group.add_argument(
        "--no-recovery",
        action="store_true",
        default=False,
        help="Run fetch and deduplication only. Skip all abstract recovery stages.",
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "Skip fetch and deduplication. Run recovery directly on JSON files in DIR.\n"
            "Each .json file must be a list of paper dicts (Semantic Scholar format).\n"
            "Without --api-recovery or --scrape-recovery, both recoveries run on DIR."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if not args.config.exists():
        logging.error("Config file not found: %s", args.config)
        sys.exit(1)

    if args.input_dir is not None and not args.input_dir.exists():
        logging.error("--input-dir does not exist: %s", args.input_dir)
        sys.exit(1)

    if args.search_queries_path is not None and not args.search_queries_path.exists():
        logging.error("--search-queries-path does not exist: %s", args.search_queries_path)
        sys.exit(1)

    config = load_config(args.config)

    
    if args.base_dir is not None:
        config.output.base_dir = str(args.base_dir.resolve())

    if args.search_queries_path is not None:
        config.search_queries_path = str(args.search_queries_path.resolve())

   
    if args.no_recovery:
        run_api = False
        run_scrape = False
    elif args.api_recovery:
        run_api = True
        run_scrape = False
    elif args.scrape_recovery:
        run_api = False
        run_scrape = True
    else:
        run_api = True
        run_scrape = True

    run_config = RunConfig(
        run_api_recovery=run_api,
        run_scrape_recovery=run_scrape,
        input_dir=args.input_dir,
    )

    MetadataOrchestrator(config, run_config).run()


if __name__ == "__main__":
    main()