from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from ..acquisition.semantic_scholar import fetch_all_categories, load_search_queries
from ..config.models import MetadataConfig, RunConfig
from ..deduplication.id_dedup import deduplicate_inter as id_dedup_inter
from ..deduplication.title_dedup import deduplicate_inter_title, deduplicate_intra_title
from ..recovery.api_recovery import ApiRecoveryProvider
from ..recovery.scrape_recovery import ScrapeRecoveryProvider, _publisher_from_doi
from ..reporting.reporter import (
    print_acquisition_report,
    print_overall_scrape_summary,
    print_recovery_report,
    print_scrape_report,
    print_title_dedup_report,
    save_csv,
    save_stats_json,
)

logger = logging.getLogger(__name__)

_INTRA_CSV_FIELDS = [
    "category", "scope", "normalized_title",
    "kept_paperId", "kept_citations", "kept_title",
    "dropped_paperId", "dropped_citations", "dropped_title",
]
_INTER_CSV_FIELDS = [
    "owner_category", "challenger_category", "scope", "normalized_title",
    "kept_paperId", "kept_citations", "kept_title",
    "dropped_paperId", "dropped_citations", "dropped_title",
    "citation_swap_performed",
]


def _save_json(data: list | dict, filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


def _load_json(filepath: Path) -> list | dict:
    with open(filepath, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _is_missing_abstract(paper: dict) -> bool:
    abstract = paper.get("abstract")
    if not abstract:
        return True
    return not str(abstract).strip()


class MetadataOrchestrator:
    def __init__(self, config: MetadataConfig, run_config: RunConfig | None = None) -> None:
        self._config = config
        self._run_config = run_config or RunConfig()
        base = Path(config.output.base_dir).resolve()

        self._dirs = {
            "raw_old":                  base / "search_results" / "raw_old",
            "raw_new":                  base / "search_results" / "raw_new",
            "final":                    base / "search_results" / "final",
            "final_title_deduped":      base / "search_results" / "final_title_deduped",
            "final_recovered_abstract": base / "search_results" / "final_recovered_abstract",
            "publisher_scraped":        base / "search_results" / "publisher_scraped",
            "reports":                  base / "search_results" / "reports",
        }

    def run(self) -> None:
        self._create_dirs()
        rc = self._run_config

        
        if rc.input_dir is not None:
            input_dir = Path(rc.input_dir).resolve()
            if not input_dir.exists():
                raise FileNotFoundError(f"--input-dir does not exist: {input_dir}")

            category_order = self._categories_from_dir(input_dir)
            if not category_order:
                raise ValueError(f"No .json files found in --input-dir: {input_dir}")

            logger.info(
                "Input-dir mode: %d categories found in %s", len(category_order), input_dir
            )

            api_output_dir = self._dirs["final_recovered_abstract"]
            scrape_input_dir = input_dir

            if rc.run_api_recovery:
                logger.info("Running API-based abstract recovery")
                self._run_api_recovery(category_order, input_dir, api_output_dir)
                scrape_input_dir = api_output_dir

            if rc.run_scrape_recovery:
                logger.info("Running scrape-based abstract recovery")
                self._run_scrape_recovery(category_order, scrape_input_dir, self._dirs["publisher_scraped"])

            if not rc.run_api_recovery and not rc.run_scrape_recovery:
                logger.warning(
                    "--input-dir given but no recovery stage requested. "
                    "Use --api-recovery and/or --scrape-recovery."
                )
            return

        # Full pipeline mode
        queries = load_search_queries(self._config.search_queries_path)
        if not queries:
            raise ValueError(
                f"search_queries.json is empty. Add at least one category. "
                f"(path: {self._config.search_queries_path})"
            )

        category_order = list(queries.keys())
        logger.info("Full pipeline: %d categories", len(category_order))

        # Stage 1 + intra ID dedup
        logger.info("Stage 1: Fetching from Semantic Scholar")
        fetch_results = fetch_all_categories(
            self._config,
            self._dirs["raw_old"],
            self._dirs["raw_new"],
        )

        # Stage 2: Inter ID dedup
        logger.info("Stage 2: Inter-category ID deduplication")
        intra_deduped = {cat: fetch_results[cat]["papers"] for cat in category_order}
        assigned_id = id_dedup_inter(intra_deduped)
        acquisition_stats: dict[str, dict] = {}

        for cat in category_order:
            raw_old_count = fetch_results[cat]["raw_old_count"]
            raw_new_count = fetch_results[cat]["raw_new_count"]
            intra_dupes   = fetch_results[cat]["intra_dupes"]
            after_intra   = len(fetch_results[cat]["papers"])
            final_papers  = assigned_id[cat]["papers"]
            inter_removed = assigned_id[cat]["inter_removed"]

            _save_json(final_papers, self._dirs["final"] / f"{cat}.json")

            acquisition_stats[cat] = {
                "raw_old":      raw_old_count,
                "raw_new":      raw_new_count,
                "raw":          raw_old_count + raw_new_count,
                "intra_dupes":  intra_dupes,
                "after_intra":  after_intra,
                "inter_removed": inter_removed,
                "final_unique": len(final_papers),
            }

        print_acquisition_report(acquisition_stats, str(self._dirs["final"]))
        save_stats_json(
            {
                "generated_at":    datetime.now().isoformat(),
                "date_filter_old": self._config.semantic_scholar.date_filter_old,
                "date_filter_new": self._config.semantic_scholar.date_filter_new,
                "min_citation_old": self._config.semantic_scholar.min_citation_old,
                "min_citation_new": self._config.semantic_scholar.min_citation_new,
                "categories":      acquisition_stats,
            },
            self._dirs["reports"] / "acquisition_stats.json",
        )

        # Stage 3: Title dedup
        logger.info("Stage 3: Title-based deduplication")
        title_input: dict[str, list[dict]] = {
            cat: assigned_id[cat]["papers"] for cat in category_order
        }
        intra_title_data: dict[str, list[dict]] = {}
        all_intra_rows: list[dict] = []
        title_stats: dict[str, dict] = {}

        for cat in category_order:
            unique, intra_rows = deduplicate_intra_title(title_input[cat], cat)
            intra_title_data[cat] = unique
            all_intra_rows.extend(intra_rows)
            title_stats[cat] = {
                "input":         len(title_input[cat]),
                "intra_removed": len(title_input[cat]) - len(unique),
                "after_intra":   len(unique),
            }

        assigned_title, inter_rows, inter_dropped = deduplicate_inter_title(intra_title_data)

        for cat, papers in assigned_title.items():
            _save_json(papers, self._dirs["final_title_deduped"] / f"{cat}.json")
            title_stats[cat]["inter_removed"] = title_stats[cat]["after_intra"] - len(papers)
            title_stats[cat]["final_unique"]  = len(papers)

        total_intra_dropped = sum(s["intra_removed"] for s in title_stats.values())

        save_csv(all_intra_rows, self._dirs["reports"] / "intra_title_duplicates.csv", _INTRA_CSV_FIELDS)
        save_csv(inter_rows,     self._dirs["reports"] / "inter_title_duplicates.csv", _INTER_CSV_FIELDS)
        save_stats_json(
            {
                "generated_at":      datetime.now().isoformat(),
                "total_intra_removed": total_intra_dropped,
                "total_inter_removed": inter_dropped,
                "total_removed":       total_intra_dropped + inter_dropped,
                "categories":          title_stats,
            },
            self._dirs["reports"] / "title_dedup_stats.json",
        )
        print_title_dedup_report(
            title_stats,
            total_intra_dropped,
            inter_dropped,
            str(self._dirs["final_title_deduped"]),
            str(self._dirs["reports"]),
        )

        # Stage 4: API recovery (optional)
        api_output_dir   = self._dirs["final_recovered_abstract"]
        scrape_input_dir = self._dirs["final_title_deduped"]

        if rc.run_api_recovery:
            logger.info("Stage 4: API-based abstract recovery")
            self._run_api_recovery(
                category_order,
                self._dirs["final_title_deduped"],
                api_output_dir,
            )
            scrape_input_dir = api_output_dir

        # Stage 5: Scrape recovery (optional)
        if rc.run_scrape_recovery:
            logger.info("Stage 5: Scrape-based abstract recovery")
            self._run_scrape_recovery(
                category_order,
                scrape_input_dir,
                self._dirs["publisher_scraped"],
            )


    def _run_api_recovery(
        self,
        category_order: list[str],
        input_dir: Path,
        output_dir: Path,
    ) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        provider = ApiRecoveryProvider(
            config=self._config.recovery,
            ss_api_key=self._config.ss_api_key,
            core_api_key=self._config.core_api_key,
        )
        sleep_between = self._config.recovery.api_sleep_between_papers

        for cat in category_order:
            input_path  = input_dir / f"{cat}.json"
            output_path = output_dir / f"{cat}.json"

            if not input_path.exists():
                logger.warning("API recovery: input file not found, skipping: %s", input_path)
                continue

            papers: list[dict] = _load_json(input_path)
            missing = [p for p in papers if _is_missing_abstract(p)]
            total        = len(papers)
            missing_count = len(missing)

            logger.info("[%s] Total: %d | Missing abstracts: %d", cat, total, missing_count)

            if missing_count == 0:
                _save_json(papers, output_path)
                continue

            recovered_count = 0
            source_counts: dict[str, int] = {}
            start_time = time.time()

            for idx, paper in enumerate(missing, start=1):
                paper_start = time.time()
                result      = provider.recover(paper)
                elapsed_paper = time.time() - paper_start
                elapsed_total = time.time() - start_time
                avg_per_paper = elapsed_total / idx
                eta           = avg_per_paper * (missing_count - idx)

                if result:
                    paper["abstract"] = result.abstract
                    recovered_count  += 1
                    source_counts[result.source] = source_counts.get(result.source, 0) + 1
                    logger.info(
                        "[%s][%d/%d] RECOVERED via %s (%.1fs) | Recovered: %d | ETA: %.0fs | %s",
                        cat, idx, missing_count, result.source,
                        elapsed_paper, recovered_count, eta,
                        paper.get("title", "")[:60],
                    )
                else:
                    logger.info(
                        "[%s][%d/%d] FAILED (%.1fs) | ETA: %.0fs | %s",
                        cat, idx, missing_count,
                        elapsed_paper, eta,
                        paper.get("title", "")[:60],
                    )

                if idx < missing_count:
                    time.sleep(sleep_between)

            total_time = time.time() - start_time
            _save_json(papers, output_path)

            print_recovery_report(
                total=total,
                missing_count=missing_count,
                recovered_count=recovered_count,
                source_counts=source_counts,
                total_time=total_time,
                output_path=str(output_path),
            )

    def _run_scrape_recovery(
        self,
        category_order: list[str],
        input_dir: Path,
        output_dir: Path,
    ) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        scrape_provider = ScrapeRecoveryProvider(config=self._config.recovery)

        all_stats: list[tuple[str, dict]]  = []
        combined_missing:   dict[str, int] = defaultdict(int)
        combined_recovered: dict[str, int] = defaultdict(int)

        for cat in category_order:
            input_path  = input_dir  / f"{cat}.json"
            output_path = output_dir / f"{cat}.json"

            if not input_path.exists():
                logger.warning("Scrape recovery: input file not found, skipping: %s", input_path)
                continue

            papers: list[dict] = _load_json(input_path)
            missing    = [p for p in papers if _is_missing_abstract(p)]
            total      = len(papers)
            miss_count = len(missing)

            logger.info("[%s] Scrape — Total: %d | Missing: %d", cat, total, miss_count)

            if miss_count == 0:
                _save_json(papers, output_path)
                all_stats.append((cat, {
                    "total": total, "missing": 0, "recovered": 0,
                    "per_publisher_missing": {}, "per_publisher_recovered": {},
                }))
                continue

            pub_missing: dict[str, int] = defaultdict(int)
            for p in missing:
                doi = (p.get("externalIds") or {}).get("DOI", "")
                pub_missing[_publisher_from_doi(doi)] += 1

            pub_recovered: dict[str, int] = defaultdict(int)
            recovered_count = 0
            start_time      = time.time()

            for idx, paper in enumerate(missing, start=1):
                doi    = (paper.get("externalIds") or {}).get("DOI", "")
                result = scrape_provider.recover(paper)
                elapsed = time.time() - start_time
                avg     = elapsed / idx
                eta     = int(avg * (miss_count - idx))

                if result:
                    paper["abstract"] = result.abstract
                    recovered_count  += 1
                    pub_label = result.source.replace("scrape:", "")
                    pub_recovered[pub_label] += 1
                    tag = "RECOVERED"
                else:
                    pub_label = _publisher_from_doi(doi)
                    tag = "FAILED   "

                logger.info(
                    "[%s][%4d/%-4d] %s | %-22s | ETA %5ds | %s",
                    cat, idx, miss_count, tag, pub_label, eta,
                    paper.get("title", "")[:50],
                )

            total_time = time.time() - start_time
            _save_json(papers, output_path)

            print_scrape_report(
                filename=f"{cat}.json",
                total=total,
                miss_count=miss_count,
                recovered_count=recovered_count,
                pub_missing=dict(pub_missing),
                pub_recovered=dict(pub_recovered),
                total_time=total_time,
                output_path=str(output_path),
            )

            for pub, cnt in pub_missing.items():
                combined_missing[pub]   += cnt
            for pub, cnt in pub_recovered.items():
                combined_recovered[pub] += cnt

            all_stats.append((cat, {
                "total":                   total,
                "missing":                 miss_count,
                "recovered":               recovered_count,
                "per_publisher_missing":   dict(pub_missing),
                "per_publisher_recovered": dict(pub_recovered),
            }))

        total_papers    = sum(s["total"]     for _, s in all_stats)
        total_missing   = sum(s["missing"]   for _, s in all_stats)
        total_recovered = sum(s["recovered"] for _, s in all_stats)

        print_overall_scrape_summary(
            total_papers=total_papers,
            total_missing=total_missing,
            total_recovered=total_recovered,
            combined_missing=dict(combined_missing),
            combined_recovered=dict(combined_recovered),
            output_dir=str(output_dir),
        )

    @staticmethod
    def _categories_from_dir(directory: Path) -> list[str]:
        return sorted(
            p.stem for p in directory.glob("*.json")
            if p.is_file()
        )

    def _create_dirs(self) -> None:
        for d in self._dirs.values():
            d.mkdir(parents=True, exist_ok=True)