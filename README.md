# Automated-Paper-Data-Retriever-System

This is an automated paper data retriever system which has is divided into two pipelines. The first part, **paper_metadata**, fetches academic paper metadata from Semantic Scholar by keyword-driven bulk search, deduplicates by paper ID and title across categories, and recovers missing metadata using multi-sources reterival systems. The second part, **paper_downloader**, resolves and downloads open-access PDFs for given paper identifiers across 11 source providers. Both pipelines are accessible from the command line and as a callable Python library via `paper_data.py`. This repository is currently under construction.

---

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [API Keys and Environment Setup](#api-keys-and-environment-setup)
- [Configuration](#configuration)
  - [paper_downloader config.json](#paper_downloader-configjson)
  - [paper_metadata config.json](#paper_metadata-configjson)
- [Paper Metadata Pipeline](#paper-metadata-pipeline)
  - [How It Works](#how-it-works)
  - [search_queries.json](#search_queriesjson)
  - [Running from the CLI](#running-from-the-cli)
  - [Programmatic Usage (paper_metadata)](#programmatic-usage-paper_metadata)
  - [Output Structure (paper_metadata)](#output-structure-paper_metadata)
- [Input Formats (paper_downloader)](#input-formats-paper_downloader)
- [Running the Download Pipeline](#running-the-download-pipeline)
- [Output Structure (paper_downloader)](#output-structure-paper_downloader)
- [Source Providers](#source-providers)
- [Resume and Idempotency](#resume-and-idempotency)
- [Programmatic Usage (paper_downloader)](#programmatic-usage-paper_downloader)
- [Troubleshooting](#troubleshooting)

---

## Requirements

- Python 3.10 or higher

---

## Installation

Clone the repository and install the dependencies:

```bash
git clone https://github.com/Panzer3232/research_paper_pdf_downloader.git
cd research_paper_pdf_downloader
pip install -r requirements.txt
```

No other system dependencies are required. All output is written to a local `data/` directory that is created automatically on first run.

---

## API Keys and Environment Setup

API keys are read from a `.env` file in the project root directory.

### Using the pipelines as a library from your own project folder

This is the recommended setup for teams using `paper_data` as a library. You have your own project folder, cloned the repository separately, and call the functions from your own code. You do not need to create `.env` files inside the cloned repository at all.

**Create a single `.env` file in your own project folder** — the folder where you run your script from:

```
/home/user/myproject/
├── my_script.py
└── .env                  ← place your .env here
```

That single `.env` file covers both pipelines. Use this format:

```
SEMANTIC_SCHOLAR_API_KEY=
OPENALEX_API_KEY=
UNPAYWALL_EMAIL=
CORE_API_KEY=
CROSSREF_EMAIL=
OPENAI_API_KEY=
OPENAI_BASE_URL=
```

**Always run your script from the folder that contains your `.env`:**

```bash
cd /home/user/myproject
python my_script.py
```

Both loaders fall back to searching upward from the current working directory when no `.env` is found next to the pipeline's own `config.json`. As long as you run from your project folder, both pipelines will find your `.env` there automatically.

---

## Configuration

### paper_downloader config.json

The `config.json` file controls all pipeline behaviour.

---

### paper_metadata config.json

Located at `paper_metadata/config.json`. Controls all metadata fetch, deduplication, and abstract recovery behaviour. Do changes accordingly, by default pipeline will run fine.

**Important fields:**

| Field | What it controls |
|---|---|
| `output.base_dir` | Root directory where all `search_results/` subdirectories are created. **Set this to your project folder** |
| `search_queries_path` | Absolute path to `search_queries.json`. **Set this to your queries file location** |

To add new keyword categories or change existing search terms, edit `search_queries.json` directly, no code changes needed.

---

## Paper Metadata Pipeline

### How It Works

The pipeline runs five stages in sequence:

**Stage 1 — Semantic Scholar bulk fetch.** For each category defined in `search_queries.json`, the pipeline runs two paginated bulk queries against the Semantic Scholar API. Each query fetches up to 1000 papers per request and follows pagination tokens until all results are retrieved. Results are saved as raw JSON slices in `search_results/raw_old/` and `search_results/raw_new/`.

**Stage 2 — ID-based deduplication.** Within each category, papers sharing the same Semantic Scholar `paperId` are collapsed (intra-category dedup). Then, across all categories in priority order, any paper that already appeared in a higher-priority category is removed from lower-priority categories (inter-category dedup). This ensures each paper appears in exactly one category. Results are saved to `search_results/final/`.

**Stage 3 — Title-based deduplication.** The same paper can exist under two different `paperId` values if it appears in SS as both a preprint and a published version. Title-based dedup catches these cases: titles are normalised (lowercased, punctuation stripped, version suffixes removed) and compared. When duplicates are found the version with the higher citation count is kept. Intra and inter-category passes are both run. Duplicate reports are saved as CSV files in `search_results/reports/`. Results are saved to `search_results/final_title_deduped/`.

**Stage 4 — API-based abstract recovery.** For papers with missing abstracts, the pipeline tries a chain of API sources in order. The chain differs based on whether an ArXiv ID is present. Sources tried include ArXiv (by ID and title), OpenAlex (by DOI and title), PubMed, ACL Anthology, EuropePMC (by DOI, PMID, and title), Crossref, CORE, and Semantic Scholar as the final fallback. Each source applies title similarity verification when searching by title to avoid returning abstracts for the wrong paper. Results are saved to `search_results/final_recovered_abstract/`.

**Stage 5 — Scrape-based abstract recovery.** For papers that still lack an abstract after Stage 4, the pipeline attempts to scrape the publisher webpage via the paper's DOI. Publisher-specific HTML parsers handle Springer, Nature, IEEE, Elsevier, Wiley, Taylor & Francis, Oxford UP, Cambridge UP, Frontiers, MDPI, ACM, PLOS, AAAI, and IJCAI. A generic JSON-LD and meta-tag fallback handles any unrecognised publisher. Results are saved to `search_results/publisher_scraped/`.

---

### search_queries.json

This file defines the keyword categories used to fetch papers from Semantic Scholar. It is a JSON object where each key is a category name and each value is a Semantic Scholar bulk search query string. Category names become the filenames for all output JSON files throughout the pipeline. The order of categories matters for inter-category deduplication: papers that match multiple categories are assigned to the first matching category. To add a new research topic, add a new key-value pair. To change the scope of an existing search, edit the query string. The Semantic Scholar bulk search supports Boolean operators `|` (OR), `+` (AND), and quoted phrases.

---

### Running from the CLI

All commands are run from the **parent directory** of `paper_metadata/` using Python's `-m` flag.

**Full pipeline with explicit output location and queries file (overrides config.json):**

```bash
python -m paper_metadata.main \
  --base-dir /path \
  --search-queries-path /path
```

---

### Programmatic Usage (paper_metadata)

```python
from paper_data import fetch_metadata, recover_abstracts

# Full pipeline with default config
fetch_metadata()

# Full pipeline, custom output location, both recoveries
fetch_metadata(
    base_dir="/path",                  # your local project directory path
    search_queries_path="/path",
)

# Full pipeline, custom config file, skip scrape recovery
fetch_metadata(
    config_path="/path/to/your/config.json",
    scrape_recovery=False,
)

# Fetch and dedup only, no recovery
fetch_metadata(api_recovery=False, scrape_recovery=False)

# Run both recoveries on already-fetched data
recover_abstracts(
    "/path/to/search_results/final_title_deduped"
)

# Run API recovery only on existing files
recover_abstracts(
    "/path/to/search_results/final_title_deduped",
    scrape_recovery=False,
)

# Run scrape recovery only on files that already went through API recovery
recover_abstracts(
    "/path/to/search_results/final_recovered_abstract",
    api_recovery=False,
)
```

---

### Output Structure (paper_metadata)

All output is created under `base_dir/search_results/`:

```
search_results/
  raw_old/
    1_xai_llm.json          # Raw SS fetch, old date range
    2_mi_llm.json
    ...
  raw_new/
    1_xai_llm.json          # Raw SS fetch, recent date range
    ...
  final/
    1_xai_llm.json          # After ID-based deduplication
    ...
  final_title_deduped/
    1_xai_llm.json          # After title-based deduplication
    ...
  final_recovered_abstract/
    1_xai_llm.json          # After API-based abstract recovery
    ...
  publisher_scraped/
    1_xai_llm.json          # After scrape-based abstract recovery
    ...
  reports/
    acquisition_stats.json
    title_dedup_stats.json
    intra_title_duplicates.csv
    inter_title_duplicates.csv
```

Each stage reads from the previous stage's directory and writes to its own. Earlier stage outputs are preserved so you can re-run any stage independently using `--input-dir`.

---

## Input Formats (paper_downloader)

The pipeline accepts several input formats, all passed via the `--input` argument.

**A JSON file containing a list of Semantic Scholar metadata records**

This is the recommended format if you are working with data exported from Semantic Scholar. You can just input papers metadata json file, based on the semantic scholar paperID it will download the papers and in the end new json file is given with download stats (file path, downloaded status etc).

**A JSON file containing a list of identifier strings**

A plain list of identifiers. Each string can be a Semantic Scholar paper ID. Although it works with arxivID but paperID is safe.

**A single identifier string passed directly**

```bash
python main.py --input "004e5d24c1e8511519fc081b6d723c55651f80b9"
python main.py --input "10.1016/j.websem.2024.100822"
python main.py --input "2410.20513"
```

---

## Running the Download Pipeline

Basic usage (config file is default):

```bash
python main.py --input your_papers.json
```

With an explicit config file (if custom config file is used):

```bash
python main.py --input your_papers.json --config config.json
```

With enriched output — writes a copy of your input JSON with `pdf_path`, `download_status`, and `downloaded` fields added to each record:

```bash
python main.py --input your_papers.json --output your_papers_enriched.json
```

If `--output` is not provided, the enriched file is written automatically as `your_papers_enriched.json` in the same directory as the input file.

With a custom stats output directory and a label to identify this run:

```bash
python main.py --input your_papers.json --stats-dir /path/to/stats --run-label batch_01
```

With logging to a file: set `"log_file": "pipeline.log"` in the `logging` section of `config.json`.

---

## Output Structure (paper_downloader)

After a run, the following directory structure is created under the configured `root_dir` (default `data/`):

```
data/
  pdfs/
    arxiv__2410.20513.pdf
    doi__10.1016_j.websem.2024.100822.pdf
    ...
  metadata/
    arxiv__2410.20513.json
    ...
  manifests/
    arxiv__2410.20513.json
    ...
  download_stats/
    download_stats_20260328T230509Z_full.json
    download_stats_20260328T230509Z_short.json
    download_stats_20260328T230509Z_short.csv
```

**PDFs** are named by the paper key derived from the best available identifier. The key format is `doi__...`, `arxiv__...`, `ss__...`, or `corpus__...`.

**Metadata** files are JSON snapshots of the paper record as it was known at the time of processing, including all recovered identifiers.

**Manifests** are per-paper JSON files that record the full processing history: every pipeline stage, its status, timestamps, retry count, and the source that was selected and downloaded. If a run is interrupted, manifests allow the pipeline to resume correctly on the next run without re-downloading files that already exist.

**Stats files** are written after every run. Three files are produced per run with a UTC timestamp in the filename so no run overwrites a previous one:

- `_full.json` — complete result for every paper including all provider attempts, download attempts, selected source, and error details.
- `_short.json` — compact version with one row per paper: paper ID, title, downloaded (true/false), status, and pdf_path.
- `_short.csv` — same compact data in CSV format for spreadsheet use.

**Enriched input file** — if `--output` is specified or auto-derived, a copy of your input JSON is written with three fields added to each paper record:

- `pdf_path` — absolute path to the downloaded PDF, or null if the download failed.
- `download_status` — one of `downloaded`, `already_exists`, `failed_unresolved_no_legal_pdf`, `failed_download_failed_all_candidates`, or similar.
- `downloaded` — boolean, true if a PDF is available on disk.

---

## Source Providers

The pipeline queries up to 11 open-access source providers in priority order. Providers are tried sequentially. All candidates from all providers are collected, scored, and ranked before the best one is selected for download.

| Provider | What it does | API key required |
|---|---|---|
| metadata_open_access | Reads the `openAccessPdf` URL directly from Semantic Scholar metadata | No |
| arxiv | Constructs the PDF URL directly from the ArXiv ID | No |
| acl | Constructs the PDF URL from the ACL Anthology ID or ACL DOI | No |
| cvf | Constructs the PDF URL from the CVF (CVPR/ICCV/ECCV) paper title | No |
| openalex | Looks up open-access locations via the OpenAlex API | Optional |
| unpaywall | Looks up open-access locations via the Unpaywall API | Email required |
| europepmc | Searches EuropePMC for life science papers with full text | No |
| crossref | Extracts PDF links from Crossref work metadata | Optional (email) |
| core | Searches CORE for repository copies | Key required |
| zenodo | Searches Zenodo for deposited copies | No |
| doaj | Searches the Directory of Open Access Journals | No |
| broad_search | Falls back to DuckDuckGo site-scoped search as a last resort | No |

### Scoring System

The scoring system prefers publisher versions over accepted manuscripts over preprints. Within each version type, candidates from trusted domains are ranked above unknown domains. A paper is considered downloaded if any candidate succeeds. If all candidates fail, the paper is marked as `failed_unresolved_no_legal_pdf` and appears in the failed count in the stats summary.

When multiple providers return PDF candidates for the same paper, the pipeline ranks them using a two-layer system before attempting any download. The first layer is a hard categorical sort: candidates are ordered by whether they are a publisher version, whether the URL is a direct `.pdf` link, and whether the hosting server is a known publisher domain — in that priority order, evaluated as a tuple so a confirmed publisher version always outranks a preprint regardless of score. The second layer is a continuous quality score built from six independent additive signals: a direct PDF link adds `+0.20`, a domain matching the `trusted_domains` config list adds `+0.20`, a publisher version type adds `+0.30` (or `+0.15` if `prefer_publisher_version` is false) while an accepted version adds `+0.20`, a preprint adds `+0.10` if allowed or `-1.00` if `allow_preprints` is false effectively eliminating it, a publisher host type adds `+0.10` and a repository host `+0.05`, a title similarity score at or above `title_similarity_threshold` adds `+0.10` while a score below threshold adds `-0.20`, and finally the provider's own base confidence — ranging from `0.62` for BroadSearch up to `0.97` for ACL Anthology — contributes at most `+0.10` (scaled by a factor of `0.10`) so that the pipeline's own structural signals always outweigh any single provider's self-reported certainty. Before ranking, duplicate URLs across providers are collapsed keeping only the higher-scored entry. The top-ranked candidate is attempted first; if it fails to download, the pipeline falls through the ranked list automatically. Every candidate's full score breakdown is persisted in the paper's manifest JSON under `stats.resolution.all_candidates` for inspection.

---

## Resume and Idempotency

The pipeline is safe to re-run on the same input. On each run:

- Papers whose PDF already exists on disk and whose manifest shows a completed download stage are skipped. The log will say `pdf already exists, skipping download`.
- Papers that previously failed are retried from the failed stage.
- Stats files use timestamps in their names so each run produces new files without overwriting previous results.

To force a full re-download of everything, delete the `data/` directory before running.

---

## Programmatic Usage (paper_downloader)

The pipeline can be used as a callable library without the CLI. This is useful when integrating the downloader into a larger pipeline or calling it from another script.

### Installation

From the project root, install in editable mode once:

```bash
pip install -e .
```

After this, `paper_data.py` is importable from any folder without path manipulation.

### Basic Usage

```python
from paper_data import download

results = download("649def34f8be52c8b66281af98ae884c09aef38b")

results = download([
    "649def34f8be52c8b66281af98ae884c09aef38b",
])

for r in results:
    if r.downloaded:
        print(r.pdf_path)
    else:
        print(r.status, r.error)
```

---

## Troubleshooting

**The pipeline reports a paper as `failed_unresolved_no_legal_pdf`**

This means all 11 providers returned no downloadable PDF for that paper. Common reasons: the paper is closed access with no preprint (IEEE, ACM, Elsevier without OA), or it is too recent to have been indexed by repositories. The pipeline does not attempt to bypass paywalls and will not download content that is not legally open access.

**The pipeline is slow**

The pipeline processes papers sequentially. Each paper queries multiple external APIs in sequence. The primary factors affecting speed are network latency to the API servers and whether rate limiting causes backoff delays. Providing API keys (particularly for Semantic Scholar and CORE) significantly reduces rate-limiting delays. If your input contains only papers with ArXiv IDs, most papers resolve in one or two provider calls and are fast.

**Import errors after installation**

Ensure you are running Python 3.10 or higher and that you installed dependencies with `pip install -r requirements.txt` in the same environment. Check that your working directory is the repository root when running `python main.py`, so that the `app/` package is on the Python path.

**Config file not found**

The pipeline looks for `config.json` next to `main.py` by default. If you run `main.py` from a different directory, pass the config path explicitly with `--config /path/to/config.json`.

**API keys not being picked up**

Ensure your `.env` file is in the same directory as `main.py` and that the variable names exactly match those listed in the API Keys section above. Environment variables set in the shell take precedence over `.env` file values.

**paper_metadata: search_queries.json or base_dir not found**

Set the `search_queries_path` field or `base_dir` in `config.json` to the absolute path of your `search_queries.json` file, or pass both explicitly on the command line:

```bash
python -m paper_metadata.main \
  --base-dir /your/output/path \
  --search-queries-path /path/to/search_queries.json
```
