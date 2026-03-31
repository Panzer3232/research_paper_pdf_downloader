# research-paper-pdf-downloader

An automated pipeline for downloading academic paper PDFs. Given a list of paper identifiers or a JSON file of Semantic Scholar metadata records, the pipeline resolves open-access PDF sources across 11 providers(for now...added in later versions), downloads the PDFs. This repository is currently under construction.

---

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [API Keys and Environment Setup](#api-keys-and-environment-setup)
- [Configuration](#configuration)
- [Input Formats](#input-formats)
- [Running the Pipeline](#running-the-pipeline)
- [Output Structure](#output-structure)
- [Source Providers](#source-providers)
- [Resume and Idempotency](#resume-and-idempotency)
- [Programmatic Usage](#programmatic-usage)
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

API keys are read from a `.env` file in the project root directory. They are never stored in `config.json` and never committed to the repository.


SEMANTIC_SCHOLAR_API_KEY=your_key_here
OPENALEX_API_KEY=your_key_here
UNPAYWALL_EMAIL=your_email@example.com
CORE_API_KEY=your_key_here
CROSSREF_EMAIL=your_email@example.com


## Configuration


The `config.json` file controls all pipeline behaviour.



## Input Formats

The pipeline accepts several input formats, all passed via the `--input` argument.

**A JSON file containing a list of Semantic Scholar metadata records**

This is the recommended format if you are working with data exported from Semantic Scholar. You can just input papers metadata json file, based on the semantic scholar paperID it will download the papers and in the end new json file is given with download stats ( file path, downloaded status etc).

**A JSON file containing a list of identifier strings**

A plain list of identifiers. Each string can be a Semantic Scholar paper ID. Although it works wth arxivID but paperID is safe.


**A single identifier string passed directly**

```bash
python main.py --input "004e5d24c1e8511519fc081b6d723c55651f80b9"
python main.py --input "10.1016/j.websem.2024.100822"
python main.py --input "2410.20513"
```

## Running the Pipeline

Basic usage (config file is default):

```bash
python main.py --input your_papers.json
```

With an explicit config file(if custom config file is used):

```bash
python main.py --input your_papers.json --config config.json
```

With enriched output: writes a copy of your input JSON with `pdf_path`, `download_status`, and `downloaded` fields added to each record:

```bash
python main.py --input your_papers.json --output your_papers_enriched.json
```

If `--output` is not provided, the enriched file is written automatically as `your_papers_enriched.json` in the same directory as the input file.

With a custom stats output directory and a label to identify this run:

```bash
python main.py --input your_papers.json --stats-dir /path/to/stats --run-label batch_01
```

With logging to a file:

Set `"log_file": "pipeline.log"` in the `logging` section of `config.json`.

---

## Output Structure

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
## Programmatic Usage

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

Ensure your `.env` file is in the same directory as `main.py` and that the variable names exactly match those in `.env.example`. Environment variables set in the shell take precedence over `.env` file values.
