# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-...
```

## Running the pipeline

```bash
# Full run with human review gate before downloads
python -m kosmos_pipeline -o ./runs/2026-04-07 --review --stage DEV

# Search + evaluate only (no downloads or upload)
python -m kosmos_pipeline -o ./runs/2026-04-07 --skip-download --skip-upload

# Resume an interrupted run (re-run same command — checkpoints are auto-detected)
python -m kosmos_pipeline -o ./runs/2026-04-07

# Exclude already-benchmarked papers
python -m kosmos_pipeline -o ./runs/2026-04-07 --exclude-dois existing_papers.xlsx --review
```

There are no automated tests or linting tools configured in this repo.

## Architecture

This is a 7-step pipeline for discovering and curating computational biology papers with public datasets for the Kosmos AI benchmark system. Entry point: `python -m kosmos_pipeline` (routes through `__main__.py` → `run.py`).

**Pipeline steps and their modules:**

| Step | Module | Purpose |
|------|--------|---------|
| 1 | `search.py` | Query Europe PMC + PubMed APIs for recent open-access papers |
| 2 | `filtering.py` | Filter by journal tier, article type, open-access status |
| 3 | `evaluate.py` | Claude API evaluation of paper suitability and dataset reproducibility |
| 3.5 | `verify_size.py` | Validate actual dataset sizes via GEO FTP, Zenodo, OpenNeuro, etc. |
| 4 | `score.py` | Quality scoring with journal-tier penalty and topic diversity adjustment |
| 5 | `run.py` | Output final JSON; optional `--review` pause for human inspection |
| 5.5–6 | `download.py` | Download paper PDFs and datasets from supported repositories |
| 7 | `upload.py` | Upload datasets to Edison via `kopt.probe.upload`; generate `run.yaml` probes |

**Key design patterns:**

- **Checkpointing**: Every step writes a JSON checkpoint to `<output-dir>/intermediate/`. Re-running the same command resumes from the last completed step.
- **Parallelism**: Steps use `ThreadPoolExecutor` with configurable worker counts (`EVAL_WORKERS=10`, `SIZE_CHECK_WORKERS=15`, etc.).
- **Script filtering** (`script_filter.py`): Strips analysis scripts and code from downloaded datasets to prevent answer leakage into benchmarks. Controlled by extension/filename/directory allowlists in `config.py`.
- **Dataset classification**: Each accession in Claude's evaluation output is tagged `primary`, `reanalyzed_key`, or `reanalyzed`. Only `primary` and `reanalyzed_key` are downloaded.
- **Paper diversity**: Final selection (`score.py`) applies a one-paper-per-domain-first rule before filling remaining slots, ensuring breadth across biological domains.

**Configuration** (`config.py`): All tunable parameters live here — journal tier lists, modality tiers, API endpoints, scoring weights, parallelism settings, rate limits, and script-filtering patterns. Defaults: max 150 candidates evaluated, max 20 papers output, max 15 GB dataset size, min quality score 3.0, 6-month rolling date window.

**Supported dataset repositories**: GEO, SRA, Zenodo, OpenNeuro, OMIX/HRA (NGDC/CNCB), Figshare, Dryad.

**Output structure:**
```
runs/<date>/
├── status.json                            # Live progress (for external monitoring)
├── computational_biology_papers_YYYY.json # Final curated papers
├── manifest.json                          # Links paper → PDF → probe → data entry
├── intermediate/                          # Step checkpoints (enables resume)
├── pdfs/
├── datasets/
└── probes/                                # Edison run.yaml files
```
