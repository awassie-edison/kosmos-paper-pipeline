# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
uv sync
export ANTHROPIC_API_KEY=sk-...
```

## Running the pipeline

```bash
# Generate a default config.yaml
uv run python -m kosmos_pipeline init ./my-run

# Edit ./my-run/config.yaml to customize parameters, then run:
uv run python -m kosmos_pipeline run --config ./my-run/config.yaml --review

# With operational flags
uv run python -m kosmos_pipeline run --config config.yaml --skip-download --skip-upload -v

# Resume an interrupted run (re-run same command — checkpoints are auto-detected)
uv run python -m kosmos_pipeline run --config config.yaml
```

CLI subcommands:
- `init [directory]` — writes a `config.yaml` with all defaults to the given directory
- `run --config <path>` — runs the pipeline. Operational flags: `--review`, `--no-resume`, `--skip-download`, `--skip-upload`, `--verbose`

All pipeline parameters (model, thresholds, journal lists, search queries, evaluation rubric, rate limits, etc.) are in `config.yaml`. See `config.default.yaml` for the full reference with defaults.

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

- **YAML-driven config** (`config.py`): All parameters are defined in a `config.yaml` passed via `--config`. `config.py` contains frozen dataclass hierarchy (`PipelineConfig` → section dataclasses), YAML loader with deep merge (partial overrides work), and a module-level singleton accessed via `get_config()`. `init_config(path)` must be called once before any module reads config.
- **Checkpointing**: Every step writes a JSON checkpoint to `<output-dir>/intermediate/`. Re-running the same command resumes from the last completed step.
- **Parallelism**: Steps use `ThreadPoolExecutor` with configurable worker counts (in `parallelism` config section).
- **Script filtering** (`script_filter.py`): Strips analysis scripts and code from downloaded datasets to prevent answer leakage into benchmarks. Controlled by extension/filename/directory patterns in `script_filtering` config section.
- **Dataset classification**: Each accession in Claude's evaluation output is tagged `primary`, `reanalyzed_key`, or `reanalyzed`. Only `primary` and `reanalyzed_key` are downloaded.
- **Paper diversity**: Final selection (`score.py`) applies a one-paper-per-domain-first rule before filling remaining slots, ensuring breadth across biological domains.

**Supported dataset repositories**: GEO, SRA, Zenodo, OpenNeuro, OMIX/HRA (NGDC/CNCB), Figshare, Dryad.

**Output structure:**
```
runs/<date>/
├── config.yaml                            # The config used for this run
├── status.json                            # Live progress (for external monitoring)
├── computational_biology_papers_YYYY.json # Final curated papers
├── manifest.json                          # Links paper → PDF → probe → data entry
├── intermediate/                          # Step checkpoints (enables resume)
├── pdfs/
├── datasets/
└── probes/                                # Edison run.yaml files
```

## Config YAML sections

All parameters live in `config.yaml`. Sections:

| Section | What it controls |
|---------|-----------------|
| `pipeline` | model, max_candidates, max_papers, date_window_months, output_dir, stage, kosmos_opt_dir, exclude_dois_file |
| `thresholds` | max_data_size_gb, max_datasets_per_paper, min_quality_score, min_domains, tier_2_penalty |
| `journals` | tier_1/tier_2 journal lists, priority ranking for candidate ordering |
| `modalities` | tier_1/tier_2 data modality lists |
| `search` | europepmc_queries (list of name+text), pubmed_query_term |
| `evaluation` | system_prompt (the full Claude rubric), max_tokens |
| `parallelism` | search_workers, eval_workers, size_check_workers, download_workers |
| `rate_limits` | per-API delays (europepmc, pubmed, geo, zenodo, claude_api, openneuro, omix, figshare, dryad) |
| `api_endpoints` | base URLs for all external APIs |
| `timeouts` | HTTP timeouts by category (api_request, api_listing, download_stream, download_large, head_request, pdf_download, upload_command) |
| `script_filtering` | data_extensions, script_extensions, script_filenames, script_directories |
| `filtering` | exclude_type_patterns (regex), exclude_tool_patterns (regex), excluded_pub_types |
| `upload` | num_steps, run_tardigrade, enable_literature_research |

## Paper filtering and scoring parameters

Papers pass through four successive gates. All thresholds are in the config YAML.

### Step 2 — Hard filters (`filtering.py`)

A paper is dropped if **any** of the following apply:

| Parameter | Rule |
|-----------|------|
| **Publication date** | Must fall within the rolling date window (`pipeline.date_window_months`, default 6) |
| **Journal** | Must appear in `journals.tier_1` or `journals.tier_2`; unknown journals are excluded |
| **Article type** | Excluded if `pub_types` or title matches patterns in `filtering.exclude_type_patterns` (review, perspective, editorial, etc.) |
| **Tool/method paper** | Excluded if title matches patterns in `filtering.exclude_tool_patterns` |
| **Open access** | `is_open_access` must equal `"Y"` |
| **DOI blocklist** | Dropped if the DOI appears in the file at `pipeline.exclude_dois_file` |

Tier 1 journals are prioritised within the candidate list using `journals.priority` (Nature/Science/Cell at top).

### Step 3 — Claude evaluation (`evaluate.py`)

Claude scores each paper 1–5 using the rubric in `evaluation.system_prompt`. Papers scoring < 3 are dropped.

| Sub-criterion | Weight | What is assessed |
|---------------|--------|-----------------|
| **Hypothesis chain** | 40% | Requires ≥3 sequential computational steps. Descriptive atlases, resource papers, and parallel independent analyses are excluded. |
| **Dataset reproducibility** | 35% | Fraction of chain reproducible from public data. No controlled access. ≥80% preferred. |
| **Data modality** | 10% | Tier 1 preferred (scRNA-seq, spatial, multiome, etc.); multi-modal bonus. |
| **Open access license** | 5% | CC-BY preferred; CC-BY-NC acceptable. |
| **Figures from dataset** | 10% | Higher fraction of main figures from deposited data = higher score. |

Hard constraints: total data < `thresholds.max_data_size_gb`, at most `thresholds.max_datasets_per_paper` datasets (primary + reanalyzed_key).

### Step 3.5 — Size verification (`verify_size.py`)

Replaces estimated sizes with real numbers from repository APIs. Papers exceeding `thresholds.max_data_size_gb` are rejected.

### Step 4 — Scoring and diversity (`score.py`)

| Adjustment | Rule |
|------------|------|
| **Tier 2 penalty** | `quality_score -= thresholds.tier_2_penalty` for Tier 2 journals |
| **Minimum score** | Papers below `thresholds.min_quality_score` after penalty are dropped |
| **Diversity selection** | One paper per domain first; remaining slots from overall ranking. Target ≥ `thresholds.min_domains` domains in final set of `pipeline.max_papers`. |
