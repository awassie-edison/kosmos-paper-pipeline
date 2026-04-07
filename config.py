"""Pipeline configuration: YAML-driven with frozen dataclass hierarchy."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field, fields, asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Dataclass hierarchy — one per YAML section
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PipelineSettings:
    model: str = "claude-opus-4-6"
    max_candidates: int = 150
    max_papers: int = 20
    date_window_months: int = 6
    output_dir: str | None = None
    kosmos_opt_dir: str | None = None
    exclude_dois_file: str | None = None
    stage: str = "DEV"


@dataclass(frozen=True)
class Thresholds:
    max_data_size_gb: float = 15.0
    max_datasets_per_paper: int = 5
    min_quality_score: float = 3.0
    min_domains: int = 4
    tier_2_penalty: float = 0.5


@dataclass(frozen=True)
class JournalConfig:
    tier_1: frozenset[str] = field(default_factory=lambda: frozenset({
        "nature", "nature genetics", "nature methods", "nature biotechnology",
        "nature neuroscience", "nature medicine", "nature cell biology",
        "nature communications", "nature ecology & evolution",
        "nature structural & molecular biology", "nature chemical biology",
        "cell", "molecular cell", "cell systems", "cell reports",
        "cell genomics", "cell stem cell", "cell metabolism",
        "science", "science advances",
        "elife", "genome biology", "genome research",
        "plos computational biology", "plos genetics",
        "nucleic acids research",
        "proceedings of the national academy of sciences",
        "the embo journal", "embo reports",
    }))
    tier_2: frozenset[str] = field(default_factory=lambda: frozenset({
        "cells", "frontiers in immunology", "frontiers in genetics",
        "frontiers in neuroscience", "frontiers in cell and developmental biology",
        "scientific reports", "bmc genomics", "bmc bioinformatics",
        "plos one", "iscience", "cell reports methods",
        "nar genomics and bioinformatics", "briefings in bioinformatics",
        "gigascience", "molecular systems biology", "communications biology",
        "bioinformatics (oxford, england)", "bioinformatics",
        "npj systems biology and applications",
        "advanced science (weinheim, baden-wurttemberg, germany)", "advanced science",
    }))
    priority: dict[str, int] = field(default_factory=lambda: {
        "nature": 0, "science": 0, "cell": 0,
        "nature genetics": 1, "nature neuroscience": 1,
        "nature medicine": 1, "nature cell biology": 1,
        "nature methods": 1, "nature biotechnology": 1,
        "cell genomics": 1, "cell systems": 1, "cell reports": 1,
        "molecular cell": 1, "cell stem cell": 1, "cell metabolism": 1,
        "science advances": 2, "nature communications": 2,
        "genome biology": 2, "genome research": 2,
        "elife": 3, "the embo journal": 3, "embo reports": 3,
        "proceedings of the national academy of sciences": 3,
        "nucleic acids research": 4, "plos genetics": 4,
        "plos computational biology": 5,
    })


@dataclass(frozen=True)
class ModalityConfig:
    tier_1: frozenset[str] = field(default_factory=lambda: frozenset({
        "scrna-seq", "snrna-seq", "spatial transcriptomics",
        "scatac-seq", "multiome", "perturb-seq", "crop-seq",
    }))
    tier_2: frozenset[str] = field(default_factory=lambda: frozenset({
        "bulk rna-seq", "proteomics", "phosphoproteomics",
        "metagenomics", "metabolomics", "chip-seq",
        "cut&run", "cut&tag", "hi-c",
        "whole-genome sequencing", "exome sequencing",
    }))


@dataclass(frozen=True)
class EuropmcQuery:
    name: str = ""
    text: str = ""


_DEFAULT_EUROPEPMC_QUERIES = [
    EuropmcQuery(
        name="query1_nature_science_cell",
        text=(
            '(JOURNAL:"Nature" OR JOURNAL:"Science" OR JOURNAL:"Cell" '
            'OR JOURNAL:"Nature Genetics" OR JOURNAL:"Nature Methods" '
            'OR JOURNAL:"Nature Biotechnology" OR JOURNAL:"Nature Communications" '
            'OR JOURNAL:"eLife" OR JOURNAL:"Genome Biology" OR JOURNAL:"Genome Research") '
            "AND (computational OR bioinformatic OR integrative analysis) "
            "AND (dataset OR GEO OR accession OR deposited)"
        ),
    ),
    EuropmcQuery(
        name="query2_cell_sub_plos_pnas",
        text=(
            '(JOURNAL:"Molecular Cell" OR JOURNAL:"Cell Systems" '
            'OR JOURNAL:"Cell Reports" OR JOURNAL:"Cell Genomics" '
            'OR JOURNAL:"PLOS Computational Biology" OR JOURNAL:"PLOS Genetics" '
            'OR JOURNAL:"Nucleic Acids Research" '
            'OR JOURNAL:"Proceedings of the National Academy of Sciences") '
            "AND (computational OR bioinformatic OR systematic analysis) "
            "AND (dataset OR publicly available OR repository)"
        ),
    ),
    EuropmcQuery(
        name="query3_nature_sub",
        text=(
            '(JOURNAL:"Nature Neuroscience" OR JOURNAL:"Nature Medicine" '
            'OR JOURNAL:"Nature Cell Biology" OR JOURNAL:"Nature Biotechnology" '
            'OR JOURNAL:"Nature Methods") '
            "AND (analysis OR computational OR modeling) "
            "AND (data OR dataset OR sequencing)"
        ),
    ),
    EuropmcQuery(
        name="query4_singlecell_spatial",
        text=(
            "(single-cell OR spatial transcriptomics OR multi-omics OR single-nucleus) "
            "AND (mechanism OR model OR pathogenesis OR reveals)"
        ),
    ),
    EuropmcQuery(
        name="query5_disease",
        text=(
            "(cancer genomics OR neurodegeneration OR immune OR developmental biology) "
            "AND (computational framework OR integrative analysis OR systematic characterization) "
            "AND (GEO OR accession OR Zenodo OR deposited)"
        ),
    ),
    EuropmcQuery(
        name="query6_evo_struct_systems",
        text=(
            "(evolutionary OR phylogenomic OR structural biology OR systems biology "
            "OR gene regulatory network) "
            "AND (computational OR modeling OR simulation) "
            "AND (dataset OR publicly available)"
        ),
    ),
    EuropmcQuery(
        name="query7_tier2_strong_datasets",
        text=(
            "(single-cell RNA-seq OR snRNA-seq OR spatial transcriptomics "
            "OR ATAC-seq OR multi-omics) "
            "AND (GEO OR accession OR deposited OR Zenodo)"
        ),
    ),
]

_DEFAULT_PUBMED_QUERY_TERM = (
    "(computational biology[MeSH] OR genomics[MeSH] OR systems biology[MeSH]) "
    "AND (Nature[journal] OR Science[journal] OR Cell[journal] OR eLife[journal] "
    "OR Genome Biol[journal] OR Nat Genet[journal] OR Nat Methods[journal] "
    "OR PLoS Comput Biol[journal] OR Genome Res[journal] OR Mol Cell[journal] "
    "OR Nat Commun[journal] OR Nucleic Acids Res[journal] "
    "OR Proc Natl Acad Sci[journal] OR Nat Neurosci[journal] "
    "OR Nat Med[journal] OR Nat Cell Biol[journal] OR Sci Adv[journal] "
    "OR Cells[journal]) "
    "AND (open access[filter])"
)


@dataclass(frozen=True)
class SearchConfig:
    europepmc_queries: tuple[EuropmcQuery, ...] = field(
        default_factory=lambda: tuple(_DEFAULT_EUROPEPMC_QUERIES)
    )
    pubmed_query_term: str = _DEFAULT_PUBMED_QUERY_TERM


_DEFAULT_SYSTEM_PROMPT = """\
You are an expert computational biology reviewer evaluating papers for the \
Kosmos AI benchmarking pipeline. Your task is to assess whether a paper is \
suitable for generating benchmark probes.

A good benchmark paper has:
1. A sequential chain of 3+ computational analysis steps where each finding \
motivates the next, building toward an impactful conclusion.
2. The entire hypothesis chain is reproducible from publicly deposited datasets.
3. The deposited data is openly accessible and <15 GB total.

## Evaluation criteria

### Sub-criterion 1: Sequential Computational Hypothesis Chain (weight 40%)
- Must have 3+ computational steps that build on each other.
- Entirely computational chains score highest.
- Mostly computational (1-2 wet-lab steps as context) is acceptable.
- Mixed wet-lab/computational: low marks — likely exclude.

EXCLUDE these paper types:
- Descriptive atlases/catalogs without mechanistic conclusions
- Method/tool/pipeline papers
- Parallel independent analyses that don't build on each other
- Papers ending with observations rather than an integrated model
- Primarily comparative studies without unifying conclusions
- Resource papers, review articles, perspectives

### Sub-criterion 2: Dataset Reproducibility (weight 35%)
- What fraction of the hypothesis chain can be reproduced from deposited data?
- How many main figures are derived from deposited data?
- All datasets must be fully public (NO controlled access, NO data use agreements).
- Total data <15 GB, ≤5 distinct datasets.
- 100% reproducible = full marks, 80-99% = good, <80% = likely exclude.

IMPORTANT — classify each dataset's role:
- "primary": Data generated by THIS paper's authors as part of this study.
- "reanalyzed_key": Data from another study, but ESSENTIAL to the paper's \
main results — e.g., a public dataset that is deeply integrated into the \
hypothesis chain, contributes to key figures, or is required to reproduce \
the core findings. These will be downloaded.
- "reanalyzed": Data from another study used only for minor comparison, \
validation, or context. These are listed but NOT downloaded.
The size limit and dataset count apply to primary + reanalyzed_key datasets \
combined (since both are downloaded).

### Sub-criterion 3: Data Modality (weight 10%)
Tier 1 (preferred): scRNA-seq, snRNA-seq, spatial transcriptomics, scATAC-seq, \
multiome, CRISPR screens.
Tier 2 (acceptable): bulk RNA-seq, proteomics, ChIP-seq, Hi-C, WGS/WES, metagenomics.
Multi-modal datasets get a bonus.

### Sub-criterion 4: Open Access License (weight 5%)
CC-BY preferred; CC-BY-NC acceptable.

### Sub-criterion 5: Figures from Dataset (weight 10%)
Higher fraction of main figures derived from deposited data = higher score.
Papers where <50% of figures come from deposited data are poor candidates.

## Scoring
5 = Ideal benchmark paper (fully computational chain, 100% reproducible, tier 1 modality)
4 = Strong candidate (3+ steps, ≥80% reproducible, minor limitations)
3 = Acceptable (meets hard requirements, may have mixed steps or tier 2 journal)
2 = Marginal — DO NOT INCLUDE
1 = Below threshold — DO NOT INCLUDE

Only papers scoring ≥3 should be included.
"""


@dataclass(frozen=True)
class EvaluationConfig:
    max_tokens: int = 16384
    system_prompt: str = _DEFAULT_SYSTEM_PROMPT


@dataclass(frozen=True)
class ParallelismConfig:
    search_workers: int = 8
    eval_workers: int = 10
    size_check_workers: int = 15
    download_workers: int = 4


@dataclass(frozen=True)
class RateLimits:
    europepmc_delay: float = 0.1
    pubmed_delay: float = 0.35
    geo_delay: float = 0.2
    zenodo_delay: float = 0.2
    claude_api_delay: float = 0.5
    openneuro_delay: float = 0.2
    omix_delay: float = 0.3
    figshare_delay: float = 0.2
    dryad_delay: float = 0.0


@dataclass(frozen=True)
class ApiEndpoints:
    europepmc_base: str = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    europepmc_fulltext: str = "https://www.ebi.ac.uk/europepmc/webservices/rest"
    pubmed_eutils: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    geo_query: str = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
    geo_ftp_base: str = "https://ftp.ncbi.nlm.nih.gov/geo/series"
    zenodo_api: str = "https://zenodo.org/api/records"
    openneuro_api: str = "https://openneuro.org/crn/datasets"
    figshare_api: str = "https://api.figshare.com/v2/articles"
    dryad_api: str = "https://datadryad.org/api/v2/datasets"
    omix_base: str = "https://ngdc.cncb.ac.cn/omix/release"


@dataclass(frozen=True)
class Timeouts:
    api_request: int = 30
    api_listing: int = 15
    download_stream: int = 300
    download_large: int = 600
    head_request: int = 10
    pdf_download: int = 60
    upload_command: int = 600


@dataclass(frozen=True)
class ScriptFiltering:
    data_extensions: frozenset[str] = field(default_factory=lambda: frozenset({
        ".h5", ".h5ad", ".hdf5", ".loom",
        ".csv", ".tsv", ".txt", ".tab",
        ".gz", ".tar", ".bz2", ".xz", ".zip", ".rar", ".7z",
        ".bam", ".sam", ".cram",
        ".fastq", ".fq", ".fasta", ".fa",
        ".bed", ".bedgraph", ".bigwig", ".bw", ".wig",
        ".vcf", ".bcf", ".gff", ".gtf", ".gff3",
        ".mtx",
        ".rds", ".rda", ".rdata",
        ".parquet", ".feather", ".arrow",
        ".json", ".xml", ".yaml", ".yml",
        ".xlsx", ".xls",
        ".tiff", ".tif", ".png", ".jpg", ".jpeg", ".svg", ".pdf",
        ".nii", ".mgz", ".mgh",
        ".edf", ".bdf", ".set", ".fdt",
        ".mzml", ".mzxml", ".raw", ".mgf",
        ".cel", ".idat",
        ".fcs",
        ".npz", ".npy", ".pkl", ".pickle", ".joblib",
        ".sqlite", ".db",
    }))
    script_extensions: frozenset[str] = field(default_factory=lambda: frozenset({
        ".py", ".r", ".rmd", ".qmd",
        ".ipynb",
        ".sh", ".bash", ".zsh", ".csh",
        ".m",
        ".jl",
        ".pl", ".pm",
        ".wdl", ".nf", ".smk",
        ".cwl",
        ".groovy",
    }))
    script_filenames: frozenset[str] = field(default_factory=lambda: frozenset({
        "makefile", "snakefile", "rakefile",
        "dockerfile", "jenkinsfile",
        "nextflow.config", "snakemake",
        "requirements.txt", "environment.yml", "environment.yaml",
        "setup.py", "setup.cfg", "pyproject.toml",
        "conda_env.yml", "conda_env.yaml",
        "renv.lock", "packrat.lock",
        ".gitignore", ".dockerignore",
    }))
    script_directories: frozenset[str] = field(default_factory=lambda: frozenset({
        "code", "codes",
        "scripts", "script",
        "analysis", "analyses",
        "notebooks", "notebook",
        "src", "source",
        "pipeline", "pipelines",
        "workflow", "workflows",
        ".snakemake",
    }))


@dataclass(frozen=True)
class FilteringPatterns:
    exclude_type_patterns: tuple[str, ...] = (
        r"\breview\b",
        r"\bperspective\b",
        r"\beditorial\b",
        r"\bcommentary\b",
        r"\bcorrespondence\b",
        r"\bletter to the editor\b",
        r"\berratum\b",
        r"\bcorrigendum\b",
        r"\bretraction\b",
    )
    exclude_tool_patterns: tuple[str, ...] = (
        r"\ba (?:tool|package|pipeline|framework|platform|software|database|resource) for\b",
        r"\bwe (?:present|introduce|develop|describe) (?:a |an )?(?:new |novel )?(?:tool|method|package|pipeline|software)\b",
    )
    excluded_pub_types: frozenset[str] = field(default_factory=lambda: frozenset({
        "review", "review-article", "editorial", "comment", "commentary",
        "letter", "correspondence", "erratum", "corrigendum",
        "retraction", "retracted publication",
    }))


@dataclass(frozen=True)
class UploadConfig:
    num_steps: int = 20
    run_tardigrade: bool = False
    enable_literature_research: bool = False


@dataclass(frozen=True)
class PipelineConfig:
    pipeline: PipelineSettings = field(default_factory=PipelineSettings)
    thresholds: Thresholds = field(default_factory=Thresholds)
    journals: JournalConfig = field(default_factory=JournalConfig)
    modalities: ModalityConfig = field(default_factory=ModalityConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    evaluation: EvaluationConfig = field(default_factory=EvaluationConfig)
    parallelism: ParallelismConfig = field(default_factory=ParallelismConfig)
    rate_limits: RateLimits = field(default_factory=RateLimits)
    api_endpoints: ApiEndpoints = field(default_factory=ApiEndpoints)
    timeouts: Timeouts = field(default_factory=Timeouts)
    script_filtering: ScriptFiltering = field(default_factory=ScriptFiltering)
    filtering: FilteringPatterns = field(default_factory=FilteringPatterns)
    upload: UploadConfig = field(default_factory=UploadConfig)


# ---------------------------------------------------------------------------
# Default factory
# ---------------------------------------------------------------------------

def _default_config() -> PipelineConfig:
    return PipelineConfig()


# ---------------------------------------------------------------------------
# Deep merge: user YAML overrides onto defaults
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into a copy of *base*."""
    result = copy.deepcopy(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


# ---------------------------------------------------------------------------
# Dict → dataclass conversion
# ---------------------------------------------------------------------------

def _config_to_dict(cfg: PipelineConfig) -> dict:
    """Serialize a PipelineConfig to a plain dict (frozensets → sorted lists,
    tuples → lists, sub-dataclasses → dicts)."""
    def _convert(obj: Any) -> Any:
        if isinstance(obj, (frozenset, set)):
            return sorted(obj)
        if isinstance(obj, tuple):
            # Tuple of dataclasses (e.g. EuropmcQuery)
            if obj and hasattr(obj[0], "__dataclass_fields__"):
                return [_convert(asdict(item)) for item in obj]
            return list(obj)
        if isinstance(obj, dict):
            return {k: _convert(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_convert(v) for v in obj]
        return obj

    raw = asdict(cfg)
    return _convert(raw)


def _dict_to_config(d: dict) -> PipelineConfig:
    """Build a PipelineConfig from a plain dict (lists → frozensets/tuples
    where the dataclass type annotation expects them)."""

    def _build(cls, data):
        if not isinstance(data, dict):
            return data
        kwargs = {}
        for f in fields(cls):
            if f.name not in data:
                continue
            val = data[f.name]
            # Resolve the type annotation
            ftype = f.type
            if isinstance(ftype, str):
                # Evaluate string annotations in the module namespace
                ftype = eval(ftype, globals())

            origin = getattr(ftype, "__origin__", None)
            args = getattr(ftype, "__args__", ())

            if hasattr(ftype, "__dataclass_fields__"):
                # Nested dataclass
                kwargs[f.name] = _build(ftype, val)
            elif origin is frozenset and isinstance(val, list):
                kwargs[f.name] = frozenset(val)
            elif origin is tuple and isinstance(val, list):
                # Check if this is a tuple of dataclasses
                if args and hasattr(args[0], "__dataclass_fields__"):
                    kwargs[f.name] = tuple(
                        _build(args[0], item) for item in val
                    )
                else:
                    kwargs[f.name] = tuple(val)
            else:
                kwargs[f.name] = val
        return cls(**kwargs)

    return _build(PipelineConfig, d)


# ---------------------------------------------------------------------------
# YAML generation (default config → YAML string)
# ---------------------------------------------------------------------------

def _literal_str_representer(dumper, data):
    """Use block scalar style for multi-line strings."""
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def generate_default_yaml() -> str:
    """Return a YAML string of the default configuration with section comments."""
    dumper = yaml.Dumper
    dumper.add_representer(str, _literal_str_representer)

    d = _config_to_dict(_default_config())
    raw = yaml.dump(d, Dumper=dumper, default_flow_style=False, sort_keys=False,
                    width=120, allow_unicode=True)

    # Inject section comments
    comments = {
        "pipeline:": "# Pipeline settings",
        "thresholds:": "# Scoring thresholds",
        "journals:": "# Journal tier configuration",
        "modalities:": "# Data modality tiers",
        "search:": "# Search queries",
        "evaluation:": "# Claude evaluation settings",
        "parallelism:": "# Parallelism (thread pool sizes)",
        "rate_limits:": "# Rate limiting (seconds between requests)",
        "api_endpoints:": "# API endpoints",
        "timeouts:": "# HTTP timeouts (seconds)",
        "script_filtering:": "# Script filtering (prevent answer leakage)",
        "filtering:": "# Article-type filtering patterns",
        "upload:": "# Edison upload settings",
    }

    lines = raw.splitlines()
    result = []
    for line in lines:
        stripped = line.strip()
        if stripped in comments:
            if result:
                result.append("")
            result.append(comments[stripped])
        result.append(line)

    return "\n".join(result) + "\n"


# ---------------------------------------------------------------------------
# Singleton config
# ---------------------------------------------------------------------------

_config: PipelineConfig | None = None


def init_config(path: Path | str | None = None) -> PipelineConfig:
    """Load config from YAML (or use defaults) and set the module singleton."""
    global _config
    if path is not None:
        path = Path(path)
        with open(path) as f:
            user_data = yaml.safe_load(f) or {}
        default_data = _config_to_dict(_default_config())
        merged = _deep_merge(default_data, user_data)
        _config = _dict_to_config(merged)
    else:
        _config = _default_config()
    return _config


def get_config() -> PipelineConfig:
    """Return the loaded config singleton.  Raises if init_config() hasn't been called."""
    if _config is None:
        raise RuntimeError(
            "Config not initialized. Call init_config() first "
            "(or pass --config to the CLI)."
        )
    return _config


# ---------------------------------------------------------------------------
# Utility functions (read from singleton)
# ---------------------------------------------------------------------------

def date_window() -> tuple[str, str]:
    """Return (start_date, end_date) as ISO strings for the rolling window."""
    cfg = get_config()
    end = date.today()
    start = end - timedelta(days=cfg.pipeline.date_window_months * 30)
    return start.isoformat(), end.isoformat()


def get_journal_tier(journal_name: str) -> int:
    """Return 1 (top), 2 (solid), or 0 (excluded)."""
    cfg = get_config()
    j = journal_name.lower().strip()
    if j in cfg.journals.tier_1:
        return 1
    if j in cfg.journals.tier_2:
        return 2
    return 0
