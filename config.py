"""Pipeline configuration: journal tiers, API endpoints, scoring weights, defaults."""

from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
DEFAULT_MODEL = "claude-opus-4-6"

# ---------------------------------------------------------------------------
# Date window
# ---------------------------------------------------------------------------
DATE_WINDOW_MONTHS = 6


def date_window() -> tuple[str, str]:
    """Return (start_date, end_date) as ISO strings for the rolling window."""
    end = date.today()
    start = end - timedelta(days=DATE_WINDOW_MONTHS * 30)
    return start.isoformat(), end.isoformat()


# ---------------------------------------------------------------------------
# Journal tiers
# ---------------------------------------------------------------------------
TIER_1_JOURNALS: set[str] = {
    # Nature family
    "nature",
    "nature genetics",
    "nature methods",
    "nature biotechnology",
    "nature neuroscience",
    "nature medicine",
    "nature cell biology",
    "nature communications",
    "nature ecology & evolution",
    "nature structural & molecular biology",
    "nature chemical biology",
    # Cell family
    "cell",
    "molecular cell",
    "cell systems",
    "cell reports",
    "cell genomics",
    "cell stem cell",
    "cell metabolism",
    # Science family
    "science",
    "science advances",
    # Top computational / genomics
    "elife",
    "genome biology",
    "genome research",
    "plos computational biology",
    "plos genetics",
    "nucleic acids research",
    # Broad high-impact
    "proceedings of the national academy of sciences",
    "the embo journal",
    "embo reports",
}

TIER_2_JOURNALS: set[str] = {
    "cells",
    "frontiers in immunology",
    "frontiers in genetics",
    "frontiers in neuroscience",
    "frontiers in cell and developmental biology",
    "scientific reports",
    "bmc genomics",
    "bmc bioinformatics",
    "plos one",
    "iscience",
    "cell reports methods",
    "nar genomics and bioinformatics",
    "briefings in bioinformatics",
    "gigascience",
    "molecular systems biology",
    "communications biology",
    "bioinformatics (oxford, england)",
    "bioinformatics",
    "npj systems biology and applications",
    "advanced science (weinheim, baden-wurttemberg, germany)",
    "advanced science",
}


def get_journal_tier(journal_name: str) -> int:
    """Return 1 (top), 2 (solid), or 0 (excluded)."""
    j = journal_name.lower().strip()
    if j in TIER_1_JOURNALS:
        return 1
    if j in TIER_2_JOURNALS:
        return 2
    return 0


# ---------------------------------------------------------------------------
# Data modality tiers
# ---------------------------------------------------------------------------
TIER_1_MODALITIES = {
    "scrna-seq",
    "snrna-seq",
    "spatial transcriptomics",
    "scatac-seq",
    "multiome",
    "perturb-seq",
    "crop-seq",
}

TIER_2_MODALITIES = {
    "bulk rna-seq",
    "proteomics",
    "phosphoproteomics",
    "metagenomics",
    "metabolomics",
    "chip-seq",
    "cut&run",
    "cut&tag",
    "hi-c",
    "whole-genome sequencing",
    "exome sequencing",
}

# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
EUROPEPMC_BASE = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EUROPEPMC_FULLTEXT = "https://www.ebi.ac.uk/europepmc/webservices/rest"
PUBMED_EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
GEO_QUERY = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
GEO_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/geo/series"
ZENODO_API = "https://zenodo.org/api/records"

# ---------------------------------------------------------------------------
# Pipeline defaults
# ---------------------------------------------------------------------------
MAX_CANDIDATES_TO_EVALUATE = 150
MAX_PAPERS_OUTPUT = 20
MAX_DATA_SIZE_GB = 15.0
MAX_DATASETS_PER_PAPER = 5
MIN_QUALITY_SCORE = 3.0
MIN_DOMAINS = 4
TIER_2_PENALTY = 0.5

# Parallelism
SEARCH_WORKERS = 8
EVAL_WORKERS = 10
SIZE_CHECK_WORKERS = 15
DOWNLOAD_WORKERS = 4

# Rate limiting (seconds between requests)
EUROPEPMC_DELAY = 0.1
PUBMED_DELAY = 0.35  # <3 req/sec
GEO_DELAY = 0.2
ZENODO_DELAY = 0.2
CLAUDE_API_DELAY = 0.5
