"""Step 2: Filter candidates by journal tier, article type, and open access."""

import logging
import re

from . import config

log = logging.getLogger(__name__)

# Patterns that indicate non-research articles
_EXCLUDE_TYPE_PATTERNS = [
    r"\breview\b",
    r"\bperspective\b",
    r"\beditorial\b",
    r"\bcommentary\b",
    r"\bcorrespondence\b",
    r"\bletter to the editor\b",
    r"\berratum\b",
    r"\bcorrigendum\b",
    r"\bretraction\b",
]

# Patterns that indicate method/tool papers (not hypothesis-driven)
_EXCLUDE_TOOL_PATTERNS = [
    r"\ba (?:tool|package|pipeline|framework|platform|software|database|resource) for\b",
    r"\bwe (?:present|introduce|develop|describe) (?:a |an )?(?:new |novel )?(?:tool|method|package|pipeline|software)\b",
]


_EXCLUDED_PUB_TYPES = {
    "review", "review-article", "editorial", "comment", "commentary",
    "letter", "correspondence", "erratum", "corrigendum",
    "retraction", "retracted publication",
}


def _is_excluded_type(paper: dict) -> bool:
    """Return True if paper looks like a review, editorial, or method/tool paper."""
    # Check structured pub_types from Europe PMC (most reliable)
    pub_types = {t.lower() for t in paper.get("pub_types", [])}
    if pub_types & _EXCLUDED_PUB_TYPES:
        return True

    title = paper.get("title", "").lower()

    for pat in _EXCLUDE_TYPE_PATTERNS:
        if re.search(pat, title, re.IGNORECASE):
            return True

    # Only flag tool papers if the pattern is in the title (too noisy in abstracts)
    for pat in _EXCLUDE_TOOL_PATTERNS:
        if re.search(pat, title, re.IGNORECASE):
            return True

    return False


def _has_open_access(paper: dict) -> bool:
    """Check whether paper has an explicit open-access indicator."""
    return paper.get("is_open_access", "").upper() == "Y"


def filter_papers(
    papers: list[dict],
    exclude_dois: set[str] | None = None,
) -> dict:
    """Filter papers by journal tier, article type, and open access.

    Returns dict with keys: tier1, tier2, excluded_tier0, excluded_type,
    excluded_oa, excluded_doi, candidates.
    """
    exclude_dois = {d.lower().strip() for d in (exclude_dois or set())}

    tier1: list[dict] = []
    tier2: list[dict] = []
    excluded_tier0 = 0
    excluded_type = 0
    excluded_oa = 0
    excluded_doi = 0

    for p in papers:
        doi = p.get("doi", "").lower().strip()

        # Skip DOIs the user wants excluded (e.g., already benchmarked)
        if doi in exclude_dois:
            excluded_doi += 1
            continue

        # Journal tier
        tier = config.get_journal_tier(p.get("journal", ""))
        if tier == 0:
            excluded_tier0 += 1
            continue

        # Article type
        if _is_excluded_type(p):
            excluded_type += 1
            continue

        # Open access
        if not _has_open_access(p):
            excluded_oa += 1
            continue

        p["journal_tier"] = tier
        if tier == 1:
            tier1.append(p)
        else:
            tier2.append(p)

    # Sort candidates to evaluate highest-value journals first.
    # Within tier 1, prioritize journals that publish hypothesis-driven biology
    # over those that skew toward method/tool papers.
    JOURNAL_PRIORITY = {
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
        "nucleic acids research": 4,
        "plos genetics": 4,
        "plos computational biology": 5,  # many method papers — evaluate last in tier 1
    }

    def _sort_key(p):
        j = p.get("journal", "").lower().strip()
        return (p.get("journal_tier", 1), JOURNAL_PRIORITY.get(j, 3))

    tier1.sort(key=_sort_key)
    candidates = tier1 + tier2

    log.info(
        "Filtering: %d tier1, %d tier2, excluded: %d tier0, %d type, %d oa, %d doi → %d candidates",
        len(tier1),
        len(tier2),
        excluded_tier0,
        excluded_type,
        excluded_oa,
        excluded_doi,
        len(candidates),
    )

    return {
        "tier1": len(tier1),
        "tier2": len(tier2),
        "excluded_tier0": excluded_tier0,
        "excluded_type": excluded_type,
        "excluded_oa": excluded_oa,
        "excluded_doi": excluded_doi,
        "candidates": candidates,
    }
