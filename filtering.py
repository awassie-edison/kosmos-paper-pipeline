"""Step 2: Filter candidates by journal tier, article type, and open access."""

import logging
import re

from . import config
from .config import get_config

log = logging.getLogger(__name__)


def _is_excluded_type(paper: dict) -> bool:
    """Return True if paper looks like a review, editorial, or method/tool paper."""
    cfg = get_config()

    # Check structured pub_types from Europe PMC (most reliable)
    pub_types = {t.lower() for t in paper.get("pub_types", [])}
    if pub_types & cfg.filtering.excluded_pub_types:
        return True

    title = paper.get("title", "").lower()

    for pat in cfg.filtering.exclude_type_patterns:
        if re.search(pat, title, re.IGNORECASE):
            return True

    # Only flag tool papers if the pattern is in the title (too noisy in abstracts)
    for pat in cfg.filtering.exclude_tool_patterns:
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
    cfg = get_config()
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
    journal_priority = cfg.journals.priority

    def _sort_key(p):
        j = p.get("journal", "").lower().strip()
        return (p.get("journal_tier", 1), journal_priority.get(j, 3))

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
