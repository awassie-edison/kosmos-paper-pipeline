"""Step 4: Score, rank, and apply topic-diversity adjustment."""

import logging
from collections import Counter

from . import config

log = logging.getLogger(__name__)


def compute_score(paper: dict) -> float:
    """Recompute the quality_score applying journal-tier penalty.

    The base quality_score comes from Claude's evaluation. We only apply
    the tier-2 penalty here (the rest of the scoring is done by Claude).
    """
    base = paper.get("quality_score", 0)
    tier = paper.get("journal_tier", 1)
    if tier == 2:
        base -= config.TIER_2_PENALTY
    return round(max(base, 1.0), 1)


def apply_diversity_adjustment(
    papers: list[dict],
    max_papers: int = config.MAX_PAPERS_OUTPUT,
    min_domains: int = config.MIN_DOMAINS,
) -> list[dict]:
    """Select top papers while ensuring topic diversity.

    Strategy:
    1. Sort by score descending.
    2. Greedily pick the highest-scoring paper from each domain first.
    3. Fill remaining slots from the overall ranking.
    """
    if len(papers) <= max_papers:
        return sorted(papers, key=lambda p: -p.get("quality_score", 0))

    # Sort by score
    ranked = sorted(papers, key=lambda p: -p.get("quality_score", 0))

    # Phase 1: one paper per domain (highest-scoring representative)
    selected: list[dict] = []
    selected_dois: set[str] = set()
    domains_covered: set[str] = set()

    for p in ranked:
        domain = p.get("domain", "unknown").lower()
        if domain not in domains_covered:
            selected.append(p)
            selected_dois.add(p.get("doi", ""))
            domains_covered.add(domain)
        if len(selected) >= max_papers:
            break

    # Phase 2: fill remaining slots from overall ranking
    for p in ranked:
        if len(selected) >= max_papers:
            break
        if p.get("doi", "") not in selected_dois:
            selected.append(p)
            selected_dois.add(p.get("doi", ""))

    n_domains = len({p.get("domain", "unknown").lower() for p in selected})
    log.info(
        "Diversity adjustment: %d papers, %d domains (target ≥%d)",
        len(selected),
        n_domains,
        min_domains,
    )

    if n_domains < min_domains:
        log.warning(
            "Only %d domains — fewer than target %d. "
            "Consider broadening search queries.",
            n_domains,
            min_domains,
        )

    return sorted(selected, key=lambda p: -p.get("quality_score", 0))


def score_and_rank(
    papers: list[dict],
    max_papers: int = config.MAX_PAPERS_OUTPUT,
) -> list[dict]:
    """Apply scoring adjustments, filter by min score, rank with diversity."""
    # Recompute scores with tier penalty
    for p in papers:
        p["quality_score"] = compute_score(p)

    # Filter by minimum score
    passing = [p for p in papers if p["quality_score"] >= config.MIN_QUALITY_SCORE]

    removed = len(papers) - len(passing)
    if removed:
        log.info("Removed %d papers below minimum score %.1f", removed, config.MIN_QUALITY_SCORE)

    # Apply diversity adjustment and select final set
    return apply_diversity_adjustment(passing, max_papers=max_papers)
