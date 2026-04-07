"""Step 1: Broad discovery via Europe PMC + PubMed APIs."""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests

from .config import get_config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

def _epmc_query_url(query_text: str, start: str, end: str) -> str:
    """Build a Europe PMC REST URL."""
    cfg = get_config()
    date_range = f"[{start} TO {end}]"
    q = f"{query_text} AND OPEN_ACCESS:y AND (FIRST_PDATE:{date_range})"
    return (
        f"{cfg.api_endpoints.europepmc_base}"
        f"?query={quote(q)}"
        f"&resultType=core&pageSize=100&format=json"
    )


def build_europepmc_queries(start: str, end: str) -> list[dict]:
    """Return list of {name, url} dicts for all Europe PMC search queries."""
    cfg = get_config()
    return [
        {"name": q.name, "url": _epmc_query_url(q.text, start, end)}
        for q in cfg.search.europepmc_queries
    ]


def build_pubmed_query(start: str, end: str) -> dict:
    """Build the PubMed E-utilities query."""
    cfg = get_config()
    # Convert YYYY-MM-DD to YYYY/MM/DD for PubMed
    s = start.replace("-", "/")
    e = end.replace("-", "/")
    term = f"{cfg.search.pubmed_query_term} AND ({s}:{e}[pdat])"
    url = (
        f"{cfg.api_endpoints.pubmed_eutils}/esearch.fcgi"
        f"?db=pubmed&term={quote(term)}&retmax=200&retmode=json"
    )
    return {"name": "query8_pubmed", "url": url}


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def _fetch_one(name: str, url: str) -> dict:
    """Fetch a single query URL and return parsed JSON with metadata."""
    cfg = get_config()
    log.info("Searching: %s", name)
    resp = requests.get(url, timeout=cfg.timeouts.api_request)
    resp.raise_for_status()
    return {"name": name, "data": resp.json()}


def _parse_europepmc_results(raw: dict) -> list[dict]:
    """Extract paper records from a Europe PMC response."""
    result_list = raw.get("resultList", {}).get("result", [])
    papers = []
    for r in result_list:
        doi = r.get("doi")
        if not doi:
            continue
        # Journal title is nested: journalInfo.journal.title
        journal_info = r.get("journalInfo", {})
        journal_title = journal_info.get("journal", {}).get("title", "")
        papers.append({
            "doi": doi,
            "title": r.get("title", ""),
            "authors": r.get("authorString", ""),
            "first_author": r.get("authorString", "").split(",")[0].strip().split()[-1] if r.get("authorString") else "",
            "journal": journal_title,
            "pub_date": r.get("firstPublicationDate", ""),
            "is_open_access": r.get("isOpenAccess", "N"),
            "license": r.get("license", ""),
            "pmid": r.get("pmid", ""),
            "pmcid": r.get("pmcid", ""),
            "abstract": r.get("abstractText", ""),
            "pub_types": r.get("pubTypeList", {}).get("pubType", []),
            "source": "europepmc",
        })
    return papers


def _parse_pubmed_results(raw: dict) -> list[dict]:
    """Extract PMIDs from PubMed E-utilities search.

    PubMed search only returns IDs.  We record them so that the filtering
    step can cross-reference, but the primary metadata comes from Europe PMC.
    """
    id_list = raw.get("esearchresult", {}).get("idlist", [])
    return [{"pmid": pid, "source": "pubmed"} for pid in id_list]


def run_search(start_date: str, end_date: str) -> list[dict]:
    """Run all search queries in parallel and return deduplicated papers.

    Returns a list of paper dicts with at minimum {doi, title, journal, ...}.
    """
    cfg = get_config()
    queries = build_europepmc_queries(start_date, end_date)
    queries.append(build_pubmed_query(start_date, end_date))

    raw_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=cfg.parallelism.search_workers) as pool:
        futures = {
            pool.submit(_fetch_one, q["name"], q["url"]): q["name"]
            for q in queries
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                raw_results.append(fut.result())
            except Exception:
                log.exception("Search query failed: %s", name)

    # Parse results
    all_papers: list[dict] = []
    pubmed_pmids: set[str] = set()

    for raw in raw_results:
        if raw["name"].startswith("query8"):
            pubmed_pmids.update(
                p["pmid"] for p in _parse_pubmed_results(raw["data"])
            )
        else:
            all_papers.extend(_parse_europepmc_results(raw["data"]))

    # Deduplicate by DOI
    seen_dois: set[str] = set()
    unique_papers: list[dict] = []
    for p in all_papers:
        doi = p["doi"].lower().strip()
        if doi not in seen_dois:
            seen_dois.add(doi)
            unique_papers.append(p)

    total_raw = sum(
        len(r["data"].get("resultList", {}).get("result", []))
        for r in raw_results
        if not r["name"].startswith("query8")
    )

    log.info(
        "Search complete: %d raw results, %d unique DOIs, %d PubMed IDs",
        total_raw,
        len(unique_papers),
        len(pubmed_pmids),
    )

    return unique_papers


def save_raw_results(raw_results: list[dict], intermediate_dir):
    """Save individual query results to intermediate/ for debugging."""
    from pathlib import Path
    d = Path(intermediate_dir)
    d.mkdir(parents=True, exist_ok=True)
    for r in raw_results:
        with open(d / f"{r['name']}_raw.json", "w") as f:
            json.dump(r["data"], f, indent=2)
