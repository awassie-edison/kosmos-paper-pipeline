"""Step 1: Broad discovery via Europe PMC + PubMed APIs."""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests

from . import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

def _epmc_query_url(query_text: str, start: str, end: str) -> str:
    """Build a Europe PMC REST URL."""
    date_range = f"[{start} TO {end}]"
    q = f"{query_text} AND OPEN_ACCESS:y AND (FIRST_PDATE:{date_range})"
    return (
        f"{config.EUROPEPMC_BASE}"
        f"?query={quote(q)}"
        f"&resultType=core&pageSize=100&format=json"
    )


def build_europepmc_queries(start: str, end: str) -> list[dict]:
    """Return list of {name, url} dicts for all Europe PMC search queries."""
    queries = [
        {
            "name": "query1_nature_science_cell",
            "text": (
                '(JOURNAL:"Nature" OR JOURNAL:"Science" OR JOURNAL:"Cell" '
                'OR JOURNAL:"Nature Genetics" OR JOURNAL:"Nature Methods" '
                'OR JOURNAL:"Nature Biotechnology" OR JOURNAL:"Nature Communications" '
                'OR JOURNAL:"eLife" OR JOURNAL:"Genome Biology" OR JOURNAL:"Genome Research") '
                "AND (computational OR bioinformatic OR integrative analysis) "
                "AND (dataset OR GEO OR accession OR deposited)"
            ),
        },
        {
            "name": "query2_cell_sub_plos_pnas",
            "text": (
                '(JOURNAL:"Molecular Cell" OR JOURNAL:"Cell Systems" '
                'OR JOURNAL:"Cell Reports" OR JOURNAL:"Cell Genomics" '
                'OR JOURNAL:"PLOS Computational Biology" OR JOURNAL:"PLOS Genetics" '
                'OR JOURNAL:"Nucleic Acids Research" '
                'OR JOURNAL:"Proceedings of the National Academy of Sciences") '
                "AND (computational OR bioinformatic OR systematic analysis) "
                "AND (dataset OR publicly available OR repository)"
            ),
        },
        {
            "name": "query3_nature_sub",
            "text": (
                '(JOURNAL:"Nature Neuroscience" OR JOURNAL:"Nature Medicine" '
                'OR JOURNAL:"Nature Cell Biology" OR JOURNAL:"Nature Biotechnology" '
                'OR JOURNAL:"Nature Methods") '
                "AND (analysis OR computational OR modeling) "
                "AND (data OR dataset OR sequencing)"
            ),
        },
        {
            "name": "query4_singlecell_spatial",
            "text": (
                "(single-cell OR spatial transcriptomics OR multi-omics OR single-nucleus) "
                "AND (mechanism OR model OR pathogenesis OR reveals)"
            ),
        },
        {
            "name": "query5_disease",
            "text": (
                "(cancer genomics OR neurodegeneration OR immune OR developmental biology) "
                "AND (computational framework OR integrative analysis OR systematic characterization) "
                "AND (GEO OR accession OR Zenodo OR deposited)"
            ),
        },
        {
            "name": "query6_evo_struct_systems",
            "text": (
                "(evolutionary OR phylogenomic OR structural biology OR systems biology "
                "OR gene regulatory network) "
                "AND (computational OR modeling OR simulation) "
                "AND (dataset OR publicly available)"
            ),
        },
        {
            "name": "query7_tier2_strong_datasets",
            "text": (
                "(single-cell RNA-seq OR snRNA-seq OR spatial transcriptomics "
                "OR ATAC-seq OR multi-omics) "
                "AND (GEO OR accession OR deposited OR Zenodo)"
            ),
        },
    ]
    return [
        {"name": q["name"], "url": _epmc_query_url(q["text"], start, end)}
        for q in queries
    ]


def build_pubmed_query(start: str, end: str) -> dict:
    """Build the PubMed E-utilities query."""
    # Convert YYYY-MM-DD to YYYY/MM/DD for PubMed
    s = start.replace("-", "/")
    e = end.replace("-", "/")
    term = (
        "(computational biology[MeSH] OR genomics[MeSH] OR systems biology[MeSH]) "
        "AND (Nature[journal] OR Science[journal] OR Cell[journal] OR eLife[journal] "
        "OR Genome Biol[journal] OR Nat Genet[journal] OR Nat Methods[journal] "
        "OR PLoS Comput Biol[journal] OR Genome Res[journal] OR Mol Cell[journal] "
        "OR Nat Commun[journal] OR Nucleic Acids Res[journal] "
        "OR Proc Natl Acad Sci[journal] OR Nat Neurosci[journal] "
        "OR Nat Med[journal] OR Nat Cell Biol[journal] OR Sci Adv[journal] "
        "OR Cells[journal]) "
        f"AND (open access[filter]) AND ({s}:{e}[pdat])"
    )
    url = (
        f"{config.PUBMED_EUTILS}/esearch.fcgi"
        f"?db=pubmed&term={quote(term)}&retmax=200&retmode=json"
    )
    return {"name": "query8_pubmed", "url": url}


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def _fetch_one(name: str, url: str) -> dict:
    """Fetch a single query URL and return parsed JSON with metadata."""
    log.info("Searching: %s", name)
    resp = requests.get(url, timeout=30)
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
    queries = build_europepmc_queries(start_date, end_date)
    queries.append(build_pubmed_query(start_date, end_date))

    raw_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=config.SEARCH_WORKERS) as pool:
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
