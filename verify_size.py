"""Step 3.5: Verify actual dataset sizes via GEO FTP and Zenodo API.

Replaces the prompt-estimated sizes with real numbers, and removes papers
whose total dataset size exceeds the configured maximum.
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from .config import get_config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GEO size checking
# ---------------------------------------------------------------------------

def _gse_ftp_url(accession: str) -> str:
    """Build the FTP-over-HTTP URL for a GEO series supplementary directory.

    GSE196018 → https://ftp.ncbi.nlm.nih.gov/geo/series/GSE196nnn/GSE196018/suppl/
    """
    cfg = get_config()
    prefix = accession[:-3] + "nnn"
    return f"{cfg.api_endpoints.geo_ftp_base}/{prefix}/{accession}/suppl/"


def get_geo_size(accession: str) -> float:
    """Return total size in GB of supplementary files for a GEO accession.

    Parses the FTP directory listing (served as HTML) for file sizes.
    Falls back to 0.0 if the listing is unavailable.
    """
    cfg = get_config()
    if not accession.upper().startswith("GSE"):
        return 0.0

    url = _gse_ftp_url(accession.upper())
    try:
        resp = requests.get(url, timeout=cfg.timeouts.api_listing)
        if resp.status_code != 200:
            log.warning("GEO FTP listing unavailable for %s (HTTP %d)", accession, resp.status_code)
            return 0.0
    except requests.RequestException as e:
        log.warning("GEO FTP request failed for %s: %s", accession, e)
        return 0.0

    # The FTP listing is a simple HTML page with <a> links and file sizes
    # Parse lines like: "GSE196018_RAW.tar     2024-01-15 10:30  1.2G"
    # or from the HTML pre-formatted listing
    total_bytes = 0.0
    text = resp.text

    # NCBI FTP listing format: size is typically in a <pre> block
    # Pattern: file_name  date  time  size (with K/M/G suffix)
    size_patterns = re.findall(
        r'(\d+(?:\.\d+)?)\s*([KMGTP])\b',
        text,
        re.IGNORECASE,
    )

    multipliers = {"K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15}
    for val, unit in size_patterns:
        total_bytes += float(val) * multipliers.get(unit.upper(), 1)

    if total_bytes == 0:
        # Try alternative: look for raw byte counts in the listing
        # Some listings show bytes directly: "  1234567890  "
        byte_matches = re.findall(r'\b(\d{6,})\b', text)
        for b in byte_matches:
            total_bytes += int(b)

    gb = total_bytes / 1e9
    log.info("GEO %s: %.2f GB (from FTP listing)", accession, gb)
    return gb


def _try_head_request(url: str) -> float:
    """Issue HEAD request and return Content-Length in GB, or 0."""
    cfg = get_config()
    try:
        resp = requests.head(url, timeout=cfg.timeouts.head_request, allow_redirects=True)
        cl = resp.headers.get("Content-Length")
        if cl:
            return int(cl) / 1e9
    except Exception:
        pass
    return 0.0


# ---------------------------------------------------------------------------
# Zenodo size checking
# ---------------------------------------------------------------------------

def get_zenodo_size(accession: str) -> float:
    """Return total size in GB for a Zenodo record.

    Accepts accession formats:
      - "10.5281/zenodo.14031498"  (DOI)
      - "14031498"                  (record ID)
      - "zenodo.org/record/14031498"
    """
    cfg = get_config()
    # Extract numeric record ID
    record_id = re.search(r'(\d{5,})', accession)
    if not record_id:
        log.warning("Cannot parse Zenodo record ID from: %s", accession)
        return 0.0
    record_id = record_id.group(1)

    url = f"{cfg.api_endpoints.zenodo_api}/{record_id}"
    try:
        resp = requests.get(url, timeout=cfg.timeouts.api_listing)
        if resp.status_code != 200:
            log.warning("Zenodo API returned %d for record %s", resp.status_code, record_id)
            return 0.0
        data = resp.json()
    except Exception as e:
        log.warning("Zenodo API request failed for %s: %s", record_id, e)
        return 0.0

    total_bytes = sum(f.get("size", 0) for f in data.get("files", []))
    gb = total_bytes / 1e9

    files = data.get("files", [])
    log.info(
        "Zenodo %s: %.2f GB (%d files)",
        record_id,
        gb,
        len(files),
    )
    return gb


# ---------------------------------------------------------------------------
# SRA size checking (for PRJNA / PRJEB accessions)
# ---------------------------------------------------------------------------

def get_sra_size(accession: str) -> float:
    """Estimate total size for an SRA project accession.

    Uses the NCBI Run Selector API to get run sizes.
    This is less reliable than GEO/Zenodo — returns 0 on failure.
    """
    cfg = get_config()
    url = (
        f"https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi"
        f"?save=efetch&db=sra&rettype=runinfo&term={accession}"
    )
    try:
        resp = requests.get(url, timeout=cfg.timeouts.api_listing)
        if resp.status_code != 200:
            return 0.0
        # RunInfo CSV has a 'size_MB' column
        total_mb = 0.0
        lines = resp.text.strip().split("\n")
        if len(lines) < 2:
            return 0.0
        header = lines[0].split(",")
        try:
            size_idx = header.index("size_MB")
        except ValueError:
            # Try 'bytes' column
            try:
                size_idx = header.index("bytes")
                for line in lines[1:]:
                    fields = line.split(",")
                    if len(fields) > size_idx:
                        total_mb += float(fields[size_idx]) / 1e6
                return total_mb / 1e3
            except (ValueError, IndexError):
                return 0.0
        for line in lines[1:]:
            fields = line.split(",")
            if len(fields) > size_idx:
                total_mb += float(fields[size_idx])
        gb = total_mb / 1e3
        log.info("SRA %s: %.2f GB", accession, gb)
        return gb
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def get_openneuro_size(accession: str) -> float:
    """Return total size in GB for an OpenNeuro dataset."""
    cfg = get_config()
    try:
        url = f"{cfg.api_endpoints.openneuro_api}/{accession}"
        resp = requests.get(url, timeout=cfg.timeouts.api_listing)
        if resp.status_code != 200:
            log.warning("OpenNeuro API returned %d for %s", resp.status_code, accession)
            return 0.0
        data = resp.json()
        # OpenNeuro stores size in the dataset metadata
        size_bytes = data.get("size", 0)
        if size_bytes:
            gb = size_bytes / 1e9
            log.info("OpenNeuro %s: %.2f GB", accession, gb)
            return gb
    except Exception as e:
        log.warning("OpenNeuro size check failed for %s: %s", accession, e)
    return 0.0


def get_figshare_size(accession: str) -> float:
    """Return total size in GB for a Figshare article."""
    cfg = get_config()
    article_id = re.search(r'(\d{5,})', accession)
    if not article_id:
        return 0.0
    article_id = article_id.group(1)
    try:
        resp = requests.get(f"{cfg.api_endpoints.figshare_api}/{article_id}/files",
                            timeout=cfg.timeouts.api_listing)
        if resp.status_code != 200:
            return 0.0
        files = resp.json()
        total = sum(f.get("size", 0) for f in files)
        gb = total / 1e9
        log.info("Figshare %s: %.2f GB (%d files)", article_id, gb, len(files))
        return gb
    except Exception:
        return 0.0


def get_dataset_size(accession_info: dict) -> float:
    """Get size in GB for a single dataset accession record.

    accession_info has keys: repository, accession, description
    """
    repo = accession_info.get("repository", "").lower()
    acc = accession_info.get("accession", "")

    if repo == "geo" or acc.upper().startswith("GSE"):
        return get_geo_size(acc)
    elif repo == "zenodo" or "zenodo" in acc.lower():
        return get_zenodo_size(acc)
    elif repo == "sra" or acc.upper().startswith(("PRJNA", "PRJEB")):
        return get_sra_size(acc)
    elif repo == "openneuro" or acc.lower().startswith("ds"):
        return get_openneuro_size(acc)
    elif repo == "figshare" or "figshare" in acc.lower():
        return get_figshare_size(acc)
    elif repo in ("omix", "hra", "ngdc", "cncb"):
        # OMIX/HRA don't have a public size API — log and return 0
        log.info("OMIX/HRA %s: size API not available, using estimate", acc)
        return 0.0
    elif repo == "dryad" or "dryad" in acc.lower():
        log.info("Dryad %s: size API not reliable, using estimate", acc)
        return 0.0
    else:
        log.warning("Unknown repository type for %s (%s) — cannot verify size", acc, repo)
        return 0.0


def _check_one_paper(paper: dict) -> dict:
    """Check PRIMARY dataset accessions for a single paper and update it.

    Only primary datasets count toward the size limit.
    Reanalyzed datasets are skipped.
    """
    cfg = get_config()
    accessions = paper.get("dataset_accession", [])
    primary = [a for a in accessions if a.get("role", "primary") != "reanalyzed"]
    if not primary:
        paper["verified_size_gb"] = 0.0
        paper["size_details"] = []
        paper["size_verified"] = False
        return paper

    details = []
    total_gb = 0.0
    for acc_info in primary:
        time.sleep(cfg.rate_limits.geo_delay)
        size = get_dataset_size(acc_info)
        details.append({
            "accession": acc_info.get("accession", ""),
            "repository": acc_info.get("repository", ""),
            "verified_size_gb": round(size, 2),
        })
        total_gb += size

    paper["verified_size_gb"] = round(total_gb, 2)
    paper["size_details"] = details
    paper["size_verified"] = total_gb > 0

    return paper


def verify_paper_sizes(
    papers: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Verify dataset sizes for all papers.

    Returns (passed, rejected) where rejected papers exceed the configured max size.
    Papers whose size cannot be determined are kept but flagged.
    """
    cfg = get_config()
    max_size_gb = cfg.thresholds.max_data_size_gb
    max_workers = cfg.parallelism.size_check_workers

    log.info("Verifying dataset sizes for %d papers...", len(papers))

    checked: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_check_one_paper, p): p.get("doi") for p in papers}
        for fut in as_completed(futures):
            doi = futures[fut]
            try:
                checked.append(fut.result())
            except Exception:
                log.exception("Size check failed for %s", doi)

    passed = []
    rejected = []
    for p in checked:
        vsize = p.get("verified_size_gb", 0)
        if vsize > max_size_gb:
            log.info(
                "REJECTED (size): %s — %.1f GB > %.1f GB limit",
                p.get("doi"),
                vsize,
                max_size_gb,
            )
            rejected.append(p)
        else:
            if p.get("size_verified"):
                # Update the estimated size with the verified one
                p["estimated_processed_data_size_gb"] = vsize
            else:
                log.warning(
                    "Size unverified for %s — keeping with estimate %.1f GB",
                    p.get("doi"),
                    p.get("estimated_processed_data_size_gb", 0),
                )
            passed.append(p)

    log.info(
        "Size verification: %d passed, %d rejected (>%.0f GB)",
        len(passed),
        len(rejected),
        max_size_gb,
    )
    return passed, rejected
