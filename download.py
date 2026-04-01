"""Step 6: Download datasets from GEO, Zenodo, OpenNeuro, OMIX/HRA, Figshare,
and Dryad, and paper PDFs.  Downloads are parallelized via ThreadPoolExecutor.
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from . import config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GEO supplementary files
# ---------------------------------------------------------------------------

def _list_geo_files(accession: str) -> list[dict]:
    """List supplementary files for a GEO accession with their download URLs."""
    prefix = accession[:-3] + "nnn"
    base_url = f"{config.GEO_FTP_BASE}/{prefix}/{accession}/suppl/"

    try:
        resp = requests.get(base_url, timeout=15)
        if resp.status_code != 200:
            log.warning("Cannot list GEO files for %s (HTTP %d)", accession, resp.status_code)
            return []
    except requests.RequestException as e:
        log.warning("GEO listing request failed for %s: %s", accession, e)
        return []

    # Parse the HTML directory listing for file links
    soup = BeautifulSoup(resp.text, "html.parser")
    files = []
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if href and not href.startswith("?") and not href.startswith("/"):
            files.append({
                "name": href,
                "url": base_url + href,
            })

    return files


def download_geo(accession: str, dest_dir: Path) -> Path:
    """Download all supplementary files for a GEO series."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = _list_geo_files(accession)

    if not files:
        log.warning("No files found for GEO %s", accession)
        return dest_dir

    for fi in files:
        dest_path = dest_dir / fi["name"]
        if dest_path.exists():
            log.info("Skipping (exists): %s", dest_path.name)
            continue

        log.info("Downloading: %s → %s", fi["url"], dest_path.name)
        try:
            with requests.get(fi["url"], stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            log.info("  Done: %s (%.1f MB)", dest_path.name, dest_path.stat().st_size / 1e6)
        except Exception:
            log.exception("Failed to download %s", fi["url"])
            if dest_path.exists():
                dest_path.unlink()

        time.sleep(config.GEO_DELAY)

    return dest_dir


# ---------------------------------------------------------------------------
# Zenodo files
# ---------------------------------------------------------------------------

def _list_zenodo_files(accession: str) -> list[dict]:
    """Get file list and download URLs from Zenodo API."""
    record_id = re.search(r'(\d{5,})', accession)
    if not record_id:
        return []
    record_id = record_id.group(1)

    try:
        resp = requests.get(f"{config.ZENODO_API}/{record_id}", timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []

    files = []
    for f in data.get("files", []):
        files.append({
            "name": f.get("key", "unknown"),
            "url": f.get("links", {}).get("self", ""),
            "size": f.get("size", 0),
        })
    return files


def download_zenodo(accession: str, dest_dir: Path) -> Path:
    """Download all files from a Zenodo record."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = _list_zenodo_files(accession)

    if not files:
        log.warning("No files found for Zenodo %s", accession)
        return dest_dir

    for fi in files:
        if not fi["url"]:
            continue
        dest_path = dest_dir / fi["name"]
        if dest_path.exists():
            log.info("Skipping (exists): %s", dest_path.name)
            continue

        log.info(
            "Downloading: %s (%.1f MB) → %s",
            fi["name"],
            fi["size"] / 1e6,
            dest_path.name,
        )
        try:
            with requests.get(fi["url"], stream=True, timeout=600) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            log.info("  Done: %s", dest_path.name)
        except Exception:
            log.exception("Failed to download %s", fi["url"])
            if dest_path.exists():
                dest_path.unlink()

        time.sleep(config.ZENODO_DELAY)

    return dest_dir


# ---------------------------------------------------------------------------
# OpenNeuro files
# ---------------------------------------------------------------------------

OPENNEURO_API = "https://openneuro.org/crn/datasets"


def _list_openneuro_files(dataset_id: str) -> list[dict]:
    """Get file list from the latest snapshot of an OpenNeuro dataset.

    Uses the OpenNeuro CRN API:
      GET /datasets/{id}/snapshots  -> list of snapshots (pick latest)
      GET /datasets/{id}/snapshots/{tag}/files  -> flat file listing
    """
    snapshots_url = f"{OPENNEURO_API}/{dataset_id}/snapshots"
    try:
        resp = requests.get(snapshots_url, timeout=15)
        if resp.status_code != 200:
            log.warning("Cannot list OpenNeuro snapshots for %s (HTTP %d)", dataset_id, resp.status_code)
            return []
        snapshots = resp.json()
    except Exception:
        log.warning("OpenNeuro snapshot request failed for %s", dataset_id)
        return []

    if not snapshots:
        log.warning("No snapshots found for OpenNeuro %s", dataset_id)
        return []

    # Snapshots are usually returned newest-first; pick the first entry
    latest = snapshots[0] if isinstance(snapshots, list) else None
    if latest is None:
        return []
    tag = latest.get("tag") or latest.get("id", "")
    if not tag:
        return []

    files_url = f"{OPENNEURO_API}/{dataset_id}/snapshots/{tag}/files"
    try:
        resp = requests.get(files_url, timeout=30)
        if resp.status_code != 200:
            log.warning("Cannot list OpenNeuro files for %s/%s (HTTP %d)", dataset_id, tag, resp.status_code)
            return []
        file_list = resp.json()
    except Exception:
        log.warning("OpenNeuro file listing failed for %s/%s", dataset_id, tag)
        return []

    files = []
    if isinstance(file_list, list):
        for entry in file_list:
            filename = entry.get("filename") or entry.get("name", "")
            urls = entry.get("urls", [])
            url = urls[0] if urls else entry.get("url", "")
            if filename and url:
                files.append({"name": filename, "url": url})

    return files


def download_openneuro(dataset_id: str, dest_dir: Path) -> Path:
    """Download all files from the latest snapshot of an OpenNeuro dataset."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = _list_openneuro_files(dataset_id)

    if not files:
        log.warning("No files found for OpenNeuro %s", dataset_id)
        return dest_dir

    for fi in files:
        dest_path = dest_dir / fi["name"]
        if dest_path.exists():
            log.info("Skipping (exists): %s", dest_path.name)
            continue

        log.info("Downloading: %s → %s", fi["url"][:80], dest_path.name)
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with requests.get(fi["url"], stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            log.info("  Done: %s (%.1f MB)", dest_path.name, dest_path.stat().st_size / 1e6)
        except Exception:
            log.exception("Failed to download %s", fi["url"])
            if dest_path.exists():
                dest_path.unlink()

        time.sleep(0.2)

    return dest_dir


# ---------------------------------------------------------------------------
# OMIX / HRA  (Chinese National Genomics Data Center — NGDC / CNCB)
# ---------------------------------------------------------------------------

OMIX_BASE = "https://ngdc.cncb.ac.cn/omix/release"


def _list_omix_files(accession: str) -> list[dict]:
    """Attempt to scrape the file list from an OMIX release page.

    Page URL: https://ngdc.cncb.ac.cn/omix/release/{accession}
    The release page usually contains a table of downloadable files.
    """
    release_url = f"{OMIX_BASE}/{accession}"
    try:
        resp = requests.get(release_url, timeout=20,
                            headers={"User-Agent": "KosmosPipeline/1.0"})
        if resp.status_code != 200:
            log.warning("Cannot access OMIX page for %s (HTTP %d)", accession, resp.status_code)
            return []
    except Exception:
        log.warning("OMIX request failed for %s", accession)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    files = []

    # Look for download links — typically <a> tags whose href points to a file
    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Match hrefs that look like file downloads (contain /download/ or end
        # with common extensions)
        if "/download/" in href or re.search(r'\.(gz|zip|tar|bz2|csv|tsv|txt|fastq|bam|h5|hdf5)$', href, re.I):
            name = href.rsplit("/", 1)[-1] or link.get_text(strip=True) or "unknown"
            # Make absolute URL if needed
            if href.startswith("/"):
                href = f"https://ngdc.cncb.ac.cn{href}"
            elif not href.startswith("http"):
                href = f"{release_url}/{href}"
            files.append({"name": name, "url": href})

    return files


def download_omix(accession: str, dest_dir: Path) -> Path:
    """Download files from an OMIX / HRA (NGDC/CNCB) release."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = _list_omix_files(accession)

    if not files:
        log.warning("No downloadable files found for OMIX %s", accession)
        return dest_dir

    for fi in files:
        dest_path = dest_dir / fi["name"]
        if dest_path.exists():
            log.info("Skipping (exists): %s", dest_path.name)
            continue

        log.info("Downloading: %s → %s", fi["url"][:80], dest_path.name)
        try:
            with requests.get(fi["url"], stream=True, timeout=300,
                              headers={"User-Agent": "KosmosPipeline/1.0"}) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            log.info("  Done: %s (%.1f MB)", dest_path.name, dest_path.stat().st_size / 1e6)
        except Exception:
            log.exception("Failed to download %s", fi["url"])
            if dest_path.exists():
                dest_path.unlink()

        time.sleep(0.3)

    return dest_dir


# ---------------------------------------------------------------------------
# Figshare files
# ---------------------------------------------------------------------------

FIGSHARE_API = "https://api.figshare.com/v2/articles"


def _list_figshare_files(article_id: str) -> list[dict]:
    """Get file list from the Figshare API for a given article ID."""
    url = f"{FIGSHARE_API}/{article_id}/files"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            log.warning("Cannot list Figshare files for article %s (HTTP %d)", article_id, resp.status_code)
            return []
        data = resp.json()
    except Exception:
        log.warning("Figshare file listing failed for article %s", article_id)
        return []

    files = []
    if isinstance(data, list):
        for entry in data:
            files.append({
                "name": entry.get("name", "unknown"),
                "url": entry.get("download_url", ""),
                "size": entry.get("size", 0),
            })
    return files


def download_figshare(article_id: str, dest_dir: Path) -> Path:
    """Download all files from a Figshare article."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = _list_figshare_files(article_id)

    if not files:
        log.warning("No files found for Figshare article %s", article_id)
        return dest_dir

    for fi in files:
        if not fi["url"]:
            continue
        dest_path = dest_dir / fi["name"]
        if dest_path.exists():
            log.info("Skipping (exists): %s", dest_path.name)
            continue

        log.info(
            "Downloading: %s (%.1f MB) → %s",
            fi["name"],
            fi["size"] / 1e6,
            dest_path.name,
        )
        try:
            with requests.get(fi["url"], stream=True, timeout=600) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            log.info("  Done: %s", dest_path.name)
        except Exception:
            log.exception("Failed to download %s", fi["url"])
            if dest_path.exists():
                dest_path.unlink()

        time.sleep(0.2)

    return dest_dir


# ---------------------------------------------------------------------------
# Dryad files
# ---------------------------------------------------------------------------

DRYAD_API = "https://datadryad.org/api/v2/datasets"


def download_dryad(doi: str, dest_dir: Path) -> Path:
    """Download the dataset archive from Dryad.

    The Dryad v2 API exposes a download endpoint at:
        GET /api/v2/datasets/{doi}/download
    which streams the full dataset as a ZIP archive.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    # Encode the DOI for the URL path (slashes must be preserved for Dryad)
    download_url = f"{DRYAD_API}/{doi}/download"

    archive_name = re.sub(r'[^\w.-]', '_', doi) + ".zip"
    dest_path = dest_dir / archive_name

    if dest_path.exists():
        log.info("Skipping (exists): %s", dest_path.name)
        return dest_dir

    log.info("Downloading Dryad dataset %s → %s", doi, dest_path.name)
    try:
        with requests.get(download_url, stream=True, timeout=600,
                          headers={"User-Agent": "KosmosPipeline/1.0"},
                          allow_redirects=True) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
        log.info("  Done: %s (%.1f MB)", dest_path.name, dest_path.stat().st_size / 1e6)
    except Exception:
        log.exception("Failed to download Dryad dataset %s", doi)
        if dest_path.exists():
            dest_path.unlink()

    return dest_dir


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def _download_single_dataset(acc_info: dict, base_dir: Path) -> tuple[str, str | None]:
    """Download a single dataset entry and return (accession, local_dir_path | None).

    Returns (accession, None) when the dataset is skipped or unsupported.
    """
    role = acc_info.get("role", "primary")
    acc = acc_info.get("accession", "")
    if role == "reanalyzed":
        log.info("Skipping reanalyzed (non-key) dataset: %s", acc)
        return acc, None

    repo = acc_info.get("repository", "").lower()
    # Use accession as subdir name (sanitize)
    safe_name = re.sub(r'[^\w.-]', '_', acc)
    dest_dir = base_dir / safe_name

    if repo == "geo" or acc.upper().startswith("GSE"):
        download_geo(acc, dest_dir)
    elif repo == "zenodo" or "zenodo" in acc.lower():
        download_zenodo(acc, dest_dir)
    elif repo == "openneuro" or (acc.lower().startswith("ds") and acc[2:].isdigit()):
        download_openneuro(acc, dest_dir)
    elif repo in ("omix", "hra", "ngdc", "cncb") or acc.upper().startswith("OMIX"):
        download_omix(acc, dest_dir)
    elif repo == "figshare" or "figshare" in acc.lower():
        # Figshare accessions are typically numeric article IDs
        article_id = re.search(r'(\d{5,})', acc)
        article_id = article_id.group(1) if article_id else acc
        download_figshare(article_id, dest_dir)
    elif repo == "dryad" or "dryad" in acc.lower():
        download_dryad(acc, dest_dir)
    else:
        log.warning("Unsupported repository %s for accession %s — skipping download", repo, acc)
        return acc, None

    return acc, str(dest_dir)


def download_paper_datasets(paper: dict, base_dir: Path) -> dict:
    """Download PRIMARY datasets for a single paper.

    Skips datasets with role='reanalyzed' (from other studies).
    Individual dataset downloads within the paper run in parallel.
    Returns a dict mapping accession -> local directory path.
    """
    accessions = paper.get("dataset_accession", [])
    results = {}

    if not accessions:
        return results

    with ThreadPoolExecutor(max_workers=config.DOWNLOAD_WORKERS) as pool:
        futures = {
            pool.submit(_download_single_dataset, acc_info, base_dir): acc_info
            for acc_info in accessions
        }

        for future in as_completed(futures):
            acc_info = futures[future]
            try:
                acc, path = future.result()
                if path is not None:
                    results[acc] = path
            except Exception:
                log.exception(
                    "Unexpected error downloading dataset %s",
                    acc_info.get("accession", "?"),
                )

    return results


def download_all(papers: list[dict], datasets_dir: Path) -> dict:
    """Download datasets for all selected papers (parallelized).

    Returns dict mapping DOI -> {accession: local_path}.
    """
    datasets_dir.mkdir(parents=True, exist_ok=True)
    all_results = {}

    def _process_paper(paper: dict) -> tuple[str, dict]:
        doi = paper.get("doi", "unknown")
        log.info("Downloading datasets for: %s", doi)
        paper_dir = datasets_dir / re.sub(r'[^\w.-]', '_', doi)
        results = download_paper_datasets(paper, paper_dir)
        return doi, results

    with ThreadPoolExecutor(max_workers=config.DOWNLOAD_WORKERS) as pool:
        futures = {pool.submit(_process_paper, p): p for p in papers}

        for future in as_completed(futures):
            paper = futures[future]
            try:
                doi, results = future.result()
                all_results[doi] = results
            except Exception:
                doi = paper.get("doi", "unknown")
                log.exception("Failed to download datasets for paper %s", doi)
                all_results[doi] = {}

    return all_results


# ---------------------------------------------------------------------------
# Paper PDF download
# ---------------------------------------------------------------------------

def _try_europepmc_pdf(pmcid: str) -> str | None:
    """Get PDF URL from Europe PMC for a given PMCID."""
    if not pmcid:
        return None
    url = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmcid}&blobtype=pdf"
    try:
        resp = requests.head(url, timeout=10, allow_redirects=True)
        if resp.status_code == 200:
            ct = resp.headers.get("Content-Type", "")
            if "pdf" in ct:
                return url
    except Exception:
        pass
    return None


def _try_unpaywall_pdf(doi: str) -> str | None:
    """Get open-access PDF URL via Unpaywall API (free, email required)."""
    try:
        resp = requests.get(
            f"https://api.unpaywall.org/v2/{doi}",
            params={"email": "kosmos-pipeline@example.com"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        best = data.get("best_oa_location") or {}
        pdf_url = best.get("url_for_pdf")
        if pdf_url:
            return pdf_url
        # Some entries only have url_for_landing_page, skip those
    except Exception:
        pass
    return None


def _try_doi_redirect_pdf(doi: str) -> str | None:
    """Follow DOI redirect and look for a PDF link on the landing page."""
    try:
        # Try requesting PDF content type directly
        resp = requests.get(
            f"https://doi.org/{doi}",
            headers={"Accept": "application/pdf", "User-Agent": "KosmosPipeline/1.0"},
            timeout=15,
            allow_redirects=True,
        )
        if resp.status_code == 200 and "pdf" in resp.headers.get("Content-Type", ""):
            return resp.url
    except Exception:
        pass
    return None


def download_pdf(doi: str, pmcid: str, dest_path: Path) -> bool:
    """Download a paper PDF, trying multiple sources.

    Tries in order: Europe PMC, Unpaywall, direct DOI redirect.
    Returns True if successful.
    """
    if dest_path.exists():
        log.info("PDF already exists: %s", dest_path.name)
        return True

    pdf_url = (
        _try_europepmc_pdf(pmcid)
        or _try_unpaywall_pdf(doi)
        or _try_doi_redirect_pdf(doi)
    )

    if not pdf_url:
        log.warning("No PDF URL found for %s", doi)
        return False

    log.info("Downloading PDF: %s → %s", pdf_url[:80], dest_path.name)
    try:
        resp = requests.get(
            pdf_url,
            headers={"User-Agent": "KosmosPipeline/1.0"},
            timeout=60,
            allow_redirects=True,
        )
        resp.raise_for_status()

        # Verify we actually got a PDF
        if not resp.headers.get("Content-Type", "").startswith("application/pdf") and not resp.content[:5] == b"%PDF-":
            log.warning("Response for %s is not a PDF (Content-Type: %s)", doi, resp.headers.get("Content-Type"))
            return False

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(resp.content)
        log.info("  Saved: %s (%.1f MB)", dest_path.name, len(resp.content) / 1e6)
        return True
    except Exception:
        log.exception("PDF download failed for %s", doi)
        if dest_path.exists():
            dest_path.unlink()
        return False


def _download_single_pdf(paper: dict, pdfs_dir: Path) -> tuple[str, dict]:
    """Download a single paper's PDF and return (doi, result_dict)."""
    doi = paper.get("doi", "unknown")
    first_author = paper.get("first_author", "unknown")
    year = paper.get("pub_date", "")[:4]
    safe_doi = re.sub(r'[^\w.-]', '_', doi)
    filename = f"{first_author}_{year}_{safe_doi}.pdf"
    dest_path = pdfs_dir / filename

    ok = download_pdf(doi, paper.get("pmcid", ""), dest_path)
    return doi, {"path": str(dest_path) if ok else "", "downloaded": ok}


def download_pdfs(papers: list[dict], pdfs_dir: Path) -> dict:
    """Download PDFs for all selected papers (parallelized).

    Returns dict mapping DOI -> {"path": str, "downloaded": bool}.
    """
    pdfs_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    with ThreadPoolExecutor(max_workers=config.DOWNLOAD_WORKERS) as pool:
        futures = {
            pool.submit(_download_single_pdf, paper, pdfs_dir): paper
            for paper in papers
        }

        for future in as_completed(futures):
            paper = futures[future]
            try:
                doi, result = future.result()
                results[doi] = result
            except Exception:
                doi = paper.get("doi", "unknown")
                log.exception("Failed to download PDF for %s", doi)
                results[doi] = {"path": "", "downloaded": False}

    downloaded = sum(1 for r in results.values() if r["downloaded"])
    log.info("PDFs: %d/%d downloaded", downloaded, len(papers))
    return results
