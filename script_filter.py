"""Filter analysis scripts from downloaded datasets to prevent answer leakage.

Benchmark datasets should contain only data files, not the analysis code
that produced the paper's results. This module identifies and excludes
scripts during download.
"""

import logging
import zipfile
from pathlib import Path, PurePosixPath

from . import config

log = logging.getLogger(__name__)


def is_script_file(filepath: str) -> bool:
    """Return True if the file looks like an analysis script rather than data.

    Logic (order matters):
    1. Known data extension → always keep
    2. Known script filename → always exclude
    3. Known script extension → always exclude
    4. File in a script directory with no recognized data extension → exclude
    5. Otherwise → keep
    """
    parts = PurePosixPath(filepath).parts
    name_lower = parts[-1].lower() if parts else filepath.lower()
    suffix = PurePosixPath(name_lower).suffix

    # 1. Known data extension — never filter
    if suffix in config.DATA_EXTENSIONS:
        return False

    # 2. Exact filename match
    if name_lower in config.SCRIPT_FILENAMES:
        return True

    # 3. Script extension match
    if suffix in config.SCRIPT_EXTENSIONS:
        return True

    # 4. Directory-based: only for files without a recognized data extension
    for part in parts[:-1]:
        if part.lower() in config.SCRIPT_DIRECTORIES:
            return True

    return False


def filter_file_list(
    files: list[dict],
    dataset_label: str = "",
) -> tuple[list[dict], list[dict]]:
    """Partition a file list into data files (kept) and scripts (excluded).

    Args:
        files: List of file dicts with at minimum a 'name' key.
        dataset_label: Label for log messages (e.g. 'Zenodo:14031498').

    Returns:
        (kept, excluded) tuple of file lists.
    """
    kept = []
    excluded = []
    for f in files:
        name = f.get("name", "")
        if is_script_file(name):
            excluded.append(f)
            log.info(
                "FILTERED (script): %s%s",
                name,
                f" [{dataset_label}]" if dataset_label else "",
            )
        else:
            kept.append(f)

    if excluded:
        log.info(
            "Script filter: kept %d, excluded %d files%s",
            len(kept),
            len(excluded),
            f" for {dataset_label}" if dataset_label else "",
        )
    return kept, excluded


def remove_scripts_from_directory(
    directory: Path,
    dataset_label: str = "",
) -> list[Path]:
    """Remove analysis scripts from an extracted directory tree.

    Used for post-download filtering (e.g. after extracting a Dryad ZIP).
    Returns list of removed file paths.
    """
    removed = []
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            rel = str(path.relative_to(directory))
            if is_script_file(rel):
                log.info(
                    "FILTERED (post-extract): %s%s",
                    rel,
                    f" [{dataset_label}]" if dataset_label else "",
                )
                path.unlink()
                removed.append(path)

    # Clean up empty directories left behind
    for dirpath in sorted(directory.rglob("*"), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()

    if removed:
        log.info(
            "Post-extract filter: removed %d scripts from %s",
            len(removed),
            dataset_label or str(directory),
        )
    return removed


def clean_zip_archives(directory: Path, dataset_label: str = "") -> int:
    """Find ZIP files in a directory, extract them, and remove scripts.

    Handles the case where a Zenodo/GEO record includes a ZIP that
    contains analysis scripts (e.g. a GitHub repo archive alongside data).
    If a ZIP contains ONLY scripts (no data files), the entire ZIP is removed.

    Returns count of script files removed.
    """
    total_removed = 0
    for zip_path in list(directory.rglob("*.zip")):
        if not zipfile.is_zipfile(zip_path):
            continue

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
        except zipfile.BadZipFile:
            continue

        # Check if this ZIP is mostly scripts
        data_files = [n for n in names if not is_script_file(n) and not n.endswith("/")]
        script_files = [n for n in names if is_script_file(n) and not n.endswith("/")]

        if not script_files:
            continue  # No scripts in this ZIP, leave it alone

        if not data_files:
            # ZIP contains ONLY scripts — remove the whole thing
            log.info(
                "FILTERED (script-only archive): %s (%d scripts)%s",
                zip_path.name,
                len(script_files),
                f" [{dataset_label}]" if dataset_label else "",
            )
            zip_path.unlink()
            total_removed += len(script_files)
            continue

        # ZIP has a mix — extract, filter, leave data files extracted
        extract_dir = zip_path.parent / zip_path.stem
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
            removed = remove_scripts_from_directory(extract_dir, dataset_label)
            total_removed += len(removed)
            # Remove the original ZIP since we've extracted its contents
            zip_path.unlink()
        except Exception:
            log.exception("Failed to extract/filter %s", zip_path)

    return total_removed
