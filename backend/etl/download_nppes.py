# etl/download_nppes.py
# Downloads NPPES files from CMS.
# First run: downloads the full replacement file (~8-10 GB)
# Weekly runs: downloads the weekly update file (~50-200 MB)

import zipfile
from datetime import datetime
from pathlib import Path
from typing import TypedDict


class DownloadUrls(TypedDict):
    page: str
    instructions: str


class FileInfo(TypedDict):
    name: str
    path: str
    size_mb: float
    modified: str


class LocalFiles(TypedDict):
    full_files: list[Path]
    weekly_files: list[Path]
    csv_files: list[Path]
    all_files: list[Path]

# The NPPES download page URL
NPPES_DOWNLOAD_PAGE = "https://download.cms.gov/nppes/NPI_Files.html"

# Local storage path for raw NPPES files
RAW_DATA_DIR = Path(__file__).parent.parent / "raw_data" / "nppes"


def get_download_urls() -> DownloadUrls:
    """
    Returns the known NPPES download URL patterns.
    CMS uses consistent naming so we can construct URLs directly.

    Returns dict with 'full' and 'weekly' URL patterns.
    """
    return {
        "page": NPPES_DOWNLOAD_PAGE,
        "instructions": (
            "Visit the NPPES download page and manually download:\n"
            "  FULL FILE: 'NPPES Data Dissemination - Full Replacement Monthly File'\n"
            "  WEEKLY:    'NPPES Data Dissemination - Weekly Update'\n"
            f"Save to: {RAW_DATA_DIR}"
        )
    }


def list_local_nppes_files() -> LocalFiles:
    """
    Lists NPPES files already downloaded locally.
    Returns dict with 'full_files' and 'weekly_files' lists.
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_files = list(RAW_DATA_DIR.glob("*.zip")) + \
                list(RAW_DATA_DIR.glob("*.csv"))

    full_files = [
        f for f in all_files
        if "full" in f.name.lower() or
           "npidata_pfile" in f.name.lower() and "update" not in f.name.lower()
    ]
    weekly_files = [
        f for f in all_files
        if "weekly" in f.name.lower() or "update" in f.name.lower()
    ]
    csv_files = [f for f in all_files if f.suffix == ".csv"]

    return {
        "full_files": sorted(full_files),
        "weekly_files": sorted(weekly_files),
        "csv_files": sorted(csv_files),
        "all_files": sorted(all_files),
    }


def extract_zip(zip_path: Path) -> Path:
    """
    Extracts a NPPES zip file.
    Returns path to the extracted CSV file.
    """
    print(f"Extracting {zip_path.name}...")
    extract_dir = zip_path.parent / zip_path.stem

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the main data CSV (largest file)
        csv_files = [
            f for f in zf.namelist()
            if f.endswith(".csv") and "npidata" in f.lower()
        ]

        if not csv_files:
            # Try any CSV
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]

        if not csv_files:
            raise ValueError(f"No CSV found in {zip_path.name}")

        # Extract all files
        zf.extractall(extract_dir)
        print(f"Extracted to {extract_dir}")

        # Return path to main CSV
        main_csv = extract_dir / csv_files[0]
        return main_csv


def find_nppes_csv() -> Path:
    """
    Finds the NPPES CSV file to process.
    Looks for extracted CSVs first, then ZIP files to extract.
    Returns path to CSV or raises FileNotFoundError.
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Look for already-extracted CSVs
    csv_files = list(RAW_DATA_DIR.rglob("npidata_pfile*.csv"))
    if csv_files:
        # Return the most recent one
        return sorted(csv_files)[-1]

    # Look for ZIP files to extract
    zip_files = list(RAW_DATA_DIR.glob("*.zip"))
    if zip_files:
        # Extract the most recent ZIP
        latest_zip = sorted(zip_files)[-1]
        return extract_zip(latest_zip)

    raise FileNotFoundError(
        f"No NPPES files found in {RAW_DATA_DIR}\n"
        f"Please download the NPPES file from:\n"
        f"{NPPES_DOWNLOAD_PAGE}\n"
        f"And save it to: {RAW_DATA_DIR}"
    )


def get_file_info(file_path: Path) -> FileInfo:
    """Returns basic info about a file."""
    stat = file_path.stat()
    size_mb = stat.st_size / (1024 * 1024)
    return {
        "name": file_path.name,
        "path": str(file_path),
        "size_mb": round(size_mb, 1),
        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
    }


if __name__ == "__main__":
    print("NPPES File Manager")
    print("-" * 40)
    files = list_local_nppes_files()

    if not files["all_files"]:
        urls = get_download_urls()
        print("No NPPES files found locally.")
        print()
        print(urls["instructions"])
    else:
        print(f"Found {len(files['all_files'])} file(s) in {RAW_DATA_DIR}:")
        for f in files["all_files"]:
            info = get_file_info(f)
            print(f"  {info['name']} ({info['size_mb']} MB)")