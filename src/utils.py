"""
Utility functions for file management.
"""
from pathlib import Path


def get_existing_months(data_dir: Path) -> set[tuple[int, int]]:
    """
    Get list of months that already have data files.

    Args:
        data_dir: Directory to scan

    Returns:
        Set of (year, month) tuples
    """
    existing = set()

    for csv_file in data_dir.glob("*.csv"):
        try:
            basename = csv_file.stem  # Filename without extension
            yyyymm = basename.split('_')[0]
            year = int(yyyymm[:4])
            month = int(yyyymm[4:6])
            existing.add((year, month))
        except (ValueError, IndexError):
            continue

    return existing
