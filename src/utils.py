"""
Utility functions for file management.
"""
import json
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
    manifest_path = data_dir / "manifest.json"

    # Try reading from manifest.json first (preferred method for sparse checkouts)
    if manifest_path.exists():
        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
                months_data = manifest.get('months', {})
                for month_info in months_data.values():
                    year = month_info.get('year')
                    month = month_info.get('month')
                    if year is not None and month is not None:
                        existing.add((year, month))
            return existing
        except (json.JSONDecodeError, KeyError, ValueError):
            # Fall back to scanning CSV files if manifest is invalid
            pass

    # Fallback: scan for CSV files (for backward compatibility)
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
