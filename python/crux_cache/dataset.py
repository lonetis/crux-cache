"""Dataset iterator for streaming CSV data."""

import csv
from typing import Iterator, Tuple, Optional, List, Dict, Any

from .cache import CacheManager
from .constants import VALID_RANK_VALUES
from .exceptions import MonthNotFoundError


class CruxDataset:
    """Iterator for streaming CrUX dataset CSV data."""

    def __init__(
        self,
        cache_manager: CacheManager,
        dataset_type: str,
        month: str,
        manifest: Dict[str, Any],
        max_rank: Optional[int] = None
    ):
        """
        Initialize the dataset iterator.

        Args:
            cache_manager: Cache manager instance
            dataset_type: Dataset type (e.g., 'global', 'us')
            month: Month in YYYYMM format (e.g., '202510')
            manifest: Manifest data for the dataset
            max_rank: Optional maximum rank value to filter by (e.g., 1000 for top 1k).
                      Must be one of: 1000, 5000, 10000, 50000, 100000, 500000, 1000000, etc.
        """
        self.cache_manager = cache_manager
        self.dataset_type = dataset_type
        self.month = month
        self.manifest = manifest
        self.max_rank = max_rank

        # Validate max_rank if specified
        if max_rank is not None and max_rank not in VALID_RANK_VALUES:
            raise ValueError(
                f"max_rank must be one of {VALID_RANK_VALUES}, got {max_rank}"
            )

        # Validate month exists in manifest
        if month not in manifest.get('months', {}):
            available_months = sorted(manifest.get('months', {}).keys())
            raise MonthNotFoundError(
                f"Month {month} not found for dataset {dataset_type}. "
                f"Available months: {', '.join(available_months)}"
            )

        # Get chunk information
        self.month_data = manifest['months'][month]
        self.chunks: List[Dict[str, Any]] = self.month_data.get('chunks', [])
        self.total_origins = self.month_data.get('origins', 0)

    def __iter__(self) -> Iterator[Tuple[str, int]]:
        """
        Iterate over the dataset rows, filtering by max_rank if specified.

        Yields:
            Tuple of (origin, rank) for domains where rank <= max_rank
        """
        for chunk_idx, chunk_info in enumerate(self.chunks):
            # Download chunk file
            filename = chunk_info['filename']
            csv_path = self.cache_manager.get_csv_chunk(self.dataset_type, filename)

            # Read and yield rows
            with open(csv_path, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f)

                # Skip header only for the first chunk
                if chunk_idx == 0:
                    next(reader, None)  # Skip header row

                # Yield rows that match the rank filter
                for row in reader:
                    if len(row) < 2:
                        continue  # Skip malformed rows

                    origin = row[0]
                    try:
                        rank = int(row[1])
                    except (ValueError, IndexError):
                        continue  # Skip rows with invalid rank

                    # Filter by max_rank if specified
                    if self.max_rank is not None and rank > self.max_rank:
                        continue

                    yield (origin, rank)

    def __len__(self) -> int:
        """
        Get the total number of origins in this dataset.

        Note: When max_rank is specified, this returns the total origins in the dataset,
        not the filtered count (which can only be determined by iterating).

        Returns:
            Total number of origins in the dataset
        """
        return self.total_origins

    def __repr__(self) -> str:
        """String representation of the dataset."""
        max_rank_str = f", max_rank={self.max_rank}" if self.max_rank else ""
        return (
            f"CruxDataset(dataset_type='{self.dataset_type}', "
            f"month='{self.month}', total_origins={self.total_origins}{max_rank_str})"
        )
