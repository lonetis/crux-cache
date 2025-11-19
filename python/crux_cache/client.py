"""Main client for the crux_cache package."""

from typing import List, Optional, Dict, Any

from .cache import CacheManager
from .dataset import CruxDataset
from .constants import DEFAULT_CACHE_DIR, DEFAULT_METADATA_TTL, VALID_RANK_VALUES
from .exceptions import DatasetNotFoundError, MonthNotFoundError


class CruxCache:
    """
    Main client for accessing CrUX (Chrome User Experience Report) cached data.

    This class provides methods to list available datasets and months, and to
    iterate over domain rankings.
    """

    def __init__(self, cache_dir: str = DEFAULT_CACHE_DIR, metadata_ttl: int = DEFAULT_METADATA_TTL):
        """
        Initialize the CruxCache client.

        Args:
            cache_dir: Directory to store cached files (default: '.crux' in current directory)
            metadata_ttl: Time-to-live for metadata files in seconds (default: 86400 = 1 day)

        Example:
            >>> cache = CruxCache()
            >>> cache = CruxCache(cache_dir='/tmp/crux', metadata_ttl=3600)
        """
        self.cache_manager = CacheManager(cache_dir, metadata_ttl)

    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all available datasets.

        Returns:
            List of dataset information dictionaries containing:
                - id: Dataset identifier (e.g., 'global', 'us', 'de', 'jp')
                - name: Full dataset name
                - total_months: Number of available months
                - earliest_month: Earliest available month (YYYYMM format)
                - latest_month: Latest available month (YYYYMM format)
                - latest_origins: Number of origins in the latest month
                - total_size: Total size in bytes

        Example:
            >>> cache = CruxCache()
            >>> datasets = cache.list_datasets()
            >>> for ds in datasets:
            ...     print(f"{ds['id']}: {ds['latest_origins']} origins")
        """
        metadata = self.cache_manager.get_datasets_metadata()
        return metadata.get('datasets', [])

    def list_months(self, dataset_type: str) -> List[str]:
        """
        List all available months for a specific dataset.

        Args:
            dataset_type: Dataset type (e.g., 'global', 'us', 'de', 'jp')

        Returns:
            List of available months in YYYYMM format, sorted chronologically

        Raises:
            DatasetNotFoundError: If the dataset type does not exist

        Example:
            >>> cache = CruxCache()
            >>> months = cache.list_months('global')
            >>> print(f"Latest month: {months[-1]}")
        """
        # Validate dataset exists
        datasets = self.list_datasets()
        dataset_ids = [ds['id'] for ds in datasets]
        if dataset_type not in dataset_ids:
            raise DatasetNotFoundError(
                f"Dataset '{dataset_type}' not found. "
                f"Available datasets: {', '.join(dataset_ids)}"
            )

        # Get manifest and extract months
        manifest = self.cache_manager.get_manifest(dataset_type)
        months = list(manifest.get('months', {}).keys())
        return sorted(months)

    def get_dataset(
        self,
        dataset_type: str,
        month: Optional[str] = None,
        max_rank: Optional[int] = None
    ) -> CruxDataset:
        """
        Get an iterator for a specific dataset and month.

        The iterator yields tuples of (origin, rank) for each domain in the dataset.

        Note: Ranks follow a log10 scale with half steps (1k, 5k, 10k, 50k, 100k, 500k, 1M, etc.).
        In the source data, each rank bucket excludes previous buckets (rank 5000 = positions 1001-5000).
        However, max_rank filtering is cumulative: max_rank=5000 returns ALL domains with rank <= 5000,
        which includes both rank 1000 and rank 5000 domains (top 5k total).

        Args:
            dataset_type: Dataset type (e.g., 'global', 'us', 'de', 'jp')
            month: Month in YYYYMM format (e.g., '202510'). If None, uses the latest month.
            max_rank: Optional maximum rank value to filter by. Must be one of:
                      1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000, 50000000
                      Example: max_rank=1000 returns top 1k domains, max_rank=5000 returns top 5k domains

        Returns:
            CruxDataset iterator that yields (origin, rank) tuples

        Raises:
            DatasetNotFoundError: If the dataset type does not exist
            MonthNotFoundError: If the specified month is not available
            ValueError: If max_rank is not one of the valid rank values

        Example:
            >>> cache = CruxCache()
            >>> # Iterate over top 1k domains from the latest global dataset
            >>> for origin, rank in cache.get_dataset('global', max_rank=1000):
            ...     print(f"{origin}: {rank}")
            >>>
            >>> # Iterate over top 5k domains from a specific month
            >>> for origin, rank in cache.get_dataset('us', month='202510', max_rank=5000):
            ...     print(f"{origin}: {rank}")
            >>>
            >>> # Iterate over all domains (no filter)
            >>> for origin, rank in cache.get_dataset('global'):
            ...     print(f"{origin}: {rank}")
        """
        # Validate dataset exists
        datasets = self.list_datasets()
        dataset_ids = [ds['id'] for ds in datasets]
        if dataset_type not in dataset_ids:
            raise DatasetNotFoundError(
                f"Dataset '{dataset_type}' not found. "
                f"Available datasets: {', '.join(dataset_ids)}"
            )

        # Get manifest
        manifest = self.cache_manager.get_manifest(dataset_type)

        # Use latest month if not specified
        if month is None:
            available_months = sorted(manifest.get('months', {}).keys())
            if not available_months:
                raise MonthNotFoundError(f"No months available for dataset {dataset_type}")
            month = available_months[-1]

        # Create and return dataset iterator
        return CruxDataset(
            cache_manager=self.cache_manager,
            dataset_type=dataset_type,
            month=month,
            manifest=manifest,
            max_rank=max_rank
        )

    def clear_cache(self) -> None:
        """
        Clear all cached files.

        This will remove all downloaded datasets, manifests, and metadata files.
        They will be re-downloaded on next access.

        Example:
            >>> cache = CruxCache()
            >>> cache.clear_cache()
        """
        self.cache_manager.clear_cache()

    def __repr__(self) -> str:
        """String representation of the CruxCache instance."""
        return f"CruxCache(cache_dir='{self.cache_manager.cache_dir}')"
