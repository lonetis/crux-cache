"""
crux_cache - Python package for accessing CrUX (Chrome User Experience Report) cached data.

This package provides easy access to cached CrUX data from the crux-cache GitHub repository,
allowing you to iterate over domain rankings.
"""

from .client import CruxCache
from .dataset import CruxDataset
from .exceptions import (
    CruxCacheError,
    DatasetNotFoundError,
    MonthNotFoundError,
    DownloadError,
    CacheError,
)

__version__ = "1.0.0"
__author__ = "Louis"
__all__ = [
    "CruxCache",
    "CruxDataset",
    "CruxCacheError",
    "DatasetNotFoundError",
    "MonthNotFoundError",
    "DownloadError",
    "CacheError",
]
