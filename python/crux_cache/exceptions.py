"""Custom exceptions for the crux_cache package."""


class CruxCacheError(Exception):
    """Base exception for all crux_cache errors."""
    pass


class DatasetNotFoundError(CruxCacheError):
    """Raised when a requested dataset does not exist."""
    pass


class MonthNotFoundError(CruxCacheError):
    """Raised when a requested month is not available for a dataset."""
    pass


class DownloadError(CruxCacheError):
    """Raised when a file download fails."""
    pass


class CacheError(CruxCacheError):
    """Raised when cache operations fail."""
    pass
