"""Cache management for crux_cache package."""

import os
import time
import json
import shutil
from typing import Dict, Any

try:
    import requests
except ImportError:
    raise ImportError(
        "The 'requests' library is required. Install it with: pip install requests"
    )

from .constants import (
    GITHUB_RAW_BASE_URL,
    DATASETS_JSON_PATH,
    MANIFEST_JSON_PATH,
    CSV_CHUNK_PATH,
)
from .exceptions import DownloadError, CacheError


class CacheManager:
    """Manages local cache for downloaded files."""

    def __init__(self, cache_dir: str, metadata_ttl: int):
        """
        Initialize the cache manager.

        Args:
            cache_dir: Directory to store cached files
            metadata_ttl: Time-to-live for metadata files in seconds
        """
        self.cache_dir = cache_dir
        self.metadata_ttl = metadata_ttl
        self._ensure_cache_dir()

    def _ensure_cache_dir(self) -> None:
        """Ensure the cache directory exists."""
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
        except Exception as e:
            raise CacheError(f"Failed to create cache directory: {e}")

    def _get_cache_path(self, relative_path: str) -> str:
        """
        Get the local cache path for a relative GitHub path.

        Args:
            relative_path: Relative path in the GitHub repo (e.g., 'data/datasets.json')

        Returns:
            Absolute local cache path
        """
        return os.path.join(self.cache_dir, relative_path)

    def _is_cache_valid(self, cache_path: str, is_metadata: bool) -> bool:
        """
        Check if a cached file is still valid.

        Args:
            cache_path: Path to the cached file
            is_metadata: Whether this is a metadata file (subject to TTL)

        Returns:
            True if cache is valid, False otherwise
        """
        if not os.path.exists(cache_path):
            return False

        # CSV chunks are cached indefinitely
        if not is_metadata:
            return True

        # Metadata files are subject to TTL
        mtime = os.path.getmtime(cache_path)
        age = time.time() - mtime
        return age < self.metadata_ttl

    def _download_file(self, url: str, destination: str) -> None:
        """
        Download a file from GitHub to local cache.

        Args:
            url: Full URL to download from
            destination: Local path to save the file

        Raises:
            DownloadError: If download fails
        """
        try:
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(destination), exist_ok=True)

            # Download with streaming to handle large files
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            # Write to file
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        except requests.RequestException as e:
            raise DownloadError(f"Failed to download {url}: {e}")
        except Exception as e:
            raise DownloadError(f"Failed to save file to {destination}: {e}")

    def get_json(self, relative_path: str, is_metadata: bool = True) -> Dict[str, Any]:
        """
        Get a JSON file, using cache if valid or downloading if needed.

        Args:
            relative_path: Relative path in the GitHub repo
            is_metadata: Whether this is a metadata file (subject to TTL)

        Returns:
            Parsed JSON data

        Raises:
            DownloadError: If download fails
        """
        cache_path = self._get_cache_path(relative_path)

        # Download if cache is invalid
        if not self._is_cache_valid(cache_path, is_metadata):
            url = f"{GITHUB_RAW_BASE_URL}/{relative_path}"
            self._download_file(url, cache_path)

        # Load and return JSON
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise CacheError(f"Failed to read JSON from {cache_path}: {e}")

    def get_csv_chunk(self, dataset_type: str, filename: str) -> str:
        """
        Get a CSV chunk file path, downloading if not cached.

        Args:
            dataset_type: Dataset type (e.g., 'global', 'us')
            filename: CSV filename (e.g., '202510_1.csv')

        Returns:
            Local path to the cached CSV file

        Raises:
            DownloadError: If download fails
        """
        relative_path = CSV_CHUNK_PATH.format(
            dataset_type=dataset_type,
            filename=filename
        )
        cache_path = self._get_cache_path(relative_path)

        # Download if not cached (CSV chunks are cached indefinitely)
        if not self._is_cache_valid(cache_path, is_metadata=False):
            url = f"{GITHUB_RAW_BASE_URL}/{relative_path}"
            self._download_file(url, cache_path)

        return cache_path

    def clear_cache(self) -> None:
        """
        Clear all cached files.

        Raises:
            CacheError: If clearing cache fails
        """
        try:
            if os.path.exists(self.cache_dir):
                shutil.rmtree(self.cache_dir)
                self._ensure_cache_dir()
        except Exception as e:
            raise CacheError(f"Failed to clear cache: {e}")

    def get_datasets_metadata(self) -> Dict[str, Any]:
        """
        Get the datasets.json metadata.

        Returns:
            Parsed datasets.json data
        """
        return self.get_json(DATASETS_JSON_PATH, is_metadata=True)

    def get_manifest(self, dataset_type: str) -> Dict[str, Any]:
        """
        Get the manifest.json for a specific dataset.

        Args:
            dataset_type: Dataset type (e.g., 'global', 'us')

        Returns:
            Parsed manifest.json data
        """
        relative_path = MANIFEST_JSON_PATH.format(dataset_type=dataset_type)
        return self.get_json(relative_path, is_metadata=True)
