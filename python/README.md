# crux-cache Python Package

Python package for accessing CrUX (Chrome User Experience Report) cached data from the crux-cache repository.

## Installation

```bash
pip install crux-cache
```

## Quick Start

```python
from crux_cache import CruxCache

# Initialize the client
cache = CruxCache()

# List available datasets
datasets = cache.list_datasets()
for ds in datasets:
    print(f"{ds['id']}: {ds['latest_origins']} origins")

# Iterate over the latest global dataset
for origin, rank in cache.get_dataset('global'):
    print(f"{origin}: {rank}")
```

## Usage Examples

### Filter by Rank (Top Domains)

Use `max_rank` to filter domains with rank ≤ max_rank. Valid values: 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000, 50000000.

```python
from crux_cache import CruxCache

cache = CruxCache()

# Get top 1k domains from the latest US dataset
for origin, rank in cache.get_dataset('us', max_rank=1000):
    print(f"{origin}: {rank}")

# Get top 5k domains (includes top 1k)
for origin, rank in cache.get_dataset('global', max_rank=5000):
    print(f"{origin}: {rank}")

# Get top 1 million domains from global dataset
for origin, rank in cache.get_dataset('global', max_rank=1000000):
    print(f"{origin}: {rank}")
```

### Access Specific Months

```python
from crux_cache import CruxCache

cache = CruxCache()

# List available months for a dataset
months = cache.list_months('de')
print(f"Available months: {', '.join(months)}")
print(f"Latest month: {months[-1]}")

# Get a specific month
for origin, rank in cache.get_dataset('global', month='202510'):
    print(f"{origin}: {rank}")
```

### Cache Management

```python
from crux_cache import CruxCache

# Use default cache (.crux in current directory)
cache = CruxCache()

# Use a custom cache directory
cache = CruxCache(cache_dir='/tmp/crux')

# Set custom metadata TTL (in seconds)
cache = CruxCache(metadata_ttl=3600)  # 1 hour

# Clear the cache
cache.clear_cache()
```

## Features

- Automatic caching with configurable TTL
- Access global and country-specific datasets (us, de, jp)
- Filter by rank value to get top domains (e.g., top 1k, 5k, 1M)
- Access current or historical data by month
- Simple API with sensible defaults

## API Reference

### CruxCache

Main client for accessing CrUX cached data.

#### `__init__(cache_dir=".crux", metadata_ttl=86400)`

Initialize the client.
- `cache_dir`: Cache directory (default: `.crux`)
- `metadata_ttl`: Metadata cache TTL in seconds (default: 86400 = 1 day)

#### `list_datasets() -> List[Dict]`

List all available datasets with their metadata (id, name, total_months, earliest_month, latest_month, latest_origins, total_size).

#### `list_months(dataset_type: str) -> List[str]`

List available months for a dataset in YYYYMM format.

#### `get_dataset(dataset_type: str, month: Optional[str] = None, max_rank: Optional[int] = None) -> CruxDataset`

Get an iterator for a specific dataset and month. Returns all domains where rank ≤ max_rank.

**Parameters:**
- `dataset_type`: 'global', 'us', 'de', or 'jp'
- `month`: YYYYMM format (e.g., '202510'). Defaults to latest month
- `max_rank`: Filter by rank (1000, 5000, 10000, 50000, 100000, 500000, 1000000, etc.)

**Returns:** Iterator yielding (origin, rank) tuples

#### `clear_cache()`

Clear all cached files. Metadata and CSV files will be re-downloaded on next access.

### CruxDataset

Iterator that yields `(origin, rank)` tuples when iterating.

## Data Format

Each iteration yields a tuple of:
- `origin` (str): Full URL (e.g., `https://www.google.com`)
- `rank` (int): Popularity bucket (1000, 10000, 100000, 1000000, etc.)

## Caching Behavior

- **Metadata files** (datasets.json, manifest.json): Cached with TTL (default: 1 day)
- **CSV chunks**: Cached indefinitely (reused across sessions)
- **Cache location**: `.crux/` in current directory (configurable)
- **Clear cache**: Use `cache.clear_cache()` to remove all cached files

## Requirements

- Python 3.7+
- requests >= 2.25.0

## License

MIT License - See [LICENSE](../LICENSE)

CrUX data provided by Google under [CrUX Dataset Terms](https://developer.chrome.com/docs/crux)

## Links

- **Main Repository**: https://github.com/lonetis/crux-cache
- **PyPI Package**: https://pypi.org/project/crux-cache/
- **Web Interface**: https://lonetis.github.io/crux-cache
