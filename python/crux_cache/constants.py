"""Constants for the crux_cache package."""

# GitHub repository base URL for raw file access
GITHUB_RAW_BASE_URL = "https://raw.githubusercontent.com/lonetis/crux-cache/main"

# Data directory paths
DATASETS_JSON_PATH = "data/datasets.json"
MANIFEST_JSON_PATH = "data/{dataset_type}/manifest.json"
CSV_CHUNK_PATH = "data/{dataset_type}/{filename}"

# Cache settings
DEFAULT_CACHE_DIR = ".crux"
DEFAULT_METADATA_TTL = 86400  # 1 day in seconds

# CSV format
CSV_HEADER = ["origin", "rank"]

# Valid rank values (log10 scale with half steps)
# Pattern: 1k, 5k, 10k, 50k, 100k, 500k, 1M, 5M, 10M, etc.
VALID_RANK_VALUES = [
    1000,      # top 1k
    5000,      # top 5k
    10000,     # top 10k
    50000,     # top 50k
    100000,    # top 100k
    500000,    # top 500k
    1000000,   # top 1M
    5000000,   # top 5M
    10000000,  # top 10M
    50000000,  # top 50M
]
