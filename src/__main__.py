"""
CLI entry point for CrUX data collector.
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime

from .collector import CruxCollector
from .processor import ChunkProcessor
from .manifest import ManifestGenerator, update_datasets_manifest
from .utils import get_existing_months


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Download and process Chrome User Experience Report datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'data_dir',
        type=str,
        help='Directory where data will be stored (e.g., data/global or data/us)'
    )

    parser.add_argument(
        '--credentials',
        type=str,
        help='Path to Google Cloud service account JSON file'
    )

    parser.add_argument(
        '--dataset-type',
        type=str,
        choices=['global', 'country'],
        default='global',
        help='Dataset type: global or country (default: global)'
    )

    parser.add_argument(
        '--country-code',
        type=str,
        help='Two-letter country code (e.g., US, DE, JP) - required when dataset-type is country'
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=2025,
        help='Starting year'
    )

    parser.add_argument(
        '--start-month',
        type=int,
        default=1,
        help='Starting month (1-12)'
    )

    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Only download missing months (skip existing data)'
    )

    parser.add_argument(
        '--manifest-only',
        action='store_true',
        help='Only update manifest.json without downloading data'
    )

    parser.add_argument(
        '--regenerate',
        action='store_true',
        help='Fully regenerate manifest from scratch instead of incremental update (use with --manifest-only)'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.dataset_type == 'country' and not args.country_code:
        parser.error("--country-code is required when --dataset-type is 'country'")

    # Setup paths
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Determine dataset name for manifest
    if args.dataset_type == 'global':
        dataset_name = 'global'
    else:
        dataset_name = args.country_code.lower()

    print("=" * 60)
    print("CrUX Cache Collector")
    print("=" * 60)
    print(f"Dataset type: {args.dataset_type}")
    if args.dataset_type == 'country':
        print(f"Country code: {args.country_code}")
    print(f"Dataset name: {dataset_name}")
    print(f"Data directory: {data_dir}")
    print()

    # If only updating manifest, do that and exit
    if args.manifest_only:
        if args.regenerate:
            print("Fully regenerating manifest from scratch...")
        else:
            print("Updating manifest incrementally...")
        generator = ManifestGenerator(data_dir, dataset_name)
        generator.update(incremental=not args.regenerate)

        # Also update the master datasets manifest
        print()
        data_root = data_dir.parent
        update_datasets_manifest(data_root)

        print("\n✓ Done!")
        return 0

    # Initialize components
    try:
        collector = CruxCollector(
            credentials_path=args.credentials,
            dataset_type=args.dataset_type,
            country_code=args.country_code
        )
        processor = ChunkProcessor(output_dir=data_dir)
        manifest_gen = ManifestGenerator(data_dir, dataset_name)
    except Exception as e:
        print(f"✗ Initialization error: {e}")
        return 1

    # Determine which months to download
    print("Querying available months from BigQuery...")
    available_months = collector.get_available_months(
        start_year=args.start_year,
        start_month=args.start_month
    )

    if not available_months:
        print("✗ No months found in BigQuery")
        return 1

    print(f"  Found {len(available_months)} available months")

    # Check for existing data
    to_download = available_months
    if args.incremental:
        existing = get_existing_months(data_dir)
        to_download = [m for m in available_months if m not in existing]
        print(f"  Already have {len(existing)} months")
        print(f"  Will download {len(to_download)} new months")

    if not to_download:
        print("\n✓ All data is up to date!")
        # Update manifest and exit
        manifest_gen.update()
        return 0

    print(f"\n{'Downloading' if args.incremental else 'Processing'} {len(to_download)} months:")
    print()

    # Download and process each month
    changes_made = False
    for year, month in to_download:
        try:
            # Fetch data from BigQuery
            df = collector.fetch_month_data(year, month)

            if df.empty:
                print(f"  ⚠ No data for {year}-{month:02d}, skipping")
                continue

            # Chunk and save
            chunks = processor.save_dataframe_chunked(df, year, month)

            if chunks:
                changes_made = True
                print()

        except Exception as e:
            print(f"  ✗ Error processing {year}-{month:02d}: {e}")
            continue

    # Generate/update manifest (incremental by default)
    print("=" * 60)
    manifest_gen.update(incremental=True)

    # Also update the master datasets manifest
    print()
    data_root = data_dir.parent
    update_datasets_manifest(data_root)

    print("\n" + "=" * 60)
    if changes_made:
        print("✓ Data update completed successfully!")
        return 0
    else:
        print("⚠ No changes made")
        return 0


if __name__ == '__main__':
    sys.exit(main())
