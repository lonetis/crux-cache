"""
Manifest generation for CrUX datasets.
"""
import json
from pathlib import Path
from typing import List, Dict, Optional


class ManifestGenerator:
    """Generates and manages the manifest.json file."""

    def __init__(self, data_dir: Path, dataset_name: str = "global"):
        """
        Initialize manifest generator.

        Args:
            data_dir: Directory containing the chunked data files
            dataset_name: Name of the dataset (e.g., "global", "US", "DE", "JP")
        """
        self.data_dir = Path(data_dir)
        self.manifest_path = self.data_dir / "manifest.json"
        self.dataset_name = dataset_name

    def scan_chunks(self) -> Dict[str, List[Dict]]:
        """
        Scan the data directory for all CSV chunks and count origins.

        Returns:
            Dictionary mapping YYYYMM to list of chunk metadata
        """
        months = {}

        # Find all CSV files
        for csv_file in sorted(self.data_dir.glob("*.csv")):
            filename = csv_file.name

            # Parse filename: YYYYMM_N.csv
            try:
                basename = filename.replace('.csv', '')
                parts = basename.split('_')
                if len(parts) != 2:
                    continue

                yyyymm = parts[0]
                chunk_num = int(parts[1])

                if yyyymm not in months:
                    months[yyyymm] = []

                # Get file stats
                stats = csv_file.stat()

                # Count actual rows (excluding header only for chunk 1)
                with open(csv_file, 'r') as f:
                    total_lines = sum(1 for _ in f)
                    # Only chunk 1 has a header, so only subtract 1 for chunk 1
                    row_count = total_lines - 1 if chunk_num == 1 else total_lines

                months[yyyymm].append({
                    'chunk': chunk_num,
                    'filename': filename,
                    'size': stats.st_size,
                    'origins': row_count
                })

            except (ValueError, IndexError):
                # Skip malformed filenames
                continue

        # Sort chunks within each month
        for yyyymm in months:
            months[yyyymm].sort(key=lambda x: x['chunk'])

        return months

    def generate(self) -> Dict:
        """
        Generate complete manifest with all metadata.

        Returns:
            Manifest dictionary
        """
        print("Generating manifest...")

        months_data = self.scan_chunks()

        # Build manifest structure
        manifest = {
            'name': f'Cached Chrome User Experience Report - {self.dataset_name}',
            'months': {}
        }

        total_origins = 0
        total_size = 0

        for yyyymm, chunks in sorted(months_data.items()):
            year = int(yyyymm[:4])
            month = int(yyyymm[4:6])

            month_size = sum(c['size'] for c in chunks)
            total_size += month_size

            # Count actual origins from all chunks
            month_origins = sum(c['origins'] for c in chunks)
            total_origins += month_origins

            manifest['months'][yyyymm] = {
                'year': year,
                'month': month,
                'chunks': chunks,
                'total_chunks': len(chunks),
                'total_size': month_size,
                'origins': month_origins
            }

            size_mb = month_size / (1024 * 1024)
            print(f"  {year}-{month:02d}: {len(chunks)} chunks, {size_mb:.1f} MB, {month_origins:,} origins")

        # Add summary statistics
        manifest['summary'] = {
            'total_months': len(months_data),
            'total_size': total_size,
            'earliest_month': min(months_data.keys()) if months_data else None,
            'latest_month': max(months_data.keys()) if months_data else None
        }

        print(f"\n  Total: {len(months_data)} months, {total_size / (1024**3):.2f} GB")

        return manifest

    def save(self, manifest: Dict) -> None:
        """
        Save manifest to JSON file.

        Args:
            manifest: Manifest dictionary to save
        """
        with open(self.manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)

        print(f"  ✓ Manifest saved to {self.manifest_path}")

    def update(self) -> Dict:
        """
        Scan directory and update manifest file.

        Returns:
            Updated manifest dictionary
        """
        manifest = self.generate()
        self.save(manifest)
        return manifest


class DatasetsManifestGenerator:
    """Generates master datasets manifest listing all available datasets."""

    def __init__(self, data_root_dir: Path):
        """
        Initialize datasets manifest generator.

        Args:
            data_root_dir: Root data directory containing dataset subdirectories
        """
        self.data_root_dir = Path(data_root_dir)
        self.manifest_path = self.data_root_dir / "datasets.json"

    def scan_datasets(self) -> List[Dict]:
        """
        Scan data directory for all datasets.

        Returns:
            List of dataset metadata dictionaries
        """
        datasets = []

        # Scan all subdirectories in data/
        for dataset_dir in sorted(self.data_root_dir.iterdir()):
            if not dataset_dir.is_dir():
                continue

            # Check if manifest.json exists
            manifest_path = dataset_dir / "manifest.json"
            if not manifest_path.exists():
                continue

            try:
                # Load the dataset manifest
                with open(manifest_path, 'r') as f:
                    manifest = json.load(f)

                dataset_id = dataset_dir.name
                dataset_name = manifest.get('name', f'Unknown - {dataset_id}')

                # Extract summary info
                summary = manifest.get('summary', {})
                months = manifest.get('months', {})

                # Get latest month data
                latest_month_id = summary.get('latest_month')
                latest_month_data = None
                if latest_month_id and latest_month_id in months:
                    latest_month_data = months[latest_month_id]

                datasets.append({
                    'id': dataset_id,
                    'name': dataset_name,
                    'total_months': summary.get('total_months', 0),
                    'earliest_month': summary.get('earliest_month'),
                    'latest_month': summary.get('latest_month'),
                    'latest_origins': latest_month_data.get('origins') if latest_month_data else None,
                    'total_size': summary.get('total_size', 0)
                })

            except (json.JSONDecodeError, KeyError) as e:
                print(f"  ⚠ Skipping {dataset_dir.name}: Invalid manifest ({e})")
                continue

        return datasets

    def generate(self) -> Dict:
        """
        Generate master datasets manifest.

        Returns:
            Datasets manifest dictionary
        """
        print("Generating datasets manifest...")

        datasets = self.scan_datasets()

        manifest = {
            'datasets': datasets,
            'total_datasets': len(datasets)
        }

        if datasets:
            for dataset in datasets:
                print(f"  ✓ {dataset['id']}: {dataset['name']}")
        else:
            print("  ⚠ No datasets found")

        return manifest

    def save(self, manifest: Dict) -> None:
        """
        Save datasets manifest to JSON file.

        Args:
            manifest: Datasets manifest dictionary to save
        """
        with open(self.manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)

        print(f"  ✓ Datasets manifest saved to {self.manifest_path}")

    def update(self) -> Dict:
        """
        Scan directory and update datasets manifest file.

        Returns:
            Updated datasets manifest dictionary
        """
        manifest = self.generate()
        self.save(manifest)
        return manifest


def update_datasets_manifest(data_root_dir: Path) -> None:
    """
    Helper function to update the master datasets manifest.

    Args:
        data_root_dir: Root data directory (e.g., 'data/')
    """
    generator = DatasetsManifestGenerator(data_root_dir)
    generator.update()
