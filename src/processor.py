"""
CSV chunking processor for splitting large datasets into manageable files.
"""
import os
import pandas as pd
from pathlib import Path
from typing import List


class ChunkProcessor:
    """Handles splitting large CSV files into smaller chunks."""

    # Target chunk size: 25MB
    CHUNK_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB

    def __init__(self, output_dir: Path):
        """
        Initialize processor with output directory.

        Args:
            output_dir: Directory where chunks will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def chunk_dataframe(self, df: pd.DataFrame, year: int, month: int) -> List[dict]:
        """
        Split a DataFrame into multiple CSV chunks.

        Args:
            df: DataFrame to split
            year: Year for filename
            month: Month for filename

        Returns:
            List of dicts with chunk metadata (filename, size, rows)
        """
        print(f"Chunking data for {year}{month:02d}...")

        # Estimate row size (bytes per row)
        sample_size = min(1000, len(df))
        sample_csv = df.head(sample_size).to_csv(index=False)
        avg_bytes_per_row = len(sample_csv.encode('utf-8')) / sample_size

        # Calculate rows per chunk (including header overhead)
        header_size = len(','.join(df.columns).encode('utf-8')) + 1  # +1 for newline
        rows_per_chunk = int((self.CHUNK_SIZE_BYTES - header_size) / avg_bytes_per_row)

        if rows_per_chunk <= 0:
            rows_per_chunk = 1000  # Fallback

        print(f"  Estimated {avg_bytes_per_row:.1f} bytes/row → {rows_per_chunk:,} rows/chunk")

        # Split into chunks
        chunks_metadata = []
        total_rows = len(df)
        chunk_num = 1

        for start_idx in range(0, total_rows, rows_per_chunk):
            end_idx = min(start_idx + rows_per_chunk, total_rows)
            chunk_df = df.iloc[start_idx:end_idx]

            # Generate filename
            filename = f"{year}{month:02d}_{chunk_num}.csv"
            filepath = self.output_dir / filename

            # Write chunk (only first chunk gets header)
            chunk_df.to_csv(filepath, index=False, header=(chunk_num == 1))

            # Get file size
            file_size = filepath.stat().st_size

            chunks_metadata.append({
                'filename': filename,
                'size': file_size,
                'rows': len(chunk_df),
                'start_row': start_idx,
                'end_row': end_idx
            })

            size_mb = file_size / (1024 * 1024)
            print(f"  ✓ Chunk {chunk_num}: {filename} ({size_mb:.2f} MB, {len(chunk_df):,} rows)")

            chunk_num += 1

        total_size = sum(c['size'] for c in chunks_metadata)
        total_size_mb = total_size / (1024 * 1024)
        print(f"  → Total: {len(chunks_metadata)} chunks, {total_size_mb:.2f} MB")

        return chunks_metadata

    def save_dataframe_chunked(self, df: pd.DataFrame, year: int, month: int) -> List[dict]:
        """
        Save a DataFrame as chunked CSV files.

        Args:
            df: DataFrame to save
            year: Year for naming
            month: Month for naming

        Returns:
            List of chunk metadata
        """
        return self.chunk_dataframe(df, year, month)
