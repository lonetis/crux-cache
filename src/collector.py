"""
BigQuery data collector for Chrome User Experience Report dataset.
"""
import os
import json
from datetime import datetime
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


class CruxCollector:
    """Handles data collection from Google BigQuery CrUX dataset."""

    PROJECT_ID = "chrome-ux-report"
    DATASET_ID = "experimental"

    def __init__(self, credentials_path: Optional[str] = None, dataset_type: str = "global", country_code: Optional[str] = None):
        """
        Initialize the collector with authentication.

        Args:
            credentials_path: Path to service account JSON file.
                            If None, attempts to use environment variable or default credentials.
            dataset_type: Type of dataset - "global" or "country"
            country_code: Two-letter country code (required if dataset_type is "country")
        """
        self.credentials = self._load_credentials(credentials_path)
        self.client = bigquery.Client(credentials=self.credentials)
        self.dataset_type = dataset_type
        self.country_code = country_code.lower() if country_code else None

        if dataset_type == "country" and not country_code:
            raise ValueError("country_code is required when dataset_type is 'country'")

    def _load_credentials(self, credentials_path: Optional[str]) -> Optional[service_account.Credentials]:
        """Load Google Cloud credentials from various sources."""
        # Priority 1: Explicit path
        if credentials_path and os.path.exists(credentials_path):
            return service_account.Credentials.from_service_account_file(credentials_path)

        # Priority 2: Environment variable with JSON content
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')
        if creds_json:
            try:
                creds_data = json.loads(creds_json)
                return service_account.Credentials.from_service_account_info(creds_data)
            except json.JSONDecodeError:
                pass

        # Priority 3: Default application credentials
        return None

    def fetch_month_data(self, year: int, month: int) -> pd.DataFrame:
        """
        Fetch all origins for a specific month from CrUX dataset.

        Args:
            year: Year (e.g., 2023)
            month: Month (1-12)

        Returns:
            DataFrame with columns: origin, rank
        """
        yyyymm = year * 100 + month
        table_name = self.dataset_type  # "global" or "country"

        if self.dataset_type == "global":
            query = f"""
            SELECT DISTINCT origin, experimental.popularity.rank
            FROM `{self.PROJECT_ID}.{self.DATASET_ID}.{table_name}`
            WHERE yyyymm = {yyyymm}
            GROUP BY origin, experimental.popularity.rank
            ORDER BY experimental.popularity.rank, origin
            """
            dataset_label = "global"
        else:  # country
            query = f"""
            SELECT DISTINCT origin, experimental.popularity.rank
            FROM `{self.PROJECT_ID}.{self.DATASET_ID}.{table_name}`
            WHERE yyyymm = {yyyymm} AND country_code = '{self.country_code}'
            GROUP BY origin, experimental.popularity.rank
            ORDER BY experimental.popularity.rank, origin
            """
            dataset_label = f"{self.country_code}"

        print(f"Fetching {dataset_label} data for {year}-{month:02d} (yyyymm={yyyymm})...")

        try:
            df = self.client.query(query).to_dataframe()
            print(f"  → Retrieved {len(df):,} origins")
            return df
        except Exception as e:
            print(f"  ✗ Error fetching data: {e}")
            raise

    def get_available_months(self, start_year: int = 2025, start_month: int = 1) -> list[tuple[int, int]]:
        """
        Query BigQuery to find all available months in the dataset.

        Args:
            start_year: Earliest year to check (default: 2025)
            start_month: Earliest month to check (default: 1 for January)

        Returns:
            List of (year, month) tuples
        """
        table_name = self.dataset_type
        where_clause = f"WHERE yyyymm >= {start_year * 100 + start_month}"

        if self.dataset_type == "country":
            where_clause += f" AND country_code = '{self.country_code}'"

        query = f"""
        SELECT DISTINCT yyyymm
        FROM `{self.PROJECT_ID}.{self.DATASET_ID}.{table_name}`
        {where_clause}
        ORDER BY yyyymm
        """

        try:
            result = self.client.query(query).to_dataframe()
            months = []
            for yyyymm in result['yyyymm']:
                year = yyyymm // 100
                month = yyyymm % 100
                months.append((year, month))
            return months
        except Exception as e:
            print(f"Error querying available months: {e}")
            return []
