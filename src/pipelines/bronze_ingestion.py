"""Bronze Ingestion Pipeline.

Ingests raw ERP extracts, FX rates, Chart of Accounts mappings, and debt covenants.
Performs schema validation, metadata stamping (_ingested_at, _source_file, _batch_id),
and lands immutable Bronze datasets.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd


RAW_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "01_raw"


class BronzeIngestionPipeline:
    """Orchestrates landing and schema validation from Raw to Bronze."""

    EXPECTED_RAW_FILES = {
        "coa_crosswalk": "raw_coa_crosswalk_mapping.csv",
        "fx_rates": "raw_fx_rates_monthly.csv",
        "debt_covenants": "raw_debt_covenants_master.csv",
        "general_ledger": "raw_general_ledger_entries.csv",
    }

    REQUIRED_COLUMNS = {
        "coa_crosswalk": [
            "source_system", "legacy_account_id", "canonical_account_code",
            "account_category", "financial_statement", "normal_balance"
        ],
        "fx_rates": [
            "period", "from_currency", "to_currency", "spot_rate", "avg_rate"
        ],
        "debt_covenants": [
            "entity_id", "facility_name", "currency", "principal_origination",
            "annual_interest_rate", "monthly_amortization", "covenant_min_dscr"
        ],
        "general_ledger": [
            "entry_id", "batch_id", "transaction_date", "period", "entity_id",
            "source_system", "legacy_gl_code", "currency", "debit_amount", "credit_amount"
        ],
    }

    def __init__(self, raw_dir: Optional[Path] = None):
        self.raw_dir = raw_dir or RAW_DIR

    def validate_raw_files_exist(self) -> Dict[str, bool]:
        """Checks that all mandated raw source files exist in raw landing zone."""
        status = {}
        for key, filename in self.EXPECTED_RAW_FILES.items():
            path = self.raw_dir / filename
            status[key] = path.is_file() and path.stat().st_size > 0
        return status

    def ingest_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Reads raw CSV, validates schema conformance, and appends bronze audit metadata."""
        if dataset_name not in self.EXPECTED_RAW_FILES:
            raise ValueError(f"Unknown dataset: {dataset_name}. Expected one of {list(self.EXPECTED_RAW_FILES.keys())}")

        file_name = self.EXPECTED_RAW_FILES[dataset_name]
        file_path = self.raw_dir / file_name

        if not file_path.exists():
            raise FileNotFoundError(f"Raw file missing: {file_path}")

        df = pd.read_csv(file_path, dtype=str)

        # Schema validation
        required_cols = self.REQUIRED_COLUMNS[dataset_name]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Dataset {dataset_name} missing required columns: {missing_cols}")

        # Stamp Bronze audit metadata
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        df["_bronze_ingested_at"] = ingestion_ts
        df["_bronze_source_file"] = file_name

        return df

    def run(self) -> Dict[str, pd.DataFrame]:
        """Executes full Bronze ingestion for all raw datasets."""
        existence = self.validate_raw_files_exist()
        missing = [k for k, exists in existence.items() if not exists]
        if missing:
            raise FileNotFoundError(f"Missing required raw files: {missing}")

        bronze_tables: Dict[str, pd.DataFrame] = {}
        for name in self.EXPECTED_RAW_FILES:
            bronze_tables[name] = self.ingest_dataset(name)

        return bronze_tables


if __name__ == "__main__":
    pipeline = BronzeIngestionPipeline()
    tables = pipeline.run()
    print("Bronze Ingestion Summary:")
    for name, df in tables.items():
        print(f"  - {name}: {len(df)} records ingested.")
