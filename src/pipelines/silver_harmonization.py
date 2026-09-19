"""Silver Harmonization Pipeline.

Harmonizes heterogeneous ERP source systems into a unified Lakehouse schema:
1. Crosswalks legacy account IDs to canonical Chart of Accounts (REV_CORE, COGS_DIRECT, etc.).
2. Translates functional currency balances to standard reporting currency (USD) via monthly FX rates.
3. Performs data quality assertions (e.g. zero unmapped accounts, valid dates, type casting).
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Dict, Optional, Tuple

# Ensure project root is in sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd

from src.pipelines.bronze_ingestion import BronzeIngestionPipeline


SILVER_DIR = PROJECT_ROOT / "data" / "02_silver"


class SilverHarmonizationPipeline:
    """Orchestrates Bronze-to-Silver harmonization and multi-currency translation."""

    def __init__(self, silver_dir: Optional[Path] = None, bronze_pipeline: Optional[BronzeIngestionPipeline] = None):
        self.silver_dir = silver_dir or SILVER_DIR
        self.bronze_pipeline = bronze_pipeline or BronzeIngestionPipeline()

    def harmonize_general_ledger(
        self,
        gl_df: pd.DataFrame,
        coa_df: pd.DataFrame,
        fx_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Harmonizes GL transactions with canonical COA and converts amounts to reporting USD."""
        # Convert numeric types
        gl = gl_df.copy()
        gl["debit_amount"] = pd.to_numeric(gl["debit_amount"], errors="raise")
        gl["credit_amount"] = pd.to_numeric(gl["credit_amount"], errors="raise")
        gl["net_amount"] = pd.to_numeric(gl["net_amount"], errors="raise")
        gl["transaction_date"] = pd.to_datetime(gl["transaction_date"])

        # Join COA crosswalk
        coa_clean = coa_df[[
            "source_system", "legacy_account_id", "canonical_account_code",
            "account_category", "financial_statement", "normal_balance"
        ]].drop_duplicates()

        harmonized = gl.merge(
            coa_clean,
            left_on=["source_system", "legacy_gl_code"],
            right_on=["source_system", "legacy_account_id"],
            how="left"
        )

        # Assertion: zero unmapped accounts
        unmapped = harmonized[harmonized["canonical_account_code"].isna()]
        if not unmapped.empty:
            sample_unmapped = unmapped[["source_system", "legacy_gl_code"]].drop_duplicates().to_dict(orient="records")
            raise ValueError(f"Found {len(unmapped)} GL lines with unmapped legacy accounts: {sample_unmapped}")

        # Join FX conversion rates (to USD)
        fx_clean = fx_df[fx_df["to_currency"] == "USD"][[
            "period", "from_currency", "spot_rate", "avg_rate"
        ]].copy()
        fx_clean["spot_rate"] = pd.to_numeric(fx_clean["spot_rate"], errors="raise")
        fx_clean["avg_rate"] = pd.to_numeric(fx_clean["avg_rate"], errors="raise")

        harmonized = harmonized.merge(
            fx_clean,
            left_on=["period", "currency"],
            right_on=["period", "from_currency"],
            how="left"
        )

        # Assertion: zero missing FX rates
        missing_fx = harmonized[harmonized["avg_rate"].isna()]
        if not missing_fx.empty:
            missing_combos = missing_fx[["period", "currency"]].drop_duplicates().to_dict(orient="records")
            raise ValueError(f"Missing FX conversion rates for: {missing_combos}")

        # Compute reporting currency (USD) conversions
        # Income Statement uses average monthly rate; Balance Sheet uses spot rate
        is_mask = harmonized["financial_statement"] == "Income_Statement"
        effective_fx = np.where(is_mask, harmonized["avg_rate"], harmonized["spot_rate"])

        harmonized["fx_conversion_rate"] = effective_fx
        harmonized["debit_usd"] = (harmonized["debit_amount"] * effective_fx).round(2)
        harmonized["credit_usd"] = (harmonized["credit_amount"] * effective_fx).round(2)
        harmonized["net_amount_usd"] = (harmonized["net_amount"] * effective_fx).round(2)

        # Clean metadata
        harmonized.drop(columns=["legacy_account_id", "from_currency"], inplace=True)
        harmonized["_silver_harmonized_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        return harmonized

    def harmonize_debt_covenants(self, debt_df: pd.DataFrame, fx_df: pd.DataFrame) -> pd.DataFrame:
        """Harmonizes debt covenant terms and converts principal/amortization to USD."""
        debt = debt_df.copy()
        numeric_cols = [
            "principal_origination", "annual_interest_rate",
            "monthly_amortization", "covenant_min_dscr", "covenant_max_leverage_ratio"
        ]
        for col in numeric_cols:
            debt[col] = pd.to_numeric(debt[col], errors="raise")

        # Use 2024-01 initial spot rate for base USD translation of facilities
        base_fx = fx_df[(fx_df["period"] == "2024-01") & (fx_df["to_currency"] == "USD")][[
            "from_currency", "spot_rate"
        ]].drop_duplicates()
        base_fx["spot_rate"] = pd.to_numeric(base_fx["spot_rate"])

        harmonized_debt = debt.merge(
            base_fx,
            left_on="currency",
            right_on="from_currency",
            how="left"
        )
        harmonized_debt["spot_rate"] = harmonized_debt["spot_rate"].fillna(1.0)
        harmonized_debt["principal_usd"] = (harmonized_debt["principal_origination"] * harmonized_debt["spot_rate"]).round(2)
        harmonized_debt["monthly_amortization_usd"] = (harmonized_debt["monthly_amortization"] * harmonized_debt["spot_rate"]).round(2)
        harmonized_debt.drop(columns=["from_currency"], inplace=True)
        harmonized_debt["_silver_harmonized_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        return harmonized_debt

    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Executes full Silver harmonization and persists Parquet tables."""
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        bronze_tables = self.bronze_pipeline.run()

        gl_silver = self.harmonize_general_ledger(
            bronze_tables["general_ledger"],
            bronze_tables["coa_crosswalk"],
            bronze_tables["fx_rates"]
        )
        debt_silver = self.harmonize_debt_covenants(
            bronze_tables["debt_covenants"],
            bronze_tables["fx_rates"]
        )

        gl_path = self.silver_dir / "silver_general_ledger_harmonized.parquet"
        debt_path = self.silver_dir / "silver_debt_covenants.parquet"

        gl_silver.to_parquet(gl_path, index=False)
        debt_silver.to_parquet(debt_path, index=False)

        return gl_silver, debt_silver


if __name__ == "__main__":
    pipeline = SilverHarmonizationPipeline()
    gl_df, debt_df = pipeline.run()
    print("Silver Harmonization Summary:")
    print(f"  - Harmonized GL records: {len(gl_df)}")
    print(f"  - Harmonized Debt Facilities: {len(debt_df)}")
