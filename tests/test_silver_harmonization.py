"""Tests for Silver Semantic Harmonization & Multi-Currency Ledger Pipeline.

Validates Phase 2 specifications:
1. Schema invariant enforcement (non-null dates, valid entity codes, non-negative amounts).
2. Canonical GAAP/IFRS Chart of Accounts crosswalking completeness.
3. Multi-currency FX translation and conversion math accuracy.
4. Transaction polarity standardization (revenue inflows vs cost items).
5. Cryptographic salted SHA-256 lineage hash integrity and uniqueness.
6. Partitioned Parquet persistence (partitioned by entity_code).
7. Audit telemetry summary reporting.
"""

import hashlib
import json
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.pipelines.bronze_ingestion import BronzeIngestionPipeline
from src.pipelines.silver_harmonization import (
    SilverHarmonizationPipeline,
    HASH_SALT,
    VALID_ENTITIES,
    EXPENSE_STATEMENT_LINES,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "02_silver"
REPORTS_DIR = PROJECT_ROOT / "reports"


@pytest.fixture(scope="module")
def pipeline() -> SilverHarmonizationPipeline:
    return SilverHarmonizationPipeline()


@pytest.fixture(scope="module")
def harmonized_data(pipeline: SilverHarmonizationPipeline):
    gl_silver, debt_silver = pipeline.run()
    return gl_silver, debt_silver


class TestSilverHarmonization:
    """Test suite for Phase 2 Silver Harmonization."""

    def test_schema_invariant_enforcement(self, pipeline: SilverHarmonizationPipeline):
        """Pipeline must raise ValueError when schema invariants are violated."""
        # 1. Test null date rejection
        bad_df_null_date = pd.DataFrame([{
            "posting_date": None,
            "entity_code": "DURA_US",
            "debit_amount": 100.0,
            "credit_amount": 0.0,
        }])
        with pytest.raises(ValueError, match="Schema Invariant Failure.*null"):
            pipeline.validate_schema_invariants(bad_df_null_date)

        # 2. Test invalid entity rejection
        bad_df_bad_entity = pd.DataFrame([{
            "posting_date": "2024-01-15",
            "entity_code": "UNKNOWN_CORP",
            "debit_amount": 100.0,
            "credit_amount": 0.0,
        }])
        with pytest.raises(ValueError, match="Schema Invariant Failure.*Unknown entity"):
            pipeline.validate_schema_invariants(bad_df_bad_entity)

        # 3. Test negative amount rejection
        bad_df_neg_amount = pd.DataFrame([{
            "posting_date": "2024-01-15",
            "entity_code": "DURA_US",
            "debit_amount": -50.0,
            "credit_amount": 0.0,
        }])
        with pytest.raises(ValueError, match="Schema Invariant Failure.*negative"):
            pipeline.validate_schema_invariants(bad_df_neg_amount)

    def test_canonical_coa_crosswalk_coverage(self, harmonized_data):
        """Every transaction must successfully attach canonical_code and financial_statement_line."""
        gl_silver, _ = harmonized_data
        assert len(gl_silver) == 1440

        # Zero unmapped accounts
        assert gl_silver["canonical_code"].isna().sum() == 0
        assert (gl_silver["canonical_code"] != "").all()
        assert gl_silver["financial_statement_line"].isna().sum() == 0

        # Validate mandatory lines are present
        actual_lines = set(gl_silver["financial_statement_line"].unique())
        expected_mandatory = {"REVENUE", "COGS", "OPEX", "DEPRECIATION", "INTEREST_EXPENSE"}
        assert expected_mandatory.issubset(actual_lines)

    def test_multi_currency_fx_translation_accuracy(self, harmonized_data):
        """Verifies USD translation adheres to amount_usd = amount_local_currency * avg_rate."""
        gl_silver, _ = harmonized_data

        # Sample check across currencies
        for curr in ["USD", "GBP", "EUR"]:
            curr_subset = gl_silver[gl_silver["currency"] == curr]
            assert len(curr_subset) > 0

            # USD to USD identity check
            if curr == "USD":
                assert (curr_subset["amount_usd"] == curr_subset["amount_local_currency"]).all()
            else:
                expected_usd = (curr_subset["amount_local_currency"] * curr_subset["avg_rate"]).round(2)
                diff = (curr_subset["amount_usd"] - expected_usd).abs().max()
                assert diff == 0.0, f"FX translation divergence for {curr}: max diff = {diff}"

    def test_transaction_polarity_standardization(self, harmonized_data):
        """Revenue credits must be positive inflows; expense debits must be positive cost items."""
        gl_silver, _ = harmonized_data

        # 1. Revenue polarity
        rev_rows = gl_silver[gl_silver["financial_statement_line"] == "REVENUE"]
        assert len(rev_rows) > 0
        for _, row in rev_rows.iterrows():
            if row["credit_amount"] > 0:
                assert row["polarity_type"] == "INFLOW"
                assert row["standardized_amount_usd"] > 0
                assert row["standardized_amount_usd"] == row["amount_usd"]

        # 2. Expense cost items polarity
        cost_rows = gl_silver[gl_silver["financial_statement_line"].isin(EXPENSE_STATEMENT_LINES)]
        assert len(cost_rows) > 0
        for _, row in cost_rows.iterrows():
            if row["debit_amount"] > 0:
                assert row["polarity_type"] == "COST_ITEM"
                assert row["standardized_amount_usd"] > 0
                assert row["standardized_amount_usd"] == row["amount_usd"]

    def test_cryptographic_lineage_hashes(self, harmonized_data):
        """Salted SHA-256 lineage hash must be non-empty, 64-chars hex, unique, and deterministic."""
        gl_silver, _ = harmonized_data
        hashes = gl_silver["_lineage_hash"]

        assert hashes.isna().sum() == 0
        assert (hashes.str.len() == 64).all()
        # All 1,440 transaction hashes should be unique
        assert len(hashes.unique()) == len(gl_silver)

        # Reproducibility check on first row
        first = gl_silver.iloc[0]
        expected_raw = (
            HASH_SALT +
            str(first["entry_id"]) + "|" +
            str(first["entity_code"]) + "|" +
            str(first["posting_date"]) + "|" +
            f"{first['amount_usd']:.2f}"
        )
        expected_hash = hashlib.sha256(expected_raw.encode("utf-8")).hexdigest()
        assert first["_lineage_hash"] == expected_hash

    def test_partitioned_parquet_persistence(self):
        """Verifies partitioned Parquet ledger structure and row count across all partitions."""
        prm_path = SILVER_DIR / "prm_harmonized_financial_ledger.parquet"
        assert prm_path.exists(), f"Partitioned directory not found: {prm_path}"

        # Verify 4 partition folders exist
        for entity in VALID_ENTITIES:
            part_dir = prm_path / f"entity_code={entity}"
            assert part_dir.is_dir(), f"Missing partition directory: {part_dir}"
            part_files = list(part_dir.glob("*.parquet"))
            assert len(part_files) > 0, f"No parquet files in partition: {part_dir}"

        # Read partitioned dataset via PyArrow
        dataset = pq.read_table(str(prm_path))
        df_from_parquet = dataset.to_pandas()
        assert len(df_from_parquet) == 1440

        # Check each entity partition has exactly 360 records
        counts = df_from_parquet["entity_code"].value_counts()
        for entity in VALID_ENTITIES:
            assert counts[entity] == 360

    def test_audit_summary_telemetry(self):
        """Verifies reports/silver_harmonization_audit_summary.json exists and has valid telemetry."""
        audit_path = REPORTS_DIR / "silver_harmonization_audit_summary.json"
        assert audit_path.exists(), f"Audit summary not found: {audit_path}"

        with open(audit_path, "r", encoding="utf-8") as f:
            summary = json.load(f)

        assert summary["pipeline"] == "SilverHarmonizationPipeline"
        assert summary["total_records"] == 1440
        assert summary["total_gross_volume_usd"] > 0
        assert set(summary["entities"].keys()) == VALID_ENTITIES

        for ent, meta in summary["entities"].items():
            assert meta["row_count"] == 360
            assert meta["revenue_usd_volume"] > 0
            assert meta["cost_usd_volume"] > 0
            assert meta["total_usd_volume"] > 0
