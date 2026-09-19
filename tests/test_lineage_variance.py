"""Tests for Lakehouse Lineage, Dimensional Referencing, and Variance Gates.

Validates:
1. End-to-end Lineage Variance Gate execution and zero-loss guarantees.
2. 100% COA mapping rate in Silver harmonization (zero unmapped transactions).
3. Non-null assertions for USD converted debits and credits.
4. Foreign key referential integrity in Gold dimensional models.
"""

from pathlib import Path
import pytest

from src.pipelines.bronze_ingestion import BronzeIngestionPipeline
from src.pipelines.gold_dimensional_modeling import GoldDimensionalModelingPipeline
from src.pipelines.lineage_variance_gate import LineageVarianceGate
from src.pipelines.silver_harmonization import SilverHarmonizationPipeline


@pytest.fixture(scope="module")
def gate_results():
    gate = LineageVarianceGate()
    return gate.evaluate_gate()


@pytest.fixture(scope="module")
def gold_models():
    pipeline = GoldDimensionalModelingPipeline()
    return pipeline.run()


class TestLakehouseLineageVariance:
    """Lineage, variance, and dimensional integrity verification."""

    def test_lineage_variance_gate_passed(self, gate_results):
        """The automated Lineage Variance Gate must return status PASSED."""
        assert gate_results["status"] == "PASSED", f"Gate failed with results: {gate_results}"
        for check_name, details in gate_results["checks"].items():
            assert details["passed"] is True, f"Check {check_name} failed: {details}"

    def test_silver_harmonization_coverage(self):
        """Silver harmonization must have 0% unmapped GL entries and positive USD rates."""
        pipeline = SilverHarmonizationPipeline()
        gl_silver, debt_silver = pipeline.run()

        # Zero unmapped accounts
        assert gl_silver["canonical_account_code"].isna().sum() == 0
        assert (gl_silver["canonical_account_code"] != "").all()

        # Zero nulls in calculated USD amounts
        assert gl_silver["debit_usd"].isna().sum() == 0
        assert gl_silver["credit_usd"].isna().sum() == 0
        assert gl_silver["net_amount_usd"].isna().sum() == 0
        assert (gl_silver["fx_conversion_rate"] > 0).all()

    def test_gold_star_schema_referential_integrity(self, gold_models):
        """Fact tables must strictly reference existing keys in dimension tables."""
        dim_entity = gold_models["dim_entity"]
        dim_period = gold_models["dim_period"]
        dim_account = gold_models["dim_account"]
        fact_fin = gold_models["fact_financial_monthly"]
        fact_covenant = gold_models["fact_covenant_health"]

        valid_entities = set(dim_entity["entity_id"])
        valid_periods = set(dim_period["period_id"])
        valid_accounts = set(dim_account["canonical_account_code"])

        # Check fact_financial_monthly FKs
        assert set(fact_fin["entity_id"]).issubset(valid_entities)
        assert set(fact_fin["period"]).issubset(valid_periods)
        assert set(fact_fin["canonical_account_code"]).issubset(valid_accounts)

        # Check fact_covenant_health FKs
        assert set(fact_covenant["entity_id"]).issubset(valid_entities)
        assert set(fact_covenant["period"]).issubset(valid_periods)

    def test_covenant_health_metric_bounds(self, gold_models):
        """Validates that DSCR, EBITDA, and debt service metrics are calculated and bounded."""
        fact_covenant = gold_models["fact_covenant_health"]
        assert len(fact_covenant) == 96  # 4 entities * 24 periods

        # Debt service must always be positive
        assert (fact_covenant["debt_service_usd"] > 0).all()

        # DSCR must be positive
        assert (fact_covenant["dscr"] > 0).all()

        # Check statuses are valid domain values
        valid_statuses = {"COMPLIANT", "WARNING", "BREACH"}
        assert set(fact_covenant["covenant_status"]).issubset(valid_statuses)
