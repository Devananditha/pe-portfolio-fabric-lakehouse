"""Unit Test Suite for Phase 3: Gold Dimensional Modeling & Covenant Health Marts.

Validates:
1. DuckDB SQL CTE monthly aggregations & financial statement lines.
2. Core P&L formulas (Gross Profit, EBITDA, EBIT, Margins).
3. Window functions: 3-month rolling EBITDA, TTM revenue & EBITDA, PoP growth.
4. Debt-service coverage ratio (DSCR) calculations and dynamic health classifications.
5. Parquet feature mart and Power BI executive CSV file generation.
"""

from pathlib import Path
import pandas as pd
import pytest

from src.pipelines.gold_dimensional_modeling import GoldDimensionalModelingPipeline

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_ROOT / "data" / "03_gold"


@pytest.fixture(scope="module")
def gold_pipeline_outputs():
    """Executes the Gold modeling pipeline and returns outputs dictionary."""
    pipeline = GoldDimensionalModelingPipeline()
    return pipeline.run()


class TestGoldDimensionalModeling:
    """Comprehensive test assertions for Gold analytical marts and window functions."""

    def test_gold_covenant_health_dimensions_and_records(self, gold_pipeline_outputs):
        """Validates that feat_portfolio_covenant_health contains 96 rows across 4 entities."""
        df = gold_pipeline_outputs["feat_portfolio_covenant_health"]
        assert len(df) == 96, f"Expected 96 records, got {len(df)}"

        expected_entities = {"DURA_US", "MED_UK", "LOGI_EU", "RETAIL_US"}
        assert set(df["entity_code"].unique()) == expected_entities

        # Verify critical financial columns are non-null
        cols_to_check = [
            "revenue", "cogs", "gross_profit", "gross_margin_pct",
            "opex", "ebitda", "ebit", "ebitda_margin_pct",
            "rolling_3m_ebitda", "ttm_revenue", "ttm_ebitda",
            "monthly_debt_service", "dscr", "covenant_health_status"
        ]
        for col in cols_to_check:
            assert df[col].isna().sum() == 0, f"Found nulls in column {col}"

    def test_pnl_metric_accounting_identities(self, gold_pipeline_outputs):
        """Confirms accounting identities: Gross Profit, EBITDA, and EBIT."""
        df = gold_pipeline_outputs["feat_portfolio_covenant_health"]

        for _, row in df.iterrows():
            calc_gross_profit = round(row["revenue"] - row["cogs"], 2)
            assert abs(row["gross_profit"] - calc_gross_profit) < 0.05, (
                f"Gross profit mismatch for {row['entity_code']} {row['period_key']}: "
                f"expected {calc_gross_profit}, got {row['gross_profit']}"
            )

            calc_ebitda = round(row["gross_profit"] - row["opex"], 2)
            assert abs(row["ebitda"] - calc_ebitda) < 0.05, (
                f"EBITDA mismatch for {row['entity_code']} {row['period_key']}: "
                f"expected {calc_ebitda}, got {row['ebitda']}"
            )

            calc_ebit = round(row["ebitda"] - row["da"], 2)
            assert abs(row["ebit"] - calc_ebit) < 0.05, (
                f"EBIT mismatch for {row['entity_code']} {row['period_key']}: "
                f"expected {calc_ebit}, got {row['ebit']}"
            )

            if row["revenue"] > 0:
                calc_gm_pct = round((row["gross_profit"] / row["revenue"]) * 100.0, 2)
                assert abs(row["gross_margin_pct"] - calc_gm_pct) < 0.1, (
                    f"Gross Margin % mismatch for {row['entity_code']} {row['period_key']}"
                )

    def test_rolling_3m_ebitda_window_accuracy(self, gold_pipeline_outputs):
        """Validates 3-month rolling average EBITDA calculated by SQL window function."""
        df = gold_pipeline_outputs["feat_portfolio_covenant_health"]

        for ent in df["entity_code"].unique():
            ent_df = df[df["entity_code"] == ent].sort_values("period_key").reset_index(drop=True)
            for idx in range(len(ent_df)):
                # Window: 2 preceding and current row
                window_slice = ent_df.loc[max(0, idx - 2):idx, "ebitda"]
                expected_rolling = round(float(window_slice.mean()), 2)
                actual_rolling = round(float(ent_df.loc[idx, "rolling_3m_ebitda"]), 2)
                assert abs(expected_rolling - actual_rolling) < 0.05, (
                    f"Rolling 3M EBITDA error at {ent} {ent_df.loc[idx, 'period_key']}: "
                    f"expected {expected_rolling}, got {actual_rolling}"
                )

    def test_ttm_window_accuracy(self, gold_pipeline_outputs):
        """Validates Trailing Twelve Months (TTM) Revenue and EBITDA calculations."""
        df = gold_pipeline_outputs["feat_portfolio_covenant_health"]

        for ent in df["entity_code"].unique():
            ent_df = df[df["entity_code"] == ent].sort_values("period_key").reset_index(drop=True)
            for idx in range(len(ent_df)):
                window_slice_rev = ent_df.loc[max(0, idx - 11):idx, "revenue"]
                expected_ttm_rev = round(float(window_slice_rev.sum()), 2)
                actual_ttm_rev = round(float(ent_df.loc[idx, "ttm_revenue"]), 2)
                assert abs(expected_ttm_rev - actual_ttm_rev) < 0.05, (
                    f"TTM Revenue error at {ent} {ent_df.loc[idx, 'period_key']}: "
                    f"expected {expected_ttm_rev}, got {actual_ttm_rev}"
                )

                window_slice_ebitda = ent_df.loc[max(0, idx - 11):idx, "ebitda"]
                expected_ttm_ebitda = round(float(window_slice_ebitda.sum()), 2)
                actual_ttm_ebitda = round(float(ent_df.loc[idx, "ttm_ebitda"]), 2)
                assert abs(expected_ttm_ebitda - actual_ttm_ebitda) < 0.05, (
                    f"TTM EBITDA error at {ent} {ent_df.loc[idx, 'period_key']}: "
                    f"expected {expected_ttm_ebitda}, got {actual_ttm_ebitda}"
                )

    def test_pop_revenue_growth_accuracy(self, gold_pipeline_outputs):
        """Validates Period-over-Period (PoP) Revenue Growth window calculation."""
        df = gold_pipeline_outputs["feat_portfolio_covenant_health"]

        for ent in df["entity_code"].unique():
            ent_df = df[df["entity_code"] == ent].sort_values("period_key").reset_index(drop=True)
            for idx in range(1, len(ent_df)):
                prev_rev = ent_df.loc[idx - 1, "revenue"]
                curr_rev = ent_df.loc[idx, "revenue"]
                expected_growth = round(((curr_rev - prev_rev) / prev_rev) * 100.0, 2)
                actual_growth = round(float(ent_df.loc[idx, "pop_revenue_growth_pct"]), 2)
                assert abs(expected_growth - actual_growth) < 0.05, (
                    f"PoP Growth error at {ent} {ent_df.loc[idx, 'period_key']}: "
                    f"expected {expected_growth}, got {actual_growth}"
                )

    def test_debt_service_and_dscr_covenant_compliance(self, gold_pipeline_outputs):
        """Validates DSCR calculation, headroom, and intentional stress breach detection."""
        df = gold_pipeline_outputs["feat_portfolio_covenant_health"]

        # All monthly debt services must be strictly positive
        assert (df["monthly_debt_service"] > 0).all()

        # All DSCR ratios must be positive
        assert (df["dscr"] > 0).all()

        # Check covenant health statuses domain values
        valid_health_statuses = {"HEALTHY", "AT_RISK", "BREACH_ALERT"}
        assert set(df["covenant_health_status"]).issubset(valid_health_statuses)

        # Check intentional stress shock for DURA_US in months 18-21 (2025-06 to 2025-09)
        dura_df = df[df["entity_code"] == "DURA_US"].sort_values("period_key").reset_index(drop=True)
        # Months 18-21 (0-indexed: 17, 18, 19, 20)
        stress_months = dura_df.iloc[17:21]
        for _, row in stress_months.iterrows():
            assert row["dscr"] < row["covenant_min_dscr"], (
                f"Expected stress shock covenant breach in {row['period_key']}, but DSCR was {row['dscr']}"
            )
            assert row["covenant_health_status"] == "BREACH_ALERT"
            assert row["covenant_status"] == "BREACH"

    def test_powerbi_csv_export_format(self):
        """Validates that powerbi_portfolio_value_creation.csv exists and is well-formed."""
        csv_path = GOLD_DIR / "powerbi_portfolio_value_creation.csv"
        assert csv_path.exists(), f"Missing Power BI export: {csv_path}"

        df = pd.read_csv(csv_path)
        assert len(df) == 96
        assert "entity_code" in df.columns
        assert "period_key" in df.columns
        assert "dscr" in df.columns
        assert "covenant_health_status" in df.columns
        assert "rolling_3m_ebitda" in df.columns
        assert "ttm_revenue" in df.columns
        assert "pop_revenue_growth_pct" in df.columns
