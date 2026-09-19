"""Tests for Financial Invariants and Accounting Integrity.

Validates:
1. Double-entry general ledger balancing (Debits == Credits) per batch and in aggregate.
2. Canonical Chart of Accounts crosswalk mapping completeness across all ERP systems.
3. Monthly FX rate validity and USD identity.
4. Master debt facility terms and covenant limits.
5. Deterministic covenant stress shock validation for DURA_US in months 18-21.
"""

from pathlib import Path
import pandas as pd
import pytest


RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "01_raw"
SILVER_DIR = Path(__file__).resolve().parent.parent / "data" / "02_silver"
GOLD_DIR = Path(__file__).resolve().parent.parent / "data" / "03_gold"


@pytest.fixture(scope="module")
def raw_gl() -> pd.DataFrame:
    path = RAW_DIR / "raw_general_ledger_entries.csv"
    assert path.exists(), f"Raw GL file not found at {path}"
    return pd.read_csv(path)


@pytest.fixture(scope="module")
def raw_coa() -> pd.DataFrame:
    path = RAW_DIR / "raw_coa_crosswalk_mapping.csv"
    assert path.exists(), f"COA mapping file not found at {path}"
    return pd.read_csv(path)


@pytest.fixture(scope="module")
def raw_fx() -> pd.DataFrame:
    path = RAW_DIR / "raw_fx_rates_monthly.csv"
    assert path.exists(), f"FX rates file not found at {path}"
    return pd.read_csv(path)


@pytest.fixture(scope="module")
def raw_debt() -> pd.DataFrame:
    path = RAW_DIR / "raw_debt_covenants_master.csv"
    assert path.exists(), f"Debt covenants file not found at {path}"
    return pd.read_csv(path)


@pytest.fixture(scope="module")
def gold_covenant_health() -> pd.DataFrame:
    path = GOLD_DIR / "feat_portfolio_covenant_health.parquet"
    if not path.exists():
        from src.pipelines.gold_dimensional_modeling import GoldDimensionalModelingPipeline
        pipeline = GoldDimensionalModelingPipeline()
        models = pipeline.run()
        return models["feat_portfolio_covenant_health"]
    return pd.read_parquet(path)


@pytest.fixture(scope="module")
def silver_gl() -> pd.DataFrame:
    path = SILVER_DIR / "silver_general_ledger_harmonized.parquet"
    if not path.exists():
        from src.pipelines.silver_harmonization import SilverHarmonizationPipeline
        pipeline = SilverHarmonizationPipeline()
        gl, _ = pipeline.run()
        return gl
    return pd.read_parquet(path)


class TestFinancialInvariants:
    """Core financial accounting invariants."""

    def test_gross_profit_le_revenue_invariant(self, gold_covenant_health: pd.DataFrame):
        """Gross Profit must never exceed Revenue for any period (Gross Profit <= Revenue)."""
        for _, row in gold_covenant_health.iterrows():
            rev = float(row["revenue"])
            gp = float(row["gross_profit"])
            assert gp <= rev + 1e-5, (
                f"Gross profit invariant violated for {row['entity_code']} {row['period_key']}: "
                f"Gross Profit ({gp}) > Revenue ({rev})"
            )

    def test_ebitda_consistency_formula(self, gold_covenant_health: pd.DataFrame):
        """EBITDA must exactly equal Revenue - COGS - OPEX across all Gold rows."""
        for _, row in gold_covenant_health.iterrows():
            rev = float(row["revenue"])
            cogs = float(row["cogs"])
            opex = float(row["opex"])
            ebitda = float(row["ebitda"])
            expected_ebitda = round(rev - cogs - opex, 2)
            assert abs(ebitda - expected_ebitda) < 0.05, (
                f"EBITDA formula mismatch for {row['entity_code']} {row['period_key']}: "
                f"expected {expected_ebitda}, got {ebitda}"
            )

    def test_covenant_metrics_non_null(self, gold_covenant_health: pd.DataFrame):
        """Ensures no null values exist in dscr, covenant_min_dscr, or covenant_health_status."""
        critical_cols = ["dscr", "covenant_min_dscr", "covenant_health_status", "monthly_debt_service"]
        for col in critical_cols:
            null_count = int(gold_covenant_health[col].isna().sum())
            assert null_count == 0, f"Found {null_count} nulls in critical column: {col}"

    def test_silver_lineage_completeness(self, silver_gl: pd.DataFrame):
        """Verifies 100% of rows in Silver maintain valid _lineage_hash and _source_erp values."""
        assert "_lineage_hash" in silver_gl.columns
        assert "_source_erp" in silver_gl.columns
        assert silver_gl["_lineage_hash"].isna().sum() == 0
        assert (silver_gl["_lineage_hash"].str.len() == 64).all()  # Valid SHA-256 hex digest length
        assert silver_gl["_source_erp"].isna().sum() == 0
        assert (silver_gl["_source_erp"] != "").all()

    def test_raw_gl_aggregate_double_entry(self, raw_gl: pd.DataFrame):
        """Total Debits must exactly equal Total Credits across the entire General Ledger."""
        debits = round(raw_gl["debit_amount"].astype(float).sum(), 2)
        credits = round(raw_gl["credit_amount"].astype(float).sum(), 2)
        assert debits == credits, f"Ledger imbalance: Debits ({debits}) != Credits ({credits})"
        assert debits > 0, "Ledger has zero debits"

    def test_raw_gl_batch_level_balancing(self, raw_gl: pd.DataFrame):
        """Every individual journal entry batch must balance Debits == Credits."""
        batch_sums = raw_gl.groupby("batch_id").agg({
            "debit_amount": lambda x: round(x.astype(float).sum(), 2),
            "credit_amount": lambda x: round(x.astype(float).sum(), 2),
        })
        imbalanced = batch_sums[batch_sums["debit_amount"] != batch_sums["credit_amount"]]
        assert len(imbalanced) == 0, f"Found {len(imbalanced)} imbalanced batches: {imbalanced.head()}"

    def test_coa_crosswalk_completeness(self, raw_coa: pd.DataFrame):
        """All 4 ERP systems must map to all required canonical P&L accounts."""
        expected_systems = {"SAP", "NetSuite", "Dynamics365", "QuickBooks"}
        actual_systems = set(raw_coa["source_system"].unique())
        assert expected_systems == actual_systems, f"Mismatch in source systems: {actual_systems}"

        required_canonicals = {"REV_CORE", "COGS_DIRECT", "OPEX_SGA", "DA_DEPR", "FIN_INT_EXP"}
        for sys in expected_systems:
            sys_canonicals = set(raw_coa[raw_coa["source_system"] == sys]["canonical_account_code"].unique())
            missing = required_canonicals - sys_canonicals
            assert not missing, f"Source ERP {sys} is missing canonical mappings: {missing}"

    def test_fx_rates_continuity_and_identities(self, raw_fx: pd.DataFrame):
        """FX rates must cover 24 periods for USD, GBP, EUR with valid positive rates."""
        periods = raw_fx["period"].unique()
        assert len(periods) == 24, f"Expected 24 monthly periods, found {len(periods)}"

        # USD to USD identity check
        usd_rows = raw_fx[(raw_fx["from_currency"] == "USD") & (raw_fx["to_currency"] == "USD")]
        assert len(usd_rows) == 24
        assert (usd_rows["spot_rate"].astype(float) == 1.0).all()
        assert (usd_rows["avg_rate"].astype(float) == 1.0).all()

        # Rates must be positive and within reasonable FX corridors
        for curr in ["GBP", "EUR"]:
            curr_rows = raw_fx[(raw_fx["from_currency"] == curr) & (raw_fx["to_currency"] == "USD")]
            assert len(curr_rows) == 24
            assert (curr_rows["spot_rate"].astype(float) > 0.8).all()
            assert (curr_rows["spot_rate"].astype(float) < 2.0).all()

    def test_debt_facility_covenant_parameters(self, raw_debt: pd.DataFrame):
        """Debt facilities must have positive origination, interest, and DSCR covenants."""
        assert len(raw_debt) == 4, f"Expected 4 entities, found {len(raw_debt)}"
        for _, row in raw_debt.iterrows():
            principal = float(row["principal_origination"])
            interest = float(row["annual_interest_rate"])
            amort = float(row["monthly_amortization"])
            min_dscr = float(row["covenant_min_dscr"])

            assert principal >= 10_000_000.0, f"Principal too low: {principal}"
            assert 0.04 <= interest <= 0.12, f"Interest rate out of bounds: {interest}"
            assert amort > 0, f"Monthly amortization must be positive: {amort}"
            assert 1.15 <= min_dscr <= 1.40, f"Min DSCR out of standard PE bounds: {min_dscr}"

    def test_intentional_stress_shock_covenant_breach(self, raw_gl: pd.DataFrame, raw_coa: pd.DataFrame, raw_debt: pd.DataFrame):
        """Confirms DURA_US triggers covenant breach (DSCR < 1.25x) during months 18-21."""
        # Join GL with COA
        gl_joined = raw_gl[raw_gl["entity_id"] == "DURA_US"].merge(
            raw_coa[["source_system", "legacy_account_id", "canonical_account_code"]],
            left_on=["source_system", "legacy_gl_code"],
            right_on=["source_system", "legacy_account_id"]
        )

        # Get DURA debt terms
        dura_debt = raw_debt[raw_debt["entity_id"] == "DURA_US"].iloc[0]
        min_dscr = float(dura_debt["covenant_min_dscr"])
        monthly_amort = float(dura_debt["monthly_amortization"])

        # Group by period and canonical account
        periods = sorted(gl_joined["period"].unique())
        for idx, p in enumerate(periods, start=1):
            p_data = gl_joined[gl_joined["period"] == p]

            rev = p_data[p_data["canonical_account_code"] == "REV_CORE"]["credit_amount"].astype(float).sum()
            cogs = p_data[p_data["canonical_account_code"] == "COGS_DIRECT"]["debit_amount"].astype(float).sum()
            opex = p_data[p_data["canonical_account_code"] == "OPEX_SGA"]["debit_amount"].astype(float).sum()
            interest = p_data[p_data["canonical_account_code"] == "FIN_INT_EXP"]["debit_amount"].astype(float).sum()

            ebitda = rev - cogs - opex
            debt_service = interest + monthly_amort
            dscr = ebitda / debt_service

            if idx in [18, 19, 20, 21]:
                # Stress period assertion: DSCR must breach covenant
                assert dscr < min_dscr, f"Expected covenant breach in period {p} (month {idx}), but DSCR was {dscr:.2f} >= {min_dscr}"
            else:
                # Normal operational periods must be compliant
                assert dscr >= min_dscr, f"Unexpected covenant breach in period {p} (month {idx}): DSCR {dscr:.2f} < {min_dscr}"
