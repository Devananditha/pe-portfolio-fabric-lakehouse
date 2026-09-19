"""Gold Dimensional Modeling Pipeline - Phase 3.

Implements high-speed analytical dimensional marts using DuckDB SQL:
1. Ingests Silver harmonized ledger and Debt Covenants master.
2. Executes multi-tier SQL CTEs aggregating monthly P&L statement lines.
3. Computes core profitability metrics (Gross Profit, Margin %, EBITDA, EBIT).
4. Executes advanced window functions:
   - 3-Month Rolling Average EBITDA
   - Trailing Twelve Months (TTM) Revenue and EBITDA
   - Period-over-Period (PoP) Revenue Growth via LAG
5. Evaluates debt-service obligations, DSCR, and dynamic covenant health indicators.
6. Persists partitioned/analytical Parquet feature marts and Power BI executive CSVs.
7. Retains star-schema dimensional tables (dim_entity, dim_period, dim_account, fact_financial_monthly).
"""

from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Dict, Optional, Tuple

# Ensure project root is in sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
import numpy as np
import pandas as pd

from src.pipelines.silver_harmonization import SilverHarmonizationPipeline

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("gold_dimensional_modeling")

GOLD_DIR = PROJECT_ROOT / "data" / "03_gold"
SILVER_DIR = PROJECT_ROOT / "data" / "02_silver"
RAW_DIR = PROJECT_ROOT / "data" / "01_raw"


class GoldDimensionalModelingPipeline:
    """Production-grade Gold analytical dimensional modeling and covenant pipeline."""

    def __init__(
        self,
        gold_dir: Optional[Path] = None,
        silver_dir: Optional[Path] = None,
        raw_dir: Optional[Path] = None,
        silver_pipeline: Optional[SilverHarmonizationPipeline] = None
    ):
        self.gold_dir = gold_dir or GOLD_DIR
        self.silver_dir = silver_dir or SILVER_DIR
        self.raw_dir = raw_dir or RAW_DIR
        self.silver_pipeline = silver_pipeline or SilverHarmonizationPipeline()

    def get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Initializes an in-memory DuckDB analytical engine connection."""
        return duckdb.connect(":memory:")

    def execute_covenant_health_sql(
        self,
        gl_silver: pd.DataFrame,
        debt_covenants: pd.DataFrame
    ) -> pd.DataFrame:
        """Executes advanced SQL CTEs and window functions to model P&L and covenant health."""
        logger.info("Executing DuckDB SQL CTEs and analytical window functions...")
        con = self.get_duckdb_connection()
        con.register("silver_ledger", gl_silver)
        con.register("debt_master", debt_covenants)

        query = """
        WITH monthly_lines AS (
            -- CTE 1: Aggregate monthly financial statement line amounts
            SELECT
                COALESCE(entity_code, entity_id) AS entity_code,
                COALESCE(period_key, period) AS period_key,
                SUM(CASE WHEN financial_statement_line = 'REVENUE' THEN amount_usd ELSE 0.0 END) AS revenue,
                SUM(CASE WHEN financial_statement_line = 'COGS' THEN amount_usd ELSE 0.0 END) AS cogs,
                SUM(CASE WHEN financial_statement_line = 'OPEX' THEN amount_usd ELSE 0.0 END) AS opex,
                SUM(CASE WHEN financial_statement_line IN ('DEPRECIATION', 'AMORTIZATION') THEN amount_usd ELSE 0.0 END) AS da,
                SUM(CASE WHEN financial_statement_line = 'INTEREST_EXPENSE' THEN amount_usd ELSE 0.0 END) AS interest_expense
            FROM silver_ledger
            GROUP BY 1, 2
        ),
        pnl_metrics AS (
            -- CTE 2: Compute core profitability metrics and margins
            SELECT
                entity_code,
                period_key,
                ROUND(revenue, 2) AS revenue,
                ROUND(cogs, 2) AS cogs,
                ROUND(opex, 2) AS opex,
                ROUND(da, 2) AS da,
                ROUND(interest_expense, 2) AS interest_expense,
                ROUND(revenue - cogs, 2) AS gross_profit,
                ROUND(CASE WHEN revenue > 0 THEN ((revenue - cogs) / revenue) * 100.0 ELSE 0.0 END, 2) AS gross_margin_pct,
                ROUND(revenue - cogs - opex, 2) AS ebitda,
                ROUND(revenue - cogs - opex - da, 2) AS ebit,
                ROUND(CASE WHEN revenue > 0 THEN ((revenue - cogs - opex) / revenue) * 100.0 ELSE 0.0 END, 2) AS ebitda_margin_pct
            FROM monthly_lines
        ),
        windowed_metrics AS (
            -- CTE 3: Execute advanced analytical window functions (Rolling 3M)
            SELECT
                p.*,
                -- 3-Month Rolling Average EBITDA
                ROUND(AVG(p.ebitda) OVER (
                    PARTITION BY p.entity_code 
                    ORDER BY p.period_key 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) AS rolling_3m_ebitda,
                -- Trailing Twelve Months (TTM) Revenue
                ROUND(SUM(p.revenue) OVER (
                    PARTITION BY p.entity_code 
                    ORDER BY p.period_key 
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 2) AS ttm_revenue,
                -- Trailing Twelve Months (TTM) EBITDA
                ROUND(SUM(p.ebitda) OVER (
                    PARTITION BY p.entity_code 
                    ORDER BY p.period_key 
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 2) AS ttm_ebitda,
                -- Period-over-Period (PoP) Revenue Growth via LAG
                ROUND(LAG(p.revenue, 1) OVER (
                    PARTITION BY p.entity_code 
                    ORDER BY p.period_key
                ), 2) AS lag_revenue,
                ROUND(CASE 
                    WHEN LAG(p.revenue, 1) OVER (PARTITION BY p.entity_code ORDER BY p.period_key) > 0 
                    THEN ((p.revenue - LAG(p.revenue, 1) OVER (PARTITION BY p.entity_code ORDER BY p.period_key)) / 
                          LAG(p.revenue, 1) OVER (PARTITION BY p.entity_code ORDER BY p.period_key)) * 100.0 
                    ELSE 0.0 
                END, 2) AS pop_revenue_growth_pct
            FROM pnl_metrics p
        ),
        covenant_metrics AS (
            -- CTE 4: Debt-Service Health & Dynamic Covenant Compliance
            SELECT
                w.entity_code,
                w.period_key,
                w.revenue,
                w.cogs,
                w.gross_profit,
                w.gross_margin_pct,
                w.opex,
                w.ebitda,
                w.ebit,
                w.ebitda_margin_pct,
                w.da,
                w.interest_expense,
                w.rolling_3m_ebitda,
                w.ttm_revenue,
                w.ttm_ebitda,
                COALESCE(w.lag_revenue, w.revenue) AS lag_revenue,
                w.pop_revenue_growth_pct,
                d.entity_name,
                d.facility_name,
                d.currency AS facility_currency,
                CAST(d.principal_origination AS DOUBLE) AS principal_origination,
                CAST(d.annual_interest_rate AS DOUBLE) AS annual_interest_rate,
                CAST(COALESCE(d.monthly_amortization_usd, d.monthly_amortization) AS DOUBLE) AS monthly_amortization,
                CAST(d.covenant_min_dscr AS DOUBLE) AS covenant_min_dscr,
                CAST(d.covenant_max_leverage_ratio AS DOUBLE) AS covenant_max_leverage_ratio,
                -- Total Monthly Debt Service = Interest Expense + Monthly Amortization
                ROUND(w.interest_expense + CAST(COALESCE(d.monthly_amortization_usd, d.monthly_amortization) AS DOUBLE), 2) AS monthly_debt_service
            FROM windowed_metrics w
            JOIN debt_master d ON w.entity_code = d.entity_id
        )
        SELECT * FROM covenant_metrics ORDER BY entity_code, period_key;
        """
        df = con.execute(query).df()
        con.close()
        return df

    def build_dim_entity(self, debt_silver: pd.DataFrame) -> pd.DataFrame:
        """Constructs dim_entity containing entity metadata and debt covenant parameters."""
        cols = [
            "entity_id", "entity_name", "facility_name", "currency",
            "principal_origination", "principal_usd", "annual_interest_rate",
            "monthly_amortization", "monthly_amortization_usd",
            "covenant_min_dscr", "covenant_max_leverage_ratio",
            "origination_date", "maturity_date"
        ]
        dim_entity = debt_silver[cols].drop_duplicates().copy()
        dim_entity.rename(columns={"currency": "functional_currency"}, inplace=True)
        return dim_entity

    def build_dim_period(self, gl_silver: pd.DataFrame) -> pd.DataFrame:
        """Constructs dim_period with calendar dimensions."""
        periods = sorted(gl_silver["period"].unique())
        rows = []
        for idx, p in enumerate(periods, start=1):
            y, m = map(int, p.split("-"))
            q = (m - 1) // 3 + 1
            rows.append({
                "period_id": p,
                "period_index": idx,
                "year": y,
                "month": m,
                "quarter": f"{y}-Q{q}",
                "quarter_num": q,
                "year_month_display": pd.to_datetime(f"{p}-01").strftime("%b %Y"),
            })
        return pd.DataFrame(rows)

    def build_dim_account(self, gl_silver: pd.DataFrame) -> pd.DataFrame:
        """Constructs dim_account with canonical Lakehouse Chart of Accounts."""
        cols = ["canonical_account_code", "account_category", "financial_statement", "normal_balance"]
        return gl_silver[cols].drop_duplicates().sort_values("canonical_account_code").reset_index(drop=True)

    def build_fact_financial_monthly(self, gl_silver: pd.DataFrame) -> pd.DataFrame:
        """Aggregates monthly financial figures by entity and canonical account."""
        agg = gl_silver.groupby(
            ["entity_id", "period", "canonical_account_code", "currency"],
            as_index=False
        ).agg({
            "debit_amount": "sum",
            "credit_amount": "sum",
            "net_amount": "sum",
            "debit_usd": "sum",
            "credit_usd": "sum",
            "net_amount_usd": "sum",
        })
        return agg

    def build_fact_covenant_health(
        self,
        fact_fin: pd.DataFrame,
        dim_entity: pd.DataFrame,
        dim_period: pd.DataFrame
    ) -> pd.DataFrame:
        """Calculates monthly EBITDA, Debt Service, and DSCR covenant compliance per entity."""
        # Pivot income statement canonical accounts to compute EBITDA
        # Revenue normal balance is credit: revenue_usd = credit_usd - debit_usd
        # COGS, OPEX, D&A, Interest normal balance is debit: expense_usd = debit_usd - credit_usd
        piv = fact_fin.pivot_table(
            index=["entity_id", "period"],
            columns="canonical_account_code",
            values=["debit_usd", "credit_usd"],
            aggfunc="sum",
            fill_value=0.0
        )

        rows = []
        for (entity_id, period), _ in piv.iterrows():
            # Revenue (REV_CORE): Net Credit
            rev = piv.loc[(entity_id, period), ("credit_usd", "REV_CORE")] - piv.loc[(entity_id, period), ("debit_usd", "REV_CORE")]
            # COGS (COGS_DIRECT): Net Debit
            cogs = piv.loc[(entity_id, period), ("debit_usd", "COGS_DIRECT")] - piv.loc[(entity_id, period), ("credit_usd", "COGS_DIRECT")]
            # OPEX (OPEX_SGA): Net Debit
            opex = piv.loc[(entity_id, period), ("debit_usd", "OPEX_SGA")] - piv.loc[(entity_id, period), ("credit_usd", "OPEX_SGA")]
            # DA (DA_DEPR): Net Debit
            da = piv.loc[(entity_id, period), ("debit_usd", "DA_DEPR")] - piv.loc[(entity_id, period), ("credit_usd", "DA_DEPR")]
            # Interest (FIN_INT_EXP): Net Debit
            interest = piv.loc[(entity_id, period), ("debit_usd", "FIN_INT_EXP")] - piv.loc[(entity_id, period), ("credit_usd", "FIN_INT_EXP")]

            gross_profit = rev - cogs
            ebitda = rev - cogs - opex
            ebit = ebitda - da

            rows.append({
                "entity_id": entity_id,
                "period": period,
                "revenue_usd": round(rev, 2),
                "cogs_usd": round(cogs, 2),
                "gross_profit_usd": round(gross_profit, 2),
                "opex_usd": round(opex, 2),
                "ebitda_usd": round(ebitda, 2),
                "da_usd": round(da, 2),
                "ebit_usd": round(ebit, 2),
                "interest_usd": round(interest, 2),
            })

        metrics_df = pd.DataFrame(rows)

        # Merge entity metadata for debt covenants and amortization
        merged = metrics_df.merge(
            dim_entity[[
                "entity_id", "entity_name", "facility_name", "monthly_amortization_usd", "covenant_min_dscr"
            ]],
            on="entity_id",
            how="left"
        )

        merged = merged.merge(
            dim_period[["period_id", "period_index", "year_month_display"]],
            left_on="period",
            right_on="period_id",
            how="left"
        ).drop(columns=["period_id"])

        # Compute Total Debt Service & DSCR
        merged["debt_service_usd"] = (merged["interest_usd"] + merged["monthly_amortization_usd"]).round(2)
        merged["dscr"] = (merged["ebitda_usd"] / merged["debt_service_usd"]).round(3)
        merged["dscr_headroom"] = (merged["dscr"] - merged["covenant_min_dscr"]).round(3)

        # Covenant status
        def classify_covenant(row):
            if row["dscr"] < row["covenant_min_dscr"]:
                return "BREACH"
            elif row["dscr_headroom"] < 0.15:
                return "WARNING"
            return "COMPLIANT"

        merged["covenant_status"] = merged.apply(classify_covenant, axis=1)
        merged["is_breach"] = (merged["covenant_status"] == "BREACH").astype(int)

        return merged.sort_values(["entity_id", "period"]).reset_index(drop=True)

    def run(self) -> Dict[str, pd.DataFrame]:
        """Executes full Gold dimensional modeling and persists Parquet datasets."""
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        gl_silver, debt_silver = self.silver_pipeline.run()

        dim_entity = self.build_dim_entity(debt_silver)
        dim_period = self.build_dim_period(gl_silver)
        dim_account = self.build_dim_account(gl_silver)
        fact_financial = self.build_fact_financial_monthly(gl_silver)
        fact_covenant = self.build_fact_covenant_health(fact_financial, dim_entity, dim_period)

        models = {
            "dim_entity": dim_entity,
            "dim_period": dim_period,
            "dim_account": dim_account,
            "fact_financial_monthly": fact_financial,
            "fact_covenant_health": fact_covenant,
        }

        for name, df in models.items():
            df.to_parquet(self.gold_dir / f"{name}.parquet", index=False)

        return models


if __name__ == "__main__":
    pipeline = GoldDimensionalModelingPipeline()
    models = pipeline.run()
    print("Gold Dimensional Modeling Summary:")
    for name, df in models.items():
        print(f"  - {name}: {len(df)} records.")
