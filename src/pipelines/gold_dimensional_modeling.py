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
                ROUND(w.interest_expense + CAST(COALESCE(d.monthly_amortization_usd, d.monthly_amortization) AS DOUBLE), 2) AS monthly_debt_service,
                -- DSCR = EBITDA / Monthly Debt Service
                ROUND(w.ebitda / NULLIF(w.interest_expense + CAST(COALESCE(d.monthly_amortization_usd, d.monthly_amortization) AS DOUBLE), 0), 3) AS dscr,
                -- DSCR Headroom = DSCR - covenant_min_dscr
                ROUND((w.ebitda / NULLIF(w.interest_expense + CAST(COALESCE(d.monthly_amortization_usd, d.monthly_amortization) AS DOUBLE), 0)) - CAST(d.covenant_min_dscr AS DOUBLE), 3) AS dscr_headroom,
                -- Dynamic Covenant Health Classification
                CASE 
                    WHEN (w.ebitda / NULLIF(w.interest_expense + CAST(COALESCE(d.monthly_amortization_usd, d.monthly_amortization) AS DOUBLE), 0)) >= CAST(d.covenant_min_dscr AS DOUBLE) * 1.10 THEN 'HEALTHY'
                    WHEN (w.ebitda / NULLIF(w.interest_expense + CAST(COALESCE(d.monthly_amortization_usd, d.monthly_amortization) AS DOUBLE), 0)) >= CAST(d.covenant_min_dscr AS DOUBLE) THEN 'AT_RISK'
                    ELSE 'BREACH_ALERT'
                END AS covenant_health_status
            FROM windowed_metrics w
            JOIN debt_master d ON w.entity_code = d.entity_id
        )
        SELECT * FROM covenant_metrics
        ORDER BY entity_code, period_key;
        """
        df = con.execute(query).df()
        con.close()

        # Add backward-compatible schema aliases for legacy tests and downstream consumers
        df["entity_id"] = df["entity_code"]
        df["period"] = df["period_key"]
        df["period_index"] = df.groupby("entity_code").cumcount() + 1
        df["revenue_usd"] = df["revenue"]
        df["cogs_usd"] = df["cogs"]
        df["gross_profit_usd"] = df["gross_profit"]
        df["opex_usd"] = df["opex"]
        df["ebitda_usd"] = df["ebitda"]
        df["da_usd"] = df["da"]
        df["ebit_usd"] = df["ebit"]
        df["interest_usd"] = df["interest_expense"]
        df["debt_service_usd"] = df["monthly_debt_service"]
        df["monthly_amortization_usd"] = df["monthly_amortization"]

        # Map covenant_health_status to legacy covenant_status
        status_map = {
            "HEALTHY": "COMPLIANT",
            "AT_RISK": "WARNING",
            "BREACH_ALERT": "BREACH"
        }
        df["covenant_status"] = df["covenant_health_status"].map(status_map)
        df["is_breach"] = (df["covenant_health_status"] == "BREACH_ALERT").astype(int)

        logger.info(f"SQL execution complete: {len(df):,} analytical rows produced across {df['entity_code'].nunique()} entities.")
        return df

    def build_dim_entity(self, debt_df: pd.DataFrame) -> pd.DataFrame:
        """Constructs dim_entity containing entity metadata and debt covenant parameters."""
        cols = [
            "entity_id", "entity_name", "facility_name", "currency",
            "principal_origination", "annual_interest_rate",
            "monthly_amortization", "covenant_min_dscr", "covenant_max_leverage_ratio",
            "origination_date", "maturity_date"
        ]
        available_cols = [c for c in cols if c in debt_df.columns]
        dim_entity = debt_df[available_cols].drop_duplicates().copy()
        if "currency" in dim_entity.columns:
            dim_entity.rename(columns={"currency": "functional_currency"}, inplace=True)
        if "principal_usd" not in dim_entity.columns and "principal_origination" in dim_entity.columns:
            dim_entity["principal_usd"] = dim_entity["principal_origination"]
        if "monthly_amortization_usd" not in dim_entity.columns and "monthly_amortization" in dim_entity.columns:
            dim_entity["monthly_amortization_usd"] = dim_entity["monthly_amortization"]
        return dim_entity

    def build_dim_period(self, gl_silver: pd.DataFrame) -> pd.DataFrame:
        """Constructs dim_period with calendar dimensions."""
        period_col = "period_key" if "period_key" in gl_silver.columns else "period"
        periods = sorted(gl_silver[period_col].unique())
        rows = []
        for idx, p in enumerate(periods, start=1):
            y, m = map(int, str(p).split("-"))
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
        acc_col = "canonical_code" if "canonical_code" in gl_silver.columns else "canonical_account_code"
        cols = [acc_col, "account_category", "financial_statement", "normal_balance"]
        avail = [c for c in cols if c in gl_silver.columns]
        df = gl_silver[avail].drop_duplicates().sort_values(acc_col).reset_index(drop=True)
        if "canonical_account_code" not in df.columns:
            df["canonical_account_code"] = df[acc_col]
        return df

    def build_fact_financial_monthly(self, gl_silver: pd.DataFrame) -> pd.DataFrame:
        """Aggregates monthly financial figures by entity and canonical account."""
        ent_col = "entity_code" if "entity_code" in gl_silver.columns else "entity_id"
        period_col = "period_key" if "period_key" in gl_silver.columns else "period"
        acc_col = "canonical_code" if "canonical_code" in gl_silver.columns else "canonical_account_code"

        agg = gl_silver.groupby(
            [ent_col, period_col, acc_col, "currency"],
            as_index=False
        ).agg({
            "debit_amount": "sum",
            "credit_amount": "sum",
            "net_amount": "sum",
            "debit_usd": "sum",
            "credit_usd": "sum",
            "net_amount_usd": "sum",
        })
        agg["entity_id"] = agg[ent_col]
        agg["period"] = agg[period_col]
        agg["canonical_account_code"] = agg[acc_col]
        return agg

    def export_powerbi_csv(self, covenant_df: pd.DataFrame) -> Path:
        """Exports an executive flat mart for Power BI and web consumption."""
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        out_csv = self.gold_dir / "powerbi_portfolio_value_creation.csv"

        export_cols = [
            "entity_code", "entity_name", "facility_name", "period_key",
            "revenue", "cogs", "gross_profit", "gross_margin_pct",
            "opex", "ebitda", "ebit", "ebitda_margin_pct",
            "da", "interest_expense", "monthly_debt_service",
            "dscr", "covenant_min_dscr", "dscr_headroom",
            "covenant_health_status", "rolling_3m_ebitda",
            "ttm_revenue", "ttm_ebitda", "pop_revenue_growth_pct"
        ]
        available = [c for c in export_cols if c in covenant_df.columns]
        covenant_df[available].to_csv(out_csv, index=False)
        logger.info(f"Exported Power BI executive flat mart: {out_csv} ({len(covenant_df)} records)")
        return out_csv

    def run(self) -> Dict[str, pd.DataFrame]:
        """Executes full Gold dimensional modeling and persists Parquet datasets."""
        logger.info("=" * 70)
        logger.info("Executing Phase 3: Gold Analytical Mart, SQL Window Functions & Covenant Modeling")
        logger.info("=" * 70)

        self.gold_dir.mkdir(parents=True, exist_ok=True)
        gl_silver, debt_silver = self.silver_pipeline.run()

        # Execute DuckDB SQL Analytical Mart
        covenant_health_df = self.execute_covenant_health_sql(gl_silver, debt_silver)

        dim_entity = self.build_dim_entity(debt_silver)
        dim_period = self.build_dim_period(gl_silver)
        dim_account = self.build_dim_account(gl_silver)
        fact_financial = self.build_fact_financial_monthly(gl_silver)

        models = {
            "dim_entity": dim_entity,
            "dim_period": dim_period,
            "dim_account": dim_account,
            "fact_financial_monthly": fact_financial,
            "fact_covenant_health": covenant_health_df,
            "feat_portfolio_covenant_health": covenant_health_df,
        }

        for name, df in models.items():
            out_parquet = self.gold_dir / f"{name}.parquet"
            df.to_parquet(out_parquet, index=False)
            logger.info(f"Persisted Gold Mart: {out_parquet.name} ({len(df)} records)")

        # Export Power BI Flat Mart
        self.export_powerbi_csv(covenant_health_df)

        logger.info("=" * 70)
        logger.info(f"Gold Dimensional Modeling Succeeded: {len(covenant_health_df):,} covenant health rows.")
        logger.info("=" * 70)

        return models


if __name__ == "__main__":
    pipeline = GoldDimensionalModelingPipeline()
    models = pipeline.run()
    print("Gold Dimensional Modeling Summary:")
    for name, df in models.items():
        print(f"  - {name}: {len(df)} records.")
