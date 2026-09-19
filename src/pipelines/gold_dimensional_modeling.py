"""Gold Dimensional Modeling Pipeline.

Builds dimensional star-schema models and financial analytics marts:
- Dimensions: dim_entity, dim_period, dim_account
- Facts: fact_financial_monthly, fact_covenant_health
- Key Metrics: EBITDA (USD), Net Debt Service, DSCR, Headroom, Covenant Breach Flags.
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Dict, Optional, Tuple

# Ensure project root is in sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.pipelines.silver_harmonization import SilverHarmonizationPipeline


GOLD_DIR = PROJECT_ROOT / "data" / "03_gold"


class GoldDimensionalModelingPipeline:
    """Orchestrates Silver-to-Gold star schema modeling and executive metrics generation."""

    def __init__(self, gold_dir: Optional[Path] = None, silver_pipeline: Optional[SilverHarmonizationPipeline] = None):
        self.gold_dir = gold_dir or GOLD_DIR
        self.silver_pipeline = silver_pipeline or SilverHarmonizationPipeline()

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
