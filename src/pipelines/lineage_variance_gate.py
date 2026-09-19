"""Lineage & Financial Variance Quality Gate.

Validates end-to-end data lineage across Medallion layers:
1. Row count completeness and record lineage.
2. Invariant conservation: Exact sum(debits) == sum(credits) in Raw & Silver.
3. Entity and Period continuity (4 entities x 24 periods = 96 monthly partitions).
4. Covenant stress trigger verification: Confirms DURA_US breaches DSCR in months 18-21,
   and verifies zero unexpected breaches across other entities/periods.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any, Dict, Optional

# Ensure project root is in sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.pipelines.bronze_ingestion import BronzeIngestionPipeline
from src.pipelines.gold_dimensional_modeling import GoldDimensionalModelingPipeline
from src.pipelines.silver_harmonization import SilverHarmonizationPipeline


REPORTS_DIR = PROJECT_ROOT / "reports"
GOLD_DIR = PROJECT_ROOT / "data" / "03_gold"

# Period-over-period variance drift threshold (±15%)
VARIANCE_THRESHOLD = 0.15

# Catalog of documented operational events explaining variance swings
DOCUMENTED_EVENTS: Dict[Tuple[str, int], str] = {
    ("DURA_US", 18): "DURA_US operational stress onset: supply chain disruption and customer contract renegotiation causing severe revenue and EBITDA compression.",
    ("DURA_US", 19): "DURA_US stress continuation: operational stabilization efforts.",
    ("DURA_US", 20): "DURA_US stress continuation: margin compression under revised pricing.",
    ("DURA_US", 21): "DURA_US stress trough: final month of stressed covenant compliance.",
    ("DURA_US", 22): "DURA_US operational turnaround and contract rebound: revenue normalization and EBITDA surge back to baseline.",
}


class LineageVarianceGate:
    """Automated quality gate for lineage and financial accounting invariants."""

    def __init__(
        self,
        reports_dir: Optional[Path] = None,
        gold_dir: Optional[Path] = None,
        gold_pipeline: Optional[GoldDimensionalModelingPipeline] = None
    ):
        self.reports_dir = reports_dir or REPORTS_DIR
        self.gold_dir = gold_dir or GOLD_DIR
        self.gold_pipeline = gold_pipeline or GoldDimensionalModelingPipeline()

    def evaluate_gate(self) -> Dict[str, Any]:
        """Runs comprehensive checks across Raw, Silver, and Gold layers."""
        # Execute / retrieve layers
        bronze_tables = self.gold_pipeline.silver_pipeline.bronze_pipeline.run()
        raw_gl = bronze_tables["general_ledger"]
        raw_coa = bronze_tables["coa_crosswalk"]
        raw_fx = bronze_tables["fx_rates"]
        raw_debt = bronze_tables["debt_covenants"]

        silver_gl, silver_debt = self.gold_pipeline.silver_pipeline.run()
        gold_models = self.gold_pipeline.run()
        covenant_health = gold_models["fact_covenant_health"]

        results: Dict[str, Any] = {
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            "status": "PASSED",
            "checks": {},
            "metrics": {},
        }

        # Check 1: Record Count Lineage (Raw GL -> Silver GL)
        raw_count = int(len(raw_gl))
        silver_count = int(len(silver_gl))
        count_variance = silver_count - raw_count
        results["checks"]["record_count_lineage"] = {
            "raw_gl_count": raw_count,
            "silver_gl_count": silver_count,
            "variance": count_variance,
            "passed": bool(count_variance == 0),
        }
        if count_variance != 0:
            results["status"] = "FAILED"

        # Check 2: Double-Entry Balancing Invariant in Raw GL
        raw_debit_sum = float(pd.to_numeric(raw_gl["debit_amount"]).sum())
        raw_credit_sum = float(pd.to_numeric(raw_gl["credit_amount"]).sum())
        raw_diff = round(raw_debit_sum - raw_credit_sum, 4)
        results["checks"]["raw_double_entry_balanced"] = {
            "total_debits": float(raw_debit_sum),
            "total_credits": float(raw_credit_sum),
            "net_difference": float(raw_diff),
            "passed": bool(raw_diff == 0.0),
        }
        if raw_diff != 0.0:
            results["status"] = "FAILED"

        # Check 3: Double-Entry Balancing in Silver (Local Currency)
        silver_debit_sum = float(silver_gl["debit_amount"].sum())
        silver_credit_sum = float(silver_gl["credit_amount"].sum())
        silver_diff = round(silver_debit_sum - silver_credit_sum, 4)
        results["checks"]["silver_double_entry_balanced"] = {
            "total_debits": float(silver_debit_sum),
            "total_credits": float(silver_credit_sum),
            "net_difference": float(silver_diff),
            "passed": bool(silver_diff == 0.0),
        }
        if silver_diff != 0.0:
            results["status"] = "FAILED"

        # Check 4: Period & Entity Matrix Completeness
        entities = sorted(raw_debt["entity_id"].unique())
        periods = sorted(raw_fx["period"].unique())
        expected_cells = int(len(entities) * len(periods))  # 4 * 24 = 96
        actual_cells = int(len(covenant_health))
        results["checks"]["entity_period_matrix"] = {
            "expected_entities": int(len(entities)),
            "expected_periods": int(len(periods)),
            "expected_cells": expected_cells,
            "actual_cells": actual_cells,
            "passed": bool(actual_cells == expected_cells),
        }
        if actual_cells != expected_cells:
            results["status"] = "FAILED"

        # Check 5: Stress Shock Detection for DURA_US (Months 18-21)
        stress_subset = covenant_health[
            (covenant_health["entity_id"] == "DURA_US") &
            (covenant_health["period_index"].isin([18, 19, 20, 21]))
        ]
        stress_breaches = int((stress_subset["covenant_status"] == "BREACH").sum())
        stress_passed = bool(stress_breaches == len(stress_subset) == 4)

        # Non-stress periods for DURA_US should NOT breach
        dura_non_stress = covenant_health[
            (covenant_health["entity_id"] == "DURA_US") &
            (~covenant_health["period_index"].isin([18, 19, 20, 21]))
        ]
        non_stress_dura_breaches = int((dura_non_stress["covenant_status"] == "BREACH").sum())

        # Other entities should have 0 breaches
        other_entities = covenant_health[covenant_health["entity_id"] != "DURA_US"]
        other_breaches = int((other_entities["covenant_status"] == "BREACH").sum())

        all_passed = bool(stress_passed and non_stress_dura_breaches == 0 and other_breaches == 0)
        results["checks"]["stress_shock_validation"] = {
            "dura_stress_months_evaluated": int(len(stress_subset)),
            "dura_stress_breaches_detected": stress_breaches,
            "dura_non_stress_breaches": non_stress_dura_breaches,
            "other_entities_breaches": other_breaches,
            "passed": all_passed,
        }
        if not all_passed:
            results["status"] = "FAILED"

        # Persist report
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.reports_dir / "lineage_variance_gate_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        return results


if __name__ == "__main__":
    gate = LineageVarianceGate()
    res = gate.evaluate_gate()
    print("=" * 60)
    print(f"Lineage & Variance Quality Gate: {res['status']}")
    print("=" * 60)
    for k, v in res["checks"].items():
        status_icon = "PASS" if v["passed"] else "FAIL"
        print(f"  [{status_icon}] {k}: {v}")

