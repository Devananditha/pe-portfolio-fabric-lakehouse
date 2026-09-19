#!/usr/bin/env python3
"""PE Portfolio Value Creation Lakehouse - Master Orchestration Runner.

Provides CLI execution entry points to verify the environment, execute
Medallion pipelines (Bronze -> Silver -> Gold), run the Lineage & Variance Gate,
and execute the pytest test harness.

Usage:
  python run_pipeline.py --generate-data   # Generates raw synthetic ERP & covenant data
  python run_pipeline.py --bronze          # Ingests and validates Raw to Bronze
  python run_pipeline.py --silver          # Harmonizes COA & translates multi-currency
  python run_pipeline.py --gold            # Builds star-schema dimensional mart & DSCR
  python run_pipeline.py --gate            # Runs Lineage & Financial Variance Gate
  python run_pipeline.py --test            # Runs pytest test suite
  python run_pipeline.py --all             # Runs full end-to-end pipeline & testing
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def print_banner(title: str) -> None:
    """Prints a styled CLI banner."""
    width = 72
    print("\n" + "=" * width)
    print(f" {title.center(width - 2)}")
    print("=" * width)


def check_environment() -> bool:
    """Verifies Python runtime and required packages."""
    print("Checking runtime environment...")
    py_ver = sys.version.split()[0]
    print(f"  [+] Python version: {py_ver}")

    req_packages = ["pandas", "numpy", "pyarrow", "pytest"]
    missing = []
    for pkg in req_packages:
        try:
            __import__(pkg)
            print(f"  [+] Package available: {pkg}")
        except ImportError:
            missing.append(pkg)
            print(f"  [-] Missing package: {pkg}")

    if missing:
        print(f"\n[!] Missing required packages: {missing}. Run: pip install -r requirements.txt")
        return False
    return True


def run_data_generation() -> bool:
    """Executes the deterministic synthetic data generator."""
    print_banner("Running Synthetic ERP & Covenant Generator")
    script_path = PROJECT_ROOT / "scripts" / "generate_portfolio_data.py"
    res = subprocess.run([sys.executable, str(script_path)], cwd=str(PROJECT_ROOT))
    return res.returncode == 0


def run_bronze() -> bool:
    """Executes Bronze layer ingestion."""
    print_banner("Step 1: Bronze Ingestion & Raw Validation")
    from src.pipelines.bronze_ingestion import BronzeIngestionPipeline
    pipeline = BronzeIngestionPipeline()
    tables = pipeline.run()
    for name, df in tables.items():
        print(f"  [+] Ingested Bronze dataset '{name}': {len(df):,} records")
    return True


def run_silver() -> bool:
    """Executes Silver layer harmonization."""
    print_banner("Step 2: Silver Harmonization & FX Translation")
    from src.pipelines.silver_harmonization import SilverHarmonizationPipeline
    pipeline = SilverHarmonizationPipeline()
    gl_df, debt_df = pipeline.run()
    print(f"  [+] Harmonized Silver GL: {len(gl_df):,} transactions translated to USD")
    print(f"  [+] Partitioned Parquet: data/02_silver/prm_harmonized_financial_ledger.parquet (4 partitions)")
    print(f"  [+] Harmonized Silver Debt Facilities: {len(debt_df):,} facility master rows")
    print(f"  [+] Silver Audit Summary: reports/silver_harmonization_audit_summary.json")
    return True


def run_gold() -> bool:
    """Executes Gold star schema dimensional modeling and exports web payload."""
    print_banner("Step 3: Gold Dimensional Modeling & DSCR Marts")
    from src.pipelines.gold_dimensional_modeling import GoldDimensionalModelingPipeline
    pipeline = GoldDimensionalModelingPipeline()
    models = pipeline.run()
    for name, df in models.items():
        print(f"  [+] Built Gold Mart '{name}': {len(df):,} records")

    # Refresh web dashboard payload
    try:
        from scripts.export_web_data import export as export_web
        export_web()
        print("  [+] Refreshed web dashboard data payload: web/data.js")
    except Exception as e:
        print(f"  [!] Web data export notice: {e}")

    return True


def run_gate() -> bool:
    """Executes the Lineage & Financial Variance Quality Gate."""
    print_banner("Step 4: Lineage & Financial Variance Gate")
    from src.pipelines.lineage_variance_gate import LineageVarianceGate
    gate = LineageVarianceGate()
    res = gate.evaluate_gate()
    status = res["status"]
    print(f"Gate Status: {status}")
    for check_name, check_data in res["checks"].items():
        pass_icon = "PASS" if check_data["passed"] else "FAIL"
        print(f"  [{pass_icon}] {check_name}")
    return status == "PASSED"


def run_tests() -> bool:
    """Runs pytest test harness."""
    print_banner("Step 5: Executing Pytest Financial & Lineage Suites")
    res = subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v"], cwd=str(PROJECT_ROOT))
    return res.returncode == 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PE Portfolio Value Creation Lakehouse Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--check-env", action="store_true", help="Verify environment dependencies")
    parser.add_argument("--generate-data", action="store_true", help="Generate synthetic multi-entity ERP CSVs")
    parser.add_argument("--bronze", action="store_true", help="Run Bronze ingestion")
    parser.add_argument("--silver", action="store_true", help="Run Silver harmonization")
    parser.add_argument("--gold", action="store_true", help="Run Gold dimensional modeling")
    parser.add_argument("--gate", action="store_true", help="Run Lineage & Variance Quality Gate")
    parser.add_argument("--test", action="store_true", help="Execute automated test suite")
    parser.add_argument("--all", action="store_true", help="Execute complete Medallion pipeline and test suite")

    args = parser.parse_args()

    # Default to showing help if no arguments passed
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    start_time = time.time()

    if args.check_env:
        ok = check_environment()
        sys.exit(0 if ok else 1)

    if args.generate_data:
        if not run_data_generation():
            print("\n[!] Data generation failed.")
            sys.exit(1)

    if args.bronze:
        if not run_bronze():
            sys.exit(1)

    if args.silver:
        if not run_silver():
            sys.exit(1)

    if args.gold:
        if not run_gold():
            sys.exit(1)

    if args.gate:
        if not run_gate():
            sys.exit(1)

    if args.test:
        if not run_tests():
            sys.exit(1)

    if args.all:
        if not check_environment():
            sys.exit(1)
        if not run_data_generation():
            sys.exit(1)
        if not run_bronze():
            sys.exit(1)
        if not run_silver():
            sys.exit(1)
        if not run_gold():
            sys.exit(1)
        if not run_gate():
            sys.exit(1)
        if not run_tests():
            sys.exit(1)

    elapsed = time.time() - start_time
    print_banner(f"All Requested Operations Completed in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
