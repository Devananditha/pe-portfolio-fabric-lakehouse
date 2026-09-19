"""Exports Gold lakehouse data into web/data.js for standalone executive dashboard."""

from pathlib import Path
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_ROOT / "data" / "03_gold"
RAW_DIR = PROJECT_ROOT / "data" / "01_raw"
REPORTS_DIR = PROJECT_ROOT / "reports"
WEB_DIR = PROJECT_ROOT / "web"


def export():
    dim_entity = pd.read_parquet(GOLD_DIR / "dim_entity.parquet").to_dict(orient="records")
    dim_period = pd.read_parquet(GOLD_DIR / "dim_period.parquet").to_dict(orient="records")
    dim_account = pd.read_parquet(GOLD_DIR / "dim_account.parquet").to_dict(orient="records")
    fact_covenant = pd.read_parquet(GOLD_DIR / "fact_covenant_health.parquet").to_dict(orient="records")
    coa_crosswalk = pd.read_csv(RAW_DIR / "raw_coa_crosswalk_mapping.csv").to_dict(orient="records")

    gate_report_path = REPORTS_DIR / "lineage_variance_gate_report.json"
    gate_report = {}
    if gate_report_path.exists():
        with open(gate_report_path, "r", encoding="utf-8") as f:
            gate_report = json.load(f)

    data = {
        "dim_entity": dim_entity,
        "dim_period": dim_period,
        "dim_account": dim_account,
        "fact_covenant": fact_covenant,
        "coa_crosswalk": coa_crosswalk,
        "gate_report": gate_report,
    }

    WEB_DIR.mkdir(parents=True, exist_ok=True)
    out_file = WEB_DIR / "data.js"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("window.PORTFOLIO_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";\n")

    print(f"[+] Wrote web data payload to {out_file}")


if __name__ == "__main__":
    export()
