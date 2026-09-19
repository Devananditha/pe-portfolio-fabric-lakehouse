# Private Equity Portfolio Value Creation Lakehouse

An institutional-grade DataOps and financial engineering Lakehouse engineered for Private Equity (PE) portfolio monitoring, debt covenant compliance, and EBITDA value creation tracking across multi-entity acquired assets.

---

## 🏛️ Medallion Architecture Overview

```
pe-portfolio-fabric-lakehouse/
├── data/
│   ├── 01_raw/                 # Ingested ERP extracts, FX cross-rates, debt covenants
│   ├── 02_silver/              # Parquet tables harmonized to Canonical COA & USD translated
│   └── 03_gold/                # Star schema mart (dim_entity, dim_period, dim_account, facts)
├── docs/                       # Architectural specifications and documentation
├── reports/                    # Lineage & Financial Variance Gate audit reports
├── scripts/
│   ├── generate_portfolio_data.py # Deterministic multi-ERP generator (seed=42)
│   └── export_web_data.py         # Compiles lakehouse data into web dashboard payload
├── src/
│   ├── __init__.py
│   └── pipelines/
│       ├── __init__.py
│       ├── bronze_ingestion.py        # Raw-to-Bronze landing & validation
│       ├── silver_harmonization.py    # COA crosswalking & multi-currency FX translation
│       ├── gold_dimensional_modeling.py # Dimensional modeling (EBITDA, DSCR, Debt Service)
│       └── lineage_variance_gate.py   # Automated data quality & lineage gate
├── tests/
│   ├── __init__.py
│   ├── test_financial_invariants.py   # Balance sheet equation, double-entry & covenant tests
│   └── test_lineage_variance.py       # Cross-layer reconciliation & variance tolerances
├── web/
│   ├── index.html                     # Executive Portfolio Monitoring & Covenant UI
│   └── data.js                        # Exported Lakehouse data payload
├── requirements.txt                   # Project dependencies
└── run_pipeline.py                    # Master CLI orchestration runner
```

---

## 🏢 Simulated Portfolio Companies & ERP Systems

| Entity ID | Company Name | Acquired ERP System | Functional Currency | Senior Debt Facility | Facility Origination | Annual Interest | Monthly Amortization | Min Covenant DSCR |
| :--- | :--- | :--- | :---: | :--- | :---: | :---: | :---: | :---: |
| `DURA_US` | DuraCorp Industrial Systems | SAP S/4HANA (Numeric GL) | USD | Senior Secured Term Loan A | $50,000,000 | 7.50% | $400,000 | **1.25x** |
| `MED_UK` | CloudMed HealthTech Ltd | NetSuite (Text Strings) | GBP | Syndicated Growth Debt Facility | £25,000,000 | 6.50% | £180,000 | **1.35x** |
| `LOGI_EU` | LogiTrans European Logistics | Dynamics 365 (SKR-04) | EUR | Euro Term Loan B | €35,000,000 | 5.80% | €250,000 | **1.20x** |
| `RETAIL_US` | Apex Omnichannel Retail | QuickBooks Online (Chart) | USD | Asset-Based Senior Revolver | $15,000,000 | 8.00% | $100,000 | **1.20x** |

---

## ⚡ Financial Invariants & Covenant Stress Dynamics

1. **Double-Entry Balancing**:
   Every journal batch across all entities strictly enforces:
   $$\sum \text{Debits} = \sum \text{Credits}$$
   The automated Lineage & Financial Variance Gate verifies $\Delta = \$0.00$ difference across all 1,440 raw and silver general ledger records.

2. **Multi-Currency Harmonization**:
   Functional currencies (GBP, EUR, USD) are converted to standard reporting USD using monthly spot and average FX rates.

3. **Engineered Covenant Stress Shock**:
   In periods **18 through 21** (`2025-06` to `2025-09`), `DURA_US` undergoes an operational supply-chain disruption where core revenue falls ~40% against sticky overhead and scheduled debt service ($660k-$670k/month). This depresses EBITDA and causes DSCR to plunge to ~0.23x - 0.27x, triggering an intentional **Covenant Breach Alert** for automated monitoring validation.

---

## 🛡️ DataOps Governance, Variance Drift Gate & CI/CD Suite

### 1. Automated Variance Drift Gate (`src/pipelines/lineage_variance_gate.py`)
The DataOps quality gate automatically monitors period-over-period (PoP) financial metric swings across the entire portfolio:
- **$\pm 15\%$ Drift Detection**: Scans consecutive periods for every entity in the Gold mart (`feat_portfolio_covenant_health.parquet`) and flags any Revenue or EBITDA fluctuation exceeding $\pm 15\%$.
- **Operational Event Catalog**: Known business transitions (such as the `DURA_US` M18 supply-chain shock and M22 turnaround rebound) are cross-referenced against documented operational events to differentiate genuine operational anomalies from unverified data drifts.
- **Audit Artifact Generation**: Flagged swings and classifications are persisted directly to `data/03_gold/audit_variance_anomalies.csv` and reported in `reports/lineage_variance_gate_report.json`.

### 2. Financial Invariants PyTest Suite (`tests/test_financial_invariants.py`)
Automated test suite asserting institutional financial truths across 29 test scenarios:
- **Double-Entry Balance Invariant**: Asserts Gross Profit $\le$ Revenue across all 96 portfolio entity-months.
- **EBITDA Invariant**: Asserts $\text{EBITDA} = \text{Revenue} - \text{COGS} - \text{OPEX}$ to machine precision ($10^{-4}$).
- **Covenant Non-Null Invariant**: Guarantees zero missing values for DSCR, minimum covenant thresholds, and health status indicators.
- **100% Cryptographic Lineage**: Validates that every Silver record retains an immutable MD5 lineage hash and preserves the originating ERP system.
- **Stress Shock Detection**: Asserts that `DURA_US` triggers an explicit `BREACH_ALERT` during scheduled stress periods (M18–M21) with zero false-positive breaches across normal operating entities.

### 3. Continuous Integration Workflow (`.github/workflows/dataops-ci.yml`)
- **Automated Triggers**: Runs on every push and pull request targeting `main`.
- **Runtime Environment**: Python 3.11 with cached pip dependencies.
- **Governance Steps**:
  1. Multi-ERP synthetic portfolio generator (`scripts/generate_portfolio_data.py`).
  2. Strict PyTest financial governance execution (`pytest tests/ -v`).
  3. Full Medallion lakehouse pipeline orchestration (`python run_pipeline.py --all`).
  4. Lakehouse Parquet and audit artifact existence and integrity assertions.

---

## 🚀 Quickstart & CLI Orchestration

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 2. Generate Synthetic ERP Data
```powershell
python scripts/generate_portfolio_data.py
```

### 3. Run the End-to-End Orchestration Pipeline
```powershell
python run_pipeline.py --all
```

Execution flags:
- `--generate-data`: Generates synthetic multi-entity ERP CSV extracts.
- `--bronze`: Performs schema conformance and stamps audit metadata.
- `--silver`: Crosswalks legacy accounts to Canonical COA and translates FX.
- `--gold`: Generates dimensional models and computes monthly EBITDA/DSCR.
- `--gate`: Evaluates the Lineage & Financial Variance Gate.
- `--test`: Runs all unit and financial invariant pytest suites.
- `--all`: Runs data generation, Medallion pipelines, quality gate, and tests.

### 4. Open Executive Monitoring Dashboard
Open `web/index.html` in your web browser to view the interactive dark-mode DSCR monitoring portal, entity cards, and real-time financial tables.
