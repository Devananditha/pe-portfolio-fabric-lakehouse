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

## 📐 Financial Window Modeling Formulas & Analytical CTE Layer

The Lakehouse Gold Mart (`src/pipelines/gold_dimensional_modeling.py`) computes institutional-grade dimensional metrics via DuckDB analytical Common Table Expressions (CTEs) and SQL window functions:

1. **Rolling 3-Month Trailing EBITDA**:
   Smoothes monthly cash-flow volatility across quarterly operating cycles:
   $$\text{Rolling 3M EBITDA}_{i, t} = \frac{1}{\min(t, 3)} \sum_{k=0}^{\min(t-1, 2)} \text{EBITDA}_{i, t-k}$$
   ```sql
   AVG(ebitda) OVER (
     PARTITION BY entity_code 
     ORDER BY period_key 
     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
   ) AS rolling_3m_ebitda
   ```

2. **Trailing Twelve Months (TTM) Performance**:
   $$\text{TTM Metric}_{i, t} = \sum_{k=0}^{\min(t-1, 11)} \text{Metric}_{i, t-k}$$
   ```sql
   SUM(revenue) OVER (
     PARTITION BY entity_code 
     ORDER BY period_key 
     ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
   ) AS ttm_revenue
   ```

3. **Monthly Debt Service & Coverage Ratio (DSCR)**:
   $$\text{Debt Service}_{i, t} = \text{Monthly Principal Amortization}_{i} + \frac{\text{Senior Facility Principal}_{i} \times \text{Annual Interest Rate}_{i}}{12}$$
   $$\text{DSCR}_{i, t} = \frac{\text{EBITDA}_{i, t}}{\text{Debt Service}_{i, t}}$$
   $$\text{DSCR Headroom}_{i, t} = \text{DSCR}_{i, t} - \text{Covenant Min DSCR}_{i}$$

4. **Dynamic Covenant Health Classification**:
   $$\text{Status}_{i, t} = \begin{cases} 
   \text{BREACH\_ALERT} & \text{if } \text{DSCR}_{i, t} < \text{Min DSCR}_{i} \\
   \text{WARNING} & \text{if } \text{Min DSCR}_{i} \le \text{DSCR}_{i, t} < \text{Min DSCR}_{i} + 0.15 \\
   \text{HEALTHY} & \text{otherwise}
   \end{cases}$$

---

## 📈 Quantified Value-Creation EBITDA Sensitivity Matrix

The table below illustrates the sensitivity of Consolidated Portfolio TTM EBITDA, EBITDA Margin, and `DURA_US` Stress DSCR across commercial pricing power, operational synergy capture, and capital market refinancing spreads:

| Scenario | Pricing Power | Synergy OPEX Savings | Debt Spread (bps) | Consolidated TTM Revenue | Consolidated TTM EBITDA | Portfolio EBITDA Margin | DURA_US Stress DSCR (M18) | DURA_US Covenant Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Severe Downturn** | 0.95x | 0.0% | +200 bps | $112.5M | $29.2M | 26.0% | **0.19x** | `BREACH_ALERT` |
| **Current Baseline** | 1.00x | 0.0% | 0 bps | $118.4M | $36.9M | 31.2% | **0.23x** | `BREACH_ALERT` |
| **Conservative Plan** | 1.02x | 5.0% | -50 bps | $120.8M | $40.5M | 33.5% | **0.31x** | `BREACH_ALERT` |
| **Target PE Case** | 1.05x | 10.0% | -100 bps | $124.3M | $45.8M | 36.8% | **0.42x** | `BREACH_ALERT` |
| **Aggressive Turnaround** | 1.10x | 15.0% | -150 bps | $130.2M | $54.7M | 42.0% | **0.61x** | `BREACH_ALERT` |
| **Normal Run-Rate (M24)** | 1.05x | 10.0% | -100 bps | $124.3M | $45.8M | 36.8% | **2.21x** | `HEALTHY` |

---

## 🏛️ Executive Power BI Mart & Warm Linen Web Dashboard

### 1. Warm Linen & Forest Green Editorial Aesthetic (`web/index.html`)
The executive web dashboard is engineered to eliminate generic AI SaaS tropes (neon glowing dark-modes, purple gradients) in favor of a stately private equity editorial aesthetic:
- **Canvas Texture & Background**: Warm Oatmeal / Linen (`#F7F5F0`) with subtle radial point texture.
- **Card System**: Pure Off-White (`#FFFFFF`) with warm stone borders (`#E5E0D8`) and elevated drop shadows.
- **Typography Pairing**: Editorial Serif (`Newsreader`) for statement headers paired with `Plus Jakarta Sans` and `JetBrains Mono` for tabular metrics.
- **Primary Accents**: Deep Forest Pine (`#234E3E`) and Sage (`#3D7058`).
- **Breach Alert Signaling**: Warm Terracotta / Burnt Sienna (`#9E2A2B`) with subtle pulsing aura.

### 2. Interactive Value Creation Controls & Zero-Cold-Start Architecture
- **Interactive What-If Sliders**: Real-time evaluation of Synergy Cost Reductions (0% to 15%), Debt Refinance Spreads (-150 to +200 bps), and Pricing Power (0.95x to 1.15x).
- **Chart.js Dynamic Trajectory**: Smooth, responsive 24-month EBITDA curve across Apex, CloudMed, LogiTrans, and DuraCorp with live breach highlight points.
- **Instant Client-Side Engine**: Loaded from pre-compiled `web/data.js` for zero-lag instant rendering without external backend requirements.

### 3. Microsoft Power BI Semantic Model (`docs/powerbi_semantic_model.md`)
- Ready for ingestion into Microsoft Power BI Desktop and Fabric Service.
- Includes full Star Schema relationship diagrams, production DAX formulas (TTM calculations, weighted DSCR, breach filters), What-If parameter tables, and Power Query M ingestion scripts.

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
