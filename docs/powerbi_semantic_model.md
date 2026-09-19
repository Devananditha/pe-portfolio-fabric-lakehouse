# Microsoft Power BI Semantic Model & Executive Mart Architecture

## 1. Executive Summary & Semantic Model Design
The **Private Equity Portfolio Value Creation Semantic Model** is engineered for C-suite operating partners, deal teams, and portfolio CFOs to monitor operational EBITDA expansion, senior debt compliance, and covenant headroom across acquired portfolio companies. 

The semantic model sits atop the **Gold Lakehouse Layer** in Microsoft Fabric / OneLake and is consumable via:
1. **Power BI DirectLake Mode**: Real-time querying against Delta/Parquet tables without data duplication or import refresh latency.
2. **Power BI Import Mode**: High-speed in-memory VertiPaq tabular caching from `data/03_gold/powerbi_portfolio_value_creation.csv` or partitioned Gold Parquet marts.

---

## 2. Star Schema Dimensional Architecture

```
                  ┌────────────────────────┐
                  │       dim_period       │
                  │  (Date & Calendar Mart) │
                  └───────────┬────────────┘
                              │ 1
                              │
                              │ *
┌──────────────────────┐  *   ▼   *  ┌────────────────────────┐
│      dim_entity      │◄────┼──────►│      dim_account       │
│  (PortCo Master Dim) │     │       │  (Canonical COA Lines) │
└──────────┬───────────┘     │       └────────────────────────┘
           │ 1               │
           │                 │
           │ *               ▼
┌──────────▼──────────────────────────────┐
│       feat_portfolio_covenant_health    │
│    (Executive Covenant & DSCR Mart)     │
└─────────────────────────────────────────┘
```

### Table Grain & Key Relationships

| Table Name | Layer / Path | Grain | Primary / Foreign Keys | Cardinality & Cross-Filter |
| :--- | :--- | :--- | :--- | :--- |
| `dim_entity` | `data/03_gold/dim_entity.parquet` | 1 row per Portfolio Company | `PK: entity_id` | 1-to-Many $\rightarrow$ Fact (`feat_portfolio_covenant_health[entity_code]`) |
| `dim_period` | `data/03_gold/dim_period.parquet` | 1 row per Calendar Month (`YYYY-MM`) | `PK: period_key` | 1-to-Many $\rightarrow$ Fact (`feat_portfolio_covenant_health[period_key]`) |
| `dim_account` | `data/03_gold/dim_account.parquet` | 1 row per Canonical Account Code | `PK: canonical_code` | 1-to-Many $\rightarrow$ Fact (`fact_financial_monthly[canonical_code]`) |
| `feat_portfolio_covenant_health` | `data/03_gold/feat_portfolio_covenant_health.parquet` | 1 row per Entity per Period (96 rows) | `PK: (entity_code, period_key)` | Central Analytical Fact Mart |
| `powerbi_portfolio_value_creation` | `data/03_gold/powerbi_portfolio_value_creation.csv` | Denormalized Executive Mart (96 rows) | Composite Key: `entity_code + period_key` | Flat Executive Dataset for Instant Ingestion |

---

## 3. Dimensional Attribute Specifications

### `dim_entity` (Portfolio Company Master)
- **`entity_id`** (`String`): Unique ticker (`DURA_US`, `MED_UK`, `LOGI_EU`, `RETAIL_US`).
- **`entity_name`** (`String`): Legal and operating name (e.g., `DuraCorp Industrial Systems`).
- **`facility_name`** (`String`): Senior debt facility description (e.g., `Senior Secured Term Loan A`).
- **`functional_currency`** (`String`): Source currency (`USD`, `GBP`, `EUR`).
- **`principal_origination`** (`Currency`): Senior facility amount in functional currency.
- **`principal_usd`** (`Currency`): Senior facility amount standardized in USD ($125,000,000 total).
- **`annual_interest_rate`** (`Percentage`): Contractual facility interest coupon (5.80% to 8.00%).
- **`monthly_amortization_usd`** (`Currency`): Scheduled monthly debt principal amortization ($100k to $400k).
- **`covenant_min_dscr`** (`Decimal`): Minimum statutory DSCR covenant threshold (1.20x to 1.35x).
- **`covenant_max_leverage_ratio`** (`Decimal`): Maximum total net leverage ratio (3.75x to 4.50x).

### `dim_period` (Calendar & Investment Timeline)
- **`period_key`** (`String`): ISO Year-Month format (`2024-01` to `2025-12`).
- **`period_index`** (`Integer`): Sequential timeline month from portfolio inception ($1$ to $24$).
- **`calendar_year`** (`Integer`): Fiscal/calendar year ($2024$, $2025$).
- **`calendar_month`** (`Integer`): Calendar month ($1$ to $12$).
- **`quarter`** (`String`): Calendar quarter (`2024-Q1`, `2025-Q4`).
- **`is_stress_period`** (`Boolean`): Flag indicating operational stress window ($M18 - M21$).
