# PE Portfolio Value Creation Lakehouse Documentation

## Overview
This lakehouse provides end-to-end data ingestion, harmonization, and financial engineering for Private Equity (PE) portfolio companies. It transforms disparate ERP source systems (SAP, NetSuite, Dynamics 365, QuickBooks) into a unified canonical financial data mart supporting EBITDA tracking, Debt Service Coverage Ratio (DSCR) monitoring, covenant stress detection, and PE value-creation bridge attribution.

## Architecture
- **01_raw**: Multi-entity General Ledger extracts, FX conversion rates, and master debt facilities.
- **02_silver**: Canonical Chart of Accounts (COA) mapped entries translated to functional & reporting USD.
- **03_gold**: Star-schema marts (`dim_entity`, `dim_period`, `dim_account`, `fact_financial_monthly`, `fact_covenant_health`).
