#!/usr/bin/env python3
"""Deterministic Multi-Entity ERP Synthetic Data Generator.

Simulates 4 private equity portfolio companies across heterogeneous ERP systems:
1. DURA_US: SAP S/4HANA (numeric GL codes, USD)
2. MED_UK: NetSuite (hierarchical strings, GBP)
3. LOGI_EU: Dynamics 365 (German SKR-04 accounts, EUR)
4. RETAIL_US: QuickBooks Online (small-business chart, USD)

Includes monthly FX cross-rates, master debt facilities with covenants,
and 24 months (2024-2025) of balanced double-entry GL ledger transactions.
An intentional operational stress shock is engineered for DURA_US in
months 18-21 (2025-06 to 2025-09) to validate automated DSCR covenant breach monitoring.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import math
from pathlib import Path
import random
from typing import Dict, List, Tuple


# Configuration Constants
SEED = 42
BASE_YEAR_START = 2024
NUM_PERIODS = 24  # 24 months: 2024-01 through 2025-12
STRESS_ENTITY = "DURA_US"
STRESS_MONTHS = {18, 19, 20, 21}  # 2025-06 to 2025-09 (1-indexed months 1..24)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "01_raw"


@dataclass(frozen=True)
class EntityConfig:
    entity_id: str
    entity_name: str
    source_system: str
    functional_currency: str
    base_monthly_revenue: float
    cogs_ratio: float
    opex_ratio: float
    da_monthly: float
    facility_name: str
    principal_origination: float
    annual_interest_rate: float
    monthly_amortization: float
    covenant_min_dscr: float
    covenant_max_leverage: float
    origination_date: str
    maturity_date: str


ENTITIES: Dict[str, EntityConfig] = {
    "DURA_US": EntityConfig(
        entity_id="DURA_US",
        entity_name="DuraCorp Industrial Systems",
        source_system="SAP",
        functional_currency="USD",
        base_monthly_revenue=3_500_000.00,
        cogs_ratio=0.46,
        opex_ratio=0.23,
        da_monthly=120_000.00,
        facility_name="Senior Secured Term Loan A",
        principal_origination=50_000_000.00,
        annual_interest_rate=0.0750,  # 7.5%
        monthly_amortization=400_000.00,
        covenant_min_dscr=1.25,
        covenant_max_leverage=4.50,
        origination_date="2023-12-15",
        maturity_date="2028-12-15",
    ),
    "MED_UK": EntityConfig(
        entity_id="MED_UK",
        entity_name="CloudMed HealthTech Ltd",
        source_system="NetSuite",
        functional_currency="GBP",
        base_monthly_revenue=1_800_000.00,
        cogs_ratio=0.32,
        opex_ratio=0.35,
        da_monthly=75_000.00,
        facility_name="Syndicated Growth Debt Facility",
        principal_origination=25_000_000.00,
        annual_interest_rate=0.0650,  # 6.5%
        monthly_amortization=180_000.00,
        covenant_min_dscr=1.35,
        covenant_max_leverage=3.75,
        origination_date="2023-11-01",
        maturity_date="2028-11-01",
    ),
    "LOGI_EU": EntityConfig(
        entity_id="LOGI_EU",
        entity_name="LogiTrans European Logistics GmbH",
        source_system="Dynamics365",
        functional_currency="EUR",
        base_monthly_revenue=2_600_000.00,
        cogs_ratio=0.48,
        opex_ratio=0.25,
        da_monthly=90_000.00,
        facility_name="Euro Term Loan B",
        principal_origination=35_000_000.00,
        annual_interest_rate=0.0580,  # 5.8%
        monthly_amortization=250_000.00,
        covenant_min_dscr=1.20,
        covenant_max_leverage=4.25,
        origination_date="2023-10-01",
        maturity_date="2029-10-01",
    ),
    "RETAIL_US": EntityConfig(
        entity_id="RETAIL_US",
        entity_name="Apex Omnichannel Retail Corp",
        source_system="QuickBooks",
        functional_currency="USD",
        base_monthly_revenue=1_200_000.00,
        cogs_ratio=0.44,
        opex_ratio=0.28,
        da_monthly=45_000.00,
        facility_name="Asset-Based Senior Revolver",
        principal_origination=15_000_000.00,
        annual_interest_rate=0.0800,  # 8.0%
        monthly_amortization=100_000.00,
        covenant_min_dscr=1.20,
        covenant_max_leverage=4.00,
        origination_date="2024-01-01",
        maturity_date="2027-12-31",
    ),
}

# Source ERP Chart of Accounts mappings to Canonical standard
COA_MAPPINGS: List[Dict[str, str]] = [
    # SAP (DURA_US)
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_REV_400010", "legacy_account_id": "GL_REV_400010", "legacy_account_name": "Gross Revenue Industrial Sales", "canonical_code": "REV_CORE", "canonical_account_code": "REV_CORE", "account_category": "Revenue", "financial_statement_line": "REVENUE", "financial_statement": "Income_Statement", "normal_balance": "Credit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_COGS_500010", "legacy_account_id": "GL_COGS_500010", "legacy_account_name": "Direct Manufacturing Material & Labor", "canonical_code": "COGS_DIRECT", "canonical_account_code": "COGS_DIRECT", "account_category": "COGS", "financial_statement_line": "COGS", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_OPEX_600010", "legacy_account_id": "GL_OPEX_600010", "legacy_account_name": "SG&A Operating Overhead & Payroll", "canonical_code": "OPEX_SGA", "canonical_account_code": "OPEX_SGA", "account_category": "Operating_Expense", "financial_statement_line": "OPEX", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_DA_700010", "legacy_account_id": "GL_DA_700010", "legacy_account_name": "Plant & Machinery Depreciation", "canonical_code": "DA_DEPR", "canonical_account_code": "DA_DEPR", "account_category": "Depreciation_Amortization", "financial_statement_line": "DEPRECIATION", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_INT_800010", "legacy_account_id": "GL_INT_800010", "legacy_account_name": "Senior Facility Interest Expense", "canonical_code": "FIN_INT_EXP", "canonical_account_code": "FIN_INT_EXP", "account_category": "Interest_Expense", "financial_statement_line": "INTEREST_EXPENSE", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_CASH_100010", "legacy_account_id": "GL_CASH_100010", "legacy_account_name": "JPMorgan Treasury Operating Checking", "canonical_code": "CASH_EQUIV", "canonical_account_code": "CASH_EQUIV", "account_category": "Cash", "financial_statement_line": "CASH", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_AR_110010", "legacy_account_id": "GL_AR_110010", "legacy_account_name": "Trade Accounts Receivable Commercial", "canonical_code": "AR_TRADE", "canonical_account_code": "AR_TRADE", "account_category": "Current_Asset", "financial_statement_line": "ACCOUNTS_RECEIVABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_AP_200010", "legacy_account_id": "GL_AP_200010", "legacy_account_name": "Trade Accounts Payable Vendors", "canonical_code": "AP_TRADE", "canonical_account_code": "AP_TRADE", "account_category": "Current_Liability", "financial_statement_line": "ACCOUNTS_PAYABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_DEBT_250010", "legacy_account_id": "GL_DEBT_250010", "legacy_account_name": "Senior Secured Term Loan Principal", "canonical_code": "DEBT_SENIOR", "canonical_account_code": "DEBT_SENIOR", "account_category": "Long_Term_Debt", "financial_statement_line": "SENIOR_DEBT", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "SAP", "entity_code": "DURA_US", "raw_account_code": "GL_EQ_300010", "legacy_account_id": "GL_EQ_300010", "legacy_account_name": "Retained Earnings & Contributed Surplus", "canonical_code": "EQUITY_RETAINED", "canonical_account_code": "EQUITY_RETAINED", "account_category": "Equity", "financial_statement_line": "RETAINED_EQUITY", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},

    # NetSuite (MED_UK)
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "REV-SUB-ARR", "legacy_account_id": "REV-SUB-ARR", "legacy_account_name": "SaaS Subscription ARR Recurring Revenue", "canonical_code": "REV_CORE", "canonical_account_code": "REV_CORE", "account_category": "Revenue", "financial_statement_line": "REVENUE", "financial_statement": "Income_Statement", "normal_balance": "Credit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "COGS-HOST-AWS", "legacy_account_id": "COGS-HOST-AWS", "legacy_account_name": "AWS Hosting & Direct Infrastructure", "canonical_code": "COGS_DIRECT", "canonical_account_code": "COGS_DIRECT", "account_category": "COGS", "financial_statement_line": "COGS", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "OPEX-SALES-MKTG", "legacy_account_id": "OPEX-SALES-MKTG", "legacy_account_name": "Sales, Marketing, R&D and Admin", "canonical_code": "OPEX_SGA", "canonical_account_code": "OPEX_SGA", "account_category": "Operating_Expense", "financial_statement_line": "OPEX", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "DA-SERVER-AMORT", "legacy_account_id": "DA-SERVER-AMORT", "legacy_account_name": "Capitalized Software Amortization", "canonical_code": "DA_DEPR", "canonical_account_code": "DA_DEPR", "account_category": "Depreciation_Amortization", "financial_statement_line": "AMORTIZATION", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "FIN-TERM-INT", "legacy_account_id": "FIN-TERM-INT", "legacy_account_name": "Growth Debt Monthly Interest Charge", "canonical_code": "FIN_INT_EXP", "canonical_account_code": "FIN_INT_EXP", "account_category": "Interest_Expense", "financial_statement_line": "INTEREST_EXPENSE", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "CASH-BARCLAYS-GBP", "legacy_account_id": "CASH-BARCLAYS-GBP", "legacy_account_name": "Barclays Commercial Operating Account", "canonical_code": "CASH_EQUIV", "canonical_account_code": "CASH_EQUIV", "account_category": "Cash", "financial_statement_line": "CASH", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "AR-CUSTOMERS", "legacy_account_id": "AR-CUSTOMERS", "legacy_account_name": "B2B Subscription Receivables Control", "canonical_code": "AR_TRADE", "canonical_account_code": "AR_TRADE", "account_category": "Current_Asset", "financial_statement_line": "ACCOUNTS_RECEIVABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "AP-VENDORS", "legacy_account_id": "AP-VENDORS", "legacy_account_name": "Trade Accounts Payable Vendor Ledger", "canonical_code": "AP_TRADE", "canonical_account_code": "AP_TRADE", "account_category": "Current_Liability", "financial_statement_line": "ACCOUNTS_PAYABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "DEBT-SENIOR-FAC", "legacy_account_id": "DEBT-SENIOR-FAC", "legacy_account_name": "Syndicated Senior Debt Tranche", "canonical_code": "DEBT_SENIOR", "canonical_account_code": "DEBT_SENIOR", "account_category": "Long_Term_Debt", "financial_statement_line": "SENIOR_DEBT", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "NetSuite", "entity_code": "MED_UK", "raw_account_code": "EQ-COMMON-CAP", "legacy_account_id": "EQ-COMMON-CAP", "legacy_account_name": "Common Equity & Retained Earnings", "canonical_code": "EQUITY_RETAINED", "canonical_account_code": "EQUITY_RETAINED", "account_category": "Equity", "financial_statement_line": "RETAINED_EQUITY", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},

    # Dynamics 365 SKR-04 (LOGI_EU)
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "4400_ERLOESE", "legacy_account_id": "4400_ERLOESE", "legacy_account_name": "Erlöse aus Logistikdienstleistungen", "canonical_code": "REV_CORE", "canonical_account_code": "REV_CORE", "account_category": "Revenue", "financial_statement_line": "REVENUE", "financial_statement": "Income_Statement", "normal_balance": "Credit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "5400_WARENEINSATZ", "legacy_account_id": "5400_WARENEINSATZ", "legacy_account_name": "Aufwendungen für Frachten & Treibstoff", "canonical_code": "COGS_DIRECT", "canonical_account_code": "COGS_DIRECT", "account_category": "COGS", "financial_statement_line": "COGS", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "6200_PERSONALAUFWAND", "legacy_account_id": "6200_PERSONALAUFWAND", "legacy_account_name": "Personalaufwand und allgemeine Verwaltung", "canonical_code": "OPEX_SGA", "canonical_account_code": "OPEX_SGA", "account_category": "Operating_Expense", "financial_statement_line": "OPEX", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "6500_ABSCHREIBUNGEN", "legacy_account_id": "6500_ABSCHREIBUNGEN", "legacy_account_name": "Abschreibungen auf Fuhrpark und Logistik", "canonical_code": "DA_DEPR", "canonical_account_code": "DA_DEPR", "account_category": "Depreciation_Amortization", "financial_statement_line": "DEPRECIATION", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "7300_ZINSAUFWAND", "legacy_account_id": "7300_ZINSAUFWAND", "legacy_account_name": "Zinsaufwendungen für Darlehen", "canonical_code": "FIN_INT_EXP", "canonical_account_code": "FIN_INT_EXP", "account_category": "Interest_Expense", "financial_statement_line": "INTEREST_EXPENSE", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "1200_BANK_EUR", "legacy_account_id": "1200_BANK_EUR", "legacy_account_name": "Deutsche Bank Geschäftsguthaben", "canonical_code": "CASH_EQUIV", "canonical_account_code": "CASH_EQUIV", "account_category": "Cash", "financial_statement_line": "CASH", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "1400_FORDERUNGEN", "legacy_account_id": "1400_FORDERUNGEN", "legacy_account_name": "Forderungen aus Lieferungen und Leistungen", "canonical_code": "AR_TRADE", "canonical_account_code": "AR_TRADE", "account_category": "Current_Asset", "financial_statement_line": "ACCOUNTS_RECEIVABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "3300_VERBINDLICHKEITEN", "legacy_account_id": "3300_VERBINDLICHKEITEN", "legacy_account_name": "Verbindlichkeiten aus Lieferungen", "canonical_code": "AP_TRADE", "canonical_account_code": "AP_TRADE", "account_category": "Current_Liability", "financial_statement_line": "ACCOUNTS_PAYABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "0630_ANLEIHEN", "legacy_account_id": "0630_ANLEIHEN", "legacy_account_name": "Langfristige Bankverbindlichkeiten", "canonical_code": "DEBT_SENIOR", "canonical_account_code": "DEBT_SENIOR", "account_category": "Long_Term_Debt", "financial_statement_line": "SENIOR_DEBT", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "Dynamics365", "entity_code": "LOGI_EU", "raw_account_code": "2000_EIGENKAPITAL", "legacy_account_id": "2000_EIGENKAPITAL", "legacy_account_name": "Gezeichnetes Kapital und Rücklagen", "canonical_code": "EQUITY_RETAINED", "canonical_account_code": "EQUITY_RETAINED", "account_category": "Equity", "financial_statement_line": "RETAINED_EQUITY", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},

    # QuickBooks (RETAIL_US)
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "401_Merch_Sales", "legacy_account_id": "401_Merch_Sales", "legacy_account_name": "Omnichannel Merchandise Sales", "canonical_code": "REV_CORE", "canonical_account_code": "REV_CORE", "account_category": "Revenue", "financial_statement_line": "REVENUE", "financial_statement": "Income_Statement", "normal_balance": "Credit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "501_Cost_Goods_Sold", "legacy_account_id": "501_Cost_Goods_Sold", "legacy_account_name": "Cost of Goods Sold & Direct Freight", "canonical_code": "COGS_DIRECT", "canonical_account_code": "COGS_DIRECT", "account_category": "COGS", "financial_statement_line": "COGS", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "601_Payroll_SG&A", "legacy_account_id": "601_Payroll_SG&A", "legacy_account_name": "Store Payroll, Marketing, & Rent", "canonical_code": "OPEX_SGA", "canonical_account_code": "OPEX_SGA", "account_category": "Operating_Expense", "financial_statement_line": "OPEX", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "680_Depreciation", "legacy_account_id": "680_Depreciation", "legacy_account_name": "Depreciation on Store Fixtures", "canonical_code": "DA_DEPR", "canonical_account_code": "DA_DEPR", "account_category": "Depreciation_Amortization", "financial_statement_line": "DEPRECIATION", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "701_Interest_Expense", "legacy_account_id": "701_Interest_Expense", "legacy_account_name": "Revolver & Term Interest Expense", "canonical_code": "FIN_INT_EXP", "canonical_account_code": "FIN_INT_EXP", "account_category": "Interest_Expense", "financial_statement_line": "INTEREST_EXPENSE", "financial_statement": "Income_Statement", "normal_balance": "Debit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "101_Operating_Checking", "legacy_account_id": "101_Operating_Checking", "legacy_account_name": "Chase Commercial Operating Account", "canonical_code": "CASH_EQUIV", "canonical_account_code": "CASH_EQUIV", "account_category": "Cash", "financial_statement_line": "CASH", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "120_Accounts_Receivable", "legacy_account_id": "120_Accounts_Receivable", "legacy_account_name": "Trade Accounts Receivable Control", "canonical_code": "AR_TRADE", "canonical_account_code": "AR_TRADE", "account_category": "Current_Asset", "financial_statement_line": "ACCOUNTS_RECEIVABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Debit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "201_Accounts_Payable", "legacy_account_id": "201_Accounts_Payable", "legacy_account_name": "Vendor Accounts Payable Control", "canonical_code": "AP_TRADE", "canonical_account_code": "AP_TRADE", "account_category": "Current_Liability", "financial_statement_line": "ACCOUNTS_PAYABLE", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "270_Note_Payable", "legacy_account_id": "270_Note_Payable", "legacy_account_name": "Asset-Based Term Loan Principal", "canonical_code": "DEBT_SENIOR", "canonical_account_code": "DEBT_SENIOR", "account_category": "Long_Term_Debt", "financial_statement_line": "SENIOR_DEBT", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
    {"source_system": "QuickBooks", "entity_code": "RETAIL_US", "raw_account_code": "301_Owners_Equity", "legacy_account_id": "301_Owners_Equity", "legacy_account_name": "Shareholder Equity & Retained Earnings", "canonical_code": "EQUITY_RETAINED", "canonical_account_code": "EQUITY_RETAINED", "account_category": "Equity", "financial_statement_line": "RETAINED_EQUITY", "financial_statement": "Balance_Sheet", "normal_balance": "Credit"},
]


def round_cur(val: float) -> Decimal:
    """Rounds currency amount to 2 decimal places using standard financial rounding."""
    return Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def get_period_strings() -> List[Tuple[int, str]]:
    """Returns list of (month_idx, 'YYYY-MM') for 24 months starting 2024-01."""
    periods = []
    for m in range(NUM_PERIODS):
        year = BASE_YEAR_START + (m // 12)
        month = (m % 12) + 1
        periods.append((m + 1, f"{year:04d}-{month:02d}"))
    return periods


def generate_coa_crosswalk(output_path: Path) -> None:
    """Generates raw_coa_crosswalk_mapping.csv."""
    headers = [
        "source_system",
        "entity_code",
        "raw_account_code",
        "legacy_account_id",
        "legacy_account_name",
        "canonical_code",
        "canonical_account_code",
        "account_category",
        "financial_statement_line",
        "financial_statement",
        "normal_balance",
    ]
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in COA_MAPPINGS:
            writer.writerow(row)
    print(f"  [+] Wrote {len(COA_MAPPINGS)} Chart of Accounts crosswalk mappings to {output_path.name}")


def generate_fx_rates(output_path: Path) -> Dict[Tuple[str, str], float]:
    """Generates raw_fx_rates_monthly.csv with GBP, EUR, and USD cross rates to USD.

    Returns dict mapping (period, currency) -> average conversion rate to USD.
    """
    random.seed(SEED)
    periods = get_period_strings()
    headers = [
        "period",
        "from_currency",
        "to_currency",
        "spot_rate",
        "avg_rate",
    ]

    fx_lookup: Dict[Tuple[str, str], float] = {}

    rows = []
    # Base trajectories for GBP and EUR
    gbp_base = 1.2700
    eur_base = 1.0850

    for idx, period in periods:
        # Realistic macroeconomic drift with deterministic random noise
        gbp_drift = 0.02 * math.sin(idx / 3.0) + (random.uniform(-0.015, 0.015))
        eur_drift = 0.015 * math.cos(idx / 4.0) + (random.uniform(-0.012, 0.012))

        gbp_spot = round(gbp_base + gbp_drift, 4)
        gbp_avg = round(gbp_spot + random.uniform(-0.005, 0.005), 4)

        eur_spot = round(eur_base + eur_drift, 4)
        eur_avg = round(eur_spot + random.uniform(-0.004, 0.004), 4)

        # USD to USD identity
        rows.append({"period": period, "from_currency": "USD", "to_currency": "USD", "spot_rate": "1.0000", "avg_rate": "1.0000"})
        fx_lookup[(period, "USD")] = 1.0000

        # GBP to USD
        rows.append({"period": period, "from_currency": "GBP", "to_currency": "USD", "spot_rate": f"{gbp_spot:.4f}", "avg_rate": f"{gbp_avg:.4f}"})
        fx_lookup[(period, "GBP")] = gbp_avg

        # EUR to USD
        rows.append({"period": period, "from_currency": "EUR", "to_currency": "USD", "spot_rate": f"{eur_spot:.4f}", "avg_rate": f"{eur_avg:.4f}"})
        fx_lookup[(period, "EUR")] = eur_avg

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  [+] Wrote {len(rows)} FX conversion rates across {NUM_PERIODS} periods to {output_path.name}")
    return fx_lookup


def generate_debt_covenants(output_path: Path) -> None:
    """Generates raw_debt_covenants_master.csv containing facility terms for all 4 entities."""
    headers = [
        "entity_id",
        "entity_name",
        "facility_name",
        "currency",
        "principal_origination",
        "annual_interest_rate",
        "monthly_amortization",
        "covenant_min_dscr",
        "covenant_max_leverage_ratio",
        "origination_date",
        "maturity_date",
    ]

    rows = []
    for cfg in ENTITIES.values():
        rows.append({
            "entity_id": cfg.entity_id,
            "entity_name": cfg.entity_name,
            "facility_name": cfg.facility_name,
            "currency": cfg.functional_currency,
            "principal_origination": f"{cfg.principal_origination:.2f}",
            "annual_interest_rate": f"{cfg.annual_interest_rate:.4f}",
            "monthly_amortization": f"{cfg.monthly_amortization:.2f}",
            "covenant_min_dscr": f"{cfg.covenant_min_dscr:.2f}",
            "covenant_max_leverage_ratio": f"{cfg.covenant_max_leverage:.2f}",
            "origination_date": cfg.origination_date,
            "maturity_date": cfg.maturity_date,
        })

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  [+] Wrote {len(rows)} debt facility master covenants to {output_path.name}")


def generate_general_ledger_entries(output_path: Path) -> int:
    """Generates 24 months of balanced multi-currency general ledger entries for all entities.

    Enforces:
    1. Exact double-entry balancing: Sum(Debit) == Sum(Credit) for each monthly batch.
    2. Intentional covenant stress shock for DURA_US in months 18-21 (2025-06 to 2025-09),
       where revenue drops by ~40% while debt service and fixed SG&A remain sticky,
       depressing EBITDA and forcing DSCR to ~0.98x - 1.05x (< 1.25x covenant min).
    """
    random.seed(SEED)
    periods = get_period_strings()

    # Map legacy GL accounts by entity and canonical role
    legacy_by_role: Dict[Tuple[str, str], Dict[str, str]] = {}
    for mapping in COA_MAPPINGS:
        sys = mapping["source_system"]
        code = mapping["canonical_account_code"]
        legacy_by_role[(sys, code)] = mapping

    headers = [
        "entry_id",
        "batch_id",
        "posting_date",
        "transaction_date",
        "period_key",
        "period",
        "entity_code",
        "entity_id",
        "source_system",
        "raw_account_code",
        "legacy_gl_code",
        "legacy_gl_description",
        "currency",
        "amount_local_currency",
        "debit_amount",
        "credit_amount",
        "net_amount",
        "description",
    ]

    all_rows = []
    entry_counter = 1

    # Track principal balance for debt schedule
    outstanding_principal = {cfg.entity_id: cfg.principal_origination for cfg in ENTITIES.values()}

    for month_idx, period_str in periods:
        year, month = map(int, period_str.split("-"))
        # Standard closing date for batch entries
        tx_date = date(year, month, 28).isoformat()

        for entity_id, cfg in ENTITIES.items():
            sys = cfg.source_system
            curr = cfg.functional_currency

            # Calculate Monthly Revenue & Costs
            # Base seasonal modulation
            seasonality = 1.0 + 0.05 * math.sin((month - 1) * math.pi / 6.0)
            noise = random.uniform(-0.02, 0.02)
            growth = 1.0 + (0.005 * month_idx)  # gradual operational scaling

            # Check if this is the intentional stress shock for DURA_US
            is_stress = (entity_id == STRESS_ENTITY and month_idx in STRESS_MONTHS)

            if is_stress:
                # Severe supply chain shock & customer deferral: Revenue drops ~40%
                rev_factor = 0.58 + random.uniform(-0.02, 0.02)
                rev_amount = cfg.base_monthly_revenue * rev_factor
                # COGS has sticky fixed contracts: falls only modestly
                cogs_amount = rev_amount * (cfg.cogs_ratio * 1.15)
                # SG&A fixed overhead & severance/consulting spikes
                opex_amount = cfg.base_monthly_revenue * cfg.opex_ratio * 0.98
            else:
                rev_factor = seasonality * growth + noise
                rev_amount = cfg.base_monthly_revenue * rev_factor
                cogs_amount = rev_amount * cfg.cogs_ratio
                opex_amount = cfg.base_monthly_revenue * cfg.opex_ratio * (1.0 + 0.002 * month_idx)

            da_amount = cfg.da_monthly

            # Debt Service calculation:
            current_debt = outstanding_principal[entity_id]
            monthly_interest = current_debt * (cfg.annual_interest_rate / 12.0)
            monthly_amort = min(cfg.monthly_amortization, current_debt)

            # Round all amounts cleanly
            d_rev = round_cur(rev_amount)
            d_cogs = round_cur(cogs_amount)
            d_opex = round_cur(opex_amount)
            d_da = round_cur(da_amount)
            d_interest = round_cur(monthly_interest)
            d_amort = round_cur(monthly_amort)

            # Update debt balance for subsequent months
            outstanding_principal[entity_id] = float(Decimal(str(current_debt)) - d_amort)

            # Cash Flow Balancing
            # Collections: assume 95% of monthly revenue collected into cash, rest into AR
            d_ar_addition = round_cur(float(d_rev) * 0.05)
            d_cash_collections = d_rev - d_ar_addition

            # Supplier/Vendor AP: 85% paid in cash, 15% net AP change
            d_ap_addition = round_cur(float(d_cogs) * 0.10)
            d_cash_cogs = d_cogs - d_ap_addition

            # Construct balanced double-entry batches for this entity in this period
            batches: List[List[Tuple[str, str, Decimal, Decimal, str]]] = []

            # Batch 1: Revenue Recognition (Debit AR, Credit Rev)
            b1 = [
                (legacy_by_role[(sys, "AR_TRADE")]["legacy_account_id"],
                 legacy_by_role[(sys, "AR_TRADE")]["legacy_account_name"],
                 d_rev, Decimal("0.00"), "Commercial Invoicing & Billing"),
                (legacy_by_role[(sys, "REV_CORE")]["legacy_account_id"],
                 legacy_by_role[(sys, "REV_CORE")]["legacy_account_name"],
                 Decimal("0.00"), d_rev, "Core Operating Revenue Earned"),
            ]
            batches.append(b1)

            # Batch 2: Cash Collection against Receivables (Debit Cash, Credit AR)
            b2 = [
                (legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_id"],
                 legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_name"],
                 d_cash_collections, Decimal("0.00"), "Cash Collections from Customers"),
                (legacy_by_role[(sys, "AR_TRADE")]["legacy_account_id"],
                 legacy_by_role[(sys, "AR_TRADE")]["legacy_account_name"],
                 Decimal("0.00"), d_cash_collections, "AR Settlement from Collections"),
            ]
            batches.append(b2)

            # Batch 3: Direct Cost of Goods Sold & Payables (Debit COGS, Credit AP)
            b3 = [
                (legacy_by_role[(sys, "COGS_DIRECT")]["legacy_account_id"],
                 legacy_by_role[(sys, "COGS_DIRECT")]["legacy_account_name"],
                 d_cogs, Decimal("0.00"), "Direct Cost of Goods & Services Delivered"),
                (legacy_by_role[(sys, "AP_TRADE")]["legacy_account_id"],
                 legacy_by_role[(sys, "AP_TRADE")]["legacy_account_name"],
                 Decimal("0.00"), d_cogs, "Vendor Invoices Received"),
            ]
            batches.append(b3)

            # Batch 4: Vendor Payments from Cash (Debit AP, Credit Cash)
            b4 = [
                (legacy_by_role[(sys, "AP_TRADE")]["legacy_account_id"],
                 legacy_by_role[(sys, "AP_TRADE")]["legacy_account_name"],
                 d_cash_cogs, Decimal("0.00"), "Vendor AP Disbursed"),
                (legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_id"],
                 legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_name"],
                 Decimal("0.00"), d_cash_cogs, "Operating Cash Disbursed for Direct COGS"),
            ]
            batches.append(b4)

            # Batch 5: Operating Expenses / SG&A (Debit OPEX_SGA, Credit Cash)
            b5 = [
                (legacy_by_role[(sys, "OPEX_SGA")]["legacy_account_id"],
                 legacy_by_role[(sys, "OPEX_SGA")]["legacy_account_name"],
                 d_opex, Decimal("0.00"), "SG&A Payroll, Overhead & Admin Expenses"),
                (legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_id"],
                 legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_name"],
                 Decimal("0.00"), d_opex, "Cash Disbursement for SG&A"),
            ]
            batches.append(b5)

            # Batch 6: Depreciation & Amortization (Debit DA_DEPR, Credit Retained Equity Reserves)
            b6 = [
                (legacy_by_role[(sys, "DA_DEPR")]["legacy_account_id"],
                 legacy_by_role[(sys, "DA_DEPR")]["legacy_account_name"],
                 d_da, Decimal("0.00"), "Monthly Depreciation & Amortization"),
                (legacy_by_role[(sys, "EQUITY_RETAINED")]["legacy_account_id"],
                 legacy_by_role[(sys, "EQUITY_RETAINED")]["legacy_account_name"],
                 Decimal("0.00"), d_da, "Accumulated Depreciation / Reserve Allocation"),
            ]
            batches.append(b6)

            # Batch 7: Debt Service Payment (Debit Interest Exp, Debit Senior Debt Amort, Credit Cash)
            debt_service_cash = d_interest + d_amort
            b7 = [
                (legacy_by_role[(sys, "FIN_INT_EXP")]["legacy_account_id"],
                 legacy_by_role[(sys, "FIN_INT_EXP")]["legacy_account_name"],
                 d_interest, Decimal("0.00"), "Senior Facility Monthly Interest Charge"),
                (legacy_by_role[(sys, "DEBT_SENIOR")]["legacy_account_id"],
                 legacy_by_role[(sys, "DEBT_SENIOR")]["legacy_account_name"],
                 d_amort, Decimal("0.00"), "Senior Debt Principal Amortization"),
                (legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_id"],
                 legacy_by_role[(sys, "CASH_EQUIV")]["legacy_account_name"],
                 Decimal("0.00"), debt_service_cash, "Cash Wire for Scheduled Debt Service"),
            ]
            batches.append(b7)

            # Convert batches to formatted CSV rows and check invariant
            for b_idx, batch in enumerate(batches, start=1):
                batch_id = f"BAT-{period_str.replace('-', '')}-{entity_id}-{b_idx:02d}"
                batch_debit_sum = Decimal("0.00")
                batch_credit_sum = Decimal("0.00")

                for gl_code, gl_desc, debit, credit, desc in batch:
                    batch_debit_sum += debit
                    batch_credit_sum += credit
                    net_amt = debit - credit
                    amount_local = debit if debit > Decimal("0.00") else credit
                    all_rows.append({
                        "entry_id": f"GL-{entry_counter:07d}",
                        "batch_id": batch_id,
                        "posting_date": tx_date,
                        "transaction_date": tx_date,
                        "period_key": period_str,
                        "period": period_str,
                        "entity_code": entity_id,
                        "entity_id": entity_id,
                        "source_system": sys,
                        "raw_account_code": gl_code,
                        "legacy_gl_code": gl_code,
                        "legacy_gl_description": gl_desc,
                        "currency": curr,
                        "amount_local_currency": f"{amount_local:.2f}",
                        "debit_amount": f"{debit:.2f}",
                        "credit_amount": f"{credit:.2f}",
                        "net_amount": f"{net_amt:.2f}",
                        "description": desc,
                    })
                    entry_counter += 1

                # Invariant assertion: batch debit must exactly match batch credit
                if batch_debit_sum != batch_credit_sum:
                    raise ValueError(
                        f"Financial invariant failure in batch {batch_id}: "
                        f"Debits={batch_debit_sum} != Credits={batch_credit_sum}"
                    )

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"  [+] Wrote {len(all_rows)} balanced General Ledger entries across {NUM_PERIODS} periods to {output_path.name}")
    return len(all_rows)


def main() -> None:
    """Main generation routine."""
    print("=" * 70)
    print("PE Portfolio Lakehouse: Deterministic Multi-Entity ERP Generator")
    print(f"Seed: {SEED} | Periods: {NUM_PERIODS} (2024-01 to 2025-12) | Entities: 4")
    print("=" * 70)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    coa_path = OUTPUT_DIR / "raw_coa_crosswalk_mapping.csv"
    fx_path = OUTPUT_DIR / "raw_fx_rates_monthly.csv"
    debt_path = OUTPUT_DIR / "raw_debt_covenants_master.csv"
    gl_path = OUTPUT_DIR / "raw_general_ledger_entries.csv"

    generate_coa_crosswalk(coa_path)
    generate_fx_rates(fx_path)
    generate_debt_covenants(debt_path)
    count = generate_general_ledger_entries(gl_path)

    print("=" * 70)
    print(f"Synthetic ERP generation complete! Total GL lines: {count}")
    print(f"Raw landing directory: {OUTPUT_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
