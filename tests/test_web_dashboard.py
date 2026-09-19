"""Unit tests validating web dashboard DOM structure, design tokens, and What-If controls."""

from pathlib import Path
import re
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = PROJECT_ROOT / "web" / "index.html"
DATA_JS = PROJECT_ROOT / "web" / "data.js"


class TestWebDashboard:
    @classmethod
    def setup_class(cls):
        assert INDEX_HTML.exists(), "web/index.html must exist"
        assert DATA_JS.exists(), "web/data.js must exist"
        cls.html_content = INDEX_HTML.read_text(encoding="utf-8")
        cls.js_data = DATA_JS.read_text(encoding="utf-8")

    def test_design_system_tokens(self):
        """Verify the bespoke Warm Linen & Forest Green editorial color palette."""
        tokens = [
            ("#F7F5F0", "Warm Oatmeal / Linen canvas"),
            ("#FFFFFF", "Pure Off-White card"),
            ("#E5E0D8", "Warm stone border"),
            ("#1B2E26", "Deep Pine Charcoal typography"),
            ("#234E3E", "Deep Forest Pine primary"),
            ("#3D7058", "Sage accent"),
            ("#9E2A2B", "Warm Terracotta alert")
        ]
        for hex_code, desc in tokens:
            assert hex_code in self.html_content, f"Design token {hex_code} ({desc}) missing in index.html"

    def test_top_kpi_cards_structure(self):
        """Verify presence of 4 executive Top KPI Cards."""
        kpi_ids = [
            "kpi-card-revenue",
            "kpi-ttm-revenue",
            "kpi-card-ebitda",
            "kpi-ebitda-margin",
            "kpi-card-debt",
            "kpi-total-debt",
            "kpi-card-breaches",
            "kpi-active-breaches"
        ]
        for elem_id in kpi_ids:
            assert f'id="{elem_id}"' in self.html_content, f"KPI element {elem_id} missing in index.html"

    def test_what_if_scenario_sliders(self):
        """Verify What-If value creation scenario sliders and configuration bounds."""
        assert 'id="slider-synergy"' in self.html_content
        assert 'id="slider-spread"' in self.html_content
        assert 'id="slider-pricing"' in self.html_content

        # Verify slider boundaries
        assert 'min="0" max="15"' in self.html_content, "Synergy slider must range 0% to 15%"
        assert 'min="-150" max="200"' in self.html_content, "Debt spread slider must range -150 to +200 bps"
        assert 'min="0.95" max="1.15"' in self.html_content, "Pricing slider must range 0.95x to 1.15x"

    def test_chart_and_table_containers(self):
        """Verify Chart.js canvas and interactive PortCo portfolio table."""
        assert 'id="ebitdaTrajectoryChart"' in self.html_content
        assert 'id="portfolio-table"' in self.html_content
        assert 'id="portfolio-table-body"' in self.html_content
        assert 'id="select-period"' in self.html_content

    def test_audit_anomaly_modal(self):
        """Verify audit modal and variance drift anomaly table elements."""
        assert 'id="audit-modal"' in self.html_content
        assert 'id="audit-anomaly-table-body"' in self.html_content
        assert 'toggleAuditModal()' in self.html_content

    def test_zero_cold_start_data_binding(self):
        """Verify data.js script inclusion and simulation functions."""
        assert '<script src="data.js"></script>' in self.html_content
        assert 'recalculateSimulation' in self.html_content
        assert 'updatePortfolioTable' in self.html_content
        assert 'updateChartData' in self.html_content
        assert 'window.PORTFOLIO_DATA' in self.js_data
