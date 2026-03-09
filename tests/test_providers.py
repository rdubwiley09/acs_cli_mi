"""Tests for AHRF provider data — CLI commands and client functions."""
from __future__ import annotations

import csv
import io

import pytest
from typer.testing import CliRunner

from acs_cli.cli import app
from acs_cli.hrsa_api.ahrf import (
    AHRF_MEASURES,
    resolve_ahrf_measures,
    fetch_ahrf_data,
)

runner = CliRunner()


# ── provider-topics command ─────────────────────────────────────────────────


class TestProviderTopicsCommand:
    def test_lists_groups(self):
        result = runner.invoke(app, ["provider-topics"])
        assert result.exit_code == 0
        assert "physicians" in result.output
        assert "mid_level" in result.output
        assert "dental" in result.output

    def test_row_count(self):
        result = runner.invoke(app, ["provider-topics"])
        lines = result.output.strip().split("\n")
        # header + 6 measures
        assert len(lines) == 7

    def test_measure_ids_present(self):
        result = runner.invoke(app, ["provider-topics"])
        assert "ahrf_primary_care_physicians" in result.output
        assert "ahrf_nurse_practitioners" in result.output
        assert "ahrf_dentists" in result.output


# ── providers command ───────────────────────────────────────────────────────


class TestProvidersCommand:
    def test_no_args_error(self):
        result = runner.invoke(app, ["providers"])
        assert result.exit_code == 1
        assert "Error" in result.output

    def test_unknown_group_error(self, mock_ahrf):
        result = runner.invoke(app, ["providers", "nonexistent"])
        assert result.exit_code == 1
        assert "Unknown AHRF group" in result.output

    def test_physicians_group(self, mock_ahrf):
        result = runner.invoke(app, ["providers", "physicians"])
        assert result.exit_code == 0
        assert "PC Physicians" in result.output
        assert "Total MDs" in result.output
        assert "Total DOs" in result.output
        # Should have Michigan data
        assert "Washtenaw" in result.output

    def test_all_groups(self, mock_ahrf):
        result = runner.invoke(app, ["providers", "all"])
        assert result.exit_code == 0
        # All measure labels should appear
        assert "PC Physicians" in result.output
        assert "NPs" in result.output
        assert "PAs" in result.output
        assert "Dentists" in result.output

    def test_county_filter(self, mock_ahrf):
        result = runner.invoke(app, ["providers", "all", "--county", "Wayne"])
        assert result.exit_code == 0
        assert "Wayne" in result.output
        # Other counties should be filtered out
        reader = csv.reader(io.StringIO(result.output))
        rows = list(reader)
        # header + 1 data row
        assert len(rows) == 2


# ── AHRF client functions ──────────────────────────────────────────────────


class TestAHRFClient:
    def test_resolve_single_group(self):
        measures = resolve_ahrf_measures(["physicians"])
        assert len(measures) == 3
        ids = {m.measure_id for m in measures}
        assert "ahrf_primary_care_physicians" in ids
        assert "ahrf_total_mds" in ids
        assert "ahrf_total_dos" in ids

    def test_resolve_all(self):
        measures = resolve_ahrf_measures(["all"])
        total = sum(len(v) for v in AHRF_MEASURES.values())
        assert len(measures) == total

    def test_resolve_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown AHRF group"):
            resolve_ahrf_measures(["nonexistent"])

    def test_fetch_filters_michigan_only(self, mock_ahrf):
        measures = resolve_ahrf_measures(["all"])
        rows = fetch_ahrf_data(measures)
        # Should only have 3 Michigan rows (not the NY row)
        assert len(rows) == 3
        counties = {r["county"] for r in rows}
        assert "Washtenaw" in counties
        assert "Wayne" in counties
        assert "Oakland" in counties

    def test_fetch_correct_values(self, mock_ahrf):
        measures = resolve_ahrf_measures(["physicians"])
        rows = fetch_ahrf_data(measures)
        washtenaw = next(r for r in rows if r["county"] == "Washtenaw")
        assert washtenaw["ahrf_primary_care_physicians"] == "120"
        assert washtenaw["ahrf_total_mds"] == "250"
        assert washtenaw["ahrf_total_dos"] == "80"

    def test_fetch_county_name_mapping(self, mock_ahrf):
        measures = resolve_ahrf_measures(["mid_level"])
        rows = fetch_ahrf_data(measures)
        # Verify FIPS 26163 maps to Wayne
        wayne = next(r for r in rows if r["county"] == "Wayne")
        assert wayne["ahrf_nurse_practitioners"] == "35"
        assert wayne["ahrf_physician_assistants"] == "25"
