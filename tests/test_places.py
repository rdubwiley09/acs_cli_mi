"""Tests for CDC PLACES integration."""

from __future__ import annotations

import csv
import io

import pytest
import respx
from httpx import Response
from typer.testing import CliRunner

from acs_cli.cli import app
from acs_cli.places_api import PLACES_MEASURES, resolve_measures
from acs_cli.places_api.client import (
    PLACES_BASE_URL,
    PlacesAPIError,
    _pivot_rows,
    fetch_places_data,
)
from tests.conftest import (
    MOCK_PLACES_COUNTIES,
    build_places_response,
)

runner = CliRunner()


def parse_csv(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


# ── places-topics command ────────────────────────────────────────────────────


class TestPlacesTopicsCommand:
    def test_lists_all_groups(self):
        result = runner.invoke(app, ["places-topics"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header == ["Group", "Measure ID", "Label", "Short Question"]
        group_names = {r[0] for r in rows[1:]}
        assert group_names == set(PLACES_MEASURES.keys())

    def test_row_count_matches_measures(self):
        result = runner.invoke(app, ["places-topics"])
        rows = parse_csv(result.stdout)
        expected = sum(len(ms) for ms in PLACES_MEASURES.values())
        assert len(rows) - 1 == expected

    def test_measure_ids_present(self):
        result = runner.invoke(app, ["places-topics"])
        rows = parse_csv(result.stdout)
        ids = {r[1] for r in rows[1:]}
        assert "DIABETES" in ids
        assert "COPD" in ids
        assert "BINGE" in ids


# ── places command ───────────────────────────────────────────────────────────


class TestPlacesCommand:
    def test_no_group_argument(self):
        result = runner.invoke(app, ["places"])
        assert result.exit_code == 1
        assert "Provide PLACES group" in result.stderr

    def test_unknown_group(self):
        with respx.mock:
            result = runner.invoke(app, ["places", "nonexistent_group"])
        assert result.exit_code == 1
        assert "Unknown PLACES group" in result.stderr

    def test_single_group(self, mock_places):
        measures = PLACES_MEASURES["health_behaviors"]
        mids = [m.measureid for m in measures]
        mock_places(measure_ids=mids)

        result = runner.invoke(app, ["places", "health_behaviors"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header[0] == "County"
        for m in measures:
            assert m.label in header
        assert len(rows) == 1 + len(MOCK_PLACES_COUNTIES)

    def test_all_groups(self, mock_places):
        all_measures = resolve_measures(["all"])
        mids = [m.measureid for m in all_measures]
        mock_places(measure_ids=mids)

        result = runner.invoke(app, ["places", "all"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) > 1

    def test_county_filter(self, mock_places):
        measures = PLACES_MEASURES["chronic_disease"]
        mids = [m.measureid for m in measures]
        mock_places(measure_ids=mids)

        result = runner.invoke(app, ["places", "chronic_disease", "--county", "Washtenaw"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) == 2  # header + 1 county
        assert "Washtenaw" in rows[1][0]

    def test_county_filter_no_match(self, mock_places):
        measures = PLACES_MEASURES["chronic_disease"]
        mids = [m.measureid for m in measures]
        mock_places(measure_ids=mids)

        result = runner.invoke(app, ["places", "chronic_disease", "--county", "Nonexistent"])
        assert "No matching rows" in result.stderr

    def test_sort_option(self, mock_places):
        measures = PLACES_MEASURES["health_behaviors"]
        mids = [m.measureid for m in measures]
        mock_places(measure_ids=mids)

        result = runner.invoke(app, ["places", "health_behaviors", "--sort", "Binge Drinking"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        # Find the Binge Drinking column index
        header = rows[0]
        binge_idx = header.index("Binge Drinking")
        values = [float(r[binge_idx]) for r in rows[1:] if r[binge_idx]]
        assert values == sorted(values, reverse=True)

    def test_output_to_file(self, mock_places, tmp_path):
        measures = PLACES_MEASURES["health_behaviors"]
        mids = [m.measureid for m in measures]
        mock_places(measure_ids=mids)

        outfile = str(tmp_path / "places.csv")
        result = runner.invoke(app, ["places", "health_behaviors", "--output", outfile])
        assert result.exit_code == 0
        assert "Wrote CSV" in result.stderr

        with open(outfile) as f:
            rows = list(csv.reader(f))
        assert rows[0][0] == "County"
        assert len(rows) == 1 + len(MOCK_PLACES_COUNTIES)

    def test_prevalence_type_crude(self, mock_places):
        measures = PLACES_MEASURES["health_behaviors"]
        mids = [m.measureid for m in measures]
        mock_places(measure_ids=mids, value_col="data_value")

        result = runner.invoke(app, ["places", "health_behaviors", "--prevalence", "crude"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) > 1

    def test_api_error(self):
        with respx.mock:
            respx.get(PLACES_BASE_URL).mock(
                return_value=Response(500, json=[])
            )
            result = runner.invoke(app, ["places", "chronic_disease"])
        assert result.exit_code == 1
        assert "500" in result.stderr


# ── PLACES client unit tests ────────────────────────────────────────────────


class TestPlacesClient:
    def test_resolve_measures_single_group(self):
        result = resolve_measures(["chronic_disease"])
        assert len(result) == len(PLACES_MEASURES["chronic_disease"])

    def test_resolve_measures_all(self):
        result = resolve_measures(["all"])
        expected = sum(len(ms) for ms in PLACES_MEASURES.values())
        assert len(result) == expected

    def test_resolve_measures_unknown_group(self):
        with pytest.raises(ValueError, match="Unknown PLACES group"):
            resolve_measures(["bogus"])

    def test_pivot_rows(self):
        from acs_cli.places_api.client import Measure

        measures = [
            Measure("DIABETES", "Diabetes", "test"),
            Measure("COPD", "COPD", "test"),
        ]
        records = [
            {"locationname": "Wayne", "measureid": "DIABETES", "data_value": "12.3"},
            {"locationname": "Wayne", "measureid": "COPD", "data_value": "8.1"},
            {"locationname": "Oakland", "measureid": "DIABETES", "data_value": "10.5"},
            {"locationname": "Oakland", "measureid": "COPD", "data_value": "7.2"},
        ]
        result = _pivot_rows(records, measures, "data_value")
        assert len(result) == 2
        # Sorted alphabetically by locationname
        assert result[0]["locationname"] == "Oakland"
        assert result[1]["locationname"] == "Wayne"
        assert result[1]["DIABETES"] == "0.123"
        assert result[1]["COPD"] == "0.081"

    def test_pivot_rows_missing_data(self):
        from acs_cli.places_api.client import Measure

        measures = [
            Measure("DIABETES", "Diabetes", "test"),
            Measure("COPD", "COPD", "test"),
        ]
        # Only DIABETES for Wayne, no COPD
        records = [
            {"locationname": "Wayne", "measureid": "DIABETES", "data_value": "12.3"},
        ]
        result = _pivot_rows(records, measures, "data_value")
        assert len(result) == 1
        assert result[0]["DIABETES"] == "0.123"
        assert "COPD" not in result[0]

    def test_pivot_rows_normalizes_st(self):
        from acs_cli.places_api.client import Measure

        measures = [Measure("DIABETES", "Diabetes", "test")]
        records = [
            {"locationname": "St. Clair", "measureid": "DIABETES", "data_value": "11.0"},
        ]
        result = _pivot_rows(records, measures, "data_value")
        assert len(result) == 1
        assert result[0]["locationname"] == "Saint Clair"

    def test_fetch_places_data_error(self):
        with respx.mock:
            respx.get(PLACES_BASE_URL).mock(
                return_value=Response(500, text="Internal Server Error")
            )
            with pytest.raises(PlacesAPIError, match="500"):
                from acs_cli.places_api.client import Measure
                measures = [Measure("DIABETES", "Diabetes", "test")]
                fetch_places_data(measures)

    def test_fetch_places_data_success(self, mock_places):
        from acs_cli.places_api.client import Measure

        measures = [Measure("DIABETES", "Diabetes", "test")]
        mock_places(measure_ids=["DIABETES"])
        result = fetch_places_data(measures)
        assert len(result) == len(MOCK_PLACES_COUNTIES)
        assert all("locationname" in r for r in result)
        assert all("DIABETES" in r for r in result)
