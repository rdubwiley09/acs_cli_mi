"""Tests for BLS LAUS and QCEW economic data integration."""

from __future__ import annotations

import csv
import io
from unittest.mock import ANY

import pytest
import respx
from httpx import Response
from typer.testing import CliRunner

from acs_cli.cli import app
from acs_cli.bls_api import (
    ECONOMY_MEASURES,
    QCEW_MEASURES,
    resolve_economy_measures,
    resolve_qcew_measures,
)
from acs_cli.bls_api.client import (
    BLS_BASE_URL,
    BLSAPIError,
    MissingBLSKeyError,
    _build_series_id,
    fetch_economy_data,
    fetch_qcew_data,
)
from acs_cli.hrsa_api.client import MI_FIPS_TO_COUNTY
from tests.conftest import (
    MOCK_BLS_FIPS,
    QCEW_BASE_URL_PATTERN,
    build_bls_response,
    build_qcew_csv_response,
)

runner = CliRunner()


def parse_csv(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


# ── economy-topics command ─────────────────────────────────────────────────


class TestEconomyTopicsCommand:
    def test_lists_all_groups(self):
        result = runner.invoke(app, ["economy-topics"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header == ["Group", "Measure ID", "Label", "Description"]
        group_names = {r[0] for r in rows[1:]}
        assert group_names == set(ECONOMY_MEASURES.keys())

    def test_row_count_matches_measures(self):
        result = runner.invoke(app, ["economy-topics"])
        rows = parse_csv(result.stdout)
        expected = sum(len(ms) for ms in ECONOMY_MEASURES.values())
        assert len(rows) - 1 == expected

    def test_measure_ids_present(self):
        result = runner.invoke(app, ["economy-topics"])
        rows = parse_csv(result.stdout)
        ids = {r[1] for r in rows[1:]}
        assert "unemployment_rate" in ids
        assert "unemployment" in ids
        assert "employment" in ids
        assert "labor_force" in ids


# ── economy command ────────────────────────────────────────────────────────


class TestEconomyCommand:
    def test_no_group_argument(self):
        result = runner.invoke(app, ["economy"])
        assert result.exit_code == 1
        assert "Provide economy group" in result.stderr

    def test_unknown_group(self):
        with respx.mock:
            result = runner.invoke(app, ["economy", "nonexistent_group"])
        assert result.exit_code == 1
        assert "Unknown economy group" in result.stderr

    def test_unemployment_group(self, mock_bls):
        # Build response with series IDs for all 83 counties × 2 unemployment measures
        all_series = []
        for fips in sorted(MI_FIPS_TO_COUNTY.keys()):
            for code in ["03", "04"]:
                all_series.append(_build_series_id(fips, code))
        resp = build_bls_response(series_ids=all_series, base_value=4.5)
        mock_bls(response=resp)

        result = runner.invoke(app, ["economy", "unemployment"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header[0] == "County"
        assert "Unemployment Rate (%)" in header
        assert "Unemployment" in header
        assert len(rows) > 1

    def test_all_groups(self, mock_bls):
        all_series = []
        for fips in sorted(MI_FIPS_TO_COUNTY.keys()):
            for code in ["03", "04", "05", "06"]:
                all_series.append(_build_series_id(fips, code))
        resp = build_bls_response(series_ids=all_series, base_value=3.0)
        mock_bls(response=resp)

        result = runner.invoke(app, ["economy", "all"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert "Unemployment Rate (%)" in header
        assert "Employment" in header
        assert "Labor Force" in header
        assert len(rows) > 1

    def test_county_filter(self, mock_bls):
        all_series = []
        for fips in sorted(MI_FIPS_TO_COUNTY.keys()):
            for code in ["03", "04"]:
                all_series.append(_build_series_id(fips, code))
        resp = build_bls_response(series_ids=all_series, base_value=5.0)
        mock_bls(response=resp)

        result = runner.invoke(app, ["economy", "unemployment", "--county", "Wayne"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) == 2  # header + 1 county
        assert "Wayne" in rows[1][0]

    def test_sort_option(self, mock_bls):
        all_series = []
        for fips in sorted(MI_FIPS_TO_COUNTY.keys()):
            for code in ["03", "04"]:
                all_series.append(_build_series_id(fips, code))

        # Give each series a different value based on index
        series_list = []
        for i, sid in enumerate(all_series):
            series_list.append({
                "seriesID": sid,
                "data": [{
                    "year": "2024",
                    "period": "M13",
                    "periodName": "Annual",
                    "value": str(round(1.0 + i * 0.1, 1)),
                    "footnotes": [{}],
                }],
            })
        resp = {
            "status": "REQUEST_SUCCEEDED",
            "responseTime": 100,
            "message": [],
            "Results": {"series": series_list},
        }
        mock_bls(response=resp)

        result = runner.invoke(
            app, ["economy", "unemployment", "--sort", "Unemployment Rate (%)"]
        )
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        idx = header.index("Unemployment Rate (%)")
        values = [float(r[idx]) for r in rows[1:] if r[idx]]
        assert values == sorted(values, reverse=True)

    def test_output_to_file(self, mock_bls, tmp_path):
        all_series = []
        for fips in sorted(MI_FIPS_TO_COUNTY.keys()):
            for code in ["03", "04"]:
                all_series.append(_build_series_id(fips, code))
        resp = build_bls_response(series_ids=all_series, base_value=5.0)
        mock_bls(response=resp)

        outfile = str(tmp_path / "economy.csv")
        result = runner.invoke(
            app, ["economy", "unemployment", "--output", outfile]
        )
        assert result.exit_code == 0
        assert "Wrote CSV" in result.stderr
        with open(outfile) as f:
            rows = list(csv.reader(f))
        assert rows[0][0] == "County"
        assert len(rows) > 1

    def test_api_error(self, bls_api_key):
        with respx.mock:
            respx.post(BLS_BASE_URL).mock(
                return_value=Response(500, json={"status": "REQUEST_FAILED", "message": [], "Results": {"series": []}})
            )
            result = runner.invoke(app, ["economy", "unemployment"])
        assert result.exit_code == 1
        assert "500" in result.stderr

    def test_missing_key(self, monkeypatch, tmp_path):
        monkeypatch.setenv("BLS_API_KEY", "")
        monkeypatch.setattr(
            "acs_cli.bls_api.client.BLS_CONFIG_FILE",
            tmp_path / "nonexistent_bls_config",
        )
        result = runner.invoke(app, ["economy", "unemployment"])
        assert result.exit_code == 1
        assert "No BLS API key" in result.stderr


# ── BLS client unit tests ─────────────────────────────────────────────────


class TestBLSClient:
    def test_resolve_measures_single_group(self):
        result = resolve_economy_measures(["unemployment"])
        assert len(result) == len(ECONOMY_MEASURES["unemployment"])

    def test_resolve_measures_all(self):
        result = resolve_economy_measures(["all"])
        expected = sum(len(ms) for ms in ECONOMY_MEASURES.values())
        assert len(result) == expected

    def test_resolve_measures_unknown_group(self):
        with pytest.raises(ValueError, match="Unknown economy group"):
            resolve_economy_measures(["bogus"])

    def test_build_series_id(self):
        sid = _build_series_id("26163", "03")
        assert sid == "LAUCN261630000000003"

    def test_build_series_id_format(self):
        sid = _build_series_id("26001", "06")
        assert sid == "LAUCN260010000000006"
        assert sid.startswith("LAUCN")
        assert "26001" in sid
        assert sid.endswith("06")
        assert len(sid) == 20

    def test_fetch_success(self, bls_api_key):
        measures = resolve_economy_measures(["unemployment"])
        all_series = []
        for fips in sorted(MI_FIPS_TO_COUNTY.keys()):
            for m in measures:
                all_series.append(_build_series_id(fips, m.series_code))

        resp = build_bls_response(series_ids=all_series, base_value=4.2)

        with respx.mock:
            respx.post(BLS_BASE_URL).mock(
                return_value=Response(200, json=resp)
            )
            result = fetch_economy_data(measures, year=2024, api_key=bls_api_key)

        assert len(result) == 83
        assert all("county" in r for r in result)
        assert all("unemployment_rate" in r for r in result)

    def test_fetch_api_error(self, bls_api_key):
        measures = resolve_economy_measures(["unemployment"])
        with respx.mock:
            respx.post(BLS_BASE_URL).mock(
                return_value=Response(500, text="Server Error")
            )
            with pytest.raises(BLSAPIError, match="500"):
                fetch_economy_data(measures, year=2024, api_key=bls_api_key)

    def test_fetch_request_failed(self, bls_api_key):
        measures = resolve_economy_measures(["unemployment"])
        resp = {
            "status": "REQUEST_NOT_PROCESSED",
            "responseTime": 50,
            "message": ["Daily threshold reached"],
            "Results": {"series": []},
        }
        with respx.mock:
            respx.post(BLS_BASE_URL).mock(
                return_value=Response(200, json=resp)
            )
            with pytest.raises(BLSAPIError, match="Daily threshold reached"):
                fetch_economy_data(measures, year=2024, api_key=bls_api_key)

    def test_batching(self, bls_api_key):
        """Verify that 83 counties × 4 measures = 332 series -> 7 batches of ≤50."""
        measures = resolve_economy_measures(["all"])
        assert len(measures) == 4

        all_series = []
        for fips in sorted(MI_FIPS_TO_COUNTY.keys()):
            for m in measures:
                all_series.append(_build_series_id(fips, m.series_code))
        assert len(all_series) == 332

        resp = build_bls_response(series_ids=all_series, base_value=3.0)

        call_count = 0
        call_payloads = []

        def side_effect(request, route):
            nonlocal call_count
            call_count += 1
            import json
            payload = json.loads(request.content)
            call_payloads.append(payload)
            # Return a response with just the series from this chunk
            chunk_ids = payload["seriesid"]
            chunk_resp = build_bls_response(series_ids=chunk_ids, base_value=3.0)
            return Response(200, json=chunk_resp)

        with respx.mock:
            respx.post(BLS_BASE_URL).mock(side_effect=side_effect)
            result = fetch_economy_data(measures, year=2024, api_key=bls_api_key)

        # 332 / 50 = 6.64, so 7 requests
        assert call_count == 7
        # Every batch should have ≤50 series
        for payload in call_payloads:
            assert len(payload["seriesid"]) <= 50
        # Total series across all batches
        total = sum(len(p["seriesid"]) for p in call_payloads)
        assert total == 332
        # Should return 83 counties
        assert len(result) == 83


# ── qcew-topics command ──────────────────────────────────────────────────


class TestQCEWTopicsCommand:
    def test_lists_all_groups(self):
        result = runner.invoke(app, ["qcew-topics"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header == ["Group", "Measure ID", "Label", "Description"]
        group_names = {r[0] for r in rows[1:]}
        assert group_names == set(QCEW_MEASURES.keys())

    def test_row_count_matches_measures(self):
        result = runner.invoke(app, ["qcew-topics"])
        rows = parse_csv(result.stdout)
        expected = sum(len(ms) for ms in QCEW_MEASURES.values())
        assert len(rows) - 1 == expected

    def test_measure_ids_present(self):
        result = runner.invoke(app, ["qcew-topics"])
        rows = parse_csv(result.stdout)
        ids = {r[1] for r in rows[1:]}
        assert "qcew_avg_annual_pay" in ids
        assert "qcew_establishments" in ids
        assert "qcew_healthcare_employment" in ids
        assert "qcew_healthcare_establishments" in ids


# ── qcew command ──────────────────────────────────────────────────────────


class TestQCEWCommand:
    def test_no_group_argument(self):
        result = runner.invoke(app, ["qcew"])
        assert result.exit_code == 1
        assert "Provide QCEW group" in result.stderr

    def test_unknown_group(self):
        with respx.mock:
            result = runner.invoke(app, ["qcew", "nonexistent_group"])
        assert result.exit_code == 1
        assert "Unknown QCEW group" in result.stderr

    def test_wages_group(self, mock_qcew):
        mock_qcew()
        result = runner.invoke(app, ["qcew", "wages"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header[0] == "County"
        assert "Avg Annual Pay" in header
        assert "Establishments" in header
        assert len(rows) > 1

    def test_all_groups(self, mock_qcew):
        mock_qcew()
        result = runner.invoke(app, ["qcew", "all"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert "Avg Annual Pay" in header
        assert "HC Employment" in header
        assert "HC Establishments" in header
        assert len(rows) > 1

    def test_county_filter(self, mock_qcew):
        mock_qcew()
        result = runner.invoke(app, ["qcew", "wages", "--county", "Alcona"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        # header + 1 county
        assert len(rows) == 2
        assert "Alcona" in rows[1][0]


# ── QCEW client unit tests ──────────────────────────────────────────────


class TestQCEWClient:
    def test_resolve_measures_single_group(self):
        result = resolve_qcew_measures(["wages"])
        assert len(result) == len(QCEW_MEASURES["wages"])

    def test_resolve_measures_all(self):
        result = resolve_qcew_measures(["all"])
        expected = sum(len(ms) for ms in QCEW_MEASURES.values())
        assert len(result) == expected

    def test_resolve_measures_unknown_group(self):
        with pytest.raises(ValueError, match="Unknown QCEW group"):
            resolve_qcew_measures(["bogus"])

    def test_fetch_success(self, mock_qcew):
        mock_qcew()
        measures = resolve_qcew_measures(["all"])
        result = fetch_qcew_data(measures, year=2024)
        assert len(result) == 83
        assert all("county" in r for r in result)
        assert all("qcew_avg_annual_pay" in r for r in result)
        assert all("qcew_healthcare_employment" in r for r in result)

    def test_correct_value_extraction(self, mock_qcew):
        mock_qcew(csv_text=build_qcew_csv_response(
            avg_annual_pay="52000",
            annual_avg_estabs="1500",
            hc_employment="750",
            hc_establishments="80",
        ))
        measures = resolve_qcew_measures(["all"])
        result = fetch_qcew_data(measures, year=2024)
        # All counties get the same mock response
        first = result[0]
        assert first["qcew_avg_annual_pay"] == "52000"
        assert first["qcew_establishments"] == "1500"
        assert first["qcew_healthcare_employment"] == "750"
        assert first["qcew_healthcare_establishments"] == "80"

    def test_http_error_skips_county(self):
        """Counties that return HTTP errors are skipped, not crashed."""
        measures = resolve_qcew_measures(["all"])
        with respx.mock(assert_all_called=False, assert_all_mocked=False) as router:
            router.get(url__startswith=QCEW_BASE_URL_PATTERN).mock(
                return_value=Response(500, text="Server Error")
            )
            result = fetch_qcew_data(measures, year=2024)
        # Should still return 83 counties (with empty data)
        assert len(result) == 83
        # Values should be missing (only "county" key present)
        for r in result:
            assert "county" in r
            assert "qcew_avg_annual_pay" not in r
