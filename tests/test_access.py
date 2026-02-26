"""Tests for CMS healthcare access and HRSA shortage data integration."""

from __future__ import annotations

import csv
import io

import pytest
import respx
from httpx import Response
from typer.testing import CliRunner

from acs_cli.cli import app
from acs_cli.cms_api import ACCESS_MEASURES, resolve_access_measures
from acs_cli.cms_api.client import (
    HOSPITAL_BASE_URL,
    CMSAPIError,
    _aggregate_hospitals_by_county,
    fetch_hospital_data,
)
from acs_cli.hrsa_api import HPSA_MEASURES, resolve_hpsa_measures
from acs_cli.hrsa_api.client import (
    HRSAAPIError,
    _aggregate_hpsa_by_county,
    fetch_hpsa_data,
)
from tests.conftest import (
    MOCK_ACCESS_COUNTIES,
    build_hospital_response,
    build_hpsa_response,
    PC_HPSA_URL,
)

runner = CliRunner()


def parse_csv(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


# ── access-topics command ───────────────────────────────────────────────────


class TestAccessTopicsCommand:
    def test_lists_all_groups(self):
        result = runner.invoke(app, ["access-topics"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header == ["Source", "Group", "Measure ID", "Label", "Description"]
        group_names = {r[1] for r in rows[1:]}
        expected_groups = set(ACCESS_MEASURES.keys()) | set(HPSA_MEASURES.keys())
        assert group_names == expected_groups

    def test_row_count_matches_measures(self):
        result = runner.invoke(app, ["access-topics"])
        rows = parse_csv(result.stdout)
        expected = sum(len(ms) for ms in ACCESS_MEASURES.values()) + sum(
            len(ms) for ms in HPSA_MEASURES.values()
        )
        assert len(rows) - 1 == expected

    def test_measure_ids_present(self):
        result = runner.invoke(app, ["access-topics"])
        rows = parse_csv(result.stdout)
        ids = {r[2] for r in rows[1:]}
        assert "hospital_count" in ids
        assert "avg_hospital_rating" in ids
        assert "pc_hpsa_count" in ids
        assert "mh_hpsa_count" in ids


# ── access command ──────────────────────────────────────────────────────────


class TestAccessCommand:
    def test_no_group_argument(self):
        result = runner.invoke(app, ["access"])
        assert result.exit_code == 1
        assert "Provide access group" in result.stderr

    def test_unknown_group(self):
        with respx.mock:
            result = runner.invoke(app, ["access", "nonexistent_group"])
        assert result.exit_code == 1
        assert "Unknown access group" in result.stderr

    def test_hospital_access_group(self, mock_cms):
        mock_cms()
        result = runner.invoke(app, ["access", "hospital_access"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header[0] == "County"
        assert "Hospital Count" in header
        assert "Acute Care Hospitals" in header
        assert len(rows) > 1

    def test_hrsa_shortage_group(self, mock_hrsa):
        mock_hrsa()
        result = runner.invoke(app, ["access", "primary_care_shortage"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header[0] == "County"
        assert "Primary Care HPSA Count" in header
        assert len(rows) > 1

    def test_all_groups(self, mock_cms, mock_hrsa):
        mock_cms()
        mock_hrsa()
        result = runner.invoke(app, ["access", "all"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert "Hospital Count" in header
        assert "Primary Care HPSA Count" in header
        assert len(rows) > 1

    def test_county_filter(self, mock_cms):
        mock_cms()
        result = runner.invoke(app, ["access", "hospital_access", "--county", "Wayne"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) == 2  # header + 1 county
        assert "Wayne" in rows[1][0]

    def test_county_filter_no_match(self, mock_cms):
        mock_cms()
        result = runner.invoke(app, ["access", "hospital_access", "--county", "Nonexistent"])
        assert "No matching rows" in result.stderr

    def test_sort_option(self, mock_hrsa):
        mock_hrsa()
        result = runner.invoke(
            app, ["access", "primary_care_shortage", "--sort", "Primary Care HPSA Count"]
        )
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        idx = header.index("Primary Care HPSA Count")
        values = [float(r[idx]) for r in rows[1:] if r[idx]]
        assert values == sorted(values, reverse=True)

    def test_output_to_file(self, mock_cms, tmp_path):
        mock_cms()
        outfile = str(tmp_path / "access.csv")
        result = runner.invoke(app, ["access", "hospital_access", "--output", outfile])
        assert result.exit_code == 0
        assert "Wrote CSV" in result.stderr
        with open(outfile) as f:
            rows = list(csv.reader(f))
        assert rows[0][0] == "County"
        assert len(rows) > 1

    def test_cms_api_error(self):
        with respx.mock:
            respx.get(HOSPITAL_BASE_URL).mock(
                return_value=Response(500, json={"results": []})
            )
            result = runner.invoke(app, ["access", "hospital_access"])
        assert result.exit_code == 1
        assert "500" in result.stderr

    def test_hrsa_api_error(self):
        with respx.mock:
            respx.get(PC_HPSA_URL).mock(
                return_value=Response(500, text="Server Error")
            )
            result = runner.invoke(app, ["access", "primary_care_shortage"])
        assert result.exit_code == 1
        assert "500" in result.stderr


# ── CMS client unit tests ──────────────────────────────────────────────────


class TestCMSClient:
    def test_resolve_access_measures_single_group(self):
        result = resolve_access_measures(["hospital_access"])
        assert len(result) == len(ACCESS_MEASURES["hospital_access"])

    def test_resolve_access_measures_all(self):
        result = resolve_access_measures(["all"])
        expected = sum(len(ms) for ms in ACCESS_MEASURES.values())
        assert len(result) == expected

    def test_resolve_access_measures_unknown_group(self):
        with pytest.raises(ValueError, match="Unknown access group"):
            resolve_access_measures(["bogus"])

    def test_aggregate_hospitals_by_county(self):
        records = [
            {
                "countyparish": "WAYNE",
                "hospital_type": "Acute Care Hospitals",
                "emergency_services": "Yes",
                "hospital_overall_rating": "4",
                "meets_criteria_for_birthing_friendly_designation": "Y",
            },
            {
                "countyparish": "WAYNE",
                "hospital_type": "Critical Access Hospitals",
                "emergency_services": "No",
                "hospital_overall_rating": "3",
                "meets_criteria_for_birthing_friendly_designation": "N",
            },
            {
                "countyparish": "OAKLAND",
                "hospital_type": "Acute Care Hospitals",
                "emergency_services": "Yes",
                "hospital_overall_rating": "5",
                "meets_criteria_for_birthing_friendly_designation": "Y",
            },
        ]
        result = _aggregate_hospitals_by_county(records)
        assert len(result) == 2
        # Sorted alphabetically
        assert result[0]["county"] == "Oakland"
        assert result[1]["county"] == "Wayne"

        wayne = result[1]
        assert wayne["hospital_count"] == 2
        assert wayne["acute_care_hospitals"] == 1
        assert wayne["critical_access_hospitals"] == 1
        assert wayne["emergency_services"] == 1
        assert wayne["birthing_friendly"] == 1
        assert wayne["avg_hospital_rating"] == "3.5"

        oakland = result[0]
        assert oakland["hospital_count"] == 1
        assert oakland["avg_hospital_rating"] == "5.0"

    def test_fetch_hospital_data_error(self):
        with respx.mock:
            respx.get(HOSPITAL_BASE_URL).mock(
                return_value=Response(500, text="Internal Server Error")
            )
            with pytest.raises(CMSAPIError, match="500"):
                fetch_hospital_data()

    def test_fetch_hospital_data_success(self):
        response = build_hospital_response()
        with respx.mock:
            respx.get(HOSPITAL_BASE_URL).mock(
                return_value=Response(200, json=response)
            )
            result = fetch_hospital_data()
        assert len(result) == len(MOCK_ACCESS_COUNTIES)
        assert all(r["state"] == "MI" for r in result)


# ── HRSA client unit tests ─────────────────────────────────────────────────


class TestHRSAClient:
    def test_resolve_hpsa_measures_single_group(self):
        result = resolve_hpsa_measures(["primary_care_shortage"])
        assert len(result) == len(HPSA_MEASURES["primary_care_shortage"])

    def test_resolve_hpsa_measures_all(self):
        result = resolve_hpsa_measures(["all"])
        expected = sum(len(ms) for ms in HPSA_MEASURES.values())
        assert len(result) == expected

    def test_resolve_hpsa_measures_unknown_group(self):
        with pytest.raises(ValueError, match="Unknown shortage group"):
            resolve_hpsa_measures(["bogus"])

    def test_aggregate_hpsa_by_county(self):
        features = [
            {
                "attributes": {
                    "CMN_STATE_COUNTY_FIPS_CD": "26163",
                    "HPSA_SCORE": 20,
                    "HPSA_ESTIMATED_UNDERSERVED_POP": 15000,
                }
            },
            {
                "attributes": {
                    "CMN_STATE_COUNTY_FIPS_CD": "26163",
                    "HPSA_SCORE": 10,
                    "HPSA_ESTIMATED_UNDERSERVED_POP": 5000,
                }
            },
            {
                "attributes": {
                    "CMN_STATE_COUNTY_FIPS_CD": "26125",
                    "HPSA_SCORE": 18,
                    "HPSA_ESTIMATED_UNDERSERVED_POP": 8000,
                }
            },
        ]
        result = _aggregate_hpsa_by_county(features, "pc")
        assert len(result) == 2

        wayne = next(r for r in result if r["county"] == "Wayne")
        assert wayne["pc_hpsa_count"] == 2
        assert wayne["pc_hpsa_max_score"] == "20.0"
        assert wayne["pc_hpsa_avg_score"] == "15.0"
        assert wayne["pc_underserved_pop"] == "20000"

        oakland = next(r for r in result if r["county"] == "Oakland")
        assert oakland["pc_hpsa_count"] == 1
        assert oakland["pc_hpsa_max_score"] == "18.0"

    def test_aggregate_hpsa_unknown_fips(self):
        features = [
            {
                "attributes": {
                    "CMN_STATE_COUNTY_FIPS_CD": "99999",
                    "HPSA_SCORE": 20,
                    "HPSA_ESTIMATED_UNDERSERVED_POP": 5000,
                }
            },
        ]
        result = _aggregate_hpsa_by_county(features, "mh")
        assert len(result) == 0

    def test_fetch_hpsa_data_error(self):
        with respx.mock:
            respx.get(PC_HPSA_URL).mock(
                return_value=Response(500, text="Server Error")
            )
            with pytest.raises(HRSAAPIError, match="500"):
                fetch_hpsa_data("/9/query")

    def test_fetch_hpsa_data_success(self):
        response = build_hpsa_response()
        with respx.mock:
            respx.get(PC_HPSA_URL).mock(
                return_value=Response(200, json=response)
            )
            result = fetch_hpsa_data("/9/query")
        assert len(result) == 3
        assert all("attributes" in f for f in result)
