"""Integration tests for the ACS CLI."""

from __future__ import annotations

import csv
import io

import pytest
import respx
from httpx import Response
from typer.testing import CliRunner

from acs_cli.cli import app
from acs_cli.census_api.client import (
    clean_county_name,
    format_value,
    resolve_variables,
)
from acs_cli.topics import TOPICS
from tests.conftest import MOCK_COUNTIES, build_census_response, census_url

runner = CliRunner()


def parse_csv(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


# ── topics command ───────────────────────────────────────────────────────────


class TestTopicsCommand:
    def test_lists_all_topics(self):
        result = runner.invoke(app, ["topics"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header == ["Topic", "Variable Code", "Label", "Format"]
        topic_names = {r[0] for r in rows[1:]}
        assert topic_names == set(TOPICS.keys())

    def test_row_count_matches_variables(self):
        result = runner.invoke(app, ["topics"])
        rows = parse_csv(result.stdout)
        expected = sum(len(vs) for vs in TOPICS.values())
        assert len(rows) - 1 == expected  # minus header

    def test_variable_codes_present(self):
        result = runner.invoke(app, ["topics"])
        rows = parse_csv(result.stdout)
        codes = {r[1] for r in rows[1:]}
        assert "B01003_001E" in codes  # population total
        assert "B19013_001E" in codes  # median household income


# ── query command ────────────────────────────────────────────────────────────


class TestQueryCommand:
    def test_missing_api_key(self, no_api_key):
        with respx.mock:
            result = runner.invoke(app, ["query", "population"])
        assert result.exit_code == 1
        assert "API key" in result.stderr or "CENSUS_API_KEY" in result.stderr

    def test_no_topic_or_variable(self, api_key):
        result = runner.invoke(app, ["query"])
        assert result.exit_code == 1
        assert "Provide topic" in result.stderr

    def test_unknown_topic(self, api_key):
        with respx.mock:
            result = runner.invoke(app, ["query", "nonexistent_topic"])
        assert result.exit_code == 1
        assert "Unknown topic" in result.stderr

    def test_single_topic(self, mock_census):
        codes = [v.code for v in TOPICS["age"]]
        mock_census(codes=codes)

        result = runner.invoke(app, ["query", "age"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header[0] == "County"
        assert "Median Age" in header
        assert len(rows) == 1 + len(MOCK_COUNTIES)

    def test_multiple_topics(self, mock_census):
        codes = [v.code for v in TOPICS["age"]] + [v.code for v in TOPICS["poverty"]]
        mock_census(codes=codes)

        result = runner.invoke(app, ["query", "age", "poverty"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert "Median Age" in header
        assert "Below Poverty Level" in header

    def test_all_topics(self, mock_census):
        codes = [v.code for topic_vars in TOPICS.values() for v in topic_vars]
        mock_census(codes=codes)

        result = runner.invoke(app, ["query", "all"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) > 1

    def test_raw_variable(self, mock_census):
        mock_census(codes=["B01003_001E"])

        result = runner.invoke(app, ["query", "--variable", "B01003_001E"])
        assert result.exit_code == 0

    def test_county_filter(self, mock_census):
        codes = [v.code for v in TOPICS["age"]]
        mock_census(codes=codes)

        result = runner.invoke(app, ["query", "age", "--county", "Washtenaw"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) == 2  # header + 1 matching county
        assert "Washtenaw" in rows[1][0]

    def test_county_filter_no_match(self, mock_census):
        codes = [v.code for v in TOPICS["age"]]
        mock_census(codes=codes)

        result = runner.invoke(app, ["query", "age", "--county", "Nonexistent"])
        assert "No matching rows" in result.stderr

    def test_sort_option(self, mock_census):
        codes = [v.code for v in TOPICS["age"]]
        counter = {"n": 0}

        def values_fn(ci, _code):
            counter["n"] += 1
            return str(counter["n"] * 10)

        resp = build_census_response(codes, values_fn=values_fn)
        mock_census(codes=codes, response=resp)

        result = runner.invoke(app, ["query", "age", "--sort", "Median Age"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        # Sorted descending by the Median Age column (index 1)
        ages = [r[1] for r in rows[1:]]
        ages_float = [float(a) for a in ages if a]
        assert ages_float == sorted(ages_float, reverse=True)

    def test_year_option(self, mock_census):
        codes = [v.code for v in TOPICS["age"]]
        mock_census(year=2019, codes=codes)

        result = runner.invoke(app, ["query", "age", "--year", "2019"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert len(rows) == 1 + len(MOCK_COUNTIES)

    def test_multi_year(self, mock_census):
        codes = [v.code for v in TOPICS["age"]]
        mock_census(year=2019, codes=codes)
        mock_census(year=2023, codes=codes)

        result = runner.invoke(app, ["query", "age", "--years", "2019,2023"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header[0] == "Year"
        assert header[1] == "County"
        # 3 counties x 2 years = 6 data rows
        assert len(rows) == 1 + len(MOCK_COUNTIES) * 2

    def test_output_to_file(self, mock_census, tmp_path):
        codes = [v.code for v in TOPICS["age"]]
        mock_census(codes=codes)

        outfile = str(tmp_path / "output.csv")
        result = runner.invoke(app, ["query", "age", "--output", outfile])
        assert result.exit_code == 0
        assert "Wrote CSV" in result.stderr

        with open(outfile) as f:
            rows = list(csv.reader(f))
        assert rows[0][0] == "County"
        assert len(rows) == 1 + len(MOCK_COUNTIES)

    def test_api_error_status(self, api_key):
        with respx.mock:
            respx.get(census_url()).mock(return_value=Response(500, text="Server Error"))
            result = runner.invoke(app, ["query", "age"])
        assert result.exit_code == 1
        assert "500" in result.stderr

    def test_invalid_api_key(self, api_key):
        with respx.mock:
            respx.get(census_url()).mock(return_value=Response(401, text="Unauthorized"))
            result = runner.invoke(app, ["query", "age"])
        assert result.exit_code == 1
        assert "API key" in result.stderr


# ── info command ─────────────────────────────────────────────────────────────


class TestInfoCommand:
    def test_missing_api_key(self, no_api_key):
        with respx.mock:
            result = runner.invoke(app, ["info", "Washtenaw"])
        assert result.exit_code == 1
        assert "API key" in result.stderr or "CENSUS_API_KEY" in result.stderr

    def test_county_profile(self, mock_census):
        all_vars = [v for vs in TOPICS.values() for v in vs]
        codes = [v.code for v in all_vars]
        mock_census(codes=codes)

        result = runner.invoke(app, ["info", "Washtenaw"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        header = rows[0]
        assert header == ["County", "Field", "Value"]
        assert len(rows) >= 2
        assert "Washtenaw" in rows[1][0]

    def test_county_not_found(self, mock_census):
        all_vars = [v for vs in TOPICS.values() for v in vs]
        codes = [v.code for v in all_vars]
        mock_census(codes=codes)

        result = runner.invoke(app, ["info", "Nonexistent"])
        assert result.exit_code == 1
        assert "No county matching" in result.stderr

    def test_info_with_year(self, mock_census):
        all_vars = [v for vs in TOPICS.values() for v in vs]
        codes = [v.code for v in all_vars]
        mock_census(year=2020, codes=codes)

        result = runner.invoke(app, ["info", "Wayne", "--year", "2020"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert any("Wayne" in r[0] for r in rows[1:])

    def test_info_case_insensitive(self, mock_census):
        all_vars = [v for vs in TOPICS.values() for v in vs]
        codes = [v.code for v in all_vars]
        mock_census(codes=codes)

        result = runner.invoke(app, ["info", "washtenaw"])
        assert result.exit_code == 0
        rows = parse_csv(result.stdout)
        assert any("Washtenaw" in r[0] for r in rows[1:])


# ── login command ────────────────────────────────────────────────────────────


class TestLoginCommand:
    def test_login_saves_key(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config"
        monkeypatch.setattr("acs_cli.census_api.client.CONFIG_DIR", tmp_path)
        monkeypatch.setattr("acs_cli.census_api.client.CONFIG_FILE", config_file)

        result = runner.invoke(app, ["login", "--api-key", "my-secret-key"])
        assert result.exit_code == 0
        assert "saved" in result.stderr.lower()
        assert config_file.read_text().strip() == "my-secret-key"


# ── Helper functions ─────────────────────────────────────────────────────────


class TestFormatValue:
    @pytest.mark.parametrize(
        "value, fmt, expected",
        [
            ("12345", "number", "12345"),
            ("12345", "dollar", "12345"),
            ("3.7", "decimal", "3.7"),
            ("45678.0", "number", "45678"),
            ("-666666666", "number", ""),
            ("-666666666.0", "dollar", ""),
            ("null", "number", ""),
            (None, "number", ""),
            ("-", "dollar", ""),
            ("None", "decimal", ""),
        ],
    )
    def test_format_value(self, value, fmt, expected):
        assert format_value(value, fmt) == expected


class TestCleanCountyName:
    def test_removes_county_and_michigan(self):
        assert clean_county_name("Washtenaw County, Michigan") == "Washtenaw"

    def test_removes_county_suffix(self):
        assert clean_county_name("Some County") == "Some"

    def test_no_suffix(self):
        assert clean_county_name("Washtenaw") == "Washtenaw"

    def test_st_to_saint(self):
        assert clean_county_name("St. Clair County, Michigan") == "Saint Clair"

    def test_st_to_saint_no_county(self):
        assert clean_county_name("St. Joseph") == "Saint Joseph"


class TestResolveVariables:
    def test_known_topic(self):
        result = resolve_variables(["age"], None)
        assert len(result) == len(TOPICS["age"])

    def test_all_topics(self):
        result = resolve_variables(["all"], None)
        expected = sum(len(vs) for vs in TOPICS.values())
        assert len(result) == expected

    def test_raw_variables(self):
        result = resolve_variables([], ["B01003_001E", "B19013_001E"])
        assert len(result) == 2
        assert result[0].code == "B01003_001E"

    def test_unknown_topic_raises(self):
        with pytest.raises(ValueError, match="Unknown topic"):
            resolve_variables(["bogus"], None)
