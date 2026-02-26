from __future__ import annotations

import pytest
import respx
from httpx import Response

from acs_cli.census_api.client import ACS_BASE_URL, MICHIGAN_FIPS
from acs_cli.places_api.client import PLACES_BASE_URL
from acs_cli.cms_api.client import HOSPITAL_BASE_URL
from acs_cli.hrsa_api.client import HRSA_BASE_URL


# ── Helpers ──────────────────────────────────────────────────────────────────

MOCK_COUNTIES = [
    ("Washtenaw County, Michigan", "161"),
    ("Wayne County, Michigan", "163"),
    ("Oakland County, Michigan", "125"),
]


def census_url(year: int = 2024) -> str:
    return ACS_BASE_URL.format(year=year)


def build_census_response(
    variable_codes: list[str],
    counties: list[tuple[str, str]] | None = None,
    values_fn=None,
) -> list[list[str]]:
    """Build a Census API JSON response (list-of-lists format).

    values_fn(county_index, var_code) -> str  controls per-cell values.
    Defaults to sequential integers starting at 1000.
    """
    if counties is None:
        counties = MOCK_COUNTIES

    if values_fn is None:
        counter = {"n": 1000}

        def values_fn(_ci, _code):
            counter["n"] += 1
            return str(counter["n"])

    header = ["NAME"] + variable_codes + ["state", "county"]
    rows = []
    for ci, (name, fips) in enumerate(counties):
        row = [name] + [values_fn(ci, c) for c in variable_codes] + [MICHIGAN_FIPS, fips]
        rows.append(row)
    return [header] + rows


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def api_key(monkeypatch):
    """Ensure CENSUS_API_KEY is set for every test that needs it."""
    monkeypatch.setenv("CENSUS_API_KEY", "test-key-123")
    return "test-key-123"


@pytest.fixture()
def no_api_key(monkeypatch, tmp_path):
    """Ensure no API key is available (env or config file)."""
    monkeypatch.setenv("CENSUS_API_KEY", "")
    # Also prevent the config file fallback from finding a key
    monkeypatch.setattr(
        "acs_cli.census_api.client.CONFIG_FILE",
        tmp_path / "nonexistent_config",
    )


@pytest.fixture()
def mock_census(api_key):
    """Activate respx and return a helper to register mock Census responses.

    Usage:
        def test_something(mock_census):
            mock_census(year=2024, codes=["B01003_001E"], response=[[...], [...]])
    """
    with respx.mock(assert_all_called=False) as router:

        def _register(
            year: int = 2024,
            codes: list[str] | None = None,
            response: list[list[str]] | None = None,
            counties: list[tuple[str, str]] | None = None,
        ):
            if codes is None:
                codes = ["B01003_001E"]
            if response is None:
                response = build_census_response(codes, counties=counties)
            route = router.get(census_url(year)).mock(
                return_value=Response(200, json=response)
            )
            return route

        yield _register


# ── PLACES helpers ───────────────────────────────────────────────────────────

MOCK_PLACES_COUNTIES = ["Washtenaw", "Wayne", "Oakland"]


def build_places_response(
    measure_ids: list[str],
    counties: list[str] | None = None,
    value_col: str = "data_value",
    base_value: float = 10.0,
) -> list[dict]:
    """Build a CDC PLACES SODA API response (list of dicts, long format)."""
    if counties is None:
        counties = MOCK_PLACES_COUNTIES
    records = []
    for ci, county in enumerate(counties):
        for mi, mid in enumerate(measure_ids):
            records.append({
                "locationname": county,
                "measureid": mid,
                value_col: str(round(base_value + ci * 2 + mi * 0.5, 1)),
            })
    return records


@pytest.fixture()
def mock_places():
    """Activate respx and return a helper to register mock PLACES responses."""
    with respx.mock(assert_all_called=False) as router:

        def _register(
            measure_ids: list[str] | None = None,
            response: list[dict] | None = None,
            counties: list[str] | None = None,
            status_code: int = 200,
            value_col: str = "data_value",
        ):
            if measure_ids is None:
                measure_ids = ["DIABETES"]
            if response is None and status_code == 200:
                response = build_places_response(
                    measure_ids, counties=counties, value_col=value_col,
                )
            route = router.get(PLACES_BASE_URL).mock(
                return_value=Response(status_code, json=response or [])
            )
            return route

        yield _register


# ── CMS helpers ──────────────────────────────────────────────────────────────

MOCK_ACCESS_COUNTIES = ["Wayne", "Oakland", "Washtenaw"]


def build_hospital_response(
    counties: list[str] | None = None,
) -> dict:
    """Build a CMS hospital API response."""
    if counties is None:
        counties = MOCK_ACCESS_COUNTIES
    results = []
    for i, county in enumerate(counties):
        results.append({
            "facility_name": f"Hospital {i+1} in {county}",
            "state": "MI",
            "countyparish": county.upper(),
            "hospital_type": "Acute Care Hospitals",
            "emergency_services": "Yes",
            "hospital_overall_rating": str(3 + (i % 3)),
            "meets_criteria_for_birthing_friendly_designation": "Y" if i % 2 == 0 else "N",
        })
    return {"results": results}


@pytest.fixture()
def mock_cms():
    """Activate respx and return a helper to register CMS hospital API mocks."""
    with respx.mock(assert_all_called=False) as router:

        def _register(
            hospital_response: dict | None = None,
            hospital_status: int = 200,
        ):
            if hospital_response is None and hospital_status == 200:
                hospital_response = build_hospital_response()
            router.get(HOSPITAL_BASE_URL).mock(
                return_value=Response(hospital_status, json=hospital_response or {"results": []})
            )

        yield _register


# ── HRSA helpers ─────────────────────────────────────────────────────────────


def build_hpsa_response(
    fips_codes: list[str] | None = None,
) -> dict:
    """Build an HRSA HPSA ArcGIS API response."""
    if fips_codes is None:
        fips_codes = ["26163", "26125", "26161"]  # Wayne, Oakland, Washtenaw
    features = []
    for i, fips in enumerate(fips_codes):
        features.append({
            "attributes": {
                "CMN_STATE_COUNTY_FIPS_CD": fips,
                "HPSA_SCORE": 15 + i * 5,
                "HPSA_STATUS_DESC": "Designated",
                "HPSA_DEGREE_OF_SHORTAGE": 3 + i,
                "HPSA_FORMAL_RATIO": "3500:1",
                "HPSA_ESTIMATED_UNDERSERVED_POP": 10000 + i * 5000,
            }
        })
    return {"features": features, "exceededTransferLimit": False}


PC_HPSA_URL = HRSA_BASE_URL + "/9/query"
MH_HPSA_URL = HRSA_BASE_URL + "/5/query"


@pytest.fixture()
def mock_hrsa():
    """Activate respx and return a helper to register HRSA HPSA mocks."""
    with respx.mock(assert_all_called=False) as router:

        def _register(
            pc_response: dict | None = None,
            mh_response: dict | None = None,
            pc_status: int = 200,
            mh_status: int = 200,
        ):
            if pc_response is None and pc_status == 200:
                pc_response = build_hpsa_response()
            router.get(PC_HPSA_URL).mock(
                return_value=Response(pc_status, json=pc_response or {"features": []})
            )
            if mh_response is None and mh_status == 200:
                mh_response = build_hpsa_response()
            router.get(MH_HPSA_URL).mock(
                return_value=Response(mh_status, json=mh_response or {"features": []})
            )

        yield _register
