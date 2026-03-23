from __future__ import annotations

import pytest
import respx
from httpx import Response

from acs_cli.census_api.client import ACS_BASE_URL, MICHIGAN_FIPS, ZCTA_FIELD
from acs_cli.places_api.client import PLACES_BASE_URL
from acs_cli.cms_api.client import HOSPITAL_BASE_URL
from acs_cli.hrsa_api.client import HRSA_BASE_URL
from acs_cli.bls_api.client import BLS_BASE_URL, QCEW_BASE_URL
from acs_cli.hrsa_api.ahrf import AHRF_HP_FILENAME


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


# ── ZCTA helpers ────────────────────────────────────────────────────────

MOCK_ZCTAS = [
    ("ZCTA5 48103", "48103"),
    ("ZCTA5 48104", "48104"),
    ("ZCTA5 49001", "49001"),
]


def build_zcta_census_response(
    variable_codes: list[str],
    zctas: list[tuple[str, str]] | None = None,
    values_fn=None,
) -> list[list[str]]:
    """Build a Census API JSON response in ZCTA format (list-of-lists)."""
    if zctas is None:
        zctas = MOCK_ZCTAS

    if values_fn is None:
        counter = {"n": 1000}

        def values_fn(_zi, _code):
            counter["n"] += 1
            return str(counter["n"])

    header = ["NAME"] + variable_codes + [ZCTA_FIELD]
    rows = []
    for zi, (name, code) in enumerate(zctas):
        row = [name] + [values_fn(zi, c) for c in variable_codes] + [code]
        rows.append(row)
    return [header] + rows


@pytest.fixture()
def mock_census_zcta(api_key):
    """Activate respx and return a helper to register mock Census ZCTA responses."""
    with respx.mock(assert_all_called=False) as router:

        def _register(
            year: int = 2024,
            codes: list[str] | None = None,
            response: list[list[str]] | None = None,
            zctas: list[tuple[str, str]] | None = None,
        ):
            if codes is None:
                codes = ["B01003_001E"]
            if response is None:
                response = build_zcta_census_response(codes, zctas=zctas)
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


# ── BLS helpers ─────────────────────────────────────────────────────────────

MOCK_BLS_FIPS = ["26161", "26163", "26125"]  # Washtenaw, Wayne, Oakland


def build_bls_response(
    series_ids: list[str] | None = None,
    year: str = "2024",
    period: str = "M13",
    status: str = "REQUEST_SUCCEEDED",
    base_value: float = 5.0,
) -> dict:
    """Build a BLS v2 API JSON response."""
    series_list = []
    if series_ids is None:
        series_ids = []
    for i, sid in enumerate(series_ids):
        series_list.append({
            "seriesID": sid,
            "data": [
                {
                    "year": year,
                    "period": period,
                    "periodName": "Annual",
                    "value": str(round(base_value + i * 0.5, 1)),
                    "footnotes": [{}],
                }
            ],
        })
    return {
        "status": status,
        "responseTime": 100,
        "message": [],
        "Results": {
            "series": series_list,
        },
    }


@pytest.fixture()
def bls_api_key(monkeypatch):
    """Ensure BLS_API_KEY is set."""
    monkeypatch.setenv("BLS_API_KEY", "test-bls-key-123")
    return "test-bls-key-123"


@pytest.fixture()
def mock_bls(bls_api_key):
    """Activate respx and return a helper to register BLS POST mocks."""
    with respx.mock(assert_all_called=False) as router:

        def _register(
            response: dict | None = None,
            status_code: int = 200,
        ):
            if response is None and status_code == 200:
                response = build_bls_response()
            route = router.post(BLS_BASE_URL).mock(
                return_value=Response(status_code, json=response or {"status": "REQUEST_FAILED", "Results": {"series": []}, "message": []})
            )
            return route

        yield _register


# ── QCEW helpers ───────────────────────────────────────────────────────────

QCEW_BASE_URL_PATTERN = "https://data.bls.gov/cew/data/api/"


def build_qcew_csv_response(
    avg_annual_pay: str = "45000",
    annual_avg_estabs: str = "1200",
    hc_employment: str = "500",
    hc_establishments: str = "50",
) -> str:
    """Build a QCEW CSV response with the two rows we filter for."""
    header = "area_fips,own_code,industry_code,agglvl_code,size_code,year,qtr,disclosure_code,annual_avg_estabs,annual_avg_emplvl,total_annual_wages,taxable_annual_wages,annual_contributions,annual_avg_wkly_wage,avg_annual_pay"
    # Total all-industries row (own=0, ind=10, agg=70)
    row1 = f"26001,0,10,70,0,2024,A,N,{annual_avg_estabs},5000,225000000,200000000,0,865,{avg_annual_pay}"
    # Healthcare row (own=5, ind=62, agg=74)
    row2 = f"26001,5,62,74,0,2024,A,N,{hc_establishments},{hc_employment},20000000,18000000,0,770,38000"
    # Extra row that should be ignored
    row3 = "26001,5,44,74,0,2024,A,N,100,200,8000000,7000000,0,600,31000"
    return header + "\n" + row1 + "\n" + row2 + "\n" + row3 + "\n"


@pytest.fixture()
def mock_qcew():
    """Activate respx and return a helper to register QCEW GET mocks."""
    with respx.mock(assert_all_called=False) as router:

        def _register(
            csv_text: str | None = None,
            status_code: int = 200,
        ):
            if csv_text is None and status_code == 200:
                csv_text = build_qcew_csv_response()
            router.get(url__startswith=QCEW_BASE_URL_PATTERN).mock(
                return_value=Response(
                    status_code,
                    text=csv_text or "",
                    headers={"content-type": "text/csv"},
                )
            )

        yield _register


# ── AHRF helpers ────────────────────────────────────────────────────────────


def build_ahrf_csv_text(
    pc_physicians: str = "120",
    total_mds: str = "250",
    total_dos: str = "80",
    nps: str = "45",
    pas: str = "30",
    dentists: str = "60",
) -> str:
    """Build a minimal AHRF hp.csv with Michigan + non-Michigan rows."""
    header = "fips_st_cnty,phys_nf_prim_care_pc_exc_rsdt_23,md_nf_activ_23,do_nf_activ_23,np_npi_24,pa_npi_24,dent_npi_24"
    # Michigan rows
    row1 = f"26161,{pc_physicians},{total_mds},{total_dos},{nps},{pas},{dentists}"  # Washtenaw
    row2 = f"26163,100,200,60,35,25,50"  # Wayne
    row3 = f"26125,150,300,90,55,40,70"  # Oakland
    # Non-Michigan row (should be filtered out)
    row4 = "36001,200,400,100,60,45,80"  # Albany, NY
    return header + "\n" + row1 + "\n" + row2 + "\n" + row3 + "\n" + row4 + "\n"


@pytest.fixture()
def mock_ahrf(tmp_path, monkeypatch):
    """Patch _download_ahrf_csv to write a fake CSV and return its path."""
    csv_path = tmp_path / AHRF_HP_FILENAME
    csv_path.write_text(build_ahrf_csv_text())

    def _fake_download(cache_dir=None):
        return csv_path

    monkeypatch.setattr(
        "acs_cli.hrsa_api.ahrf._download_ahrf_csv",
        _fake_download,
    )
    return csv_path
