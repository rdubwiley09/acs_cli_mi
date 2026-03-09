from __future__ import annotations

import csv
import io
import os
from dataclasses import dataclass
from pathlib import Path

import httpx

from acs_cli.hrsa_api.client import MI_FIPS_TO_COUNTY

# ── Constants ────────────────────────────────────────────────────────────────

BLS_BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
QCEW_BASE_URL = "https://data.bls.gov/cew/data/api/{year}/a/area/{fips}.csv"
DEFAULT_QCEW_YEAR = 2024

BLS_CONFIG_DIR = Path.home() / ".config" / "acs-cli"
BLS_CONFIG_FILE = BLS_CONFIG_DIR / "bls_config"

DEFAULT_BLS_YEAR = 2024
DEFAULT_PERIOD = "M13"  # Annual average

MAX_SERIES_PER_REQUEST = 50  # BLS API limit

# LAUS series code suffixes
LAUS_CODES: dict[str, str] = {
    "03": "unemployment_rate",
    "04": "unemployment",
    "05": "employment",
    "06": "labor_force",
}


# ── Data model ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class EconomyMeasure:
    measure_id: str
    label: str
    description: str
    series_code: str


ECONOMY_MEASURES: dict[str, list[EconomyMeasure]] = {
    "unemployment": [
        EconomyMeasure(
            "unemployment_rate",
            "Unemployment Rate (%)",
            "Unemployment rate as a percentage of the labor force",
            "03",
        ),
        EconomyMeasure(
            "unemployment",
            "Unemployment",
            "Number of unemployed persons",
            "04",
        ),
    ],
    "employment": [
        EconomyMeasure(
            "employment",
            "Employment",
            "Number of employed persons",
            "05",
        ),
        EconomyMeasure(
            "labor_force",
            "Labor Force",
            "Total labor force size",
            "06",
        ),
    ],
}


@dataclass(frozen=True)
class QCEWMeasure:
    measure_id: str
    label: str
    description: str
    own_code: str
    industry_code: str
    agglvl_code: str
    csv_column: str


QCEW_MEASURES: dict[str, list[QCEWMeasure]] = {
    "wages": [
        QCEWMeasure(
            "qcew_avg_annual_pay",
            "Avg Annual Pay",
            "Average annual pay across all industries",
            "0", "10", "70",
            "avg_annual_pay",
        ),
        QCEWMeasure(
            "qcew_establishments",
            "Establishments",
            "Total number of business establishments",
            "0", "10", "70",
            "annual_avg_estabs",
        ),
    ],
    "healthcare": [
        QCEWMeasure(
            "qcew_healthcare_employment",
            "HC Employment",
            "Healthcare sector employment (NAICS 62)",
            "5", "62", "74",
            "annual_avg_emplvl",
        ),
        QCEWMeasure(
            "qcew_healthcare_establishments",
            "HC Establishments",
            "Healthcare sector establishments (NAICS 62)",
            "5", "62", "74",
            "annual_avg_estabs",
        ),
    ],
}


# ── Errors ───────────────────────────────────────────────────────────────────


class BLSAPIError(Exception):
    def __init__(self, status_code: int, detail: str = ""):
        self.status_code = status_code
        super().__init__(
            f"BLS API returned {status_code}" + (f": {detail}" if detail else "")
        )


class MissingBLSKeyError(Exception):
    pass


# ── API key management ──────────────────────────────────────────────────────


def save_bls_api_key(api_key: str) -> Path:
    BLS_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    BLS_CONFIG_FILE.write_text(api_key.strip())
    BLS_CONFIG_FILE.chmod(0o600)
    return BLS_CONFIG_FILE


def get_bls_api_key() -> str:
    key = os.environ.get("BLS_API_KEY")
    if key:
        return key
    if BLS_CONFIG_FILE.exists():
        stored = BLS_CONFIG_FILE.read_text().strip()
        if stored:
            return stored
    raise MissingBLSKeyError(
        "No BLS API key found.\n"
        "Run 'acs-cli-mi bls-login' to save your key, or set BLS_API_KEY in .env\n"
        "Get a free key at: https://data.bls.gov/registrationEngine/"
    )


# ── Series ID helpers ───────────────────────────────────────────────────────


def _build_series_id(fips: str, code: str) -> str:
    """Build a LAUS series ID for a Michigan county.

    Format: LAUCN{fips}00000000{code}  (20 chars total)
    fips is a 5-digit string like "26163".
    """
    return f"LAUCN{fips}00000000{code}"


# ── Resolve measures ────────────────────────────────────────────────────────


def resolve_economy_measures(groups: list[str]) -> list[EconomyMeasure]:
    if "all" in groups:
        groups = list(ECONOMY_MEASURES.keys())
    measures: list[EconomyMeasure] = []
    for g in groups:
        if g not in ECONOMY_MEASURES:
            raise ValueError(
                f"Unknown economy group '{g}'. "
                f"Available: {', '.join(ECONOMY_MEASURES.keys())}"
            )
        measures.extend(ECONOMY_MEASURES[g])
    return measures


# ── Fetch economy data ──────────────────────────────────────────────────────


def fetch_economy_data(
    measures: list[EconomyMeasure],
    year: int = DEFAULT_BLS_YEAR,
    api_key: str = "",
    period: str = DEFAULT_PERIOD,
) -> list[dict]:
    """Fetch LAUS data for all 83 Michigan counties.

    Builds series IDs for each county × measure combination,
    batches into chunks of 50 (BLS limit), and reassembles by county.
    """
    fips_list = sorted(MI_FIPS_TO_COUNTY.keys())

    # Build all series IDs and track mapping: series_id -> (fips, measure)
    series_map: dict[str, tuple[str, EconomyMeasure]] = {}
    for fips in fips_list:
        for m in measures:
            sid = _build_series_id(fips, m.series_code)
            series_map[sid] = (fips, m)

    all_series = list(series_map.keys())
    chunks = [
        all_series[i : i + MAX_SERIES_PER_REQUEST]
        for i in range(0, len(all_series), MAX_SERIES_PER_REQUEST)
    ]

    # Accumulate results by county
    by_county: dict[str, dict] = {}

    for chunk in chunks:
        payload = {
            "seriesid": chunk,
            "startyear": str(year),
            "endyear": str(year),
            "registrationkey": api_key,
        }
        resp = httpx.post(BLS_BASE_URL, json=payload, timeout=30)
        if resp.status_code != 200:
            raise BLSAPIError(resp.status_code, resp.text[:200])

        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            msg = "; ".join(data.get("message", []))
            raise BLSAPIError(resp.status_code, msg or "Request failed")

        for series in data.get("Results", {}).get("series", []):
            sid = series.get("seriesID", "")
            if sid not in series_map:
                continue
            fips, measure = series_map[sid]
            county = MI_FIPS_TO_COUNTY.get(fips, "")
            if not county:
                continue

            if county not in by_county:
                by_county[county] = {"county": county}

            # Find the matching period; fall back to latest month if
            # the requested period (e.g. M13 annual avg) isn't available.
            year_data = [
                dp for dp in series.get("data", [])
                if dp.get("year") == str(year)
            ]
            match = next(
                (dp for dp in year_data if dp.get("period") == period),
                None,
            )
            if match is None and year_data:
                # Pick the latest month (highest period string)
                match = max(year_data, key=lambda dp: dp.get("period", ""))
            if match:
                by_county[county][measure.measure_id] = match.get("value", "")

    return sorted(by_county.values(), key=lambda r: r.get("county", ""))


# ── CSV output ──────────────────────────────────────────────────────────────


def write_economy_csv(
    rows: list[dict],
    measures: list[EconomyMeasure],
    writer: csv.writer,
    county_filter: str | None = None,
    sort_col: str | None = None,
    header: bool = True,
) -> int:
    if county_filter:
        filt = county_filter.lower()
        rows = [r for r in rows if filt in r.get("county", "").lower()]

    if not rows:
        return 0

    if sort_col:
        target_id = None
        for m in measures:
            if m.label.lower() == sort_col.lower():
                target_id = m.measure_id
                break
        if target_id is None:
            target_id = sort_col
        rows.sort(key=lambda r: float(r.get(target_id, 0) or 0), reverse=True)

    if header:
        columns = ["County"] + [m.label for m in measures]
        writer.writerow(columns)

    for row in rows:
        csv_row = [row.get("county", "")]
        for m in measures:
            csv_row.append(row.get(m.measure_id, ""))
        writer.writerow(csv_row)

    return len(rows)


# ── QCEW resolve measures ─────────────────────────────────────────────────


def resolve_qcew_measures(groups: list[str]) -> list[QCEWMeasure]:
    if "all" in groups:
        groups = list(QCEW_MEASURES.keys())
    measures: list[QCEWMeasure] = []
    for g in groups:
        if g not in QCEW_MEASURES:
            raise ValueError(
                f"Unknown QCEW group '{g}'. "
                f"Available: {', '.join(QCEW_MEASURES.keys())}"
            )
        measures.extend(QCEW_MEASURES[g])
    return measures


# ── Fetch QCEW data ───────────────────────────────────────────────────────


def fetch_qcew_data(
    measures: list[QCEWMeasure],
    year: int = DEFAULT_QCEW_YEAR,
) -> list[dict]:
    """Fetch QCEW data for all 83 Michigan counties.

    One GET request per county. Filters CSV rows by (own_code, industry_code,
    agglvl_code) and extracts the named csv_column for each measure.
    """
    fips_list = sorted(MI_FIPS_TO_COUNTY.keys())

    # Build lookup: (own_code, industry_code, agglvl_code) -> list of measures
    filter_map: dict[tuple[str, str, str], list[QCEWMeasure]] = {}
    for m in measures:
        key = (m.own_code, m.industry_code, m.agglvl_code)
        filter_map.setdefault(key, []).append(m)

    results: list[dict] = []

    for fips in fips_list:
        county = MI_FIPS_TO_COUNTY.get(fips, "")
        if not county:
            continue

        row_data: dict[str, str] = {"county": county}

        url = QCEW_BASE_URL.format(year=year, fips=fips)
        try:
            resp = httpx.get(url, timeout=30)
            if resp.status_code == 200:
                reader = csv.DictReader(io.StringIO(resp.text))
                for csv_row in reader:
                    own = csv_row.get("own_code", "").strip()
                    ind = csv_row.get("industry_code", "").strip()
                    agg = csv_row.get("agglvl_code", "").strip()
                    key = (own, ind, agg)
                    if key in filter_map:
                        for m in filter_map[key]:
                            val = csv_row.get(m.csv_column, "").strip()
                            if val:
                                row_data[m.measure_id] = val
        except (httpx.TimeoutException, httpx.HTTPError):
            pass

        results.append(row_data)

    return sorted(results, key=lambda r: r.get("county", ""))


# ── QCEW CSV output ──────────────────────────────────────────────────────


def write_qcew_csv(
    rows: list[dict],
    measures: list[QCEWMeasure],
    writer: csv.writer,
    county_filter: str | None = None,
    sort_col: str | None = None,
    header: bool = True,
) -> int:
    if county_filter:
        filt = county_filter.lower()
        rows = [r for r in rows if filt in r.get("county", "").lower()]

    if not rows:
        return 0

    if sort_col:
        target_id = None
        for m in measures:
            if m.label.lower() == sort_col.lower():
                target_id = m.measure_id
                break
        if target_id is None:
            target_id = sort_col
        rows.sort(key=lambda r: float(r.get(target_id, 0) or 0), reverse=True)

    if header:
        columns = ["County"] + [m.label for m in measures]
        writer.writerow(columns)

    for row in rows:
        csv_row = [row.get("county", "")]
        for m in measures:
            csv_row.append(row.get(m.measure_id, ""))
        writer.writerow(csv_row)

    return len(rows)
