from __future__ import annotations

import csv
from dataclasses import dataclass

import httpx

from acs_cli import clean_county_name

# ── Constants ────────────────────────────────────────────────────────────────

PLACES_BASE_URL = "https://data.cdc.gov/resource/swc5-untb.json"
MICHIGAN_STATE_ABBR = "MI"
DEFAULT_PLACES_YEAR = 2023


# ── Data model ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Measure:
    measureid: str
    label: str
    short_question: str


PLACES_MEASURES: dict[str, list[Measure]] = {
    "chronic_disease": [
        Measure("DIABETES", "Diabetes", "Diagnosed diabetes among adults"),
        Measure("COPD", "COPD", "Chronic obstructive pulmonary disease among adults"),
        Measure("CHD", "Coronary Heart Disease", "Coronary heart disease among adults"),
        Measure("OBESITY", "Obesity", "Obesity among adults"),
        Measure("CSMOKING", "Smoking", "Current smoking among adults"),
        Measure("CASTHMA", "Current Asthma", "Current asthma among adults"),
        Measure("STROKE", "Stroke", "Stroke among adults"),
        Measure("BPHIGH", "High Blood Pressure", "High blood pressure among adults"),
        Measure("HIGHCHOL", "High Cholesterol", "High cholesterol among adults"),
        Measure("DEPRESSION", "Depression", "Depression among adults"),
        Measure("ARTHRITIS", "Arthritis", "Arthritis among adults"),
        Measure("CANCER", "Cancer (excl skin)", "Cancer (non-skin) among adults"),
    ],
    "health_behaviors": [
        Measure("BINGE", "Binge Drinking", "Binge drinking among adults"),
        Measure("LPA", "Physical Inactivity", "No leisure-time physical activity among adults"),
        Measure("SLEEP", "Short Sleep", "Sleeping less than 7 hours among adults"),
    ],
    "prevention": [
        Measure("CHECKUP", "Annual Checkup", "Visits to doctor for routine checkup"),
        Measure("DENTAL", "Dental Visit", "Visits to dentist or dental clinic"),
        Measure("CHOLSCREEN", "Cholesterol Screening", "Cholesterol screening among adults"),
        Measure("MAMMOUSE", "Mammography", "Mammography use among women 50-74"),
        Measure("COLON_SCREEN", "Colorectal Screening", "Colorectal cancer screening among adults 45-75"),
    ],
    "disability": [
        Measure("DISABILITY", "Any Disability", "Any disability among adults"),
        Measure("HEARING", "Hearing Disability", "Hearing disability among adults"),
        Measure("VISION", "Vision Disability", "Vision disability among adults"),
        Measure("COGNITION", "Cognitive Disability", "Cognitive disability among adults"),
        Measure("MOBILITY", "Mobility Disability", "Mobility disability among adults"),
    ],
    "sdoh": [
        Measure("FOODINSECU", "Food Insecurity", "Food insecurity among adults"),
        Measure("HOUSINSECU", "Housing Insecurity", "Housing insecurity among adults"),
        Measure("LACKTRPT", "Transportation Barriers", "Lack of transportation among adults"),
    ],
    "mental_health": [
        Measure("MHLTH", "Frequent Mental Distress", "Frequent mental distress among adults"),
        Measure("EMOTIONSPT", "Lack of Emotional Support", "Lack of social and emotional support among adults"),
        Measure("LONELINESS", "Loneliness", "Loneliness among adults"),
    ],
}


# ── Errors ───────────────────────────────────────────────────────────────────


class PlacesAPIError(Exception):
    def __init__(self, status_code: int, detail: str = ""):
        self.status_code = status_code
        super().__init__(f"CDC PLACES API returned {status_code}" + (f": {detail}" if detail else ""))


# ── Resolve measures ─────────────────────────────────────────────────────────


def resolve_measures(groups: list[str]) -> list[Measure]:
    if "all" in groups:
        groups = list(PLACES_MEASURES.keys())
    measures: list[Measure] = []
    for g in groups:
        if g not in PLACES_MEASURES:
            raise ValueError(
                f"Unknown PLACES group '{g}'. "
                f"Available: {', '.join(PLACES_MEASURES.keys())}"
            )
        measures.extend(PLACES_MEASURES[g])
    return measures


# ── Fetch data ───────────────────────────────────────────────────────────────


def fetch_places_data(
    measures: list[Measure],
    year: int = DEFAULT_PLACES_YEAR,
    prevalence_type: str = "age_adjusted",
) -> list[dict]:
    """Query CDC PLACES SODA API and pivot long→wide (one row per county)."""
    measure_ids = [m.measureid for m in measures]
    data_value_col = "data_value"
    datavaluetypeid = "AgeAdjPrv" if prevalence_type == "age_adjusted" else "CrdPrv"

    all_records: list[dict] = []
    limit = 10000
    offset = 0

    while True:
        params: dict[str, str | int] = {
            "$where": (
                f"stateabbr='{MICHIGAN_STATE_ABBR}' "
                f"AND year='{year}' "
                f"AND datavaluetypeid='{datavaluetypeid}' "
                f"AND locationname != '{MICHIGAN_STATE_ABBR}' "
                f"AND measureid in({','.join(repr(m) for m in measure_ids)})"
            ),
            "$select": f"locationname,measureid,{data_value_col}",
            "$limit": limit,
            "$offset": offset,
        }

        resp = httpx.get(PLACES_BASE_URL, params=params, timeout=30)
        if resp.status_code != 200:
            raise PlacesAPIError(resp.status_code, resp.text[:200])

        batch = resp.json()
        if not batch:
            break
        all_records.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return _pivot_rows(all_records, measures, data_value_col)


def _pivot_rows(
    records: list[dict],
    measures: list[Measure],
    value_col: str,
) -> list[dict]:
    """Pivot long CDC rows into one row per county with measures as columns."""
    by_county: dict[str, dict] = {}
    for rec in records:
        county = clean_county_name(rec.get("locationname", ""))
        if county not in by_county:
            by_county[county] = {"locationname": county}
        mid = rec.get("measureid", "")
        raw = rec.get(value_col, "")
        try:
            by_county[county][mid] = str(round(float(raw) / 100, 4))
        except (ValueError, TypeError):
            by_county[county][mid] = raw

    return sorted(by_county.values(), key=lambda r: r.get("locationname", ""))


# ── CSV output ───────────────────────────────────────────────────────────────


def write_places_csv(
    rows: list[dict],
    measures: list[Measure],
    writer: csv.writer,
    county_filter: str | None = None,
    sort_col: str | None = None,
    header: bool = True,
) -> int:
    if county_filter:
        filt = county_filter.lower()
        rows = [r for r in rows if filt in r.get("locationname", "").lower()]

    if not rows:
        return 0

    if sort_col:
        target_id = None
        for m in measures:
            if m.label.lower() == sort_col.lower():
                target_id = m.measureid
                break
        if target_id is None:
            target_id = sort_col
        rows.sort(key=lambda r: float(r.get(target_id, 0) or 0), reverse=True)

    if header:
        columns = ["County"] + [m.label for m in measures]
        writer.writerow(columns)

    for row in rows:
        csv_row = [row.get("locationname", "")]
        for m in measures:
            csv_row.append(row.get(m.measureid, ""))
        writer.writerow(csv_row)

    return len(rows)
