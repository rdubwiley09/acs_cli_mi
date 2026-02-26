from __future__ import annotations

import csv
from dataclasses import dataclass

import httpx

# ── Constants ────────────────────────────────────────────────────────────────

HOSPITAL_BASE_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"


# ── Data model ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AccessMeasure:
    measure_id: str
    label: str
    description: str


ACCESS_MEASURES: dict[str, list[AccessMeasure]] = {
    "hospital_access": [
        AccessMeasure("hospital_count", "Hospital Count", "Total hospitals in county"),
        AccessMeasure("acute_care_hospitals", "Acute Care Hospitals", "Acute care hospital count"),
        AccessMeasure("critical_access_hospitals", "Critical Access Hospitals", "Critical access hospital count"),
        AccessMeasure("emergency_services", "Emergency Services", "Hospitals with emergency services"),
        AccessMeasure("birthing_friendly", "Birthing Friendly", "Birthing-friendly designated hospitals"),
        AccessMeasure("avg_hospital_rating", "Avg Hospital Rating", "Average CMS overall hospital rating"),
    ],
}


# ── Errors ───────────────────────────────────────────────────────────────────


class CMSAPIError(Exception):
    def __init__(self, status_code: int, detail: str = ""):
        self.status_code = status_code
        super().__init__(f"CMS API returned {status_code}" + (f": {detail}" if detail else ""))


# ── Resolve measures ─────────────────────────────────────────────────────────


def resolve_access_measures(groups: list[str]) -> list[AccessMeasure]:
    if "all" in groups:
        groups = list(ACCESS_MEASURES.keys())
    measures: list[AccessMeasure] = []
    for g in groups:
        if g not in ACCESS_MEASURES:
            raise ValueError(
                f"Unknown access group '{g}'. "
                f"Available: {', '.join(ACCESS_MEASURES.keys())}"
            )
        measures.extend(ACCESS_MEASURES[g])
    return measures


# ── Fetch hospital data ─────────────────────────────────────────────────────


def fetch_hospital_data() -> list[dict]:
    """Fetch Michigan hospital records from CMS Provider Data."""
    all_records: list[dict] = []
    limit = 500
    offset = 0

    while True:
        params: dict[str, str | int] = {
            "offset": offset,
            "limit": limit,
            "conditions[0][property]": "state",
            "conditions[0][value]": "MI",
            "conditions[0][operator]": "=",
        }
        resp = httpx.get(HOSPITAL_BASE_URL, params=params, timeout=30)
        if resp.status_code != 200:
            raise CMSAPIError(resp.status_code, resp.text[:200])

        data = resp.json()
        results = data.get("results", []) if isinstance(data, dict) else data
        if not results:
            break
        all_records.extend(results)
        if len(results) < limit:
            break
        offset += limit

    return all_records


# ── Aggregate hospitals ─────────────────────────────────────────────────────


def _aggregate_hospitals_by_county(records: list[dict]) -> list[dict]:
    """Group hospital records by county and compute access metrics."""
    by_county: dict[str, dict] = {}

    for rec in records:
        county = rec.get("countyparish", "").strip()
        if not county:
            continue
        county = county.title()

        if county not in by_county:
            by_county[county] = {
                "county": county,
                "hospital_count": 0,
                "acute_care_hospitals": 0,
                "critical_access_hospitals": 0,
                "emergency_services": 0,
                "birthing_friendly": 0,
                "_ratings": [],
            }

        data = by_county[county]
        data["hospital_count"] += 1

        h_type = rec.get("hospital_type", "")
        if "Acute Care" in h_type:
            data["acute_care_hospitals"] += 1
        elif "Critical Access" in h_type:
            data["critical_access_hospitals"] += 1

        if rec.get("emergency_services", "").lower() in ("yes", "y", "true"):
            data["emergency_services"] += 1

        if rec.get("meets_criteria_for_birthing_friendly_designation", "").upper() == "Y":
            data["birthing_friendly"] += 1

        rating = rec.get("hospital_overall_rating", "")
        try:
            data["_ratings"].append(float(rating))
        except (ValueError, TypeError):
            pass

    for data in by_county.values():
        ratings = data.pop("_ratings")
        data["avg_hospital_rating"] = str(round(sum(ratings) / len(ratings), 1)) if ratings else ""

    return sorted(by_county.values(), key=lambda r: r.get("county", ""))


# ── Top-level orchestrator ──────────────────────────────────────────────────


def fetch_access_data(measures: list[AccessMeasure]) -> list[dict]:
    """Fetch all requested access data and return one row per county."""
    records = fetch_hospital_data()
    return _aggregate_hospitals_by_county(records)


# ── CSV output ──────────────────────────────────────────────────────────────


def write_access_csv(
    rows: list[dict],
    measures: list[AccessMeasure],
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
