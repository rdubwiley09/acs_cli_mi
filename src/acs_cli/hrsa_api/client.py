from __future__ import annotations

import csv
from dataclasses import dataclass

import httpx

# ── Constants ────────────────────────────────────────────────────────────────

HRSA_BASE_URL = (
    "https://gisportal.hrsa.gov/server/rest/services/Shortage"
    "/HealthProfessionalShortageAreas_FS/MapServer"
)

HPSA_LAYERS: dict[str, tuple[str, str]] = {
    # group_name → (layer_path, measure_prefix)
    "primary_care_shortage": ("/9/query", "pc"),
    "mental_health_shortage": ("/5/query", "mh"),
}

HPSA_OUT_FIELDS = (
    "CMN_STATE_COUNTY_FIPS_CD,HPSA_SCORE,HPSA_STATUS_DESC,"
    "HPSA_DEGREE_OF_SHORTAGE,HPSA_FORMAL_RATIO,HPSA_ESTIMATED_UNDERSERVED_POP"
)

# Michigan FIPS → county name
MI_FIPS_TO_COUNTY: dict[str, str] = {
    "26001": "Alcona", "26003": "Alger", "26005": "Allegan",
    "26007": "Alpena", "26009": "Antrim", "26011": "Arenac",
    "26013": "Baraga", "26015": "Barry", "26017": "Bay",
    "26019": "Benzie", "26021": "Berrien", "26023": "Branch",
    "26025": "Calhoun", "26027": "Cass", "26029": "Charlevoix",
    "26031": "Cheboygan", "26033": "Chippewa", "26035": "Clare",
    "26037": "Clinton", "26039": "Crawford", "26041": "Delta",
    "26043": "Dickinson", "26045": "Eaton", "26047": "Emmet",
    "26049": "Genesee", "26051": "Gladwin", "26053": "Gogebic",
    "26055": "Grand Traverse", "26057": "Gratiot", "26059": "Hillsdale",
    "26061": "Houghton", "26063": "Huron", "26065": "Ingham",
    "26067": "Ionia", "26069": "Iosco", "26071": "Iron",
    "26073": "Isabella", "26075": "Jackson", "26077": "Kalamazoo",
    "26079": "Kalkaska", "26081": "Kent", "26083": "Keweenaw",
    "26085": "Lake", "26087": "Lapeer", "26089": "Leelanau",
    "26091": "Lenawee", "26093": "Livingston", "26095": "Luce",
    "26097": "Mackinac", "26099": "Macomb", "26101": "Manistee",
    "26103": "Marquette", "26105": "Mason", "26107": "Mecosta",
    "26109": "Menominee", "26111": "Midland", "26113": "Missaukee",
    "26115": "Monroe", "26117": "Montcalm", "26119": "Montmorency",
    "26121": "Muskegon", "26123": "Newaygo", "26125": "Oakland",
    "26127": "Oceana", "26129": "Ogemaw", "26131": "Ontonagon",
    "26133": "Osceola", "26135": "Oscoda", "26137": "Otsego",
    "26139": "Ottawa", "26141": "Presque Isle", "26143": "Roscommon",
    "26145": "Saginaw", "26147": "Saint Clair", "26149": "Saint Joseph",
    "26151": "Sanilac", "26153": "Schoolcraft", "26155": "Shiawassee",
    "26157": "Tuscola", "26159": "Van Buren", "26161": "Washtenaw",
    "26163": "Wayne", "26165": "Wexford",
}


# ── Data model ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class HPSAMeasure:
    measure_id: str
    label: str
    description: str


HPSA_MEASURES: dict[str, list[HPSAMeasure]] = {
    "primary_care_shortage": [
        HPSAMeasure("pc_hpsa_count", "Primary Care HPSA Count", "Number of primary care HPSA designations"),
        HPSAMeasure("pc_hpsa_max_score", "Primary Care Max HPSA Score", "Maximum HPSA score (higher = greater shortage)"),
        HPSAMeasure("pc_hpsa_avg_score", "Primary Care Avg HPSA Score", "Average HPSA score across designations"),
        HPSAMeasure("pc_underserved_pop", "Primary Care Underserved Pop", "Estimated underserved population"),
    ],
    "mental_health_shortage": [
        HPSAMeasure("mh_hpsa_count", "Mental Health HPSA Count", "Number of mental health HPSA designations"),
        HPSAMeasure("mh_hpsa_max_score", "Mental Health Max HPSA Score", "Maximum HPSA score (higher = greater shortage)"),
        HPSAMeasure("mh_hpsa_avg_score", "Mental Health Avg HPSA Score", "Average HPSA score across designations"),
        HPSAMeasure("mh_underserved_pop", "Mental Health Underserved Pop", "Estimated underserved population"),
    ],
}


# ── Errors ───────────────────────────────────────────────────────────────────


class HRSAAPIError(Exception):
    def __init__(self, status_code: int, detail: str = ""):
        self.status_code = status_code
        super().__init__(f"HRSA API returned {status_code}" + (f": {detail}" if detail else ""))


# ── Resolve measures ─────────────────────────────────────────────────────────


def resolve_hpsa_measures(groups: list[str]) -> list[HPSAMeasure]:
    if "all" in groups:
        groups = list(HPSA_MEASURES.keys())
    measures: list[HPSAMeasure] = []
    for g in groups:
        if g not in HPSA_MEASURES:
            raise ValueError(
                f"Unknown shortage group '{g}'. "
                f"Available: {', '.join(HPSA_MEASURES.keys())}"
            )
        measures.extend(HPSA_MEASURES[g])
    return measures


# ── Fetch HPSA data ─────────────────────────────────────────────────────────


def fetch_hpsa_data(layer_path: str) -> list[dict]:
    """Fetch Michigan HPSA data from HRSA ArcGIS service for a given layer."""
    url = HRSA_BASE_URL + layer_path
    all_features: list[dict] = []
    offset = 0
    limit = 1000

    while True:
        params: dict[str, str | int] = {
            "where": "PRIMARY_STATE_NM='Michigan'",
            "outFields": HPSA_OUT_FIELDS,
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": limit,
        }
        resp = httpx.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            raise HRSAAPIError(resp.status_code, resp.text[:200])

        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        if not data.get("exceededTransferLimit", False):
            break
        offset += limit

    return all_features


# ── Aggregate HPSA by county ────────────────────────────────────────────────


def _aggregate_hpsa_by_county(features: list[dict], prefix: str) -> list[dict]:
    """Group HPSA features by county FIPS and compute scores."""
    by_county: dict[str, dict] = {}

    for feat in features:
        attrs = feat.get("attributes", {})
        fips = str(attrs.get("CMN_STATE_COUNTY_FIPS_CD", ""))
        county = MI_FIPS_TO_COUNTY.get(fips)
        if not county:
            continue

        if county not in by_county:
            by_county[county] = {
                "county": county,
                f"{prefix}_hpsa_count": 0,
                f"_{prefix}_scores": [],
                f"_{prefix}_pop": 0,
            }

        data = by_county[county]
        data[f"{prefix}_hpsa_count"] += 1

        score = attrs.get("HPSA_SCORE")
        if score is not None:
            try:
                data[f"_{prefix}_scores"].append(float(score))
            except (ValueError, TypeError):
                pass

        pop = attrs.get("HPSA_ESTIMATED_UNDERSERVED_POP")
        if pop is not None:
            try:
                data[f"_{prefix}_pop"] += int(pop)
            except (ValueError, TypeError):
                pass

    for data in by_county.values():
        scores = data.pop(f"_{prefix}_scores")
        data[f"{prefix}_hpsa_max_score"] = str(max(scores)) if scores else ""
        data[f"{prefix}_hpsa_avg_score"] = str(round(sum(scores) / len(scores), 1)) if scores else ""
        pop = data.pop(f"_{prefix}_pop")
        data[f"{prefix}_underserved_pop"] = str(pop) if pop else ""

    return sorted(by_county.values(), key=lambda r: r.get("county", ""))


# ── Top-level orchestrator ──────────────────────────────────────────────────


def fetch_shortage_data(measures: list[HPSAMeasure]) -> list[dict]:
    """Fetch all requested shortage data and return one row per county."""
    measure_ids = {m.measure_id for m in measures}
    by_county: dict[str, dict] = {}

    for group_name, (layer_path, prefix) in HPSA_LAYERS.items():
        group_measures = HPSA_MEASURES.get(group_name, [])
        if not any(m.measure_id in measure_ids for m in group_measures):
            continue

        features = fetch_hpsa_data(layer_path)
        for row in _aggregate_hpsa_by_county(features, prefix):
            county = row["county"]
            by_county.setdefault(county, {"county": county}).update(row)

    return sorted(by_county.values(), key=lambda r: r.get("county", ""))


# ── CSV output ──────────────────────────────────────────────────────────────


def write_shortage_csv(
    rows: list[dict],
    measures: list[HPSAMeasure],
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
