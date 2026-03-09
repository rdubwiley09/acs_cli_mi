from __future__ import annotations

import csv
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from acs_cli.hrsa_api.client import MI_FIPS_TO_COUNTY

# ── Constants ────────────────────────────────────────────────────────────────

AHRF_ZIP_URL = (
    "https://data.hrsa.gov/DataDownload/AHRF/AHRF_2024-2025_CSV.zip"
)
AHRF_HP_FILENAME = "AHRF2025hp.csv"
AHRF_CACHE_DIR = Path.home() / ".config" / "acs-cli" / "ahrf"


# ── Data model ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AHRFMeasure:
    measure_id: str
    label: str
    description: str
    csv_column: str


AHRF_MEASURES: dict[str, list[AHRFMeasure]] = {
    "physicians": [
        AHRFMeasure(
            "ahrf_primary_care_physicians",
            "PC Physicians",
            "Primary care physicians (non-fed, excl residents), 2023",
            "phys_nf_prim_care_pc_exc_rsdt_23",
        ),
        AHRFMeasure(
            "ahrf_total_mds",
            "Total MDs",
            "Total active non-federal MDs, 2023",
            "md_nf_activ_23",
        ),
        AHRFMeasure(
            "ahrf_total_dos",
            "Total DOs",
            "Total active non-federal DOs, 2023",
            "do_nf_activ_23",
        ),
    ],
    "mid_level": [
        AHRFMeasure(
            "ahrf_nurse_practitioners",
            "NPs",
            "Nurse practitioners (from NPI), 2024",
            "np_npi_24",
        ),
        AHRFMeasure(
            "ahrf_physician_assistants",
            "PAs",
            "Physician assistants (from NPI), 2024",
            "pa_npi_24",
        ),
    ],
    "dental": [
        AHRFMeasure(
            "ahrf_dentists",
            "Dentists",
            "Dentists (from NPI), 2024",
            "dent_npi_24",
        ),
    ],
}


# ── Resolve measures ─────────────────────────────────────────────────────────


def resolve_ahrf_measures(groups: list[str]) -> list[AHRFMeasure]:
    if "all" in groups:
        groups = list(AHRF_MEASURES.keys())
    measures: list[AHRFMeasure] = []
    for g in groups:
        if g not in AHRF_MEASURES:
            raise ValueError(
                f"Unknown AHRF group '{g}'. "
                f"Available: {', '.join(AHRF_MEASURES.keys())}"
            )
        measures.extend(AHRF_MEASURES[g])
    return measures


# ── Download & cache ─────────────────────────────────────────────────────────


def _download_ahrf_csv(cache_dir: Path = AHRF_CACHE_DIR) -> Path:
    """Download AHRF ZIP if not cached, extract hp.csv, return path."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    csv_path = cache_dir / AHRF_HP_FILENAME

    if csv_path.exists():
        return csv_path

    zip_path = cache_dir / "ahrf.zip"
    urllib.request.urlretrieve(AHRF_ZIP_URL, zip_path)

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the hp.csv file inside the ZIP (may be in a subdirectory)
        hp_name = None
        for name in zf.namelist():
            if name.endswith(AHRF_HP_FILENAME):
                hp_name = name
                break
        if hp_name is None:
            raise FileNotFoundError(
                f"{AHRF_HP_FILENAME} not found in downloaded ZIP"
            )
        # Extract to cache dir
        with zf.open(hp_name) as src, open(csv_path, "wb") as dst:
            dst.write(src.read())

    # Clean up ZIP
    zip_path.unlink(missing_ok=True)
    return csv_path


# ── Fetch AHRF data ─────────────────────────────────────────────────────────


def fetch_ahrf_data(measures: list[AHRFMeasure]) -> list[dict]:
    """Read cached AHRF CSV, filter to Michigan, return one row per county."""
    csv_path = _download_ahrf_csv()

    # Build column list: always need FIPS, plus measure columns
    fips_col = "fips_st_cnty"
    needed_cols = [fips_col] + [m.csv_column for m in measures]

    df = pd.read_csv(csv_path, usecols=needed_cols, dtype=str)

    # Filter to Michigan (FIPS starts with "26")
    df = df[df[fips_col].str.startswith("26")]

    results: list[dict] = []
    for _, row in df.iterrows():
        fips = row[fips_col]
        county = MI_FIPS_TO_COUNTY.get(fips)
        if not county:
            continue

        record: dict[str, str] = {"county": county}
        for m in measures:
            val = row.get(m.csv_column)
            if pd.notna(val) and str(val).strip():
                record[m.measure_id] = str(val).strip()
            else:
                record[m.measure_id] = ""
        results.append(record)

    return sorted(results, key=lambda r: r.get("county", ""))


# ── CSV output ───────────────────────────────────────────────────────────────


def write_ahrf_csv(
    rows: list[dict],
    measures: list[AHRFMeasure],
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
