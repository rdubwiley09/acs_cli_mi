from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

from acs_cli import clean_county_name
from acs_cli.topics import TOPICS, Variable

load_dotenv()

# ── Constants ────────────────────────────────────────────────────────────────

ACS_BASE_URL = "https://api.census.gov/data/{year}/acs/acs5"
MICHIGAN_FIPS = "26"
DEFAULT_YEAR = 2024
CONFIG_DIR = Path.home() / ".config" / "acs-cli"
CONFIG_FILE = CONFIG_DIR / "config"
MAX_VARS_PER_CALL = 49  # Census API allows 50 fields; NAME takes one slot
SUPPRESSED = {"-666666666", "-666666666.0", "null", "-", "None", None}


# ── API key management ───────────────────────────────────────────────────────

def save_api_key(api_key: str) -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(api_key.strip())
    CONFIG_FILE.chmod(0o600)
    return CONFIG_FILE


def get_api_key() -> str:
    # 1. Environment variable (includes .env via dotenv)
    key = os.environ.get("CENSUS_API_KEY")
    if key:
        return key
    # 2. Config file from `login` command
    if CONFIG_FILE.exists():
        stored = CONFIG_FILE.read_text().strip()
        if stored:
            return stored
    raise MissingAPIKeyError(
        "No Census API key found.\n"
        "Run 'acs-cli login' to save your key, or set CENSUS_API_KEY in .env\n"
        "Get a free key at: https://api.census.gov/data/key_signup.html"
    )


class MissingAPIKeyError(Exception):
    pass


class InvalidAPIKeyError(Exception):
    pass


class CensusAPIError(Exception):
    def __init__(self, status_code: int, year: int):
        self.status_code = status_code
        self.year = year
        super().__init__(f"Census API returned {status_code} for year {year}")


# ── Data fetching ────────────────────────────────────────────────────────────

def _fetch_acs_batch(codes: list[str], year: int, api_key: str) -> list[dict]:
    url = ACS_BASE_URL.format(year=year)
    params = {
        "get": f"NAME,{','.join(codes)}",
        "for": "county:*",
        "in": f"state:{MICHIGAN_FIPS}",
        "key": api_key,
    }
    resp = httpx.get(url, params=params, timeout=30)
    if resp.status_code in (401, 403):
        raise InvalidAPIKeyError(
            "Census API rejected your API key.\n"
            "Run 'acs-cli login' to update your key.\n"
            "Get a free key at: https://api.census.gov/data/key_signup.html"
        )
    if resp.status_code != 200:
        raise CensusAPIError(resp.status_code, year)
    data = resp.json()
    headers = data[0]
    return [dict(zip(headers, row)) for row in data[1:]]


def fetch_acs_data(variables: list[Variable], year: int, api_key: str) -> list[dict]:
    all_codes = [v.code for v in variables]
    # Single batch — fast path
    if len(all_codes) <= MAX_VARS_PER_CALL:
        return _fetch_acs_batch(all_codes, year, api_key)
    # Multiple batches — merge on county FIPS (state + county)
    chunks = [all_codes[i:i + MAX_VARS_PER_CALL] for i in range(0, len(all_codes), MAX_VARS_PER_CALL)]
    merged: dict[str, dict] = {}
    for chunk in chunks:
        batch_rows = _fetch_acs_batch(chunk, year, api_key)
        for row in batch_rows:
            key = row["state"] + row["county"]
            if key in merged:
                merged[key].update(row)
            else:
                merged[key] = row
    return list(merged.values())


def fetch_multi_year(variables: list[Variable], years: list[int], api_key: str) -> list[dict]:
    combined: list[dict] = []
    for year in years:
        rows = fetch_acs_data(variables, year, api_key)
        for row in rows:
            row["year"] = str(year)
        combined.extend(rows)
    return combined


# ── Formatting & output ──────────────────────────────────────────────────────

def format_value(value: str | None, fmt: str) -> str:
    if value in SUPPRESSED or value is None:
        return ""
    try:
        num = float(value)
    except (ValueError, TypeError):
        return str(value)
    if fmt == "decimal":
        return f"{num:.1f}"
    # number, dollar, percent — return plain numeric value
    return str(int(num)) if num == int(num) else str(num)


def resolve_variables(topics: list[str], raw_variables: list[str] | None) -> list[Variable]:
    variables: list[Variable] = []
    if raw_variables:
        for code in raw_variables:
            variables.append(Variable(code, code, "number"))
        return variables

    names = topics
    if "all" in names:
        names = list(TOPICS.keys())

    for name in names:
        if name not in TOPICS:
            raise ValueError(f"Unknown topic '{name}'. Run 'topics' to see available topics.")
        variables.extend(TOPICS[name])
    return variables


def write_csv(
    rows: list[dict],
    variables: list[Variable],
    writer: csv.writer,
    show_year: bool = False,
    county_filter: str | None = None,
    sort_col: str | None = None,
    header: bool = True,
) -> int:
    """Write rows as CSV. Returns number of data rows written."""
    # Filter by county substring
    if county_filter:
        filt = county_filter.lower()
        rows = [r for r in rows if filt in r.get("NAME", "").lower()]

    if not rows:
        return 0

    # Sort
    if sort_col:
        target_code = None
        for v in variables:
            if v.label.lower() == sort_col.lower():
                target_code = v.code
                break
        if target_code is None:
            target_code = sort_col
        rows.sort(key=lambda r: float(r.get(target_code, 0) or 0), reverse=True)
    else:
        rows.sort(key=lambda r: (r.get("NAME", ""), r.get("year", "")))

    # Build header
    columns: list[str] = []
    if show_year:
        columns.append("Year")
    columns.append("County")
    for v in variables:
        columns.append(v.label)

    if header:
        writer.writerow(columns)

    # Build data rows
    for row in rows:
        csv_row: list[str] = []
        if show_year:
            csv_row.append(row.get("year", ""))
        csv_row.append(clean_county_name(row.get("NAME", "")))
        for v in variables:
            csv_row.append(format_value(row.get(v.code), v.format))
        writer.writerow(csv_row)

    return len(rows)
