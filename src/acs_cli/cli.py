from __future__ import annotations

import csv
import sys
from typing import Optional

import typer

from acs_cli.census_api import (
    fetch_acs_data,
    fetch_multi_year,
    format_value,
    get_api_key,
    write_csv,
)
from acs_cli.census_api.client import (
    CensusAPIError,
    InvalidAPIKeyError,
    MissingAPIKeyError,
    resolve_variables,
    save_api_key,
    DEFAULT_YEAR,
)
from acs_cli.bls_api import (
    ECONOMY_MEASURES,
    QCEW_MEASURES,
    BLSAPIError,
    MissingBLSKeyError as MissingBLSKeyError_,
    fetch_economy_data,
    fetch_qcew_data,
    resolve_economy_measures,
    resolve_qcew_measures,
    write_economy_csv,
    write_qcew_csv,
    get_bls_api_key,
    save_bls_api_key,
)
from acs_cli.bls_api.client import DEFAULT_BLS_YEAR, DEFAULT_PERIOD, DEFAULT_QCEW_YEAR
from acs_cli.cms_api import (
    ACCESS_MEASURES,
    CMSAPIError,
    fetch_access_data,
    resolve_access_measures,
)
from acs_cli.hrsa_api import (
    HPSA_MEASURES,
    HRSAAPIError,
    fetch_shortage_data,
    resolve_hpsa_measures,
    AHRF_MEASURES,
    fetch_ahrf_data,
    resolve_ahrf_measures,
    write_ahrf_csv,
)
from acs_cli.places_api import (
    PLACES_MEASURES,
    PlacesAPIError,
    fetch_places_data,
    resolve_measures,
    write_places_csv,
)
from acs_cli.places_api.client import DEFAULT_PLACES_YEAR
from acs_cli.topics import TOPICS

app = typer.Typer(help="ACS CLI — Query Census ACS 5-year data for Michigan counties (CSV output).")

ALL_ACCESS_GROUPS = set(ACCESS_MEASURES.keys()) | set(HPSA_MEASURES.keys())


def _get_key() -> str:
    try:
        return get_api_key()
    except MissingAPIKeyError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


def _handle_api_error(e: CensusAPIError | InvalidAPIKeyError) -> None:
    typer.echo(f"Error: {e}", err=True)
    raise typer.Exit(1)


def _get_bls_key() -> str:
    try:
        return get_bls_api_key()
    except MissingBLSKeyError_ as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def login(
    api_key: str = typer.Option(..., prompt="Census API key", help="Your Census API key"),
):
    """Save your Census API key. Get one at https://api.census.gov/data/key_signup.html"""
    path = save_api_key(api_key)
    typer.echo(f"API key saved to {path}", err=True)


@app.command()
def query(
    topics: Optional[list[str]] = typer.Argument(None, help="Topic names (e.g. population income) or 'all'"),
    year: int = typer.Option(DEFAULT_YEAR, "--year", "-y", help="Single ACS vintage year"),
    years: Optional[str] = typer.Option(None, "--years", help="Comma-separated years, e.g. '2019,2020,2023' (overrides --year)"),
    county: Optional[str] = typer.Option(None, "--county", "-c", help="Filter by county name substring"),
    sort: Optional[str] = typer.Option(None, "--sort", "-s", help="Sort descending by column label"),
    variable: Optional[list[str]] = typer.Option(None, "--variable", "-v", help="Raw Census variable codes"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write CSV to this file path instead of stdout"),
):
    """Query ACS data for Michigan counties. Output is CSV to stdout or a file via --output."""
    if not topics and not variable:
        typer.echo("Error: Provide topic names or use --variable / -v.", err=True)
        raise typer.Exit(1)
    api_key = _get_key()
    try:
        variables = resolve_variables(topics or [], variable)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    show_year = False
    try:
        if years:
            year_list = [int(y.strip()) for y in years.split(",")]
            rows = fetch_multi_year(variables, year_list, api_key)
            show_year = True
        else:
            rows = fetch_acs_data(variables, year, api_key)
    except (InvalidAPIKeyError, CensusAPIError) as e:
        _handle_api_error(e)

    if output:
        with open(output, "w", newline="") as f:
            writer = csv.writer(f)
            count = write_csv(rows, variables, writer, show_year=show_year, county_filter=county, sort_col=sort)
        if count:
            typer.echo(f"Wrote CSV to {output}", err=True)
        else:
            typer.echo("No matching rows found.", err=True)
    else:
        writer = csv.writer(sys.stdout)
        count = write_csv(rows, variables, writer, show_year=show_year, county_filter=county, sort_col=sort)
        if not count:
            typer.echo("No matching rows found.", err=True)


@app.command("topics")
def topics_cmd():
    """List available topics and their Census variables (CSV)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(["Topic", "Variable Code", "Label", "Format"])
    for topic_name, vars_ in TOPICS.items():
        for v in vars_:
            writer.writerow([topic_name, v.code, v.label, v.format])


@app.command()
def info(
    county_name: str = typer.Argument(help="County name (or substring) to profile"),
    year: int = typer.Option(DEFAULT_YEAR, "--year", "-y", help="ACS vintage year"),
):
    """Full profile for a single county — all topics, CSV output."""
    api_key = _get_key()
    try:
        all_vars = resolve_variables(["all"], None)
        rows = fetch_acs_data(all_vars, year, api_key)
    except (InvalidAPIKeyError, CensusAPIError) as e:
        _handle_api_error(e)

    filt = county_name.lower()
    matches = [r for r in rows if filt in r.get("NAME", "").lower()]

    if not matches:
        typer.echo(f"No county matching '{county_name}' found.", err=True)
        raise typer.Exit(1)

    writer = csv.writer(sys.stdout)
    writer.writerow(["County", "Field", "Value"])
    from acs_cli import clean_county_name
    for row in matches:
        name = clean_county_name(row.get("NAME", ""))
        for v in all_vars:
            writer.writerow([name, v.label, format_value(row.get(v.code), v.format)])

    # Append PLACES data (best-effort — CDC failure should not break Census output)
    try:
        places_measures = resolve_measures(["all"])
        places_rows = fetch_places_data(places_measures)
        for row in matches:
            census_name = clean_county_name(row.get("NAME", ""))
            # Match by county name substring against PLACES locationname
            for pr in places_rows:
                loc = pr.get("locationname", "")
                if filt in loc.lower():
                    for m in places_measures:
                        val = pr.get(m.measureid, "")
                        writer.writerow([census_name, f"PLACES: {m.label}", val])
                    break
    except Exception:
        pass

    # Append CMS access data (best-effort)
    try:
        access_measures = resolve_access_measures(["all"])
        access_rows = fetch_access_data(access_measures)
        for row in matches:
            census_name = clean_county_name(row.get("NAME", ""))
            for ar in access_rows:
                if filt in ar.get("county", "").lower():
                    for m in access_measures:
                        val = ar.get(m.measure_id, "")
                        writer.writerow([census_name, f"Access: {m.label}", val])
                    break
    except Exception:
        pass

    # Append HRSA shortage data (best-effort)
    try:
        hpsa_measures = resolve_hpsa_measures(["all"])
        hpsa_rows = fetch_shortage_data(hpsa_measures)
        for row in matches:
            census_name = clean_county_name(row.get("NAME", ""))
            for hr in hpsa_rows:
                if filt in hr.get("county", "").lower():
                    for m in hpsa_measures:
                        val = hr.get(m.measure_id, "")
                        writer.writerow([census_name, f"Shortage: {m.label}", val])
                    break
    except Exception:
        pass

    # Append BLS economy data (best-effort)
    try:
        bls_key = get_bls_api_key()
        econ_measures = resolve_economy_measures(["all"])
        econ_rows = fetch_economy_data(econ_measures, year=year, api_key=bls_key)
        for row in matches:
            census_name = clean_county_name(row.get("NAME", ""))
            for er in econ_rows:
                if filt in er.get("county", "").lower():
                    for m in econ_measures:
                        val = er.get(m.measure_id, "")
                        writer.writerow([census_name, f"Economy: {m.label}", val])
                    break
    except Exception:
        pass

    # Append BLS QCEW data (best-effort, no API key needed)
    try:
        qcew_measures = resolve_qcew_measures(["all"])
        qcew_rows = fetch_qcew_data(qcew_measures)
        for row in matches:
            census_name = clean_county_name(row.get("NAME", ""))
            for qr in qcew_rows:
                if filt in qr.get("county", "").lower():
                    for m in qcew_measures:
                        val = qr.get(m.measure_id, "")
                        writer.writerow([census_name, f"QCEW: {m.label}", val])
                    break
    except Exception:
        pass

    # Append AHRF provider data (best-effort, no API key needed)
    try:
        ahrf_measures = resolve_ahrf_measures(["all"])
        ahrf_rows = fetch_ahrf_data(ahrf_measures)
        for row in matches:
            census_name = clean_county_name(row.get("NAME", ""))
            for ar in ahrf_rows:
                if filt in ar.get("county", "").lower():
                    for m in ahrf_measures:
                        val = ar.get(m.measure_id, "")
                        writer.writerow([census_name, f"Provider: {m.label}", val])
                    break
    except Exception:
        pass


@app.command("places-topics")
def places_topics_cmd():
    """List available PLACES measure groups and their measures (CSV)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(["Group", "Measure ID", "Label", "Short Question"])
    for group_name, measures in PLACES_MEASURES.items():
        for m in measures:
            writer.writerow([group_name, m.measureid, m.label, m.short_question])


@app.command("places")
def places_cmd(
    groups: Optional[list[str]] = typer.Argument(None, help="PLACES measure groups (e.g. chronic_disease) or 'all'"),
    year: int = typer.Option(DEFAULT_PLACES_YEAR, "--year", "-y", help="PLACES data year"),
    county: Optional[str] = typer.Option(None, "--county", "-c", help="Filter by county name substring"),
    sort: Optional[str] = typer.Option(None, "--sort", "-s", help="Sort descending by measure label"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write CSV to file instead of stdout"),
    prevalence: str = typer.Option("age_adjusted", "--prevalence", "-p", help="Prevalence type: age_adjusted or crude"),
):
    """Query CDC PLACES chronic disease data for Michigan counties (CSV output)."""
    if not groups:
        typer.echo("Error: Provide PLACES group names or 'all'. Run 'places-topics' to see available groups.", err=True)
        raise typer.Exit(1)

    try:
        measures = resolve_measures(groups)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    try:
        rows = fetch_places_data(measures, year=year, prevalence_type=prevalence)
    except PlacesAPIError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    if output:
        with open(output, "w", newline="") as f:
            writer = csv.writer(f)
            count = write_places_csv(rows, measures, writer, county_filter=county, sort_col=sort)
        if count:
            typer.echo(f"Wrote CSV to {output}", err=True)
        else:
            typer.echo("No matching rows found.", err=True)
    else:
        writer = csv.writer(sys.stdout)
        count = write_places_csv(rows, measures, writer, county_filter=county, sort_col=sort)
        if not count:
            typer.echo("No matching rows found.", err=True)


@app.command("access-topics")
def access_topics_cmd():
    """List available healthcare access and shortage measure groups (CSV)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(["Source", "Group", "Measure ID", "Label", "Description"])
    for group_name, measures in ACCESS_MEASURES.items():
        for m in measures:
            writer.writerow(["CMS", group_name, m.measure_id, m.label, m.description])
    for group_name, measures in HPSA_MEASURES.items():
        for m in measures:
            writer.writerow(["HRSA", group_name, m.measure_id, m.label, m.description])


@app.command("access")
def access_cmd(
    groups: Optional[list[str]] = typer.Argument(None, help="Access groups (e.g. hospital_access primary_care_shortage) or 'all'"),
    county: Optional[str] = typer.Option(None, "--county", "-c", help="Filter by county name substring"),
    sort: Optional[str] = typer.Option(None, "--sort", "-s", help="Sort descending by measure label"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write CSV to file instead of stdout"),
):
    """Query healthcare access data for Michigan counties (CSV output)."""
    if not groups:
        typer.echo(
            "Error: Provide access group names or 'all'. Run 'access-topics' to see available groups.",
            err=True,
        )
        raise typer.Exit(1)

    if "all" in groups:
        cms_groups = list(ACCESS_MEASURES.keys())
        hrsa_groups = list(HPSA_MEASURES.keys())
    else:
        unknown = [g for g in groups if g not in ALL_ACCESS_GROUPS]
        if unknown:
            typer.echo(
                f"Error: Unknown access group(s): {', '.join(unknown)}. "
                f"Available: {', '.join(sorted(ALL_ACCESS_GROUPS))}",
                err=True,
            )
            raise typer.Exit(1)
        cms_groups = [g for g in groups if g in ACCESS_MEASURES]
        hrsa_groups = [g for g in groups if g in HPSA_MEASURES]

    cms_measures = resolve_access_measures(cms_groups) if cms_groups else []
    hrsa_measures = resolve_hpsa_measures(hrsa_groups) if hrsa_groups else []

    try:
        cms_rows = fetch_access_data(cms_measures) if cms_measures else []
        hrsa_rows = fetch_shortage_data(hrsa_measures) if hrsa_measures else []
    except (CMSAPIError, HRSAAPIError) as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    # Merge CMS and HRSA data by county
    by_county: dict[str, dict] = {}
    for row in cms_rows:
        c = row.get("county", "")
        by_county.setdefault(c, {"county": c}).update(row)
    for row in hrsa_rows:
        c = row.get("county", "")
        by_county.setdefault(c, {"county": c}).update(row)

    merged = sorted(by_county.values(), key=lambda r: r.get("county", ""))
    all_measure_cols = (
        [(m.measure_id, m.label) for m in cms_measures]
        + [(m.measure_id, m.label) for m in hrsa_measures]
    )

    # Filter / sort / write
    if county:
        filt = county.lower()
        merged = [r for r in merged if filt in r.get("county", "").lower()]

    if sort:
        target_id = sort
        for mid, label in all_measure_cols:
            if label.lower() == sort.lower():
                target_id = mid
                break
        merged.sort(key=lambda r: float(r.get(target_id, 0) or 0), reverse=True)

    def _write_rows(writer: csv.writer) -> int:
        if not merged:
            return 0
        writer.writerow(["County"] + [label for _, label in all_measure_cols])
        for row in merged:
            csv_row = [row.get("county", "")]
            for mid, _ in all_measure_cols:
                csv_row.append(row.get(mid, ""))
            writer.writerow(csv_row)
        return len(merged)

    if output:
        with open(output, "w", newline="") as f:
            count = _write_rows(csv.writer(f))
        if count:
            typer.echo(f"Wrote CSV to {output}", err=True)
        else:
            typer.echo("No matching rows found.", err=True)
    else:
        count = _write_rows(csv.writer(sys.stdout))
        if not count:
            typer.echo("No matching rows found.", err=True)


@app.command("bls-login")
def bls_login(
    api_key: str = typer.Option(..., prompt="BLS API key", help="Your BLS API key"),
):
    """Save your BLS API key. Get one at https://data.bls.gov/registrationEngine/"""
    path = save_bls_api_key(api_key)
    typer.echo(f"BLS API key saved to {path}", err=True)


@app.command("economy-topics")
def economy_topics_cmd():
    """List available economy measure groups and their measures (CSV)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(["Group", "Measure ID", "Label", "Description"])
    for group_name, measures in ECONOMY_MEASURES.items():
        for m in measures:
            writer.writerow([group_name, m.measure_id, m.label, m.description])


@app.command("economy")
def economy_cmd(
    groups: Optional[list[str]] = typer.Argument(None, help="Economy groups (e.g. unemployment employment) or 'all'"),
    year: int = typer.Option(DEFAULT_BLS_YEAR, "--year", "-y", help="Data year"),
    county: Optional[str] = typer.Option(None, "--county", "-c", help="Filter by county name substring"),
    sort: Optional[str] = typer.Option(None, "--sort", "-s", help="Sort descending by measure label"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write CSV to file instead of stdout"),
    period: str = typer.Option(DEFAULT_PERIOD, "--period", "-p", help="BLS period (M13=annual average, M01-M12=monthly)"),
):
    """Query BLS LAUS economic data for Michigan counties (CSV output)."""
    if not groups:
        typer.echo(
            "Error: Provide economy group names or 'all'. Run 'economy-topics' to see available groups.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        measures = resolve_economy_measures(groups)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    api_key = _get_bls_key()

    try:
        rows = fetch_economy_data(measures, year=year, api_key=api_key, period=period)
    except BLSAPIError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    if output:
        with open(output, "w", newline="") as f:
            w = csv.writer(f)
            count = write_economy_csv(rows, measures, w, county_filter=county, sort_col=sort)
        if count:
            typer.echo(f"Wrote CSV to {output}", err=True)
        else:
            typer.echo("No matching rows found.", err=True)
    else:
        w = csv.writer(sys.stdout)
        count = write_economy_csv(rows, measures, w, county_filter=county, sort_col=sort)
        if not count:
            typer.echo("No matching rows found.", err=True)


@app.command("qcew-topics")
def qcew_topics_cmd():
    """List available QCEW measure groups and their measures (CSV)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(["Group", "Measure ID", "Label", "Description"])
    for group_name, measures in QCEW_MEASURES.items():
        for m in measures:
            writer.writerow([group_name, m.measure_id, m.label, m.description])


@app.command("qcew")
def qcew_cmd(
    groups: Optional[list[str]] = typer.Argument(None, help="QCEW groups (e.g. wages healthcare) or 'all'"),
    year: int = typer.Option(DEFAULT_QCEW_YEAR, "--year", "-y", help="Data year"),
    county: Optional[str] = typer.Option(None, "--county", "-c", help="Filter by county name substring"),
    sort: Optional[str] = typer.Option(None, "--sort", "-s", help="Sort descending by measure label"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write CSV to file instead of stdout"),
):
    """Query BLS QCEW wage and establishment data for Michigan counties (CSV output)."""
    if not groups:
        typer.echo(
            "Error: Provide QCEW group names or 'all'. Run 'qcew-topics' to see available groups.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        measures = resolve_qcew_measures(groups)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    try:
        rows = fetch_qcew_data(measures, year=year)
    except BLSAPIError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    if output:
        with open(output, "w", newline="") as f:
            w = csv.writer(f)
            count = write_qcew_csv(rows, measures, w, county_filter=county, sort_col=sort)
        if count:
            typer.echo(f"Wrote CSV to {output}", err=True)
        else:
            typer.echo("No matching rows found.", err=True)
    else:
        w = csv.writer(sys.stdout)
        count = write_qcew_csv(rows, measures, w, county_filter=county, sort_col=sort)
        if not count:
            typer.echo("No matching rows found.", err=True)


@app.command("provider-topics")
def provider_topics_cmd():
    """List available AHRF provider measure groups and their measures (CSV)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(["Group", "Measure ID", "Label", "Description"])
    for group_name, measures in AHRF_MEASURES.items():
        for m in measures:
            writer.writerow([group_name, m.measure_id, m.label, m.description])


@app.command("providers")
def providers_cmd(
    groups: Optional[list[str]] = typer.Argument(None, help="Provider groups (e.g. physicians mid_level dental) or 'all'"),
    county: Optional[str] = typer.Option(None, "--county", "-c", help="Filter by county name substring"),
    sort: Optional[str] = typer.Option(None, "--sort", "-s", help="Sort descending by measure label"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write CSV to file instead of stdout"),
):
    """Query HRSA AHRF provider counts for Michigan counties (CSV output)."""
    if not groups:
        typer.echo(
            "Error: Provide provider group names or 'all'. Run 'provider-topics' to see available groups.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        measures = resolve_ahrf_measures(groups)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    try:
        rows = fetch_ahrf_data(measures)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    if output:
        with open(output, "w", newline="") as f:
            w = csv.writer(f)
            count = write_ahrf_csv(rows, measures, w, county_filter=county, sort_col=sort)
        if count:
            typer.echo(f"Wrote CSV to {output}", err=True)
        else:
            typer.echo("No matching rows found.", err=True)
    else:
        w = csv.writer(sys.stdout)
        count = write_ahrf_csv(rows, measures, w, county_filter=county, sort_col=sort)
        if not count:
            typer.echo("No matching rows found.", err=True)


if __name__ == "__main__":
    app()
