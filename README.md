# acs-cli-mi

A command-line tool for querying U.S. Census Bureau American Community Survey (ACS) 5-year estimates for Michigan counties. Returns clean CSV output ready for spreadsheets, data pipelines, or further analysis.

## Install

```bash
# Run directly without installing
uvx acs-cli-mi --help

# Or install with pip/uv
pip install acs-cli-mi
```

Requires Python 3.14+.

## Setup

Get a free Census API key at <https://api.census.gov/data/key_signup.html>, then save it:

```bash
acs-cli-mi login --api-key YOUR_KEY
```

The key is stored in `~/.config/acs-cli/config`. Alternatively, set `CENSUS_API_KEY` in your environment or a `.env` file.

## Usage

### Query topics

```bash
# Population demographics for all Michigan counties
acs-cli-mi query population

# Filter to a specific county
acs-cli-mi query income --county Wayne

# Combine multiple topics
acs-cli-mi query population income education --county Washtenaw

# All topics at once
acs-cli-mi query all
```

### Multi-year queries

Query across multiple ACS vintages to see trends over time. Adds a `Year` column to the output.

```bash
acs-cli-mi query population --years 2019,2020,2021,2022,2023,2024
acs-cli-mi query income --years 2019,2024 --county Wayne
```

### Save to file

```bash
acs-cli-mi query population income --output data.csv
acs-cli-mi query all --years 2020,2024 --output trend.csv
```

### Sort results

```bash
acs-cli-mi query population --sort "Total Population"
```

### County profile

Get all topics for a single county in a vertical layout:

```bash
acs-cli-mi info Washtenaw
acs-cli-mi info "Grand Traverse" --year 2022
```

### List available topics

```bash
acs-cli-mi topics
```

### Raw Census variables

Bypass the topic system and request specific ACS variable codes:

```bash
acs-cli-mi query -v B01003_001E -v B19013_001E
```

## Available Topics

| Topic | Description |
|---|---|
| `population` | Total population, male/female, age breakouts (by sex), under 18, nativity/citizenship, households, housing units |
| `income` | Median household/family income, per capita income, Gini index, earnings by sex, household income distribution (16 brackets), income sources (Social Security, SSI, public assistance, retirement) |
| `age` | Median age overall, male, female |
| `poverty` | Poverty universe and count below poverty level |
| `education` | Full educational attainment for pop 25+ (no schooling through doctorate, including partial levels) |
| `race` | Total, White, Black, AIAN, Asian |
| `health_insurance` | Insurance universe, with coverage, without coverage |
| `disability` | Disability universe, male with disability, female with disability |
| `insurance_income` | Insurance status by household income brackets |

## Output Format

All output is CSV written to stdout (or a file with `--output`). Values are raw numbers without formatting so they can be consumed directly by pandas, Excel, database imports, or other tools. Suppressed Census values appear as empty fields.

## Data Source

All data comes from the [Census Bureau ACS 5-Year Estimates API](https://www.census.gov/data/developers/data-sets/acs-5year.html). Available vintages: 2009-2024. Data covers all 83 Michigan counties (FIPS state code 26).

## Project Structure

```
src/acs_cli/
├── cli.py              # Typer CLI commands
├── topics.py           # Variable definitions and topic registry
└── census_api/
    ├── __init__.py
    └── client.py       # API client, key management, CSV output
```
