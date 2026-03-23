# acs-cli-mi

CLI tool for querying U.S. Census Bureau ACS 5-year estimates and related federal data sources for **Michigan counties** (default) or **Michigan zip codes** (ZCTA). Outputs clean CSV.

## Data Sources

| Source | Commands | API Key Required |
|--------|----------|-----------------|
| Census ACS | `query`, `topics`, `info`, `login` | Yes (`CENSUS_API_KEY`) |
| CDC PLACES | `places`, `places-topics` | No |
| CMS Hospitals | `access`, `access-topics` | No |
| HRSA HPSA | `access`, `access-topics` | No |
| BLS LAUS | `economy`, `economy-topics`, `bls-login` | Yes (`BLS_API_KEY`) |
| BLS QCEW | `qcew`, `qcew-topics` | No |
| AHRF | `providers`, `provider-topics` | No |

**2024 is the latest available census data year.**

## Architecture

```
src/acs_cli/
  cli.py              # Typer app — all commands registered here
  topics.py            # Census variable registry (TOPICS dict)
  census_api/client.py # Census ACS client
  census_api/zcta.py   # Michigan ZCTA (zip code) registry
  bls_api/client.py    # BLS LAUS + QCEW clients
  cms_api/client.py    # CMS hospital data
  hrsa_api/client.py   # HRSA HPSA shortage areas
  hrsa_api/ahrf.py     # AHRF provider counts (zip download + CSV parse)
  places_api/client.py # CDC PLACES chronic disease
```

**Module pattern:** Each data source is a `*_api/` package with `client.py` containing:
- Frozen `@dataclass` models (e.g., `Variable`, `Measure`)
- A `MEASURES` dict grouping measures by topic
- `resolve_*()` to convert topic names to measure objects
- `fetch_*()` to call the API and return `list[dict]`
- `write_csv()` to format and output results
- Custom exception classes per source

## Dev Workflow

```bash
uv sync                        # Install dependencies
uv run acs-cli-mi <command>    # Run CLI from source
uv run pytest                  # Run full test suite
uv run pytest tests/test_cli.py -k "test_name"  # Run specific test
```

Requires Python 3.14+ (see `.python-version`).

## API Keys

Keys are resolved in order: env var → config file → `.env` (via python-dotenv).

| Key | Env Var | Config File | CLI Command |
|-----|---------|-------------|-------------|
| Census | `CENSUS_API_KEY` | `~/.config/acs-cli/config` | `login` |
| BLS | `BLS_API_KEY` | `~/.config/acs-cli/bls_config` | `bls-login` |

Config files are created with mode `0o600`.

## Testing

- **Framework:** pytest + respx (mocks httpx requests)
- **Fixtures:** `tests/conftest.py` has per-source mock fixtures (`mock_census`, `mock_bls`, `mock_places`, etc.) with helper functions to register mock responses and build realistic test data
- **Always run the full test suite before submitting changes:** `uv run pytest`
- **Always write tests for new features** — follow the existing fixture patterns in conftest.py

## Coding Conventions

- `from __future__ import annotations` in all source files
- Type hints on all function signatures
- Frozen dataclasses for data models
- Constants as `UPPER_SNAKE_CASE` at module level
- Errors to stderr (`typer.echo(..., err=True)`), data to stdout (CSV)
- Custom exceptions per module (e.g., `CensusAPIError`, `BLSAPIError`)
- Section separators: `# ── Section Name ──────────────────`
- Keep modules small — one data source per `*_api/` package
