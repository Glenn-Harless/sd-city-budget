# SD City Budget — Municipal Spending Dashboard

## Project Overview
San Diego city budget analysis. Hierarchical/relational data (departments, programs, line items, revenue/expenditure flows). Goal: connect spending to service outcomes, especially alongside the Get It Done 311 dashboard.

## Architecture — Follow sd-get-it-done Pattern

### Project Structure
```
pipeline/       # Data ingestion + transformation
data/raw/       # Raw source data (gitignored)
data/processed/ # Cleaned parquet(s) — commit to git if under 100MB
data/aggregated/# Pre-aggregated parquets for dashboard
dashboard/      # Streamlit app
```

### Dashboard Rules
- **Use DuckDB for all data access** — no Polars/pandas for loading full datasets. Streamlit Cloud has 1GB RAM limit.
- `query()` helper: fresh `duckdb.connect()` per call, returns pandas DataFrame.
- Shared `_where_clause()` for sidebar filters across all tabs.
- Each query should return small aggregated DataFrames (~10-50 rows).
- `requirements.txt` at project root for Streamlit Cloud (not pyproject.toml).

### Pipeline
- Use DuckDB for transforms (consistent with Get It Done)
- `uv` for dependency management, `pyproject.toml` for project config
- Data source: San Diego open data portal (data.sandiego.gov)

### Deployment
- GitHub Actions for scheduled data refresh
- `.gitignore`: use `dir/*` pattern (not `dir/`) when negation exceptions are needed
- Parquet files under 100MB can be committed directly to git

## Data Shape
This is hierarchical/relational data, not event-level:
- Revenue sources → fund allocations
- Departments → programs → line items
- Fiscal year comparisons (budget vs actuals)
- Council district spending breakdowns

## Visualization Ideas
- Sankey diagram: revenue sources → department spending ("where does your tax dollar go")
- District-level spending per capita
- Year-over-year budget trends by department
- Cross-reference with Get It Done: spending vs service response times by district

## Related Projects
- `sd-get-it-done/` — 311 service requests, pairs naturally for spending-vs-outcomes analysis
