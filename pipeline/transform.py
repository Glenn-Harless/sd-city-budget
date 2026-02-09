"""Clean, enrich, and aggregate San Diego budget data using DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
AGGREGATED_DIR = Path(__file__).resolve().parent.parent / "data" / "aggregated"
DB_PATH = Path(__file__).resolve().parent.parent / "db" / "budget.duckdb"


def transform(*, db_path: Path | None = None) -> None:
    """Load raw CSVs, join with reference tables, export Parquet."""
    db = db_path or DB_PATH
    db.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db))

    # ── Load reference tables ──
    _load_reference_tables(con)

    # ── Load and enrich operating data ──
    _build_operating_table(con)

    # ── Load and enrich CIP data ──
    _build_cip_table(con)

    # ── Export processed parquet (operating only — CIP is small) ──
    processed_path = PROCESSED_DIR / "budget.parquet"
    con.execute(f"""
        COPY (
            SELECT * FROM operating
            UNION ALL
            SELECT
                amount, fiscal_year, budget_cycle, source,
                NULL AS account_type, NULL AS account_class, NULL AS account_group,
                'Expense' AS revenue_or_expense,
                NULL AS account, NULL AS account_number,
                NULL AS dept_group, asset_owning_dept AS dept_name, NULL AS dept_division,
                NULL AS fund_type, NULL AS fund_name, NULL AS fund_number
            FROM cip
        ) TO '{processed_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    size_mb = processed_path.stat().st_size / (1024 * 1024)
    print(f"  Exported processed data -> {processed_path} ({size_mb:.1f} MB)")

    # ── Build aggregations ──
    _build_aggregations(con)

    con.close()
    print("Transform complete.")


def _load_reference_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Load the 3 reference CSVs into DuckDB tables."""
    ref_files = {
        "ref_accounts": RAW_DIR / "ref_accounts.csv",
        "ref_departments": RAW_DIR / "ref_departments.csv",
        "ref_funds": RAW_DIR / "ref_funds.csv",
    }
    for name, path in ref_files.items():
        if not path.exists():
            print(f"  [warn] Reference file not found: {path}")
            continue
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"""
            CREATE TABLE {name} AS
            SELECT * FROM read_csv('{path}', header=true, ignore_errors=true)
        """)
        count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        print(f"  Loaded {name}: {count:,} rows")


def _build_operating_table(con: duckdb.DuckDBPyConnection) -> None:
    """Combine operating budget + actuals, join with reference tables."""
    budget_path = RAW_DIR / "operating_budget.csv"
    actuals_path = RAW_DIR / "operating_actuals.csv"

    # Load raw CSVs
    for name, path in [("raw_op_budget", budget_path), ("raw_op_actuals", actuals_path)]:
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"""
            CREATE TABLE {name} AS
            SELECT * FROM read_csv('{path}', header=true, ignore_errors=true)
        """)
        count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        print(f"  Loaded {name}: {count:,} rows")

    # Build the enriched operating table
    # The fiscal year in these files is 2-digit (e.g. "23") — add 2000
    con.execute("DROP TABLE IF EXISTS operating")
    con.execute("""
        CREATE TABLE operating AS

        -- Budget records
        SELECT
            TRY_CAST(b.amount AS DOUBLE) AS amount,
            CASE
                WHEN TRY_CAST(b.report_fy AS INTEGER) < 100
                THEN TRY_CAST(b.report_fy AS INTEGER) + 2000
                ELSE TRY_CAST(b.report_fy AS INTEGER)
            END AS fiscal_year,
            COALESCE(b.budget_cycle, 'adopted') AS budget_cycle,
            'budget' AS source,
            a.account_type,
            a.account_class,
            a.account_group,
            CASE
                WHEN a.account_type IN ('Personnel', 'Non-Personnel') THEN 'Expense'
                WHEN a.account_type IS NOT NULL THEN 'Revenue'
                ELSE NULL
            END AS revenue_or_expense,
            b.account,
            TRY_CAST(b.account_number AS VARCHAR) AS account_number,
            d.dept_group,
            b.dept_name,
            d.dept_division,
            b.fund_type,
            f.fund_name,
            TRY_CAST(b.fund_number AS VARCHAR) AS fund_number
        FROM raw_op_budget b
        LEFT JOIN ref_accounts a ON TRY_CAST(b.account_number AS VARCHAR) = TRY_CAST(a.account_number AS VARCHAR)
        LEFT JOIN ref_departments d ON TRY_CAST(b.funds_center_number AS VARCHAR) = TRY_CAST(d.funds_center_number AS VARCHAR)
        LEFT JOIN ref_funds f ON TRY_CAST(b.fund_number AS VARCHAR) = TRY_CAST(f.fund_number AS VARCHAR)

        UNION ALL

        -- Actuals records
        SELECT
            TRY_CAST(ac.amount AS DOUBLE) AS amount,
            CASE
                WHEN TRY_CAST(ac.report_fy AS INTEGER) < 100
                THEN TRY_CAST(ac.report_fy AS INTEGER) + 2000
                ELSE TRY_CAST(ac.report_fy AS INTEGER)
            END AS fiscal_year,
            'actual' AS budget_cycle,
            'actual' AS source,
            a.account_type,
            a.account_class,
            a.account_group,
            CASE
                WHEN a.account_type IN ('Personnel', 'Non-Personnel') THEN 'Expense'
                WHEN a.account_type IS NOT NULL THEN 'Revenue'
                ELSE NULL
            END AS revenue_or_expense,
            ac.account,
            TRY_CAST(ac.account_number AS VARCHAR) AS account_number,
            d.dept_group,
            ac.dept_name,
            d.dept_division,
            ac.fund_type,
            f.fund_name,
            TRY_CAST(ac.fund_number AS VARCHAR) AS fund_number
        FROM raw_op_actuals ac
        LEFT JOIN ref_accounts a ON TRY_CAST(ac.account_number AS VARCHAR) = TRY_CAST(a.account_number AS VARCHAR)
        LEFT JOIN ref_departments d ON TRY_CAST(ac.funds_center_number AS VARCHAR) = TRY_CAST(d.funds_center_number AS VARCHAR)
        LEFT JOIN ref_funds f ON TRY_CAST(ac.fund_number AS VARCHAR) = TRY_CAST(f.fund_number AS VARCHAR)
    """)

    count = con.execute("SELECT count(*) FROM operating").fetchone()[0]
    print(f"  Operating table: {count:,} rows")


def _build_cip_table(con: duckdb.DuckDBPyConnection) -> None:
    """Combine CIP budget + actuals into enriched table."""
    cip_budget_path = RAW_DIR / "cip_budget_fy.csv"
    cip_actuals_path = RAW_DIR / "cip_actuals_fy.csv"

    for name, path in [("raw_cip_budget", cip_budget_path), ("raw_cip_actuals", cip_actuals_path)]:
        if not path.exists():
            print(f"  [warn] CIP file not found: {path}, skipping")
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.execute(f"CREATE TABLE {name} (dummy INTEGER)")
            continue
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"""
            CREATE TABLE {name} AS
            SELECT * FROM read_csv('{path}', header=true, ignore_errors=true)
        """)
        count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        print(f"  Loaded {name}: {count:,} rows")

    con.execute("DROP TABLE IF EXISTS cip")
    con.execute("""
        CREATE TABLE cip AS

        SELECT
            TRY_CAST(b.amount AS DOUBLE) AS amount,
            CASE
                WHEN TRY_CAST(b.report_fy AS INTEGER) < 100
                THEN TRY_CAST(b.report_fy AS INTEGER) + 2000
                ELSE TRY_CAST(b.report_fy AS INTEGER)
            END AS fiscal_year,
            COALESCE(b.budget_cycle, 'adopted') AS budget_cycle,
            'budget' AS source,
            b.asset_owning_dept,
            b.project_name,
            b.project_number
        FROM raw_cip_budget b
        WHERE TRY_CAST(b.amount AS DOUBLE) IS NOT NULL

        UNION ALL

        SELECT
            TRY_CAST(ac.amount AS DOUBLE) AS amount,
            CASE
                WHEN TRY_CAST(ac.report_fy AS INTEGER) < 100
                THEN TRY_CAST(ac.report_fy AS INTEGER) + 2000
                ELSE TRY_CAST(ac.report_fy AS INTEGER)
            END AS fiscal_year,
            'actual' AS budget_cycle,
            'actual' AS source,
            ac.asset_owning_dept,
            ac.project_name,
            ac.project_number_parent AS project_number
        FROM raw_cip_actuals ac
        WHERE TRY_CAST(ac.amount AS DOUBLE) IS NOT NULL
    """)

    count = con.execute("SELECT count(*) FROM cip").fetchone()[0]
    print(f"  CIP table: {count:,} rows")


def _build_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Build pre-computed aggregation Parquet files for the dashboard."""

    # 1a) Sankey: revenue source → fund type (left → middle)
    con.execute(f"""
        COPY (
            SELECT
                account_type AS revenue_source,
                fund_type,
                fiscal_year,
                budget_cycle,
                SUM(amount) AS amount
            FROM operating
            WHERE revenue_or_expense = 'Revenue'
              AND account_type IS NOT NULL
              AND fund_type IS NOT NULL
              AND source = 'budget'
            GROUP BY account_type, fund_type, fiscal_year, budget_cycle
            ORDER BY amount DESC
        ) TO '{AGGREGATED_DIR}/sankey_revenue.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] sankey_revenue")

    # 1b) Sankey: fund type → dept group (middle → right, expense side)
    con.execute(f"""
        COPY (
            SELECT
                fund_type,
                dept_group,
                fiscal_year,
                budget_cycle,
                SUM(amount) AS amount
            FROM operating
            WHERE revenue_or_expense = 'Expense'
              AND fund_type IS NOT NULL
              AND dept_group IS NOT NULL
              AND source = 'budget'
            GROUP BY fund_type, dept_group, fiscal_year, budget_cycle
            ORDER BY amount DESC
        ) TO '{AGGREGATED_DIR}/sankey_expense.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] sankey_expense")

    # 2) Department budget trends
    con.execute(f"""
        COPY (
            SELECT
                dept_group,
                dept_name,
                revenue_or_expense,
                fiscal_year,
                source,
                budget_cycle,
                SUM(amount) AS amount
            FROM operating
            WHERE dept_name IS NOT NULL
            GROUP BY dept_group, dept_name, revenue_or_expense, fiscal_year, source, budget_cycle
            ORDER BY dept_group, dept_name, fiscal_year
        ) TO '{AGGREGATED_DIR}/dept_budget_trends.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] dept_budget_trends")

    # 3) Fund allocation
    con.execute(f"""
        COPY (
            SELECT
                fund_type,
                revenue_or_expense,
                fiscal_year,
                account_type,
                budget_cycle,
                source,
                SUM(amount) AS amount
            FROM operating
            WHERE fund_type IS NOT NULL
            GROUP BY fund_type, revenue_or_expense, fiscal_year, account_type, budget_cycle, source
            ORDER BY fund_type, fiscal_year
        ) TO '{AGGREGATED_DIR}/fund_allocation.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] fund_allocation")

    # 4) Budget vs actuals
    con.execute(f"""
        COPY (
            SELECT
                dept_name,
                dept_group,
                fiscal_year,
                account_type,
                SUM(CASE WHEN source = 'budget' AND budget_cycle = 'adopted' THEN amount ELSE 0 END) AS budget_amount,
                SUM(CASE WHEN source = 'actual' THEN amount ELSE 0 END) AS actual_amount
            FROM operating
            WHERE dept_name IS NOT NULL
            GROUP BY dept_name, dept_group, fiscal_year, account_type
            HAVING (budget_amount != 0 OR actual_amount != 0)
            ORDER BY dept_name, fiscal_year
        ) TO '{AGGREGATED_DIR}/budget_vs_actuals.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] budget_vs_actuals")

    # 5) Revenue breakdown
    con.execute(f"""
        COPY (
            SELECT
                account_type,
                account_class,
                fiscal_year,
                budget_cycle,
                source,
                SUM(amount) AS amount
            FROM operating
            WHERE revenue_or_expense = 'Revenue'
              AND account_type IS NOT NULL
            GROUP BY account_type, account_class, fiscal_year, budget_cycle, source
            ORDER BY account_type, fiscal_year
        ) TO '{AGGREGATED_DIR}/revenue_breakdown.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] revenue_breakdown")

    # 6) Department detail (drill-down)
    con.execute(f"""
        COPY (
            SELECT
                dept_group,
                dept_name,
                dept_division,
                account_class,
                account_type,
                revenue_or_expense,
                fiscal_year,
                budget_cycle,
                source,
                SUM(amount) AS amount
            FROM operating
            WHERE dept_name IS NOT NULL
            GROUP BY dept_group, dept_name, dept_division, account_class, account_type,
                     revenue_or_expense, fiscal_year, budget_cycle, source
            ORDER BY dept_group, dept_name, fiscal_year
        ) TO '{AGGREGATED_DIR}/dept_detail.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] dept_detail")

    # 7) Council offices
    con.execute(f"""
        COPY (
            SELECT
                dept_name,
                account_class,
                account_type,
                fiscal_year,
                budget_cycle,
                source,
                SUM(amount) AS amount
            FROM operating
            WHERE dept_name LIKE 'Council District%'
               OR dept_name LIKE 'City Council%'
            GROUP BY dept_name, account_class, account_type, fiscal_year, budget_cycle, source
            ORDER BY dept_name, fiscal_year
        ) TO '{AGGREGATED_DIR}/council_offices.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] council_offices")

    # 8) General Fund summary
    con.execute(f"""
        COPY (
            SELECT
                dept_name,
                dept_group,
                account_class,
                account_type,
                revenue_or_expense,
                fiscal_year,
                budget_cycle,
                source,
                SUM(amount) AS amount
            FROM operating
            WHERE fund_type = 'General Fund'
            GROUP BY dept_name, dept_group, account_class, account_type,
                     revenue_or_expense, fiscal_year, budget_cycle, source
            ORDER BY dept_name, fiscal_year
        ) TO '{AGGREGATED_DIR}/general_fund_summary.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] general_fund_summary")

    # 9) CIP by department
    con.execute(f"""
        COPY (
            SELECT
                asset_owning_dept,
                project_name,
                fiscal_year,
                source,
                SUM(amount) AS amount
            FROM cip
            GROUP BY asset_owning_dept, project_name, fiscal_year, source
            ORDER BY asset_owning_dept, fiscal_year
        ) TO '{AGGREGATED_DIR}/cip_by_dept.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] cip_by_dept")


if __name__ == "__main__":
    transform()
