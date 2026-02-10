"""Shared query layer — all SQL lives here.

Both the FastAPI endpoints and MCP tools call these functions.
Each function creates a fresh DuckDB connection, queries parquet files,
and returns list[dict] (or dict for single-row responses).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# Resolve parquet directory relative to repo root
_ROOT = Path(__file__).resolve().parent.parent
_AGG = str(_ROOT / "data" / "aggregated")


def _q(where: str, condition: str) -> str:
    """Append a condition to a WHERE clause safely."""
    if not where:
        return f"WHERE {condition}"
    return f"{where} AND {condition}"


def _where(
    fy_min: int | None,
    fy_max: int | None,
    cycle: str | None = None,
    fund_type: str | None = None,
    dept_group: str | None = None,
    *,
    has_fund_type: bool = True,
    has_dept_group: bool = True,
    has_cycle: bool = True,
) -> str:
    """Build a WHERE clause from optional filter params.

    Respects which columns each parquet actually has.
    """
    clauses: list[str] = []
    if fy_min is not None:
        clauses.append(f"fiscal_year >= {int(fy_min)}")
    if fy_max is not None:
        clauses.append(f"fiscal_year <= {int(fy_max)}")
    if cycle and has_cycle:
        clauses.append(f"budget_cycle = '{cycle.replace(chr(39), chr(39)*2)}'")
    if fund_type and has_fund_type:
        clauses.append(f"fund_type = '{fund_type.replace(chr(39), chr(39)*2)}'")
    if dept_group and has_dept_group:
        clauses.append(f"dept_group = '{dept_group.replace(chr(39), chr(39)*2)}'")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def _run(sql: str) -> list[dict]:
    """Execute SQL and return list of row dicts."""
    con = duckdb.connect()
    df = con.execute(sql).fetchdf()
    con.close()
    return df.to_dict(orient="records")


# ── 1. Filter options ──


def get_filter_options() -> dict:
    """Return available fiscal years, budget cycles, fund types, and dept groups."""
    con = duckdb.connect()
    years = sorted(
        con.execute(
            f"SELECT DISTINCT fiscal_year FROM '{_AGG}/dept_budget_trends.parquet' "
            "WHERE fiscal_year IS NOT NULL ORDER BY fiscal_year"
        ).fetchdf()["fiscal_year"].tolist()
    )
    cycles = sorted(
        con.execute(
            f"SELECT DISTINCT budget_cycle FROM '{_AGG}/dept_budget_trends.parquet' "
            "WHERE budget_cycle IS NOT NULL ORDER BY budget_cycle"
        ).fetchdf()["budget_cycle"].tolist()
    )
    fund_types = con.execute(
        f"SELECT DISTINCT fund_type FROM '{_AGG}/fund_allocation.parquet' "
        "WHERE fund_type IS NOT NULL ORDER BY fund_type"
    ).fetchdf()["fund_type"].tolist()
    dept_groups = con.execute(
        f"SELECT DISTINCT dept_group FROM '{_AGG}/dept_budget_trends.parquet' "
        "WHERE dept_group IS NOT NULL ORDER BY dept_group"
    ).fetchdf()["dept_group"].tolist()
    con.close()
    return {
        "fiscal_years": [int(y) for y in years],
        "budget_cycles": cycles,
        "fund_types": fund_types,
        "dept_groups": dept_groups,
    }


# ── 2. Overview ──


def get_overview(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
    fund_type: str | None = None,
    dept_group: str | None = None,
) -> dict:
    """Total expense, total revenue, and general fund percentage."""
    con = duckdb.connect()

    # Expense total — dept_budget_trends (has dept_group, NO fund_type)
    w_exp = _where(fy_min, fy_max, cycle, dept_group=dept_group, has_fund_type=False)
    w_exp = _q(w_exp, "source = 'budget' AND revenue_or_expense = 'Expense'")
    total_expense = con.execute(
        f"SELECT COALESCE(SUM(amount), 0) AS total FROM '{_AGG}/dept_budget_trends.parquet' {w_exp}"
    ).fetchone()[0]

    # Revenue total — revenue_breakdown (NO dept_group, NO fund_type)
    w_rev = _where(fy_min, fy_max, cycle, has_fund_type=False, has_dept_group=False)
    w_rev = _q(w_rev, "source = 'budget'")
    total_revenue = con.execute(
        f"SELECT COALESCE(SUM(amount), 0) AS total FROM '{_AGG}/revenue_breakdown.parquet' {w_rev}"
    ).fetchone()[0]

    # General fund — fund_allocation (has fund_type, NO dept_group)
    w_gf = _where(fy_min, fy_max, cycle, fund_type="General Fund", has_dept_group=False)
    w_gf = _q(w_gf, "source = 'budget' AND revenue_or_expense = 'Expense'")
    total_gf = con.execute(
        f"SELECT COALESCE(SUM(amount), 0) AS total FROM '{_AGG}/fund_allocation.parquet' {w_gf}"
    ).fetchone()[0]

    con.close()
    gf_pct = (total_gf / total_expense * 100) if total_expense else 0
    return {
        "total_expense": float(total_expense),
        "total_revenue": float(total_revenue),
        "general_fund_pct": round(gf_pct, 1),
    }


# ── 3. Department spending ──


def get_department_spending(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
    dept_group: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Top N departments by expense amount."""
    w = _where(fy_min, fy_max, cycle, dept_group=dept_group, has_fund_type=False)
    w = _q(w, "source = 'budget' AND revenue_or_expense = 'Expense'")
    return _run(
        f"SELECT dept_name, SUM(amount) AS amount "
        f"FROM '{_AGG}/dept_budget_trends.parquet' {w} "
        f"GROUP BY dept_name ORDER BY amount DESC LIMIT {int(limit)}"
    )


# ── 4. Fund allocation ──


def get_fund_allocation(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
) -> list[dict]:
    """Spending by fund type."""
    w = _where(fy_min, fy_max, cycle, has_dept_group=False)
    w = _q(w, "source = 'budget' AND revenue_or_expense = 'Expense'")
    return _run(
        f"SELECT fund_type, SUM(amount) AS amount "
        f"FROM '{_AGG}/fund_allocation.parquet' {w} "
        f"GROUP BY fund_type ORDER BY amount DESC"
    )


# ── 5. Revenue sources ──


def get_revenue_sources(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
) -> list[dict]:
    """Revenue by account type."""
    w = _where(fy_min, fy_max, cycle, has_fund_type=False, has_dept_group=False)
    w = _q(w, "source = 'budget'")
    return _run(
        f"SELECT account_type, SUM(amount) AS amount "
        f"FROM '{_AGG}/revenue_breakdown.parquet' {w} "
        f"GROUP BY account_type ORDER BY amount DESC"
    )


# ── 6. Budget vs actuals ──


def get_budget_vs_actuals(
    fy_min: int = 2020,
    fy_max: int = 2023,
    limit: int = 15,
) -> list[dict]:
    """Budget vs actual spending by department.

    Actuals are only available through FY2023 (city data lag).
    """
    # budget_vs_actuals: has dept_group, NO fund_type, NO budget_cycle
    w = _where(fy_min, fy_max, has_fund_type=False, has_cycle=False)
    w = _q(w, "account_type IN ('Personnel', 'Non-Personnel')")
    return _run(
        f"SELECT dept_name, "
        f"  SUM(budget_amount) AS budget_amount, "
        f"  SUM(actual_amount) AS actual_amount, "
        f"  SUM(actual_amount) - SUM(budget_amount) AS variance "
        f"FROM '{_AGG}/budget_vs_actuals.parquet' {w} "
        f"GROUP BY dept_name HAVING SUM(budget_amount) != 0 "
        f"ORDER BY SUM(budget_amount) DESC LIMIT {int(limit)}"
    )


# ── 7. Department detail ──


def get_department_detail(
    dept_name: str,
    fy_min: int = 2024,
    fy_max: int = 2026,
) -> list[dict]:
    """Division/account breakdown for one department."""
    w = _where(fy_min, fy_max, has_fund_type=False)
    w = _q(w, "revenue_or_expense = 'Expense'")
    safe_name = dept_name.replace("'", "''")
    w = _q(w, f"dept_name = '{safe_name}'")
    return _run(
        f"SELECT dept_division AS division, account_class, "
        f"  SUM(CASE WHEN source = 'budget' THEN amount ELSE 0 END) AS budget, "
        f"  SUM(CASE WHEN source = 'actual' THEN amount ELSE 0 END) AS actual "
        f"FROM '{_AGG}/dept_detail.parquet' {w} "
        f"GROUP BY dept_division, account_class ORDER BY budget DESC"
    )


# ── 8. Spending trends ──


def get_spending_trends(
    fy_min: int = 2015,
    fy_max: int = 2026,
) -> list[dict]:
    """Year-over-year spending by department group (adopted budgets only)."""
    w = _where(fy_min, fy_max, cycle="adopted", has_fund_type=False)
    w = _q(w, "source = 'budget' AND revenue_or_expense = 'Expense' AND dept_group IS NOT NULL")
    return _run(
        f"SELECT fiscal_year, dept_group, SUM(amount) AS amount "
        f"FROM '{_AGG}/dept_budget_trends.parquet' {w} "
        f"GROUP BY fiscal_year, dept_group ORDER BY fiscal_year, amount DESC"
    )


# ── 9. Capital projects ──


def get_capital_projects(
    fy_min: int = 2024,
    fy_max: int = 2026,
    dept: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """CIP projects, optionally filtered by department."""
    # cip_by_dept: NO dept_group, NO fund_type, NO budget_cycle
    w = _where(fy_min, fy_max, has_fund_type=False, has_dept_group=False, has_cycle=False)
    w = _q(w, "source = 'budget'")
    if dept:
        safe_dept = dept.replace("'", "''")
        w = _q(w, f"asset_owning_dept = '{safe_dept}'")
    return _run(
        f"SELECT asset_owning_dept, project_name, SUM(amount) AS amount "
        f"FROM '{_AGG}/cip_by_dept.parquet' {w} "
        f"GROUP BY asset_owning_dept, project_name "
        f"ORDER BY amount DESC LIMIT {int(limit)}"
    )
