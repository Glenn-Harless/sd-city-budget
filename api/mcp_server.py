"""MCP server for San Diego city budget data.

Exposes 9 tools that let Claude query budget parquet files directly.
Uses FastMCP (v2) with stdio transport — spawned by Claude Code as a subprocess.
"""

from __future__ import annotations

from fastmcp import FastMCP

from api import queries

mcp = FastMCP(
    "San Diego City Budget",
    instructions=(
        "San Diego city budget data covering FY2011-FY2026 for budgets and "
        "FY2011-FY2023 for actuals. Call get_filter_options first to see "
        "available fiscal years, budget cycles, fund types, and department groups. "
        "Amounts are in US dollars."
    ),
)


@mcp.tool()
def get_filter_options() -> dict:
    """Get available filter values: fiscal years, budget cycles, fund types, and department groups.

    Call this first to see what values are valid for other tools.
    """
    return queries.get_filter_options()


@mcp.tool()
def get_overview(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
    fund_type: str | None = None,
    dept_group: str | None = None,
) -> dict:
    """Get budget overview: total expense, total revenue, and general fund percentage.

    Amounts are in US dollars. general_fund_pct is a percentage (0-100).
    """
    return queries.get_overview(fy_min, fy_max, cycle, fund_type, dept_group)


@mcp.tool()
def get_department_spending(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
    dept_group: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Get top departments by expense spending.

    Returns dept_name and amount (USD). Use limit to control how many (default 10).
    """
    return queries.get_department_spending(fy_min, fy_max, cycle, dept_group, limit)


@mcp.tool()
def get_fund_allocation(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
) -> list[dict]:
    """Get spending breakdown by fund type (General Fund, Enterprise Funds, etc.).

    Returns fund_type and amount (USD).
    """
    return queries.get_fund_allocation(fy_min, fy_max, cycle)


@mcp.tool()
def get_revenue_sources(
    fy_min: int = 2024,
    fy_max: int = 2026,
    cycle: str | None = None,
) -> list[dict]:
    """Get revenue breakdown by source type (Property Tax, Sales Tax, etc.).

    Returns account_type and amount (USD).
    """
    return queries.get_revenue_sources(fy_min, fy_max, cycle)


@mcp.tool()
def get_budget_vs_actuals(
    fy_min: int = 2020,
    fy_max: int = 2023,
    limit: int = 15,
) -> list[dict]:
    """Compare budgeted vs actual spending by department.

    IMPORTANT: Actual spending data is only available through FY2023.
    Returns dept_name, budget_amount, actual_amount, and variance (all USD).
    Positive variance = over budget.
    """
    return queries.get_budget_vs_actuals(fy_min, fy_max, limit)


@mcp.tool()
def get_department_detail(
    dept_name: str,
    fy_min: int = 2024,
    fy_max: int = 2026,
) -> list[dict]:
    """Get division and account breakdown for a specific department.

    Returns division, account_class, budget (USD), and actual (USD).
    Use get_department_spending or get_filter_options to find valid dept names.
    """
    return queries.get_department_detail(dept_name, fy_min, fy_max)


@mcp.tool()
def get_spending_trends(
    fy_min: int = 2015,
    fy_max: int = 2026,
) -> list[dict]:
    """Get year-over-year spending trends by department group.

    Uses adopted budgets only to avoid double-counting. Returns fiscal_year,
    dept_group, and amount (USD).
    """
    return queries.get_spending_trends(fy_min, fy_max)


@mcp.tool()
def get_capital_projects(
    fy_min: int = 2024,
    fy_max: int = 2026,
    dept: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Get capital improvement projects (CIP) — infrastructure investments.

    Optionally filter by department (e.g. "Fire-Rescue", "Public Utilities").
    Returns asset_owning_dept, project_name, and amount (USD).
    """
    return queries.get_capital_projects(fy_min, fy_max, dept, limit)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
