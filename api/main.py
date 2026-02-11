"""FastAPI app — thin wrappers around the shared query layer."""

from __future__ import annotations

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from api import queries
from api.models import (
    BudgetVsActual,
    CapitalProject,
    DepartmentDetail,
    DepartmentSpending,
    FilterOptions,
    FundAllocation,
    OverviewResponse,
    RevenueSource,
    SpendingTrend,
)

app = FastAPI(
    title="San Diego City Budget API",
    description=(
        "Query San Diego's city budget data: department spending, revenue sources, "
        "fund allocations, budget vs actuals, and capital projects. "
        "Data covers FY2011-FY2026 for budgets and FY2011-FY2023 for actuals."
    ),
    version="0.1.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
def health():
    """Debug endpoint — shows data path and file availability."""
    from pathlib import Path
    agg = Path(queries._AGG)
    files = sorted(p.name for p in agg.glob("*.parquet")) if agg.exists() else []
    return {"agg_path": str(agg), "exists": agg.exists(), "files": files}


@app.get("/")
def root():
    return {
        "message": "San Diego City Budget API",
        "docs": "/docs",
        "endpoints": [
            "/filters", "/overview", "/departments", "/funds",
            "/revenue", "/budget-vs-actuals", "/departments/{dept_name}",
            "/trends", "/capital-projects",
        ],
    }


@app.get("/filters", response_model=FilterOptions)
def filters():
    """Available fiscal years, budget cycles, fund types, and department groups."""
    return queries.get_filter_options()


@app.get("/overview", response_model=OverviewResponse)
def overview(
    fy_min: int = Query(2024, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
    cycle: str | None = Query(None, description="Budget cycle (adopted, proposed)"),
    fund_type: str | None = Query(None, description="Filter by fund type"),
    dept_group: str | None = Query(None, description="Filter by department group"),
):
    """Total expense, total revenue, and general fund percentage."""
    return queries.get_overview(fy_min, fy_max, cycle, fund_type, dept_group)


@app.get("/departments", response_model=list[DepartmentSpending])
def departments(
    fy_min: int = Query(2024, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
    cycle: str | None = Query(None, description="Budget cycle"),
    dept_group: str | None = Query(None, description="Filter by department group"),
    limit: int = Query(10, ge=1, le=100, description="Max departments to return"),
):
    """Top departments by expense spending."""
    return queries.get_department_spending(fy_min, fy_max, cycle, dept_group, limit)


@app.get("/funds", response_model=list[FundAllocation])
def funds(
    fy_min: int = Query(2024, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
    cycle: str | None = Query(None, description="Budget cycle"),
):
    """Spending breakdown by fund type."""
    return queries.get_fund_allocation(fy_min, fy_max, cycle)


@app.get("/revenue", response_model=list[RevenueSource])
def revenue(
    fy_min: int = Query(2024, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
    cycle: str | None = Query(None, description="Budget cycle"),
):
    """Revenue breakdown by source type."""
    return queries.get_revenue_sources(fy_min, fy_max, cycle)


@app.get("/budget-vs-actuals", response_model=list[BudgetVsActual])
def budget_vs_actuals(
    fy_min: int = Query(2020, description="Start fiscal year"),
    fy_max: int = Query(2023, description="End fiscal year (actuals available through FY2023)"),
    limit: int = Query(15, ge=1, le=100, description="Max departments to return"),
):
    """Budget vs actual spending by department. Actuals available through FY2023."""
    return queries.get_budget_vs_actuals(fy_min, fy_max, limit)


@app.get("/departments/{dept_name}", response_model=list[DepartmentDetail])
def department_detail(
    dept_name: str,
    fy_min: int = Query(2024, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
):
    """Division and account breakdown for a specific department."""
    return queries.get_department_detail(dept_name, fy_min, fy_max)


@app.get("/trends", response_model=list[SpendingTrend])
def trends(
    fy_min: int = Query(2015, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
):
    """Year-over-year spending trends by department group (adopted budgets)."""
    return queries.get_spending_trends(fy_min, fy_max)


@app.get("/capital-projects", response_model=list[CapitalProject])
def capital_projects(
    fy_min: int = Query(2024, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
    dept: str | None = Query(None, description="Filter by department (e.g. Fire-Rescue)"),
    limit: int = Query(20, ge=1, le=200, description="Max projects to return"),
):
    """Capital improvement projects, optionally filtered by department."""
    return queries.get_capital_projects(fy_min, fy_max, dept, limit)
