"""Pydantic response models for FastAPI's auto-generated OpenAPI docs."""

from __future__ import annotations

from pydantic import BaseModel


class FilterOptions(BaseModel):
    fiscal_years: list[int]
    budget_cycles: list[str]
    fund_types: list[str]
    dept_groups: list[str]


class OverviewResponse(BaseModel):
    total_expense: float
    total_revenue: float
    general_fund_pct: float


class DepartmentSpending(BaseModel):
    dept_name: str
    amount: float


class FundAllocation(BaseModel):
    fund_type: str
    amount: float


class RevenueSource(BaseModel):
    account_type: str
    amount: float


class BudgetVsActual(BaseModel):
    dept_name: str
    budget_amount: float
    actual_amount: float
    variance: float


class DepartmentDetail(BaseModel):
    division: str | None
    account_class: str | None
    budget: float
    actual: float


class SpendingTrend(BaseModel):
    fiscal_year: int
    dept_group: str
    amount: float


class CapitalProject(BaseModel):
    asset_owning_dept: str
    project_name: str | None
    amount: float
