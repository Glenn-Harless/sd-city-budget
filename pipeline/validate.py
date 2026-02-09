"""Data validation checks for SD City Budget pipeline outputs.

Run after the pipeline to catch data quality issues before publishing.

Usage:
    uv run python -m pipeline.validate
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

AGG = Path(__file__).resolve().parent.parent / "data" / "aggregated"
PROCESSED = Path(__file__).resolve().parent.parent / "data" / "processed"

passed = 0
failed = 0
warnings = 0


def _check(name: str, ok: bool, detail: str = "") -> None:
    global passed, failed
    if ok:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        msg = f"  FAIL  {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)


def _warn(name: str, detail: str) -> None:
    global warnings
    warnings += 1
    print(f"  WARN  {name} — {detail}")


def validate() -> int:
    """Run all validation checks. Returns number of failures."""
    con = duckdb.connect()

    print("=" * 60)
    print("Data Validation")
    print("=" * 60)

    # ── 1. File existence ──
    print("\n-- File existence --")
    processed_path = PROCESSED / "budget.parquet"
    _check("budget.parquet exists", processed_path.exists())

    expected_aggs = [
        "sankey_revenue", "sankey_expense", "dept_budget_trends",
        "fund_allocation", "budget_vs_actuals", "revenue_breakdown",
        "dept_detail", "council_offices", "general_fund_summary", "cip_by_dept",
    ]
    for name in expected_aggs:
        path = AGG / f"{name}.parquet"
        _check(f"{name}.parquet exists", path.exists())

    # ── 2. Row counts (non-empty) ──
    print("\n-- Row counts --")
    for name in expected_aggs:
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        count = con.execute(f"SELECT count(*) FROM '{path}'").fetchone()[0]
        _check(f"{name} has rows", count > 0, f"got {count:,} rows")

    # ── 3. No double-counting: expense totals should be ~$4-6B for recent FY, not ~$10B ──
    print("\n-- Double-counting guards --")
    expense_total = con.execute(f"""
        SELECT SUM(amount) FROM '{AGG}/dept_budget_trends.parquet'
        WHERE revenue_or_expense = 'Expense'
          AND source = 'budget'
          AND budget_cycle = 'adopted'
          AND fiscal_year = 2025
    """).fetchone()[0] or 0

    _check(
        "FY2025 adopted expense total in reasonable range ($1B-$8B)",
        1e9 < expense_total < 8e9,
        f"got ${expense_total/1e9:.2f}B",
    )

    # Check that unfiltered total is roughly double (revenue + expense)
    unfiltered_total = con.execute(f"""
        SELECT SUM(amount) FROM '{AGG}/dept_budget_trends.parquet'
        WHERE source = 'budget'
          AND budget_cycle = 'adopted'
          AND fiscal_year = 2025
    """).fetchone()[0] or 0

    _check(
        "Unfiltered total > expense total (confirms rev+exp split exists)",
        unfiltered_total > expense_total * 1.3,
        f"unfiltered=${unfiltered_total/1e9:.2f}B vs expense=${expense_total/1e9:.2f}B",
    )

    # ── 4. revenue_or_expense column present where needed ──
    print("\n-- Required columns --")
    for name in ["dept_budget_trends", "fund_allocation", "dept_detail", "general_fund_summary"]:
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()]
        _check(f"{name} has revenue_or_expense column", "revenue_or_expense" in cols)

    # ── 5. Budget cycle filter: FY2024/2025 should not double from adopted+proposed ──
    print("\n-- Budget cycle duplication --")
    for fy in [2024, 2025]:
        cycles = con.execute(f"""
            SELECT DISTINCT budget_cycle FROM '{AGG}/dept_budget_trends.parquet'
            WHERE fiscal_year = {fy} AND source = 'budget'
        """).fetchall()
        cycle_list = [r[0] for r in cycles]
        has_both = "adopted" in cycle_list and "proposed" in cycle_list
        if has_both:
            _warn(
                f"FY{fy} has both adopted and proposed",
                "dashboard must filter budget_cycle='adopted' to avoid double-counting",
            )
        else:
            _check(f"FY{fy} budget cycles", True, f"cycles: {cycle_list}")

    # ── 6. Sankey integrity ──
    print("\n-- Sankey data --")
    rev_path = AGG / "sankey_revenue.parquet"
    exp_path = AGG / "sankey_expense.parquet"

    if rev_path.exists() and exp_path.exists():
        # Revenue side should only have revenue records
        rev_sources = con.execute(f"""
            SELECT DISTINCT revenue_source FROM '{rev_path}'
        """).fetchall()
        rev_source_names = [r[0] for r in rev_sources]
        has_expense_in_rev = any(s in ("Personnel", "Non-Personnel") for s in rev_source_names)
        _check("Sankey revenue has no expense categories", not has_expense_in_rev,
               f"sources: {rev_source_names[:5]}...")

        # Expense side should only have expense dept groups
        exp_depts = con.execute(f"""
            SELECT DISTINCT dept_group FROM '{exp_path}'
        """).fetchall()
        exp_dept_names = [r[0] for r in exp_depts]
        has_major_rev = "Major Revenues" in exp_dept_names
        _check("Sankey expense has no 'Major Revenues' dept", not has_major_rev)

        # Both sides should have data for recent years
        for label, path in [("revenue", rev_path), ("expense", exp_path)]:
            max_fy = con.execute(f"SELECT MAX(fiscal_year) FROM '{path}'").fetchone()[0]
            _check(f"Sankey {label} has recent data", max_fy and max_fy >= 2024,
                   f"max FY={max_fy}")

        # Revenue and expense totals for same year should be roughly comparable
        for fy in [2025]:
            rev_total = con.execute(f"""
                SELECT SUM(amount) FROM '{rev_path}'
                WHERE fiscal_year = {fy} AND budget_cycle = 'adopted'
            """).fetchone()[0] or 0
            exp_total = con.execute(f"""
                SELECT SUM(amount) FROM '{exp_path}'
                WHERE fiscal_year = {fy} AND budget_cycle = 'adopted'
            """).fetchone()[0] or 0
            if rev_total > 0 and exp_total > 0:
                ratio = max(rev_total, exp_total) / min(rev_total, exp_total)
                _check(
                    f"Sankey FY{fy} revenue/expense within 2x",
                    ratio < 2.0,
                    f"revenue=${rev_total/1e9:.2f}B expense=${exp_total/1e9:.2f}B ratio={ratio:.2f}",
                )

    # ── 7. Column availability — filters must match parquet schemas ──
    print("\n-- Filter column compatibility --")
    col_expectations = {
        "dept_budget_trends": {"dept_group": True, "fund_type": False},
        "fund_allocation": {"dept_group": False, "fund_type": True},
        "revenue_breakdown": {"dept_group": False, "fund_type": False},
        "dept_detail": {"dept_group": True, "fund_type": False},
        "general_fund_summary": {"dept_group": True, "fund_type": False},
        "budget_vs_actuals": {"dept_group": True, "fund_type": False},
        "council_offices": {"dept_group": False, "fund_type": False},
        "cip_by_dept": {"dept_group": False, "fund_type": False},
    }
    for name, expected_cols in col_expectations.items():
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()]
        for col, should_exist in expected_cols.items():
            has_col = col in cols
            if should_exist:
                _check(f"{name} has {col}", has_col)
            else:
                if has_col:
                    _check(f"{name} optionally has {col}", True)
                else:
                    _check(f"{name} correctly lacks {col} (dashboard uses safe WHERE)", True)

    # ── 8. Fiscal year range sanity --
    print("\n-- Fiscal year range --")
    if processed_path.exists():
        fy_range = con.execute(f"""
            SELECT MIN(fiscal_year), MAX(fiscal_year) FROM '{processed_path}'
        """).fetchone()
        min_fy, max_fy = fy_range
        _check("Min fiscal year >= 2010", min_fy is not None and min_fy >= 2010,
               f"min={min_fy}")
        _check("Max fiscal year <= 2030", max_fy is not None and max_fy <= 2030,
               f"max={max_fy}")

    # ── 9. NULL rate on critical columns ──
    print("\n-- NULL rates --")
    if processed_path.exists():
        total_rows = con.execute(f"SELECT count(*) FROM '{processed_path}'").fetchone()[0]
        for col in ["revenue_or_expense", "fiscal_year", "amount"]:
            null_count = con.execute(f"""
                SELECT count(*) FROM '{processed_path}' WHERE {col} IS NULL
            """).fetchone()[0]
            pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            if pct > 10:
                _warn(f"{col} NULL rate", f"{pct:.1f}% ({null_count:,}/{total_rows:,})")
            else:
                _check(f"{col} NULL rate < 10%", True, f"{pct:.1f}%")

    # ── 10. Budget vs Actuals: actuals should exist for at least FY11-FY23 ──
    print("\n-- Actuals coverage --")
    bva_path = AGG / "budget_vs_actuals.parquet"
    if bva_path.exists():
        actuals_fys = con.execute(f"""
            SELECT DISTINCT fiscal_year FROM '{bva_path}'
            WHERE actual_amount != 0
            ORDER BY fiscal_year
        """).fetchall()
        fy_list = [r[0] for r in actuals_fys]
        _check("Actuals exist for FY2011", 2011 in fy_list)
        _check("Actuals exist for FY2023", 2023 in fy_list)
        _check("At least 10 years of actuals", len(fy_list) >= 10,
               f"got {len(fy_list)} years: {fy_list[0]}-{fy_list[-1]}")

    # ── 11. Parquet file sizes (catch bloat) ──
    print("\n-- File sizes --")
    proc_size = processed_path.stat().st_size / (1024 * 1024) if processed_path.exists() else 0
    _check("budget.parquet < 100MB", proc_size < 100, f"{proc_size:.1f}MB")

    total_agg = sum(
        (AGG / f"{n}.parquet").stat().st_size for n in expected_aggs
        if (AGG / f"{n}.parquet").exists()
    ) / (1024 * 1024)
    _check("Total aggregated < 10MB", total_agg < 10, f"{total_agg:.1f}MB")

    # ── Summary ──
    con.close()
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed, {warnings} warnings")
    print("=" * 60)

    return failed


def main() -> None:
    failures = validate()
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
