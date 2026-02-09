"""Streamlit dashboard for San Diego city budget analysis."""

from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.graph_objects as go
import streamlit as st

# ── Parquet paths (relative to repo root, where Streamlit Cloud runs) ──
_AGG = "data/aggregated"

# Resolve paths for local dev (running from project root or dashboard/)
_root = Path(__file__).resolve().parent.parent
if (_root / _AGG).exists():
    _AGG = str(_root / _AGG)

st.set_page_config(
    page_title="San Diego City Budget",
    page_icon="\U0001f4b0",
    layout="wide",
)

# Light blue — default chart color across all visualizations
CHART_COLOR = "#83c9ff"


def query(sql: str, params: list | None = None):
    """Run SQL against parquet files and return a pandas DataFrame."""
    con = duckdb.connect()
    return con.execute(sql, params or []).fetchdf()


# ── Sidebar filters ──
st.sidebar.title("Filters")


@st.cache_data(ttl=3600)
def _sidebar_options():
    years = sorted(query(f"""
        SELECT DISTINCT fiscal_year FROM '{_AGG}/dept_budget_trends.parquet'
        WHERE fiscal_year IS NOT NULL
        ORDER BY fiscal_year
    """)["fiscal_year"].tolist())

    cycles = sorted(query(f"""
        SELECT DISTINCT budget_cycle FROM '{_AGG}/dept_budget_trends.parquet'
        WHERE budget_cycle IS NOT NULL
        ORDER BY budget_cycle
    """)["budget_cycle"].tolist())

    fund_types = query(f"""
        SELECT DISTINCT fund_type FROM '{_AGG}/fund_allocation.parquet'
        WHERE fund_type IS NOT NULL
        ORDER BY fund_type
    """)["fund_type"].tolist()

    dept_groups = query(f"""
        SELECT DISTINCT dept_group FROM '{_AGG}/dept_budget_trends.parquet'
        WHERE dept_group IS NOT NULL
        ORDER BY dept_group
    """)["dept_group"].tolist()

    return years, cycles, fund_types, dept_groups


all_years, all_cycles, all_fund_types, all_dept_groups = _sidebar_options()

if all_years:
    year_range = st.sidebar.slider(
        "Fiscal Year Range",
        min_value=int(min(all_years)),
        max_value=int(max(all_years)),
        value=(int(max(all_years)) - 1, int(max(all_years))),
    )
else:
    year_range = (2020, 2026)

budget_cycle = st.sidebar.selectbox(
    "Budget Cycle",
    options=["All"] + all_cycles,
    index=0,
)

selected_fund_types = st.sidebar.multiselect(
    "Fund Type",
    options=all_fund_types,
    default=None,
    placeholder="All fund types",
)

selected_dept_groups = st.sidebar.multiselect(
    "Department Group",
    options=all_dept_groups,
    default=None,
    placeholder="All departments",
)


def _where_clause(
    yr: tuple[int, int] = year_range,
    cycle: str = budget_cycle,
    fund_types: list[str] = selected_fund_types,
    dept_groups: list[str] = selected_dept_groups,
) -> str:
    """Build WHERE clause from sidebar filter selections."""
    clauses = [f"fiscal_year BETWEEN {yr[0]} AND {yr[1]}"]
    if cycle != "All":
        clauses.append(f"budget_cycle = '{cycle.replace(chr(39), chr(39)*2)}'")
    if fund_types:
        escaped = ", ".join(f"'{t.replace(chr(39), chr(39)*2)}'" for t in fund_types)
        clauses.append(f"fund_type IN ({escaped})")
    if dept_groups:
        escaped = ", ".join(f"'{g.replace(chr(39), chr(39)*2)}'" for g in dept_groups)
        clauses.append(f"dept_group IN ({escaped})")
    return "WHERE " + " AND ".join(clauses)


WHERE = _where_clause()

# ── Header ──
st.title("San Diego City Budget")
st.markdown(
    "Explore how San Diego allocates its budget across departments, funds, and revenue "
    "sources. Data covers **FY2011-FY2026** for operating budgets and **FY2011-FY2023** "
    "for actuals. Sourced from the city's [open data portal](https://data.sandiego.gov). "
    "Use the sidebar filters to narrow by fiscal year, budget cycle, fund type, or department."
)

# ==================================================================
# Tab layout
# ==================================================================
tab_sankey, tab_overview, tab_bva, tab_trends, tab_deep = st.tabs(
    ["Money Flow", "Overview", "Budget vs Actuals", "Trends", "Deep Dive"]
)

# ── TAB 1: Money Flow (Sankey) ──
with tab_sankey:
    st.subheader("Where Does Your Tax Dollar Go?")
    st.caption(
        "Revenue sources (left) flow through fund types (middle) into department spending (right). "
        f"Showing the most recent year in your selected range (**FY{year_range[1]}**). "
        "Note: Inflows to a fund may exceed outflows because some revenue goes to reserves, "
        "debt service, fund balance, or capital projects not captured in operating expenses."
    )

    # Use the most recent year in the range for the Sankey
    sankey_year = year_range[1]
    sankey_cycle = budget_cycle if budget_cycle != "All" else "adopted"

    # Layer 1: revenue source → fund type (from revenue records)
    layer1 = query(f"""
        SELECT revenue_source, fund_type, SUM(amount) AS amount
        FROM '{_AGG}/sankey_revenue.parquet'
        WHERE fiscal_year = {sankey_year}
          AND budget_cycle = '{sankey_cycle}'
        GROUP BY revenue_source, fund_type
        HAVING SUM(amount) > 0
        ORDER BY amount DESC
    """)

    # Layer 2: fund type → dept group (from expense records)
    layer2 = query(f"""
        SELECT fund_type, dept_group, SUM(amount) AS amount
        FROM '{_AGG}/sankey_expense.parquet'
        WHERE fiscal_year = {sankey_year}
          AND budget_cycle = '{sankey_cycle}'
        GROUP BY fund_type, dept_group
        HAVING SUM(amount) > 0
        ORDER BY amount DESC
    """)

    if layer1.empty and layer2.empty:
        st.info(f"No Sankey data available for FY{sankey_year} ({sankey_cycle}).")
    else:
        # Build node labels: revenue sources + fund types + dept groups
        rev_sources = layer1["revenue_source"].unique().tolist() if not layer1.empty else []
        fund_types_l1 = set(layer1["fund_type"].unique()) if not layer1.empty else set()
        fund_types_l2 = set(layer2["fund_type"].unique()) if not layer2.empty else set()
        fund_types_s = sorted(fund_types_l1 | fund_types_l2)
        dept_groups_s = layer2["dept_group"].unique().tolist() if not layer2.empty else []

        labels = rev_sources + fund_types_s + dept_groups_s
        label_idx = {name: i for i, name in enumerate(labels)}

        sources = (
            [label_idx[r] for r in layer1["revenue_source"]]
            + [label_idx[f] for f in layer2["fund_type"]]
        )
        targets = (
            [label_idx[f] for f in layer1["fund_type"]]
            + [label_idx[d] for d in layer2["dept_group"]]
        )
        values = layer1["amount"].tolist() + layer2["amount"].tolist()

        # Distinct color per node — revenue greens, fund blues, dept warm tones
        _rev_palette = [
            "#2ecc71", "#27ae60", "#1abc9c", "#16a085", "#3498db",
            "#2980b9", "#0097a7", "#00897b", "#43a047", "#66bb6a",
            "#4db6ac", "#26a69a", "#81c784",
        ]
        _fund_palette = [
            "#5c6bc0", "#42a5f5", "#7e57c2", "#5e35b1",
            "#3949ab", "#1e88e5", "#039be5",
        ]
        _dept_palette = [
            "#ef5350", "#ec407a", "#ab47bc", "#ff7043",
            "#ffa726", "#ffca28", "#d4e157", "#66bb6a",
            "#26c6da", "#78909c", "#8d6e63", "#f06292",
            "#ba68c8", "#ff8a65", "#ffb74d", "#fff176",
            "#aed581", "#4db6ac", "#4dd0e1", "#90a4ae",
            "#a1887f", "#e57373", "#f48fb1", "#ce93d8",
            "#ffab91", "#ffe082", "#c5e1a5", "#80cbc4",
            "#80deea", "#b0bec5", "#bcaaa4", "#ef9a9a",
            "#f8bbd0", "#d1c4e9", "#ffccbc", "#fff9c4",
            "#dcedc8", "#b2dfdb", "#b2ebf2", "#cfd8dc",
            "#d7ccc8", "#e0e0e0", "#f5f5f5", "#ffcdd2",
            "#e1bee7", "#c5cae9", "#bbdefb", "#b3e5fc",
            "#b2ebf2", "#b2dfdb", "#c8e6c9", "#f0f4c3",
            "#fff9c4", "#ffecb3", "#ffe0b2", "#ffccbc",
        ]

        node_colors = (
            [_rev_palette[i % len(_rev_palette)] for i in range(len(rev_sources))]
            + [_fund_palette[i % len(_fund_palette)] for i in range(len(fund_types_s))]
            + [_dept_palette[i % len(_dept_palette)] for i in range(len(dept_groups_s))]
        )

        # Link colors: semi-transparent version of the source node color
        def _to_rgba(hex_color: str, alpha: float = 0.35) -> str:
            r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
            return f"rgba({r},{g},{b},{alpha})"

        link_colors = [_to_rgba(node_colors[s]) for s in sources]

        fig = go.Figure(go.Sankey(
            textfont=dict(size=14, color="white", family="sans-serif"),
            node=dict(
                pad=15,
                thickness=20,
                label=labels,
                color=node_colors,
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values,
                color=link_colors,
            ),
        ))
        fig.update_layout(
            height=650,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

        # KPI metrics
        total_revenue = layer1["amount"].sum() if not layer1.empty else 0
        sd_population = 1_386_000  # 2023 census estimate
        col1, col2 = st.columns(2)
        col1.metric(
            f"Total Revenue FY{sankey_year}",
            f"${total_revenue / 1e9:.2f}B",
        )
        col2.metric(
            "Your Share (per resident)",
            f"${total_revenue / sd_population:,.0f}",
            help="Total revenue divided by San Diego's estimated population of ~1.4M (2023 Census estimate).",
        )

    with st.expander("What do these labels mean?"):
        gl, gm, gr = st.columns(3)
        with gl:
            st.markdown("""**Revenue Sources** (left)

Where the money comes from:
- **Charges for Current Services** — fees for city services: water/sewer bills, permits, recreation fees, parking
- **Property Tax Revenue** — taxes on real estate (the city's single largest General Fund source)
- **Other Revenue** — miscellaneous: asset sales, reimbursements, prior-year adjustments
- **Sales Taxes** — the city's local share of California state sales tax (Bradley-Burns 1%)
- **Transfers In** — money moved between city funds (e.g. General Fund transferring to a capital project fund)
- **Other Local Taxes** — franchise fees paid by SDG&E, cable companies, property transfer tax
- **Transient Occupancy Taxes** — 10.5% tax on hotel and short-term rental stays
- **Licenses and Permits** — building permits, business licenses, encroachment permits
- **Revenue from Use of Money & Property** — rents, concessions, interest income (e.g. Mission Bay leases)
- **Fines, Forfeitures & Penalties** — parking citations, code enforcement fines, court penalties
- **Revenue from Other Agencies** — state/county shared revenue, motor vehicle license fees
- **Revenue from Federal Agencies** — federal grants and reimbursements
- **Special Assessments** — charges on specific properties for local improvements (lighting, landscaping)
""")
        with gm:
            st.markdown("""**Fund Types** (middle)

How money is organized:
- **Enterprise Funds** — self-supporting services funded by user fees, not taxes. Includes water, sewer, airports, golf courses, refuse, and Development Services. The largest fund type by far.
- **General Fund** — the city's main discretionary account funded by taxes. Pays for police, fire, parks, libraries, and most city services residents interact with.
- **Special Revenue Funds** — legally earmarked for specific purposes. Includes gas tax (roads), Maintenance Assessment Districts (neighborhood upkeep), parking meter revenue, and transient occupancy tax.
- **Internal Service Funds** — departments that provide shared services and bill other departments. Includes Fleet Operations, IT, energy conservation, and central stores.
- **Capital Project Funds** — funding for long-term infrastructure: buildings, roads, water/sewer pipes, parks. Often funded by bonds or developer impact fees.
- **Debt Service and Tax Funds** — payments on bonds and short-term borrowing (Tax and Revenue Anticipation Notes).
""")
        with gr:
            st.markdown("""**Department Groups** (right)

Who spends the money:
- **Public Utilities** — water and sewer systems, the city's largest operation (~$1.2B, almost entirely enterprise funds from ratepayer bills)
- **Police** — law enforcement, the largest General Fund department (~$680M)
- **Fire-Rescue** — fire suppression, EMS/ambulance, lifeguards, community risk reduction (~$480M)
- **Parks & Recreation** — 400+ parks, rec centers, Balboa Park, Mission Bay Park, open space
- **Citywide Program Expenditures** — citywide costs not tied to one department: pension obligations, retiree health care, citywide contracts
- **Environmental Services** — waste collection, recycling, landfills, environmental compliance
- **Transportation** — street maintenance, traffic signals, streetlights, transit coordination
- **General Services** — facilities maintenance, security, fleet operations, building management
- **Engineering & Capital Projects** — design and construction of city infrastructure
- **Special Promotional Programs** — tourism marketing, convention center promotion (funded by hotel tax)
- **Citywide Other/Special Funds** — debt service, insurance reserves, special fund expenditures
- **Development Services** — building permits, plan review, code enforcement (enterprise fund)
- **Dept. of Information Technology** — citywide IT infrastructure, systems, cybersecurity
- **City Attorney** — legal counsel, litigation, prosecution
- **Library** — 36 branch libraries and the Central Library
- **Stormwater** — storm drain maintenance, water quality, flood control
- **Homelessness Strategies & Solutions** — shelters, outreach, housing programs
- **Real Estate & Airport Management** — city-owned properties, airport operations
- **Redevelopment Agency / Housing Successor** — wind-down of dissolved redevelopment agency, affordable housing obligations
- **City Treasurer** — investment management, debt administration
- **Other departments** — City Council, Planning, Purchasing & Contracting, Economic Development, Sustainability & Mobility, Risk Management, Human Resources, City Clerk, and more
""")

# ── TAB 2: Overview ──
with tab_overview:
    # KPI row
    kpi_expense = query(f"""
        SELECT SUM(amount) AS total
        FROM '{_AGG}/dept_budget_trends.parquet'
        {WHERE} AND source = 'budget' AND revenue_or_expense = 'Expense'
    """)
    kpi_revenue = query(f"""
        SELECT SUM(amount) AS total
        FROM '{_AGG}/revenue_breakdown.parquet'
        {WHERE} AND source = 'budget'
    """)
    kpi_gf = query(f"""
        SELECT SUM(amount) AS total
        FROM '{_AGG}/fund_allocation.parquet'
        {WHERE} AND fund_type = 'General Fund' AND source = 'budget'
          AND revenue_or_expense = 'Expense'
    """)

    total_expense = kpi_expense["total"].iloc[0] if not kpi_expense.empty else 0
    total_revenue = kpi_revenue["total"].iloc[0] if not kpi_revenue.empty else 0
    total_gf = kpi_gf["total"].iloc[0] if not kpi_gf.empty else 0

    total_expense = total_expense if total_expense else 0
    total_revenue = total_revenue if total_revenue else 0
    total_gf = total_gf if total_gf else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Budget (Expense)", f"${total_expense / 1e9:.2f}B" if total_expense else "N/A")
    col2.metric("Total Revenue", f"${total_revenue / 1e9:.2f}B" if total_revenue else "N/A")
    col3.metric(
        "General Fund %",
        f"{total_gf / total_expense * 100:.0f}%" if total_expense else "N/A",
    )

    # Top departments by spending
    chart_left, chart_right = st.columns(2)

    with chart_left:
        st.subheader("Top 10 Departments by Spending")
        top_dept = query(f"""
            SELECT dept_name AS "Department", SUM(amount) AS "Amount"
            FROM '{_AGG}/dept_budget_trends.parquet'
            {WHERE} AND source = 'budget' AND revenue_or_expense = 'Expense'
            GROUP BY dept_name
            ORDER BY "Amount" DESC
            LIMIT 10
        """)
        if not top_dept.empty:
            top_dept["Amount"] = top_dept["Amount"] / 1e6
            st.bar_chart(top_dept.set_index("Department"), horizontal=True, y_label="Millions ($)", color=CHART_COLOR)

    with chart_right:
        st.subheader("Spending by Fund Type")
        fund_dist = query(f"""
            SELECT fund_type AS "Fund Type", SUM(amount) AS "Amount"
            FROM '{_AGG}/fund_allocation.parquet'
            {WHERE} AND source = 'budget' AND revenue_or_expense = 'Expense'
            GROUP BY fund_type
            ORDER BY "Amount" DESC
        """)
        if not fund_dist.empty:
            fund_dist["Amount"] = fund_dist["Amount"] / 1e6
            st.bar_chart(fund_dist.set_index("Fund Type"), horizontal=True, y_label="Millions ($)", color=CHART_COLOR)

    # Revenue sources
    st.subheader("Revenue Sources")
    rev_src = query(f"""
        SELECT account_type AS "Revenue Source", SUM(amount) AS "Amount"
        FROM '{_AGG}/revenue_breakdown.parquet'
        {WHERE} AND source = 'budget'
        GROUP BY account_type
        ORDER BY "Amount" DESC
    """)
    if not rev_src.empty:
        rev_src["Amount"] = rev_src["Amount"] / 1e6
        st.bar_chart(rev_src.set_index("Revenue Source"), horizontal=True, y_label="Millions ($)", color=CHART_COLOR)

    # Department breakdown table
    with st.expander("Full Department Breakdown"):
        detail = query(f"""
            SELECT
                dept_name AS "Department",
                dept_division AS "Division",
                account_class AS "Category",
                SUM(amount) AS "Budget ($)"
            FROM '{_AGG}/dept_detail.parquet'
            {WHERE} AND source = 'budget' AND revenue_or_expense = 'Expense'
            GROUP BY dept_name, dept_division, account_class
            ORDER BY dept_name, "Budget ($)" DESC
        """)
        if not detail.empty:
            st.dataframe(
                detail,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Budget ($)": st.column_config.NumberColumn(
                        "Budget ($)", format="$ %.0f"
                    ),
                },
            )

# ── TAB 3: Budget vs Actuals ──
with tab_bva:
    st.subheader("Budget vs Actual Spending")
    st.caption(
        "Compares adopted budget to actual spending. Actuals are only available through FY2023 "
        "because the City of San Diego has not yet published more recent spending data to the "
        "open data portal. This can lag 12-18+ months behind the current fiscal year."
    )

    bva = query(f"""
        SELECT
            dept_name,
            SUM(budget_amount) AS budget_total,
            SUM(actual_amount) AS actual_total,
            SUM(actual_amount) - SUM(budget_amount) AS variance
        FROM '{_AGG}/budget_vs_actuals.parquet'
        WHERE fiscal_year BETWEEN {year_range[0]} AND {year_range[1]}
          AND account_type IN ('Personnel', 'Non-Personnel')
        GROUP BY dept_name
        HAVING SUM(budget_amount) != 0
        ORDER BY SUM(budget_amount) DESC
        LIMIT 15
    """)

    if bva.empty:
        st.info("No budget vs actuals data available for the selected filters.")
    else:
        # Execution rate
        total_budget = bva["budget_total"].sum()
        total_actual = bva["actual_total"].sum()
        exec_rate = total_actual / total_budget * 100 if total_budget else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Budgeted", f"${total_budget / 1e9:.2f}B")
        col2.metric("Total Actual", f"${total_actual / 1e9:.2f}B")
        col3.metric("Execution Rate", f"{exec_rate:.1f}%")

        # Side-by-side bar
        chart_data = bva[["dept_name", "budget_total", "actual_total"]].copy()
        chart_data["budget_total"] = chart_data["budget_total"] / 1e6
        chart_data["actual_total"] = chart_data["actual_total"] / 1e6
        chart_data = chart_data.rename(columns={
            "dept_name": "Department",
            "budget_total": "Budget ($M)",
            "actual_total": "Actual ($M)",
        }).set_index("Department")
        st.bar_chart(chart_data, horizontal=True, color=[CHART_COLOR, "#2a6496"])

    # Over/under spends
    st.subheader("Biggest Over/Under Spends")
    variance = query(f"""
        SELECT
            dept_name AS "Department",
            SUM(budget_amount) AS "Budget",
            SUM(actual_amount) AS "Actual",
            SUM(actual_amount) - SUM(budget_amount) AS "Over/Under"
        FROM '{_AGG}/budget_vs_actuals.parquet'
        WHERE fiscal_year BETWEEN {year_range[0]} AND {year_range[1]}
          AND account_type IN ('Personnel', 'Non-Personnel')
        GROUP BY dept_name
        HAVING SUM(budget_amount) != 0
        ORDER BY ABS(SUM(actual_amount) - SUM(budget_amount)) DESC
        LIMIT 15
    """)
    if not variance.empty:
        for col_name in ["Budget", "Actual", "Over/Under"]:
            variance[col_name] = variance[col_name].apply(lambda x: f"${x:,.0f}")
        st.dataframe(variance, use_container_width=True, hide_index=True)

# ── TAB 4: Trends ──
with tab_trends:
    st.subheader("Total Budget Over Time")
    total_trend = query(f"""
        SELECT fiscal_year AS "Fiscal Year", SUM(amount) / 1e9 AS "Budget ($B)"
        FROM '{_AGG}/dept_budget_trends.parquet'
        WHERE source = 'budget' AND budget_cycle = 'adopted'
          AND revenue_or_expense = 'Expense'
        GROUP BY fiscal_year
        ORDER BY fiscal_year
    """)
    if not total_trend.empty:
        total_trend["Fiscal Year"] = total_trend["Fiscal Year"].astype(str)
        st.line_chart(total_trend.set_index("Fiscal Year"), color=CHART_COLOR)

    # Department group trends
    st.subheader("Spending by Department Group")
    dept_trend = query(f"""
        SELECT fiscal_year, dept_group, SUM(amount) / 1e6 AS amount_m
        FROM '{_AGG}/dept_budget_trends.parquet'
        WHERE source = 'budget' AND budget_cycle = 'adopted'
          AND revenue_or_expense = 'Expense'
          AND dept_group IS NOT NULL
        GROUP BY fiscal_year, dept_group
        ORDER BY fiscal_year
    """)
    if not dept_trend.empty:
        pivot = dept_trend.pivot_table(
            index="fiscal_year", columns="dept_group", values="amount_m", fill_value=0
        )
        # Keep only top 10 dept groups by total
        top_groups = pivot.sum().nlargest(10).index.tolist()
        pivot = pivot[top_groups]
        pivot.index = pivot.index.astype(str)
        st.area_chart(pivot)

    # Revenue trend
    st.subheader("Revenue by Source Over Time")
    rev_trend = query(f"""
        SELECT fiscal_year, account_type, SUM(amount) / 1e6 AS amount_m
        FROM '{_AGG}/revenue_breakdown.parquet'
        WHERE source = 'budget' AND budget_cycle = 'adopted'
        GROUP BY fiscal_year, account_type
        ORDER BY fiscal_year
    """)
    if not rev_trend.empty:
        rev_pivot = rev_trend.pivot_table(
            index="fiscal_year", columns="account_type", values="amount_m", fill_value=0
        )
        top_rev = rev_pivot.sum().nlargest(8).index.tolist()
        rev_pivot = rev_pivot[top_rev]
        rev_pivot.index = rev_pivot.index.astype(str)
        st.area_chart(rev_pivot)

    # General Fund trend
    st.subheader("General Fund Budget Over Time")
    gf_trend = query(f"""
        SELECT fiscal_year AS "Fiscal Year", SUM(amount) / 1e9 AS "General Fund ($B)"
        FROM '{_AGG}/general_fund_summary.parquet'
        WHERE source = 'budget' AND budget_cycle = 'adopted'
          AND revenue_or_expense = 'Expense'
        GROUP BY fiscal_year
        ORDER BY fiscal_year
    """)
    if not gf_trend.empty:
        gf_trend["Fiscal Year"] = gf_trend["Fiscal Year"].astype(str)
        st.line_chart(gf_trend.set_index("Fiscal Year"), color=CHART_COLOR)

# ── TAB 5: Deep Dive ──
with tab_deep:
    # Department drill-down
    st.subheader("Department Detail")

    dept_list = query(f"""
        SELECT DISTINCT dept_name FROM '{_AGG}/dept_detail.parquet'
        WHERE dept_name IS NOT NULL
        ORDER BY dept_name
    """)["dept_name"].tolist()

    selected_dept = st.selectbox("Select Department", options=dept_list)

    if selected_dept:
        dept_data = query(f"""
            SELECT
                dept_division AS "Division",
                account_class AS "Account Class",
                SUM(CASE WHEN source = 'budget' THEN amount ELSE 0 END) AS "Budget",
                SUM(CASE WHEN source = 'actual' THEN amount ELSE 0 END) AS "Actual"
            FROM '{_AGG}/dept_detail.parquet'
            WHERE dept_name = $1
              AND fiscal_year BETWEEN {year_range[0]} AND {year_range[1]}
              AND revenue_or_expense = 'Expense'
            GROUP BY dept_division, account_class
            ORDER BY "Budget" DESC
        """, [selected_dept])
        if not dept_data.empty:
            for col_name in ["Budget", "Actual"]:
                dept_data[col_name] = dept_data[col_name].apply(lambda x: f"${x:,.0f}")
            st.dataframe(dept_data, use_container_width=True, hide_index=True)
        else:
            st.info(f"No detail data for {selected_dept} in the selected year range.")

    # General Fund focus
    st.subheader("General Fund — Top Departments")
    gf_depts = query(f"""
        SELECT dept_name AS "Department", SUM(amount) / 1e6 AS "Amount ($M)"
        FROM '{_AGG}/general_fund_summary.parquet'
        {WHERE} AND source = 'budget' AND revenue_or_expense = 'Expense'
        GROUP BY dept_name
        ORDER BY SUM(amount) DESC
        LIMIT 15
    """)
    if not gf_depts.empty:
        st.bar_chart(gf_depts.set_index("Department"), horizontal=True, color=CHART_COLOR)

    # Council office budgets
    st.subheader("Council Office Budgets")
    st.caption(
        "Note: This shows council office operating budgets (~$15M total), not total "
        "spending allocated to each council district. District-level spending data is "
        "not available in the city's open budget datasets."
    )
    council = query(f"""
        SELECT dept_name AS "Council Office", SUM(amount) / 1e6 AS "Budget ($M)"
        FROM '{_AGG}/council_offices.parquet'
        WHERE fiscal_year BETWEEN {year_range[0]} AND {year_range[1]}
          AND source = 'budget'
        GROUP BY dept_name
        ORDER BY dept_name
    """)
    if not council.empty:
        st.bar_chart(council.set_index("Council Office"), horizontal=True, color=CHART_COLOR)

    # Capital projects
    st.subheader("Capital Improvement Projects")
    cip = query(f"""
        SELECT asset_owning_dept AS "Department", SUM(amount) / 1e6 AS "Amount ($M)"
        FROM '{_AGG}/cip_by_dept.parquet'
        WHERE fiscal_year BETWEEN {year_range[0]} AND {year_range[1]}
          AND source = 'budget'
        GROUP BY asset_owning_dept
        ORDER BY SUM(amount) DESC
        LIMIT 15
    """)
    if not cip.empty:
        st.bar_chart(cip.set_index("Department"), horizontal=True, color=CHART_COLOR)
