"""
Cell 7 — Conversion Charts

Charts
- Period-based first-time conversion (monthly): new customers / new users in month
- Cohort D7 conversion: fraction of cohort converting by day 7
- Monthly conversion: unique payers / unique active players

Data
- Uses `summary` from Cell 2 if present. Falls back to placeholders otherwise.
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme, percent_axis


def plot_time_series(df: pd.DataFrame, x: str, y: str, title: str, outfile: str) -> None:
    ensure_theme()
    plt.figure(figsize=(12, 6))
    ax = sns.lineplot(data=df, x=x, y=y, marker="o")
    plt.title(title)
    percent_axis(ax)
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        plt.savefig(outfile, dpi=160)


def _monthly_period_conversion_from_summary(summary: pd.DataFrame) -> pd.DataFrame:
    s = summary.copy().sort_values("month_start")
    cols = ["month_start", "monthly_new_users", "monthly_new_customers"]
    for c in cols:
        if c not in s.columns:
            raise KeyError(f"summary missing {c}")
    m = s.groupby("month_start").agg(
        monthly_new_users=("monthly_new_users", "max"),
        monthly_new_customers=("monthly_new_customers", "max"),
    ).reset_index()
    m["period_first_time_conversion"] = (m["monthly_new_customers"] / m["monthly_new_users"]).replace([float("inf"), -float("inf")], float("nan"))
    return m[["month_start", "period_first_time_conversion"]].rename(columns={"month_start": "month"})


def _monthly_conversion_from_summary(summary: pd.DataFrame) -> pd.DataFrame:
    s = summary.copy().sort_values("month_start")
    cols = ["month_start", "monthly_payers", "monthly_active_users"]
    for c in cols:
        if c not in s.columns:
            raise KeyError(f"summary missing {c}")
    m = s.groupby("month_start").agg(
        monthly_payers=("monthly_payers", "max"),
        monthly_active_users=("monthly_active_users", "max"),
    ).reset_index()
    m["monthly_conversion"] = (m["monthly_payers"] / m["monthly_active_users"]).replace([float("inf"), -float("inf")], float("nan"))
    return m[["month_start", "monthly_conversion"]].rename(columns={"month_start": "month"})


def _cohort_d7_from_summary(summary: pd.DataFrame) -> pd.DataFrame:
    s = summary.copy()
    if "cohort_size" not in s.columns or "cohort_d7_converted" not in s.columns:
        raise KeyError("summary missing cohort fields")
    s = s[s["cohort_size"] > 0].copy()
    s["cohort_month"] = pd.to_datetime(s["metric_date_dt"]).dt.to_period("M").dt.to_timestamp()
    s["cohort_d7_conversion"] = (s["cohort_d7_converted"] / s["cohort_size"]).replace([float("inf"), -float("inf")], float("nan"))
    c = s.sort_values("cohort_month").drop_duplicates("cohort_month")
    return c[["cohort_month", "cohort_d7_conversion"]].rename(columns={"cohort_month": "month"})


def main() -> None:
    g = globals()
    if "summary" not in g:
        print("summary not found; charts will not render. Run Cell 2 first.")
        return
    s = g["summary"]

    try:
        df_period = _monthly_period_conversion_from_summary(s)
        plot_time_series(df_period, x="month", y="period_first_time_conversion", title="Period First-time Conversion (New Customers / New Users)", outfile="cell_7_period_first_time_conversion.png")
    except Exception as e:
        print("Skipping period first-time conversion:", e)

    try:
        df_monthly_conv = _monthly_conversion_from_summary(s)
        plot_time_series(df_monthly_conv, x="month", y="monthly_conversion", title="Monthly Conversion (Payers / Active Players)", outfile="cell_7_monthly_conversion.png")
    except Exception as e:
        print("Skipping monthly conversion:", e)

    try:
        df_c7 = _cohort_d7_from_summary(s)
        plot_time_series(df_c7, x="month", y="cohort_d7_conversion", title="Cohort D7 Conversion", outfile="cell_7_cohort_d7_conversion.png")
    except Exception as e:
        print("Skipping cohort D7 conversion:", e)


if __name__ == "__main__":
    main()

