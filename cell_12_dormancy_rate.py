"""
Cell 12 — Dormancy Rate

Share of customers purchasing in the month; complement suggests dormancy.
Metric: monthly_payers / monthly_active_customers (from `summary`).
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme, percent_axis


def plot_dormancy(df: pd.DataFrame) -> None:
    ensure_theme()
    plt.figure(figsize=(12, 6))
    ax = sns.lineplot(data=df, x="month", y="purchase_rate", marker="o")
    plt.title("Customers Purchasing in Month (Dormancy Complement)")
    percent_axis(ax)
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        plt.savefig("cell_12_dormancy_rate.png", dpi=160)


def main() -> None:
    g = globals()
    if "summary" not in g:
        print("summary not found; run Cell 2 first.")
        return
    s = g["summary"].copy()
    cols = ["month_start", "monthly_payers", "monthly_active_customers"]
    for c in cols:
        if c not in s.columns:
            print("Skipping dormancy: missing", c)
            return
    m = s.groupby("month_start").agg(
        monthly_payers=("monthly_payers", "max"),
        mac=("monthly_active_customers", "max"),
    ).reset_index().rename(columns={"month_start": "month"})
    m["purchase_rate"] = (m["monthly_payers"] / m["mac"]).clip(lower=0, upper=1)
    plot_dormancy(m[["month", "purchase_rate"]])


if __name__ == "__main__":
    main()

