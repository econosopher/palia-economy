"""
Cell 10 — Activity Mix: Customers as Share of Active Players

Chart
- Customers / Active Players (monthly), using `summary` from Cell 2.
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme, percent_axis


def plot_customers_share(df: pd.DataFrame) -> None:
    ensure_theme()
    plt.figure(figsize=(12, 6))
    ax = sns.lineplot(data=df, x="month", y="customers_share", marker="o")
    plt.title("Customers Share of Active Players")
    percent_axis(ax)
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        plt.savefig("cell_10_activity_mix_customers_share.png", dpi=160)


def main() -> None:
    g = globals()
    if "summary" not in g:
        print("summary not found; run Cell 2 first.")
        return
    s = g["summary"].copy()
    cols = ["month_start", "monthly_active_customers", "monthly_active_users"]
    for c in cols:
        if c not in s.columns:
            print("Skipping activity mix: missing", c)
            return
    m = s.groupby("month_start").agg(
        mac=("monthly_active_customers", "max"),
        mau=("monthly_active_users", "max"),
    ).reset_index()
    m = m.rename(columns={"month_start": "month"})
    m["customers_share"] = (m["mac"] / m["mau"]).clip(lower=0, upper=1)
    plot_customers_share(m[["month", "customers_share"]])


if __name__ == "__main__":
    main()

