"""
Cell 8 — MTX vs Entitlement Mix (Monthly and LTD)

Data
- Prefers `summary` from Cell 2. Looks for columns to compute shares:
  - Monthly: `monthly_total_revenue` and `monthly_mtx_revenue`; or `monthly_revenue` (MTX) plus entitlement fields if present.
  - LTD (optional): `ltd_total_revenue` and `ltd_mtx_revenue`.

Outputs
- Line charts for monthly share and LTD share (if fields are available).
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme, percent_axis


def compute_share(numer: pd.Series, denom: pd.Series) -> pd.Series:
    return (numer / denom).clip(lower=0, upper=1)


def plot_share(df: pd.DataFrame, x: str, y: str, title: str, outfile: str) -> None:
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


def main() -> None:
    g = globals()
    if "summary" not in g:
        print("summary not found; run Cell 2 first.")
        return
    s = g["summary"].copy().sort_values("month_start")

    total_col, mtx_col = None, None
    if {"monthly_total_revenue", "monthly_mtx_revenue"}.issubset(s.columns):
        total_col, mtx_col = "monthly_total_revenue", "monthly_mtx_revenue"
    elif "monthly_revenue" in s.columns and "monthly_entitlement_revenue" in s.columns:
        s["monthly_total_revenue"] = s["monthly_revenue"] + s["monthly_entitlement_revenue"]
        total_col, mtx_col = "monthly_total_revenue", "monthly_revenue"
    else:
        print("Skipping monthly mix: total revenue fields not found.")

    if total_col and mtx_col:
        m = s.groupby("month_start").agg(
            total=(total_col, "max"),
            mtx=(mtx_col, "max"),
        ).reset_index()
        m["mtx_share"] = compute_share(m["mtx"], m["total"]) 
        plot_share(m.rename(columns={"month_start": "month"}), x="month", y="mtx_share", title="Monthly MTX Share of Total Revenue", outfile="cell_8_monthly_mtx_share.png")

    if {"ltd_total_revenue", "ltd_mtx_revenue"}.issubset(s.columns):
        l = s.sort_values("metric_date_dt").tail(1)[["ltd_total_revenue", "ltd_mtx_revenue"]]
        share = compute_share(l["ltd_mtx_revenue"], l["ltd_total_revenue"]).iloc[0]
        print(f"LTD MTX share: {share:.2%}")


if __name__ == "__main__":
    main()

