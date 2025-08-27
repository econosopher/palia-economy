"""
Cell 9 — LTV Charts

Charts (if data is available)
- Entitlement LTV = ltd_entitlement_revenue / ltd_unique_new_players
- MTX Payer LTV = ltd_mtx_revenue / ltd_unique_mtx_purchasers
- Player MTX LTV = ltd_mtx_revenue / ltd_unique_players

Note: If LTD fields are not available in `summary`, this cell will skip.
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme


def plot_ltvs(df: pd.DataFrame) -> None:
    ensure_theme()
    plt.figure(figsize=(12, 6))
    for col in [c for c in df.columns if c != "month"]:
        sns.lineplot(data=df, x="month", y=col, marker="o", label=col)
    plt.title("LTV Metrics Over Time")
    plt.legend()
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        plt.savefig("cell_9_ltv_charts.png", dpi=160)


def main() -> None:
    g = globals()
    if "summary" not in g:
        print("summary not found; run Cell 2 first.")
        return
    s = g["summary"].copy().sort_values("month_start")

    needed_any = [
        {"ltd_mtx_revenue", "ltd_unique_mtx_purchasers"},
        {"ltd_mtx_revenue", "ltd_unique_players"},
        {"ltd_entitlement_revenue", "ltd_unique_new_players"},
    ]
    if not any(cols.issubset(s.columns) for cols in needed_any):
        print("Skipping LTV charts: LTD fields not found in summary.")
        return

    df = pd.DataFrame({"month": s["month_start"].dropna().unique()})
    df = df.sort_values("month")

    if {"ltd_mtx_revenue", "ltd_unique_mtx_purchasers"}.issubset(s.columns):
        last = s.dropna(subset=["ltd_mtx_revenue", "ltd_unique_mtx_purchasers"]).drop_duplicates("month_start", keep="last")
        df = df.merge(
            last[["month_start", "ltd_mtx_revenue", "ltd_unique_mtx_purchasers"]].rename(columns={"month_start": "month"}),
            on="month", how="left"
        )
        df["ltv_mtx_payer"] = (df["ltd_mtx_revenue"] / df["ltd_unique_mtx_purchasers"]) 

    if {"ltd_mtx_revenue", "ltd_unique_players"}.issubset(s.columns):
        last = s.dropna(subset=["ltd_mtx_revenue", "ltd_unique_players"]).drop_duplicates("month_start", keep="last")
        df = df.merge(
            last[["month_start", "ltd_mtx_revenue", "ltd_unique_players"]].rename(columns={"month_start": "month", "ltd_mtx_revenue": "ltd_mtx_revenue_players"}),
            on="month", how="left"
        )
        df["ltv_player_mtx"] = (df["ltd_mtx_revenue_players"] / df["ltd_unique_players"]) 

    if {"ltd_entitlement_revenue", "ltd_unique_new_players"}.issubset(s.columns):
        last = s.dropna(subset=["ltd_entitlement_revenue", "ltd_unique_new_players"]).drop_duplicates("month_start", keep="last")
        df = df.merge(
            last[["month_start", "ltd_entitlement_revenue", "ltd_unique_new_players"]].rename(columns={"month_start": "month"}),
            on="month", how="left"
        )
        df["ltv_entitlement"] = (df["ltd_entitlement_revenue"] / df["ltd_unique_new_players"]) 

    value_cols = [c for c in df.columns if c != "month"]
    keep = [c for c in value_cols if df[c].notna().any()]
    if not keep:
        print("No LTV series with data; skipping chart.")
        return
    plot_ltvs(df[["month"] + keep])


if __name__ == "__main__":
    main()

