"""
Cell 6 — Cohort Conversion by Horizon (D1/D7/D30/D60/D90)

Purpose
- Plot conversion curves by cohort using horizons available from Cell 1/2.
- Serves as a proxy until explicit cohort-based retention by month-since-first-MTX is available.

Data
- Uses `summary` from Cell 2. Requires columns: `cohort_size`, `cohort_d{h}_converted`.
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme, percent_axis


def build_cohort_horizon_rates(summary: pd.DataFrame) -> pd.DataFrame:
    s = summary.copy()
    if "cohort_size" not in s.columns:
        raise KeyError("summary missing cohort_size; run Cell 1/2 with cohort rollup")
    s = s[s["cohort_size"] > 0].copy()
    s["cohort_month"] = pd.to_datetime(s["metric_date_dt"]).dt.to_period("M").dt.to_timestamp()
    horizons = [1, 7, 30, 60, 90]
    out = {"month": s["cohort_month"]}
    for h in horizons:
        conv_col = f"cohort_d{h}_converted"
        if conv_col in s.columns:
            rate = (s[conv_col] / s["cohort_size"]).replace([float("inf"), -float("inf")], float("nan"))
            out[f"d{h}_conversion"] = rate
    df = pd.DataFrame(out).drop_duplicates("month").sort_values("month")
    return df


def plot_horizon_rates(df: pd.DataFrame) -> None:
    ensure_theme()
    plt.figure(figsize=(12, 7))
    value_cols = [c for c in df.columns if c != "month"]
    for col in value_cols:
        ax = sns.lineplot(data=df, x="month", y=col, marker="o", label=col)
    plt.title("Cohort Conversion by Horizon (D1/D7/D30/D60/D90)")
    plt.xlabel("Cohort Month")
    plt.ylabel("Conversion Rate")
    percent_axis(ax)
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        plt.savefig("cell_6_cohort_conversion_horizons.png", dpi=160)


def main() -> None:
    g = globals()
    if "summary" not in g:
        print("summary not found; run Cell 2 first.")
        return
    s = g["summary"]
    try:
        df = build_cohort_horizon_rates(s)
        if len(df.columns) <= 1:
            print("No cohort horizon fields present; skipping chart.")
            return
        plot_horizon_rates(df)
    except Exception as e:
        print("Skipping cohort horizon chart:", e)


if __name__ == "__main__":
    main()

