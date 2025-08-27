"""
Cell 6 — Period Retention (DoD, WoW, MoM)

Scope
- Customer retention: prior active customers returning (DoD/WoW/MoM)
- Payer→Payer retention: prior payers returning as payers (DoD/WoW/MoM)

Notes
- Uses precomputed rates from Cell 2 `summary` to avoid SQL changes.
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme, percent_axis


def plot_series(df: pd.DataFrame, x: str, y: str, title: str, outfile: str) -> None:
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
    s = g["summary"].copy().sort_values("metric_date_dt")
    s["date"] = pd.to_datetime(s["metric_date_dt"]).dt.date

    # Customers — DoD, WoW, MoM
    cust_cols = [
        ("dod_customer_retention", "Customer Retention — Day over Day", "cell_6_cust_dod_retention.png"),
        ("wow_customer_retention", "Customer Retention — Week over Week", "cell_6_cust_wow_retention.png"),
        ("mom_customer_retention", "Customer Retention — Month over Month", "cell_6_cust_mom_retention.png"),
    ]
    for col, title, outfile in cust_cols:
        if col in s.columns:
            plot_series(s, x="date", y=col, title=title, outfile=outfile)
        else:
            print("Skipping customers retention:", col, "not found.")

    # Payers — DoD, WoW, MoM (payer→payer)
    payer_cols = [
        ("dod_payer_retention", "Payer Retention — Day over Day", "cell_6_payer_dod_retention.png"),
        ("wow_payer_retention", "Payer Retention — Week over Week", "cell_6_payer_wow_retention.png"),
        ("mom_payer_retention", "Payer Retention — Month over Month", "cell_6_payer_mom_retention.png"),
    ]
    for col, title, outfile in payer_cols:
        if col in s.columns:
            plot_series(s, x="date", y=col, title=title, outfile=outfile)
        else:
            print("Skipping payer retention:", col, "not found.")


if __name__ == "__main__":
    main()

