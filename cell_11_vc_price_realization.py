"""
Cell 11 — VC Price Realization vs. List

Effective VC price = realized_mtx_vc_revenue / total_vc_units_consumed_at_list_price
Optionally adjust numerator by excluding discounts or including net-of-fees.
Currently optional — may be unused if VC pricing is out of scope.
"""

from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from cell_style import ensure_theme


def plot_price_realization(df: pd.DataFrame) -> None:
    ensure_theme()
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x="month", y="effective_price", marker="o", label="Effective")
    if "list_price" in df.columns:
        sns.lineplot(data=df, x="month", y="list_price", marker="o", label="List")
    plt.title("VC Price Realization vs. List")
    plt.legend()
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        plt.savefig("cell_11_vc_price_realization.png", dpi=160)


def main() -> None:
    # Placeholder; requires VC ledger mapping. Skipping by default.
    pass


if __name__ == "__main__":
    main()

