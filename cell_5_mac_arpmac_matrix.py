"""
Cell 5 — MAC × ARPMAC Run-rate Matrix

Purpose
- Compute a matrix of monthly MTX run-rate by combinations of MAC (Monthly Active Customers) and ARPMAC.
- Visualize as a heatmap to identify target corridors.

Data requirements
- monthly_mtx_revenue (float)
- mac (int) — monthly active customers
- Optionally, provide candidate grids for mac_values and arpmac_values.

Output
- Matplotlib/Seaborn heatmap figure.

Note: Wires to `summary` (Cell 2) if available to derive a grid around latest values.
"""

from __future__ import annotations

from typing import Iterable, Tuple

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def ensure_theme() -> None:
    sns.set_theme(style="whitegrid", context="talk", palette="crest")


def build_matrix(
    mac_values: Iterable[int],
    arpmac_values: Iterable[float],
) -> pd.DataFrame:
    data = {"MAC": list(mac_values)}
    df = pd.DataFrame(data)
    for a in arpmac_values:
        df[f"ARPMAC={a:.2f}"] = df["MAC"] * a
    return df.set_index("MAC")


def to_annual(df_monthly: pd.DataFrame) -> pd.DataFrame:
    return df_monthly * 12.0


def plot_heatmap(
    annual_matrix: pd.DataFrame,
    title: str = "MTX Run Rate (Annualized) — MAC × ARPMAC",
    fmt: str = ".0f",
    cbar: bool = True,
):
    ensure_theme()
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.heatmap(
        annual_matrix,
        annot=True,
        fmt=fmt,
        linewidths=0.5,
        linecolor="white",
        cbar=cbar,
        ax=ax,
    )
    ax.set_title(title)
    ax.set_xlabel("ARPMAC")
    ax.set_ylabel("MAC")
    plt.tight_layout()
    return fig, ax


def _derive_grid_from_summary(summary: pd.DataFrame) -> Tuple[list[int], list[float]]:
    s = summary.copy().sort_values("month_start")
    for c in ("monthly_active_customers", "monthly_revenue", "month_start"):
        if c not in s.columns:
            raise KeyError(f"summary missing {c}")
    last = s.dropna(subset=["monthly_active_customers", "monthly_revenue"]).tail(1)
    if last.empty:
        raise ValueError("summary has no monthly rows with required fields")
    mac0 = int(last["monthly_active_customers"].iloc[0])
    rev0 = float(last["monthly_revenue"].iloc[0])
    arpmac0 = rev0 / max(mac0, 1)
    mac_values = [max(1, int(mac0 * m)) for m in [0.8, 0.9, 1.0, 1.2, 1.4, 1.6]]
    arpmac_values = [max(0.01, round(arpmac0 * f, 2)) for f in [0.8, 0.9, 1.0, 1.2, 1.4, 1.6]]
    return sorted(set(mac_values)), sorted(set(arpmac_values))


def main() -> None:
    try:
        g = globals()
        if "summary" in g:
            mac_values, arpmac_values = _derive_grid_from_summary(g["summary"])
        else:
            raise KeyError("summary not found; using defaults")
    except Exception:
        mac_values = [1_000_000, 1_200_000, 1_500_000, 1_800_000, 2_160_000, 2_520_000, 2_880_000, 3_240_000, 3_600_000]
        arpmac_values = [3.00, 3.60, 4.20, 4.83, 5.43, 5.97]

    monthly_matrix = build_matrix(mac_values, arpmac_values)
    annual_matrix = to_annual(monthly_matrix)
    fig, _ = plot_heatmap(annual_matrix)
    try:
        plt.show()
    except Exception:
        fig.savefig("cell_5_mac_arpmac_matrix.png", dpi=160)


if __name__ == "__main__":
    main()

