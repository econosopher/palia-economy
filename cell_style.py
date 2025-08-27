"""
Lightweight shared style helpers for charts.

Safe to import from other cell scripts or `%run` first in a notebook.
"""

from __future__ import annotations

import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter


def ensure_theme() -> None:
    """Apply a clean, modern Seaborn theme suitable for notebooks and exports."""
    sns.set_theme(style="whitegrid", context="talk")


def percent_axis(ax: plt.Axes, axis: str = "y", decimals: int = 0) -> None:
    """Format the given axis as a percentage for values in [0,1]."""
    fmt = PercentFormatter(xmax=1.0, decimals=decimals)
    if axis.lower() == "y":
        ax.yaxis.set_major_formatter(fmt)
    else:
        ax.xaxis.set_major_formatter(fmt)

