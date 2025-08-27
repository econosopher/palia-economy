#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Notebook Cell 3 — Inline visual table from a prepared summary DataFrame.

Use in a notebook by either:
- Pasting this whole file into Cell 3 and running it (recommended). It will
  render a compact table from `summary` automatically.
- Or importing `display_summary_inline()` and calling it yourself.

Includes a light theme override and optional heatmap highlighting so tables
remain readable in dark-themed notebooks.
"""

from typing import Optional, Sequence
import pandas as pd


def display_summary_inline(
    summary: pd.DataFrame,
    columns: Optional[Sequence[str]] = None,
    *,
    light_theme: bool = True,
    heatmap: bool = True,
) -> None:
    from IPython.display import display

    default_cols = [
        "day_of_week", "date",
        # installs
        "daily_new_users", "weekly_new_users", "monthly_new_users",
        # users
        "daily_active_users", "d7_regulars", "d30_regulars", "dau_minus_new",
        "weekly_active_users", "monthly_active_users",
        # customers
        "daily_new_customers", "weekly_new_customers", "monthly_new_customers",
        "daily_new_user_to_customer_rate", "weekly_new_user_to_customer_rate", "monthly_new_user_to_customer_rate",
        "daily_active_customers", "weekly_active_customers", "monthly_active_customers",
        # payers
        "daily_payers", "weekly_payers", "monthly_payers",
        # revenue
        "daily_revenue", "weekly_revenue", "monthly_revenue",
        "daily_arpu", "daily_arppu", "daily_arpc",
    ]

    cols = [c for c in (columns or default_cols) if c in summary.columns]
    view = summary.head(30)[cols].copy()

    fmt = {c: "{:,.0f}" for c in cols if c not in ("day_of_week","date") and not c.endswith("_rate") and "arpu" not in c and "revenue" not in c}
    fmt.update({c: "${:,.0f}" for c in cols if c in ("daily_revenue","weekly_revenue","monthly_revenue")})
    fmt.update({c: "${:,.2f}" for c in cols if c in ("daily_arpu","daily_arppu","daily_arpc")})
    fmt.update({c: "{:.1%}" for c in cols if c.endswith("_rate")})

    # Build styler and hide index using forward-compatible API
    styler = view.style.format(fmt)
    try:
        # pandas >= 1.4
        styler = styler.hide(axis="index")
    except Exception:
        # Back-compat
        try:
            styler = styler.hide_index()
        except Exception:
            pass

    # Light theme override to avoid unreadable dark backgrounds
    if light_theme:
        styler = styler.set_table_styles([
            {"selector": "table", "props": [("background-color", "#ffffff"), ("color", "#111827"), ("border-collapse","collapse")]},
            {"selector": "thead th", "props": [("background-color", "#f3f4f6"), ("color", "#111827"), ("font-weight","bold"), ("border","1px solid #e5e7eb")]},
            {"selector": "tbody td", "props": [("border","1px solid #e5e7eb")]},
        ]).set_properties(**{"background-color": "#ffffff", "color": "#111827"})

    # Optional heatmap highlighting
    if heatmap:
        heat_cols = [c for c in ("daily_revenue","daily_arpu","daily_arppu","daily_user_payer_rate","daily_customer_payer_rate") if c in view.columns]
        for hc in heat_cols:
            # Use perceptually uniform colormaps where possible
            cmap = "Greens" if "revenue" in hc else ("Blues" if "arpp" in hc else "Oranges")
            try:
                styler = styler.background_gradient(subset=[hc], cmap=cmap)
            except Exception:
                pass

    display(styler)


if __name__ == "__main__":
    g = globals()
    if 'summary' not in g:
        raise NameError("summary not found; run Cell 2 first to define `summary`.")
    try:
        display  # type: ignore[name-defined]
        display_summary_inline(g['summary'])
    except Exception:
        # Fallback: print a limited plain-text view
        present = [
            c for c in (
                "day_of_week","date","daily_active_users","daily_payers",
                "daily_revenue","daily_arpu","daily_arppu"
            ) if c in g['summary'].columns
        ]
        print(g['summary'].head(10)[present].to_string(index=False))
