#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Notebook Cell 2 — In-memory summary.

Use in a notebook by either:
- Pasting this whole file into Cell 2 and running it (recommended). It will
  resolve `df_daily` (or `_sqldf`/`sdf_daily`), compute `summary`, print
  shapes, and show a preview automatically.
- Or importing `process_summary_inline()` / `run_summary_cell()` and calling
  them yourself.
"""

import pandas as pd
import numpy as np
from typing import Any


def process_summary_inline(kpi_agg: pd.DataFrame) -> pd.DataFrame:
    df = kpi_agg.copy()

    # Coerce likely numeric columns
    numeric_cols = [
        "new_users","new_customers","daily_revenue","daily_transactions",
        "daily_active_users","daily_active_customers","daily_payers",
        "prior_day_active_users","d_over_d_returning_users",
        "prior_day_payers","d_over_d_returning_payers",
        "weekly_active_users","prior_week_active_users","w_over_w_returning_users",
        "weekly_active_customers","weekly_payers",
        "monthly_active_users","prior_month_active_users","m_over_m_returning_users",
        "monthly_active_customers","monthly_payers",
        "d7_regulars","d30_regulars","weekly_regulars","monthly_regulars",
        "prior_day_active_customers","d_over_d_returning_customers",
        "prior_week_active_customers","w_over_w_returning_customers",
        "prior_week_payers","w_over_w_returning_payers",
        "prior_month_active_customers","m_over_m_returning_customers",
        "prior_month_payers","m_over_m_returning_payers",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Dates and labels
    df["metric_date_dt"] = pd.to_datetime(df["metric_date"], errors="coerce") if "metric_date" in df.columns else pd.to_datetime([])
    df = df.sort_values("metric_date_dt", ascending=True)
    df["day_of_week"] = df["metric_date_dt"].dt.strftime("%a")
    df["date"] = df["metric_date_dt"].dt.strftime("%m/%d")
    df["week_start"] = df["metric_date_dt"].dt.to_period("W").dt.start_time
    df["month_start"] = df["metric_date_dt"].dt.to_period("M").dt.start_time

    # Consistent aliases
    df["daily_new_users"] = df.get("new_users", 0)
    df["daily_new_customers"] = df.get("new_customers", 0)

    # Safe division
    def _as_series(x):
        # Avoid pandas ABC isinstance checks (can recurse in some environments)
        try:
            _ = x.index  # Series-like objects have .index
            return x
        except Exception:
            return pd.Series([x] * len(df), index=df.index)

    def safe_div(a, b):
        a = _as_series(a)
        b = _as_series(b)
        a_arr = pd.to_numeric(a, errors="coerce").to_numpy(dtype="float64", copy=False)
        b_arr = pd.to_numeric(b, errors="coerce").to_numpy(dtype="float64", copy=False)
        with np.errstate(divide='ignore', invalid='ignore'):
            res = np.divide(a_arr, b_arr, out=np.full_like(a_arr, np.nan, dtype="float64"), where=b_arr != 0)
        return pd.Series(res, index=df.index)

    # Simple derivations & rates
    df["dau_minus_new"] = df.get("daily_active_users", 0) - df.get("daily_new_users", 0)
    df["daily_customer_rate"] = safe_div(df.get("daily_active_customers", 0), df.get("daily_active_users", 0))
    df["daily_user_payer_rate"] = safe_div(df.get("daily_payers", 0), df.get("daily_active_users", 0))
    df["daily_customer_payer_rate"] = safe_div(df.get("daily_payers", 0), df.get("daily_active_customers", 0))

    # New users -> new customers conversions
    df["weekly_new_users"] = df.groupby("week_start")["daily_new_users"].transform("cumsum")
    df["monthly_new_users"] = df.groupby("month_start")["daily_new_users"].transform("cumsum")
    df["weekly_new_customers"] = df.groupby("week_start")["daily_new_customers"].transform("cumsum")
    df["monthly_new_customers"] = df.groupby("month_start")["daily_new_customers"].transform("cumsum")
    df["daily_new_user_to_customer_rate"] = safe_div(df["daily_new_customers"], df["daily_new_users"])
    df["weekly_new_user_to_customer_rate"] = safe_div(df["weekly_new_customers"], df["weekly_new_users"])
    df["monthly_new_user_to_customer_rate"] = safe_div(df["monthly_new_customers"], df["monthly_new_users"])

    # Revenue metrics
    df["daily_arpu"] = safe_div(df.get("daily_revenue", 0.0), df.get("daily_active_users", 0))
    df["daily_arppu"] = safe_div(df.get("daily_revenue", 0.0), df.get("daily_payers", 0))
    df["daily_arpc"] = safe_div(df.get("daily_revenue", 0.0), df.get("daily_active_customers", 0))
    if "daily_revenue" in df.columns:
        df["weekly_revenue"] = df.groupby("week_start")["daily_revenue"].transform("cumsum")
        df["monthly_revenue"] = df.groupby("month_start")["daily_revenue"].transform("cumsum")
        # Weekly/Monthly ARPU/ARPPU/ARPC
        if "weekly_active_users" in df.columns:
            df["weekly_arpu"] = safe_div(df["weekly_revenue"], df.get("weekly_active_users", 0))
        if "weekly_payers" in df.columns:
            df["weekly_arppu"] = safe_div(df["weekly_revenue"], df.get("weekly_payers", 0))
        if "weekly_active_customers" in df.columns:
            df["weekly_arpc"] = safe_div(df["weekly_revenue"], df.get("weekly_active_customers", 0))
        if "monthly_active_users" in df.columns:
            df["monthly_arpu"] = safe_div(df["monthly_revenue"], df.get("monthly_active_users", 0))
        if "monthly_payers" in df.columns:
            df["monthly_arppu"] = safe_div(df["monthly_revenue"], df.get("monthly_payers", 0))
        if "monthly_active_customers" in df.columns:
            df["monthly_arpc"] = safe_div(df["monthly_revenue"], df.get("monthly_active_customers", 0))

    # Retention rates
    df["dod_retention"] = safe_div(df.get("d_over_d_returning_users", 0), df.get("prior_day_active_users", 0))
    df["wow_retention"] = safe_div(df.get("w_over_w_returning_users", 0), df.get("prior_week_active_users", 0))
    df["mom_retention"] = safe_div(df.get("m_over_m_returning_users", 0), df.get("prior_month_active_users", 0))
    df["dod_payer_retention"] = safe_div(df.get("d_over_d_returning_payers", 0), df.get("prior_day_payers", 0))
    df["wow_payer_retention"] = safe_div(df.get("w_over_w_returning_payers", 0), df.get("prior_week_payers", 0))
    df["mom_payer_retention"] = safe_div(df.get("m_over_m_returning_payers", 0), df.get("prior_month_payers", 0))
    df["dod_customer_retention"] = safe_div(df.get("d_over_d_returning_customers", 0), df.get("prior_day_active_customers", 0))
    df["wow_customer_retention"] = safe_div(df.get("w_over_w_returning_customers", 0), df.get("prior_week_active_customers", 0))
    df["mom_customer_retention"] = safe_div(df.get("m_over_m_returning_customers", 0), df.get("prior_month_active_customers", 0))

    # Regular shares
    if "d7_regulars" in df.columns:
        df["d7_regular_share"] = safe_div(df["d7_regulars"], df.get("daily_active_users", 0))
    if "d30_regulars" in df.columns:
        df["d30_regular_share"] = safe_div(df["d30_regulars"], df.get("daily_active_users", 0))
    # DAU shares (stickiness)
    if "weekly_active_users" in df.columns:
        df["dau_wau_share"] = safe_div(df.get("daily_active_users", 0), df["weekly_active_users"])
    if "monthly_active_users" in df.columns:
        df["dau_mau_share"] = safe_div(df.get("daily_active_users", 0), df["monthly_active_users"])

    # Cohort conversion rates and ARPU/ARPC by horizon (if present from SQL)
    if "cohort_size" in df.columns:
        for h in (1, 7, 30, 60, 90):
            conv_col = f"cohort_d{h}_converted"
            rev_col = f"cohort_d{h}_revenue"
            if conv_col in df.columns:
                df[f"cohort_d{h}_conversion_rate"] = safe_div(df[conv_col], df["cohort_size"]) 
            if rev_col in df.columns:
                df[f"cohort_d{h}_arpu"] = safe_div(df[rev_col], df["cohort_size"]) 
                if conv_col in df.columns:
                    df[f"cohort_d{h}_arpc"] = safe_div(df[rev_col], df[conv_col]) 

    return df.sort_values("metric_date_dt", ascending=False).fillna(0)


def run_summary_cell(df_daily: Any) -> pd.DataFrame:
    """Ensure pandas input, compute summary, print shapes, and display a preview.

    - Accepts a pandas DataFrame or a Spark DataFrame (with toPandas()).
    - Prints df/summary shapes for quick sanity.
    - Displays a 5-row preview of the summary.
    - Returns the summary DataFrame.
    """
    # Spark → pandas if needed
    if hasattr(df_daily, "toPandas"):
        df_daily = df_daily.toPandas()

    if not isinstance(df_daily, pd.DataFrame):
        raise TypeError("df_daily must be a pandas DataFrame or Spark DataFrame with toPandas().")

    print(f"df_daily rows: {len(df_daily)} | cols: {len(df_daily.columns)}")
    summary = process_summary_inline(df_daily)
    print(f"summary rows: {len(summary)} | cols: {len(summary.columns)}")

    # Preview with robust fallbacks (some notebook envs have custom display behavior)
    try:
        try:
            from IPython.display import display as ipy_display  # type: ignore
            ipy_display(summary.head(5))
        except Exception:
            # Non-notebook or display unavailable
            print(summary.head(5).to_string(index=False))
    except RecursionError:
        # Rarely, some environments can trigger deep recursion in display; fallback to plain text
        try:
            print(summary.head(5).to_string(index=False))
        except Exception:
            print("Preview unavailable due to environment display recursion.")

    return summary


def _resolve_df_daily() -> pd.DataFrame:
    """Resolve df_daily from common notebook patterns: df_daily, _sqldf, sdf_daily."""
    g = globals()
    if 'df_daily' in g and not hasattr(g['df_daily'], 'toPandas'):
        # Assume plain pandas if it doesn't expose Spark's toPandas()
        return g['df_daily']
    if '_sqldf' in g:
        return g['_sqldf'].toPandas()
    if 'sdf_daily' in g:
        return g['sdf_daily'].toPandas()
    raise RuntimeError(
        "No input found for Cell 2. Define one in Cell 1:\n"
        "- df_daily (pandas)\n- _sqldf (Spark from %sql)\n- sdf_daily (Spark from Python)"
    )


if __name__ == "__main__":
    # Execute Cell 2 when this file is run in a notebook cell.
    df_daily = _resolve_df_daily()
    print("Cell 2 — df_daily shape:", df_daily.shape)
    summary = process_summary_inline(df_daily)
    print("Cell 2 — summary shape:", summary.shape)
    # Publish and preview
    globals()['summary'] = summary
    # Try to publish a Spark temp view for cross-language (R) access as `summary_kpis`
    try:
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.builder.getOrCreate()
        spark.createDataFrame(summary).createOrReplaceTempView("summary_kpis")
        print("Cell 2 — published Spark temp view: summary_kpis")
    except Exception as _e:
        # Non-fatal if Spark not available
        pass
    try:
        display(summary.head(5))  # type: ignore[name-defined]
    except Exception:
        try:
            from IPython.display import display as _display  # type: ignore
            _display(summary.head(5))
        except Exception:
            print(summary.head(5).to_string(index=False))
