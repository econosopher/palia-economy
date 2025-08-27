#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Notebook Cell 4 — Export the prepared summary DataFrame to a Tableau‑friendly CSV.

Behavior:
- Expects `summary` from Cell 2 to be present in the notebook globals.
- Normalizes date/time columns to ISO strings.
- Writes a single CSV (no partitioning) suitable for Tableau ingest.
- Always writes to the current working directory (./<filename>).
- Also tries to write to DBFS (/dbfs/FileStore/palia/<filename>) and shows a notebook link if available.

Usage:
    # Run after Cell 2/3
    # Paste this entire file into Cell 4 and run it.
    # Optional: set FILENAME via env PALIA_CSV_FILENAME

Outputs:
- Local: ./palia_daily_summary.csv (always)
- DBFS (if available): /dbfs/FileStore/palia/palia_daily_summary.csv with /files link
"""

from __future__ import annotations
import os
import sys
from typing import Iterable
import numpy as np
import pandas as pd


def _normalize_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Ensure expected first column is a clean date string
    if "metric_date_dt" in out.columns:
        out["metric_date_dt"] = pd.to_datetime(out["metric_date_dt"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "metric_date" in out.columns:
        out["metric_date"] = pd.to_datetime(out["metric_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Normalize period-derived timestamps
    for c in ("week_start", "month_start"):
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d")

    # Keep numeric columns numeric (Tableau will infer types)
    # Avoid object dtypes with mixed types
    for c in out.columns:
        if out[c].dtype == object:
            # Try numeric, else leave as string
            tmp = pd.to_numeric(out[c], errors="ignore")
            out[c] = tmp

    # Option A: export currency as integer cents to preserve precision
    money_cols = [c for c in out.columns if any(k in c.lower() for k in ("revenue", "arpu"))]
    for c in money_cols:
        try:
            vals = pd.to_numeric(out[c], errors="coerce")
            cents = np.rint(vals * 100.0).astype("Int64")  # nullable integer cents
            out[c + "_cents"] = cents
        except Exception:
            # If conversion fails, skip silently
            pass
    # Drop the floating dollar columns; export cents only for monetary fields
    out = out.drop(columns=[c for c in money_cols if c in out.columns])
    return out


def _rename_and_order_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def camelize(name: str) -> str:
        parts = name.split('_')
        return parts[0] + ''.join(p.capitalize() for p in parts[1:])

    rename = {}

    # Exceptions for active users
    if 'daily_active_users' in out.columns: rename['daily_active_users'] = 'DAU'
    if 'weekly_active_users' in out.columns: rename['weekly_active_users'] = 'WAU'
    if 'monthly_active_users' in out.columns: rename['monthly_active_users'] = 'MAU'

    # Map D/W/M prefixes for common bases
    bases = (
        'arpu','arppu','arpc','revenue','payers','active_customers','new_users','new_customers','new_user_to_customer_rate',
        'regulars'
    )
    for base in bases:
        # daily_*
        src = f'daily_{base}'
        if src in out.columns: rename[src] = 'd' + (base.upper() if base.startswith('arp') else camelize(base))
        if src + '_cents' in out.columns: rename[src + '_cents'] = 'd' + (base.upper() if base.startswith('arp') else camelize(base)) + 'Cents'
        # weekly_*
        src = f'weekly_{base}'
        if src in out.columns: rename[src] = 'w' + (base.upper() if base.startswith('arp') else camelize(base))
        if src + '_cents' in out.columns: rename[src + '_cents'] = 'w' + (base.upper() if base.startswith('arp') else camelize(base)) + 'Cents'
        # monthly_*
        src = f'monthly_{base}'
        if src in out.columns: rename[src] = 'm' + (base.upper() if base.startswith('arp') else camelize(base))
        if src + '_cents' in out.columns: rename[src + '_cents'] = 'm' + (base.upper() if base.startswith('arp') else camelize(base)) + 'Cents'

    # Regulars and shares
    if 'd7_regulars' in out.columns: rename['d7_regulars'] = 'D7Regulars'
    if 'd30_regulars' in out.columns: rename['d30_regulars'] = 'D30Regulars'
    if 'weekly_regulars' in out.columns: rename['weekly_regulars'] = 'wRegulars'
    if 'monthly_regulars' in out.columns: rename['monthly_regulars'] = 'mRegulars'
    if 'd7_regular_share' in out.columns: rename['d7_regular_share'] = 'D7RegularShare'
    if 'd30_regular_share' in out.columns: rename['d30_regular_share'] = 'D30RegularShare'
    if 'dau_wau_share' in out.columns: rename['dau_wau_share'] = 'DAU_WAU_Share'
    if 'dau_mau_share' in out.columns: rename['dau_mau_share'] = 'DAU_MAU_Share'

    # Retention renames
    mapping_ret = {
        'dod_retention':'dRetention','wow_retention':'wRetention','mom_retention':'mRetention',
        'dod_payer_retention':'dPayerRetention','wow_payer_retention':'wPayerRetention','mom_payer_retention':'mPayerRetention',
        'dod_customer_retention':'dCustomerRetention','wow_customer_retention':'wCustomerRetention','mom_customer_retention':'mCustomerRetention',
    }
    for k,v in mapping_ret.items():
        if k in out.columns: rename[k]=v

    # Apply rename except for cohort_* columns
    for col in list(out.columns):
        if col.startswith('cohort_'): continue
        if col in rename:
            out.rename(columns={col: rename[col]}, inplace=True)

    # Order columns by time grain: Dates, D*, W*, M*, cohort_*, then remainder
    cols = list(out.columns)
    date_keys = [c for c in ['metric_date','day_of_week','date','week_start','month_start'] if c in cols]
    d_cols = [c for c in cols if c.startswith('d') or c in ('DAU','D7Regulars','D30Regulars','D7RegularShare','D30RegularShare','DAU_WAU_Share','DAU_MAU_Share')]
    w_cols = [c for c in cols if (c.startswith('w') or c=='WAU') and c not in d_cols]
    m_cols = [c for c in cols if (c.startswith('m') or c=='MAU') and c not in d_cols and c not in w_cols]
    cohort_cols = [c for c in cols if c.startswith('cohort_')]
    remaining = [c for c in cols if c not in set(date_keys + d_cols + w_cols + m_cols + cohort_cols)]
    ordered = date_keys + d_cols + w_cols + m_cols + cohort_cols + remaining
    return out[ordered]


def _write_csv_tableau(summary: pd.DataFrame, filename: str = "palia_daily_summary.csv") -> dict:
    df = _normalize_for_csv(summary)
    df = _rename_and_order_for_csv(df)
    results = {}

    # Always write to local working directory
    local_path = os.path.join(os.getcwd(), filename)
    df.to_csv(local_path, index=False)
    results["local"] = local_path
    print(f"Wrote CSV (local): {local_path}")

    # Try DBFS for Databricks download convenience
    dbfs_root = "/dbfs/FileStore/palia"
    try:
        if os.path.isdir("/dbfs"):
            os.makedirs(dbfs_root, exist_ok=True)
            dbfs_path = os.path.join(dbfs_root, filename)
            df.to_csv(dbfs_path, index=False)
            results["dbfs"] = dbfs_path
            print(f"Wrote CSV (DBFS): {dbfs_path}")
            try:
                from IPython.display import display, HTML  # type: ignore
                link = f"/files/palia/{filename}"
                display(HTML(f'<a href="{link}" target="_blank">Download CSV</a>'))
            except Exception:
                print("Download (Databricks): /files/palia/" + filename)
    except Exception as e:
        print(f"DBFS write not available ({e}).")

    return results


if __name__ == "__main__":
    g = globals()
    if "summary" not in g:
        raise NameError("`summary` not found. Run Cell 2 to compute it before exporting.")
    FILENAME = os.getenv("PALIA_CSV_FILENAME", "palia_daily_summary.csv")
    paths = _write_csv_tableau(g["summary"], filename=FILENAME)
    # Emit paths as last expression for easy visibility
    print(paths)
