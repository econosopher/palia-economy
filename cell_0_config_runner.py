#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Notebook Cell 0 — Config + Runner

Paste this whole file into the first cell of your Databricks notebook.
It sets the analysis window + CSV filename and, if later cells are already
present in the notebook or importable as modules, it will orchestrate:
  1) Cell 1 (SQL) → df_daily/sdf_daily
  2) Cell 2 (summary) → summary
  3) Cell 4 (export) → CSV (local + DBFS)

You can also set dates via env vars before running:
  PALIA_RUN_START=YYYY-MM-DD
  PALIA_RUN_END=YYYY-MM-DD
  PALIA_CSV_FILENAME=your_file.csv

Nothing in this cell reaches out to external services; it just wires up
functions that are either already pasted in later cells or importable from
local files in this repo.
"""

from __future__ import annotations
import os
from typing import Any, Optional


# -----------------------------------------------------------------------------
# Configuration (edit these, or set env PALIA_RUN_START/PALIA_RUN_END)
# -----------------------------------------------------------------------------
RUN_START: str = os.getenv("PALIA_RUN_START", "2025-07-01")  # <- edit as needed
RUN_END: str = os.getenv("PALIA_RUN_END", "2025-07-31")      # <- edit as needed

# Filename for Cell 4 export; default uses the RUN_START/RUN_END window
FILENAME: str = os.getenv(
    "PALIA_CSV_FILENAME",
    f"palia_daily_summary_{RUN_START}_{RUN_END}.csv",
)

# Make filename available to Cell 4 via env
os.environ["PALIA_CSV_FILENAME"] = FILENAME

print(f"Configured window: {RUN_START}..{RUN_END}")
print(f"CSV filename: {FILENAME}")


def _resolve_in_globals(*names: str) -> Optional[Any]:
    g = globals()
    for n in names:
        if n in g:
            return g[n]
    return None


def _maybe_import(module: str, *names: str) -> dict:
    """Try importing specific names from a module. Missing imports are non-fatal."""
    out: dict = {}
    try:
        mod = __import__(module, fromlist=list(names))
        for n in names:
            if hasattr(mod, n):
                out[n] = getattr(mod, n)
    except Exception:
        pass
    return out


def _orchestrate_pipeline(start: str, end: str, filename: str) -> None:
    # ------------------------------------------------------------
    # Cell 1 — Run KPI SQL → df_daily/sdf_daily
    # ------------------------------------------------------------
    run_cell_1 = _resolve_in_globals("run_cell_1")
    get_daily_kpis = _resolve_in_globals("get_daily_kpis")
    if not run_cell_1 and not get_daily_kpis:
        fns = _maybe_import("cell_1_monthly_kpis", "run_cell_1", "get_daily_kpis")
        run_cell_1 = run_cell_1 or fns.get("run_cell_1")
        get_daily_kpis = get_daily_kpis or fns.get("get_daily_kpis")

    df_daily = _resolve_in_globals("df_daily")
    if not df_daily:
        try:
            if callable(run_cell_1):
                df_daily = run_cell_1(start, end)
            elif callable(get_daily_kpis):
                sdf_daily = get_daily_kpis(start, end)
                try:
                    df_daily = sdf_daily.toPandas()
                except Exception:
                    df_daily = None
                globals().update({"sdf_daily": sdf_daily, "df_daily": df_daily})
            else:
                print("Note: paste/run Cell 1 (SQL) to produce df_daily.")
        except Exception as e:
            print("Cell 1 runner error:", e)
            df_daily = None

    # ------------------------------------------------------------
    # Cell 2 — Build summary → summary
    # ------------------------------------------------------------
    run_summary_cell = _resolve_in_globals("run_summary_cell")
    process_summary_inline = _resolve_in_globals("process_summary_inline")
    if not run_summary_cell and not process_summary_inline:
        fns = _maybe_import("cell_2_summary", "run_summary_cell", "process_summary_inline")
        run_summary_cell = run_summary_cell or fns.get("run_summary_cell")
        process_summary_inline = process_summary_inline or fns.get("process_summary_inline")

    summary = _resolve_in_globals("summary")
    if not summary and df_daily is not None:
        try:
            if callable(run_summary_cell):
                summary = run_summary_cell(df_daily)
            elif callable(process_summary_inline):
                summary = process_summary_inline(df_daily)
            else:
                print("Note: paste/run Cell 2 to build summary if not present.")
            if summary is not None:
                globals()["summary"] = summary
        except Exception as e:
            print("Cell 2 runner error:", e)
            summary = None

    # ------------------------------------------------------------
    # Cell 4 — Export CSV
    # ------------------------------------------------------------
    write_csv = _resolve_in_globals("_write_csv_tableau")
    if not write_csv:
        fns = _maybe_import("cell_4_export_csv", "_write_csv_tableau")
        write_csv = write_csv or fns.get("_write_csv_tableau")

    if callable(write_csv) and summary is not None:
        try:
            paths = write_csv(summary, filename=filename)
            print(paths)
        except Exception as e:
            print("Cell 4 export error:", e)
    else:
        print("Note: paste/run Cell 4 to export CSV when ready.")


if __name__ == "__main__":
    try:
        _orchestrate_pipeline(RUN_START, RUN_END, FILENAME)
    except Exception as e:
        print("Runner encountered an issue:", e)

