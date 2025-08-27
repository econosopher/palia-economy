#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Driver Notebook — minimal, intuitive runner for the KPI export.

Usage in Databricks Repos:
- Open this notebook file in the repo and click Run.
- Optionally adjust widgets (dates, catalog, schema) before running.

Behavior:
- Sets optional UC catalog/schema for table resolution.
- Sets env vars for Cell 0 (runner) and calls the orchestrator.
- Writes CSV to /dbfs/FileStore/palia/<filename> with a /files link.
"""

from __future__ import annotations
import os


def _get_dbutils():
    try:
        return dbutils  # type: ignore[name-defined]
    except Exception:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            from pyspark.sql import SparkSession  # type: ignore
            return DBUtils(SparkSession.builder.getOrCreate())
        except Exception:
            return None


def main() -> None:
    dbu = _get_dbutils()

    # Defaults can also be provided via environment variables
    default_start = os.getenv("PALIA_RUN_START", "2025-07-01")
    default_end = os.getenv("PALIA_RUN_END", "2025-07-31")
    default_catalog = os.getenv("PALIA_CATALOG", "hive_metastore")
    default_schema = os.getenv("PALIA_SCHEMA", "palia")

    # Widgets (if available)
    if dbu is not None:
        try:
            dbu.widgets.text("RUN_START", default_start, "Start Date (YYYY-MM-DD)")
            dbu.widgets.text("RUN_END", default_end, "End Date (YYYY-MM-DD)")
            dbu.widgets.text("CATALOG", default_catalog, "Catalog (optional)")
            dbu.widgets.text("SCHEMA", default_schema, "Schema/Database (optional)")
        except Exception:
            pass

    # Read values (widgets if present; else env defaults)
    def _get(name: str, env_default: str) -> str:
        try:
            if dbu is not None:
                return dbu.widgets.get(name)  # type: ignore[attr-defined]
        except Exception:
            pass
        return env_default

    start = _get("RUN_START", default_start)
    end = _get("RUN_END", default_end)
    catalog = _get("CATALOG", default_catalog)
    schema = _get("SCHEMA", default_schema)

    filename = os.getenv("PALIA_CSV_FILENAME", f"palia_daily_summary_{start}_{end}.csv")
    os.environ["PALIA_RUN_START"] = start
    os.environ["PALIA_RUN_END"] = end
    os.environ["PALIA_CSV_FILENAME"] = filename

    print(f"Window: {start}..{end}")
    print(f"CSV filename: {filename}")

    # Optionally set catalog/schema for table resolution
    try:
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.builder.getOrCreate()
        if catalog:
            try:
                spark.sql(f"USE CATALOG `{catalog}`")
                print(f"Using catalog: {catalog}")
            except Exception as e:
                print(f"Catalog '{catalog}' not set ({e})")
        if schema:
            try:
                spark.sql(f"USE `{schema}`")
                print(f"Using schema: {schema}")
            except Exception as e:
                print(f"Schema '{schema}' not set ({e})")
    except Exception:
        pass

    # Run the orchestrator from Cell 0
    try:
        import cell_0_config_runner as c0
    except Exception as e:
        print("Could not import cell_0_config_runner. Ensure repo is synced.", e)
        return

    try:
        c0._orchestrate_pipeline(start, end, filename)
    except Exception as e:
        print("Pipeline run failed:", e)


if __name__ == "__main__":
    main()

