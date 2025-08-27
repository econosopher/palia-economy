#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Notebook Cell 1 — Python cell that runs the embedded SQL using the built-in
Spark session (Databricks), with default July 2025 params. No external
connector module is required.

Use in a notebook by either:
- Pasting this whole file into Cell 1 and running it (recommended). It will
  set `sdf_daily` (Spark) and `df_daily` (pandas), print shapes, and show a
  preview automatically.
- Or importing `get_daily_kpis()` and calling it yourself.
"""

import pandas as pd
from datetime import date

# Default window: Open Beta start → today
START = "2023-08-10"
END   = date.today().strftime("%Y-%m-%d")

# Embedded SQL template (parameterized with {{START_DATE}} / {{END_DATE}})
SQL_TMPL = r"""
-- Parameterized Monthly Daily KPIs (static credits->USD)
-- Replace {{START_DATE}} and {{END_DATE}} via the runner.

WITH players AS (
    -- Bedrock set: only real players
    SELECT account_id
    FROM palia.player_engagement
    WHERE COALESCE(is_player, true)
),
sessions AS (
    -- Minimal activity spine: distinct account-day presence for players only
    SELECT DISTINCT
        DATE(s.session_start) AS metric_date,
        s.account_id
    FROM palia.player_sessions s
    JOIN players p ON p.account_id = s.account_id
    WHERE DATE(s.session_start) BETWEEN DATE('{{START_DATE}}') AND DATE('{{END_DATE}}')
),
payments AS (
    SELECT
        DATE(FROM_UNIXTIME(event_timestamp/1000)) AS metric_date,
        pmt.account_id,
        SUM(COALESCE(pc.dollar_amount, 0)) AS revenue,
        COUNT(*) AS transactions
    FROM platform_raw.paymentservice_checkout_v1 pmt
    JOIN players pl ON pl.account_id = pmt.account_id
    LEFT JOIN static.premium_credits_to_dollars pc
      ON pc.premium_credit = CAST(COALESCE(pmt.premium_credit,0) AS BIGINT)
    WHERE event_date BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
      AND request_state IN ('SUCCESS','succeeded')
      AND EXISTS(headers, h -> h.key = 'producer_type' AND h.value = 'Player')
      AND NOT EXISTS(headers, h -> h.key = 'producer_env' AND LOWER(h.value) IN ('dev','staging','test'))
      AND (provider IS NULL OR LOWER(provider) NOT IN ('dev','test','staging'))
    GROUP BY 1, 2
),
first_login AS (
    -- Use pre-aggregated engagement table (lifetime first play), restricted to players
    SELECT pe.account_id, pe.firstplay_date AS first_login_dt
    FROM palia.player_engagement pe
    JOIN players p ON p.account_id = pe.account_id
),
first_purchase AS (
    -- Prefer lifetime table for first purchase (pre-aggregated) to avoid full-history scans
    SELECT lr.account_id, lr.first_spend_dt AS first_purchase_date
    FROM palia.lifetime_revenue lr
    JOIN players p ON p.account_id = lr.account_id
),
spine AS (
    SELECT metric_date, account_id FROM sessions
    UNION
    SELECT metric_date, account_id FROM payments
),
daily_state AS (
    SELECT
        s.metric_date,
        s.account_id,
        fl.first_login_dt,
        fp.first_purchase_date,
        COALESCE(ps.revenue, 0) AS daily_revenue,
        COALESCE(ps.transactions, 0) AS daily_transactions,
        (se.account_id IS NOT NULL) AS is_active_today,
        (COALESCE(ps.revenue, 0) > 0) AS paid_today,
        ((se.account_id IS NOT NULL) AND fp.first_purchase_date IS NOT NULL AND fp.first_purchase_date <= s.metric_date) AS is_active_customer_today
    FROM spine s
    LEFT JOIN sessions se ON s.metric_date = se.metric_date AND s.account_id = se.account_id
    LEFT JOIN payments ps ON s.metric_date = ps.metric_date AND s.account_id = ps.account_id
    LEFT JOIN first_login fl ON s.account_id = fl.account_id
    LEFT JOIN first_purchase fp ON s.account_id = fp.account_id
),
weekly_aggregates AS (
    SELECT
        account_id,
        DATE_TRUNC('WEEK', metric_date) AS week_start,
        MAX(CASE WHEN is_active_today THEN 1 ELSE 0 END) AS was_active_in_week,
        MAX(CASE WHEN paid_today THEN 1 ELSE 0 END) AS paid_in_week,
        MAX(CASE WHEN is_active_customer_today THEN 1 ELSE 0 END) AS was_active_customer_in_week
    FROM daily_state
    GROUP BY 1, 2
),
monthly_aggregates AS (
    SELECT
        account_id,
        DATE_TRUNC('MONTH', metric_date) AS month_start,
        MAX(CASE WHEN is_active_today THEN 1 ELSE 0 END) AS was_active_in_month,
        MAX(CASE WHEN paid_today THEN 1 ELSE 0 END) AS paid_in_month,
        MAX(CASE WHEN is_active_customer_today THEN 1 ELSE 0 END) AS was_active_customer_in_month
    FROM daily_state
    GROUP BY 1, 2
),
state_with_windows AS (
    SELECT
        d.*,
        LAG(CASE WHEN is_active_today THEN 1 ELSE 0 END, 1, 0) OVER (PARTITION BY account_id ORDER BY metric_date) AS was_active_yesterday,
        LAG(CASE WHEN paid_today THEN 1 ELSE 0 END, 1, 0) OVER (PARTITION BY account_id ORDER BY metric_date) AS paid_yesterday,
        LAG(CASE WHEN is_active_customer_today THEN 1 ELSE 0 END, 1, 0) OVER (PARTITION BY account_id ORDER BY metric_date) AS was_active_customer_yesterday,
        DATE_TRUNC('WEEK', metric_date) AS week_start,
        DATE_TRUNC('MONTH', metric_date) AS month_start,
        MAX(CASE WHEN is_active_today THEN 1 ELSE 0 END) OVER (
            PARTITION BY account_id, DATE_TRUNC('WEEK', metric_date)
            ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS is_active_this_week,
        MAX(CASE WHEN paid_today THEN 1 ELSE 0 END) OVER (
            PARTITION BY account_id, DATE_TRUNC('WEEK', metric_date)
            ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS paid_this_week,
        MAX(CASE WHEN is_active_customer_today THEN 1 ELSE 0 END) OVER (
            PARTITION BY account_id, DATE_TRUNC('WEEK', metric_date)
            ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS is_active_customer_this_week,
        MAX(CASE WHEN is_active_today THEN 1 ELSE 0 END) OVER (
            PARTITION BY account_id, DATE_TRUNC('MONTH', metric_date)
            ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS is_active_this_month,
        MAX(CASE WHEN paid_today THEN 1 ELSE 0 END) OVER (
            PARTITION BY account_id, DATE_TRUNC('MONTH', metric_date)
            ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS paid_this_month,
        MAX(CASE WHEN is_active_customer_today THEN 1 ELSE 0 END) OVER (
            PARTITION BY account_id, DATE_TRUNC('MONTH', metric_date)
            ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS is_active_customer_this_month,
        -- First appearance within the week/month for carry-forward counts
        MIN(CASE WHEN is_active_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('WEEK', metric_date)
        ) AS first_user_active_in_week,
        MIN(CASE WHEN is_active_customer_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('WEEK', metric_date)
        ) AS first_cust_active_in_week,
        MIN(CASE WHEN paid_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('WEEK', metric_date)
        ) AS first_paid_in_week,
        MIN(CASE WHEN is_active_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('MONTH', metric_date)
        ) AS first_user_active_in_month,
        MIN(CASE WHEN is_active_customer_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('MONTH', metric_date)
        ) AS first_cust_active_in_month,
        MIN(CASE WHEN paid_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('MONTH', metric_date)
        ) AS first_paid_in_month,
        -- Rolling windows for regulars
        COUNT(CASE WHEN is_active_today THEN metric_date END) OVER (
            PARTITION BY account_id ORDER BY metric_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS active_days_last_7,
        COUNT(CASE WHEN is_active_today THEN metric_date END) OVER (
            PARTITION BY account_id ORDER BY metric_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS active_days_last_30,
        -- Full period active days for weekly/monthly regulars
        COUNT(CASE WHEN is_active_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('WEEK', metric_date)
        ) AS active_days_in_week,
        COUNT(CASE WHEN is_active_today THEN metric_date END) OVER (
            PARTITION BY account_id, DATE_TRUNC('MONTH', metric_date)
        ) AS active_days_in_month,
        DATEDIFF(ADD_MONTHS(DATE_TRUNC('MONTH', metric_date), 1), DATE_TRUNC('MONTH', metric_date)) AS days_in_month
    FROM daily_state d
),
state_with_prior AS (
    SELECT
        s.*,
        COALESCE(w_last.was_active_in_week, 0) AS was_active_last_week,
        COALESCE(w_last.paid_in_week, 0) AS paid_last_week,
        COALESCE(w_last.was_active_customer_in_week, 0) AS was_active_customer_last_week,
        COALESCE(m_last.was_active_in_month, 0) AS was_active_last_month,
        COALESCE(m_last.paid_in_month, 0) AS paid_last_month,
        COALESCE(m_last.was_active_customer_in_month, 0) AS was_active_customer_last_month
    FROM state_with_windows s
    LEFT JOIN weekly_aggregates w_last
        ON s.account_id = w_last.account_id
       AND DATE_TRUNC('WEEK', s.metric_date) = date_add(w_last.week_start, 7)
    LEFT JOIN monthly_aggregates m_last
        ON s.account_id = m_last.account_id
       AND DATE_TRUNC('MONTH', s.metric_date) = add_months(m_last.month_start, 1)
)
, days AS (
    SELECT DISTINCT metric_date, week_start, month_start
    FROM state_with_windows
)
, new_users_counts AS (
    SELECT first_login_dt AS metric_date,
           APPROX_COUNT_DISTINCT(account_id) AS new_users
    FROM first_login
    WHERE first_login_dt BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
    GROUP BY first_login_dt
)
, user_week_incr AS (
    SELECT metric_date, week_start,
           APPROX_COUNT_DISTINCT(account_id) AS wau_incr
    FROM state_with_windows
    WHERE metric_date = first_user_active_in_week
    GROUP BY metric_date, week_start
)
, user_month_incr AS (
    SELECT metric_date, month_start,
           APPROX_COUNT_DISTINCT(account_id) AS mau_incr
    FROM state_with_windows
    WHERE metric_date = first_user_active_in_month
    GROUP BY metric_date, month_start
)
, cust_week_incr AS (
    SELECT metric_date, week_start,
           APPROX_COUNT_DISTINCT(account_id) AS wac_incr
    FROM state_with_windows
    WHERE metric_date = first_cust_active_in_week
    GROUP BY metric_date, week_start
)
, cust_month_incr AS (
    SELECT metric_date, month_start,
           APPROX_COUNT_DISTINCT(account_id) AS mac_incr
    FROM state_with_windows
    WHERE metric_date = first_cust_active_in_month
    GROUP BY metric_date, month_start
)
, payer_week_incr AS (
    SELECT metric_date, week_start,
           APPROX_COUNT_DISTINCT(account_id) AS wpp_incr
    FROM state_with_windows
    WHERE metric_date = first_paid_in_week
    GROUP BY metric_date, week_start
)
, payer_month_incr AS (
    SELECT metric_date, month_start,
           APPROX_COUNT_DISTINCT(account_id) AS mpp_incr
    FROM state_with_windows
    WHERE metric_date = first_paid_in_month
    GROUP BY metric_date, month_start
)
, daily_wm AS (
    SELECT d.metric_date, d.week_start, d.month_start,
           COALESCE(uw.wau_incr, 0) AS wau_incr,
           COALESCE(cw.wac_incr, 0) AS wac_incr,
           COALESCE(pw.wpp_incr, 0) AS wpp_incr,
           COALESCE(um.mau_incr, 0) AS mau_incr,
           COALESCE(cm.mac_incr, 0) AS mac_incr,
           COALESCE(pm.mpp_incr, 0) AS mpp_incr
    FROM days d
    LEFT JOIN user_week_incr uw ON uw.metric_date = d.metric_date AND uw.week_start = d.week_start
    LEFT JOIN cust_week_incr cw ON cw.metric_date = d.metric_date AND cw.week_start = d.week_start
    LEFT JOIN payer_week_incr pw ON pw.metric_date = d.metric_date AND pw.week_start = d.week_start
    LEFT JOIN user_month_incr um ON um.metric_date = d.metric_date AND um.month_start = d.month_start
    LEFT JOIN cust_month_incr cm ON cm.metric_date = d.metric_date AND cm.month_start = d.month_start
    LEFT JOIN payer_month_incr pm ON pm.metric_date = d.metric_date AND pm.month_start = d.month_start
)
, daily_wm_cum AS (
    SELECT
        metric_date,
        SUM(wau_incr) OVER (PARTITION BY week_start ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS weekly_active_users,
        SUM(wac_incr) OVER (PARTITION BY week_start ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS weekly_active_customers,
        SUM(wpp_incr) OVER (PARTITION BY week_start ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS weekly_payers,
        SUM(mau_incr) OVER (PARTITION BY month_start ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS monthly_active_users,
        SUM(mac_incr) OVER (PARTITION BY month_start ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS monthly_active_customers,
        SUM(mpp_incr) OVER (PARTITION BY month_start ORDER BY metric_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS monthly_payers
    FROM daily_wm
)
, cohort_activity AS (
    SELECT
        fl.first_login_dt AS cohort_date,
        COALESCE(p.account_id, fl.account_id) AS account_id,
        MAX(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 0) AND COALESCE(p.revenue,0) > 0 THEN 1 ELSE 0 END) AS d1_paid,
        MAX(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 6) AND COALESCE(p.revenue,0) > 0 THEN 1 ELSE 0 END) AS d7_paid,
        MAX(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 29) AND COALESCE(p.revenue,0) > 0 THEN 1 ELSE 0 END) AS d30_paid,
        MAX(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 59) AND COALESCE(p.revenue,0) > 0 THEN 1 ELSE 0 END) AS d60_paid,
        MAX(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 89) AND COALESCE(p.revenue,0) > 0 THEN 1 ELSE 0 END) AS d90_paid,
        SUM(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 0) THEN COALESCE(p.revenue,0) ELSE 0 END) AS d1_rev,
        SUM(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 6) THEN COALESCE(p.revenue,0) ELSE 0 END) AS d7_rev,
        SUM(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 29) THEN COALESCE(p.revenue,0) ELSE 0 END) AS d30_rev,
        SUM(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 59) THEN COALESCE(p.revenue,0) ELSE 0 END) AS d60_rev,
        SUM(CASE WHEN p.metric_date BETWEEN fl.first_login_dt AND date_add(fl.first_login_dt, 89) THEN COALESCE(p.revenue,0) ELSE 0 END) AS d90_rev
    FROM first_login fl
    LEFT JOIN payments p ON p.account_id = fl.account_id
    WHERE fl.first_login_dt BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
    GROUP BY 1,2
),
cohort_rollup AS (
    SELECT
        cohort_date,
        APPROX_COUNT_DISTINCT(account_id) AS cohort_size,
        SUM(d1_paid) AS cohort_d1_converted,
        SUM(d7_paid) AS cohort_d7_converted,
        SUM(d30_paid) AS cohort_d30_converted,
        SUM(d60_paid) AS cohort_d60_converted,
        SUM(d90_paid) AS cohort_d90_converted,
        SUM(d1_rev) AS cohort_d1_revenue,
        SUM(d7_rev) AS cohort_d7_revenue,
        SUM(d30_rev) AS cohort_d30_revenue,
        SUM(d60_rev) AS cohort_d60_revenue,
        SUM(d90_rev) AS cohort_d90_revenue
    FROM cohort_activity
    GROUP BY cohort_date
)
SELECT
    s.metric_date,
    MAX(nu.new_users) AS new_users,
    APPROX_COUNT_DISTINCT(IF(first_purchase_date = s.metric_date, account_id, NULL)) AS new_customers,
    SUM(daily_revenue) AS daily_revenue,
    SUM(daily_transactions) AS daily_transactions,
    APPROX_COUNT_DISTINCT(IF(is_active_today = 1, account_id, NULL)) AS daily_active_users,
    APPROX_COUNT_DISTINCT(IF(was_active_yesterday = 1, account_id, NULL)) AS prior_day_active_users,
    APPROX_COUNT_DISTINCT(IF(is_active_today = 1 AND was_active_yesterday = 1, account_id, NULL)) AS d_over_d_returning_users,
    APPROX_COUNT_DISTINCT(IF(is_active_customer_today = 1, account_id, NULL)) AS daily_active_customers,
    APPROX_COUNT_DISTINCT(IF(was_active_customer_yesterday = 1, account_id, NULL)) AS prior_day_active_customers,
    APPROX_COUNT_DISTINCT(IF(is_active_customer_today = 1 AND was_active_customer_yesterday = 1, account_id, NULL)) AS d_over_d_returning_customers,
    APPROX_COUNT_DISTINCT(IF(paid_today = 1, account_id, NULL)) AS daily_payers,
    APPROX_COUNT_DISTINCT(IF(paid_yesterday = 1, account_id, NULL)) AS prior_day_payers,
    APPROX_COUNT_DISTINCT(IF(paid_today = 1 AND paid_yesterday = 1, account_id, NULL)) AS d_over_d_returning_payers,
    MAX(wm.weekly_active_users) AS weekly_active_users,
    APPROX_COUNT_DISTINCT(IF(was_active_last_week = 1, account_id, NULL)) AS prior_week_active_users,
    APPROX_COUNT_DISTINCT(IF(is_active_this_week = 1 AND was_active_last_week = 1, account_id, NULL)) AS w_over_w_returning_users,
    MAX(wm.weekly_active_customers) AS weekly_active_customers,
    APPROX_COUNT_DISTINCT(IF(was_active_customer_last_week = 1, account_id, NULL)) AS prior_week_active_customers,
    APPROX_COUNT_DISTINCT(IF(is_active_customer_this_week = 1 AND was_active_customer_last_week = 1, account_id, NULL)) AS w_over_w_returning_customers,
    MAX(wm.weekly_payers) AS weekly_payers,
    APPROX_COUNT_DISTINCT(IF(paid_last_week = 1, account_id, NULL)) AS prior_week_payers,
    APPROX_COUNT_DISTINCT(IF(paid_this_week = 1 AND paid_last_week = 1, account_id, NULL)) AS w_over_w_returning_payers,
    MAX(wm.monthly_active_users) AS monthly_active_users,
    APPROX_COUNT_DISTINCT(IF(was_active_last_month = 1, account_id, NULL)) AS prior_month_active_users,
    APPROX_COUNT_DISTINCT(IF(is_active_this_month = 1 AND was_active_last_month = 1, account_id, NULL)) AS m_over_m_returning_users,
    MAX(wm.monthly_active_customers) AS monthly_active_customers,
    APPROX_COUNT_DISTINCT(IF(was_active_customer_last_month = 1, account_id, NULL)) AS prior_month_active_customers,
    APPROX_COUNT_DISTINCT(IF(is_active_customer_this_month = 1 AND was_active_customer_last_month = 1, account_id, NULL)) AS m_over_m_returning_customers,
    MAX(wm.monthly_payers) AS monthly_payers,
    APPROX_COUNT_DISTINCT(IF(paid_last_month = 1, account_id, NULL)) AS prior_month_payers,
    APPROX_COUNT_DISTINCT(IF(paid_this_month = 1 AND paid_last_month = 1, account_id, NULL)) AS m_over_m_returning_payers,
    -- Regulars
    APPROX_COUNT_DISTINCT(IF(active_days_last_7 >= 6 AND DATEDIFF(s.metric_date, first_login_dt) >= 6, account_id, NULL)) AS d7_regulars,
    APPROX_COUNT_DISTINCT(IF(active_days_last_30 >= 23 AND DATEDIFF(s.metric_date, first_login_dt) >= 29, account_id, NULL)) AS d30_regulars,
    APPROX_COUNT_DISTINCT(IF(active_days_in_week >= 6 AND first_login_dt < DATE_TRUNC('WEEK', s.metric_date), account_id, NULL)) AS weekly_regulars,
    APPROX_COUNT_DISTINCT(IF(active_days_in_month >= CEIL(days_in_month * 0.75) AND first_login_dt < DATE_TRUNC('MONTH', s.metric_date), account_id, NULL)) AS monthly_regulars
    , MAX(cr.cohort_size) AS cohort_size
    , MAX(cr.cohort_d1_converted) AS cohort_d1_converted
    , MAX(cr.cohort_d7_converted) AS cohort_d7_converted
    , MAX(cr.cohort_d30_converted) AS cohort_d30_converted
    , MAX(cr.cohort_d60_converted) AS cohort_d60_converted
    , MAX(cr.cohort_d90_converted) AS cohort_d90_converted
    , MAX(cr.cohort_d1_revenue) AS cohort_d1_revenue
    , MAX(cr.cohort_d7_revenue) AS cohort_d7_revenue
    , MAX(cr.cohort_d30_revenue) AS cohort_d30_revenue
    , MAX(cr.cohort_d60_revenue) AS cohort_d60_revenue
    , MAX(cr.cohort_d90_revenue) AS cohort_d90_revenue
FROM state_with_prior s
LEFT JOIN cohort_rollup cr ON s.metric_date = cr.cohort_date
LEFT JOIN new_users_counts nu ON s.metric_date = nu.metric_date
LEFT JOIN daily_wm_cum wm ON s.metric_date = wm.metric_date
GROUP BY s.metric_date
ORDER BY s.metric_date
"""


def get_daily_kpis(start: str = START, end: str = END):
    """Return a Spark DataFrame by executing the parameterized SQL via spark.sql.

    Expects to run in a Databricks notebook/cluster where a global `spark`
    session is available.
    """
    sql = SQL_TMPL.strip().rstrip(";")
    sql = sql.replace("{{START_DATE}}", start).replace("{{END_DATE}}", end)
    try:
        spark  # type: ignore[name-defined]
    except NameError as e:
        raise RuntimeError(
            "No Spark session found. Run this in a Databricks notebook or create a SparkSession as `spark`."
        ) from e
    return spark.sql(sql)  # Spark DataFrame


def run_cell_1(start: str = START, end: str = END):
    """Execute SQL, publish `sdf_daily` (Spark) and `df_daily` (pandas), and
    show confirmations + preview. Returns the pandas DataFrame.
    """
    sdf = get_daily_kpis(start, end)
    # Cast DecimalType columns to DoubleType for efficient toPandas conversion
    try:
        from pyspark.sql.functions import col  # type: ignore
        decimal_cols = [c for (c, t) in sdf.dtypes if t.startswith('decimal')]
        if decimal_cols:
            sdf = sdf.select(*[col(c).cast('double').alias(c) if c in decimal_cols else col(c) for c in sdf.columns])
    except Exception:
        # Non-fatal; keep original types if Spark context or functions not available
        pass
    # Confirm Spark results and show quick preview
    try:
        spark_count = sdf.count()
    except Exception:
        spark_count = None
    print(f"Cell 1 — Spark rows: {spark_count if spark_count is not None else 'n/a'}")
    try:
        display(sdf.limit(5))  # type: ignore[name-defined]
    except Exception:
        try:
            print(sdf.limit(5).toPandas().to_string(index=False))
        except Exception:
            pass

    # Convert to pandas for downstream cells and confirm
    pdf = sdf.toPandas()
    print("Cell 1 — pandas shape:", pdf.shape)
    try:
        print("Cell 1 — pandas columns (first 10):", pdf.columns.tolist()[:10])
    except Exception:
        pass

    # Publish into the notebook/global namespace for convenience
    g = globals()
    g['sdf_daily'] = sdf
    g['df_daily'] = pdf

    # Emit a small preview as last expression in notebooks
    try:
        display(pdf.head(5))  # type: ignore[name-defined]
    except Exception:
        print(pdf.head(5).to_string(index=False))

    return pdf


if __name__ == "__main__":
    # Running this file directly in a Databricks notebook cell will execute Cell 1.
    run_cell_1(START, END)
