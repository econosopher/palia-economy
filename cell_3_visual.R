#!/usr/bin/env Rscript
#
# Notebook Cell 3 (R) — Render GT table inline (HTML) from Spark temp view.
# - Requires Spark temp view `summary_kpis` published by Cell 2
# - Displays GT table inline in the cell (no file export)

ensure_pkg <- function(pkgs) {
  for (p in pkgs) {
    if (!requireNamespace(p, quietly = TRUE)) {
      install.packages(p, repos = "https://cloud.r-project.org")
    }
  }
}

ensure_pkg(c("gt", "dplyr", "scales"))

suppressPackageStartupMessages({
  library(gt)
  library(dplyr)
  library(scales)
})

# Collect `summary` via SparkR temp view
summary_df <- NULL
try({
  suppressPackageStartupMessages({
    library(SparkR)
  })
  sparkR.session()
  sdf <- SparkR::sql("SELECT * FROM summary_kpis")
  summary_df <- SparkR::collect(sdf)
}, silent = TRUE)
if (is.null(summary_df)) stop("summary_kpis view not found. Run Cell 2 to publish it, then re-run this cell.")

# Select columns similar to the Python visual cell
display_cols <- c(
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
  "daily_arpu", "daily_arppu", "daily_arpc"
)

present <- intersect(display_cols, colnames(summary_df))
view <- head(summary_df[, present, drop = FALSE], 30)

# Inline display only (no file export)
show_inline <- TRUE

# Build GT table with clearer labels, spanners, and readable theme
num_cols <- setdiff(present, c(
  "day_of_week","date",
  "daily_revenue","weekly_revenue","monthly_revenue",
  "daily_arpu","daily_arppu","daily_arpc",
  present[grepl("_rate$", present)]
))

gt_tbl <- gt(view) |>
  tab_header(
    title = md("**Palia KPIs**"),
    subtitle = paste0("Credits→USD (static), as of ", format(Sys.Date(), "%Y-%m-%d"))
  ) |>
  cols_label(
    day_of_week = "Day", date = "Date",
    daily_new_users = "New Users (D)", weekly_new_users = "New Users (W)", monthly_new_users = "New Users (M)",
    daily_active_users = "DAU", d7_regulars = "D7", d30_regulars = "D30", dau_minus_new = "DAU - New",
    weekly_active_users = "WAU", monthly_active_users = "MAU",
    daily_new_customers = "New Cust (D)", weekly_new_customers = "New Cust (W)", monthly_new_customers = "New Cust (M)",
    daily_new_user_to_customer_rate = "New→Cust %",
    daily_active_customers = "DAC", weekly_active_customers = "WAC", monthly_active_customers = "MAC",
    daily_payers = "Payers (D)", weekly_payers = "Payers (W)", monthly_payers = "Payers (M)",
    daily_revenue = "Revenue ($)", weekly_revenue = "Rev (W)", monthly_revenue = "Rev (M)",
    daily_arpu = "ARPU", daily_arppu = "ARPPU", daily_arpc = "ARPC"
  ) |>
  tab_spanner(label = "Installs", columns = intersect(present, c("daily_new_users","weekly_new_users","monthly_new_users"))) |>
  tab_spanner(label = "Users", columns = intersect(present, c("daily_active_users","d7_regulars","d30_regulars","dau_minus_new","weekly_active_users","monthly_active_users"))) |>
  tab_spanner(label = "Customers", columns = intersect(present, c(
    "daily_new_customers","weekly_new_customers","monthly_new_customers",
    "daily_new_user_to_customer_rate",
    "daily_active_customers","weekly_active_customers","monthly_active_customers"
  ))) |>
  tab_spanner(label = "Payers", columns = intersect(present, c("daily_payers","weekly_payers","monthly_payers"))) |>
  tab_spanner(label = "Revenue", columns = intersect(present, c(
    "daily_revenue","weekly_revenue","monthly_revenue","daily_arpu","daily_arppu","daily_arpc"
  ))) |>
  fmt_number(columns = num_cols, decimals = 0, use_seps = TRUE) |>
  fmt_currency(columns = intersect(present, c("daily_revenue","weekly_revenue","monthly_revenue")), currency = "USD", decimals = 0) |>
  fmt_currency(columns = intersect(present, c("daily_arpu","daily_arppu","daily_arpc")), currency = "USD", decimals = 2) |>
  fmt_percent(columns = present[grepl("_rate$", present)], decimals = 1) |>
  cols_align(columns = intersect(present, c("day_of_week","date")), align = "center") |>
  cols_align(columns = setdiff(present, c("day_of_week","date")), align = "right") |>
  opt_row_striping() |>
  tab_options(
    table.font.names = c("Inter", "Helvetica Neue", "Arial", "sans-serif"),
    table.font.size = px(12),
    heading.title.font.size = px(20),
    heading.subtitle.font.size = px(12),
    column_labels.font.size = px(12),
    data_row.padding = px(6)
  )

# Light theme styling
gt_tbl <- gt_tbl |>
  tab_style(style = list(cell_fill(color = "#ffffff"), cell_text(color = "#111827")), locations = cells_body()) |>
  tab_style(style = list(cell_fill(color = "#f3f4f6"), cell_text(color = "#111827", weight = "bold")), locations = cells_column_labels(everything()))

# Heatmap-like data coloring on select columns (domains auto-detected)
hm_cols <- intersect(present, c("daily_revenue","daily_arpu","daily_arppu","daily_new_user_to_customer_rate","daily_customer_payer_rate","daily_user_payer_rate"))
if (length(hm_cols) > 0) {
  for (c in hm_cols) {
    pal <- if (grepl("revenue", c)) c("#e8f5e9", "#2e7d32") else if (grepl("arpp", c)) c("#e3f2fd", "#1565c0") else c("#fff3e0", "#ef6c00")
    gt_tbl <- data_color(gt_tbl, columns = c, colors = scales::col_numeric(palette = pal, domain = NULL))
  }
}

if (show_inline) print(gt_tbl)
