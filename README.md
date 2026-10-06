# NYC Taxi Data Engineering Pipeline

An end-to-end pipeline on Databricks (Community Edition) that extracts NYC TLC yellow
taxi trip data, lands it, loads it into a bronze table, cleans and models it into a
star schema, validates it with automated data quality checks, and serves the results
through a dashboard — all orchestrated as a single scheduled Workflow.

## Architecture

```
extract_and_land  →  bronze_ingestion  →  silver_cleaning  →  gold_modeling  →  data_quality_checks
  (download +          (raw load,           (dedupe, filter,     (star schema:      (15 automated
   land in volume,       idempotent)          standardize)         fact + 6 dims)     checks)
   structured logging,
   retry on timeout)
```

See `SOURCE.md` for dataset details and `DECISIONS.md` for the reasoning behind key
design choices.

## Prerequisites

- A Databricks Community Edition account (free): https://www.databricks.com/try-databricks
- A GitHub account, to host this repo

## Running from zero

### 1. Clone this repo into Databricks
1. In Databricks: **Workspace → Repos → Create Repo**
2. Paste this repo's GitHub URL
3. Confirm the repo name auto-fills correctly and click **Create**

### 2. Create storage
1. Go to **Catalog** in the sidebar
2. Under the `workspace` catalog, `default` schema, click **Create → Volume**
3. Name it `raw_files` — this gives you `/Volumes/workspace/default/raw_files`

### 3. Run the pipeline notebooks, in order
All five notebooks live in this repo and are designed to be run top-to-bottom
(use "Run all" rather than running cells out of order):

1. `00_extract_and_land` — downloads the source parquet + zone lookup CSV directly
   from TLC's public CDN and lands them in the volume. Takes a `year_month` parameter
   (default `2024-01`).
2. `01_bronze_ingestion` — loads the raw files as-is into `workspace.default.trips_raw`
   and `zone_lookup_raw`. Idempotent: deletes any existing rows for the same
   `year_month` before reloading, so re-running never duplicates data.
3. `02_silver_cleaning` — dedupes and applies data quality filters (positive fares,
   valid passenger counts, valid timestamps, valid location IDs), producing
   `trips_clean` and `zones_clean`.
4. `03_gold_modeling` — builds the star schema: `fact_trips` plus `dim_date`,
   `dim_time`, `dim_location`, `dim_rate_code`, `dim_payment_type`, `dim_vendor`.
5. `04_data_quality_checks` — runs 15 automated checks (not-null, uniqueness,
   accepted values, referential integrity, range sanity) against the gold layer and
   raises an error if any check fails.

### 4. Set up orchestration
1. Go to **Workflows → Create Job**
2. Add five tasks, one per notebook above, each named to match its notebook
3. Chain them with **Depends on**: `extract_and_land → bronze_ingestion →
   silver_cleaning → gold_modeling → data_quality_checks`
4. On `extract_and_land`, set **Retries** to at least 1, with "retry on timeout" —
   this is the pipeline's most failure-prone step (a network call)
5. Set a schedule (monthly is reasonable, matching the source's publish cadence)
6. Click **Run now** to verify the whole chain succeeds end-to-end

### 5. Build the dashboard
1. Go to **Dashboards → Create Dashboard**
2. Add the three datasets/queries in `05_analytics_queries` (or the dashboard's own
   Data tab) and visualize each as a chart — see that notebook for the exact SQL
3. Refresh the dashboard any time the gold tables change

## Re-running for a new month

Change the `year_month` widget value on `00_extract_and_land` (and pass the same
value through to `01_bronze_ingestion` if running manually) and re-run the Workflow.
Because ingestion is idempotent per month, existing months are left untouched and
only the new month's data is added.

## Known limitations

- Community Edition serverless compute auto-suspends when idle; the first run after
  a period of inactivity may prompt you to restart compute.
- This project processes a single month as a fixed snapshot rather than a continuously
  streaming feed; see `SOURCE.md` for the source's actual publish cadence.
In Databricks, paste that text into your README.md file (replacing whatever's there), then create DECISIONS.md the same
