# Design Decisions

Five choices made during this project, and the reasoning behind each.

---

## 1. Idempotent bronze ingestion (delete-before-append per month)

**Decision:** Before writing a month's data into `trips_raw`, delete any existing rows
for that same `year_month`, then append fresh.

**Why it matters / how I got here:** The first version of `01_bronze_ingestion` simply
appended every time it ran, with no check for existing data. This wasn't caught until
the gold-layer dashboard showed roughly 18 million trips instead of the expected 2.7
million — the orchestrated Workflow had been run seven times during development and
testing, and each run appended another full copy of January's data, since Spark
treated each run's rows as distinct (they carried different `_ingested_at`
timestamps, so `dropDuplicates()` in the silver layer never caught them).

**What I considered instead:**
- *Do nothing, just don't re-run the pipeline carelessly* — rejected, because a
  pipeline that depends on the operator never running it twice for the same period
  isn't a real pipeline; scheduled/retried runs make accidental re-execution
  inevitable.
- *MERGE/upsert on a composite key* — more correct for streaming or row-level
  incremental loads, but overkill for this project's batch-per-month grain, where
  "delete the month, reload the month" is simpler and easier to reason about.

**Trade-off accepted:** Re-running the pipeline for a month re-downloads and
re-processes the full month rather than doing a true incremental diff. Acceptable
at this data volume (a few million rows); would need revisiting at larger scale.

---

## 2. Medallion architecture (bronze/silver/gold) over a single-stage transform

**Decision:** Split the pipeline into three distinct layers with separate tables at
each stage, rather than transforming raw data directly into the star schema in one
step.

**Why:** Keeping bronze untouched means the pipeline can always be re-run from the
original raw data if a downstream bug is found — which is exactly what happened with
the idempotency bug above. If bronze had been overwritten or skipped, recovering
would have required re-downloading and manually reconstructing the original load
order. Separating silver (cleaning) from gold (modeling) also made it possible to
measure and report the cleaning impact precisely (8.2% of rows removed) as a
standalone number, rather than it being buried inside a single large transformation.

**Trade-off accepted:** Three extra tables and three extra notebook executions
per run, compared to a single-pass transform. Worth it for the debuggability.

---

## 3. One month of data (January 2024) rather than the full historical dataset

**Decision:** Scope the working dataset to a single month (~2.96M raw rows) instead
of loading multiple years of TLC data.

**Why:** Databricks Community Edition provides limited, shared compute. A single
month is large enough to be a genuine pipeline — real data quality issues, a
meaningful star schema, realistic runtimes (bronze ingestion alone takes ~2-3
minutes) — without risking job timeouts or unusably slow iteration during
development. The pipeline is parameterized by `year_month` specifically so that
scope can be expanded later (or in a grading environment with more compute) without
changing any logic.

**Trade-off accepted:** Trends that only show up across multiple months or seasons
(e.g., weather effects, holiday spikes) aren't visible in this dataset's current
scope.

---

## 4. Custom Python assertions for data quality checks, not a dedicated DQ framework

**Decision:** Implement the 15 data quality checks as plain Python functions
(`check(name, condition)`) inside a notebook, rather than adopting a framework like
Great Expectations or dbt tests.

**Why:** For a project of this size, a dedicated DQ framework adds setup overhead
(configuration files, a separate execution model) without a proportional benefit —
the checks needed here (not-null, uniqueness, accepted values, referential
integrity, range sanity) are straightforward boolean conditions that are easy to
read and modify directly in the same notebook that already has the data loaded.
The checks still produce the same evidence a framework would: a pass/fail per rule
and a hard failure (raised exception) if anything fails, which is what the
orchestrator needs to halt the pipeline on bad data.

**Trade-off accepted:** No built-in reporting UI, check history, or anomaly
detection — a framework would provide more of this out of the box at larger scale
or with more checks.

---

## 5. Full extract step inside the orchestrated pipeline, not a manual upload

**Decision:** Add a dedicated `00_extract_and_land` notebook that downloads the
source files directly from TLC's public CDN via `urllib`, rather than relying on
manually uploading files through the Databricks UI (which is how the project
initially worked, before this step was added).

**Why:** A pipeline that depends on a human manually downloading and uploading a
file isn't actually automatable or schedulable — it breaks the point of having an
orchestrator. Moving extraction into a notebook with retry-on-timeout means the
whole chain (extract → land → load → transform/test → publish) can run unattended
on a schedule, and a transient network failure on the download step is retried
automatically rather than silently stalling the whole pipeline.

**Trade-off accepted:** The pipeline now has a hard dependency on TLC's CDN URL
structure remaining stable; if TLC changes their file naming or hosting, the extract
step would need updating. This is a reasonable trade-off for a project working with
a single, well-established public data source.