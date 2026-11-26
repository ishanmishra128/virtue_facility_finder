# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 05 — Publish silver review views (READ-ONLY over the review table)
# MAGIC
# MAGIC Reads `vf.silver.facilities_review_v1` and reuses its existing flags — no source data is reread, no table
# MAGIC is modified, and no new cleaning is performed. Creates two persistent views in `vf.silver` (CREATE
# MAGIC VIEW, stopping if either name already exists):
# MAGIC
# MAGIC * `facilities_current_v1` — representative rows only (`keep_exact_representative = true`): removes only
# MAGIC   redundant exact copies, **not** different records describing the same hospital.
# MAGIC * `facilities_review_queue_v1` — current rows with at least one review reason; adds
# MAGIC   `review_reason_codes` (existing `quality_issue_codes` combined with new codes, deduplicated; the
# MAGIC   original `quality_issue_codes` column is unchanged) and `review_priority` (P1/P2/P3 — these organize
# MAGIC   **data review**; they are NOT clinical urgency or medical confidence).
# MAGIC
# MAGIC View names: `vf.silver.facilities_current_v1`, `vf.silver.facilities_review_queue_v1`.

# COMMAND ----------

# DBTITLE 1,Inspect schema & dependencies
# Cell 1 — Inspect the input schema and confirm every column/status/code the views depend on.
# Dependencies are reported from the actual data; nothing is assumed or guessed.

from pyspark.sql import functions as F

INPUT_TABLE = "vf.silver.facilities_review_v1"
CURRENT_VIEW = "vf.silver.facilities_current_v1"
QUEUE_VIEW = "vf.silver.facilities_review_queue_v1"

df_in = spark.read.table(INPUT_TABLE)

REQUIRED_COLS = [
    "keep_exact_representative", "unique_id_status", "quality_issue_codes",
    "postal_match_status", "postal_state_comparison_v3", "coordinate_status",
    "name", "state_clean", "city_clean", "pincode_clean", "postal_pair_candidates", "_source_row_id",
]
missing_cols = [c for c in REQUIRED_COLS if c not in df_in.columns]
if missing_cols:
    raise ValueError(f"Missing required columns in {INPUT_TABLE}: {missing_cols} — stopping rather than guessing.")

print(f"{INPUT_TABLE}: {df_in.count():,} rows x {len(df_in.columns)} columns (schema reused; no new cleaning)")

# Actual status values present in the data (used verbatim by the view logic)
STATUS_DEPS = {
    "unique_id_status": ["unexpected_format"],
    "coordinate_status": ["invalid_numeric", "outside_global_range", "outside_india_screen", "missing"],
    "postal_match_status": ["facility_pin_missing_or_invalid", "reference_unmatched", "reference_incomplete", "review_required", "unambiguous_pair"],
    "postal_state_comparison_v3": ["needs_alias_or_conflict_review"],
}
for col_name, expected in STATUS_DEPS.items():
    present = sorted([r[col_name] for r in df_in.select(col_name).distinct().collect() if r[col_name] is not None])
    absent = [v for v in expected if v not in present]
    print(f"{col_name} values present: {present}" + (f"  | referenced but ABSENT from data: {absent}" if absent else ""))

# Actual quality_issue_codes values present, and which rule-referenced codes never occur
present_codes = sorted({
    r["code"] for r in df_in.where(F.size("quality_issue_codes") > 0)
    .select(F.explode("quality_issue_codes").alias("code")).distinct().collect()
})
RULE_CODE_DEPS = [
    "missing_name",  # P1
    "invalid_or_missing_pin", "invalid_number_doctors", "invalid_capacity", "invalid_founding_year",
    "provisional_outlier_doctors_over_1000", "provisional_outlier_capacity_over_5000",
    "specialties_parse_failure", "affiliation_types_parse_failure", "source_urls_parse_failure", "future_date",  # P2
]
absent_codes = [c for c in RULE_CODE_DEPS if c not in present_codes]
print(f"quality_issue_codes present: {present_codes}")
if absent_codes:
    print(f"Rule-referenced codes that never occur in the data (rules still valid, reported not guessed): {absent_codes}")

# COMMAND ----------

# DBTITLE 1,Create the two views
# Cell 2 — Create the two persistent views (CREATE VIEW; stop if EITHER name already exists).
# No table is modified; both views only read vf.silver.facilities_review_v1 (the queue via the current view).

for view_name in (CURRENT_VIEW, QUEUE_VIEW):
    if spark.catalog.tableExists(view_name):
        raise ValueError(f"{view_name} already exists — stopping. Nothing was created or overwritten.")

# View 1: representative rows only — removes redundant EXACT copies only, never different records
# describing the same hospital. All columns preserved.
spark.sql(f"""
CREATE VIEW {CURRENT_VIEW} AS
SELECT * FROM {INPUT_TABLE}
WHERE keep_exact_representative = true
""")

# View 2: review queue over the current view. review_reason_codes combines the UNCHANGED
# quality_issue_codes with new codes, deduplicated. Priorities: P1 = unexpected identifier, missing
# name, coordinates outside/invalid (not merely missing), or unresolved state comparison; P2 = other
# geography issues (coordinates merely missing, any non-unambiguous postal status, PIN format issues),
# invalid numeric values, numeric outliers, array parse failures, or future dates; P3 = remaining.
# Highest applicable priority wins. Priorities organize DATA REVIEW — not clinical urgency.
spark.sql(f"""
CREATE VIEW {QUEUE_VIEW} AS
WITH base AS (
    SELECT *,
        array_distinct(
            concat(
                coalesce(quality_issue_codes, CAST(ARRAY() AS ARRAY<STRING>)),
                filter(
                    ARRAY(
                        CASE WHEN unique_id_status = 'unexpected_format' THEN 'unexpected_identifier_format' END,
                        CASE WHEN postal_match_status <> 'unambiguous_pair' THEN 'geography_' || postal_match_status END,
                        CASE WHEN postal_state_comparison_v3 = 'needs_alias_or_conflict_review' THEN 'state_alias_or_conflict_unresolved' END
                    ),
                    c -> c IS NOT NULL
                )
            )
        ) AS review_reason_codes
    FROM {CURRENT_VIEW}
)
SELECT *,
    CASE
        WHEN unique_id_status = 'unexpected_format'
            OR array_contains(quality_issue_codes, 'missing_name')
            OR coordinate_status IN ('invalid_numeric', 'outside_global_range', 'outside_india_screen')
            OR postal_state_comparison_v3 = 'needs_alias_or_conflict_review'
            THEN 'P1'
        WHEN coordinate_status = 'missing'
            OR postal_match_status <> 'unambiguous_pair'
            OR arrays_overlap(review_reason_codes, ARRAY(
                'invalid_or_missing_pin', 'invalid_number_doctors', 'invalid_capacity', 'invalid_founding_year',
                'provisional_outlier_doctors_over_1000', 'provisional_outlier_capacity_over_5000',
                'specialties_parse_failure', 'affiliation_types_parse_failure', 'source_urls_parse_failure', 'future_date'
            ))
            THEN 'P2'
        ELSE 'P3'
    END AS review_priority
FROM base
WHERE size(review_reason_codes) > 0  -- only rows with at least one review reason
""")

n_current = spark.table(CURRENT_VIEW).count()
n_queue = spark.table(QUEUE_VIEW).count()
print(f"Created {CURRENT_VIEW}: {n_current:,} rows (all columns preserved)")
print(f"Created {QUEUE_VIEW}: {n_queue:,} rows x {len(spark.table(QUEUE_VIEW).columns)} columns "
      f"(+review_reason_codes, +review_priority)")

# COMMAND ----------

# DBTITLE 1,Displays & checks
# Cell 3 — Displays and checks. Reads the views and the underlying table only; modifies nothing.

df_current = spark.table(CURRENT_VIEW)
df_queue = spark.table(QUEUE_VIEW)
n_current, n_queue = df_current.count(), df_queue.count()

print(f"facilities_current_v1 total rows: {n_current:,} (expected baseline 10,077: "
      f"{'MATCH' if n_current == 10_077 else 'MISMATCH'})")

print("\nDistinct review rows by priority:")
display(df_queue.groupBy("review_priority").count().orderBy(F.asc("review_priority")))

# Rows can carry MULTIPLE reasons — counts overlap and do not sum to the row count.
reason_counts = (
    df_queue.select(F.explode("review_reason_codes").alias("reason"))
    .groupBy("reason").count().orderBy(F.desc("count"))
)
print("\nCounts by review reason (NOTE: reasons overlap — one row can carry several; these do NOT sum to the row count):")
display(reason_counts)
total_reasons = reason_counts.agg(F.sum("count").alias("s")).first()["s"] or 0
print(f"Reason occurrences: {total_reasons:,} across {n_queue:,} review rows (difference = rows carrying multiple reasons)")

print("\nTwenty P1 examples (row ID, unique_id, name, state/city/PIN, coordinate status, postal candidates, reasons):")
display(
    df_queue.where(F.col("review_priority") == "P1")
    .select(
        "_source_row_id", "unique_id", "name", "state_clean", "city_clean", "pincode_clean",
        "coordinate_status", "postal_pair_candidates", "review_reason_codes",
    )
    .limit(20)
)

# --- Checks: unique row IDs in both views; queue is a subset of current; underlying table unchanged.
cur_ids = df_current.select("_source_row_id")
que_ids = df_queue.select("_source_row_id")
cur_distinct = cur_ids.distinct().count()
que_distinct = que_ids.distinct().count()
que_nulls = df_queue.where(F.col("_source_row_id").isNull()).count()
subset_extra = que_ids.exceptAll(cur_ids).count()

df_table = spark.read.table(INPUT_TABLE)
table_rows = df_table.count()
table_id_distinct = df_table.select("_source_row_id").distinct().count()

checks = [
    ("Both views have unique, non-null row IDs",
     cur_distinct == n_current and que_distinct == n_queue and que_nulls == 0,
     f"current {cur_distinct:,}/{n_current:,}; queue {que_distinct:,}/{n_queue:,}; {que_nulls} nulls"),
    ("Review queue is a subset of current records", subset_extra == 0,
     f"{subset_extra} queue row IDs absent from facilities_current_v1"),
    ("Underlying table unchanged (10,088 rows, 10,088 distinct IDs)",
     table_rows == 10_088 and table_id_distinct == 10_088,
     f"{table_rows:,} rows, {table_id_distinct:,} distinct IDs"),
    ("Current view matches the 10,077 baseline", n_current == 10_077, f"{n_current:,} rows"),
]
display(spark.createDataFrame([(c, p, d) for c, p, d in checks], ["check", "passed", "detail"]))