# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 04 — Flag record integrity (exact source duplicates + unique_id checks)
# MAGIC
# MAGIC Reads `vf.silver.facilities_geo_v3`; writes `vf.silver.facilities_review_v1` (error-if-exists). Every
# MAGIC row and existing column is preserved — nothing is dropped or merged (not by name, cluster_id, or
# MAGIC identifier). The exact-duplicate representative is an arbitrary but repeatable choice (order by the
# MAGIC persisted `_source_row_id`), NOT a quality ranking. An `unexpected_format` unique_id is a review flag,
# MAGIC NOT proof that the record is invalid.

# COMMAND ----------

# DBTITLE 1,Derive original 51 columns from source schema
# Cell 1 — Load input; derive the original 51 column names from the shared source table's schema ONLY
# (never from the enriched table), then validate they all exist in the input.

from pyspark.sql import functions as F
from pyspark.sql import Window

SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
INPUT_TABLE = "vf.silver.facilities_geo_v3"
REVIEW_OUTPUT_TABLE = "vf.silver.facilities_review_v1"

df_input = spark.read.table(INPUT_TABLE)
ORIGINAL_COLS = spark.read.table(SOURCE_TABLE).columns  # schema only; the source is never modified

# Shared missing-value rule (identical to the cleaning notebook): SQL NULL, or trimmed + lowercased
# "", "null", "none", "n/a", "nan" — evaluated before any validation or comparison.
MISSING_MARKERS = ["", "null", "none", "n/a", "nan"]


def is_missing(c):
    return c.isNull() | F.lower(F.trim(c)).isin(MISSING_MARKERS)


if len(ORIGINAL_COLS) != 51:
    raise ValueError(f"Source schema has {len(ORIGINAL_COLS)} columns, expected exactly 51 — stopping.")
missing_from_input = [c for c in ORIGINAL_COLS if c not in df_input.columns]
if missing_from_input:
    raise ValueError(f"Original columns absent from {INPUT_TABLE}: {missing_from_input} — stopping.")

print(f"Original source columns identified from schema: {len(ORIGINAL_COLS)} (all present in the input)")
print(f"Input {INPUT_TABLE}: {df_input.count():,} rows x {len(df_input.columns)} columns")

# COMMAND ----------

# DBTITLE 1,Exact source duplicates
# Cell 2 — Exact source duplicates: group by ALL 51 original columns with exact column equality
# (no hash shortcut, no merging). Rank within each group by the persisted _source_row_id — an
# arbitrary but repeatable representative choice, NOT a quality ranking. No rows are dropped.

w_group = Window.partitionBy(*ORIGINAL_COLS)
w_ranked = w_group.orderBy(F.col("_source_row_id"))  # persisted row ID -> deterministic order

df_dup = (
    df_input
    .withColumn("exact_duplicate_group_size", F.count(F.lit(1)).over(w_group))
    .withColumn("exact_duplicate_rank", F.row_number().over(w_ranked))
    .withColumn("keep_exact_representative", F.col("exact_duplicate_rank") == F.lit(1))
)

print(f"Columns: {len(df_input.columns)} -> {len(df_dup.columns)} "
      f"(+exact_duplicate_group_size, exact_duplicate_rank, keep_exact_representative)")

rows_in_dupes = df_dup.where(F.col("exact_duplicate_group_size") > 1).count()
groups_total = df_dup.where(F.col("keep_exact_representative")).count()  # one per distinct 51-column value group
groups_dupes = df_dup.where((F.col("exact_duplicate_group_size") > 1) & F.col("keep_exact_representative")).count()
print(f"Distinct original-value groups: {groups_total:,}; groups containing duplicates: {groups_dupes:,}; "
      f"rows in duplicate groups: {rows_in_dupes:,}")

print("\nRows by exact_duplicate_group_size:")
display(df_dup.groupBy("exact_duplicate_group_size").count().orderBy(F.asc("exact_duplicate_group_size")))

print("\nUp to 15 example rows from duplicate groups (representative first):")
display(
    df_dup.where(F.col("exact_duplicate_group_size") > 1)
    .select("name", "_source_row_id", "exact_duplicate_group_size", "exact_duplicate_rank", "keep_exact_representative")
    .orderBy(F.asc("exact_duplicate_group_size"), F.asc("_source_row_id"))
    .limit(15)
)

# COMMAND ----------

# DBTITLE 1,unique_id status and repeat analysis
# Cell 3 — unique_id checks. 'unexpected_format' is a review flag, NOT proof the record is invalid.
# No records are merged by name, cluster_id, or identifier.

UUID_PATTERN = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"  # 8-4-4-4-12 hex

df_review = df_dup.withColumn(
    "unique_id_status",
    F.when(is_missing(F.col("unique_id")), "missing")
     .when(F.trim(F.col("unique_id")).rlike(UUID_PATTERN), "uuid_format")
     .otherwise("unexpected_format"),
)

print(f"Columns: {len(df_dup.columns)} -> {len(df_review.columns)} (+unique_id_status)")
print("\nunique_id_status counts:")
display(df_review.groupBy("unique_id_status").count().orderBy(F.desc("count")))

# Repeated NONMISSING trimmed unique_id values, and IDs tied to more than one distinct 51-column record.
uid_groups = (
    df_review.where(~is_missing(F.col("unique_id")))
    .withColumn("unique_id_trimmed", F.trim("unique_id"))
    .groupBy("unique_id_trimmed")
    .agg(
        F.count(F.lit(1)).alias("rows_sharing_id"),
        F.countDistinct(F.struct(*ORIGINAL_COLS)).alias("distinct_original_records"),
    )
)

repeated_ids = uid_groups.where(F.col("rows_sharing_id") > 1)
multi_record_ids = uid_groups.where(F.col("distinct_original_records") > 1)

# sum over an empty set returns NULL (one row, s = NULL) -> coalesce to 0 before formatting
rep_rows = repeated_ids.agg(F.sum("rows_sharing_id").alias("s")).first()["s"] or 0
multi_rows = multi_record_ids.agg(F.sum("rows_sharing_id").alias("s")).first()["s"] or 0
print(f"\nRepeated nonmissing trimmed unique_id values: {repeated_ids.count():,} IDs covering {rep_rows:,} rows")
print(f"IDs associated with more than one distinct original 51-column record: {multi_record_ids.count():,} IDs "
      f"covering {multi_rows:,} rows")

print("\nUp to 10 such multi-record IDs (shared-row and distinct-record counts):")
display(multi_record_ids.orderBy(F.desc("distinct_original_records"), F.desc("rows_sharing_id")).limit(10))

# COMMAND ----------

# DBTITLE 1,Save facilities_review_v1 and verify
# Cell 4 — Save with error-if-exists and verify. The input (and the source table) are never modified.

if spark.catalog.tableExists(REVIEW_OUTPUT_TABLE):
    raise ValueError(f"{REVIEW_OUTPUT_TABLE} already exists — stopping. Nothing was overwritten or appended.")

df_review.write.format("delta").mode("error").saveAsTable(REVIEW_OUTPUT_TABLE)

df_saved = spark.read.table(REVIEW_OUTPUT_TABLE)
input_cols = df_input.columns  # all 97 existing columns
rows_in, rows_out = df_input.count(), df_saved.count()

id_nulls = df_saved.where(F.col("_source_row_id").isNull()).count()
id_distinct = df_saved.select("_source_row_id").distinct().count()
ids_missing = df_input.select("_source_row_id").exceptAll(df_saved.select("_source_row_id")).count()
ids_extra = df_saved.select("_source_row_id").exceptAll(df_input.select("_source_row_id")).count()

vals_diff_a = df_input.select(*input_cols).exceptAll(df_saved.select(*input_cols)).count()
vals_diff_b = df_saved.select(*input_cols).exceptAll(df_input.select(*input_cols)).count()

# Group/rank integrity: exactly one representative per original-value group, rank never above group size.
rep_count = df_saved.where(F.col("keep_exact_representative")).count()
distinct_groups = df_saved.groupBy(*ORIGINAL_COLS).agg(F.count(F.lit(1)).alias("g")).count()
rank_ok = df_saved.where(F.col("exact_duplicate_rank") > F.col("exact_duplicate_group_size")).count() == 0
reps_per_group_ok = (
    df_saved.groupBy(*ORIGINAL_COLS)
    .agg(F.sum(F.col("keep_exact_representative").cast("long")).alias("reps"))
    .where(F.col("reps") != 1).count() == 0
)

checks = [
    ("Row count equals input", rows_out == rows_in, f"{rows_out:,} vs {rows_in:,}"),
    ("_source_row_id unique and non-null", id_nulls == 0 and id_distinct == rows_out, f"{id_distinct:,} distinct, {id_nulls} nulls"),
    ("Every input row ID appears exactly once", ids_missing == 0 and ids_extra == 0, f"missing {ids_missing}, extra {ids_extra}"),
    ("All existing column values unchanged (EXCEPT ALL both directions)", vals_diff_a == 0 and vals_diff_b == 0, f"{vals_diff_a} / {vals_diff_b} differing rows"),
    ("Exactly one representative per original-value group", reps_per_group_ok and rep_count == distinct_groups, f"{rep_count:,} representatives for {distinct_groups:,} groups"),
    ("Rank never exceeds group size", rank_ok, "0 rows violate"),
    ("v3 read intact (facilities_geo_v3)", rows_in == 10_088, f"{rows_in:,} rows in v3"),
]
display(spark.createDataFrame([(c, p, d) for c, p, d in checks], ["check", "passed", "detail"]))

print(f"\nSaved {REVIEW_OUTPUT_TABLE}: {rows_out:,} rows x {len(df_saved.columns)} columns")