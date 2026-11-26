# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 04 — Geography review (READ-ONLY)
# MAGIC
# MAGIC Reviews `vf.silver.facilities_geo_v2` only — nothing is written or modified. No automatic replacements,
# MAGIC no fuzzy location matching, no LLM row-level classification, and postal candidates are **not** treated as
# MAGIC verified facility locations.

# COMMAND ----------

# DBTITLE 1,Section 1 — alias/conflict review pairs
# Cell 1 — READ-ONLY Section 1: needs_alias_or_conflict_review pairs. Reads vf.silver.facilities_geo_v2;
# writes nothing. Differences below are cases in need of review, NOT confirmed conflicts or proposed fixes.

from pyspark.sql import functions as F

GEO_TABLE = "vf.silver.facilities_geo_v2"
df = spark.read.table(GEO_TABLE)

# Section 1: group the 387 rows by normalized facility state vs postal state candidate.
s1 = df.where(F.col("postal_state_comparison") == "needs_alias_or_conflict_review")

alias_groups = (
    s1.withColumn("state_clean_norm", F.lower(F.trim(F.col("state_clean"))))
    .groupBy("state_clean_norm", "postal_state_candidate")
    .agg(
        F.count(F.lit(1)).alias("affected_rows"),
        F.slice(  # up to three example facilities per group; plain display, no classification
            F.collect_list(F.struct(F.col("name").alias("name"), F.col("city_clean").alias("city_clean"), F.col("pincode_clean").alias("pincode_clean"))),
            1, 3,
        ).alias("examples"),
    )
)

s1_rows = s1.count()
s1_group_count = alias_groups.count()
s1_group_sum = alias_groups.agg(F.sum("affected_rows").alias("s")).first()["s"]

print(f"Section 1 — needs_alias_or_conflict_review: {s1_rows:,} rows across {s1_group_count} distinct "
      f"(facility state, postal state candidate) pairs; grouped counts sum to {s1_group_sum:,}")
print(f"Reconciliation: {'PASSED' if s1_rows == s1_group_sum == 387 else 'FAILED'} "
      f"(expected 387 rows from the v2 run; group sum == row count: {s1_group_sum == s1_rows})")

print("\nTop 30 groups by affected rows (all 387 rows are included in the reconciliation above):")
display(alias_groups.orderBy(F.desc("affected_rows"), F.asc("state_clean_norm")).limit(30))

# COMMAND ----------

# DBTITLE 1,Sections 2 & 3 — review groups and reference_incomplete
# Cell 2 — READ-ONLY Sections 2 & 3: review_required candidate-pair groups and the four
# reference_incomplete records. Reads only; writes nothing; resolves nothing.

# Section 2: group review_required rows by the SORTED postal_pair_candidates array (group key is
# independent of pair order within the array).
s2 = (
    df.where(F.col("postal_match_status") == "review_required")
    .withColumn("sorted_pairs", F.array_sort("postal_pair_candidates"))
    .withColumn("n_complete_candidates", F.size("sorted_pairs"))
)

review_groups = (
    s2.groupBy("sorted_pairs", "n_complete_candidates")
    .agg(
        F.count(F.lit(1)).alias("facilities"),
        F.slice(  # up to three example facilities per group; plain display, no classification
            F.collect_list(F.struct(F.col("name").alias("name"), F.col("city_clean").alias("city_clean"), F.col("pincode_clean").alias("pincode_clean"))),
            1, 3,
        ).alias("examples"),
    )
)

s2_rows = s2.count()
s2_group_count = review_groups.count()
s2_group_sum = review_groups.agg(F.sum("facilities").alias("s")).first()["s"]
one_cand = s2.where(F.col("n_complete_candidates") == 1).count()
multi_cand = s2.where(F.col("n_complete_candidates") > 1).count()

print(f"Section 2 — review_required: {s2_rows:,} facilities across {s2_group_count} distinct candidate-pair sets; "
      f"grouped counts sum to {s2_group_sum:,}")
print(f"By candidate count: {one_cand:,} with exactly ONE complete candidate, {multi_cand:,} with MULTIPLE complete candidates "
      f"(sum {one_cand + multi_cand:,})")
print("One-candidate rows remain under review ONLY because reference entries were incomplete for their PIN.")
print(f"Reconciliation: {'PASSED' if s2_rows == s2_group_sum == 513 and one_cand + multi_cand == s2_rows else 'FAILED'} "
      f"(expected 513 facilities from the v2 run; group sum == row count: {s2_group_sum == s2_rows})")

print("\nTop 30 candidate-pair groups by facility count:")
display(review_groups.orderBy(F.desc("facilities"), F.asc("sorted_pairs")).limit(30))

# Section 3: the four reference_incomplete records (matched PIN, no complete reference state/district pair).
s3 = df.where(F.col("postal_match_status") == "reference_incomplete")
print(f"\nSection 3 — reference_incomplete: {s3.count()} records (expected 4 from the v2 run)")
display(
    s3.select(
        "_source_row_id", "name", "state_clean", "city_clean", "pincode_clean",
        "postal_match_status", "postal_pair_candidates",
    )
)