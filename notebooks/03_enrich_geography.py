# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 03 — Enrich geography (postal PIN lookup)
# MAGIC
# MAGIC Inputs: `vf.silver.facilities_clean_v1` and `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory`.
# MAGIC
# MAGIC Every facility row and column is preserved. No fuzzy matching, geocoding, hospital merging, alias resolution,
# MAGIC or postal-coordinate use. A postal candidate is **not** a verified hospital location, and a spelling
# MAGIC difference is **not** a confirmed conflict — it goes to alias/conflict review.
# MAGIC
# MAGIC **v2 revision:** for reference statename/district ONLY, trimmed case-insensitive exact `"na"` is additionally
# MAGIC treated as missing (raw reference values preserved; the global missing-marker rule is unchanged everywhere
# MAGIC else). Output is written to `vf.silver.facilities_geo_v2` with error-if-exists; v1 is preserved untouched.

# COMMAND ----------

# DBTITLE 1,Load inputs & inspect schemas
# Cell 1 — Load inputs and inspect schemas. Nothing is modified in this cell.

from pyspark.sql import functions as F

FACILITIES_TABLE = "vf.silver.facilities_clean_v1"
REFERENCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory"
GEO_OUTPUT_TABLE = "vf.silver.facilities_geo_v2"  # v2: reference-only "na" rule; facilities_geo_v1 is preserved untouched

df_facilities = spark.read.table(FACILITIES_TABLE)
df_post_raw = spark.read.table(REFERENCE_TABLE)

# Shared missing-value rule (identical to the cleaning notebook): SQL NULL, or trimmed + lowercased
# "", "null", "none", "n/a", "nan" — always evaluated before any validation or comparison.
MISSING_MARKERS = ["", "null", "none", "n/a", "nan"]


def is_missing(c):
    return c.isNull() | F.lower(F.trim(c)).isin(MISSING_MARKERS)


print("Reference schema (india_post_pincode_directory):")
df_post_raw.printSchema()  # pincode is BIGINT; statename/district are STRING

facility_count = df_facilities.count()
reference_count = df_post_raw.count()
print(f"Facilities: {facility_count:,} rows x {len(df_facilities.columns)} columns ({FACILITIES_TABLE})")
print(f"Reference:  {reference_count:,} rows x {len(df_post_raw.columns)} columns ({REFERENCE_TABLE})")

# COMMAND ----------

# DBTITLE 1,Build one-row-per-PIN postal lookup
# Cell 2 — Postal lookup: exactly ONE row per six-digit PIN. Distinct complete state/district pairs are
# preserved as STRUCTS (never split into separate arrays); postal coordinates are ignored; aliases are not resolved.

PIN_PATTERN = "^[1-9][0-9]{5}$"


def normalize_reference_text(col_name):
    """Reference-ONLY rule for statename/district: in addition to the global missing markers, trimmed
    case-insensitive exact "na" is treated as missing (India Post writes literal "NA"). The global
    missing-marker rule is unchanged for every other field; raw reference values are preserved."""
    raw = F.col(col_name)
    return F.when(
        is_missing(raw) | (F.lower(F.trim(raw)) == "na"),
        F.lit(None).cast("string"),
    ).otherwise(F.lower(F.trim(F.regexp_replace(raw, "\\s+", " "))))


# pincode BIGINT -> trimmed STRING; retain only six-digit PINs [1-9][0-9]{5}
df_post_clean = (
    df_post_raw
    .withColumn("pin", F.trim(F.col("pincode").cast("string")))
    .withColumn("state_norm", normalize_reference_text("statename"))
    .withColumn("district_norm", normalize_reference_text("district"))
    .where(F.col("pin").rlike(PIN_PATTERN))
    .withColumn("pair_complete", F.col("state_norm").isNotNull() & F.col("district_norm").isNotNull())
)

postal_lookup = (
    df_post_clean.groupBy("pin")
    .agg(
        # distinct COMPLETE pairs only, pairing preserved via struct; collect_set drops the NULLs
        F.collect_set(
            F.when(F.col("pair_complete"), F.struct(F.col("state_norm").alias("state"), F.col("district_norm").alias("district")))
        ).alias("pair_candidates"),
        F.count(F.lit(1)).alias("ref_row_count"),
        F.sum((~F.col("pair_complete")).cast("long")).alias("incomplete_pair_count"),
    )
)

valid_pin_rows = df_post_clean.count()
incomplete_pair_rows = df_post_clean.where(~F.col("pair_complete")).count()
lookup_summary = postal_lookup.agg(
    F.count(F.lit(1)).alias("pins"),
    F.sum((F.size("pair_candidates") == 0).cast("long")).alias("no_complete_pair"),
    F.sum(((F.size("pair_candidates") == 1) & (F.col("incomplete_pair_count") == 0)).cast("long")).alias("unambiguous"),
    F.sum(((F.size("pair_candidates") > 1) | (F.col("incomplete_pair_count") > 0)).cast("long")).alias("review"),
).first()

print(f"Reference rows: {reference_count:,} total; {valid_pin_rows:,} with a valid six-digit PIN; "
      f"{incomplete_pair_rows:,} with an incomplete state/district pair")
print(f"Lookup: {lookup_summary['pins']:,} distinct PINs -> "
      f"{lookup_summary['no_complete_pair']:,} with no complete pair, "
      f"{lookup_summary['unambiguous']:,} unambiguous, {lookup_summary['review']:,} requiring review")

# COMMAND ----------

# DBTITLE 1,LEFT JOIN enrichment
# Cell 3 — Enrich facilities via LEFT JOIN on pincode_clean. Facility geography is never overwritten;
# postal candidates are not verified locations; spelling differences go to review, never auto-resolved.

match_status = (
    F.when(F.col("pincode_status") != "valid_format", "facility_pin_missing_or_invalid")
     .when(F.col("pair_candidates").isNull(), "reference_unmatched")
     .when(F.size("pair_candidates") == 0, "reference_incomplete")
     .when((F.size("pair_candidates") == 1) & (F.col("incomplete_pair_count") == 0), "unambiguous_pair")
     .otherwise("review_required")  # multiple pairs, or incomplete reference rows exist
)

df_geo = (
    df_facilities
    .join(postal_lookup, F.col("pincode_clean") == F.col("pin"), "left")
    .withColumn("postal_match_status", match_status)
    .withColumn("postal_pair_candidates", F.col("pair_candidates"))  # candidate pairs always preserved
    .withColumn("postal_state_candidate", F.when(F.col("postal_match_status") == "unambiguous_pair", F.col("pair_candidates").getItem(0).getField("state")))
    .withColumn("postal_district_candidate", F.when(F.col("postal_match_status") == "unambiguous_pair", F.col("pair_candidates").getItem(0).getField("district")))
    .withColumn(
        "postal_state_comparison",
        F.when(F.col("postal_match_status") != "unambiguous_pair", "no_comparable_pair")
         .when(is_missing(F.col("state_clean")), "facility_state_missing")
         .when(F.lower(F.trim(F.col("state_clean"))) == F.col("postal_state_candidate"), "exact_normalized_match")
         .otherwise("needs_alias_or_conflict_review"),  # difference in need of review — NOT a confirmed conflict
    )
    .drop("pin", "pair_candidates", "ref_row_count", "incomplete_pair_count")  # drop join helpers; no facility column is touched
)

print(f"Enriched: {df_geo.count():,} rows x {len(df_geo.columns)} columns "
      f"(facilities {len(df_facilities.columns)} -> +{len(df_geo.columns) - len(df_facilities.columns)}: "
      "postal_match_status, postal_pair_candidates, postal_state_candidate, postal_district_candidate, postal_state_comparison)")

# COMMAND ----------

# DBTITLE 1,Save facilities_geo_v2, verify & summarize
# Cell 4 — Save with error-if-exists, verify, and summarize. Neither input table is ever modified.

if spark.catalog.tableExists(GEO_OUTPUT_TABLE):
    raise ValueError(f"{GEO_OUTPUT_TABLE} already exists — stopping. Nothing was overwritten or appended.")

df_geo.write.format("delta").mode("error").saveAsTable(GEO_OUTPUT_TABLE)

df_geo_saved = spark.read.table(GEO_OUTPUT_TABLE)
facility_cols = df_facilities.columns  # all 89 input columns
rows_in, rows_saved = facility_count, df_geo_saved.count()
v1_rows = spark.read.table("vf.silver.facilities_geo_v1").count()  # v1 must remain preserved and untouched

lookup_pins = postal_lookup.count()
lookup_pins_distinct = postal_lookup.select("pin").distinct().count()

id_nulls = df_geo_saved.where(F.col("_source_row_id").isNull()).count()
id_distinct = df_geo_saved.select("_source_row_id").distinct().count()
ids_missing = df_facilities.select("_source_row_id").exceptAll(df_geo_saved.select("_source_row_id")).count()
ids_extra = df_geo_saved.select("_source_row_id").exceptAll(df_facilities.select("_source_row_id")).count()

vals_diff_a = df_facilities.select(*facility_cols).exceptAll(df_geo_saved.select(*facility_cols)).count()
vals_diff_b = df_geo_saved.select(*facility_cols).exceptAll(df_facilities.select(*facility_cols)).count()

checks = [
    ("Lookup has exactly one row per PIN", lookup_pins == lookup_pins_distinct, f"{lookup_pins:,} rows, {lookup_pins_distinct:,} distinct PINs"),
    ("Output row count equals input", rows_saved == rows_in, f"{rows_saved:,} vs {rows_in:,}"),
    ("_source_row_id unique and non-null", id_nulls == 0 and id_distinct == rows_saved, f"{id_distinct:,} distinct, {id_nulls} nulls"),
    ("Every input row ID appears exactly once", ids_missing == 0 and ids_extra == 0 and id_distinct == rows_in, f"missing {ids_missing}, extra {ids_extra}"),
    ("All existing facility-column values unchanged (EXCEPT ALL both directions)", vals_diff_a == 0 and vals_diff_b == 0, f"{vals_diff_a} / {vals_diff_b} differing rows"),
    ("v1 preserved (facilities_geo_v1 untouched)", v1_rows == rows_in, f"{v1_rows:,} rows in v1"),
]
display(spark.createDataFrame([(c, p, d) for c, p, d in checks], ["check", "passed", "detail"]))

print(f"\nSaved {GEO_OUTPUT_TABLE}: {rows_saved:,} rows x {len(df_geo_saved.columns)} columns")

print("\nPostal match status counts:")
display(df_geo_saved.groupBy("postal_match_status").count().orderBy(F.desc("count")))

print("\nPostal state vs facility state_clean comparison counts:")
display(df_geo_saved.groupBy("postal_state_comparison").count().orderBy(F.desc("count")))

# Up to 15 review examples (reference_incomplete rows pad if fewer than 15 review_required rows).
review_examples = df_geo_saved.where(F.col("postal_match_status") == "review_required").limit(15)
n_review = review_examples.count()
if n_review < 15:
    pad = df_geo_saved.where(F.col("postal_match_status") == "reference_incomplete").limit(15 - n_review)
    review_examples = review_examples.unionByName(pad)

print(f"\nReview examples ({n_review} review_required, padded with reference_incomplete to 15):")
display(
    review_examples.select(
        "name", "state_clean", "city_clean", "pincode_clean",
        "postal_match_status", "postal_pair_candidates",
    )
)

# v1 examples that contained a {"state":"na","district":"na"} candidate pair, shown with their v2
# outcome under the reference-only "na" rule. District aliases / historical boundaries are never auto-resolved.
df_geo_v1 = spark.read.table("vf.silver.facilities_geo_v1")  # read-only; v1 is preserved
na_pair = F.struct(F.lit("na").alias("state"), F.lit("na").alias("district"))
na_pair_ids = df_geo_v1.where(F.array_contains(F.col("postal_pair_candidates"), na_pair)).select("_source_row_id")
na_total = df_geo_saved.join(na_pair_ids, "_source_row_id").count()
na_pair_examples = df_geo_saved.join(na_pair_ids, "_source_row_id").limit(15)
print(f"\nFacilities whose v1 candidates contained an na/na pair: {na_total:,} (up to 15 shown, with their v2 status)")
display(
    na_pair_examples.select(
        "name", "state_clean", "city_clean", "pincode_clean",
        "postal_match_status", "postal_pair_candidates",
    )
)

# COMMAND ----------

# DBTITLE 1,v3 — state normalization (single explicit alias)
# Cell 5 — Build facilities_geo_v3 columns. Reads vf.silver.facilities_geo_v2 only.
# ONE explicit alias: formatted exact "tamilnadu" -> "tamil nadu". NO other geographic mappings:
# cities (Thane/Mumbai/Chennai/Hyderabad...) are NOT mapped to states, Andhra Pradesh is NOT replaced
# with Telangana, and historical names / district boundaries / ambiguous PINs are NOT resolved.
# postal_match_status, all postal candidates, and every existing column are unchanged; missing
# states are never filled from postal candidates.

GEO_V2_TABLE = "vf.silver.facilities_geo_v2"
GEO_V3_TABLE = "vf.silver.facilities_geo_v3"

df_geo_v2 = spark.read.table(GEO_V2_TABLE)

state_formatted = F.lower(F.trim(F.regexp_replace(F.col("state_clean"), "\\s+", " ")))
is_tamilnadu = state_formatted == "tamilnadu"  # exact match on the FORMATTED value
df_geo_v3 = (
    df_geo_v2
    .withColumn(
        "state_normalized",
        F.when(is_missing(F.col("state_clean")), F.lit(None).cast("string"))  # missing stays missing
         .when(is_tamilnadu, F.lit("tamil nadu"))  # the single explicit alias
         .otherwise(state_formatted),
    )
    .withColumn(
        "state_normalization_method",
        F.when(is_missing(F.col("state_clean")), "missing")
         .when(is_tamilnadu, "explicit_alias")
         .otherwise("formatting_only"),
    )
    .withColumn(
        # same categories as v2, comparing the two normalized values (postal_state_candidate is
        # already lowercase/trimmed/collapsed from the reference normalization in Cell 2)
        "postal_state_comparison_v3",
        F.when(F.col("postal_match_status") != "unambiguous_pair", "no_comparable_pair")
         .when(F.col("state_normalized").isNull(), "facility_state_missing")
         .when(F.col("state_normalized") == F.col("postal_state_candidate"), "exact_normalized_match")
         .otherwise("needs_alias_or_conflict_review"),
    )
)

print(f"v3 build: {df_geo_v3.count():,} rows x {len(df_geo_v3.columns)} columns "
      f"(v2 {len(df_geo_v2.columns)} -> +3: state_normalized, state_normalization_method, postal_state_comparison_v3)")
print("\nNormalization method counts:")
display(df_geo_v3.groupBy("state_normalization_method").count().orderBy(F.desc("count")))

# Alias-change count and examples — ALL rows carrying the alias, not only the v2 disagreement group.
alias_rows = df_geo_v3.where(F.col("state_normalization_method") == "explicit_alias")
alias_total = alias_rows.count()
alias_in_disagreement = alias_rows.where(F.col("postal_state_comparison") == "needs_alias_or_conflict_review").count()
print(f"\nRows carrying the exact alias 'tamilnadu': {alias_total:,} "
      f"({alias_in_disagreement} of them were in the v2 needs_alias_or_conflict_review group; review baseline was 19)")
print("Up to 15 alias examples (old vs new comparison):")
display(
    alias_rows.select(
        "name", "state_clean", "pincode_clean", "postal_state_candidate",
        "postal_state_comparison", "postal_state_comparison_v3",
    ).limit(15)
)

# COMMAND ----------

# DBTITLE 1,Save facilities_geo_v3, verify preservation
# Cell 6 — Save v3 with error-if-exists (v2 and every input preserved); verify nothing existing changed.

if spark.catalog.tableExists(GEO_V3_TABLE):
    raise ValueError(f"{GEO_V3_TABLE} already exists — stopping. Nothing was overwritten or appended.")

df_geo_v3.write.format("delta").mode("error").saveAsTable(GEO_V3_TABLE)

df_geo_v3_saved = spark.read.table(GEO_V3_TABLE)
v2_cols = df_geo_v2.columns  # all 94 existing columns
rows_v2, rows_v3 = df_geo_v2.count(), df_geo_v3_saved.count()

id_nulls = df_geo_v3_saved.where(F.col("_source_row_id").isNull()).count()
id_distinct = df_geo_v3_saved.select("_source_row_id").distinct().count()
ids_missing = df_geo_v2.select("_source_row_id").exceptAll(df_geo_v3_saved.select("_source_row_id")).count()
ids_extra = df_geo_v3_saved.select("_source_row_id").exceptAll(df_geo_v2.select("_source_row_id")).count()

vals_diff_a = df_geo_v2.select(*v2_cols).exceptAll(df_geo_v3_saved.select(*v2_cols)).count()
vals_diff_b = df_geo_v3_saved.select(*v2_cols).exceptAll(df_geo_v2.select(*v2_cols)).count()

checks = [
    ("Row count unchanged from v2", rows_v3 == rows_v2, f"{rows_v3:,} vs {rows_v2:,}"),
    ("_source_row_id unique and non-null", id_nulls == 0 and id_distinct == rows_v3, f"{id_distinct:,} distinct, {id_nulls} nulls"),
    ("Every v2 row ID appears exactly once", ids_missing == 0 and ids_extra == 0, f"missing {ids_missing}, extra {ids_extra}"),
    ("All existing column values unchanged (EXCEPT ALL both directions)", vals_diff_a == 0 and vals_diff_b == 0, f"{vals_diff_a} / {vals_diff_b} differing rows"),
    ("v2 preserved (facilities_geo_v2 read intact)", rows_v2 == 10_088, f"{rows_v2:,} rows in v2"),
]
display(spark.createDataFrame([(c, p, d) for c, p, d in checks], ["check", "passed", "detail"]))

print(f"\nSaved {GEO_V3_TABLE}: {rows_v3:,} rows x {len(df_geo_v3_saved.columns)} columns")

# COMMAND ----------

# DBTITLE 1,v3 comparison results & transitions
# Cell 7 — v3 comparison counts, old-vs-new transitions, and the tamilnadu baseline (observed, never forced).

print("New state-comparison counts (postal_state_comparison_v3):")
display(df_geo_v3_saved.groupBy("postal_state_comparison_v3").count().orderBy(F.desc("count")))

print("\nOld (v2) -> new (v3) comparison transitions, all rows:")
display(
    df_geo_v3_saved.groupBy("postal_state_comparison", "postal_state_comparison_v3")
    .count().orderBy(F.desc("count"))
)

# Baseline: the earlier review reported 19 (tamilnadu, tamil nadu) disagreement rows. Report what
# actually happened to alias-carrying rows — no result is forced.
tamil = df_geo_v3_saved.where(F.col("state_normalization_method") == "explicit_alias")
print(f"\nAlias-carrying rows: {tamil.count():,}; their old -> new transitions:")
display(tamil.groupBy("postal_state_comparison", "postal_state_comparison_v3").count().orderBy(F.desc("count")))

resolved = tamil.where(
    (F.col("postal_state_comparison") == "needs_alias_or_conflict_review")
    & (F.col("postal_state_comparison_v3") == "exact_normalized_match")
).count()
print(f"Baseline comparison: the review found 19 tamilnadu/tamil nadu disagreements; "
      f"{resolved} alias-carrying rows moved to exact_normalized_match in v3 (observed, not forced). "
      f"Alias rows outside the disagreement group keep their v2 comparison category.")