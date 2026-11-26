# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 06 — Dialysis pilot sample (Jaipur city-string pilot, read-only)
# MAGIC Reads `vf.silver.facilities_current_v1` and LEFT JOINs `vf.silver.facilities_review_queue_v1` by `_source_row_id` to attach `review_priority` / `review_reason_codes` (verified not to multiply rows). Pilot filter: `city_clean`, after trim and lowercase, equals "jaipur" — this is a CITY-STRING PILOT FILTER, not verified geographic inclusion. Mentions of "dialysis" are CANDIDATE mentions, not confirmed services. No tables are written.

# COMMAND ----------

# DBTITLE 1,Join review flags & apply Jaipur pilot filter
# Cell 1 — LEFT JOIN review flags onto current records; verify the join does not multiply rows;
# then apply the Jaipur city-string pilot filter (NOT verified geographic inclusion).

from pyspark.sql import functions as F

CURRENT_VIEW = "vf.silver.facilities_current_v1"
QUEUE_VIEW = "vf.silver.facilities_review_queue_v1"

df_current = spark.table(CURRENT_VIEW)
df_queue = spark.table(QUEUE_VIEW)

df_joined = df_current.alias("c").join(
    df_queue.select("_source_row_id", "review_priority", "review_reason_codes").alias("q"),
    on="_source_row_id",
    how="left",
)

# The review queue is a subset of current records, so the LEFT JOIN must not multiply rows — verify.
n_current, n_joined = df_current.count(), df_joined.count()
ids_current = df_current.select("_source_row_id").distinct().count()
ids_joined = df_joined.select("_source_row_id").distinct().count()
join_ok = (n_joined == n_current) and (ids_joined == ids_current)
print(f"Join row-multiplication check: before {n_current:,} / after {n_joined:,} rows; "
      f"distinct IDs {ids_current:,} / {ids_joined:,} -> "
      f"{'no multiplication (OK)' if join_ok else 'ROW MULTIPLICATION — STOP'}")
if not join_ok:
    raise ValueError("LEFT JOIN multiplied rows — stopping.")

# City-string pilot filter: trim + lowercase of city_clean equals 'jaipur'.
df_pilot = df_joined.where(F.lower(F.trim(F.col("city_clean"))) == F.lit("jaipur"))
n_pilot = df_pilot.count()
n_flagged = df_pilot.where(F.col("review_priority").isNotNull()).count()

print(f"\nJaipur city-string pilot records: {n_pilot:,} of {n_current:,} current records")
print(f"With review flags: {n_flagged:,}; without review flags: {n_pilot - n_flagged:,}")
print("\nFlagged pilot records by review priority:")
display(df_pilot.groupBy("review_priority").count().orderBy(F.asc("review_priority")))

# COMMAND ----------

# DBTITLE 1,Dialysis mentions & evidence examples
# Cell 2 — Case-insensitive 'dialysis' mentions in description/capability/procedure/equipment/specialties.
# Mentions are CANDIDATES, not confirmed services. Evidence text is shown in full — never summarized.

from functools import reduce

MENTION_FIELDS = ["description_clean", "capability_clean", "procedure_clean", "equipment_clean", "specialties_clean"]
dtypes = dict(df_pilot.dtypes)  # actual types: 4 strings + 1 array<string> — handle both safely

def field_mentions(c):
    col = F.col(c)
    if dtypes[c].startswith("array<"):  # array<string>: any element mentions the term
        hit = F.exists(F.transform(col, lambda e: F.lower(e)), lambda e: e.contains("dialysis"))
    else:  # plain string columns
        hit = F.lower(col).contains("dialysis")
    return F.coalesce(hit, F.lit(False))

flag_by_field = {c: field_mentions(c) for c in MENTION_FIELDS}
mention_any = reduce(lambda a, b: a | b, flag_by_field.values())
hit_list = F.filter(F.array(*[F.when(v, F.lit(c)) for c, v in flag_by_field.items()]),
                    lambda x: x.isNotNull())

df_pilot_m = (df_pilot
    .withColumn("mention_fields", hit_list)
    .withColumn("mention_positive", mention_any))

n_pos = df_pilot_m.where(F.col("mention_positive")).count()
n_neg = n_pilot - n_pos
print(f"Dialysis mention-positive pilot records: {n_pos:,}; mention-negative: {n_neg:,} "
      "(case-insensitive substring across the five evidence fields — candidate mentions only, "
      "NOT confirmed services)")
print("\nMention field combinations (overlap across fields):")
display(df_pilot_m.groupBy("mention_fields").count().orderBy(F.desc("count")))

EXAMPLE_COLS = [
    "_source_row_id", "name_clean",
    "address_city", "address_stateOrRegion", "address_zipOrPostcode",  # original source city/state/PIN
    "postal_pair_candidates", "coordinate_status", "review_reason_codes",
    "description_clean", "capability_clean", "procedure_clean", "equipment_clean", "specialties_clean",
    "source_url_candidates", "recency_of_page_update_clean", "mention_fields",
]

print("\nUp to 15 mention-positive records (ordered by _source_row_id; full evidence text preserved):")
display(df_pilot_m.where(F.col("mention_positive")).select(*EXAMPLE_COLS)
        .orderBy(F.asc("_source_row_id")).limit(15))

if n_pos == 0:
    print("NO mention-positive records in the Jaipur city-string pilot — reported as-is, "
          "NOT silently expanding geography.")

print("\nUp to 5 mention-negative records (ordered by _source_row_id):")
display(df_pilot_m.where(~F.col("mention_positive")).select(*EXAMPLE_COLS)
        .orderBy(F.asc("_source_row_id")).limit(5))