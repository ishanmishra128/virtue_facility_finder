# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Facilities Cleaning — Step 1
# MAGIC
# MAGIC Source: `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities`
# MAGIC
# MAGIC Scope of this step only: `name`, `address_zipOrPostcode`, `numberDoctors`, `capacity`, `acceptsVolunteers`.
# MAGIC All original columns and rows are preserved; only the same nine `_clean` / `_status` columns are added.
# MAGIC A shared missing-value rule (SQL NULL, or trimmed + lowercased "", "null", "none", "n/a", "nan") is applied to all five fields, evaluated before any format validation, numeric parsing, or Boolean recognition. No tables are written.

# COMMAND ----------

# DBTITLE 1,Load source and inspect schema
# Cell 1 — Load the source table and inspect the schema of the columns we will clean.

SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
SOURCE_COLS = ["name", "address_zipOrPostcode", "numberDoctors", "capacity", "acceptsVolunteers"]

df_raw = spark.read.table(SOURCE_TABLE)

df_raw.select(*SOURCE_COLS).printSchema()  # all five source columns are STRING

input_row_count = df_raw.count()
print(f"Input rows: {input_row_count:,}")

# COMMAND ----------

# DBTITLE 1,Cleaning transforms
# Cell 2 — Cleaning transforms (built-in Spark functions only; no rows dropped, no other fields touched).
from pyspark.sql import functions as F

# Shared missing-value rule for ALL five fields, evaluated BEFORE format validation,
# numeric parsing, or Boolean recognition: SQL NULL, or the trimmed + lowercased value
# exactly equals "", "null", "none", "n/a", or "nan".
MISSING_MARKERS = ["", "null", "none", "n/a", "nan"]


def is_missing(c):
    """True when the value is SQL NULL or a literal missing marker after trim + lowercase."""
    return c.isNull() | F.lower(F.trim(c)).isin(MISSING_MARKERS)


# --- 1) name_clean: trim + collapse repeated whitespace; blank or marker -> NULL ----------------
name_trimmed = F.trim(F.regexp_replace(F.col("name"), "\\s+", " "))
# (missingness uses the shared is_missing() rule on the whitespace-collapsed, trimmed value)

df_clean = df_raw.withColumn(
    "name_clean",
    F.when(is_missing(name_trimmed), F.lit(None).cast("string")).otherwise(name_trimmed),
)

# --- 2) pincode_clean / pincode_status: accept only ^[1-9][0-9]{5}$ ----------------------------
pin_trimmed = F.trim(F.col("address_zipOrPostcode"))
pin_valid = pin_trimmed.rlike("^[1-9][0-9]{5}$")

df_clean = df_clean.withColumn(
    "pincode_status",
    F.when(is_missing(pin_trimmed), "missing")  # shared rule, BEFORE format validation
     .when(pin_valid, "valid_format")
     .otherwise("invalid_format"),
).withColumn(
    "pincode_clean",
    F.when(F.col("pincode_status") == "valid_format", pin_trimmed),  # missing/invalid -> NULL
)

# --- 3) number_doctors / capacity: nonnegative whole numbers representable as BIGINT -----------
def add_whole_number_columns(df, src_col, prefix):
    v_trim = F.trim(F.col(src_col))
    v_bigint = F.expr(f"try_cast(trim({src_col}) AS bigint)")  # safe parse; NULL when not representable
    parsed = v_bigint.isNotNull() & (v_bigint >= 0) & v_trim.rlike("^[0-9]+$")  # whole, nonnegative; 0 is valid
    df = df.withColumn(
        f"{prefix}_status",
        F.when(is_missing(v_trim), "missing")  # shared rule, BEFORE numeric parsing
         .when(parsed, "parsed")
         .otherwise("invalid"),
    ).withColumn(
        f"{prefix}_clean",
        F.when(parsed, v_bigint),  # missing/invalid -> NULL; no capping, no imputation
    )
    return df


df_clean = add_whole_number_columns(df_clean, "numberDoctors", "number_doctors")
df_clean = add_whole_number_columns(df_clean, "capacity", "capacity")

# --- 4) accepts_volunteers_clean / accepts_volunteers_status: trimmed true/false -> Boolean ----
vol_trim = F.trim(F.col("acceptsVolunteers"))
vol_lower = F.lower(vol_trim)

df_clean = df_clean.withColumn(
    "accepts_volunteers_status",
    F.when(is_missing(vol_trim), "missing")  # shared rule, BEFORE Boolean recognition
     .when(vol_lower == "true", "true")
     .when(vol_lower == "false", "false")   # "false" is a legitimate value
     .otherwise("unrecognized"),
).withColumn(
    "accepts_volunteers_clean",
    F.when(F.col("accepts_volunteers_status") == "true", F.lit(True))
     .when(F.col("accepts_volunteers_status") == "false", F.lit(False)),
)

added_cols = [c for c in df_clean.columns if c not in df_raw.columns]
print("Added columns:", added_cols)

# COMMAND ----------

# DBTITLE 1,Row and column counts
# Cell 3 — Input vs output row and column counts (rows must be fully preserved).
output_row_count = df_clean.count()

print(f"Input rows:  {input_row_count:,}")
print(f"Output rows: {output_row_count:,}")
print(f"Rows preserved (input == output): {input_row_count == output_row_count}")
print(f"Columns: {len(df_raw.columns)} input -> {len(df_clean.columns)} output ({len(df_clean.columns) - len(df_raw.columns)} added, 0 dropped)")

# COMMAND ----------

# DBTITLE 1,Batched status counts
# Cell 4 — Actual status counts (ONE batched aggregation) + comparison against the previous profile.

# field -> (status column, expected statuses)
STATUS_COLUMNS = {
    "pincode": ("pincode_status", ["missing", "valid_format", "invalid_format"]),
    "number_doctors": ("number_doctors_status", ["missing", "invalid", "parsed"]),
    "capacity": ("capacity_status", ["missing", "invalid", "parsed"]),
    "accepts_volunteers": ("accepts_volunteers_status", ["missing", "true", "false", "unrecognized"]),
}

# name has no status column; derive its two outcomes directly from name_clean.
agg_exprs = [
    F.sum(F.col("name_clean").isNull().cast("long")).alias("name__missing"),
    F.sum(F.col("name_clean").isNotNull().cast("long")).alias("name__retained"),
]
for field, (colname, statuses) in STATUS_COLUMNS.items():
    for s in statuses:
        agg_exprs.append(F.sum((F.col(colname) == s).cast("long")).alias(f"{field}__{s}"))

counts = df_clean.agg(*agg_exprs).first()  # single pass; statuses of a field sum to total rows

status_rows = [
    ("name", "missing", counts["name__missing"]),
    ("name", "retained", counts["name__retained"]),
]
for field, (colname, statuses) in STATUS_COLUMNS.items():
    for s in statuses:
        status_rows.append((field, s, counts[f"{field}__{s}"]))

actual_df = spark.createDataFrame(status_rows, ["field", "status", "actual_count"])

# Expected counts from the previous profile — reference only; actuals are computed fresh and never forced to match.
expected_df = spark.createDataFrame(
    [
        ("name", "missing", 65), ("name", "retained", 10023),
        ("pincode", "missing", 66), ("pincode", "valid_format", 9766), ("pincode", "invalid_format", 256),
        ("number_doctors", "missing", 6455), ("number_doctors", "invalid", 3), ("number_doctors", "parsed", 3630),
        ("capacity", "missing", 7568), ("capacity", "invalid", 3), ("capacity", "parsed", 2517),
        ("accepts_volunteers", "missing", 10046), ("accepts_volunteers", "true", 21),
        ("accepts_volunteers", "false", 7), ("accepts_volunteers", "unrecognized", 14),
    ],
    ["field", "status", "expected_count"],
)

comparison_df = (
    actual_df.join(expected_df, ["field", "status"], "full_outer")
    .withColumn("match", F.col("actual_count") == F.col("expected_count"))
    .orderBy("field", "status")
)

print("Updated status counts (actual):")
display(actual_df.orderBy("field", "status"))

print("\nActual vs expected counts:")
display(comparison_df)

# COMMAND ----------

# DBTITLE 1,Before/after examples
# Cell 5 — Up to 15 before/after examples (unique_id, field_name, raw_value, cleaned_value, status):
# literal missing markers, genuinely invalid values, and null/blank missings across ALL five fields.

def field_frame(field_name, raw, clean, status):
    return df_clean.select(
        F.col("unique_id"),
        F.lit(field_name).alias("field_name"),
        raw.alias("raw_value"),
        clean.cast("string").alias("cleaned_value"),
        status.alias("status"),
    )


long_fields = (
    field_frame(
        "name", F.col("name"), F.col("name_clean"),
        F.when(F.col("name_clean").isNull(), "missing").otherwise("retained"),
    )
    .unionByName(field_frame("pincode", F.col("address_zipOrPostcode"), F.col("pincode_clean"), F.col("pincode_status")))
    .unionByName(field_frame("number_doctors", F.col("numberDoctors"), F.col("number_doctors_clean"), F.col("number_doctors_status")))
    .unionByName(field_frame("capacity", F.col("capacity"), F.col("capacity_clean"), F.col("capacity_status")))
    .unionByName(field_frame("accepts_volunteers", F.col("acceptsVolunteers"), F.col("accepts_volunteers_clean"), F.col("accepts_volunteers_status")))
)

issues = long_fields.where(
    ~F.col("status").isin("retained", "valid_format", "parsed", "true", "false")  # any missing/invalid/unrecognized
)

invalid_examples = issues.where(
    F.col("status").isin("invalid", "invalid_format", "unrecognized")
).limit(6)

marker_examples = issues.where(
    (F.col("status") == "missing")
    & F.col("raw_value").isNotNull()
    & F.lower(F.trim(F.col("raw_value"))).isin("null", "none", "n/a", "nan")
).limit(5)

null_blank_examples = issues.where(
    (F.col("status") == "missing")
    & (F.col("raw_value").isNull() | (F.trim(F.col("raw_value")) == ""))
).limit(4)

print(f"Rows (long format) with a missing or invalid value: {issues.count():,} — showing up to 15 examples\n")
display(
    invalid_examples
    .unionByName(marker_examples)
    .unionByName(null_blank_examples)
)

# COMMAND ----------

# DBTITLE 1,Coordinate validation
# Cell 6 — Coordinate validation: extends the EXISTING df_clean with exactly one column, coordinate_status.
# latitude/longitude are already DOUBLE in the source. Nothing is reread; no coordinate value is
# changed, swapped, filled, or removed; no temp helper columns are created (helpers are plain Python vars).

COORD_MISSING_MARKERS = ["", "null", "none", "n/a", "nan"]


def coord_is_missing(c):
    """Shared missing rule; genuine NaN is excluded here so it lands in invalid_numeric."""
    return c.isNull() | (F.lower(F.trim(c.cast("string"))).isin(COORD_MISSING_MARKERS) & ~F.isnan(c))


def coord_is_bad_numeric(c):
    """NaN or +/- infinity (unparseable values are already NULL, i.e. missing)."""
    return F.isnan(c) | (c == float("inf")) | (c == float("-inf"))


any_missing = coord_is_missing(F.col("latitude")) | coord_is_missing(F.col("longitude"))
any_bad_num = coord_is_bad_numeric(F.col("latitude")) | coord_is_bad_numeric(F.col("longitude"))
both_global = F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)
both_india = F.col("latitude").between(6, 38) & F.col("longitude").between(68, 98)

df_coords = df_clean.withColumn(
    "coordinate_status",
    F.when(any_missing, "missing")  # 1) missing rule first (NULL or literal marker)
     .when(any_bad_num, "invalid_numeric")  # 2) NaN or +/- infinity
     .when(both_global,  # 3) finite and inside [-90,90] / [-180,180]
           F.when(both_india, "inside_india_screen")  # 5)
           .otherwise("outside_india_screen"))  # 4)
     .otherwise("outside_global_range"),
)

# --- Status counts: all five categories (zero counts included), one batched aggregation ---
COORD_STATUSES = ["missing", "invalid_numeric", "outside_global_range", "outside_india_screen", "inside_india_screen"]
coord_counts = df_coords.agg(
    *[F.sum((F.col("coordinate_status") == s).cast("long")).alias(s) for s in COORD_STATUSES]
).first()

# Expected counts from the previous profile — reference only; actuals are computed fresh, never forced.
COORD_EXPECTED = {"missing": 118, "invalid_numeric": 0, "outside_global_range": 0,
                  "outside_india_screen": 6, "inside_india_screen": 9964}

rows_before, cols_before = df_clean.count(), len(df_clean.columns)
rows_after, cols_after = df_coords.count(), len(df_coords.columns)
status_total = sum(coord_counts[s] for s in COORD_STATUSES)
null_status_rows = df_coords.where(F.col("coordinate_status").isNull()).count()

print(f"Input (df_clean):   {rows_before:,} rows / {cols_before} columns")
print(f"Output (df_coords): {rows_after:,} rows / {cols_after} columns (added: coordinate_status only)")
print(f"Rows preserved: {rows_before == rows_after}")
print(f"Status counts sum to total: {status_total == rows_after} ({status_total:,} vs {rows_after:,})")
print(f"Rows with NULL coordinate_status: {null_status_rows}")

display(
    spark.createDataFrame(
        [(s, coord_counts[s], COORD_EXPECTED[s], coord_counts[s] == COORD_EXPECTED[s]) for s in COORD_STATUSES],
        ["coordinate_status", "actual_count", "expected_count", "match"],
    )
)

# NOTE: the India rectangle (lat [6,38], lon [68,98]) is a coarse ANOMALY SCREEN, not proof that a
# facility is in India or at its claimed address — it only flags coordinates that could not plausibly
# be in India; passing it says nothing about address accuracy.

# COMMAND ----------

# DBTITLE 1,Coordinate & prior-step examples
# Cell 7 — Examples: (a) up to 10 invalid / outside-screen coordinates; (b) five before/after rows
# from the Step-1 cleaning (PIN, doctors, capacity, volunteers — reusing the long format from Cell 5).

coord_example_cols = ["unique_id", "name", "address_city", "address_stateOrRegion", "latitude", "longitude", "coordinate_status"]

bad_coords = df_coords.where(
    F.col("coordinate_status").isin("invalid_numeric", "outside_global_range", "outside_india_screen")
)
print(f"Rows with invalid or outside-screen coordinates: {bad_coords.count():,} (showing up to 10)\n")
display(bad_coords.limit(10).select(*coord_example_cols))

# Five before/after examples from the previous cleaning step:
# 3 genuinely invalid values, then 2 literal missing markers.
prev_fields = issues.where(F.col("field_name") != "name")  # PIN / doctors / capacity / volunteers only

prev_invalid = prev_fields.where(
    F.col("status").isin("invalid", "invalid_format", "unrecognized")
).limit(3)

prev_markers = prev_fields.where(
    (F.col("status") == "missing")
    & F.col("raw_value").isNotNull()
    & F.lower(F.trim(F.col("raw_value"))).isin("null", "none", "n/a", "nan")
).limit(2)

print("\nStep-1 before/after examples (raw_value -> cleaned_value):")
display(prev_invalid.unionByName(prev_markers))

# COMMAND ----------

# DBTITLE 1,Parse & clean array fields
# Cell 8 — specialties & affiliationTypeIds: parse as ARRAY<STRING> (permissive from_json) and clean.
# Extends df_coords only; all rows and columns preserved; source_urls, source_ids, source_types untouched.
# Cleaned arrays: elements trimmed, NULL/blank/marker elements dropped, exact duplicates removed
# (case preserved, first occurrence kept). No service inference — a repeated specialty is not stronger evidence.

ARRAY_FIELDS = ["specialties", "affiliationTypeIds"]
ARRAY_STATUSES = ["missing_input", "not_parseable_as_string_array", "parsed_empty", "parsed_nonempty"]
ARRAY_DROP_MARKERS = ["null", "none", "n/a", "nan"]  # case-insensitive exact, per element


def add_array_clean_columns(df, src_col):
    v_trim = F.trim(F.col(src_col))
    parsed = F.from_json(v_trim, "array<string>")  # PERMISSIVE (default mode): malformed input -> NULL

    usable = F.filter(  # trim each element, then drop NULLs, blanks, and literal markers
        F.transform(parsed, lambda x: F.trim(x)),
        lambda x: x.isNotNull() & (x != "") & ~F.lower(x).isin(ARRAY_DROP_MARKERS),
    )
    deduped = F.array_distinct(usable)  # exact trimmed values; case preserved; order kept

    status = (
        F.when(is_missing(v_trim), "missing_input")  # shared missing rule, BEFORE parsing
         .when(parsed.isNull(), "not_parseable_as_string_array")
         .when(F.size(parsed) == 0, "parsed_empty")
         .otherwise("parsed_nonempty")
    )
    clean = F.when(status.isin("parsed_empty", "parsed_nonempty"), deduped)  # missing/unparseable -> NULL; parsed with no usable elements -> []

    return df.withColumn(f"{src_col}_status", status).withColumn(f"{src_col}_clean", clean)


df_arrays = df_coords
for src_col in ARRAY_FIELDS:
    df_arrays = add_array_clean_columns(df_arrays, src_col)

# --- Status counts + integrity checks (one batched aggregation) ---
agg_exprs = [
    F.sum((F.col(f"{f}_status") == s).cast("long")).alias(f"{f}__{s}")
    for f in ARRAY_FIELDS
    for s in ARRAY_STATUSES
]
counts = df_arrays.agg(*agg_exprs).first()

status_rows = [(f, s, counts[f"{f}__{s}"]) for f in ARRAY_FIELDS for s in ARRAY_STATUSES]
display(spark.createDataFrame(status_rows, ["field", "status", "row_count"]))

rows_in, rows_out = df_coords.count(), df_arrays.count()
print(f"Input rows: {rows_in:,}  Output rows: {rows_out:,}  Unchanged: {rows_in == rows_out}")
print(f"Columns: {len(df_coords.columns)} -> {len(df_arrays.columns)} (added four: two _status + two _clean)")
for f in ARRAY_FIELDS:
    total = sum(counts[f"{f}__{s}"] for s in ARRAY_STATUSES)
    print(f"{f}: status counts sum to total rows: {total == rows_out} ({total:,} vs {rows_out:,})")

# COMMAND ----------

# DBTITLE 1,Array cleaning stats & examples
# Cell 9 — Duplicate-removal / empty-after-cleaning counts, and 10 raw-versus-cleaned examples.

def usable_expr(src_col):
    """Rebuilds the post-filter, pre-dedup element list as an expression (no helper columns)."""
    parsed = F.from_json(F.trim(F.col(src_col)), "array<string>")
    return F.filter(
        F.transform(parsed, lambda x: F.trim(x)),
        lambda x: x.isNotNull() & (x != "") & ~F.lower(x).isin(ARRAY_DROP_MARKERS),
    )


def parsed_ok(src_col):
    return F.col(f"{src_col}_status").isin("parsed_empty", "parsed_nonempty")


cleaning_stats = []
for src_col in ARRAY_FIELDS:
    usable = usable_expr(src_col)
    row = df_arrays.agg(
        # duplicates removed: dedup shrank the usable element list
        F.sum((parsed_ok(src_col) & (F.size(F.array_distinct(usable)) < F.size(usable))).cast("long")).alias("dupes_removed"),
        # parsed input whose cleaned array ended up empty ([])
        F.sum((parsed_ok(src_col) & (F.size(F.col(f"{src_col}_clean")) == 0)).cast("long")).alias("empty_after"),
    ).first()
    cleaning_stats.append((src_col, row["dupes_removed"], row["empty_after"]))

display(spark.createDataFrame(cleaning_stats, ["field", "arrays_with_duplicates_removed", "empty_after_cleaning"]))


# 10 raw-versus-cleaned examples: rows where cleaning changed the parsed content first,
# then any other successfully parsed rows, so the examples always show real before/after pairs.
def changed(src_col):
    parsed_size = F.size(F.from_json(F.trim(F.col(src_col)), "array<string>"))
    return (F.col(f"{src_col}_status") == "parsed_nonempty") & (F.size(F.col(f"{src_col}_clean")) < parsed_size)


any_changed = changed("specialties") | changed("affiliationTypeIds")

examples = df_arrays.where(any_changed).limit(10)
n_examples = examples.count()
if n_examples < 10:
    extras = df_arrays.where(
        ~any_changed
        & ((F.col("specialties_status") == "parsed_nonempty") | (F.col("affiliationTypeIds_status") == "parsed_nonempty"))
    ).limit(10 - n_examples)
    examples = examples.unionByName(extras)

print(f"Showing {examples.count()} raw-versus-cleaned examples\n")
display(
    examples.select(
        "unique_id",
        "specialties", "specialties_clean", "specialties_status",
        "affiliationTypeIds", "affiliationTypeIds_clean", "affiliationTypeIds_status",
    )
)

# COMMAND ----------

# DBTITLE 1,Parse source_urls & extract HTTP(S) candidates
# Cell 10 — source_urls: permissive ARRAY<STRING> parse + HTTP(S) candidate extraction.
# Extends df_arrays only; source_ids and source_types are NOT modified, and no positional
# correspondence between them and the URLs is assumed. No pages are fetched — URL syntax
# is not verification of anything.

URL_STATUSES = ["missing_input", "not_parseable_as_string_array", "parsed_empty", "parsed_nonempty"]
URL_MARKER_ELEMENTS = ["null", "none", "n/a", "nan"]  # case-insensitive exact, per element

src_trim = F.trim(F.col("source_urls"))
urls_parsed = F.from_json(src_trim, "array<string>")  # PERMISSIVE: order, duplicates, NULL elements preserved

urls_status = (
    F.when(is_missing(src_trim), "missing_input")  # shared missing rule, BEFORE parsing
     .when(urls_parsed.isNull(), "not_parseable_as_string_array")
     .when(F.size(urls_parsed) == 0, "parsed_empty")
     .otherwise("parsed_nonempty")
)

# Candidates: distinct trimmed HTTP(S) URLs with nonempty hosts. Regex screening is safe by design —
# it cannot fail on malformed URLs; it simply leaves them out. URL case and paths are preserved.
urls_candidates = F.array_distinct(
    F.filter(
        F.transform(urls_parsed, lambda x: F.trim(x)),
        lambda x: x.isNotNull() & (x != "")
        & ~F.lower(x).isin(URL_MARKER_ELEMENTS)
        & F.lower(x).rlike("^https?://[^/?#\\s]+"),
    )
)

parsed_ok = urls_status.isin("parsed_empty", "parsed_nonempty")

df_urls = (
    df_arrays
    .withColumn("source_urls_parsed", urls_parsed)  # NULL for missing/unparseable input
    .withColumn("source_urls_status", urls_status)
    .withColumn("source_url_candidates", F.when(parsed_ok, urls_candidates))
    .withColumn("source_url_candidate_count", F.when(parsed_ok, F.size(urls_candidates)))  # 0 when parsed but no candidates
)

# --- Parsing status counts + integrity checks (one batched aggregation) ---
url_counts = df_urls.agg(
    *[F.sum((F.col("source_urls_status") == s).cast("long")).alias(s) for s in URL_STATUSES]
).first()

display(spark.createDataFrame([(s, url_counts[s]) for s in URL_STATUSES], ["source_urls_status", "row_count"]))

rows_in, rows_out = df_arrays.count(), df_urls.count()
status_total = sum(url_counts[s] for s in URL_STATUSES)
print(f"Input rows: {rows_in:,}  Output rows: {rows_out:,}  Unchanged: {rows_in == rows_out}")
print(f"Columns: {len(df_arrays.columns)} -> {len(df_urls.columns)} (added four: parsed, status, candidates, count)")
print(f"Status counts sum to total rows: {status_total == rows_out} ({status_total:,} vs {rows_out:,})")

# COMMAND ----------

# DBTITLE 1,URL candidate counts & examples
# Cell 11 — URL candidate categories + ten raw/parsed/candidate examples.

cc = F.col("source_url_candidate_count")

candidate_counts = df_urls.agg(
    F.sum(cc.isNull().cast("long")).alias("unknown_count"),  # missing/unparseable input
    F.sum((cc == 0).cast("long")).alias("zero_count"),       # parsed, but no usable candidates
    F.sum((cc >= 1).cast("long")).alias("at_least_one_count"),
).first()

category_rows = [
    ("unknown (missing/unparseable input)", candidate_counts["unknown_count"]),
    ("zero candidates (parsed)", candidate_counts["zero_count"]),
    ("at least one candidate", candidate_counts["at_least_one_count"]),
]
display(spark.createDataFrame(category_rows, ["candidate_category", "row_count"]))

category_total = sum(r[1] for r in category_rows)
print(f"Candidate categories sum to total rows: {category_total == rows_out} ({category_total:,} vs {rows_out:,})")

# Examples: prioritize arrays containing blank elements or no candidates, then pad with usable rows.
has_blank_element = F.size(
    F.filter(F.col("source_urls_parsed"), lambda x: x.isNull() | (F.trim(x) == ""))
) > 0
no_candidates = cc == 0
priority = has_blank_element | no_candidates

examples = df_urls.where(priority).limit(10)
n_examples = examples.count()
if n_examples < 10:
    extras = df_urls.where(~priority & (cc >= 1)).limit(10 - n_examples)
    examples = examples.unionByName(extras)

print(f"\nShowing {examples.count()} raw/parsed/candidate examples (blank-element / no-candidate rows first)")
display(
    examples.select(
        "unique_id",
        "source_urls", "source_urls_parsed", "source_urls_status",
        "source_url_candidates", "source_url_candidate_count",
    )
)

# COMMAND ----------

# DBTITLE 1,Date fields: parse & classify
# Cell 12 — recency_of_page_update & post_metrics_most_recent_social_media_post_date: DATE cleaning.
# Extends df_urls only. Raw values preserved. No days are invented for partial dates, no relative
# dates are interpreted, and social-post dates are not used to verify anything (incl. service availability).

DATE_FIELDS = ["recency_of_page_update", "post_metrics_most_recent_social_media_post_date"]
DATE_STATUSES = ["missing", "parsed_date", "future_date", "year_only", "other_unparsed_or_partial"]

# Anchor is fixed ONCE from current_date() so every row and check in this step compares against the same value.
as_of_date = spark.range(1).select(F.current_date().alias("d")).first()["d"]
as_of = F.lit(as_of_date)
print(f"as_of_date (set once from current_date()): {as_of_date}")


def add_date_clean_columns(df, src_col):
    v_trim = F.trim(F.col(src_col))
    v_year_only = v_trim.rlike("^[0-9]{4}$")  # exactly four digits
    v_date = F.expr(f"try_to_date(trim({src_col}), 'yyyy-MM-dd')")  # safe parse: NULL for anything invalid or unparseable

    status = (
        F.when(is_missing(v_trim), "missing")  # shared missing rule, BEFORE date parsing
         .when(v_year_only, "year_only")
         .when(v_date.isNotNull() & (v_date <= as_of), "parsed_date")  # exact yyyy-MM-dd, valid, not after as_of
         .when(v_date.isNotNull() & (v_date > as_of), "future_date")    # valid ISO date after as_of
         .otherwise("other_unparsed_or_partial")
    )
    clean = F.when(status == "parsed_date", v_date)  # ONLY parsed_date populates the DATE column
    return df.withColumn(f"{src_col}_status", status).withColumn(f"{src_col}_clean", clean)


df_dates = df_urls
for src_col in DATE_FIELDS:
    df_dates = add_date_clean_columns(df_dates, src_col)

# --- Status counts (zero-count categories included) + integrity checks (one batched aggregation) ---
date_counts = df_dates.agg(
    *[F.sum((F.col(f"{f}_status") == s).cast("long")).alias(f"{f}__{s}")
      for f in DATE_FIELDS for s in DATE_STATUSES]
).first()

display(spark.createDataFrame(
    [(f, s, date_counts[f"{f}__{s}"]) for f in DATE_FIELDS for s in DATE_STATUSES],
    ["field", "status", "row_count"],
))

rows_in, rows_out = df_urls.count(), df_dates.count()
print(f"Input rows: {rows_in:,}  Output rows: {rows_out:,}  Unchanged: {rows_in == rows_out}")
print(f"Columns: {len(df_urls.columns)} -> {len(df_dates.columns)} (added four: two _status, two _clean)")
for f in DATE_FIELDS:
    total = sum(date_counts[f"{f}__{s}"] for s in DATE_STATUSES)
    print(f"{f}: statuses reconcile to total rows: {total == rows_out} ({total:,} vs {rows_out:,})")

# Cleaned dates must exist only for parsed_date rows and can never be after as_of_date.
chk = df_dates.agg(
    *[
        e
        for f in DATE_FIELDS
        for e in (
            F.sum((F.col(f"{f}_clean") > as_of).cast("long")).alias(f"{f}__after_asof"),
            F.sum((F.col(f"{f}_clean").isNotNull() & (F.col(f"{f}_status") != "parsed_date")).cast("long")).alias(f"{f}__bad_populated"),
        )
    ]
).first()
for f in DATE_FIELDS:
    print(f"{f}: cleaned dates after as_of_date: {chk[f'{f}__after_asof']}; populated for non-parsed_date rows: {chk[f'{f}__bad_populated']}")

# COMMAND ----------

# DBTITLE 1,Future-date & partial-date examples
# Cell 13 — All future-date examples, then up to 10 partial/unparsed examples.

example_cols = ["unique_id", "name"] + [c for f in DATE_FIELDS for c in (f, f"{f}_status", f"{f}_clean")]

future_rows = df_dates.where(
    (F.col(f"{DATE_FIELDS[0]}_status") == "future_date") | (F.col(f"{DATE_FIELDS[1]}_status") == "future_date")
)
print(f"Rows with a future_date value: {future_rows.count():,} (showing all)\n")
display(future_rows.select(*example_cols))

partial_cond = (F.col(f"{DATE_FIELDS[0]}_status") == "other_unparsed_or_partial") | (
    F.col(f"{DATE_FIELDS[1]}_status") == "other_unparsed_or_partial"
)
partial_examples = df_dates.where(partial_cond).limit(10)
n_examples = partial_examples.count()
if n_examples < 10:  # pad with year_only rows (also partial), never duplicating the rows above
    year_only_pad = df_dates.where(
        ~partial_cond
        & ((F.col(f"{DATE_FIELDS[0]}_status") == "year_only") | (F.col(f"{DATE_FIELDS[1]}_status") == "year_only"))
    ).limit(10 - n_examples)
    partial_examples = partial_examples.unionByName(year_only_pad)

print(f"\nPartial/unparsed examples (other_unparsed_or_partial first, padded with year_only): {partial_examples.count()}")
display(partial_examples.select(*example_cols))

# COMMAND ----------

# DBTITLE 1,Remaining basic fields
# Cell 14 — Remaining basic fields. Extends df_dates (73 columns) only; no rows or prior columns changed.
# No geography inference or alias merging; no service inference from any free-text field.

current_year = as_of_date.year  # same fixed anchor as the date step

# --- yearEstablished: whole years 1..current_year (INT); missing / invalid / parsed ----------------
ye_trim = F.trim(F.col("yearEstablished"))
ye_num = F.expr("try_cast(trim(yearEstablished) AS int)")
ye_ok = ye_num.isNotNull() & ye_trim.rlike("^[0-9]+$") & (ye_num >= 1) & (ye_num <= current_year)

df_basic = df_dates.withColumn(
    "year_established_status",
    F.when(is_missing(ye_trim), "missing").when(ye_ok, "parsed").otherwise("invalid"),
).withColumn(
    "year_established_clean",
    F.when(F.col("year_established_status") == "parsed", ye_num),  # NULL for missing/invalid
)


# --- simple text fields: trim (+ collapse whitespace / lowercase / exact corrections), markers -> NULL
def clean_simple_text(src_col, collapse_ws=False, lowercase=False, exact_map=None):
    raw_trim = F.trim(F.col(src_col))
    v = F.trim(F.regexp_replace(raw_trim, "\\s+", " ")) if collapse_ws else raw_trim
    if lowercase:
        v = F.lower(v)
    for old, new in (exact_map or {}).items():  # exact post-normalization corrections only (farmacy -> pharmacy)
        v = F.when(v == old, F.lit(new)).otherwise(v)
    return F.when(is_missing(raw_trim), F.lit(None).cast("string")).otherwise(v)


df_basic = (
    df_basic
    .withColumn("facility_type_clean", clean_simple_text("facilityTypeId", lowercase=True, exact_map={"farmacy": "pharmacy"}))
    .withColumn("operator_type_clean", clean_simple_text("operatorTypeId", lowercase=True))
    .withColumn("city_clean", clean_simple_text("address_city", collapse_ws=True))
    .withColumn("state_clean", clean_simple_text("address_stateOrRegion", collapse_ws=True))
    .withColumn("description_clean", clean_simple_text("description"))
    .withColumn("capability_clean", clean_simple_text("capability"))
    .withColumn("procedure_clean", clean_simple_text("procedure"))
    .withColumn("equipment_clean", clean_simple_text("equipment"))
)

print(f"Columns: {len(df_dates.columns)} -> {len(df_basic.columns)} (added 10: year pair + 8 text _clean columns)")

# COMMAND ----------

# DBTITLE 1,Review flags
# Cell 15 — Review flags: non-null quality_issue_codes built from existing + new status columns.
# Outlier codes are PROVISIONAL flags, not invalidations. Ordinary missingness (doctors, capacity,
# founding year) is NOT corruption and is not flagged. An EMPTY array does NOT mean the facility is verified.


def date_status_any(statuses):
    return F.col(f"{DATE_FIELDS[0]}_status").isin(statuses) | F.col(f"{DATE_FIELDS[1]}_status").isin(statuses)


ISSUE_RULES = [
    ("missing_name", F.col("name_clean").isNull()),
    ("invalid_or_missing_pin", F.col("pincode_status") != "valid_format"),
    ("coordinates_missing_or_failed_india_screen", F.col("coordinate_status") != "inside_india_screen"),
    ("invalid_number_doctors", F.col("number_doctors_status") == "invalid"),
    ("invalid_capacity", F.col("capacity_status") == "invalid"),
    ("provisional_outlier_doctors_over_1000", F.col("number_doctors_clean") > 1000),
    ("provisional_outlier_capacity_over_5000", F.col("capacity_clean") > 5000),
    ("unrecognized_volunteers", F.col("accepts_volunteers_status") == "unrecognized"),
    ("specialties_parse_failure", F.col("specialties_status") == "not_parseable_as_string_array"),
    ("affiliation_types_parse_failure", F.col("affiliationTypeIds_status") == "not_parseable_as_string_array"),
    ("source_urls_parse_failure", F.col("source_urls_status") == "not_parseable_as_string_array"),
    ("missing_source_url_input", F.col("source_urls_status") == "missing_input"),
    ("zero_url_candidates", F.col("source_url_candidate_count") == 0),
    ("future_date", date_status_any(["future_date"])),
    ("partial_or_unparsed_date", date_status_any(["year_only", "other_unparsed_or_partial"])),
    ("invalid_founding_year", F.col("year_established_status") == "invalid"),
]

df_flagged = (
    df_basic
    .withColumn(
        "quality_issue_codes",
        F.filter(F.array(*[F.when(cond, code) for code, cond in ISSUE_RULES]), lambda c: c.isNotNull()),
    )
    .withColumn("quality_issue_count", F.size("quality_issue_codes"))
)

print(f"Columns: {len(df_basic.columns)} -> {len(df_flagged.columns)} (added quality_issue_codes, quality_issue_count)")

# COMMAND ----------

# DBTITLE 1,Save facilities_clean_v1
# Cell 16 — Save the first version as a managed Delta table (error-if-exists; never overwrite, append,
# or modify the source table).

target_catalog, target_schema, output_table = "vf", "silver", "facilities_clean_v1"
full_table = f"{target_catalog}.{target_schema}.{output_table}"

available_catalogs = [c.name for c in spark.catalog.listCatalogs()]
if target_catalog not in available_catalogs:
    raise ValueError(f"Catalog '{target_catalog}' is unavailable — stopping. Please provide an existing writable catalog.")

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
except Exception as e:
    raise PermissionError(f"Cannot create schema {target_catalog}.{target_schema} — stopping. Detail: {e}")

if spark.catalog.tableExists(full_table):
    raise ValueError(f"Target table {full_table} already exists — stopping. Nothing was overwritten or appended.")

# Fixed provenance metadata: one timestamp for the whole run, source pointer, rule version.
_processed_at = spark.range(1).select(F.current_timestamp().alias("t")).first()["t"]

df_final = (
    df_flagged
    .withColumn("_source_row_id", F.expr("uuid()"))
    .withColumn("_processed_at", F.lit(_processed_at))
    .withColumn("_source_table", F.lit(SOURCE_TABLE))
    .withColumn("_cleaning_rule_version", F.lit("v1"))
)

df_final.write.format("delta").mode("error").saveAsTable(full_table)

print(f"Saved {full_table}: {df_final.count():,} rows x {len(df_final.columns)} columns (rule version v1, processed at {_processed_at})")

# COMMAND ----------

# DBTITLE 1,Verify & summarize
# Cell 17 — Verify the saved table and summarize. Reads the TARGET only; the source is never modified.

df_saved = spark.read.table(full_table)
original_cols = df_raw.columns  # the 51 source columns

rows_in, rows_saved = df_raw.count(), df_saved.count()

missing_cols = [c for c in original_cols if c not in df_saved.columns]
src_types, saved_types = dict(df_raw.dtypes), dict(df_saved.dtypes)
type_diffs = [(c, src_types[c], saved_types.get(c)) for c in original_cols if c in df_saved.columns and saved_types[c] != src_types[c]]

diff_input_vs_saved = df_raw.select(*original_cols).exceptAll(df_saved.select(*original_cols)).count()
diff_saved_vs_input = df_saved.select(*original_cols).exceptAll(df_raw.select(*original_cols)).count()

row_id_nulls = df_saved.where(F.col("_source_row_id").isNull()).count()
row_id_distinct = df_saved.select("_source_row_id").distinct().count()
codes_nulls = df_saved.where(F.col("quality_issue_codes").isNull()).count()

checks = [
    ("Row count matches input", rows_saved == rows_in, f"{rows_saved:,} saved vs {rows_in:,} input"),
    ("All 51 original columns present", not missing_cols and len(original_cols) == 51, f"missing: {missing_cols or 'none'}"),
    ("Original column types unchanged", not type_diffs, f"{len(type_diffs)} type differences"),
    ("Original values + duplicate multiplicities match (EXCEPT ALL both directions)", diff_input_vs_saved == 0 and diff_saved_vs_input == 0, f"{diff_input_vs_saved} / {diff_saved_vs_input} differing rows"),
    ("_source_row_id unique and non-null", row_id_nulls == 0 and row_id_distinct == rows_saved, f"{row_id_distinct:,} distinct ids, {row_id_nulls} nulls"),
    ("quality_issue_codes never NULL", codes_nulls == 0, f"{codes_nulls} null arrays"),
]
display(spark.createDataFrame([(c, p, d) for c, p, d in checks], ["check", "passed", "detail"]))

print(f"\nTarget table: {full_table} — {rows_saved:,} rows x {len(df_saved.columns)} columns")

issue_counts = (
    df_saved.select(F.explode("quality_issue_codes").alias("code"))
    .groupBy("code").count().orderBy(F.desc("count"))
)
display(issue_counts)

no_issue_rows = df_saved.where(F.col("quality_issue_count") == 0).count()
print(f"Facilities with zero issue codes: {no_issue_rows:,} of {rows_saved:,} (an empty array does NOT mean verified)")