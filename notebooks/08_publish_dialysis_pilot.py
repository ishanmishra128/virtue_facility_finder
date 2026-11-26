# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 08 — Publish dialysis Jaipur pilot (gold VIEW, read-only over inputs)
# MAGIC
# MAGIC Reads `vf.silver.dialysis_pilot_review_v1` and `vf.silver.facilities_current_v1` (schemas inspected first), joins on
# MAGIC `_source_row_id` (one current-view row per record — no row multiplication), keeps `assertion != 'not_mentioned'`
# MAGIC (expected baseline 13; actual count calculated), and publishes `vf.gold.dialysis_jaipur_pilot_v1` via CREATE VIEW
# MAGIC (error-if-exists; `vf.gold` created only if permitted). Inputs are not modified; no model calls.
# MAGIC
# MAGIC App contract: `record_id` (source-record row ID, NOT a canonical hospital ID), facility_name, service_code, city/state/pin_code,
# MAGIC source-record latitude/longitude + coordinate_status, assertion, patient_scope, facility_attribution,
# MAGIC possible_mixed_facility_record, needs_attribution_review, evidence_quotations, attribution_evidence,
# MAGIC unattributed_source_url_candidates (unverified candidate links from source records — NOT attributed to any quotation or
# MAGIC facility), human_review_status, verification_status, current_availability_status, plus evidence_label (explicit vs
# MAGIC procedure-list vs equipment-only vs specialty-only claim types), location_label (the Jaipur city-string filter and India
# MAGIC coordinate screen do NOT verify the facility address), and notice ("Source-record claim; service and current availability
# MAGIC have not been verified."). No record is labeled recommended, available, verified, or safe; no scores, distances, contacts, or
# MAGIC URL-to-quotation attribution are invented; flagged records stay visible with their warnings; raw model responses and
# MAGIC internal reviewer identities are not exposed.

# COMMAND ----------

# DBTITLE 1,Inspect schemas & publish gold view
# Cell 1 — Inspect schemas, create vf.gold if permitted, publish the app-contract VIEW (error-if-exists).
# Read-only over inputs: no model calls, no modifications to review or current data.
from pyspark.sql import functions as F

REVIEW_TABLE = "vf.silver.dialysis_pilot_review_v1"
CURRENT_VIEW = "vf.silver.facilities_current_v1"
GOLD_SCHEMA = "vf.gold"
GOLD_VIEW = "vf.gold.dialysis_jaipur_pilot_v1"

rv = spark.table(REVIEW_TABLE)
cur = spark.table(CURRENT_VIEW)

# Inspect actual schemas first.
print("review columns:", rv.columns)
print("current-view join/location/url columns:",
      [c for c in cur.columns if c in ("_source_row_id", "name_clean", "city_clean", "state_clean",
                                       "pincode_clean", "latitude", "longitude", "coordinate_status",
                                       "source_url_candidates")])

try:
    view_exists = spark.catalog.tableExists(GOLD_VIEW)
except Exception:
    view_exists = False
if view_exists:
    raise ValueError(f"{GOLD_VIEW} already exists — stopping. Nothing overwritten; no inputs modified.")

# Create the gold schema if permitted.
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    print(f"Schema {GOLD_SCHEMA} ready")
except Exception as e:
    raise ValueError(f"Cannot create schema {GOLD_SCHEMA} — stopping before view creation: {str(e)[:300]}")

# Eligible records: assertion != not_mentioned. Expected baseline 13; calculate the actual count.
eligible = rv.where(rv["assertion"] != "not_mentioned")
n_eligible = eligible.count()
print(f"Eligible records (assertion != 'not_mentioned'): actual {n_eligible} (expected baseline: 13)")

# Join-multiplication guard: the current view must contribute at most one row per eligible record ID.
dup_ids = (cur.join(eligible.select("_source_row_id"), "_source_row_id")
           .groupBy("_source_row_id").count().where(F.col("count") > 1).count())
print(f"Current-view duplicate IDs among eligible records: {dup_ids}")
if dup_ids > 0:
    raise ValueError(f"Join would multiply rows: {dup_ids} record IDs match multiple current-view rows — stopping before view creation.")

# Compact app contract — pure SQL over persistent tables only (a permanent VIEW cannot reference temp objects);
# selects only: no raw model responses, reviewer identities, or review notes exposed.
VIEW_COMMENT = ("App contract for the Jaipur dialysis pilot. record_id is the source-record row ID, NOT a canonical hospital ID. "
                "unattributed_source_url_candidates are unverified candidate links from source records, NOT attributed to any quotation or facility. "
                "No record is recommended, available, verified, or safe. Flagged records remain visible with their warnings.")
CREATE_VIEW_SQL = f"""
CREATE VIEW {GOLD_VIEW} COMMENT '{VIEW_COMMENT}' AS
SELECT
    rv._source_row_id AS record_id,
    rv.name_clean AS facility_name,
    rv.service_code,
    cur.city_clean AS city,
    cur.state_clean AS state,
    cur.pincode_clean AS pin_code,
    cur.latitude,
    cur.longitude,
    cur.coordinate_status,
    rv.assertion,
    rv.patient_scope,
    rv.facility_attribution,
    rv.possible_mixed_facility_record,
    rv.needs_attribution_review,
    rv.evidence AS evidence_quotations,
    rv.attribution_evidence,
    cur.source_url_candidates AS unattributed_source_url_candidates,
    rv.human_review_status,
    rv.verification_status,
    rv.current_availability_status,
    CASE
      WHEN rv.assertion = 'explicit_service_claim' THEN 'explicit service claim: record states dialysis is provided (present tense)'
      WHEN rv.assertion = 'procedure_list_claim' THEN 'procedure-list claim: dialysis is a list item; does not establish delivery'
      WHEN rv.assertion = 'equipment_only' THEN 'equipment-only: dialysis equipment listed; does not establish delivery'
      WHEN rv.assertion = 'specialty_only' THEN 'specialty-only: dialysis/nephrology appears as a specialty listing; does not establish dialysis'
      ELSE CONCAT('other: ', rv.assertion)
    END AS evidence_label,
    'Location is unverified: the Jaipur city-string filter and India coordinate screen do not verify the facility address.' AS location_label,
    'Source-record claim; service and current availability have not been verified.' AS notice
FROM {REVIEW_TABLE} rv
LEFT JOIN (
    SELECT _source_row_id, city_clean, state_clean, pincode_clean, latitude, longitude, coordinate_status, source_url_candidates
    FROM {CURRENT_VIEW}
) cur ON rv._source_row_id = cur._source_row_id
WHERE rv.assertion != 'not_mentioned'
"""
spark.sql(CREATE_VIEW_SQL)
print(f"Created {GOLD_VIEW}")

# COMMAND ----------

# DBTITLE 1,Publish checks & example records
# Cell 2 — Publish checks (unique IDs, expected row count, unchanged statuses, no join multiplication) + 3 example records.
from pyspark.sql import functions as F

GOLD_VIEW = "vf.gold.dialysis_jaipur_pilot_v1"
REVIEW_TABLE = "vf.silver.dialysis_pilot_review_v1"

gv = spark.table(GOLD_VIEW)
rv = spark.table(REVIEW_TABLE)

n, n_id = gv.count(), gv.select("record_id").distinct().count()
print(("PASS  " if n_id == n else "FAIL  ") + f"unique record IDs: {n} rows, {n_id} distinct")

n_eligible = rv.where(rv["assertion"] != "not_mentioned").count()
print(("PASS  " if n == n_eligible else "FAIL  ") + f"row count matches eligible review records: view {n} vs eligible {n_eligible} (expected baseline 13, actual {n_eligible})")

bad = gv.where((gv["current_availability_status"] != "unknown") | (gv["verification_status"] != "unverified")).count()
print(("PASS  " if bad == 0 else "FAIL  ") + f"unchanged statuses: unknown availability + unverified verification on every row ({bad} deviating)")

mult = gv.groupBy("record_id").count().where(F.col("count") > 1).count()
print(("PASS  " if mult == 0 and n == n_eligible else "FAIL  ") + f"no join multiplication: {mult} duplicated record IDs; view rows {n} == eligible review rows {n_eligible}")

print(f"\nContract columns ({len(gv.columns)}):", gv.columns)
print("\nThree complete example app records:")
display(gv.orderBy(F.asc("record_id")).limit(3))