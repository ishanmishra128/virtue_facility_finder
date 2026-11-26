# Databricks notebook source
# DBTITLE 1,Purpose and publication rules
# MAGIC %md
# MAGIC # 17 — Publish National App View (no model calls, no fetching)
# MAGIC
# MAGIC Creates **`vf.gold.facility_service_mentions_v2`** via `CREATE VIEW`, **stopping if it already exists** (never OR REPLACE). Uses persistent objects only: `vf.gold.facility_service_mentions_v1`, `vf.silver.national_dialysis_validated_v2`, `vf.silver.national_dialysis_validation_review_v2` (schemas inspected before building).
# MAGIC
# MAGIC **Structure:** ALL 4,273 record/service pairs and the 20 original v1 columns are preserved unchanged. Validated AI results are LEFT-JOINed **only to dialysis rows** by source-record ID — one applicable extraction per record is verified before joining (STOP on multiple versions rather than selecting arbitrarily). New `ai_` fields:
# MAGIC
# MAGIC * `ai_enrichment_status`: `validated_extraction` (content from validated_v2 only) / `extraction_needs_review` (status only — failed AI content is NOT exposed) / `no_extraction` / `not_applicable` (other five services: AI fields NULL)
# MAGIC * `ai_assertion`, `ai_patient_scope`, `ai_treatment_types_for_display`, `ai_has_generic_dialysis_mention`, `ai_facility_attribution`, `ai_possible_mixed_facility_record`, `ai_evidence_quotations`, `ai_patient_scope_evidence`, `ai_attribution_evidence`, `ai_validator_version`
# MAGIC * `ai_enrichment_note`: **“validated extraction” means extraction-format and verbatim-quotation checks passed — NOT that care delivery was verified.**
# MAGIC
# MAGIC SQL mentions are NEVER removed when the AI assertion is `not_mentioned`, `negated`, `ambiguous`, or otherwise context-limited — the difference is preserved for the frontend. Existing review flags, source quotations, verification, and availability pass through unchanged. Raw model responses and internal reviewer information are not exposed.

# COMMAND ----------

# DBTITLE 1,Cell 1 — Pre-checks and CREATE VIEW
# Cell 1 — INSPECT, PRE-CHECK, CREATE. Schemas are inspected from the actual persistent objects;
# the join is built only after verifying ONE applicable extraction per record (STOP on multiple
# versions rather than selecting arbitrarily) and that gold v1 has unique record/service pairs.
# The view is created with CREATE VIEW and the cell STOPS if it already exists.
from pyspark.sql import functions as F

G1 = "vf.gold.facility_service_mentions_v1"
G2 = "vf.gold.facility_service_mentions_v2"
VAL = "vf.silver.national_dialysis_validated_v2"
REV = "vf.silver.national_dialysis_validation_review_v2"

assert not spark.catalog.tableExists(G2), G2 + " already exists — STOPPING (no overwrite)"

g1 = spark.table(G1)
print("gold v1 schema:", [f"{f.name}: {f.dataType.simpleString()}" for f in g1.schema.fields])
n_g1 = g1.count()
n_pairs = g1.select("_source_row_id", "service_code").distinct().count()
dial = g1.where(F.col("service_code") == "dialysis")
n_dial, n_dial_ids = dial.count(), dial.select("_source_row_id").distinct().count()
print(f"gold v1: {n_g1} rows | unique (record, service) pairs: {n_pairs} | dialysis pairs: {n_dial} | distinct dialysis record IDs: {n_dial_ids}")
assert n_g1 == 4273 and n_pairs == 4273 and n_dial == n_dial_ids, "unexpected v1 pair structure — STOPPING"

val, rev = spark.table(VAL), spark.table(REV)
n_val, n_val_ids = val.count(), val.select("_source_row_id").distinct().count()
n_rev, n_rev_ids = rev.count(), rev.select("_source_row_id").distinct().count()
n_union = val.select("_source_row_id").union(rev.select("_source_row_id")).distinct().count()
print(f"validated_v2: {n_val} rows / {n_val_ids} distinct record IDs | review_v2: {n_rev} rows / {n_rev_ids} distinct record IDs | combined distinct: {n_union}")
assert n_val == n_val_ids and n_rev == n_rev_ids and n_union == n_val + n_rev, \
    "multiple extraction versions per record — STOPPING rather than selecting arbitrarily"

spark.sql(f"""
CREATE VIEW {G2} AS
SELECT
  g._source_row_id, g.service_code, g.facility_name, g.city_clean, g.state_clean, g.pincode_clean,
  g.latitude, g.longitude, g.coordinate_status, g.source_url_candidates, g.review_priority,
  g.review_reason_codes, g.matched_terms_all, g.evidence, g.evidence_count, g.matching_rule_version,
  g.verification_status, g.current_availability_status, g.human_review_status, g.notice,
  CASE WHEN g.service_code <> 'dialysis' THEN 'not_applicable'
       WHEN v._source_row_id IS NOT NULL THEN 'validated_extraction'
       WHEN r._source_row_id IS NOT NULL THEN 'extraction_needs_review'
       ELSE 'no_extraction' END AS ai_enrichment_status,
  v.assertion AS ai_assertion,
  v.patient_scope AS ai_patient_scope,
  v.treatment_types_for_display AS ai_treatment_types_for_display,
  v.has_generic_dialysis_mention AS ai_has_generic_dialysis_mention,
  v.facility_attribution AS ai_facility_attribution,
  v.possible_mixed_facility_record AS ai_possible_mixed_facility_record,
  CASE WHEN v._source_row_id IS NOT NULL THEN transform(v.evidence, x -> x.exact_quote) END AS ai_evidence_quotations,
  CASE WHEN v._source_row_id IS NOT NULL THEN v.patient_scope_evidence END AS ai_patient_scope_evidence,
  CASE WHEN v._source_row_id IS NOT NULL THEN transform(v.attribution_evidence, x -> x.exact_quote) END AS ai_attribution_evidence,
  v.validator_version AS ai_validator_version,
  CASE WHEN g.service_code = 'dialysis' THEN
    'AI-enrichment only: validated_extraction means extraction-format and verbatim-quotation checks passed under the stated validator version — NOT that care delivery or service availability was verified. Availability remains unknown; verification unverified; human review pending. SQL mentions are never removed when the AI assertion is context-limited.'
  END AS ai_enrichment_note
FROM {G1} g
LEFT JOIN (SELECT _source_row_id, assertion, patient_scope, treatment_types_for_display,
                  has_generic_dialysis_mention, facility_attribution, possible_mixed_facility_record,
                  evidence, patient_scope_evidence, attribution_evidence, validator_version
           FROM {VAL}) v
  ON g.service_code = 'dialysis' AND g._source_row_id = v._source_row_id
LEFT JOIN (SELECT DISTINCT _source_row_id FROM {REV}) r
  ON g.service_code = 'dialysis' AND g._source_row_id = r._source_row_id
""")
print(f"Created {G2} (view over persistent objects only; no model calls, no fetching)")

# COMMAND ----------

# DBTITLE 1,Cell 2 — Verification checks
# Cell 2 — VERIFICATION CHECKS (read-only over v1 and v2). Confirms: 4,273 unique record/service
# pairs unchanged; original columns identical via EXCEPT ALL in BOTH directions; dialysis
# enrichment counts (1,024 validated / 31 review); other services have no AI enrichment; the 486
# mixed-facility flags from validated results are represented; availability stays unknown and
# verification unverified; review rows expose status but NO failed AI content; v1 untouched.
from pyspark.sql import functions as F

G1 = "vf.gold.facility_service_mentions_v1"
G2 = "vf.gold.facility_service_mentions_v2"
ORIG_COLS = ["_source_row_id", "service_code", "facility_name", "city_clean", "state_clean", "pincode_clean",
             "latitude", "longitude", "coordinate_status", "source_url_candidates", "review_priority",
             "review_reason_codes", "matched_terms_all", "evidence", "evidence_count", "matching_rule_version",
             "verification_status", "current_availability_status", "human_review_status", "notice"]
AI_CONTENT = ["ai_assertion", "ai_patient_scope", "ai_treatment_types_for_display", "ai_has_generic_dialysis_mention",
              "ai_facility_attribution", "ai_possible_mixed_facility_record", "ai_evidence_quotations",
              "ai_patient_scope_evidence", "ai_attribution_evidence", "ai_validator_version"]

g1, v2 = spark.table(G1), spark.table(G2)
checks = []

n_v2 = v2.count()
n_pairs_v2 = v2.select("_source_row_id", "service_code").distinct().count()
checks.append(("4,273 unique record/service pairs in v2, unchanged from v1",
               n_v2 == 4273 and n_pairs_v2 == 4273, f"{n_v2} rows / {n_pairs_v2} pairs"))
pair_diff = (g1.select("_source_row_id", "service_code").exceptAll(v2.select("_source_row_id", "service_code")).count()
             + v2.select("_source_row_id", "service_code").exceptAll(g1.select("_source_row_id", "service_code")).count())
checks.append(("Pair set identical to v1 (no additions/losses)", pair_diff == 0, f"{pair_diff} differences"))
orig_diff = (g1.select(*ORIG_COLS).exceptAll(v2.select(*ORIG_COLS)).count()
             + v2.select(*ORIG_COLS).exceptAll(g1.select(*ORIG_COLS)).count())
checks.append(("Original 20 columns unchanged (EXCEPT ALL both directions)", orig_diff == 0, f"{orig_diff} row differences"))

es = {r["ai_enrichment_status"]: r["n"] for r in v2.groupBy("ai_enrichment_status").count().withColumnRenamed("count", "n").collect()}
dial_es = {r["ai_enrichment_status"]: r["n"] for r in v2.where("service_code = 'dialysis'").groupBy("ai_enrichment_status").count().withColumnRenamed("count", "n").collect()}
checks.append(("Dialysis: 1,024 validated enrichments and 31 review statuses",
               dial_es.get("validated_extraction") == 1024 and dial_es.get("extraction_needs_review") == 31,
               str(dial_es)))

other = v2.where("service_code <> 'dialysis'")
n_other_ok = other.where("ai_enrichment_status = 'not_applicable'").count()
n_other_ai = sum(other.where(F.col(c).isNotNull()).count() for c in AI_CONTENT)
checks.append(("Other five services: no AI enrichment (not_applicable, all AI fields NULL)",
               n_other_ok == other.count() and n_other_ai == 0,
               f"{n_other_ok}/{other.count()} not_applicable, {n_other_ai} non-null AI values"))

n_mixed = v2.where("ai_possible_mixed_facility_record = true").count()
checks.append(("All 486 mixed-facility flags from validated results represented", n_mixed == 486, f"{n_mixed} flagged"))

rev_rows = v2.where("ai_enrichment_status = 'extraction_needs_review'")
n_rev_leak = sum(rev_rows.where(F.col(c).isNotNull()).count() for c in AI_CONTENT)
checks.append(("Review records: status exposed, NO failed AI content", rev_rows.count() == 31 and n_rev_leak == 0,
               f"{rev_rows.count()} rows, {n_rev_leak} non-null AI values"))

avail = {r["current_availability_status"]: r["n"] for r in v2.groupBy("current_availability_status").count().withColumnRenamed("count", "n").collect()}
verif = {r["verification_status"]: r["n"] for r in v2.groupBy("verification_status").count().withColumnRenamed("count", "n").collect()}
checks.append(("Availability remains unknown and verification unverified (v1 columns carried)",
               avail == {"unknown": 4273} and verif == {"unverified": 4273}, f"availability {avail}, verification {verif}"))

n_g1_after = spark.table(G1).count()
checks.append(("v1 view untouched (still 4,273 rows)", n_g1_after == 4273, f"{n_g1_after} rows"))

print("VERIFICATION CHECKS")
all_pass = True
for c, p, d in checks:
    all_pass &= p
    print(("PASS  " if p else "FAIL  ") + c + " — " + d)
print("\nALL CHECKS PASS" if all_pass else "\nSOME CHECKS FAILED — do not use the published view")

# COMMAND ----------

# DBTITLE 1,Cell 3 — Displays and examples
# Cell 3 — DISPLAY: enrichment counts by service and five REAL examples (queried from the
# published view — nothing fabricated). The review example shows status only; its AI content is
# withheld by design.
from pyspark.sql import functions as F

v2 = spark.table("vf.gold.facility_service_mentions_v2")

print("Enrichment counts by service × status:")
for r in (v2.groupBy("service_code", "ai_enrichment_status").count()
          .orderBy("service_code", "ai_enrichment_status").collect()):
    print(f"  {r['service_code']:28s} {r['ai_enrichment_status']:26s} {r['count']}")

EXAMPLES = [
    ("explicit service claim", "ai_assertion = 'explicit_service_claim'"),
    ("equipment-only (SQL mention kept, AI context preserved)", "ai_assertion = 'equipment_only'"),
    ("pediatric scope", "ai_patient_scope = 'pediatric'"),
    ("mixed-facility warning", "ai_possible_mixed_facility_record = true"),
    ("extraction needing review (content withheld)", "ai_enrichment_status = 'extraction_needs_review'"),
]
for label, cond in EXAMPLES:
    row = (v2.where(cond)
          .select("_source_row_id", "facility_name", "service_code", "ai_enrichment_status",
                  "ai_assertion", "ai_patient_scope", "ai_treatment_types_for_display",
                  "ai_possible_mixed_facility_record",
                  F.element_at(F.col("ai_evidence_quotations"), 1).alias("first_quote"))
          .orderBy("_source_row_id").first())
    print(f"\n--- {label} ---")
    if row is None:
        print("  (no matching row found in the published view)")
        continue
    print(f"  record: {row['_source_row_id']} | {row['facility_name']} | service={row['service_code']}")
    print(f"  status={row['ai_enrichment_status']} | assertion={row['ai_assertion']} | scope={row['ai_patient_scope']}")
    print(f"  treatments_for_display={row['ai_treatment_types_for_display']} | mixed_facility={row['ai_possible_mixed_facility_record']}")
    if row["ai_enrichment_status"] == "extraction_needs_review":
        print("  AI content withheld: this record failed validation; only the enrichment status is exposed.")
    else:
        print(f"  first evidence quote: {str(row['first_quote'])[:120]}")

# COMMAND ----------

