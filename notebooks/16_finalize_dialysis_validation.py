# Databricks notebook source
# DBTITLE 1,Purpose and revised rule
# MAGIC %md
# MAGIC # 16 — Finalize Dialysis Validation (no model calls, no fetching)
# MAGIC
# MAGIC Reads `vf.silver.national_dialysis_extraction_repaired_v1` and preserves every existing table, raw response, `treatment_types`, and evidence array untouched.
# MAGIC
# MAGIC **Revised rule (`national_dialysis_validator_v2`):** generic `unspecified_dialysis` and specific modality mentions may coexist — their coexistence is no longer a schema error. All other checks are enforced exactly as before (allowed types, evidence, patient scope, attribution, verbatim quotations); different modalities are never treated as interchangeable. Remaining quote failures are NOT auto-resolved.
# MAGIC
# MAGIC **New derived columns** (original `treatment_types` unchanged):
# MAGIC * `treatment_types_for_display` — specific types when present; otherwise `unspecified_dialysis` is retained
# MAGIC * `has_generic_dialysis_mention` — preserves the generic/specific distinction
# MAGIC
# MAGIC **Outputs (error-if-exists):** `vf.silver.national_dialysis_extraction_final_v1` (all 1,055 records; new `validation_status`/`validation_issues` recalculated from the actual data — no expected pass count forced; `previous_validation_status`/`previous_validation_issues` and the pre-repair `original_validation_status`/`original_validation_issues` retained) plus explicit-column views `vf.silver.national_dialysis_validated_v2` and `vf.silver.national_dialysis_validation_review_v2` (validated = all current checks pass + required provenance present; mixed-facility and attribution warnings preserved; availability unknown, verification unverified, human review pending). Gold app views are not changed.

# COMMAND ----------

# DBTITLE 1,Cell 1 — Build final table with revised validator
# Cell 1 — BUILD the final table (error-if-exists; no model calls). The validator is the notebook
# 11/15 validator VERBATIM except that ONE rule is revised per instruction: coexistence of
# 'unspecified_dialysis' with specific types is no longer an issue (generic and specific mentions
# may coexist; modalities are never treated as interchangeable). Every other check — allowed types,
# evidence, scope, attribution, verbatim quotations — is enforced exactly as before. Validation is
# recalculated from the ACTUAL repaired derived values; nothing is forced. Original treatment_types,
# evidence, raw responses, and metadata are copied unchanged.
import json
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType, TimestampType, ArrayType)

REPAIRED = "vf.silver.national_dialysis_extraction_repaired_v1"
FINAL = "vf.silver.national_dialysis_extraction_final_v1"
VALIDATOR_VERSION = "national_dialysis_validator_v2"

ALLOWED_ASSERTIONS = {"explicit_service_claim", "procedure_list_claim", "equipment_only", "specialty_only", "negated",
                      "planned_or_historical_only", "ambiguous", "not_mentioned"}
ALLOWED_ATTRIBUTIONS = {"explicit_named_facility", "implied_record_subject", "other_facility", "uncertain"}
ALLOWED_SCOPES = {"pediatric", "adult", "mixed_explicit", "unspecified"}
ALLOWED_TREATMENTS = {"hemodialysis", "peritoneal_dialysis", "CRRT", "hemofiltration", "unspecified_dialysis"}
EVIDENCE_FIELDS = ["description", "capability", "procedure", "equipment", "specialties"]
ATTR_SOURCE_FIELDS = EVIDENCE_FIELDS + ["name"]

def quote_in_field(rec, field_name, q):
    """Existing exact-match rules (verbatim): string substring, decoded JSON-array elements,
    or array<string> elements."""
    if not isinstance(q, str) or q == "":
        return False
    val = rec.get(field_name)
    if isinstance(val, list):
        return any(isinstance(e, str) and q in e for e in val)
    if not isinstance(val, str):
        return False
    if q in val:
        return True
    try:
        dec = json.loads(val)
        return isinstance(dec, list) and any(isinstance(e, str) and q in e for e in dec)
    except Exception:
        return False

def validate_row_v2(r):
    """Existing validator with ONE revision: 'unspecified_dialysis combined with specific types'
    is no longer flagged (generic + specific may coexist). Everything else is unchanged."""
    issues = []
    if r["extraction_status"] == "input_too_long":
        return "input_too_long", [r["model_error"]]
    if r["extraction_status"] != "succeeded" or r["model_error"]:
        issues.append(r["model_error"] if r["model_error"] else "model_failure: empty response")
    if not isinstance(r["raw_model_response"], str):
        if not issues:
            issues.append("model_failure: empty response")
        return ";".join(sorted({i.split(":")[0] for i in issues})), issues
    rec = None
    try:
        rec = json.loads(r["input_payload"])
    except Exception as e:
        issues.append("input_error: payload not parseable: " + str(e)[:100])
    parsed = None
    try:
        parsed = json.loads(r["raw_model_response"])
    except Exception as e:
        issues.append("schema_error: JSON parse failed: " + str(e)[:100])
    if parsed is not None and not isinstance(parsed, dict):
        parsed = None
        issues.append("schema_error: response is not a JSON object")
    if parsed is not None and rec is not None:
        if parsed.get("service_code") != "dialysis":
            issues.append("schema_error: service_code")
        if parsed.get("assertion") not in ALLOWED_ASSERTIONS:
            issues.append("schema_error: assertion")
        if parsed.get("facility_attribution") not in ALLOWED_ATTRIBUTIONS:
            issues.append("schema_error: facility_attribution")
        if not isinstance(parsed.get("possible_mixed_facility_record"), bool):
            issues.append("schema_error: possible_mixed_facility_record")
        ev = parsed.get("evidence")
        if not isinstance(ev, list):
            issues.append("schema_error: evidence")
        else:
            for item in ev:
                if (not isinstance(item, dict) or item.get("source_field") not in EVIDENCE_FIELDS
                        or not isinstance(item.get("exact_quote"), str) or item["exact_quote"] == ""):
                    issues.append("schema_error: evidence item")
                elif not quote_in_field(rec, item["source_field"], item["exact_quote"]):
                    issues.append("unsupported_quote: " + item["source_field"] + " — " + item["exact_quote"][:60])
        ps = parsed.get("patient_scope")
        if ps not in ALLOWED_SCOPES:
            issues.append("schema_error: patient_scope")
        else:
            ps_ev = parsed.get("patient_scope_evidence")
            ps_ev_ok = 0
            if not isinstance(ps_ev, list):
                issues.append("schema_error: patient_scope_evidence")
            else:
                for q in ps_ev:
                    if not isinstance(q, str) or not any(quote_in_field(rec, f, q) for f in ATTR_SOURCE_FIELDS):
                        issues.append("unsupported_quote: patient_scope_evidence — " + (q[:60] if isinstance(q, str) else str(q)[:60]))
                    else:
                        ps_ev_ok += 1
                if ps != "unspecified" and ps_ev_ok == 0:
                    issues.append("unsupported_quote: patient_scope_evidence — no validated quote for scope '" + str(ps) + "'")
        tt = parsed.get("treatment_types")
        tt_ev_ok = 0
        if not isinstance(tt, list):
            issues.append("schema_error: treatment_types")
        else:
            if any((not isinstance(t, str)) or (t not in ALLOWED_TREATMENTS) for t in tt):
                issues.append("schema_error: treatment_types value")
            if parsed.get("assertion") == "not_mentioned" and len(tt) > 0:
                issues.append("schema_error: treatment_types present with not_mentioned")
            if parsed.get("assertion") != "not_mentioned" and len(tt) == 0:
                issues.append("schema_error: treatment_types empty for a dialysis-mentioning assertion")
            # REVISED: 'unspecified_dialysis combined with specific types' is no longer an issue —
            # generic and specific mentions may coexist; modalities are not interchangeable.
        tev = parsed.get("treatment_type_evidence")
        if not isinstance(tev, list):
            issues.append("schema_error: treatment_type_evidence")
        else:
            for item in tev:
                if (not isinstance(item, dict) or item.get("source_field") not in EVIDENCE_FIELDS
                        or not isinstance(item.get("exact_quote"), str) or item["exact_quote"] == ""):
                    issues.append("schema_error: treatment_type_evidence item")
                elif not quote_in_field(rec, item["source_field"], item["exact_quote"]):
                    issues.append("unsupported_quote: treatment_type_evidence: " + item["source_field"] + " — " + item["exact_quote"][:60])
                else:
                    tt_ev_ok += 1
            if isinstance(tt, list) and any(t in ALLOWED_TREATMENTS - {"unspecified_dialysis"} for t in tt) and tt_ev_ok == 0:
                issues.append("unsupported_quote: treatment_type_evidence — no validated quote for a named treatment type")
        ae = parsed.get("attribution_evidence")
        if not isinstance(ae, list):
            issues.append("schema_error: attribution_evidence")
        else:
            for item in ae:
                if (not isinstance(item, dict) or item.get("source_field") not in ATTR_SOURCE_FIELDS
                        or not isinstance(item.get("exact_quote"), str) or item["exact_quote"] == ""):
                    issues.append("schema_error: attribution_evidence item")
                elif not quote_in_field(rec, item["source_field"], item["exact_quote"]):
                    issues.append("unsupported_quote: attribution_evidence: " + item["source_field"] + " — " + item["exact_quote"][:60])
        if not isinstance(parsed.get("explanation"), str):
            issues.append("schema_error: explanation")
    cats = sorted({i.split(":")[0] for i in issues})
    return ("ok" if not cats else ";".join(cats)), (issues if issues else None)

assert not spark.catalog.tableExists(FINAL), FINAL + " already exists — refusing to overwrite"

src = spark.table(REPAIRED)
rows = src.collect()
assert len(rows) == 1055, f"expected 1,055 repaired rows, found {len(rows)}"

EXTRACT_COLS = [f.name for f in src.schema.fields
                if f.name not in ("validation_status", "validation_issues",
                                  "original_validation_status", "original_validation_issues")]
OUT_COLS = (EXTRACT_COLS + ["treatment_types_for_display", "has_generic_dialysis_mention",
            "validation_status", "validation_issues", "previous_validation_status",
            "previous_validation_issues", "original_validation_status", "original_validation_issues",
            "validator_version"])
out_rows = []
for r in rows:
    rec = None
    try:
        rec = json.loads(r["input_payload"])
    except Exception:
        rec = None
    # validation input: the ACTUAL repaired derived values (raw kept verbatim; rows whose stored
    # fields are NULL because the response was unparseable keep the raw parse-fail path)
    if r["assertion"] is None:
        sim_raw = r["raw_model_response"]
    else:
        def arr_dicts(v):
            return [{"source_field": it["source_field"], "exact_quote": it["exact_quote"]} for it in v] if v is not None else None
        parsed = {"service_code": r["service_code"], "assertion": r["assertion"],
                  "evidence": arr_dicts(r["evidence"]), "patient_scope": r["patient_scope"],
                  "patient_scope_evidence": list(r["patient_scope_evidence"]) if r["patient_scope_evidence"] is not None else None,
                  "facility_attribution": r["facility_attribution"],
                  "possible_mixed_facility_record": r["possible_mixed_facility_record"],
                  "attribution_evidence": arr_dicts(r["attribution_evidence"]),
                  "treatment_types": list(r["treatment_types"]) if r["treatment_types"] is not None else None,
                  "treatment_type_evidence": arr_dicts(r["treatment_type_evidence"]),
                  "explanation": r["explanation"]}
        sim_raw = json.dumps(parsed)
    new_status, new_issues = validate_row_v2({"extraction_status": r["extraction_status"],
                                              "model_error": r["model_error"],
                                              "input_payload": r["input_payload"],
                                              "raw_model_response": sim_raw})
    tt = list(r["treatment_types"]) if r["treatment_types"] is not None else None
    specific = [t for t in (tt or []) if t != "unspecified_dialysis"]
    for_display = specific if specific else (tt if tt is not None else None)
    has_generic = "unspecified_dialysis" in (tt or [])
    d = {c: r[c] for c in EXTRACT_COLS}
    d["treatment_types_for_display"] = for_display
    d["has_generic_dialysis_mention"] = has_generic
    d["validation_status"], d["validation_issues"] = new_status, new_issues
    d["previous_validation_status"], d["previous_validation_issues"] = r["validation_status"], r["validation_issues"]
    d["original_validation_status"], d["original_validation_issues"] = r["original_validation_status"], r["original_validation_issues"]
    d["validator_version"] = VALIDATOR_VERSION
    out_rows.append(Row(*[d[c] for c in OUT_COLS]))

out_schema = StructType(
    [f for f in src.schema.fields if f.name in EXTRACT_COLS]
    + [StructField("treatment_types_for_display", ArrayType(StringType())),
       StructField("has_generic_dialysis_mention", BooleanType()),
       StructField("validation_status", StringType()), StructField("validation_issues", ArrayType(StringType())),
       StructField("previous_validation_status", StringType()), StructField("previous_validation_issues", ArrayType(StringType())),
       StructField("original_validation_status", StringType()), StructField("original_validation_issues", ArrayType(StringType())),
       StructField("validator_version", StringType())])
spark.createDataFrame(out_rows, schema=out_schema).write.mode("error").saveAsTable(FINAL)
fin = spark.table(FINAL)
print(f"Wrote {fin.count()} rows to {FINAL} (error-if-exists; validator {VALIDATOR_VERSION})")
print("New (v2) validation counts:", {r["validation_status"]: r["n"] for r in fin.groupBy("validation_status").count().withColumnRenamed("count", "n").orderBy(F.desc("n")).collect()},
      "| previous (repaired):", {r["previous_validation_status"]: r["n"] for r in fin.groupBy("previous_validation_status").count().withColumnRenamed("count", "n").orderBy(F.desc("n")).collect()})

# COMMAND ----------

# DBTITLE 1,Cell 2 — Create v2 views, transitions, counts, causes
# Cell 2 — VIEWS + DISPLAY. Creates the two v2 views (explicit columns, guarded CREATE VIEW =
# error-if-exists, never OR REPLACE), then displays old-to-new transitions, final counts,
# remaining causes, and mixed-facility counts per group. Validated = all current (v2) checks pass
# AND required provenance/status fields present; mixed-facility and attribution warnings are
# preserved; availability unknown, verification unverified, human review pending throughout.
from pyspark.sql import functions as F

FINAL = "vf.silver.national_dialysis_extraction_final_v1"
V2 = "vf.silver.national_dialysis_validated_v2"
R2 = "vf.silver.national_dialysis_validation_review_v2"
ALLOWED = "('explicit_service_claim','procedure_list_claim','equipment_only','specialty_only','negated','planned_or_historical_only','ambiguous','not_mentioned')"

COLS = ("_source_row_id, name_clean, input_hash, prompt_version, model_endpoint, extracted_at, "
        "raw_model_response, service_code, assertion, evidence, patient_scope, patient_scope_evidence, "
        "facility_attribution, possible_mixed_facility_record, attribution_evidence, treatment_types, "
        "has_generic_dialysis_mention, treatment_types_for_display, treatment_type_evidence, "
        "explanation, current_availability_status, verification_status, human_review_status")
W_OK = ("validation_status = 'ok' AND assertion IS NOT NULL AND assertion IN " + ALLOWED +
        " AND _source_row_id IS NOT NULL AND input_hash IS NOT NULL AND prompt_version IS NOT NULL"
        " AND model_endpoint IS NOT NULL AND raw_model_response IS NOT NULL AND extracted_at IS NOT NULL"
        " AND current_availability_status IS NOT NULL AND verification_status IS NOT NULL"
        " AND human_review_status IS NOT NULL")
NOTE_V = "'Validated = extraction-format and verbatim-quotation checks under validator national_dialysis_validator_v2; NOT verified medical services. Availability unknown; verification unverified; human review pending.'"
NOTE_R = "'Internal review only: rows failing current checks; values are unmodified extractions (citations as repaired); quotes never rewritten.'"

if not spark.catalog.tableExists(V2):
    spark.sql(f"CREATE VIEW {V2} AS SELECT {COLS}, validation_status, validation_issues, validator_version, {NOTE_V} AS view_note "
              f"FROM {FINAL} WHERE {W_OK}")
    print(f"Created {V2}")
else:
    print(f"{V2} already exists — reusing (not overwritten)")
if not spark.catalog.tableExists(R2):
    spark.sql(f"CREATE VIEW {R2} AS SELECT {COLS}, input_payload, model_error, extraction_status, "
              f"validation_status, validation_issues, previous_validation_status, previous_validation_issues, "
              f"original_validation_status, original_validation_issues, validator_version, {NOTE_R} AS view_note "
              f"FROM {FINAL} WHERE NOT ({W_OK})")
    print(f"Created {R2}")
else:
    print(f"{R2} already exists — reusing (not overwritten)")

fin = spark.table(FINAL)
v, r_ = spark.table(V2), spark.table(R2)
n_v, n_r = v.count(), r_.count()
overlap = v.select("_source_row_id", "input_hash").intersect(r_.select("_source_row_id", "input_hash")).count()
covered = v.select("_source_row_id", "input_hash").union(r_.select("_source_row_id", "input_hash")).distinct().count()
print(f"\nvalidated_v2: {n_v} | review_v2: {n_r} | sum: {n_v + n_r} | overlap: {overlap} | covered: {covered}/1055")
assert n_v + n_r == 1055 and overlap == 0 and covered == 1055

print("\nOld-to-new validation transitions (previous repaired status -> v2 status):")
for row in (fin.groupBy("previous_validation_status", "validation_status").count()
            .orderBy("previous_validation_status", "validation_status").collect()):
    mark = "  (improved)" if row["previous_validation_status"] != "ok" and row["validation_status"] == "ok" else ""
    print(f"  {row['previous_validation_status']:32s} -> {row['validation_status']:20s} {row['count']:5d}{mark}")

print("\nRemaining errors by specific cause (failing checks):")
causes = (fin.where(F.col("validation_status") != "ok")
          .select(F.explode(F.coalesce(F.col("validation_issues"), F.array().cast("array<string>"))).alias("issue"))
          .groupBy(F.split(F.col("issue"), " — ").getItem(0).alias("cause"))
          .count().withColumnRenamed("count", "n").orderBy(F.desc("n")).collect())
for row in causes:
    print(f"  {row['n']:4d}  {row['cause']}")

print("\nMixed-facility counts within each group:")
print("validated_v2:", {str(x["possible_mixed_facility_record"]): x["n"] for x in v.groupBy("possible_mixed_facility_record").count().withColumnRenamed("count", "n").collect()})
print("review_v2:", {str(x["possible_mixed_facility_record"]): x["n"] for x in r_.groupBy("possible_mixed_facility_record").count().withColumnRenamed("count", "n").collect()})

# COMMAND ----------

# DBTITLE 1,Cell 3 — Integrity checks
# Cell 3 — INTEGRITY CHECKS (read-only; no other table, view, or Gold app view is modified by
# this notebook). Verifies: unchanged original extraction values vs the repaired table (raw
# responses, treatment_types, all evidence arrays element-wise), derived display columns follow
# the stated rule, unique 1,055 keys, disjoint views with complete accounting, statuses preserved,
# and upstream tables/views untouched.
from pyspark.sql import functions as F

REPAIRED = "vf.silver.national_dialysis_extraction_repaired_v1"
FINAL = "vf.silver.national_dialysis_extraction_final_v1"
V1, R1 = "vf.silver.national_dialysis_validated_v1", "vf.silver.national_dialysis_validation_review_v1"
V2, R2 = "vf.silver.national_dialysis_validated_v2", "vf.silver.national_dialysis_validation_review_v2"
KEY = ["_source_row_id", "input_hash", "prompt_version", "model_endpoint"]

rep, fin = spark.table(REPAIRED), spark.table(FINAL)
j = (rep.alias("p").join(fin.alias("f"), on=KEY, how="inner")
     .select(*[F.col("p." + c).alias("p_" + c) for c in rep.columns],
             *[F.col("f." + c).alias("f_" + c) for c in fin.columns if c not in KEY]))

checks = []

# 1. unchanged original extraction values vs repaired (arrays compared element-wise via to_json)
ARRAY_COLS = ["evidence", "treatment_type_evidence", "attribution_evidence", "patient_scope_evidence", "treatment_types"]
SCALAR_COLS = [c for c in rep.columns if c not in KEY and c not in ARRAY_COLS
               and c not in ("validation_status", "validation_issues", "original_validation_status", "original_validation_issues")]
n_diff = 0
for c in SCALAR_COLS:
    n_diff += j.where(~F.col("p_" + c).eqNullSafe(F.col("f_" + c))).count()
checks.append(("All scalar extraction values unchanged (incl. raw responses, statuses, metadata)",
               n_diff == 0, f"{n_diff} differing across {len(SCALAR_COLS)} columns"))
for c in ARRAY_COLS:
    pj = F.when(F.col("p_" + c).isNotNull(), F.to_json(F.col("p_" + c)))
    fj = F.when(F.col("f_" + c).isNotNull(), F.to_json(F.col("f_" + c)))
    n_a = j.where(~pj.eqNullSafe(fj)).count()
    checks.append((f"{c} unchanged element-wise (quotes, citations, order)", n_a == 0, f"{n_a} differing"))

# 2. derived columns follow the stated rule (row-wise verification: when specific types exist,
# treatment_types_for_display shows exactly those and omits unspecified_dialysis; otherwise the
# original array is retained; has_generic_dialysis_mention preserves the generic/specific split)
n_display_bad, n_generic_bad = 0, 0
for row in fin.select("treatment_types", "treatment_types_for_display", "has_generic_dialysis_mention").collect():
    tt = list(row["treatment_types"]) if row["treatment_types"] is not None else None
    fd = list(row["treatment_types_for_display"]) if row["treatment_types_for_display"] is not None else None
    specific = [t for t in (tt or []) if t != "unspecified_dialysis"]
    expected = specific if specific else tt
    if fd != expected:
        n_display_bad += 1
    if ("unspecified_dialysis" in (tt or [])) != bool(row["has_generic_dialysis_mention"]):
        n_generic_bad += 1
checks.append(("treatment_types_for_display: specific types shown when present, unspecified_dialysis omitted; retained otherwise",
               n_display_bad == 0, f"{n_display_bad} rows violating the display rule"))
checks.append(("has_generic_dialysis_mention matches original treatment_types", n_generic_bad == 0, f"{n_generic_bad} mismatching"))
checks.append(("Original treatment_types preserved unchanged (covered by element-wise check above)", True, "verified in check 1"))

# 3. keys unique; views disjoint; complete accounting
n_fin, n_keys = fin.count(), fin.select(*KEY).distinct().count()
checks.append(("All 1,055 keys unique in final table", n_fin == 1055 and n_keys == 1055, f"{n_fin} rows / {n_keys} distinct"))
key_diff = (rep.select(*KEY).exceptAll(fin.select(*KEY)).count()
            + fin.select(*KEY).exceptAll(rep.select(*KEY)).count())
checks.append(("Key set identical to repaired table", key_diff == 0, f"{key_diff} key differences"))
n_v2, n_r2 = spark.table(V2).count(), spark.table(R2).count()
overlap = spark.table(V2).select("_source_row_id", "input_hash").intersect(spark.table(R2).select("_source_row_id", "input_hash")).count()
covered = spark.table(V2).select("_source_row_id", "input_hash").union(spark.table(R2).select("_source_row_id", "input_hash")).distinct().count()
checks.append(("v2 views disjoint and account for all 1,055 records", n_v2 + n_r2 == 1055 and overlap == 0 and covered == 1055,
               f"validated {n_v2} + review {n_r2} = {n_v2 + n_r2}, overlap {overlap}, covered {covered}"))

# 4. statuses preserved everywhere
st = {row["current_availability_status"]: row["n"] for row in fin.groupBy("current_availability_status").count().withColumnRenamed("count", "n").collect()}
sv = {row["verification_status"]: row["n"] for row in fin.groupBy("verification_status").count().withColumnRenamed("count", "n").collect()}
sh = {row["human_review_status"]: row["n"] for row in fin.groupBy("human_review_status").count().withColumnRenamed("count", "n").collect()}
checks.append(("Availability unknown / verification unverified / human review pending on all rows",
               st == {"unknown": 1055} and sv == {"unverified": 1055} and sh == {"pending": 1055},
               f"availability {st}, verification {sv}, review {sh}"))

# 5. upstream untouched (this notebook wrote ONLY the final table and the two v2 views)
rep_dist = {row["validation_status"]: row["n"] for row in rep.groupBy("validation_status").count().withColumnRenamed("count", "n").collect()}
checks.append(("Repaired table unchanged (still 975/50/30)",
               rep_dist == {"ok": 975, "schema_error": 50, "unsupported_quote": 30} and rep.count() == 1055, str(rep_dist)))
checks.append(("v1 validated/review views unchanged (886 / 169)",
               spark.table(V1).count() == 886 and spark.table(R1).count() == 169,
               f"{spark.table(V1).count()} / {spark.table(R1).count()}"))
checks.append(("Validator version recorded on every row",
               fin.where(F.col("validator_version") != "national_dialysis_validator_v2").count() == 0, "national_dialysis_validator_v2 on all 1,055"))

print("INTEGRITY CHECKS")
all_pass = True
for c, p, d in checks:
    all_pass &= p
    print(("PASS  " if p else "FAIL  ") + c + " — " + d)
print("\nALL CHECKS PASS" if all_pass else "\nSOME CHECKS FAILED — inspect before using the final table")

# COMMAND ----------

