# Databricks notebook source
# DBTITLE 1,Purpose and repair rule
# MAGIC %md
# MAGIC # 15 — Repair Dialysis Field Citations (deterministic; no inference)
# MAGIC
# MAGIC Repairs ONLY evidence-citation `source_field` values in the persisted national extraction, using the EXACT original candidate inputs identified by (`_source_row_id`, `input_hash`). **No model calls, no website fetching.** All existing tables, Gold app views, validated/review views, raw responses, and historical metadata are preserved untouched.
# MAGIC
# MAGIC **Repair rule (`dialysis_field_citation_repair_v1`):** for every quote-bearing evidence item (`evidence`, `treatment_type_evidence` — permitted fields `description/capability/procedure/equipment/specialties`; `attribution_evidence` — those plus `name`), if the UNCHANGED quotation fails the existing `quote_in_field` exact-match in its cited field (string substring, decoded JSON-array elements, array elements), search the permitted fields of the record's own input:
# MAGIC
# MAGIC * exactly ONE distinct matching field → correct `source_field` only (multiple occurrences within that field are fine)
# MAGIC * matches in 2+ fields (ambiguous) or 0 fields (no match) → left unchanged for review
# MAGIC * never another facility's text; never quotations, assertions, patient scope, treatment types, attribution decisions, or raw responses
# MAGIC * items with schema-level defects (missing/empty quote, non-permitted cited field) are not citation repairs and stay flagged
# MAGIC * `patient_scope_evidence` carries quotations without source fields — nothing to repair there
# MAGIC
# MAGIC Outputs (both **error-if-exists**, never overwritten):
# MAGIC * `vf.silver.national_dialysis_extraction_repaired_v1` — all 1,055 derived rows; `validation_status`/`validation_issues` re-run with the FULL existing validator against repaired derived values; `original_validation_status`/`original_validation_issues` retained separately. A repaired citation does **not** automatically make a record valid.
# MAGIC * `vf.ops.dialysis_field_citation_repairs_v1` — one row per applied repair: extraction key, evidence array, element position, original/corrected field, unchanged quotation, rule version, timestamp.
# MAGIC
# MAGIC No batch provenance is inferred; no historical metadata is rewritten.

# COMMAND ----------

# DBTITLE 1,Cell 1 — Apply citation repair, write repaired + repair-log tables
# Cell 1 — APPLY the repair and write both tables (error-if-exists). The validator below is
# copied VERBATIM from notebook 11 Cell 3 (same allowed sets, same quote_in_field exact-match
# rules — string substring, decoded JSON-array elements, array<string> elements — same checks),
# so re-validation is the FULL existing validation re-run against the repaired derived values.
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType, TimestampType, ArrayType, IntegerType)

EXT = "vf.silver.national_dialysis_extraction_v1"
CAND = "vf.silver.national_dialysis_candidates_v1"
REPAIRED = "vf.silver.national_dialysis_extraction_repaired_v1"
REPAIR_LOG = "vf.ops.dialysis_field_citation_repairs_v1"
REPAIR_RULE_VERSION = "dialysis_field_citation_repair_v1"

ALLOWED_ASSERTIONS = {"explicit_service_claim", "procedure_list_claim", "equipment_only", "specialty_only", "negated",
                      "planned_or_historical_only", "ambiguous", "not_mentioned"}
ALLOWED_ATTRIBUTIONS = {"explicit_named_facility", "implied_record_subject", "other_facility", "uncertain"}
ALLOWED_SCOPES = {"pediatric", "adult", "mixed_explicit", "unspecified"}
ALLOWED_TREATMENTS = {"hemodialysis", "peritoneal_dialysis", "CRRT", "hemofiltration", "unspecified_dialysis"}
EVIDENCE_FIELDS = ["description", "capability", "procedure", "equipment", "specialties"]
ATTR_SOURCE_FIELDS = EVIDENCE_FIELDS + ["name"]  # mixed-facility quotes may also come from the name field
QUOTE_BEARING = {"evidence": EVIDENCE_FIELDS,
                 "treatment_type_evidence": EVIDENCE_FIELDS,
                 "attribution_evidence": ATTR_SOURCE_FIELDS}

def quote_in_field(rec, field_name, q):
    """Exact-match validation (existing rules). String fields: exact substring (or within decoded
    elements if the string is a JSON array). Array fields (specialties): substring within an element."""
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

def validate_row(r):
    """Existing validator (verbatim from notebook 11 Cell 3): returns (status, issues)."""
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
            if len(tt) > 1 and "unspecified_dialysis" in tt:
                issues.append("schema_error: unspecified_dialysis combined with specific types")
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

# --- guard: both outputs must be new (error-if-exists) ---
assert not spark.catalog.tableExists(REPAIRED), REPAIRED + " already exists — refusing to overwrite"
assert not spark.catalog.tableExists(REPAIR_LOG), REPAIR_LOG + " already exists — refusing to overwrite"

# --- load persisted results joined to the EXACT original candidate inputs by (row id, hash) ---
src = spark.table(EXT)
ext_rows = (src.join(spark.table(CAND).select("_source_row_id", "input_hash",
                                           F.col("input_payload").alias("cand_payload")),
                      on=["_source_row_id", "input_hash"], how="left").collect())
n_no_cand = sum(1 for r in ext_rows if r["cand_payload"] is None)
n_payload_diff = sum(1 for r in ext_rows if r["cand_payload"] is not None and r["cand_payload"] != r["input_payload"])
print(f"rows loaded: {len(ext_rows)} | without matching candidate input: {n_no_cand} | payload != candidate snapshot: {n_payload_diff}")
assert len(ext_rows) == 1055 and n_no_cand == 0 and n_payload_diff == 0, "input reconciliation failed"

now = datetime.now(timezone.utc).replace(tzinfo=None)
repair_log, out_rows = [], []
n_amb, n_nomatch, n_records_repaired = 0, 0, 0
orig_cols = [f.name for f in src.schema.fields]
for r in ext_rows:
    payload = r["cand_payload"]  # exact original candidate input
    try:
        rec = json.loads(payload)
    except Exception:
        rec = None
    parsed = None
    try:
        parsed = json.loads(r["raw_model_response"])
        if not isinstance(parsed, dict):
            parsed = None
    except Exception:
        parsed = None
    row_repairs = []
    if rec is not None and parsed is not None:
        for arr_name, permitted in QUOTE_BEARING.items():
            arr = parsed.get(arr_name)
            if not isinstance(arr, list):
                continue
            for pos, item in enumerate(arr):
                if not isinstance(item, dict):
                    continue
                q, cited = item.get("exact_quote"), item.get("source_field")
                if not isinstance(q, str) or q == "" or cited not in permitted:
                    continue  # schema-level item defects are NOT citation repairs
                if quote_in_field(rec, cited, q):
                    continue  # already valid in its cited field
                matches = [f for f in permitted if quote_in_field(rec, f, q)]
                if len(matches) == 1:
                    item["source_field"] = matches[0]
                    row_repairs.append((arr_name, pos, cited, matches[0], q))
                elif len(matches) >= 2:
                    n_amb += 1
                else:
                    n_nomatch += 1
    # re-run ALL existing validation checks against the repaired DERIVED values (same validator;
    # raw response stays verbatim in the output — the validator here sees a lossless JSON
    # round-trip of the repaired derived values)
    sim = {"extraction_status": r["extraction_status"], "model_error": r["model_error"],
           "raw_model_response": r["raw_model_response"] if parsed is None else json.dumps(parsed),
           "input_payload": payload}
    new_status, new_issues = validate_row(sim)
    d = {c: r[c] for c in orig_cols}
    if row_repairs:
        n_records_repaired += 1
        for arr_name in QUOTE_BEARING:
            arr = parsed.get(arr_name)
            d[arr_name] = [{"source_field": it["source_field"], "exact_quote": it["exact_quote"]}
                           for it in arr] if isinstance(arr, list) else r[arr_name]
        for arr_name, pos, o_f, c_f, q in row_repairs:
            repair_log.append(Row(_source_row_id=r["_source_row_id"], input_hash=r["input_hash"],
                                  prompt_version=r["prompt_version"], model_endpoint=r["model_endpoint"],
                                  evidence_array=arr_name, element_position=pos,
                                  original_field=o_f, corrected_field=c_f, exact_quote=q,
                                  repair_rule_version=REPAIR_RULE_VERSION, repaired_at=now))
    d["validation_status"], d["validation_issues"] = new_status, new_issues
    d["original_validation_status"] = r["validation_status"]
    d["original_validation_issues"] = r["validation_issues"]
    all_cols = orig_cols + ["original_validation_status", "original_validation_issues"]
    out_rows.append(Row(*[d[c] for c in all_cols]))

out_schema = StructType(list(src.schema.fields) + [StructField("original_validation_status", StringType()),
                                                   StructField("original_validation_issues", ArrayType(StringType()))])
spark.createDataFrame(out_rows, schema=out_schema).write.mode("error").saveAsTable(REPAIRED)

spark.sql("CREATE SCHEMA IF NOT EXISTS vf.ops")
log_schema = StructType([StructField("_source_row_id", StringType()), StructField("input_hash", StringType()),
                         StructField("prompt_version", StringType()), StructField("model_endpoint", StringType()),
                         StructField("evidence_array", StringType()), StructField("element_position", IntegerType()),
                         StructField("original_field", StringType()), StructField("corrected_field", StringType()),
                         StructField("exact_quote", StringType()), StructField("repair_rule_version", StringType()),
                         StructField("repaired_at", TimestampType())])
if repair_log:
    spark.createDataFrame(repair_log, schema=log_schema).write.mode("error").saveAsTable(REPAIR_LOG)
else:
    spark.createDataFrame([], schema=log_schema).write.mode("error").saveAsTable(REPAIR_LOG)

print(f"\nRepairs applied: {len(repair_log)} across {n_records_repaired} distinct records")
print(f"Citations left unchanged for review: ambiguous (2+ fields) {n_amb} | no-match {n_nomatch}")
print(f"Wrote {spark.table(REPAIRED).count()} rows to {REPAIRED} (error-if-exists)")
print(f"Wrote {spark.table(REPAIR_LOG).count()} repair records to {REPAIR_LOG} (error-if-exists)")

# COMMAND ----------

# DBTITLE 1,Cell 2 — Validation transitions and remaining causes
# Cell 2 — RE-VALIDATION DISPLAY (read-only over the repaired table). Original-to-new validation
# transitions, and remaining errors grouped by their SPECIFIC cause. A repaired citation does not
# automatically make a record valid — rows keep any other schema/quote/scope issues they had.
from pyspark.sql import functions as F

REPAIRED = "vf.silver.national_dialysis_extraction_repaired_v1"
rep = spark.table(REPAIRED)

n = rep.count()
print(f"Repaired derived results: {n} rows")
print("\nNew validation status counts:", {r["validation_status"]: r["n"] for r in rep.groupBy("validation_status").count().withColumnRenamed("count", "n").orderBy(F.desc("n")).collect()},
      "| original:", {r["original_validation_status"]: r["n"] for r in rep.groupBy("original_validation_status").count().withColumnRenamed("count", "n").orderBy(F.desc("n")).collect()})

print("\nOriginal-to-new validation transitions (rows):")
for r in (rep.groupBy("original_validation_status", "validation_status").count().orderBy("original_validation_status", "validation_status").collect()):
    mark = "  (improved)" if r["original_validation_status"] != "ok" and r["validation_status"] == "ok" else ""
    print(f"  {r['original_validation_status']:32s} -> {r['validation_status']:32s} {r['count']:5d}{mark}")

print("\nRemaining errors by specific cause (failing checks):")
causes = (rep.where(F.col("validation_status") != "ok")
          .select(F.explode(F.coalesce(F.col("validation_issues"), F.array().cast("array<string>"))).alias("issue"))
          .groupBy(F.split(F.col("issue"), " — ").getItem(0).alias("cause"))
          .count().withColumnRenamed("count", "n").orderBy(F.desc("n")).collect())
for r in causes:
    print(f"  {r['n']:4d}  {r['cause']}")
print(f"\nNote: remaining flagged rows stay in vf.silver.national_dialysis_validation_review_v1 semantics "
      "(that view is unchanged and still reflects the ORIGINAL results).", )

# COMMAND ----------

# DBTITLE 1,Cell 3 — Integrity checks
# Cell 3 — INTEGRITY CHECKS (read-only; the source table, Gold app views, and validated/review
# views are never touched by this notebook). Verifies against the ORIGINAL stored rows that:
#   1. all 1,055 keys remain unique and the key set is identical;
#   2. raw_model_response is unchanged everywhere;
#   3. every quotation is unchanged (element-wise, all quote-bearing arrays);
#   4. ONLY permitted source_field edits occurred — every other column identical, and the count
#      of source-field differences equals the repair log, with each repair re-verified from the
#      record's own original candidate input;
#   5. untouched-view checks (validated/review counts and source-table distribution unchanged).
import json
from pyspark.sql import functions as F

EXT = "vf.silver.national_dialysis_extraction_v1"
CAND = "vf.silver.national_dialysis_candidates_v1"
REPAIRED = "vf.silver.national_dialysis_extraction_repaired_v1"
REPAIR_LOG = "vf.ops.dialysis_field_citation_repairs_v1"
EVIDENCE_FIELDS = ["description", "capability", "procedure", "equipment", "specialties"]
ATTR_SOURCE_FIELDS = EVIDENCE_FIELDS + ["name"]
QUOTE_BEARING = {"evidence": set(EVIDENCE_FIELDS),
                 "treatment_type_evidence": set(EVIDENCE_FIELDS),
                 "attribution_evidence": set(ATTR_SOURCE_FIELDS)}
KEY = ["_source_row_id", "input_hash", "prompt_version", "model_endpoint"]

def quote_in_field(rec, field_name, q):
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

o, rep, log = spark.table(EXT), spark.table(REPAIRED), spark.table(REPAIR_LOG)
j = (o.alias("o").join(rep.alias("r"), on=KEY, how="inner")
     .select(*[F.col("o." + c).alias("o_" + c) for c in o.columns],
             *[F.col("r." + c).alias("r_" + c) for c in rep.columns if c not in KEY]))

checks = []

# 1. keys
n_o, n_rep = o.count(), rep.count()
n_keys_o = o.select(*KEY).distinct().count()
n_keys_rep = rep.select(*KEY).distinct().count()
key_set_diff = (o.select(*KEY).exceptAll(rep.select(*KEY)).count() + rep.select(*KEY).exceptAll(o.select(*KEY)).count())
checks.append(("1,055 keys unique in repaired table", n_rep == 1055 and n_keys_rep == 1055,
               f"{n_rep} rows / {n_keys_rep} distinct"))
checks.append(("Key set identical to original (no additions/losses)", key_set_diff == 0,
               f"{n_o} vs {n_rep} rows, {key_set_diff} key diffs"))

# 2. raw responses unchanged
n_raw_diff = j.where(~(F.col("o_raw_model_response").eqNullSafe(F.col("r_raw_model_response")))).count()
checks.append(("raw_model_response unchanged (all 1,055)", n_raw_diff == 0, f"{n_raw_diff} differing"))

# 3. quotations unchanged (element-wise per array)
for arr in QUOTE_BEARING:
    q_o = F.when(F.col(f"o_{arr}").isNotNull(), F.expr(f"array_join(transform(o_{arr}, x -> x.exact_quote), '||')"))
    q_r = F.when(F.col(f"r_{arr}").isNotNull(), F.expr(f"array_join(transform(r_{arr}, x -> x.exact_quote), '||')"))
    n_diff = j.where(~q_o.eqNullSafe(q_r)).count()
    checks.append((f"quotations unchanged in {arr}", n_diff == 0, f"{n_diff} rows with quote-sequence differences"))

# 4a. every non-validation, non-evidence column identical
other_cols = [c for c in o.columns if c not in KEY and c not in ("validation_status", "validation_issues")
              and c not in QUOTE_BEARING]
n_other_diff = 0
for c in other_cols:
    n_other_diff += j.where(~F.col("o_" + c).eqNullSafe(F.col("r_" + c))).count()
checks.append(("All non-evidence, non-validation columns identical (incl. input_hash, provenance, metadata)",
               n_other_diff == 0, f"{n_other_diff} differing values across {len(other_cols)} columns"))
for arr in ("patient_scope_evidence", "treatment_types"):
    n_arr_diff = j.where(~F.col("o_" + arr).eqNullSafe(F.col("r_" + arr))).count()
    checks.append((f"{arr} identical", n_arr_diff == 0, f"{n_arr_diff} differing"))

# 4b. only source_field edits; exactly the logged ones; each repair re-verified from the input
o_rows = {tuple(r[k] for k in KEY): r for r in o.select(*KEY, *QUOTE_BEARING).collect()}
cand_payload = {tuple(r[k] for k in KEY[:2]): r["input_payload"] for r in spark.table(CAND).select("_source_row_id", "input_hash", "input_payload").collect()}
n_field_diff, repair_verified, repair_bad = 0, 0, []
for lr in log.collect():
    key = (lr["_source_row_id"], lr["input_hash"], lr["prompt_version"], lr["model_endpoint"])
    o_arr, r_arr = o_rows[key][lr["evidence_array"]], None
    rec = json.loads(cand_payload[(lr["_source_row_id"], lr["input_hash"])])
    pos = lr["element_position"]
    o_item = o_arr[pos] if o_arr and pos < len(o_arr) else None
    ok_item = (o_item is not None and o_item["source_field"] == lr["original_field"]
               and o_item["exact_quote"] == lr["exact_quote"]
               and not quote_in_field(rec, lr["original_field"], lr["exact_quote"])
               and quote_in_field(rec, lr["corrected_field"], lr["exact_quote"])
               and lr["corrected_field"] in QUOTE_BEARING[lr["evidence_array"]]
               and lr["original_field"] != lr["corrected_field"])
    if ok_item:
        repair_verified += 1
    else:
        repair_bad.append(lr["_source_row_id"])
rep_rows = {tuple(r[k] for k in KEY): r for r in rep.select(*KEY, *QUOTE_BEARING).collect()}
logged_fix = {(lr["_source_row_id"], lr["input_hash"], lr["prompt_version"], lr["model_endpoint"],
               lr["evidence_array"], lr["element_position"]): lr["corrected_field"]
              for lr in log.collect()}
for key, o_row in o_rows.items():
    r_row = rep_rows.get(key)
    if r_row is None:
        continue
    for arr in QUOTE_BEARING:
        o_a, r_a = o_row[arr] or [], r_row[arr] or []
        if len(o_a) != len(r_a):
            n_field_diff += 1
            continue
        for pos, (oi, ri) in enumerate(zip(o_a, r_a)):
            if oi["exact_quote"] != ri["exact_quote"]:
                n_field_diff += 1  # any quote change is forbidden
            elif oi["source_field"] != ri["source_field"] \
                    and logged_fix.get((key[0], key[1], key[2], key[3], arr, pos)) != ri["source_field"]:
                n_field_diff += 1  # source-field change outside the logged repairs
checks.append(("Only permitted source_field edits occurred (element-wise diff vs repair log)", n_field_diff == 0,
               f"{n_field_diff} unlogged/extra element differences"))
n_log = log.count()
checks.append(("Repair log count matches re-verified repairs", repair_verified == n_log and n_log == spark.table(REPAIR_LOG).count(),
               f"{n_log} logged, {repair_verified} independently re-verified from original inputs, {len(repair_bad)} failed re-verification"))

# 5. untouched views / source table
ext_dist = {r["validation_status"]: r["n"] for r in o.groupBy("validation_status").count().withColumnRenamed("count", "n").collect()}
n_val = spark.table("vf.silver.national_dialysis_validated_v1").count()
n_rev = spark.table("vf.silver.national_dialysis_validation_review_v1").count()
checks.append(("Source extraction table unchanged (still 886/47/119/3 distribution, 1,055 rows)",
               ext_dist == {'ok': 886, 'schema_error': 47, 'unsupported_quote': 119, 'schema_error;unsupported_quote': 3} and n_o == 1055,
               f"{ext_dist} / {n_o} rows"))
checks.append(("Validated/review views unchanged (886 / 169)", n_val == 886 and n_rev == 169, f"{n_val} / {n_rev}"))

print("INTEGRITY CHECKS")
all_pass = True
for c, p, d in checks:
    all_pass &= p
    print(("PASS  " if p else "FAIL  ") + c + " — " + d)
print("\nALL CHECKS PASS" if all_pass else "\nSOME CHECKS FAILED — inspect before using repaired table")

# COMMAND ----------

