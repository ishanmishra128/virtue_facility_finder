# Databricks notebook source
# DBTITLE 1,Purpose and constraints
# MAGIC %md
# MAGIC # 14 — Validate National Dialysis Outputs
# MAGIC
# MAGIC Read-only validation of `vf.silver.national_dialysis_extraction_v1` using ONLY persisted raw responses and the original candidate inputs. **No model calls, no website fetching, no changes to existing tables/Gold views.**
# MAGIC
# MAGIC 1. **Reconcile** — 1,055 unique keys; validation counts; NULL assertions and their validation group; validation × mixed-facility cross-tab.
# MAGIC 2. **Two persistent views** (explicit columns, error-if-exists on creation; reruns reuse) — `national_dialysis_validated_v1` (ok rows with non-null allowed assertions and required provenance/status fields present; ALL attribution warnings retained; “validated” = extraction-format/verbatim-quotation checks ONLY, never verified medical services) and `national_dialysis_validation_review_v1` (every remaining row with exact reasons and raw responses for internal review). Disjoint membership and complete accounting are asserted.
# MAGIC 3. **Diagnose flagged rows** from saved data — grouped by the SPECIFIC failing check; quotation failures are fuzzy-matched against the record's own input text to distinguish altered quotations, misattributed source fields, and genuinely unsupported content. Nothing is repaired, loosened, or rewritten here.
# MAGIC 4. **Display** — view counts, mixed-facility counts, failure causes with examples, and staging/raw provenance (first legacy batch vs the 955 staged rows).

# COMMAND ----------

# DBTITLE 1,Cell 1 — Reconcile keys, counts, NULL assertions, cross-tab
# Cell 1 — RECONCILE (read-only). Verify the ACTUAL schema, key uniqueness, validation counts,
# NULL-assertion rows and their validation group, and the validation × mixed-facility cross-tab
# against the stored table — nothing is assumed from prior reports.
from pyspark.sql import functions as F

EXT = "vf.silver.national_dialysis_extraction_v1"
ext = spark.table(EXT)

print("Schema as stored:")
for f in ext.schema.fields:
    print(f"  {f.name}: {f.dataType.simpleString()}")

n_rows = ext.count()
n_keys = ext.select("_source_row_id", "input_hash", "prompt_version", "model_endpoint").distinct().count()
n_rowid_hash = ext.select("_source_row_id", "input_hash").distinct().count()
print(f"\nrows: {n_rows} | unique 4-part keys: {n_keys} | unique (_source_row_id, input_hash): {n_rowid_hash}")
assert n_rows == 1055 and n_keys == 1055, f"reconciliation failed: {n_rows} rows / {n_keys} keys"

vs = {r["validation_status"]: r["n"] for r in ext.groupBy("validation_status").count().withColumnRenamed("count", "n").collect()}
print("\nvalidation_status counts:", vs, "| sum:", sum(vs.values()))

nulls = ext.where(F.col("assertion").isNull())
n_null = nulls.count()
null_groups = {r["validation_status"]: r["n"] for r in nulls.groupBy("validation_status").count().withColumnRenamed("count", "n").collect()}
null_ids = [r["_source_row_id"] for r in nulls.select("_source_row_id").collect()]
print(f"\nNULL assertions: {n_null} | validation groups: {null_groups} | ids: {null_ids}")
print("NULL patient_scope:", ext.where(F.col("patient_scope").isNull()).count(),
      "| NULL facility_attribution:", ext.where(F.col("facility_attribution").isNull()).count(),
      "| NULL possible_mixed_facility_record:", ext.where(F.col("possible_mixed_facility_record").isNull()).count(),
      "| NULL raw_model_response:", ext.where(F.col("raw_model_response").isNull()).count())

print("\nCross-tab: validation_status × possible_mixed_facility_record")
for r in (ext.groupBy("validation_status", "possible_mixed_facility_record").count()
          .orderBy("validation_status", "possible_mixed_facility_record").collect()):
    print(f"  {r['validation_status']:32s} mixed={str(r['possible_mixed_facility_record']):5s} {r['count']}")
print("\nReconcile: PASS" if n_rows == 1055 == n_keys and sum(vs.values()) == 1055 else "Reconcile: CHECK FAILED")

# COMMAND ----------

# DBTITLE 1,Cell 2 — Create validated + review views, verify accounting
# Cell 2 — TWO PERSISTENT VIEWS. Explicit columns; guarded CREATE VIEW = error-if-exists on
# first creation, reuse on rerun (no OR REPLACE, never silently overwritten). The validated view
# requires validation_status='ok' AND a non-null ALLOWED assertion AND required provenance/status
# fields present — the count is COMPUTED, not assumed to be 886. “Validated” means extraction-
# format and verbatim-quotation checks only — never verified medical services. All attribution
# warnings (possible_mixed_facility_record, attribution_evidence, facility_attribution) are kept.
# The review view holds every remaining row with exact validation reasons and raw responses.
from pyspark.sql import functions as F

VALIDATED_VIEW = "vf.silver.national_dialysis_validated_v1"
REVIEW_VIEW = "vf.silver.national_dialysis_validation_review_v1"
ALLOWED = "('explicit_service_claim','procedure_list_claim','equipment_only','specialty_only','negated','planned_or_historical_only','ambiguous','not_mentioned')"

COLS = ("_source_row_id, name_clean, input_hash, prompt_version, model_endpoint, extracted_at, "
        "raw_model_response, service_code, assertion, evidence, patient_scope, patient_scope_evidence, "
        "facility_attribution, possible_mixed_facility_record, attribution_evidence, treatment_types, "
        "treatment_type_evidence, explanation, current_availability_status, verification_status, "
        "human_review_status, validation_status, validation_issues")
W_OK = ("validation_status = 'ok' AND assertion IS NOT NULL AND assertion IN " + ALLOWED +
        " AND _source_row_id IS NOT NULL AND input_hash IS NOT NULL AND prompt_version IS NOT NULL"
        " AND model_endpoint IS NOT NULL AND raw_model_response IS NOT NULL AND extracted_at IS NOT NULL"
        " AND current_availability_status IS NOT NULL AND verification_status IS NOT NULL"
        " AND human_review_status IS NOT NULL")

NOTE_V = "'Validated = extraction-format and verbatim-quotation checks only; NOT verified medical services. Availability unknown; verification unverified; human review pending.'"
NOTE_R = "'Internal review only: validation flagged these rows; values and quotations are unmodified raw extractions.'"

if not spark.catalog.tableExists(VALIDATED_VIEW):
    spark.sql(f"CREATE VIEW {VALIDATED_VIEW} AS SELECT {COLS}, {NOTE_V} AS view_note "
              f"FROM vf.silver.national_dialysis_extraction_v1 WHERE {W_OK}")
    print(f"Created {VALIDATED_VIEW}")
else:
    print(f"{VALIDATED_VIEW} already exists — reusing (not overwritten)")
if not spark.catalog.tableExists(REVIEW_VIEW):
    spark.sql(f"CREATE VIEW {REVIEW_VIEW} AS SELECT {COLS}, input_payload, model_error, extraction_status, {NOTE_R} AS view_note "
              f"FROM vf.silver.national_dialysis_extraction_v1 WHERE NOT ({W_OK})")
    print(f"Created {REVIEW_VIEW}")
else:
    print(f"{REVIEW_VIEW} already exists — reusing (not overwritten)")

v, r = spark.table(VALIDATED_VIEW), spark.table(REVIEW_VIEW)
n_v, n_r = v.count(), r.count()
overlap = v.select("_source_row_id", "input_hash").intersect(r.select("_source_row_id", "input_hash")).count()
covered = (v.select("_source_row_id", "input_hash").union(r.select("_source_row_id", "input_hash"))).distinct().count()
print(f"\nvalidated: {n_v} | review: {n_r} | sum: {n_v + n_r} | overlap: {overlap} | covered keys: {covered}/1055")
assert n_v + n_r == 1055 and overlap == 0 and covered == 1055, "view accounting failed"
if n_v != 886:
    n_excluded = (spark.table("vf.silver.national_dialysis_extraction_v1")
                  .where("validation_status='ok' AND (assertion IS NULL OR assertion NOT IN " + ALLOWED + ")").count())
    print(f"NOTE: validated count is {n_v}, NOT 886 — ok-rows excluded by additional checks: {n_excluded}")
print("View accounting: PASS")

# COMMAND ----------

# DBTITLE 1,Cell 3 — Diagnose flagged rows by specific cause
# Cell 3 — DIAGNOSE flagged rows from SAVED data only (no model calls). Each failing check is
# grouped by its SPECIFIC cause (not the broad status). For quotation failures the full returned
# quote is fuzzy-matched against the record's own decoded input text to classify:
#   quote_found_in_other_field  – verbatim quote exists in a different source field (misattributed field)
#   altered_quotation          – near-verbatim match in the cited field (whitespace/punctuation/light paraphrase)
#   unsupported_content        – no near match anywhere in the record
#   scope_claim_without_supported_quote – scope asserted with no validatable quote
# Up to 5 examples per cause show record ID, facility name, returned value/quotation, cited source
# field, exact input excerpt, and the validation error. NOTHING is repaired or rewritten here.
import json
import difflib
from pyspark.sql import functions as F

EXT = "vf.silver.national_dialysis_extraction_v1"
EVIDENCE_FIELDS = ["description", "capability", "procedure", "equipment", "specialties"]
ATTR_FIELDS = EVIDENCE_FIELDS + ["name"]

flagged = (spark.table(EXT).where(F.col("validation_status") != "ok")
           .select("_source_row_id", "name_clean", "input_payload", "raw_model_response",
                   "validation_status", "validation_issues")
           .collect())
print(f"Flagged rows to diagnose: {len(flagged)}")

def field_text(rec, f):
    v = rec.get(f)
    if isinstance(v, list):
        return " | ".join(e for e in v if isinstance(e, str))
    return v if isinstance(v, str) else ""

def best_window(quote, text):
    if not text or not quote:
        return 0.0, ""
    ql, best_r, best_i = len(quote), 0.0, 0
    for i in range(0, max(1, len(text) - 3)):
        r = difflib.SequenceMatcher(None, quote, text[i:i + ql]).ratio()
        if r > best_r:
            best_r, best_i = r, i
        if best_r > 0.995:
            break
    s, e = max(0, best_i - 40), min(len(text), best_i + ql + 40)
    return best_r, text[s:e]

def classify_quote(quote, rec, cited):
    cited_text = field_text(rec, cited) if cited else ""
    for f in ATTR_FIELDS:
        if f != cited and quote in field_text(rec, f):
            return "quote_found_in_other_field", f"verbatim in field '{f}' (cited: '{cited}')"
    r, ex = best_window(quote, cited_text)
    if r < 0.75:
        all_text = " ".join(field_text(rec, f) for f in ATTR_FIELDS)
        r, ex = best_window(quote, all_text)
    if r >= 0.75:
        return "altered_quotation", ex
    return "unsupported_content", (ex or "(no matching input text found)")[:90]

CAUSES, EXAMPLES = {}, {}
for row in flagged:
    try:
        rec = json.loads(row["input_payload"])
    except Exception:
        rec = None
    try:
        parsed = json.loads(row["raw_model_response"])
        if not isinstance(parsed, dict):
            parsed = None
    except Exception:
        parsed = None
    for issue in (row["validation_issues"] or []):
        cat = issue.split(":")[0]
        sub = issue.split(":", 1)[1].split(" — ")[0].strip() if ":" in issue else issue
        frag = issue.split(" — ", 1)[1] if " — " in issue else None
        cause, detail = sub, ""
        if cat == "unsupported_quote" and parsed is not None and rec is not None \
                and frag and not frag.startswith("no validated"):
            cited_field = sub.split(":")[-1].strip()
            full = None
            for key in ("evidence", "treatment_type_evidence", "attribution_evidence"):
                for item in (parsed.get(key) or []):
                    if isinstance(item, dict) and isinstance(item.get("exact_quote"), str) \
                            and item["exact_quote"][:60] == frag[:60]:
                        full, cited_field = item["exact_quote"], item.get("source_field", cited_field)
                if full:
                    break
            if full is None:
                for q in (parsed.get("patient_scope_evidence") or []):
                    if isinstance(q, str) and q[:60] == frag[:60]:
                        full, cited_field = q, None
                        break
            if full is None:
                full, cause = frag, sub + "/unrecovered_full_quote"
            elif cited_field:
                cls, detail = classify_quote(full, rec, cited_field)
                cause = f"{sub}/{cls}"
                if cls == "unsupported_content":
                    r_all, ex_all = best_window(full, " ".join(field_text(rec, f) for f in ATTR_FIELDS))
                    detail = f"best fuzzy match {r_all:.2f}: {ex_all[:90]}"
            else:
                cls, detail = classify_scope = ("scope_quote_checked_all_fields", "")
                cause = f"patient_scope_evidence/{cls}"
        elif cat == "unsupported_quote" and frag and frag.startswith("no validated"):
            cause = sub + "/scope_claim_without_supported_quote"
        elif cat == "schema_error" and parsed is not None:
            field_hint = sub.split(":")[-1].strip()
            detail = f"returned: {str(parsed.get(field_hint))[:70]}" if field_hint in parsed else ""
        elif parsed is None:
            cause = sub + "/response_unparseable"
            detail = (row["raw_model_response"] or "")[:70]
        key = f"{cat}: {cause}"
        CAUSES[key] = CAUSES.get(key, 0) + 1
        EXAMPLES.setdefault(key, [])
        if len(EXAMPLES[key]) < 5:
            quoted_val = frag if frag else (detail or "")
            EXAMPLES[key].append({
                "_source_row_id": row["_source_row_id"], "name_clean": row["name_clean"],
                "returned/quote": (quoted_val or "")[:110],
                "source_field": sub.split(":")[-1] if cat == "unsupported_quote" else sub,
                "detail": detail[:120], "validation_error": issue[:110]})

print(f"\nDistinct specific causes: {len(CAUSES)} | total flagged checks: {sum(CAUSES.values())} across {len(flagged)} rows")
for k, n in sorted(CAUSES.items(), key=lambda kv: -kv[1]):
    print(f"  {n:4d}  {k}")
print()
for k in sorted(EXAMPLES, key=lambda k: -CAUSES[k]):
    print("=" * 20 + " " + k + " " + "=" * 20)
    for ex in EXAMPLES[k]:
        print(f"  {ex['_source_row_id']} | {ex['name_clean']}")
        print(f"    returned/quote : {ex['returned/quote']}")
        print(f"    source_field   : {ex['source_field']} | detail: {ex['detail']}")
        print(f"    validation_err : {ex['validation_error']}")

# COMMAND ----------

# DBTITLE 1,Cell 4 — Display counts, provenance, publish status
# Cell 4 — DISPLAY + PROVENANCE. Counts and mixed-facility counts inside both views; staging/raw
# provenance distinguishing the first (legacy, never-staged) batch from the 955 staged rows;
# run_id coverage. Read-only; no repairs executed; AI results are NOT published to any app view.
from pyspark.sql import functions as F

V = "vf.silver.national_dialysis_validated_v1"
R = "vf.silver.national_dialysis_validation_review_v1"
EXT = "vf.silver.national_dialysis_extraction_v1"
STG = "vf.silver.national_dialysis_extraction_stage_v1"

v, r = spark.table(V), spark.table(R)
print(f"validated view: {v.count()} rows | review view: {r.count()} rows (sum = {v.count() + r.count()})")
print("validated mixed-facility:", {str(x["possible_mixed_facility_record"]): x["n"] for x in v.groupBy("possible_mixed_facility_record").count().withColumnRenamed("count", "n").collect()})
print("review mixed-facility:", {str(x["possible_mixed_facility_record"]): x["n"] for x in r.groupBy("possible_mixed_facility_record").count().withColumnRenamed("count", "n").collect()})

ext, stg = spark.table(EXT), spark.table(STG)
staged_keys = stg.select("_source_row_id", "input_hash", "prompt_version", "model_endpoint")
keyed = ["_source_row_id", "input_hash", "prompt_version", "model_endpoint"]
n_staged_rows = ext.join(staged_keys, on=keyed, how="left_semi").count()
n_unstaged = ext.join(staged_keys, on=keyed, how="left_anti").count()
n_stg_unmatched = stg.join(ext.select(*keyed), on=keyed, how="left_anti").count()
print(f"\nProvenance: results rows with a staged raw response: {n_staged_rows} | without (legacy batch 1): {n_unstaged}")
print(f"staging rows total: {stg.count()} | staging rows not represented in results: {n_stg_unmatched}")
print("staging run_id coverage:", {str(x["has_run_id"]): x["n"] for x in
      stg.select(F.col("run_id").isNotNull().alias("has_run_id")).groupBy("has_run_id").count().withColumnRenamed("count", "n").collect()},
      "(NULL run_id = batch 2, staged before the run_id column was added)")
print("results with NULL raw_model_response:", ext.where(F.col("raw_model_response").isNull()).count())
print("\nAI results are NOT published to any app view. Views created here are diagnostic/internal only.")

# COMMAND ----------

# DBTITLE 1,Recommendations (not executed)
# MAGIC %md
# MAGIC # Cell 5 — RECOMMENDATIONS (narrowly scoped, deterministic — NOT executed; nothing below has been applied)
# MAGIC
# MAGIC Diagnosis of the 169 flagged rows (225 individual failing checks, 21 specific causes) shows the failures are dominated by **mechanical, evidence-verifiable issues**, not model confusion about dialysis:
# MAGIC
# MAGIC | Cause class | Checks | Evidence-based interpretation |
# MAGIC | --- | --- | --- |
# MAGIC | Quote verbatim in a DIFFERENT source field (cited field wrong) | 121 | Source-field misattribution: e.g. quote cited to `description` is verbatim in `capability` |
# MAGIC | `unspecified_dialysis` listed alongside specific types | 49 | Schema-rule violation; specific types were also returned |
# MAGIC | Altered quotation (near-verbatim, ≥0.75 fuzzy match) | 44 | Punctuation/spacing/light paraphrase vs the record text (e.g. "Has 1,000 dialysis" vs "Has 1000 dialysis services") |
# MAGIC | Scope claim with no supported quote (treatment_type_evidence) | 8 | No deterministic repair — human review |
# MAGIC | Genuinely unsupported content | 2 | No deterministic repair — human review |
# MAGIC | Response unparseable (row `70970ebb…`, also the 1 NULL-assertion row) | 1 | Raw response itself is not valid JSON; retained for review |
# MAGIC
# MAGIC **Recommended deterministic repairs (pending explicit approval; none executed here):**
# MAGIC
# MAGIC 1. **Source-field re-attribution (121 checks)** — for each failing evidence item whose `exact_quote` appears **verbatim in exactly one other allowed source field**, update `source_field` to that field via the update-only MERGE pattern already proven in notebook 11 Cell 2b (no inference, no quote-text changes, idempotent). Quotes are never rewritten; only the citation is corrected to where the quote verbatim exists.
# MAGIC 2. **Altered quotations (44 checks)** — do **not** auto-rewrite quotes. If desired later, an explicit review-approved repair could replace each with the exact substring it fuzzy-matched, clearly marked as a derived correction; default is human review of the review view.
# MAGIC 3. **`unspecified_dialysis` combined (49 checks)** — a deterministic repair could drop `unspecified_dialysis` when specific types are present (per schema rule), but this alters model output — require review approval first.
# MAGIC 4. Items 4–6 above have no evidence-supported deterministic repair; keep in `vf.silver.national_dialysis_validation_review_v1`.
# MAGIC
# MAGIC Any applied repair must be followed by re-validation with this notebook's Cell 1–3 logic; repaired rows may legitimately move between the views, and the accounting assertions will re-verify disjointness and 1,055-row coverage. **AI results remain unpublished; no repairs were executed; no validation rule was loosened.**

# COMMAND ----------

