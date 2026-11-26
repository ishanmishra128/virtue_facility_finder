# Databricks notebook source
# DBTITLE 1,Overview and rules
# MAGIC %md
# MAGIC # 11_finalize_dialysis_pilot — Finalize the Jaipur dialysis pilot from SAVED data only
# MAGIC
# MAGIC **Inputs (read-only):** `vf.silver.pilot_service_evidence_v1`, `vf.gold.dialysis_jaipur_pilot_v1`, `vf.silver.pilot_source_pages_v1`, `vf.silver.pilot_source_pages_followup_v1`, and the recorded facility address fields in `vf.silver.facilities_current_v1`.
# MAGIC
# MAGIC **Output (new, error-if-exists):** `vf.silver.pilot_evidence_publication_v1` — one row per pilot facility.
# MAGIC
# MAGIC **Rules enforced:**
# MAGIC * No new page fetching and no model/LLM calls; every statement comes from already-saved text.
# MAGIC * Compact review uses **service-page paragraphs, never navigation labels**; evidence strength is never measured by keyword counts — strongest passages are selected by deterministic content rules (facility/department-linked service or capacity statements outrank generic educational text).
# MAGIC * Evidence is **deduplicated by facility + final page URL + exact quotation**, so dynamic-HTML duplicates of the same page collapse; **underlying fetch records are preserved untouched** in both source-page tables.
# MAGIC * Monilek's dataset claim (12 machines) vs the retrieved 23-station statement is flagged as an **unresolved difference in quantities, units, or dates** — never automatically a contradiction or correction.
# MAGIC * Vaishali's 2023 statement stays a **dated historical statement**.
# MAGIC * `matching_address_text` is asserted only when facility identity, street/locality, and PIN all deterministically support it; city/PIN alone is insufficient; when uncertain, the cell abstains.
# MAGIC * Every selected quote is validated verbatim against its saved page text before the write; `review_method="automated_text_comparison"`, `human_review_status="pending"` on every row; no verified status is assigned; the Gold view is untouched.
# MAGIC
# MAGIC **Correction step (cell 3):** `vf.silver.pilot_evidence_publication_v1` is **preserved as-is** (no DROP; its Monilek row predates the capacity-rule fix). Cell 3 rebuilds the three rows from the fixed review, enforces hard pre-write assertions (3 unique record IDs; Monilek 12-machines-vs-23-stations unresolved difference present; CKS ambiguous; Vaishali 2023 evidence historical; quotes match saved text; review pending) and writes `vf.silver.pilot_evidence_publication_v2` error-if-exists. The final app view — `vf.gold.dialysis_jaipur_pilot_v2` (or `_v3` if `v2` exists) — is the original 13-record pilot view LEFT JOIN the corrected publication, adding only `retrieved_*` columns so no original column name changes. All records stay unverified / availability unknown / human review pending; the other ten records remain dataset-only.

# COMMAND ----------

# DBTITLE 1,Cell 1 — Compact per-facility review (saved data only)
# Cell 1 — Compact per-facility review from SAVED data only (no fetching, no model calls):
# recorded vs quoted branch address, strongest substantive dialysis passage (service paragraphs, not
# navigation labels; keyword counts are never evidence strength), and branch ambiguity / capacity
# differences. Evidence is deduplicated by facility + final page URL + exact quotation (dynamic-hash
# duplicates of the same page collapse); underlying fetch records are preserved untouched.
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVIDENCE_TABLE = "vf.silver.pilot_service_evidence_v1"
GOLD_VIEW = "vf.gold.dialysis_jaipur_pilot_v1"
CURRENT_VIEW = "vf.silver.facilities_current_v1"
PAGE_TABLES = ["vf.silver.pilot_source_pages_v1", "vf.silver.pilot_source_pages_followup_v1"]
TARGETS = ["Vaishali Hospital & Surgical Research Center Jaipur", "CKS Hospital",
           "Monilek Hospital and Research Center"]
FACILITY_PINS = {"CKS Hospital": "302013",
                 "Monilek Hospital and Research Center": "302004",
                 "Vaishali Hospital & Surgical Research Center Jaipur": "302021"}

ev = spark.table(EVIDENCE_TABLE)
gold = spark.table(GOLD_VIEW)
n_check = {t: gold.where(F.col("facility_name") == t).count() for t in TARGETS}
if any(c != 1 for c in n_check.values()):
    raise ValueError(f"Expected exactly 1 exact name match per target; got {n_check}. Stopping - no fuzzy matching.")
pilot = gold.where(F.col("facility_name").isin(TARGETS))
gold_rows = {r["facility_name"]: r for r in pilot.collect()}
record_ids = {f: gold_rows[f]["record_id"] for f in TARGETS}

addr_df = (spark.table(CURRENT_VIEW)
           .where(F.col("_source_row_id").isin(list(record_ids.values())))
           .select(F.col("_source_row_id").alias("record_id"), "name_clean", "address_line1",
                   "city_clean", "state_clean", "pincode_clean"))
addr_rows = {r["record_id"]: r for r in addr_df.collect()}
print(f"Evidence table {EVIDENCE_TABLE}: {ev.count()} rows, {len(ev.columns)} columns; underlying fetch records preserved")
print("Recorded facility address fields (vf.silver.facilities_current_v1):")
display(addr_df.orderBy("name_clean"))

# Deduplicate evidence by facility + final page URL + exact quotation (dynamic HTML hashes differ; the quoted text does not).
ev_rows = sorted(ev.collect(), key=lambda r: (r["facility_name"] or "", r["final_url"] or "",
                                               r["exact_quote"] or "", r["requested_url"] or "",
                                               r["retrieved_at"] or ""))
_dedup = {}
for r in ev_rows:
    _dedup.setdefault((r["facility_name"], r["final_url"], r["exact_quote"]), r)
dedup_ev = list(_dedup.values())
print(f"\nEvidence rows: {len(ev_rows)}; deduplicated by (facility, final page URL, exact quotation): {len(dedup_ev)}")

# Saved page texts keyed by (record_id, final_url) - one deterministic pick per final URL (smallest requested_url, then earliest retrieval).
page_rows = []
for tbl in PAGE_TABLES:
    if spark.catalog.tableExists(tbl):
        page_rows += (spark.table(tbl).where(F.col("fetch_status") == "success")
                      .select("record_id", "facility_name", "requested_url", "final_url",
                              "retrieved_at_utc", "extracted_text").collect())
page_rows.sort(key=lambda r: (r["record_id"] or "", r["final_url"] or "", r["requested_url"] or "",
                              r["retrieved_at_utc"] or ""))
pages_by_url = {}
for r in page_rows:
    pages_by_url.setdefault((r["record_id"], r["final_url"]), r)
print(f"Saved successful page fetches: {len(page_rows)} rows (preserved); unique (record, final URL) pages: {len(pages_by_url)}")

DIALYSIS_KW = re.compile(r"dialysis|haemodialysis|hemodialysis", re.IGNORECASE)
ADDR_TOKEN = re.compile(r"marg|road|nagar|sector|street|vihar|colony|near|plot|block", re.IGNORECASE)
NAV_LABELS = {"dialysis", "nephrology", "nephrology & dialysis", "nephrology and dialysis",
              "home/nephrology and dialysis", "icu", "radiology", "urology", "orthopaedics"}
GENERIC_DEF = re.compile(r"^(?:What is|The risk associated|Suppose a person|To whom dialysis|"
                         r"Dialysis performs the following|The dialysis treatment is offered to the person|"
                         r"Nephrology is the study)", re.IGNORECASE)
GENERIC_DEF2 = re.compile(r"^(?:Dialysis|Hemodialysis|Haemodialysis|Peritoneal dialysis) is (?:a|an|the)", re.IGNORECASE)

def is_service_paragraph(q):
    """Navigation labels, breadcrumbs, and bare department headings are not evidence."""
    s = (q or "").strip()
    return not (len(s) < 25 or s.lower() in NAV_LABELS or "Home/" in s or s.startswith("Explore"))

def is_substantive(q):
    """Generic educational/definitional boilerplate is not a facility claim."""
    return not (GENERIC_DEF.search(q or "") or GENERIC_DEF2.search(q or ""))

def passage_tier(q):
    """Deterministic strength tiers (content rules only, never keyword counts):
    0 = dialysis-family mention tied to the facility/department as a service or capacity statement,
    1 = dialysis-family capacity claim (station/bed/machine counts), 2 = other dialysis-family text,
    3 = nephrology-only text (never treated as dialysis)."""
    if not DIALYSIS_KW.search(q):
        return 3
    has_service = re.search(r"offer|provid|perform|available|care through|unit|24 hour|emergency|station|expanded", q, re.IGNORECASE)
    has_subject = re.search(r"department|hospital|our |centre|center|facility|ward|ICU", q, re.IGNORECASE)
    if has_service and has_subject:
        return 0
    if re.search(r"\b\d+\s*(?:station|bed|machine)", q, re.IGNORECASE):
        return 1
    return 2

def strongest_passage(facility):
    cands = [r for r in dedup_ev
             if r["facility_name"] == facility
             and r["proposed_support_type"] in ("explicit_dialysis_mention", "nephrology_only")
             and is_service_paragraph(r["exact_quote"]) and is_substantive(r["exact_quote"])]
    cands = sorted(cands, key=lambda r: (passage_tier(r["exact_quote"]),
                                         0 if re.search(r"dialysis|nephro", r["final_url"] or "") else 1,
                                         r["final_url"] or "", r["source_location"] or ""))
    return cands[0] if cands else None

def pick_address_quote(facility):
    """Deterministic quoted branch address: prefer a paragraph containing the facility PIN and address
    tokens; fall back to an identity + street/locality statement. Never invents text."""
    rid, pin = record_ids[facility], FACILITY_PINS[facility]
    fw = facility.split()[0].lower()
    cands = []
    for (r_id, furl), r in pages_by_url.items():
        if r_id != rid:
            continue
        for i, p in enumerate((r["extracted_text"] or "").split("\n")):
            if not p.strip():
                continue
            pl = p.lower()
            n_tok = len(ADDR_TOKEN.findall(p))
            if pin in p and (n_tok > 0 or fw in pl):
                cands.append(((2, n_tok, len(p)), furl, i, p, r["retrieved_at_utc"]))
            elif n_tok > 0 and fw in pl:
                cands.append(((1, n_tok, len(p)), furl, i, p, r["retrieved_at_utc"]))
    if not cands:
        return None
    cands.sort(key=lambda c: (tuple(-x for x in c[0]), c[1], c[2]))
    return cands[0]

NUM_UNIT = re.compile(r"\b(\d+)\s*(?:station|bed(?:ded)?|machine)s?\b", re.IGNORECASE)

def dataset_capacity_numbers(facility):
    """Numbers in dataset dialysis-related quotations (e.g. '12 Japanese Toray dialysis machines' -
    the number need not sit directly next to the unit word in the dataset text)."""
    nums = {}
    for e in (gold_rows[facility]["evidence_quotations"] or []):
        q = e["exact_quote"] or ""
        if DIALYSIS_KW.search(q) or "toray" in q.lower():
            for m in re.finditer(r"\b(\d+)\b", q):
                nums.setdefault(int(m.group(1)), q)
    return nums

def page_capacity_numbers(facility):
    rid = record_ids[facility]
    nums = {}
    for (r_id, furl), r in pages_by_url.items():
        if r_id != rid:
            continue
        for p in (r["extracted_text"] or "").split("\n"):
            if DIALYSIS_KW.search(p):
                for m in NUM_UNIT.finditer(p):
                    nums.setdefault(int(m.group(1)), p)
    return nums

review_rows = []
for facility in TARGETS:
    g = gold_rows[facility]
    rid = record_ids[facility]
    a = addr_rows.get(rid)
    recorded = " | ".join(str(x) for x in [a["address_line1"], a["city_clean"], a["state_clean"],
                                             a["pincode_clean"]] if x) if a else None
    sp = strongest_passage(facility)
    aq = pick_address_quote(facility)
    dnums, pnums = dataset_capacity_numbers(facility), page_capacity_numbers(facility)
    capacity_note = None
    if dnums and pnums and not (set(dnums) & set(pnums)):
        capacity_note = ("Unresolved difference in quantities, units, or dates (NOT automatically a "
                         "contradiction or correction): dataset dialysis-related quotation mentions "
                         f"{sorted(dnums)}; retrieved pages mention {sorted(pnums)}.")
    amb = []
    if g["possible_mixed_facility_record"]:
        ae = "; ".join((e["exact_quote"] or "") for e in (g["attribution_evidence"] or []))
        amb.append(f"possible_mixed_facility_record=True; attribution evidence: {ae}")
    if g["needs_attribution_review"]:
        amb.append("needs_attribution_review=True")
    review_rows.append({
        "facility_name": facility, "record_id": rid,
        "recorded_address": recorded,
        "quoted_branch_address": (aq[3] if aq else None),
        "address_quote_url": (aq[1] if aq else None),
        "address_quote_retrieved_at": (aq[4] if aq else None),
        "strongest_passage": (sp["exact_quote"] if sp else None),
        "strongest_passage_url": (sp["final_url"] if sp else None),
        "strongest_retrieved_at": (sp["retrieved_at"] if sp else None),
        "temporal_context": (sp["temporal_context"] if sp else None),
        "branch_ambiguity": ("; ".join(amb) if amb else None),
        "capacity_difference": capacity_note,
    })

REVIEW_SCHEMA = T.StructType([
    T.StructField("facility_name", T.StringType()), T.StructField("record_id", T.StringType()),
    T.StructField("recorded_address", T.StringType()), T.StructField("quoted_branch_address", T.StringType()),
    T.StructField("address_quote_url", T.StringType()), T.StructField("address_quote_retrieved_at", T.StringType()),
    T.StructField("strongest_passage", T.StringType()),
    T.StructField("strongest_passage_url", T.StringType()), T.StructField("strongest_retrieved_at", T.StringType()),
    T.StructField("temporal_context", T.StringType()), T.StructField("branch_ambiguity", T.StringType()),
    T.StructField("capacity_difference", T.StringType())])
display(spark.createDataFrame(review_rows, REVIEW_SCHEMA).orderBy("facility_name"))

print("\nNotes: selections above use service-page paragraphs (navigation labels excluded) and deterministic")
print("content rules; keyword counts were never used as evidence strength. Vaishali's 2023 statement, if")
print("selected, keeps temporal_context=dated_historical_statement. No table has been modified yet.")

# COMMAND ----------

# DBTITLE 1,Cell 2 — Publish pilot_evidence_publication_v1 (error-if-exists)
# Cell 2 — Create vf.silver.pilot_evidence_publication_v1 (one row per facility, error-if-exists) with
# deterministic branch assessment, quote validation against saved page text, and human review pending.
# matching_address_text requires facility identity + street/locality + PIN support; city/PIN alone is
# insufficient; when uncertain, abstain. No verified status; the Gold view is untouched.
from pyspark.sql import functions as F
from pyspark.sql import types as T

PUB_TABLE = "vf.silver.pilot_evidence_publication_v1"
if spark.catalog.tableExists(PUB_TABLE):
    raise ValueError(f"{PUB_TABLE} already exists; stopping without overwriting or modifying it.")

STOP_TOKS = {"jaipur", "rajasthan", "india", "road", "nagar", "sector", "near", "hospital", "centre", "center"}

def quote_in_saved_text(record_id, final_url, quote):
    r = pages_by_url.get((record_id, final_url))
    return bool(r and quote and (quote in (r["extracted_text"] or "")))

def assess_branch(facility, review):
    """Deterministic address assessment; abstains whenever identity + street/locality + PIN are not all supported."""
    g, rid = gold_rows[facility], record_ids[facility]
    a = addr_rows.get(rid)
    pin = FACILITY_PINS[facility]
    fw = facility.split()[0].lower()
    quoted = review["quoted_branch_address"]
    if g["possible_mixed_facility_record"]:
        return "ambiguous", ("Gold record is flagged possible_mixed_facility_record with attribution evidence "
                              "referencing another facility location; automated text comparison abstains.")
    if not quoted:
        return "insufficient_evidence", "No address-like paragraph with facility identity found on the retrieved pages."
    ql = quoted.lower()
    page_pins = set(re.findall(r"\b[1-9][0-9]{5}\b", quoted))
    if page_pins and pin not in page_pins:
        return "conflicting_address_text", (f"Quoted page address contains PIN {sorted(page_pins)[0]} while the dataset "
                                            f"record has {pin}; conflicting address text.")
    recorded_line = (a["address_line1"] or "") if a else ""
    toks = [t for t in re.split(r"[^a-z0-9]+", recorded_line.lower()) if len(t) >= 4 and t not in STOP_TOKS]
    overlap = [t for t in dict.fromkeys(toks) if t in ql]
    pin_in_recorded = bool(a and a["pincode_clean"] == pin)
    if recorded_line and (fw in ql) and (pin in quoted) and overlap and pin_in_recorded:
        return "matching_address_text", (f"Facility identity ('{fw}'), PIN {pin}, and street/locality "
                                          f"({', '.join(overlap)}) all appear in the quoted page address — "
                                          "deterministic text match (city/PIN alone were not used).")
    if recorded_line and overlap and (fw in ql) and not (pin in quoted):
        return "ambiguous", ("Identity and street/locality appear in the quoted text but the PIN is not present; "
                             "a deterministic match requires identity + street/locality + PIN — abstaining.")
    if not recorded_line:
        return "insufficient_evidence", ("Dataset address_line1 is unavailable; street/locality comparison cannot "
                                        "be performed — abstaining.")
    return "ambiguous", "Partial support only (identity, street/locality, and PIN are not all present) — abstaining."

# Validate every selected quote against its saved page text BEFORE building the write.
validation = []
for review in review_rows:
    rid = review["record_id"]
    ok_strong = quote_in_saved_text(rid, review["strongest_passage_url"], review["strongest_passage"])
    ok_addr = quote_in_saved_text(rid, review["address_quote_url"], review["quoted_branch_address"])
    validation.append({"facility_name": review["facility_name"], "quote_kind": "strongest_dialysis_passage",
                       "ok": ok_strong})
    validation.append({"facility_name": review["facility_name"], "quote_kind": "quoted_branch_address",
                       "ok": ok_addr})
val_df = spark.createDataFrame(validation, T.StructType([
    T.StructField("facility_name", T.StringType()), T.StructField("quote_kind", T.StringType()),
    T.StructField("ok", T.BooleanType())]))
n_ok = val_df.where(F.col("ok")).count()
n_all = val_df.count()
print(f"Quote validation against saved page text: {n_ok}/{n_all} PASS")
display(val_df.orderBy("facility_name", "quote_kind"))
if n_ok != n_all:
    raise ValueError("Quote validation failed; refusing to write the publication table.")

# Build one publication row per facility.
pub_rows = []
for review in review_rows:
    facility = review["facility_name"]
    assessment, rationale = assess_branch(facility, review)
    issues = []
    if review["capacity_difference"]:
        issues.append(review["capacity_difference"])
    if review["branch_ambiguity"]:
        issues.append(f"Branch identity unresolved: {review['branch_ambiguity']}")
    if review["temporal_context"] == "dated_historical_statement":
        issues.append("Strongest substantive passage is a dated historical statement; current availability is not established.")
    if assessment != "matching_address_text":
        issues.append(f"Branch address assessment: {assessment} — pending human review.")
    pub_rows.append({
        "record_id": review["record_id"], "facility_name": facility,
        "recorded_address": review["recorded_address"],
        "strongest_dialysis_quote": review["strongest_passage"],
        "strongest_quote_url": review["strongest_passage_url"],
        "strongest_quote_retrieved_at": review["strongest_retrieved_at"],
        "temporal_context": review["temporal_context"],
        "quoted_branch_address": review["quoted_branch_address"],
        "address_quote_url": review["address_quote_url"],
        "address_quote_retrieved_at": review["address_quote_retrieved_at"],
        "branch_assessment": assessment, "assessment_rationale": rationale,
        "unresolved_issues": (" | ".join(issues) if issues else None),
        "review_method": "automated_text_comparison", "human_review_status": "pending",
    })

PUB_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType()), T.StructField("facility_name", T.StringType()),
    T.StructField("recorded_address", T.StringType()),
    T.StructField("strongest_dialysis_quote", T.StringType()),
    T.StructField("strongest_quote_url", T.StringType()),
    T.StructField("strongest_quote_retrieved_at", T.StringType()),
    T.StructField("temporal_context", T.StringType()),
    T.StructField("quoted_branch_address", T.StringType()),
    T.StructField("address_quote_url", T.StringType()),
    T.StructField("address_quote_retrieved_at", T.StringType()),
    T.StructField("branch_assessment", T.StringType()),
    T.StructField("assessment_rationale", T.StringType()),
    T.StructField("unresolved_issues", T.StringType()),
    T.StructField("review_method", T.StringType()),
    T.StructField("human_review_status", T.StringType())])
pub_df = spark.createDataFrame(pub_rows, PUB_SCHEMA)
pub_df.write.mode("error").saveAsTable(PUB_TABLE)
print(f"\nWrote {PUB_TABLE}: {spark.table(PUB_TABLE).count()} rows (one per facility, error-if-exists).")

# Post-write checks: statuses fixed, quote validation re-run from the saved table, Gold view untouched.
saved = spark.table(PUB_TABLE)
print("Review method / status values fixed:")
display(saved.groupBy("review_method", "human_review_status").count())
n_ok2 = sum(1 for row in saved.collect()
            for kind in [("strongest_dialysis_quote", "strongest_quote_url"),
                         ("quoted_branch_address", "address_quote_url")]
            if quote_in_saved_text(row["record_id"], row[kind[1]], row[kind[0]]))
print(f"Quote validation re-run from the saved publication table: {n_ok2}/{2 * saved.count()} PASS")
gold_now = spark.table(GOLD_VIEW).where(F.col("facility_name").isin(TARGETS)).count()
print(f"Gold view untouched: {gold_now} pilot records still present in {GOLD_VIEW}; no status was changed anywhere.")
display(saved.select("facility_name", "recorded_address", "quoted_branch_address", "branch_assessment",
                    "temporal_context", "strongest_quote_url", "address_quote_url",
                    "unresolved_issues", "human_review_status").orderBy("facility_name"))

# COMMAND ----------

# DBTITLE 1,Cell 3 — Corrected publication v2 + final app view
# Cell 3 — Corrected publication v2 + final app view (v1 PRESERVED; no deletions anywhere).
# If vf.silver.pilot_evidence_publication_v2 does not exist: rebuild the three rows from the fixed
# cell-1 review, assert every guard, write error-if-exists. If it already exists (written by an
# earlier run of this corrected cell): load the saved rows and re-run the same assertions against
# them — the table is never overwritten. The app view is created by inline SQL over persistent
# tables only (no temp views), as v2 or v3, error-if-exists. No fetching, no model calls.
from pyspark.sql import functions as F
from pyspark.sql import types as T

PUB_V1 = "vf.silver.pilot_evidence_publication_v1"          # preserved as-is per instruction (no DROP)
PUB_V2 = "vf.silver.pilot_evidence_publication_v2"
GOLD_V1 = GOLD_VIEW                                       # vf.gold.dialysis_jaipur_pilot_v1 (13 records)
GOLD_V2 = "vf.gold.dialysis_jaipur_pilot_v2"
GOLD_V3 = "vf.gold.dialysis_jaipur_pilot_v3"

STOP_TOKS = {"jaipur", "rajasthan", "india", "road", "nagar", "sector", "near", "hospital", "centre", "center"}

def quote_in_saved_text(record_id, final_url, quote):
    r = pages_by_url.get((record_id, final_url))
    return bool(r and quote and (quote in (r["extracted_text"] or "")))

def assess_branch(facility, review):
    """Same deterministic assessment as cell 2: abstain unless identity + street/locality + PIN all support a match."""
    g, rid = gold_rows[facility], record_ids[facility]
    a = addr_rows.get(rid)
    pin = FACILITY_PINS[facility]
    fw = facility.split()[0].lower()
    quoted = review["quoted_branch_address"]
    if g["possible_mixed_facility_record"]:
        return "ambiguous", ("Gold record is flagged possible_mixed_facility_record with attribution evidence "
                              "referencing another facility location; automated text comparison abstains.")
    if not quoted:
        return "insufficient_evidence", "No address-like paragraph with facility identity found on the retrieved pages."
    ql = quoted.lower()
    page_pins = set(re.findall(r"\b[1-9][0-9]{5}\b", quoted))
    if page_pins and pin not in page_pins:
        return "conflicting_address_text", (f"Quoted page address contains PIN {sorted(page_pins)[0]} while the dataset "
                                            f"record has {pin}; conflicting address text.")
    recorded_line = (a["address_line1"] or "") if a else ""
    toks = [t for t in re.split(r"[^a-z0-9]+", recorded_line.lower()) if len(t) >= 4 and t not in STOP_TOKS]
    overlap = [t for t in dict.fromkeys(toks) if t in ql]
    pin_in_recorded = bool(a and a["pincode_clean"] == pin)
    if recorded_line and (fw in ql) and (pin in quoted) and overlap and pin_in_recorded:
        return "matching_address_text", (f"Facility identity ('{fw}'), PIN {pin}, and street/locality "
                                          f"({', '.join(overlap)}) all appear in the quoted page address — "
                                          "deterministic text match (city/PIN alone were not used).")
    if recorded_line and overlap and (fw in ql) and not (pin in quoted):
        return "ambiguous", ("Identity and street/locality appear in the quoted text but the PIN is not present; "
                             "a deterministic match requires identity + street/locality + PIN — abstaining.")
    if not recorded_line:
        return "insufficient_evidence", ("Dataset address_line1 is unavailable; street/locality comparison cannot "
                                        "be performed — abstaining.")
    return "ambiguous", "Partial support only (identity, street/locality, and PIN are not all present) — abstaining."

# ---- Build (or load the already-saved) publication rows ----
created_now = not spark.catalog.tableExists(PUB_V2)
if created_now:
    pub_rows = []
    for review in review_rows:
        facility = review["facility_name"]
        assessment, rationale = assess_branch(facility, review)
        issues = []
        if review["capacity_difference"]:
            issues.append(review["capacity_difference"])
        if review["branch_ambiguity"]:
            issues.append(f"Branch identity unresolved: {review['branch_ambiguity']}")
        if review["temporal_context"] == "dated_historical_statement":
            issues.append("Strongest substantive passage is a dated historical statement; current availability is not established.")
        if assessment != "matching_address_text":
            issues.append(f"Branch address assessment: {assessment} — pending human review.")
        pub_rows.append({
            "record_id": review["record_id"], "facility_name": facility,
            "recorded_address": review["recorded_address"],
            "strongest_dialysis_quote": review["strongest_passage"],
            "strongest_quote_url": review["strongest_passage_url"],
            "strongest_quote_retrieved_at": review["strongest_retrieved_at"],
            "temporal_context": review["temporal_context"],
            "quoted_branch_address": review["quoted_branch_address"],
            "address_quote_url": review["address_quote_url"],
            "address_quote_retrieved_at": review["address_quote_retrieved_at"],
            "branch_assessment": assessment, "assessment_rationale": rationale,
            "unresolved_issues": (" | ".join(issues) if issues else None),
            "review_method": "automated_text_comparison", "human_review_status": "pending",
        })
    origin = "rebuilt from the fixed cell-1 review"
else:
    pub_rows = [r.asDict() for r in spark.table(PUB_V2).collect()]
    origin = f"loaded from the existing saved table {PUB_V2} (never overwritten)"

# ---- Assertions (run on pub_rows regardless of origin) ----
assert len(pub_rows) == 3 and len({r["record_id"] for r in pub_rows}) == 3, "expected exactly 3 unique record IDs"
by_name = {r["facility_name"]: r for r in pub_rows}
mon = by_name["Monilek Hospital and Research Center"]
cks = by_name["CKS Hospital"]
vai = by_name["Vaishali Hospital & Surgical Research Center Jaipur"]
assert mon["unresolved_issues"] and "12" in mon["unresolved_issues"] and "23" in mon["unresolved_issues"] \
       and "unresolved difference in quantities, units, or dates" in mon["unresolved_issues"].lower() \
       and "not automatically a contradiction or correction" in mon["unresolved_issues"].lower(), \
       "Monilek unresolved 12-vs-23 issue missing"
assert cks["branch_assessment"] == "ambiguous", "CKS must remain ambiguous"
assert vai["temporal_context"] == "dated_historical_statement" \
       and (vai["strongest_dialysis_quote"] or "").startswith("In 2023"), "Vaishali 2023 evidence must remain historical"
qchecks = []
for r in pub_rows:
    qchecks.append(quote_in_saved_text(r["record_id"], r["strongest_quote_url"], r["strongest_dialysis_quote"]))
    qchecks.append(quote_in_saved_text(r["record_id"], r["address_quote_url"], r["quoted_branch_address"]))
assert all(qchecks), "a selected quote does not match its saved source text"
assert all(r["human_review_status"] == "pending" for r in pub_rows), "human review must remain pending"
assert all(r["review_method"] == "automated_text_comparison" for r in pub_rows)
print(f"Assertions passed on rows {origin}: 3 unique record IDs; Monilek 12-vs-23 unresolved issue present; "
      f"CKS ambiguous; Vaishali 2023 historical; quote checks {sum(qchecks)}/{len(qchecks)} PASS; review pending.")

if created_now:
    PUB_SCHEMA = T.StructType([
        T.StructField("record_id", T.StringType()), T.StructField("facility_name", T.StringType()),
        T.StructField("recorded_address", T.StringType()), T.StructField("strongest_dialysis_quote", T.StringType()),
        T.StructField("strongest_quote_url", T.StringType()), T.StructField("strongest_quote_retrieved_at", T.StringType()),
        T.StructField("temporal_context", T.StringType()), T.StructField("quoted_branch_address", T.StringType()),
        T.StructField("address_quote_url", T.StringType()), T.StructField("address_quote_retrieved_at", T.StringType()),
        T.StructField("branch_assessment", T.StringType()), T.StructField("assessment_rationale", T.StringType()),
        T.StructField("unresolved_issues", T.StringType()),
        T.StructField("review_method", T.StringType()), T.StructField("human_review_status", T.StringType())])
    spark.createDataFrame(pub_rows, PUB_SCHEMA).write.mode("error").saveAsTable(PUB_V2)
    print(f"Wrote {PUB_V2}: {spark.table(PUB_V2).count()} rows (error-if-exists).")
else:
    print(f"{PUB_V2} preserved ({spark.table(PUB_V2).count()} rows); v1 also preserved untouched at {PUB_V1} (no DROP used).")

# ---- App view: v2 if absent, else v3; stop if both exist ----
if not spark.catalog.tableExists(GOLD_V2):
    app_target = GOLD_V2
elif not spark.catalog.tableExists(GOLD_V3):
    app_target = GOLD_V3
else:
    raise ValueError(f"Both {GOLD_V2} and {GOLD_V3} exist; stopping without modifying either.")

g1 = spark.table(GOLD_V1)
new_cols = ["retrieved_evidence_quote", "retrieved_evidence_url", "retrieved_evidence_retrieved_at",
            "retrieved_temporal_context", "retrieved_branch_address_quote", "retrieved_branch_address_url",
            "retrieved_branch_assessment", "retrieved_unresolved_issues"]
assert not (set(new_cols) & set(g1.columns)), f"nonconflicting-name check failed: {set(new_cols) & set(g1.columns)}"

app_sql = f"""
CREATE VIEW {app_target} AS
SELECT g.*,
       p.retrieved_evidence_quote, p.retrieved_evidence_url, p.retrieved_evidence_retrieved_at,
       p.retrieved_temporal_context, p.retrieved_branch_address_quote, p.retrieved_branch_address_url,
       p.retrieved_branch_assessment, p.retrieved_unresolved_issues
FROM {GOLD_V1} g
LEFT JOIN (
    SELECT record_id,
           strongest_dialysis_quote AS retrieved_evidence_quote,
           strongest_quote_url AS retrieved_evidence_url,
           strongest_quote_retrieved_at AS retrieved_evidence_retrieved_at,
           temporal_context AS retrieved_temporal_context,
           quoted_branch_address AS retrieved_branch_address_quote,
           address_quote_url AS retrieved_branch_address_url,
           branch_assessment AS retrieved_branch_assessment,
           unresolved_issues AS retrieved_unresolved_issues
    FROM {PUB_V2}
) p ON g.record_id = p.record_id
"""
spark.sql(app_sql)  # CREATE VIEW fails if the view already exists -> error-if-exists
print(f"\nCreated app view {app_target} (error-if-exists honored; persistent tables only, no temp views).")

# ---- Persisted-output verification ----
av = spark.table(app_target)
n_rows, n_ids = av.count(), av.select("record_id").distinct().count()
n_match = av.where(F.col("retrieved_evidence_url").isNotNull()).count()
n_only = av.where(F.col("retrieved_evidence_url").isNull()).count()
assert (n_rows, n_ids) == (13, 13), "app view must contain 13 unique records"
assert n_match == 3 and n_only == 10, "expected exactly 3 publication matches and 10 dataset-only records"
mon_row = av.where(F.col("facility_name") == "Monilek Hospital and Research Center").collect()[0]
assert mon_row["retrieved_unresolved_issues"] and "12" in mon_row["retrieved_unresolved_issues"] \
       and "23" in mon_row["retrieved_unresolved_issues"], "Monilek issue missing from final app view"
orig = g1.columns
diff1 = g1.select(*orig).exceptAll(av.select(*orig)).count()
diff2 = av.select(*orig).exceptAll(g1.select(*orig)).count()
assert diff1 == 0 and diff2 == 0, "original fields/statuses changed in the app view"
n_quote_ok = 0
for row in av.where(F.col("retrieved_evidence_url").isNotNull()).collect():
    n_quote_ok += quote_in_saved_text(row["record_id"], row["retrieved_evidence_url"], row["retrieved_evidence_quote"])
    n_quote_ok += quote_in_saved_text(row["record_id"], row["retrieved_branch_address_url"], row["retrieved_branch_address_quote"])
assert n_quote_ok == 6, "quote checks failed on the persisted app view"
status_cols = [c for c in orig if re.search(r"status|review", c, re.IGNORECASE)]
for c in status_cols:
    vals = sorted({str(r[c]) for r in av.select(c).distinct().collect()})
    assert "verified" not in vals, f"{c} must not contain 'verified'"
    print(f"  {c}: {vals}")
print(f"\nVerification: 13 unique app records; exactly {n_match} publication matches ({n_only} dataset-only); "
      f"Monilek 12-vs-23 issue present; original fields/statuses unchanged (0 differences both ways); "
      f"quote checks {n_quote_ok}/6 PASS; v1 publication and v1 gold view untouched.")

# ---- Final output for the frontend developer ----
print(f"\nFINAL APP VIEW NAME: {app_target}")
print(f"Columns: {len(av.columns)} (13 records)")
schema_rows = [{"column": i, "name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable}
               for i, f in enumerate(av.schema.fields, 1)]
display(spark.createDataFrame(schema_rows, T.StructType([
    T.StructField("column", T.IntegerType()), T.StructField("name", T.StringType()),
    T.StructField("type", T.StringType()), T.StructField("nullable", T.BooleanType())])))