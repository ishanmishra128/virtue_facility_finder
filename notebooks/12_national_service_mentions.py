# Databricks notebook source
# DBTITLE 1,Overview and rules
# MAGIC %md
# MAGIC # 12_national_service_mentions — SQL-only service mention index (no inference)
# MAGIC
# MAGIC **Databricks SQL only.** No `ai_query`, no Python inference, no website fetching, no changes to any existing extraction job or table (dialysis/national extraction tables are never read or written here).
# MAGIC
# MAGIC Inputs: `vf.silver.facilities_current_v1` LEFT JOINed with review reasons from `vf.silver.facilities_review_queue_v1` on `_source_row_id` (schemas inspected first; join shown not to multiply rows).
# MAGIC
# MAGIC **Long-form mention index** over `description_clean`, `capability_clean`, `procedure_clean`, `equipment_clean`, `specialties_clean`: string fields that are valid JSON arrays are decoded element-by-element with position; actual arrays (`specialties_clean`) are exploded directly with position; all other fields retain their full text as one element. Exact source text is preserved.
# MAGIC
# MAGIC **Service codes and patterns** come from a small explicit SQL `VALUES` mapping (case-insensitive, applied to lowercased text; camel-case specialty labels covered by unanchored substring patterns): `dialysis` (with modality tags kept separately: hemodialysis/haemodialysis, peritoneal dialysis, CRRT, continuous renal replacement therapy, hemofiltration/haemofiltration), `cesarean_delivery` (spelling variants + c-section + LSCS), `NICU`, `cardiac_surgery` (incl. open-heart surgery, CABG, cardiothoracic surgery), `MRI`, `burn_care`.
# MAGIC
# MAGIC **Broad proxies are deliberately excluded:** nephrology is NOT dialysis; cardiology is NOT cardiac surgery; radiology is NOT MRI; ICU is NOT NICU; plain "section" is not a C-section; "bypass surgery" is not matched.
# MAGIC
# MAGIC **Outputs (error-if-exists, never overwritten):**
# MAGIC - `vf.silver.service_mentions_v1` — one row per source record + service + source field + element position, with exact evidence text, matched terms (deduplicated within the grain), source field, matching-rule version, timestamp, and the label "Unverified text mention; context may negate or qualify it." on every row.
# MAGIC - `vf.gold.facility_service_mentions_v1` — compact app view, one row per record/service, aggregating evidence structs and attaching facility name, location, coordinates/status, source URL candidates (kept unattributed), and review reasons. `verification_status='unverified'`, `current_availability_status='unknown'`. No model responses and no reviewer identities are exposed.
# MAGIC
# MAGIC **No inference is performed:** negation, current delivery, branch attribution, patient eligibility, and verified availability are NOT determined. A C-section mention does NOT establish emergency C-section capability. No mention means unknown, not unavailable.

# COMMAND ----------

# DBTITLE 1,Cell 1 — Inspect input schemas
# MAGIC %sql
# MAGIC -- Cell 1 — Inspect input schemas first (read-only): the columns this build relies on,
# MAGIC -- including the actual element types of the five text fields (specialties_clean is
# MAGIC -- array<string>; the other four are strings that may hold JSON arrays).
# MAGIC SELECT table_name, column_name, data_type
# MAGIC FROM vf.information_schema.columns
# MAGIC WHERE table_schema = 'silver'
# MAGIC   AND table_name IN ('facilities_current_v1', 'facilities_review_queue_v1')
# MAGIC   AND column_name IN (
# MAGIC     '_source_row_id', 'name_clean', 'city_clean', 'state_clean', 'pincode_clean',
# MAGIC     'latitude', 'longitude', 'coordinate_status', 'source_url_candidates',
# MAGIC     'description_clean', 'capability_clean', 'procedure_clean', 'equipment_clean', 'specialties_clean',
# MAGIC     'review_priority', 'review_reason_codes')
# MAGIC ORDER BY table_name, column_name;

# COMMAND ----------

# DBTITLE 1,Cell 2 — Input count and join integrity
# MAGIC %sql
# MAGIC -- Cell 2 — Input record count and LEFT JOIN integrity: the review-queue join must not
# MAGIC -- multiply rows (fail loudly here, before anything is written).
# MAGIC SELECT
# MAGIC   c.input_records,
# MAGIC   j.joined_records,
# MAGIC   CASE WHEN c.input_records = j.joined_records
# MAGIC        THEN 'PASS: LEFT JOIN adds review reasons without row multiplication'
# MAGIC        ELSE 'FAIL: review-queue join multiplied rows - stop' END AS join_integrity
# MAGIC FROM
# MAGIC   (SELECT count(*) AS input_records FROM vf.silver.facilities_current_v1) c,
# MAGIC   (SELECT count(*) AS joined_records
# MAGIC      FROM vf.silver.facilities_current_v1 f
# MAGIC      LEFT JOIN (SELECT _source_row_id, review_priority, review_reason_codes
# MAGIC                FROM vf.silver.facilities_review_queue_v1) q
# MAGIC        ON f._source_row_id = q._source_row_id) j;

# COMMAND ----------

# DBTITLE 1,Cell 3 — Service patterns (VALUES)
# MAGIC %sql
# MAGIC -- Cell 3 — Service codes and case-insensitive regex patterns (small explicit VALUES mapping).
# MAGIC -- Patterns are applied to LOWERCASED element text; no backslash escapes are used anywhere.
# MAGIC -- Deliberate proxy exclusions: nephrology≠dialysis, cardiology≠cardiac surgery, radiology≠MRI,
# MAGIC -- ICU≠NICU, plain 'section'≠c-section, 'bypass surgery' alone≠cardiac surgery.
# MAGIC CREATE OR REPLACE TEMP VIEW service_mention_patterns AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('dialysis',        'dialysis',                              'dialysis'),
# MAGIC   ('dialysis',        'hemodialysis_or_haemodialysis',         'hae?mo[- ]?dialysis'),
# MAGIC   ('dialysis',        'peritoneal_dialysis',                   'peritoneal[- ]?dialysis'),
# MAGIC   ('dialysis',        'crrt',                                  '(^|[^a-z0-9])crrt([^a-z0-9]|$)'),
# MAGIC   ('dialysis',        'continuous_renal_replacement_therapy',  'continuous[ -]?renal[ -]?replacement[ -]?therapy'),
# MAGIC   ('dialysis',        'hemofiltration_or_haemofiltration',    'hae?mo[- ]?filtration'),
# MAGIC   ('cesarean_delivery','cesarean_spelling_variant',            '(cesarean|caesarean|cesarian|caesarian)'),
# MAGIC   ('cesarean_delivery','c_section',                            '(^|[^a-z])c[- ]?section'),
# MAGIC   ('cesarean_delivery','lscs',                                 '(^|[^a-z0-9])lscs([^a-z0-9]|$)'),
# MAGIC   ('NICU',            'nicu',                                  'nicu'),
# MAGIC   ('NICU',            'neonatal_intensive_care',               'neonatal[ -]?intensive[ -]?care'),
# MAGIC   ('NICU',            'neonatal_icu',                          'neonatal[ -]?icu'),
# MAGIC   ('cardiac_surgery', 'cardiac_surgery',                       'cardiac[ -]?(surgery|surgical|surgeon)'),
# MAGIC   ('cardiac_surgery', 'open_heart_surgery',                    'open[ -]?heart[ -]?(surgery|surgical|surgeon)'),
# MAGIC   ('cardiac_surgery', 'heart_surgery',                         'heart[ -]?(surgery|surgeon)'),
# MAGIC   ('cardiac_surgery', 'cabg',                                  '(^|[^a-z0-9])cabg([^a-z0-9]|$)'),
# MAGIC   ('cardiac_surgery', 'cardiothoracic_surgery',                'cardiothoracic[ -]?(surgery|surgical|surgeon)'),
# MAGIC   ('MRI',             'mri',                                   'mri'),
# MAGIC   ('MRI',             'mri_dotted',                            'm[.]r[.]i'),
# MAGIC   ('burn_care',       'burn_care_context',                     'burns?[ -]?(unit|units|care|centre|center|ward|wards|department|dept|icu|beds|bed|treatment|trauma|surgery|surgical)')
# MAGIC AS t(service_code, term_label, pattern);
# MAGIC
# MAGIC SELECT service_code, count(*) AS pattern_count FROM service_mention_patterns GROUP BY service_code ORDER BY service_code;

# COMMAND ----------

# DBTITLE 1,Cell 4 — Element normalization (decode/explode)
# MAGIC %sql
# MAGIC -- Cell 4 — Element normalization (long-form input): string fields that are VALID JSON arrays
# MAGIC -- are decoded element-by-element (position preserved); the actual array<string> field
# MAGIC -- (specialties_clean) is exploded directly; all other field text is retained in full.
# MAGIC -- Exact source text is preserved; NULL elements are dropped; NULL fields yield no rows.
# MAGIC CREATE OR REPLACE TEMP VIEW mention_elements AS
# MAGIC SELECT u._source_row_id, u.source_field, u.element_kind,
# MAGIC        e.pos AS element_position, e.element_text AS evidence_text
# MAGIC FROM (
# MAGIC   SELECT _source_row_id, 'description_clean' AS source_field,
# MAGIC          CASE WHEN description_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(description_clean, 'array<string>') IS NOT NULL THEN 'json_array_element'
# MAGIC               ELSE 'full_text' END AS element_kind,
# MAGIC          CASE WHEN description_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(description_clean, 'array<string>') IS NOT NULL THEN from_json(description_clean, 'array<string>')
# MAGIC               ELSE array(description_clean) END AS elements
# MAGIC   FROM vf.silver.facilities_current_v1
# MAGIC   UNION ALL
# MAGIC   SELECT _source_row_id, 'capability_clean',
# MAGIC          CASE WHEN capability_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(capability_clean, 'array<string>') IS NOT NULL THEN 'json_array_element'
# MAGIC               ELSE 'full_text' END,
# MAGIC          CASE WHEN capability_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(capability_clean, 'array<string>') IS NOT NULL THEN from_json(capability_clean, 'array<string>')
# MAGIC               ELSE array(capability_clean) END
# MAGIC   FROM vf.silver.facilities_current_v1
# MAGIC   UNION ALL
# MAGIC   SELECT _source_row_id, 'procedure_clean',
# MAGIC          CASE WHEN procedure_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(procedure_clean, 'array<string>') IS NOT NULL THEN 'json_array_element'
# MAGIC               ELSE 'full_text' END,
# MAGIC          CASE WHEN procedure_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(procedure_clean, 'array<string>') IS NOT NULL THEN from_json(procedure_clean, 'array<string>')
# MAGIC               ELSE array(procedure_clean) END
# MAGIC   FROM vf.silver.facilities_current_v1
# MAGIC   UNION ALL
# MAGIC   SELECT _source_row_id, 'equipment_clean',
# MAGIC          CASE WHEN equipment_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(equipment_clean, 'array<string>') IS NOT NULL THEN 'json_array_element'
# MAGIC               ELSE 'full_text' END,
# MAGIC          CASE WHEN equipment_clean IS NULL THEN NULL
# MAGIC               WHEN from_json(equipment_clean, 'array<string>') IS NOT NULL THEN from_json(equipment_clean, 'array<string>')
# MAGIC               ELSE array(equipment_clean) END
# MAGIC   FROM vf.silver.facilities_current_v1
# MAGIC   UNION ALL
# MAGIC   SELECT _source_row_id, 'specialties_clean', 'array_element', specialties_clean
# MAGIC   FROM vf.silver.facilities_current_v1
# MAGIC ) u
# MAGIC LATERAL VIEW posexplode(u.elements) e AS pos, element_text
# MAGIC WHERE e.element_text IS NOT NULL;
# MAGIC
# MAGIC SELECT count(*) AS element_rows FROM mention_elements;

# COMMAND ----------

# DBTITLE 1,Cell 5 — Create silver mention index (error-if-exists)
# MAGIC %sql
# MAGIC -- Cell 5 — Create the silver mention index (error-if-exists: plain CREATE TABLE fails if
# MAGIC -- the table already exists; nothing is ever overwritten). Grain: one row per source record +
# MAGIC -- service + source field + element position. Multiple pattern hits within that grain are
# MAGIC -- deduplicated into matched_terms. Every row carries the explicit unverified-mention label.
# MAGIC CREATE TABLE vf.silver.service_mentions_v1
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   m._source_row_id,
# MAGIC   p.service_code,
# MAGIC   m.source_field,
# MAGIC   m.element_position,
# MAGIC   m.element_kind,
# MAGIC   m.evidence_text,
# MAGIC   sort_array(array_distinct(collect_list(p.term_label))) AS matched_terms,
# MAGIC   'service_mention_rules_v1' AS matching_rule_version,
# MAGIC   current_timestamp() AS discovered_at,
# MAGIC   'Unverified text mention; context may negate or qualify it.' AS notice
# MAGIC FROM mention_elements m
# MAGIC JOIN service_mention_patterns p
# MAGIC   ON lower(m.evidence_text) RLIKE p.pattern
# MAGIC GROUP BY m._source_row_id, p.service_code, m.source_field, m.element_position, m.element_kind, m.evidence_text;
# MAGIC
# MAGIC SELECT count(*) AS mention_index_rows FROM vf.silver.service_mentions_v1;

# COMMAND ----------

# DBTITLE 1,Cell 6 — Create gold app view (error-if-exists)
# MAGIC %sql
# MAGIC -- Cell 6 — Create the compact gold app view (error-if-exists): one row per record/service,
# MAGIC -- aggregating evidence structs and attaching facility name, location, coordinates/status,
# MAGIC -- source URL candidates (kept unattributed), and review reasons. Statuses fixed to
# MAGIC -- unverified / unknown; no model responses or reviewer identities are exposed.
# MAGIC CREATE VIEW vf.gold.facility_service_mentions_v1 AS
# MAGIC SELECT
# MAGIC   a._source_row_id,
# MAGIC   a.service_code,
# MAGIC   r.name_clean AS facility_name,
# MAGIC   r.city_clean,
# MAGIC   r.state_clean,
# MAGIC   r.pincode_clean,
# MAGIC   r.latitude,
# MAGIC   r.longitude,
# MAGIC   r.coordinate_status,
# MAGIC   r.source_url_candidates,
# MAGIC   q.review_priority,
# MAGIC   q.review_reason_codes,
# MAGIC   a.matched_terms_all,
# MAGIC   a.evidence,
# MAGIC   size(a.evidence) AS evidence_count,
# MAGIC   'service_mention_rules_v1' AS matching_rule_version,
# MAGIC   'unverified' AS verification_status,
# MAGIC   'unknown' AS current_availability_status,
# MAGIC   'pending' AS human_review_status,
# MAGIC   'Unverified text mention; context may negate or qualify it. No mention means unknown, not unavailable.' AS notice
# MAGIC FROM (
# MAGIC   SELECT _source_row_id, service_code,
# MAGIC          sort_array(array_distinct(flatten(collect_list(matched_terms)))) AS matched_terms_all,
# MAGIC          sort_array(collect_list(struct(source_field, element_position, element_kind,
# MAGIC                                         evidence_text, matched_terms))) AS evidence
# MAGIC   FROM vf.silver.service_mentions_v1
# MAGIC   GROUP BY _source_row_id, service_code
# MAGIC ) a
# MAGIC JOIN vf.silver.facilities_current_v1 r
# MAGIC   ON a._source_row_id = r._source_row_id
# MAGIC LEFT JOIN (SELECT _source_row_id, review_priority, review_reason_codes
# MAGIC            FROM vf.silver.facilities_review_queue_v1) q
# MAGIC   ON a._source_row_id = q._source_row_id;
# MAGIC
# MAGIC SELECT count(*) AS record_service_pairs FROM vf.gold.facility_service_mentions_v1;

# COMMAND ----------

# DBTITLE 1,Cell 7 — Discovery summary per service
# MAGIC %sql
# MAGIC -- Cell 7 — Display: input record count; distinct records per service; total record/service pairs.
# MAGIC WITH pairs AS (
# MAGIC   SELECT service_code, count(DISTINCT _source_row_id) AS distinct_records, count(*) AS evidence_rows
# MAGIC   FROM vf.silver.service_mentions_v1
# MAGIC   GROUP BY service_code
# MAGIC )
# MAGIC SELECT service_code, distinct_records, evidence_rows,
# MAGIC        round(100.0 * distinct_records / (SELECT count(*) FROM vf.silver.facilities_current_v1), 2) AS pct_of_input_records
# MAGIC FROM pairs
# MAGIC ORDER BY distinct_records DESC;

# COMMAND ----------

# DBTITLE 1,Cell 7b — Input records and total pairs
# MAGIC %sql
# MAGIC -- Cell 7b — Display: input record count and total record/service pairs.
# MAGIC SELECT (SELECT count(*) FROM vf.silver.facilities_current_v1) AS input_records,
# MAGIC        (SELECT count(*) FROM vf.gold.facility_service_mentions_v1) AS total_record_service_pairs;

# COMMAND ----------

# DBTITLE 1,Cell 8 — Ten evidence examples per service
# MAGIC %sql
# MAGIC -- Cell 8 — Ten evidence examples per service (exact source text; up to 10 rows per service code,
# MAGIC -- deterministic order by record id, source field, element position).
# MAGIC SELECT r.service_code, r._source_row_id, f.name_clean AS facility_name,
# MAGIC        r.source_field, r.element_kind, r.element_position, r.evidence_text, r.matched_terms
# MAGIC FROM (
# MAGIC   SELECT m.*, row_number() OVER (
# MAGIC            PARTITION BY m.service_code
# MAGIC            ORDER BY m._source_row_id, m.source_field, m.element_position) AS rn
# MAGIC   FROM vf.silver.service_mentions_v1 m
# MAGIC ) r
# MAGIC LEFT JOIN (SELECT _source_row_id, name_clean FROM vf.silver.facilities_current_v1) f
# MAGIC   ON r._source_row_id = f._source_row_id
# MAGIC WHERE r.rn <= 10
# MAGIC ORDER BY r.service_code, r.rn;

# COMMAND ----------

# DBTITLE 1,Cell 9 — Integrity and status checks
# MAGIC %sql
# MAGIC -- Cell 9 — Integrity and status checks (unique grain, no orphans, label everywhere, fixed statuses).
# MAGIC SELECT t.*,
# MAGIC   CASE WHEN t.mention_index_rows = t.distinct_grain_keys
# MAGIC        THEN 'PASS: unique grain (record + service + source field + element position)'
# MAGIC        ELSE 'FAIL: duplicate grain rows' END AS grain_check,
# MAGIC   CASE WHEN t.orphan_rows = 0
# MAGIC        THEN 'PASS: every mention maps to an input record' ELSE 'FAIL: orphan mentions' END AS orphan_check,
# MAGIC   CASE WHEN t.rows_missing_label = 0
# MAGIC        THEN 'PASS: unverified-mention label on every silver row' ELSE 'FAIL: label missing' END AS label_check,
# MAGIC   CASE WHEN t.gold_status_deviations = 0
# MAGIC        THEN 'PASS: gold statuses unverified/unknown everywhere' ELSE 'FAIL: status deviation' END AS status_check
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     (SELECT count(*) FROM vf.silver.facilities_current_v1) AS input_records,
# MAGIC     (SELECT count(*) FROM vf.silver.service_mentions_v1) AS mention_index_rows,
# MAGIC     (SELECT count(DISTINCT concat_ws('|', _source_row_id, service_code, source_field, cast(element_position AS string)))
# MAGIC        FROM vf.silver.service_mentions_v1) AS distinct_grain_keys,
# MAGIC     (SELECT count(*) FROM vf.silver.facilities_review_queue_v1) AS review_queue_rows,
# MAGIC     (SELECT count(*) FROM vf.gold.facility_service_mentions_v1) AS record_service_pairs,
# MAGIC     (SELECT count(*) FROM vf.silver.service_mentions_v1 m
# MAGIC        LEFT JOIN (SELECT _source_row_id FROM vf.silver.facilities_current_v1) c
# MAGIC          ON m._source_row_id = c._source_row_id
# MAGIC        WHERE c._source_row_id IS NULL) AS orphan_rows,
# MAGIC     (SELECT count(*) FROM vf.silver.service_mentions_v1
# MAGIC        WHERE notice <> 'Unverified text mention; context may negate or qualify it.') AS rows_missing_label,
# MAGIC     (SELECT count(*) FROM vf.gold.facility_service_mentions_v1
# MAGIC        WHERE verification_status <> 'unverified' OR current_availability_status <> 'unknown') AS gold_status_deviations
# MAGIC ) t;

# COMMAND ----------

# DBTITLE 1,Cell 10 — Term distribution and build note
# MAGIC %sql
# MAGIC -- Cell 10 — Matched-term distribution (records per term; dialysis modalities shown separately —
# MAGIC -- these treatments are NOT interchangeable and are tagged independently).
# MAGIC SELECT service_code, term, count(*) AS records_with_term
# MAGIC FROM (
# MAGIC   SELECT DISTINCT service_code, _source_row_id, explode(matched_terms) AS term
# MAGIC   FROM vf.silver.service_mentions_v1
# MAGIC )
# MAGIC GROUP BY service_code, term
# MAGIC ORDER BY service_code, records_with_term DESC, term;

# COMMAND ----------

# DBTITLE 1,Cell 11 — Build note (no inference)
# MAGIC %sql
# MAGIC -- Cell 11 — Read-only build note: what this index does NOT claim.
# MAGIC SELECT 'SQL-only index build complete. No inference performed: negation, current delivery, branch attribution, patient eligibility, and verified availability are NOT determined. verification_status=unverified and current_availability_status=unknown throughout; source URL candidates remain unattributed; no model responses or reviewer identities are exposed. No mention means unknown, not unavailable. A cesarean-delivery mention does NOT establish emergency C-section capability.' AS build_note;

# COMMAND ----------

