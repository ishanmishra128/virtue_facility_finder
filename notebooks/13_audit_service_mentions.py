# Databricks notebook source
# DBTITLE 1,Overview and rules
# MAGIC %md
# MAGIC # 13_audit_service_mentions — READ-ONLY SQL audit of the service mention index
# MAGIC
# MAGIC **Databricks SQL only.** No model calls, no website fetching, no writes to any table (this notebook never modifies existing tables). Inputs: `vf.silver.service_mentions_v1` (11,178-row mention index) and `vf.gold.facility_service_mentions_v1` (4,273-row app view). Schemas are inspected before anything else.
# MAGIC
# MAGIC **Triage heuristics only.** Simple SQL regex heuristics flag evidence rows as **"possible context issue"** — they are NOT confirmed findings, no rows are removed automatically, and SQL cannot reliably detect all these cases (negation detection in free text is especially unreliable). Heuristic families: negated/unavailable; planned/proposed/discontinued/historical; referral-only; equipment- or specialty-source mention without an explicit service statement; reference to another named hospital or branch.
# MAGIC
# MAGIC **Unflagged ≠ verified.** Reproducibly selected unflagged examples are shown for contrast only. Full evidence text is displayed without truncation. No ranking or recommendation eligibility is produced anywhere in this notebook.

# COMMAND ----------

# DBTITLE 1,Cell 1 — Inspect input schemas
# MAGIC %sql
# MAGIC -- Cell 1 — Inspect schemas first (read-only). Both relations must expose the columns used below.
# MAGIC SELECT table_schema, table_name, column_name, data_type
# MAGIC FROM vf.information_schema.columns
# MAGIC WHERE ((table_schema = 'silver' AND table_name = 'service_mentions_v1')
# MAGIC     OR (table_schema = 'gold' AND table_name = 'facility_service_mentions_v1'))
# MAGIC ORDER BY table_schema, table_name, ordinal_position;

# COMMAND ----------

# DBTITLE 1,Cell 2 — Exact regex mapping (verbatim from notebook 12)
# MAGIC %sql
# MAGIC -- Cell 2 — The EXACT service regex mapping used by notebook 12 (12_national_service_mentions,
# MAGIC -- Cell 3 "Service patterns (VALUES)"). Reproduced VERBATIM from that notebook's persisted cell
# MAGIC -- content — not reconstructed from memory. This temp view is for reference/display only; it
# MAGIC -- never modifies the mention tables.
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
# MAGIC SELECT service_code, term_label, pattern FROM service_mention_patterns ORDER BY service_code, term_label;

# COMMAND ----------

# DBTITLE 1,Cell 3 — Triage heuristics and per-heuristic counts
# MAGIC %sql
# MAGIC -- Cell 3 — Context-triage heuristics (SIMPLE SQL triage only; applied to lowercased evidence
# MAGIC -- text; no backslash escapes). Flags label rows "possible context issue" — NOT confirmed
# MAGIC -- findings. Rows are never removed automatically, and SQL cannot reliably detect all these
# MAGIC -- cases (negation in free text especially). Per-heuristic pair counts below CAN OVERLAP
# MAGIC -- across heuristics — do not sum them; the headline report counts distinct affected pairs.
# MAGIC CREATE OR REPLACE TEMP VIEW mention_context_flags AS
# MAGIC SELECT m._source_row_id, m.service_code, m.source_field, m.element_position, m.element_kind,
# MAGIC        m.evidence_text, m.matched_terms,
# MAGIC        filter(array(
# MAGIC          CASE WHEN lower(m.evidence_text) RLIKE '(not available|unavailable|not offered|not provided|not performed|no longer|not equipped|lacks|does not (offer|provide|perform|have|support))'
# MAGIC               THEN 'possible_negated_or_unavailable' END,
# MAGIC          CASE WHEN lower(m.evidence_text) RLIKE '(planned|proposed|upcoming|coming soon|to be (launched|started|introduced|opened|commissioned|added)|under construction|future|formerly|previously|no longer|discontinued|yet to be|historical|in the past)'
# MAGIC               THEN 'possible_planned_or_historical' END,
# MAGIC          CASE WHEN lower(m.evidence_text) RLIKE '(refer|referral|transferred to|transfer to)'
# MAGIC               THEN 'possible_referral_only' END,
# MAGIC          CASE WHEN m.source_field IN ('equipment_clean', 'specialties_clean')
# MAGIC                AND lower(m.evidence_text) NOT RLIKE '(provid|perform|offer|availab|operat|equipped|houses|runs|has |have )'
# MAGIC               THEN 'possible_equipment_or_specialty_only' END,
# MAGIC          CASE WHEN lower(m.evidence_text) RLIKE '(branch|sister|affiliated|collaboration|association with|partnership with|unit of|another (hospital|centre|center|clinic|facility)|parent (hospital|company|group|organisation|organization))'
# MAGIC               THEN 'possible_other_named_facility_or_branch' END
# MAGIC        ), f -> f IS NOT NULL) AS triggered_heuristics
# MAGIC FROM vf.silver.service_mentions_v1 m;
# MAGIC
# MAGIC SELECT h.heuristic, count(*) AS flagged_rows,
# MAGIC        count(DISTINCT concat_ws('|', f._source_row_id, f.service_code)) AS affected_record_service_pairs
# MAGIC FROM mention_context_flags f LATERAL VIEW explode(f.triggered_heuristics) h AS heuristic
# MAGIC GROUP BY h.heuristic ORDER BY affected_record_service_pairs DESC;

# COMMAND ----------

# DBTITLE 1,Cell 4 — Five flagged examples per service
# MAGIC %sql
# MAGIC -- Cell 4 — Up to five FLAGGED examples per service (reproducible order; full evidence text,
# MAGIC -- no truncation). Flags are "possible context issue" triage from simple heuristics — not
# MAGIC -- confirmed findings.
# MAGIC SELECT f._source_row_id AS record_id, g.facility_name, f.service_code, f.evidence_text,
# MAGIC        f.source_field, f.element_kind, f.element_position, f.matched_terms, f.triggered_heuristics
# MAGIC FROM (SELECT m.*, row_number() OVER (PARTITION BY m.service_code
# MAGIC              ORDER BY m._source_row_id, m.source_field, m.element_position) AS rn
# MAGIC       FROM mention_context_flags m
# MAGIC       WHERE size(m.triggered_heuristics) > 0) f
# MAGIC LEFT JOIN vf.gold.facility_service_mentions_v1 g
# MAGIC   ON f._source_row_id = g._source_row_id AND f.service_code = g.service_code
# MAGIC WHERE f.rn <= 5
# MAGIC ORDER BY f.service_code, f.rn;

# COMMAND ----------

# DBTITLE 1,Cell 5 — Five unflagged examples per service
# MAGIC %sql
# MAGIC -- Cell 5 — Five reproducibly selected UNFLAGGED examples per service (no heuristic fired).
# MAGIC -- Unflagged does NOT mean verified.
# MAGIC SELECT f._source_row_id AS record_id, g.facility_name, f.service_code, f.evidence_text,
# MAGIC        f.source_field, f.element_kind, f.element_position, f.matched_terms
# MAGIC FROM (SELECT m.*, row_number() OVER (PARTITION BY m.service_code
# MAGIC              ORDER BY m._source_row_id, m.source_field, m.element_position) AS rn
# MAGIC       FROM mention_context_flags m
# MAGIC       WHERE size(m.triggered_heuristics) = 0) f
# MAGIC LEFT JOIN vf.gold.facility_service_mentions_v1 g
# MAGIC   ON f._source_row_id = g._source_row_id AND f.service_code = g.service_code
# MAGIC WHERE f.rn <= 5
# MAGIC ORDER BY f.service_code, f.rn;

# COMMAND ----------

# DBTITLE 1,Cell 6 — Headline audit report
# MAGIC %sql
# MAGIC -- Cell 6 — Headline audit report. Unique records are counted ACROSS all six services,
# MAGIC -- separately from record/service pairs. Pairs with possible context issues are counted
# MAGIC -- DISTINCTLY per record/service pair (any-flag semantics) — overlapping heuristics are
# MAGIC -- never summed.
# MAGIC SELECT
# MAGIC   (SELECT count(DISTINCT _source_row_id) FROM vf.silver.service_mentions_v1) AS unique_records_all_services,
# MAGIC   (SELECT count(*) FROM vf.gold.facility_service_mentions_v1) AS record_service_pairs,
# MAGIC   (SELECT count(*) FROM vf.gold.facility_service_mentions_v1
# MAGIC     WHERE review_priority IS NOT NULL) AS pairs_with_facility_review_flags,
# MAGIC   (SELECT count(*) FROM (SELECT _source_row_id, service_code FROM mention_context_flags
# MAGIC                           WHERE size(triggered_heuristics) > 0
# MAGIC                           GROUP BY _source_row_id, service_code)) AS pairs_with_possible_context_issues;

# COMMAND ----------

# DBTITLE 1,Cell 7 — Counts by service and source field
# MAGIC %sql
# MAGIC -- Cell 7 — Mention rows and distinct records by service and source field.
# MAGIC SELECT service_code, source_field, count(*) AS mention_rows, count(DISTINCT _source_row_id) AS distinct_records
# MAGIC FROM vf.silver.service_mentions_v1
# MAGIC GROUP BY service_code, source_field
# MAGIC ORDER BY service_code, mention_rows DESC, source_field;

# COMMAND ----------

# DBTITLE 1,Cell 8 — Dialysis multi-modality records (overlaps preserved)
# MAGIC %sql
# MAGIC -- Cell 8 — Dialysis records with MULTIPLE modality terms, grouped by the exact modality set
# MAGIC -- (overlaps preserved: each record is counted once under its own combination, never summed
# MAGIC -- across modalities). A record's set is array_intersect of its dialysis matched_terms with the
# MAGIC -- five modality labels from the notebook-12 mapping.
# MAGIC WITH per_record AS (
# MAGIC   SELECT _source_row_id,
# MAGIC          array_intersect(array_distinct(flatten(collect_list(matched_terms))),
# MAGIC            array('hemodialysis_or_haemodialysis', 'peritoneal_dialysis', 'crrt',
# MAGIC                  'continuous_renal_replacement_therapy', 'hemofiltration_or_haemofiltration')) AS modalities
# MAGIC   FROM vf.silver.service_mentions_v1
# MAGIC   WHERE service_code = 'dialysis'
# MAGIC   GROUP BY _source_row_id
# MAGIC )
# MAGIC SELECT modalities, count(*) AS records
# MAGIC FROM per_record
# MAGIC WHERE size(modalities) >= 2
# MAGIC GROUP BY modalities
# MAGIC ORDER BY records DESC, modalities;

# COMMAND ----------

# DBTITLE 1,Cell 9 — Audit note
# MAGIC %sql
# MAGIC -- Cell 9 — Read-only audit note.
# MAGIC SELECT 'SQL-only audit complete. All flags are possible-context-issue triage from simple regex heuristics — not confirmed findings. No rows were removed. SQL cannot reliably detect negation, planned/historical status, referral-only intent, or other-facility references in free text. Unflagged does not mean verified. No ranking or recommendation eligibility was produced.' AS audit_note;