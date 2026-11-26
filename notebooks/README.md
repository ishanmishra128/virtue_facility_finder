# Pipeline notebooks

The 20 notebooks here (Databricks SOURCE format, `.py`, executed outputs
stripped) are the workflow that produced every table the app reads. In the
original workspace they live in the author's home directory; names are
preserved verbatim, with `01_facilities_cleaning_step1` renamed from
“Facilities Cleaning — Step 1 (name, pincode, counts, volunteers)”.

Pipeline order (two parallel `04_` and two parallel `11_` steps are intentional;
no `01_`-numbered notebook exists — manual step-1 cleaning is
`01_facilities_cleaning_step1`):

- `00_profile_sources` — source profiling (**empty placeholder — nothing was
  ever saved in this notebook**)
- `01_facilities_cleaning_step1` — name/pincode/count cleaning with
  volunteer manual-review workbook
- `02_clean_facilities_silver` — silver cleaning (**empty placeholder**; the
  silver build logic lives in `04_flag_record_integrity` and
  `05_publish_silver_review_views`)
- `03_enrich_geography`, `04_review_geography`, `04_flag_record_integrity`,
  `05_publish_silver_review_views` — geography enrichment, review flags,
  `vf.silver.facilities_current_v1`
- `06_dialysis_pilot_sample` … `11_finalize_dialysis_pilot` — Jaipur dialysis
  pilot: extraction, publish, source verification, evidence linking
- `11_national_dialysis_extraction` … `16_finalize_dialysis_validation` —
  national dialysis AI enrichment, service mentions, audits, repairs,
  validation → `vf.gold.facility_service_mentions_v2`
- `17_publish_national_app_view` — narrow app-facing `vf.app` views and
  `vf.search.facility_search_documents_v1`

Honest notes:

- The AI-enrichment and pilot-retrieval notebooks document their prompts and
  validation rules, but re-running them requires a model provider and the
  retrieved website pages, neither of which is shipped here. Their validated
  outputs are exported as Parquet in `../data/`.
- The Jaipur pilot retrieved-evidence table (`vf.gold.dialysis_jaipur_pilot_v2`)
  is also exported in `../data/gold_dialysis_pilot.parquet`.
- Original raw source spreadsheets and volunteer lists are intentionally not
  included (they contain data beyond what the app exposes).