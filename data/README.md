# Data export — complete snapshot of everything the app reads

This folder contains a complete export of every Unity Catalog table and view the
application queries, as of **2026-11-22**. Format: Apache Parquet, one file per
table. Row counts were verified against the source tables at export time.

| File | Source table/view | Rows |
| --- | --- | --- |
| `search_documents.parquet` | `vf.app.vw_facility_search_documents` (free-text search index) | 579,335 |
| `facilities_public.parquet` | `vf.app.vw_facilities_public` (record names, city, state, pincode, review priority) | 10,077 |
| `review_flags.parquet` | `vf.app.vw_facilities_review_flags` (review priority + reason codes) | 1,474 |
| `coordinates.parquet` | `vf.app.vw_facilities_coordinates` (recorded lat/lng + coordinate status) | 10,077 |
| `contacts.parquet` | `vf.app.vw_facilities_contacts` (phones, websites, provenance note) | 10,077 |
| `gold_service_mentions.parquet` | `vf.gold.facility_service_mentions_v2` (record/service pairs incl. dialysis AI enrichments) | 4,273 |
| `gold_dialysis_pilot.parquet` | `vf.gold.dialysis_jaipur_pilot_v2` (Jaipur pilot retrieved website evidence) | 13 |

## Notes

- Columns that are arrays/structs in Unity Catalog are stored losslessly as JSON strings:
  - `review_flags`: `review_reason_codes`
  - `gold_service_mentions`: `source_url_candidates`, `review_reason_codes`, `matched_terms_all`, `evidence`, `ai_treatment_types_for_display`, `ai_evidence_quotations`, `ai_patient_scope_evidence`, `ai_attribution_evidence`
  - `gold_dialysis_pilot`: `evidence_quotations`, `attribution_evidence`, `unattributed_source_url_candidates`
- The live application reads these tables in Unity Catalog via DBSQL under its managed identity — this folder is a snapshot for review and offline analysis, not the app's runtime data source.
- Caveats inherited from the dataset: coordinates are **recorded**, never independently verified; contacts are unverified values copied from source data; rows are source records, not necessarily distinct facilities; availability is unknown unless explicitly stated.

## Loading

```python
# pandas / pyarrow
import pandas as pd
df = pd.read_parquet("data/search_documents.parquet")

# Spark (Databricks)
df = spark.read.parquet("data/search_documents.parquet")
```
