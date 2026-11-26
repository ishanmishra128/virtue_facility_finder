# Virtue Foundation Facility Finder

An evidence-backed facility finder for NGO coordinators and healthcare planners: search care-service mentions across the Virtue Foundation national facility dataset, understand exactly why each record appears, compare facilities side by side, and see results on an interactive map — before investigating or contacting facilities.

The app never claims "best hospital", confirmed availability, or clinical suitability. Every result explains **why it appears** and **how the system decided**.

Live deployment (Databricks App — requires Databricks sign-in with access to this workspace):
https://virtue-facility-finder-7474659425200889.aws.databricksapps.com

## User-facing features

- **Search:** six quick-search service buttons (dialysis, MRI, NICU, cardiac surgery, C-section, burn care) plus free-text care phrases (e.g. `cataract surgery`), with optional city and PIN-code text filters. One result per source record, with aggregated evidence.
- **Evidence-first ordering (default):** a documented, deterministic provisional shortlist policy computed over the complete result set **before** pagination — never a medical ranking. Alternative sorts: facility name, or straight-line distance.
- **Distance search:** optionally share your device location (browser geolocation, explicit consent) with a chosen radius (0.5–500 km). Coordinates travel **only in the POST request body** — never in a URL — and are never logged or persisted. Location and city/PIN filters are mutually exclusive.
- **Side-by-side comparison:** select up to 3 facilities from result cards; a tray tracks selections across pagination, and a modal compares name, location, distance, contact provenance, availability, evidence quality, patient scope, and dialysis interpretation, with explicit "not recorded" placeholders instead of misleading blanks.
- **Interactive map:** Leaflet 1.9.4 (BSD-2-Clause, self-hosted — no CDN, no build step) with OpenStreetMap raster tiles (open data, ODbL). Markers plot the records' real recorded coordinates (never independently verified), distinguish compared facilities, show the search radius circle around a shared device location, and expose popups with evidence category and straight-line distance. If basemap tiles cannot load, an explicit notice appears — there is never a silent schematic fallback.
- **Evidence drawer:** per-record detail with source-field provenance, exact evidence quotations, review flags, contact notes ("Unverified — copied from source"), and dialysis AI-interpretation status with limitations.
- **Accessibility & mobile:** skip link, ARIA roles/labels, keyboard-reachable controls, focus management, responsive layout.
- **Built-in self-test:** the served app exposes `/selftest.html` — browser checks covering chips, a live search, evidence rendering, the comparison flow, and a live basemap-tile load check.

## Architecture

- **Backend:** FastAPI (`app.py`, `server/`) — parameterized SQL (named `:parameters` only) over Unity Catalog views via DBSQL, using the Databricks App's managed identity. No tokens in code; credentials are resolved at request time. A non-fatal startup self-test exercises the real connector and logs a summary to app logs. Errors never fall back to mock data.
- **Frontend:** no-build static HTML/CSS/JS (`frontend/`) served from the same origin — same-origin `/api/*` fetches only, no CDN dependencies, Leaflet vendored locally.
- **Deployment:** Databricks Apps V2 (`app.yaml` — uvicorn; `DATABRICKS_WAREHOUSE_ID` env; SQL warehouse `CAN_USE` resource).

## API contract

`GET /api/health` → `{"status": "ok"}`

`GET /api/facilities/services` → `{services: [{code, label}...], note}`

`GET /api/facilities/search?q=<care phrase>|service=<code>&city=<text>&pin=<text>&page=<1..>&page_size=<1..100>&sort=evidence|name`

`POST /api/facilities/search` — same engine and filters as GET, plus an optional device origin for distance search (`origin: {lat, lng, accuracy_m}`, `radius_km`) and `sort=distance`. Precise coordinates are accepted only in the POST body, used solely for the requested search, never logged or persisted, and never combined silently with city/PIN (that combination is rejected with 400).

- Provide **either** `q` (free-text care phrase) **or** `service` (legacy code, translated server-side to its canonical care phrase and run through the SAME token-boundary engine). Supplying both or neither → 400. Invalid service code → 400.
- **One matching engine** for typed phrases and quick-search buttons: token-boundary phrase match of the normalized query (or an audited orthographic alias, e.g. `physiotherapy` / `physical therapy`) within a single evidence element — fragments are never combined across fields. Input is treated as literal text (never regex/SQL), bounded to 120 chars / 8 words. Conversational sentences ("I need the best...") → 400 with a suggestion to try a shorter care phrase. No model calls or embeddings.
- **Alias rules (audited):** `mri`/`nicu` abbreviations require token boundaries (full-name equivalents included); cardiac surgery requires surgical context; `cardiac surgeon`-style matches are labeled `specialist_mention` — a named specialist is not proof surgery is performed onsite; verified `Caesarian` spelling variants and explicit burn-surgery phrases are included; modifiers like `pediatric` are preserved in every expanded alias. Modality precision (2026-11-26): **CRRT** matches only CRRT / continuous renal replacement therapy; **peritoneal dialysis**, **hemodialysis/haemodialysis**, and **hemofiltration/haemofiltration** each match only their own orthographic variants — a modality query never broadens to other modalities or to generic dialysis text. Only the generic **dialysis** query (the quick-search button) broadly discovers the whole dialysis family, one-directionally, with modifiers preserved. Alias substitution is single-pass from the base phrase, so composite artifacts like "peritoneal peritoneal dialysis" cannot occur.
- **Ordering:** `evidence` (default — documented shortlist policy over the full result set before pagination), `name`, or `distance` (requires an origin). Ties broken deterministically.
- **Dialysis AI interpretation and Jaipur pilot evidence** attach only when the interpreted query maps unambiguously to the dialysis service family; other free-text results carry explicit `null` enrichment and a stated limitation.
- **Response:** `{total_count, page, page_size, query: {interpreted, aliases_used, ...}, ordering: {rules, first_result_explanation, exclusions?}, results: [...]}`. Each result: `record_id`, `facility_name`, `location {city, state, pincode, latitude, longitude, coordinate_status}`, `sql_evidence {items[{source_field, evidence_text, matched_terms}], count, matched_terms}`, `ordering {category_label, category_detail, distance_km?, unresolved_reasons}`, `ai_enrichment_status`, `ai_interpretation{...} | null`, `jaipur_pilot_supplement{...} | null`, `contacts {phones, websites, note, branch_warning}` — contacts are copied from source data and always labeled unverified.
- Unusable coordinates (missing/invalid, outside India's bounding rectangle, known location conflict) are excluded from distance search and map markers, and counted in `ordering.exclusions`.

## Authentication & privacy

The Databricks Apps gateway enforces Databricks OAuth for both the page and the API. The frontend uses same-origin `/api/*` fetches, which ride the authenticated browser session — no tokens or PATs in browser code. Cross-origin access is not supported. Device location, when explicitly shared, is sent only in the POST body, used for the one requested search, and never logged or persisted.

## Data sources (read-only)

- `vf.app.vw_facility_search_documents` — narrow app-facing view over the persisted free-text search index `vf.search.facility_search_documents_v1` (built once from `vf.silver.facilities_current_v1` — 579,335 normalized text elements covering 9,997 of 10,077 records)
- `vf.app.vw_facilities_public` — narrow public record view (name, city, state, pincode, review flags)
- `vf.app.vw_facilities_coordinates` — recorded coordinates with coordinate status (used for distance search and map markers)
- `vf.app.vw_facilities_contacts` — contact provenance
- `vf.gold.facility_service_mentions_v2` — national record/service pairs, including validated dialysis AI enrichments
- `vf.gold.dialysis_jaipur_pilot_v2` — Jaipur dialysis pilot supplement (retrieved website evidence, 1:1 join)

The app's service principal is granted SELECT only on the narrow `vf.app` views and the `vf.gold` tables it needs — never on internal silver/search tables beyond the exposed views.

## Project layout

```
virtue-facility-finder/
  app.py            # FastAPI app: API routes, static frontend mount, startup self-test
  app.yaml          # Databricks App config (command, env, SQL warehouse resource)
  requirements.txt
  README.md
  server/
    config.py       # managed-identity / local auth, warehouse + host resolution
    sql.py           # DBSQL wrapper with named :parameters
    facilities.py    # token-boundary matching engine, ordering, distance, enrichment
  frontend/
    index.html      # no-build shell
    styles.css      # design system (local, no CDN)
    app.js          # search, drawer, comparison, map, pagination
    leaflet.js/.css # Leaflet 1.9.4 (BSD-2-Clause), vendored
    images/         # Leaflet marker/layer icons
    selftest.html/js # served browser self-test page
```

## Running and deploying

The app is designed for Databricks Apps V2. With the Databricks CLI:

```bash
databricks apps get <app-name>                 # confirm compute is ACTIVE before deploying
databricks apps deploy <app-name> --source-code-path <this folder>
```

`app.yaml` pins the uvicorn command, sets `DATABRICKS_WAREHOUSE_ID`, and requests `CAN_USE` on the SQL warehouse. Python dependencies are in `requirements.txt`. For local development, `server/config.py` falls back to ambient or `DATABRICKS_PROFILE`-based auth; the app raises rather than serving mock data on failure.

## Testing

- **Backend:** a startup self-test runs in the deployed runtime under the app's managed identity and logs per-check results (connector, managed-identity DBSQL, free-text/alias searches, evidence ordering, distance + coordinate exclusions, contacts) to the app logs — non-fatal, no mock fallback.
- **Frontend:** open `/selftest.html` (while signed in) for browser checks of chips, live search, evidence rendering, the comparison flow, the interactive map, and live basemap-tile loading.

## Credits and licences

- Map library: [Leaflet 1.9.4](https://leafletjs.com) (BSD-2-Clause), vendored in `frontend/`.
- Basemap: © OpenStreetMap contributors, tiles from `tile.openstreetmap.org` under the [ODbL licence](https://www.openstreetmap.org/copyright). No API keys; no paid services.

## Evidence-rendering fixes and verification (2026-11-25, deployment `01f1e8eb43771b9ba393ab5011a2448c`)

Each fix was verified before deploy with bounded tests — backend: 25/25 checks against the real gold/pilot rows for the affected records (Monilek, Soni, Deep, KKS, CKS, Vaishali, Liberty); frontend: 36/36 DOM-stub runtime checks in a real JS engine; both files parse-verified; live smoke: OAuth 302 on the page and 401 on the API for unauthenticated calls, as designed.

1. **AI quotation normalization** — `ai_evidence_quotations` in the gold table is an array of plain strings; the backend repackages each into `{source_field, exact_quote}` and the frontend double-guards, so the Monilek drawer shows the real quotation (previously “(no text)”). Quotations are never fabricated or completed.
2. **Temporal state rendered faithfully** — website badges and drawer notes derive from the actual `retrieved_temporal_context` (`undated_statement`, `dated_historical_statement`, or none). Only Vaishali Hospital (dated 2023 statement) is labeled historical; Monilek and CKS show “undated statement”.
3. **Attribution branches split** — records flagged for attribution review *without* retrieved evidence (Soni, Deep, KKS) show “Source attribution for this record is unresolved — no retrieved website evidence is available for it yet”; only records with an actual retrieved page (e.g. CKS) carry the “Retrieved website attribution unresolved” branch assessment.
4. **Capacity differences not overstated** — Monilek's 12-machines vs 23-stations note is labeled “Unresolved capacity difference”, passing the issue text through verbatim (the source explicitly says it is NOT automatically a contradiction).
5. **Comparison UX** — conspicuous sticky compare tray with live selection count; the wide comparison table scrolls horizontally with a visible hint and a keyboard-focusable region (`tabindex="0"`, `role="region"`, `aria-label`); Escape closes dialogs and restores focus; the capacity row reads “No live appointment or treatment-capacity information is available.” (previously a false dataset-wide-absence claim).
6. **Plain-language enums** — `unspecified_dialysis` → “Dialysis — type not specified in the source text”; `peritoneal_dialysis`, `CRRT` and the temporal enums are similarly labeled; raw snake_case values never reach the UI.
7. **Deterministic excerpt selection** — cards prefer a present-tense supporting claim when one exists (Liberty shows “Provides dialysis services”, not “Dialysis wing started in 2003”); when every matching element is historical, the excerpt is labeled historical context and never claims availability.
8. **Exact quotations with safe links** — long quotations (>300 chars) collapse behind a “Show full exact quotation” toggle (`aria-expanded`); stored excerpts ending mid-sentence are marked “reproduced exactly as recorded — nothing has been added”; source URLs render as clickable HTTP(S)-only links (`target="_blank"`, `rel="noopener noreferrer nofollow"`) — `javascript:`/`data:` schemes are never linked and nothing auto-navigates. The Leaflet/OSM map is unchanged.

## Reproducibility / submission package

The GitHub submission repository packages everything needed to rebuild and audit the app:

```
notebooks/         # 20 pipeline notebooks (00–17 + manual step-1 cleaning), SOURCE format, outputs stripped
sql/app_schema.sql # DDL export: the 5 narrow vf.app views (SHOW CREATE) + DESCRIBE of the 4 base tables
data/              # Parquet exports of the 7 app tables (row-count-verified) + provenance README
docs/screenshots/  # real UI screenshots captured against live data
tests/             # alias-engine regression tests (no database required)
.env.example       # configuration template — names only, no secrets
THIRD-PARTY-NOTICES.md
app.py, server/, frontend/   # the app source (this folder, verbatim)
```

Honest gaps, stated here rather than papered over: notebook `00_profile_sources` exported as an empty placeholder — the author's local notebook is **non-empty** per direct inspection, and its output-free source will be supplied separately to fill this placeholder (the workflow is not claimed complete until then); `02_clean_facilities_silver` also exported as an empty placeholder (the substantive step-1 cleaning lives in `01_facilities_cleaning_step1`). There are two `04_` and two `11_` notebooks (parallel steps) and no `01_`-numbered notebook. Raw source data (original spreadsheets, volunteered facility lists) is intentionally **not** included — `data/` carries only the derived app-facing tables. The AI-enrichment and pilot-retrieval notebooks document their prompts and validation rules; re-running them requires a model provider and retrieved pages not shipped in this package, so their **validated outputs** are exported instead.

## Modality-precision fix and verified count reconciliation (2026-11-26, deployment `01f1e9c29bf913a6a0981a5f2a75d3c0`)

Before this fix, the dialysis family was a single symmetric alias group mixing the generic phrase with every modality, and variant expansion substituted recursively to a fixpoint — so `crrt`, `peritoneal dialysis`, `hemodialysis`, `hemofiltration`, and `dialysis` all expanded to the same 24-variant set (capped by MAX_MATCH_VARIANTS, padded with composites like "peritoneal peritoneal dialysis") and returned **identical result sets**. Fixed by splitting each modality into its own equivalence group, making broad family discovery a one-directional rule from the generic phrase only, and making substitution single-pass/longest-match-first. Verified with `tests/test_alias_engine.py` (12/12, pure logic, no DB) and a read-only reconciliation against the search index (distinct matching records):

| Query | Before | After | Why the count changed |
| --- | --- | --- | --- |
| dialysis | 1,055 | 1,055 | broad generic discovery preserved exactly |
| crrt | 1,055 | 61 | now only CRRT + continuous renal replacement therapy (+ plural) |
| peritoneal dialysis | 1,055 | 108 | now only peritoneal dialysis itself |
| hemodialysis / haemodialysis | 1,055 | 273 | now only the two spellings of that modality |
| hemofiltration | 1,055 | 5 | now only the two spellings of that modality |
| pediatric dialysis | 13 | 12 | one record previously reachable only via the composite "paediatric haemodialysis"; it remains findable by its own modality query |
| physio therapy | 1,205 | 1,205 | removed composites ("physiotherapy therapy") matched nothing |
| cesarean | 443 | 445 | +2: the old 24-variant cap had crowded out legitimate plurals ("c sections") that the clean expansion fits |

## Comparison-parity and safe-link fixes (2026-11-26, deployment `01f1e9c396ab1ce5ae1f890fcda0f3d7`)

Both gaps were first observed in live-browser verification (Firefox, by the author) on the restored deployment; both were then fixed and verified with bounded DOM-stub checks (11/11):

1. **Comparison preserves every "needs checking" item the drawer shows** — the comparison's "Needs checking" row previously listed only current availability; it now mirrors the drawer exactly, including the recorded unresolved issues (e.g. Monilek's "Unresolved capacity difference —" with the issue text passed through verbatim), the CRRT-distinction note, unspecified-scope and mixed-facility flags, and pediatric-scope items.
2. **Consistent safe HTTP(S) links** — unattributed candidate URLs in the evidence drawer and contact websites in the comparison table now render as clickable links using the same safe pattern as source links (`target="_blank"`, `rel="noopener noreferrer nofollow"`, HTTP(S) only; `javascript:`/`data:` schemes are never linked). Labels still state the URLs are unattributed/unverified — linking never implies attribution, and a non-HTTP(S) candidate is preserved as plain text, not linked.

## Known limitations

- The evidence ordering is a documented, deterministic shortlist policy — not a clinically validated ranking; mention counts and confidence scores are never used.
- Recorded coordinates and contacts are copied from source data, never independently verified; distances are straight-line, not travel time.
- AI dialysis interpretation: `equipment_only` does not establish service delivery; `extraction_needs_review` rows withhold the interpretation and show the dataset quotations instead.
- Retrieved website evidence carries a retrieval date, not a service-verification date; attribution-review flags and unresolved-issue notes pass through verbatim and are never silently resolved.
- No live appointment or capacity information exists anywhere in the pipeline; the app never claims availability — it says so explicitly wherever the topic arises.
- Enrichment attaches only to dialysis-family queries; all other services carry explicit nulls with a stated limitation.
