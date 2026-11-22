"""Virtue Foundation facility search API.

Implements the documented facility-finder contract over the Virtue Foundation
gold views:
  - vf.gold.facility_service_mentions_v2  (national record/service pairs)
  - vf.gold.dialysis_jaipur_pilot_v2      (Jaipur dialysis pilot supplement)

One row is returned per record/service pair — evidence joins never multiply
rows. AI interpretation is exposed only for validated extractions; records
whose AI extraction failed review expose SQL evidence only. Recorded
(original) coordinates are returned for the schematic map and distance
search — they are never verified geolocation. Contact values are returned
exactly as stored, validated and labeled unverified at render time.

Ordering is a documented, deterministic shortlist policy computed in SQL
BEFORE pagination: sort=evidence (default; unresolved issues demoted, then
conservative evidence category, then name, then record id), sort=name, and
sort=distance (straight-line, origin supplied via POST body only). It is
never a medical ranking, a confidence percentage, or a "best hospital"
claim.
"""
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from .sql import run_query, run_query_pair

# uvicorn.error output is verifiably shipped to the app logs. Query text,
# search phrases, and coordinates are NEVER logged — only counts/timings.
log = logging.getLogger("uvicorn.error")

SERVICES: List[Dict[str, str]] = [
    {"code": "dialysis", "label": "Dialysis"},
    {"code": "MRI", "label": "MRI"},
    {"code": "NICU", "label": "NICU"},
    {"code": "burn_care", "label": "Burn care"},
    {"code": "cardiac_surgery", "label": "Cardiac surgery"},
    {"code": "cesarean_delivery", "label": "C-section"},
]
_SERVICE_CODES = {s["code"] for s in SERVICES}

SERVICE_NOTE = (
    "Service mentions come from dataset text and, for dialysis where available, an AI interpretation. "
    "A mention is not verification that the service is currently offered. Current availability is "
    "unknown for all records; verification is unverified."
)

AI_VALIDATED_NOTE = (
    "AI interpretation from a validated extraction: extraction-format and verbatim-quotation checks "
    "passed. This is NOT a verified hospital/service claim, and current availability remains unknown."
)

NATIONAL_VIEW = "vf.gold.facility_service_mentions_v2"
PILOT_VIEW = "vf.gold.dialysis_jaipur_pilot_v2"

# Static view names only — user input never enters this string; filters use :parameters.
_SELECT = f"""
SELECT
  g._source_row_id AS record_id, g.service_code, g.facility_name,
  g.city_clean AS city, g.state_clean AS state, g.pincode_clean AS pincode,
  g.coordinate_status, g.review_priority, g.review_reason_codes,
  g.evidence, g.evidence_count, g.matched_terms_all,
  g.current_availability_status, g.verification_status, g.human_review_status, g.notice,
  g.ai_enrichment_status, g.ai_assertion, g.ai_patient_scope,
  g.ai_treatment_types_for_display, g.ai_has_generic_dialysis_mention,
  g.ai_facility_attribution, g.ai_possible_mixed_facility_record,
  g.ai_evidence_quotations, g.ai_validator_version, g.ai_enrichment_note,
  p.record_id AS pilot_record_id,
  p.evidence_label, p.retrieved_evidence_quote, p.retrieved_evidence_url,
  p.retrieved_evidence_retrieved_at, p.retrieved_temporal_context,
  p.retrieved_branch_address_quote, p.retrieved_branch_address_url,
  p.retrieved_branch_assessment, p.retrieved_unresolved_issues,
  p.unattributed_source_url_candidates AS pilot_unattributed_urls,
  p.needs_attribution_review AS pilot_needs_attribution_review
FROM {NATIONAL_VIEW} g
LEFT JOIN {PILOT_VIEW} p
  ON g._source_row_id = p.record_id AND g.service_code = p.service_code
WHERE g.service_code = :service
"""


def _complex(v: Any) -> Any:
    """Normalize DBSQL complex-column values to plain Python.

    The connector may return ARRAY/STRUCT columns as JSON strings or as numpy
    arrays (whose truthiness is ambiguous); convert both to plain lists/dicts
    so downstream truthiness and iteration are always well-defined.
    """
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except (ValueError, TypeError):
            return v
    if hasattr(v, "tolist"):  # numpy arrays / pandas objects -> plain Python
        v = v.tolist()
    if isinstance(v, list):
        return [_complex(x) for x in v]
    if isinstance(v, dict):
        return {k: _complex(x) for k, x in v.items()}
    return v


def _assertion_class(a: Optional[str]) -> str:
    """Coarse grouping of the verified AI assertion enum; the raw assertion
    value is always exposed alongside so the UI keeps every category distinct.
    """
    if a == "explicit_service_claim":
        return "explicit_service_claim"
    if a == "procedure_list_claim":
        return "procedure_list_claim"
    if a == "equipment_only":
        return "equipment_only"
    if a == "specialty_only":
        return "specialty_only"
    if a == "planned_or_historical_only":
        return "planned_or_historical_only"
    if a == "negated":
        return "negated"
    if a == "not_mentioned":
        return "not_mentioned"
    return "context_limited"


def _treatment_types(t: Any) -> List[str]:
    v = _complex(t)
    if not v:
        return []
    return [str(x) for x in v]


def _evidence_items(ev: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for e in (_complex(ev) or []):
        if isinstance(e, dict):
            out.append({
                "source_field": e.get("source_field"),
                "evidence_text": e.get("evidence_text"),
                "matched_terms": _complex(e.get("matched_terms")) or [],
            })
        else:
            out.append({"source_field": None, "evidence_text": str(e), "matched_terms": []})
    return out


def _shape_row(r: Dict[str, Any]) -> Dict[str, Any]:
    ai_status = r.get("ai_enrichment_status")
    validated = ai_status == "validated_extraction"
    ttypes = _treatment_types(r.get("ai_treatment_types_for_display")) if validated else []

    pilot: Optional[Dict[str, Any]] = None
    if r.get("pilot_record_id"):
        pilot = {
            "evidence_kind": "attributed_retrieved_website_evidence",
            "evidence_label": r.get("evidence_label"),
            "retrieved_evidence_quote": r.get("retrieved_evidence_quote"),
            "retrieved_evidence_url": r.get("retrieved_evidence_url"),
            "retrieved_evidence_retrieved_at": r.get("retrieved_evidence_retrieved_at"),
            "retrieved_temporal_context": r.get("retrieved_temporal_context"),
            "temporal_note": (
                "Historical statement from retrieved website evidence — current availability "
                "remains unknown; this is not a current-availability claim."
            ),
            "retrieved_branch_address_quote": r.get("retrieved_branch_address_quote"),
            "retrieved_branch_address_url": r.get("retrieved_branch_address_url"),
            "retrieved_branch_assessment": r.get("retrieved_branch_assessment"),
            "retrieved_unresolved_issues": r.get("retrieved_unresolved_issues"),
            "unattributed_source_url_candidates": _complex(r.get("pilot_unattributed_urls")) or [],
            "needs_attribution_review": r.get("pilot_needs_attribution_review"),
        }

    src_rank = r.get("src_rank")
    category_label, category_detail = _category_label(
        src_rank, ai_status, r.get("ai_assertion")
    )
    conflict_codes = sorted(
        set(_complex(r.get("review_reason_codes")) or []) & set(LOCATION_CONFLICT_CODES)
    )
    distance = r.get("distance_km")
    result: Dict[str, Any] = {
        "record_id": r.get("record_id"),
        "service_code": r.get("service_code"),
        "facility_name": r.get("facility_name"),
        "location": {
            "city": r.get("city"),
            "state": r.get("state"),
            "pincode": r.get("pincode"),
            "coordinate_status": r.get("coordinate_status"),
            # Raw RECORDED coordinates, for the schematic map and distance
            # display only — never verified geolocation; never offered as
            # verified directions.
            "latitude": r.get("latitude"),
            "longitude": r.get("longitude"),
            "location_conflict": bool(conflict_codes),
        },
        "sql_evidence": {
            "evidence_kind": "dataset_text",
            "items": _evidence_items(r.get("evidence")),
            "count": r.get("evidence_count"),
            "matched_terms": _complex(r.get("matched_terms_all")) or [],
        },
        "review": {
            "priority": r.get("review_priority"),
            "reason_codes": _complex(r.get("review_reason_codes")) or [],
            "human_review_status": r.get("human_review_status"),
        },
        "status": {
            "current_availability": r.get("current_availability_status"),
            "verification": r.get("verification_status"),
        },
        "ordering": {
            "category_label": category_label,
            "category_detail": category_detail,
            "category_rank": r.get("category_rank"),
            "unresolved": bool(r.get("unresolved")),
            "unresolved_reasons": _unresolved_reasons(r, conflict_codes),
            "distance_km": (round(float(distance), 1) if distance is not None else None),
        },
        "contacts": _shape_contacts(r, bool(conflict_codes)),
        "notice": r.get("notice"),
        "ai_enrichment_status": ai_status,
        "ai_interpretation_awaiting_review": ai_status == "extraction_needs_review",
        "ai_interpretation": None,
        "jaipur_pilot_supplement": pilot,
    }

    if ai_status == "extraction_needs_review":
        result["awaiting_review_note"] = (
            "AI interpretation awaiting review — the SQL dataset evidence above is shown instead; "
            "failed AI content is not exposed."
        )
    elif validated:
        mixed = r.get("ai_possible_mixed_facility_record")
        result["ai_interpretation"] = {
            "assertion": r.get("ai_assertion"),
            "assertion_class": _assertion_class(r.get("ai_assertion")),
            "explicit_service_claim": r.get("ai_assertion") == "explicit_service_claim",
            "patient_scope": r.get("ai_patient_scope"),
            "pediatric_scope": r.get("ai_patient_scope") == "pediatric",
            "treatment_types": ttypes,
            "is_crrt": any(t.lower() == "crrt" for t in ttypes),
            "has_generic_dialysis_mention": bool(r.get("ai_has_generic_dialysis_mention")),
            "facility_attribution": r.get("ai_facility_attribution"),
            "possible_mixed_facility_record": bool(mixed) if mixed is not None else None,
            "mixed_facility_warning": (
                "Possible mixed-facility/branch record — evidence may refer to a different facility or branch."
                if mixed else None
            ),
            "evidence_quotations": _complex(r.get("ai_evidence_quotations")) or [],
            "validator_version": r.get("ai_validator_version"),
            "note": r.get("ai_enrichment_note") or AI_VALIDATED_NOTE,
        }
    return result


def search_facilities(
    service: str,
    city: Optional[str] = None,
    pin: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
) -> Dict[str, Any]:
    """Search the national facility-services view. Raises on invalid input or query
    failure — never returns mock data."""
    if service not in _SERVICE_CODES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown service: {service!r}. Valid codes: {sorted(_SERVICE_CODES)}",
        )

    # Server-validated integers only — LIMIT/OFFSET literals are safe; user text
    # (service/city/pin) always goes through named parameters.
    page = max(1, int(page))
    page_size = min(100, max(1, int(page_size)))
    limit, offset = page_size, (page - 1) * page_size

    clauses = ""
    params: Dict[str, Any] = {"service": service}
    if city:
        clauses += " AND lower(g.city_clean) = lower(:city)"
        params["city"] = city.strip()
    if pin:
        clauses += " AND g.pincode_clean = :pin"
        params["pin"] = pin.strip()

    base = _SELECT + clauses
    count_sql = f"SELECT COUNT(*) AS total_count FROM ({base}) t"
    data_sql = (
        base + f" ORDER BY g.facility_name, g._source_row_id LIMIT {limit} OFFSET {offset}"
    )

    total = run_query(count_sql, params)[0]["total_count"]
    rows = run_query(data_sql, params)

    return {
        "total_count": total,
        "page": page,
        "page_size": page_size,
        "note": SERVICE_NOTE,
        "results": [_shape_row(r) for r in rows],
    }


# ---------------------------------------------------------------------------
# Free-text search (q) over the persisted search-document index
#
# The index table (vf.search.facility_search_documents_v1) is built once from
# vf.silver.facilities_current_v1; web requests never explode the source
# table. Matching is a token-boundary phrase match of the normalized query
# (or an orthographic alias) within a SINGLE evidence element — fragments
# are never combined across fields. All user text travels as named parameters
# and is treated as literal text (never as SQL or regex). No model calls,
# embeddings, or external services are involved.
# ---------------------------------------------------------------------------

# App-facing narrow views (vf.app) expose only the columns this search needs;
# the app identity is never granted SELECT on internal tables or the review
# view's full column set.
SEARCH_DOCS = "vf.app.vw_facility_search_documents"
FACILITIES_TABLE = "vf.app.vw_facilities_public"
REVIEW_QUEUE_TABLE = "vf.app.vw_facilities_review_flags"
GOLD_MENTIONS_VIEW = "vf.gold.facility_service_mentions_v2"
PILOT_VIEW = "vf.gold.dialysis_jaipur_pilot_v2"

MAX_QUERY_CHARS = 120
MAX_QUERY_TOKENS = 8
MAX_MATCH_VARIANTS = 24
EVIDENCE_DISPLAY_CAP = 12

# Alias groups — additive, inspectable, and deliberately narrow: spelling
# variants, abbreviations, dotted-letter forms, and the reviewed textual
# aliases of the six-service mapping (service_mention_rules_v1). These are
# ORTHOGRAPHIC equivalents only. No clinical equivalence is expressed or
# implied: nephrology is deliberately absent, plain "section" is not
# "c section", and nothing here infers emergency capability.
# Audit corrections (2026-11-22): all matching is token-boundary — raw
# substrings never match ("mri" inside "Amritsar" or "nicu" inside
# "Genicular" is not a match); bare "cardiothoracic"/"cardio thoracic" are
# excluded from cardiac surgery (surgical context required, e.g.
# "cardiothoracic surgery"); "cardiac surgeon"/"heart surgeon" match but are
# labeled specialist mentions, not proof of onsite surgery; the verified
# "Caesarian" spelling and explicit burn-surgery phrases are included.
ALIAS_GROUPS: List[List[str]] = [
    # Dialysis family — reviewed mapping (dialysis + separately tagged modalities)
    ["dialysis", "hemodialysis", "haemodialysis", "peritoneal dialysis",
     "crrt", "continuous renal replacement therapy",
     "hemofiltration", "haemofiltration"],
    # Cesarean delivery — reviewed mapping + verified "Caesarian" spelling
    ["cesarean", "cesarean section", "caesarean", "caesarean section",
     "caesarian", "caesarian section", "c section", "lscs"],
    # NICU — abbreviation requires token boundaries; full-name equivalents kept
    ["nicu", "neonatal intensive care", "neonatal icu"],
    # Cardiac surgery — surgical context required; bare "cardiothoracic" and
    # "cardio thoracic" are deliberately EXCLUDED (they match non-surgical
    # text such as "cardiothoracic radiology"). "cardiac surgeon"/"heart
    # surgeon" are specialist mentions — labeled at query time.
    ["cardiac surgery", "cardiac surgical", "heart surgery",
     "open heart surgery", "cabg", "cardiothoracic surgery",
     "cardio thoracic surgery", "cardiac surgeon", "heart surgeon",
     "cardiac surgeons", "heart surgeons",
     "cardiac surgeries", "heart surgeries"],
    # MRI — token boundaries required for the abbreviation (engine-enforced);
    # full-name equivalent included
    ["mri", "m r i", "magnetic resonance imaging"],
    # Burn care — explicit phrases only; bare "burn" substrings excluded
    ["burn care", "burns care", "burn unit", "burns unit",
     "burn treatment", "burns treatment", "burn surgery", "burns surgery",
     "burn surgeries"],
    # Orthographic equivalents (examples per project brief)
    ["physiotherapy", "physical therapy", "physio therapy", "physio"],
    ["chemotherapy", "chemo"],
    ["pediatric", "paediatric"],
]

DIALYSIS_FAMILY = set(ALIAS_GROUPS[0])
# Modifiers that may accompany a service phrase without changing its family
# (they stay in the match — never dropped).
ENRICHMENT_MODIFIERS = {"pediatric", "paediatric"}

# Aliases that name a specialist rather than evidence that the service is
# performed onsite. Evidence matched via these (or their plural forms) is
# labeled at query time.
SPECIALIST_MENTION_ALIASES = frozenset({"cardiac surgeon", "heart surgeon"})


def _is_specialist_mention(matched_terms) -> bool:
    """True when a matched phrase names a specialist (incl. plural forms)."""
    for m in matched_terms:
        for a in SPECIALIST_MENTION_ALIASES:
            if m == a or m == a + "s":
                return True
    return False

# Legacy service codes -> the canonical user-facing care phrase searched by
# the single matching engine. Quick-search buttons and typed input both use
# the q path; service= requests are translated here so every caller shares
# one engine and one result set per phrase.
SERVICE_CANONICAL: Dict[str, str] = {
    "dialysis": "dialysis",
    "MRI": "mri",
    "NICU": "nicu",
    "burn_care": "burn care",
    "cardiac_surgery": "cardiac surgery",
    "cesarean_delivery": "c section",
}

# Reviewed service-family equivalence: the unmodified phrase
# "cesarean delivery" (and its spelling variants) names the same reviewed
# service family as the canonical "c section" phrase, so typed input and the
# quick-search button return one consistent record set. Modifiers (e.g.
# "emergency") are preserved by the rewrite.
_REVIEWED_EQUIVALENTS: Dict[Tuple[str, ...], Tuple[str, ...]] = {
    ("cesarean", "delivery"): ("c", "section"),
    ("caesarean", "delivery"): ("c", "section"),
    ("caesarian", "delivery"): ("c", "section"),
}


def _apply_reviewed_equivalence(tokens: List[str]) -> Tuple[List[str], Optional[str]]:
    """Rewrite reviewed equivalent token pairs (modifiers preserved)."""
    out: List[str] = []
    note: Optional[str] = None
    i = 0
    while i < len(tokens):
        pair = tuple(tokens[i:i + 2])
        if pair in _REVIEWED_EQUIVALENTS:
            repl = _REVIEWED_EQUIVALENTS[pair]
            if note is None:
                note = (
                    f"'{' '.join(pair)}' was searched as the same reviewed "
                    f"service family as '{' '.join(repl)}' (modifiers preserved)."
                )
            out.extend(repl)
            i += 2
        else:
            out.append(tokens[i])
            i += 1
    return out, note


# ---------------------------------------------------------------------------
# Explainable ordering (sort=evidence by default) — deterministic policy.
# A provisional shortlist order, NEVER a medical ranking or score.
# ---------------------------------------------------------------------------
SORT_OPTIONS = ("evidence", "name", "distance")

# Source fields of the search index -> conservative evidence category.
# These are the only source_field values the index contains.
SOURCE_FIELD_RANK: Dict[str, int] = {
    "capability": 0,    # services/capability list — closest to a service claim
    "procedure": 1,     # procedure list
    "description": 2,   # free-text facility description
    "specialties": 3,   # specialties list
    "equipment": 4,    # equipment list
}
SOURCE_FIELD_LABEL: Dict[str, Tuple[str, str]] = {
    "capability": ("Service listed in source text", "services list"),
    "procedure": ("Procedure listed", "procedures list"),
    "description": ("Service listed in source text", "facility description"),
    "specialties": ("Specialty mention only", "specialties list"),
    "equipment": ("Equipment mention only", "equipment list"),
}
_RANK_TO_FIELD = {v: k for k, v in SOURCE_FIELD_RANK.items()}

# Records carrying any of these review flags have a KNOWN location
# conflict or unresolved geography: they are demoted below ordinary
# positive matches in evidence ordering (never deleted) and excluded from
# distance search — a known-conflict location must not be presented as a
# reliable nearby option.
LOCATION_CONFLICT_CODES = (
    "geography_review_required",
    "state_alias_or_conflict_unresolved",
    "coordinates_missing_or_failed_india_screen",
    "geography_reference_unmatched",
    "geography_facility_pin_missing_or_invalid",
)
_LOCATION_CONFLICT_SQL = "ARRAY(" + ", ".join(f"'{c}'" for c in LOCATION_CONFLICT_CODES) + ")"

# Narrow app-facing contact/coordinate views (source values only).
CONTACTS_VIEW = "vf.app.vw_facilities_contacts"
COORDS_VIEW = "vf.app.vw_facilities_coordinates"
MAX_CONTACT_PHONES = 3
MAX_CONTACT_WEBSITES = 3

# Straight-line distance only (no driving time). The India rectangle is an
# additional SCREEN on recorded coordinates — passing it is not verified
# geolocation. Haversine constant in km.
INDIA_RECT = (6.5, 37.5, 68.0, 97.5)
_EARTH_RADIUS_KM = 6371.0088
DEFAULT_RADIUS_KM = 25.0
MIN_RADIUS_KM = 0.5
MAX_RADIUS_KM = 500.0


def _category_label(
    src_rank: Optional[int], ai_status: Optional[str], ai_assertion: Optional[str]
) -> Tuple[str, str]:
    """Plain-language evidence label derived from actual evidence and
    interpretation. 'Website statement available' is applied separately
    (retrieved pilot evidence) and is never treated as verified care."""
    if ai_status == "validated_extraction":
        if ai_assertion == "explicit_service_claim":
            return ("Service listed in source text", "AI-validated interpretation")
        if ai_assertion == "procedure_list_claim":
            return ("Procedure listed", "AI-validated interpretation")
        if ai_assertion == "specialty_only":
            return ("Specialty mention only", "AI-validated interpretation")
        if ai_assertion == "equipment_only":
            return ("Equipment mention only", "AI-validated interpretation")
        if ai_assertion in ("ambiguous", "negated", "planned_or_historical_only", "not_mentioned"):
            return ("Conflicting or unclear information", f"AI interpretation: {ai_assertion}")
    label = SOURCE_FIELD_LABEL.get(_RANK_TO_FIELD.get(src_rank if src_rank is not None else -1, ""),
                                   ("Service listed in source text", "source text"))
    if ai_status == "extraction_needs_review":
        return ("Interpretation awaiting review", label[1])
    return label


def _unresolved_reasons(r: Dict[str, Any], conflict_codes: List[str]) -> List[str]:
    """Human-readable reasons a record is separated from ordinary
    positive matches (dimension C of the ordering policy)."""
    reasons: List[str] = []
    if conflict_codes:
        reasons.append(
            "Known location conflict — recorded location needs clarification before use"
        )
    if r.get("ai_possible_mixed_facility_record"):
        reasons.append(
            "Possible mixed-facility/branch record — evidence may describe another branch"
        )
    if r.get("pilot_needs_attribution_review"):
        reasons.append("Retrieved website attribution unresolved for this record")
    a = r.get("ai_assertion")
    if r.get("ai_enrichment_status") == "validated_extraction" and a in (
        "ambiguous", "negated", "planned_or_historical_only", "not_mentioned"
    ):
        reasons.append(f"AI interpretation is {a.replace('_or_', '/').replace('_', ' ')} — not a positive claim")
    return reasons


_PHONE_RE = re.compile(r"\+?[0-9][0-9 ()\-]{5,}[0-9]")
_URL_RE = re.compile(r"^(https?://|www\.)[^\s]+$", re.IGNORECASE)


def _valid_phone(s: str) -> bool:
    digits = re.sub(r"\D", "", s)
    return 7 <= len(digits) <= 13


def _source_strings(v: Any) -> List[str]:
    """Extract the individual string values of a source contact field.
    Values are stored as JSON-array strings (or plain strings); both forms
    are preserved as-is — only parsing, never rewriting."""
    v = _complex(v)
    if isinstance(v, list):
        return [str(x).strip() for x in v if x is not None and str(x).strip()]
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        try:
            j = json.loads(s)
            if isinstance(j, list):
                return [str(x).strip() for x in j if x is not None and str(x).strip()]
        except (ValueError, TypeError):
            pass
        return [s]
    return []


def _shape_contacts(r: Dict[str, Any], conflict: bool) -> Optional[Dict[str, Any]]:
    """Validated, deduplicated, capped contacts from stored source values.
    Nothing is invented; values that fail syntax validation are dropped;
    the result is always labeled unverified."""
    phones: List[str] = []
    for p in _source_strings(r.get("contact_phones")):
        if _PHONE_RE.search(p) and _valid_phone(p) and p not in phones:
            phones.append(p)
    sites: List[str] = []
    for u in _source_strings(r.get("contact_websites")):
        if _URL_RE.match(u) and u not in sites:
            sites.append(u)
    if not phones and not sites:
        return None
    return {
        "phones": phones[:MAX_CONTACT_PHONES],
        "websites": sites[:MAX_CONTACT_WEBSITES],
        "note": "Unverified — copied from source data; confirm directly with the facility.",
        "branch_warning": (
            "Contact details may belong to another branch — this record has a "
            "location/attribution conflict."
            if conflict else None
        ),
    }

# Conversational glue: a query containing any of these tokens is an
# unsupported conversational request. We say so and suggest a shorter care
# phrase instead of pretending to understand it.
CONVERSATIONAL_TOKENS = frozenset({
    "i", "me", "my", "we", "our", "you", "need", "want", "wanting",
    "looking", "look", "seek", "seeking", "searching", "search", "please",
    "help", "can", "could", "would", "should", "find", "get", "give",
    "best", "good", "better", "top", "nearest", "near", "nearby", "close",
    "around", "there", "any", "anyone", "which", "where", "who",
    "recommend", "recommended", "recommendation", "suggest", "suggestion",
    "tell", "show", "list",
})

CONVERSATIONAL_SUGGESTION = (
    "This looks like a sentence rather than a care phrase. Try a short "
    "service phrase such as 'cataract surgery', 'chemotherapy', "
    "'physiotherapy', or 'dental implants'."
)

FREE_TEXT_MATCH_METHOD = (
    "Token-boundary phrase match: the normalized query (or an orthographic "
    "alias) must appear as a contiguous phrase inside a single evidence "
    "element. Case, punctuation, and camel-case boundaries are normalized "
    "on both sides; fragments are never combined across fields."
)

FREE_TEXT_ROW_NOTE = (
    "Unverified text mention matched by free-text search. Context may "
    "negate or qualify it, and text matching has not resolved clinical "
    "context."
)

FREE_TEXT_LIMITATIONS = [
    "Text matching has not resolved clinical context — a mention is not "
    "verification that the service is currently offered.",
    "Only orthographic equivalents and the reviewed six-service textual "
    "aliases are matched; no clinical synonyms are inferred (e.g., "
    "nephrology is never treated as dialysis).",
    "Every match must appear within a single evidence element; unrelated "
    "fragments across fields are never combined.",
]


def _norm_text(s: str) -> str:
    """Mirror of the SQL normalization used to build search_text_norm:
    camel-case split, lowercase, punctuation to space, whitespace collapse."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", s)
    s = s.lower()
    s = re.sub(r"[\W_]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _last_token_forms(phrase: str) -> List[str]:
    """Unambiguous singular/plural forms of the phrase's final token
    (e.g., 'dental implants' <-> 'dental implant'). No stemming beyond
    this; words ending in 'ss' or 'is' (dialysis, hemodialysis) are left
    alone, as are y-final words (surgery)."""
    toks = phrase.split()
    if not toks:
        return []
    last = toks[-1]
    out = []
    if last.endswith("ies") and len(last) > 4:
        out.append(" ".join(toks[:-1] + [last[:-3] + "y"]))
    elif last.endswith("s") and not last.endswith(("ss", "is")) and len(last) > 3:
        out.append(" ".join(toks[:-1] + [last[:-1]]))
    if not last.endswith("s") and not last.endswith("y"):
        out.append(" ".join(toks[:-1] + [last + "s"]))
    return [p for p in out if p != phrase]


def _expand_variants(base: str) -> List[str]:
    """Ordered match variants: base first, then alias substitutions applied
    at token boundaries to a fixpoint, then plain singular/plural forms.
    Bounded by MAX_MATCH_VARIANTS. Every variant is a plain literal phrase
    — no regex or SQL is ever generated from user input."""
    ordered = [base]
    seen = {base}
    for _ in range(3):
        new = []
        for v in list(ordered):
            toks = v.split()
            for group in ALIAS_GROUPS:
                for member in group:
                    mt = member.split()
                    n = len(mt)
                    for i in range(len(toks) - n + 1):
                        if toks[i:i + n] == mt:
                            for repl in group:
                                if repl == member:
                                    continue
                                cand = " ".join(toks[:i] + repl.split() + toks[i + n:])
                                if cand not in seen:
                                    new.append(cand)
                                    seen.add(cand)
        ordered.extend(new)
        if not new or len(ordered) >= MAX_MATCH_VARIANTS:
            break
    for v in list(ordered):
        for cand in _last_token_forms(v):
            if cand not in seen and len(ordered) < MAX_MATCH_VARIANTS:
                ordered.append(cand)
                seen.add(cand)
    return ordered[:MAX_MATCH_VARIANTS]


def _maps_to_dialysis_family(base: str) -> bool:
    """The query maps unambiguously to the dialysis service family when,
    after removing patient-scope modifier tokens (pediatric/paediatric,
    which stay in the match itself), the remaining phrase is exactly one of
    the reviewed dialysis-family aliases. Everything else is plain text
    search with no enrichment."""
    service_tokens = [t for t in base.split() if t not in ENRICHMENT_MODIFIERS]
    return " ".join(service_tokens) in DIALYSIS_FAMILY


def _matches_cte(variants: List[str]) -> Tuple[str, Dict[str, Any]]:
    """Parameterized UNION-ALL match arms, one per literal phrase variant.
    instr() is a literal substring test — no wildcard/regex semantics."""
    parts = []
    params: Dict[str, Any] = {}
    for i, phrase in enumerate(variants):
        name = f"p{i}"
        params[name] = phrase
        parts.append(
            f"SELECT d.record_id, d.source_field, d.element_position, d.evidence_text, :{name} AS matched_phrase "
            f"FROM {SEARCH_DOCS} d "
            f"WHERE instr(concat(' ', d.search_text_norm, ' '), concat(' ', :{name}, ' ')) > 0"
        )
    return " UNION ALL ".join(parts), params


def _location_clauses(city: Optional[str], pin: Optional[str]) -> Tuple[List[str], Dict[str, Any]]:
    clauses: List[str] = []
    params: Dict[str, Any] = {}
    if city:
        clauses.append("lower(f.city_clean) = lower(:city)")
        params["city"] = city.strip()
    if pin:
        clauses.append("f.pincode_clean = :pin")
        params["pin"] = pin.strip()
    return clauses, params


# Gold/pilot columns consumed by _shape_row for the dialysis enrichment path.
_AI_COLS = [
    "ai_enrichment_status", "ai_assertion", "ai_patient_scope",
    "ai_treatment_types_for_display", "ai_has_generic_dialysis_mention",
    "ai_facility_attribution", "ai_possible_mixed_facility_record",
    "ai_evidence_quotations", "ai_validator_version", "ai_enrichment_note",
]
_PILOT_COLS = [
    ("pilot_record_id", "pl.record_id"),
    ("evidence_label", "pl.evidence_label"),
    ("retrieved_evidence_quote", "pl.retrieved_evidence_quote"),
    ("retrieved_evidence_url", "pl.retrieved_evidence_url"),
    ("retrieved_evidence_retrieved_at", "pl.retrieved_evidence_retrieved_at"),
    ("retrieved_temporal_context", "pl.retrieved_temporal_context"),
    ("retrieved_branch_address_quote", "pl.retrieved_branch_address_quote"),
    ("retrieved_branch_address_url", "pl.retrieved_branch_address_url"),
    ("retrieved_branch_assessment", "pl.retrieved_branch_assessment"),
    ("retrieved_unresolved_issues", "pl.retrieved_unresolved_issues"),
    ("pilot_unattributed_urls", "pl.unattributed_source_url_candidates"),
    ("pilot_needs_attribution_review", "pl.needs_attribution_review"),
]


def _haversine_sql(lat: str = "latitude", lng: str = "longitude") -> str:
    """Straight-line (haversine) distance in km from the search point
    (:olat/:olng, named parameters) to the record's RECORDED coordinates.
    No user input ever enters this SQL text."""
    return (
        f"2 * {_EARTH_RADIUS_KM} * asin(sqrt("
        f"pow(sin(radians(:olat - {lat}) / 2), 2) + "
        f"cos(radians(:olat)) * cos(radians({lat})) * "
        f"pow(sin(radians(:olng - {lng}) / 2), 2)))"
    )


# Documented ordering policy (exposed verbatim in every response).
ORDERING_RULES: Dict[str, List[str]] = {
    "evidence": [
        "Ordinary positive matches come first — records with no unresolved issue flag.",
        "Within ordinary matches, by conservative evidence category: AI-validated explicit service claim (dialysis searches only), AI-validated procedure list, services/capability list text, procedures list text, facility description text, specialty mentions, then equipment mentions.",
        "Records needing clarification come after ordinary matches — negated / planned-or-historical / ambiguous interpretation, possible mixed-facility or attribution problems, or known location conflicts. They are never deleted, only clearly separated and flagged.",
        "Ties are broken by facility name (A–Z), then record ID. Mention counts, URL counts, and repeated text are never used.",
        "This is a provisional shortlist policy, not a clinically validated ranking.",
    ],
    "name": [
        "Facility name A–Z, then record ID.",
        "Unresolved-issue flags remain visible on the cards but do not reorder records under this sort.",
    ],
    "distance": [
        "Straight-line (haversine) distance from the search point to the RECORDED facility coordinates, nearest first.",
        "Records with missing or invalid coordinates, coordinates outside the India screen rectangle, or known location conflicts are excluded from distance search; the exclusion counts are reported. Passing the India rectangle is a screen, not verified geolocation.",
        "Ties are broken by facility name (A–Z), then record ID. Distance is never a medical suitability signal.",
    ],
}


def search_facilities_text(
    q: str,
    city: Optional[str] = None,
    pin: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    sort: str = "evidence",
    origin: Optional[Dict[str, Any]] = None,
    radius_km: Optional[float] = None,
) -> Dict[str, Any]:
    """Free-text facility search over the persisted search-document index.

    One result per source record with aggregated matched evidence. Ordering
    (sort=evidence/name/distance) is computed in SQL BEFORE pagination — a
    documented provisional shortlist policy, never a medical ranking. Raises
    HTTPException(400) for empty, oversized, or conversational queries (with
    a suggestion), for invalid sort/origin values, and for incompatible
    filters (origin combined with city/PIN). Never returns mock data."""
    original = (q or "").strip()
    if not original:
        raise HTTPException(status_code=400, detail="Search phrase is empty.")
    if len(original) > MAX_QUERY_CHARS:
        raise HTTPException(
            status_code=400,
            detail=(f"Search phrase is too long (max {MAX_QUERY_CHARS} "
                    "characters). Try a shorter care phrase, e.g. 'cataract surgery'."),
        )
    base = _norm_text(original)
    tokens = base.split()
    if not tokens:
        raise HTTPException(
            status_code=400,
            detail="Search phrase contained no searchable words.",
        )
    if len(tokens) > MAX_QUERY_TOKENS:
        raise HTTPException(
            status_code=400,
            detail=(f"Search phrase is too long (max {MAX_QUERY_TOKENS} words). "
                    "Try a shorter care phrase, e.g. 'cataract surgery'."),
        )
    if any(t in CONVERSATIONAL_TOKENS for t in tokens):
        raise HTTPException(status_code=400, detail=CONVERSATIONAL_SUGGESTION)

    # Reviewed service-family equivalence (documented and inspectable in
    # the response): keeps the unmodified phrase consistent with the
    # reviewed family phrase while preserving every modifier.
    tokens, equivalence_note = _apply_reviewed_equivalence(tokens)
    base = " ".join(tokens)

    # Sort and origin validation. An origin is accepted only via the request
    # BODY (never a URL query parameter) so precise coordinates never appear
    # in URL/query logs. Origin and city/PIN are incompatible filters and are
    # never silently combined.
    if sort not in SORT_OPTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown sort: {sort!r}. Valid sorts: {', '.join(SORT_OPTIONS)}.",
        )
    if origin is not None:
        if city or pin:
            raise HTTPException(
                status_code=400,
                detail=("Device location and city/PIN filters cannot be combined — "
                        "choose one way to narrow the search."),
            )
        try:
            olat = float(origin.get("lat"))
            olng = float(origin.get("lng"))
            oacc = float(origin.get("accuracy_m") or 0.0)
        except (TypeError, ValueError, KeyError):
            raise HTTPException(
                status_code=400,
                detail="Origin must provide numeric 'lat' and 'lng' (degrees).",
            )
        if olat != olat or olng != olng or not (-90.0 <= olat <= 90.0) or not (-180.0 <= olng <= 180.0):
            raise HTTPException(
                status_code=400, detail="Origin coordinates are out of range."
            )
        if not (0.0 <= oacc <= 100000.0):
            raise HTTPException(
                status_code=400, detail="Origin accuracy_m is out of range (0-100000)."
            )
        radius = DEFAULT_RADIUS_KM if radius_km is None else float(radius_km)
        if not (MIN_RADIUS_KM <= radius <= MAX_RADIUS_KM):
            raise HTTPException(
                status_code=400,
                detail=(f"Radius must be between {MIN_RADIUS_KM} and {MAX_RADIUS_KM} km; "
                        "the radius is never silently expanded."),
            )
    else:
        if sort == "distance":
            raise HTTPException(
                status_code=400,
                detail="Distance ordering requires a device location (origin).",
            )
        radius = None

    variants = _expand_variants(base)
    dialysis = _maps_to_dialysis_family(base)

    page = max(1, int(page))
    page_size = min(100, max(1, int(page_size)))
    limit, offset = page_size, (page - 1) * page_size

    matches_sql, params = _matches_cte(variants)
    clauses, lparams = _location_clauses(city, pin)
    params.update(lparams)

    # Count query: distinct records only — no evidence aggregation and no
    # enrichment joins. The phrases actually used come from this same pass.
    # Distance mode additionally reports WHY records were excluded (missing
    # or invalid coordinates, outside the India screen rectangle, known
    # location conflicts, outside the chosen radius) — exclusions are
    # explained, never silent.
    if origin is not None:
        params.update({"olat": olat, "olng": olng, "radius_km": radius})
        hav = _haversine_sql()
        count_sql = f"""
WITH m AS ({matches_sql}),
per AS (
  SELECT m.record_id,
         ANY_VALUE(c.latitude) AS latitude,
         ANY_VALUE(c.longitude) AS longitude,
         ANY_VALUE(rq.review_reason_codes) AS review_reason_codes
  FROM m
  LEFT JOIN {COORDS_VIEW} c ON c._source_row_id = m.record_id
  LEFT JOIN {REVIEW_QUEUE_TABLE} rq ON rq._source_row_id = m.record_id
  GROUP BY m.record_id
),
ev AS (
  SELECT record_id,
         CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
               AND NOT isnan(latitude) AND NOT isnan(longitude)
               AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180
               AND NOT (latitude = 0 AND longitude = 0)
              THEN 1 ELSE 0 END AS coords_ok,
         CASE WHEN latitude BETWEEN {INDIA_RECT[0]} AND {INDIA_RECT[1]}
               AND longitude BETWEEN {INDIA_RECT[2]} AND {INDIA_RECT[3]}
              THEN 1 ELSE 0 END AS in_india,
         CASE WHEN size(array_intersect(coalesce(review_reason_codes, array('')),
                                        {_LOCATION_CONFLICT_SQL})) > 0
              THEN 1 ELSE 0 END AS location_conflict,
         {hav} AS distance_km
  FROM per
),
b AS (
  SELECT record_id,
         CASE WHEN coords_ok = 0 THEN 'no_or_invalid_coords'
              WHEN in_india = 0 THEN 'outside_india_rect'
              WHEN location_conflict = 1 THEN 'location_conflict'
              WHEN distance_km > :radius_km THEN 'outside_radius'
              ELSE 'included' END AS bucket
  FROM ev
),
ph AS (SELECT array_sort(array_distinct(collect_list(m.matched_phrase))) AS phrases_matched FROM m)
SELECT
  SUM(CASE WHEN b.bucket = 'included' THEN 1 ELSE 0 END) AS total_count,
  SUM(CASE WHEN b.bucket = 'no_or_invalid_coords' THEN 1 ELSE 0 END) AS excluded_no_coords,
  SUM(CASE WHEN b.bucket = 'outside_india_rect' THEN 1 ELSE 0 END) AS excluded_outside_india,
  SUM(CASE WHEN b.bucket = 'location_conflict' THEN 1 ELSE 0 END) AS excluded_location_conflict,
  SUM(CASE WHEN b.bucket = 'outside_radius' THEN 1 ELSE 0 END) AS excluded_outside_radius,
  MAX(ph.phrases_matched) AS phrases_matched
FROM b CROSS JOIN ph
"""
    else:
        count_filter = (
            f" FROM m JOIN {FACILITIES_TABLE} f ON f._source_row_id = m.record_id"
            f" WHERE {' AND '.join(clauses)}"
            if clauses else " FROM m"
        )
        count_sql = (
            f"WITH m AS ({matches_sql}) "
            f"SELECT COUNT(DISTINCT m.record_id) AS total_count, "
            f"array_sort(array_distinct(collect_list(m.matched_phrase))) AS phrases_matched"
            f"{count_filter}"
        )

    # Data query: one result per record, evidence aggregated per record.
    # The EXPLAINABLE ORDERING is computed here in SQL BEFORE pagination.
    # Enrichment and contacts are joined only for the page's rows —
    # enrichment only when the query maps unambiguously to dialysis.
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    if dialysis:
        service_expr = "g.service_code AS service_code"
        ai_exprs = [f"g.{c} AS {c}" for c in _AI_COLS]
        pilot_exprs = [f"{expr} AS {alias}" for alias, expr in _PILOT_COLS]
        enrich_joins = (
            f" LEFT JOIN {GOLD_MENTIONS_VIEW} g "
            f"ON g._source_row_id = p.record_id AND g.service_code = 'dialysis'"
            f" LEFT JOIN {PILOT_VIEW} pl "
            f"ON pl.record_id = g._source_row_id AND pl.service_code = g.service_code"
        )
        # Ranking inputs joined BEFORE pagination so the ordering uses the
        # validated AI interpretation that applies to the requested service.
        rank_joins = (
            f" LEFT JOIN {GOLD_MENTIONS_VIEW} gr "
            f"ON gr._source_row_id = a.record_id AND gr.service_code = 'dialysis'"
            f" LEFT JOIN {PILOT_VIEW} plr "
            f"ON plr.record_id = gr._source_row_id AND plr.service_code = gr.service_code"
        )
        rank_cols = (
            "gr.ai_enrichment_status AS rank_ai_status,"
            " gr.ai_assertion AS rank_ai_assertion,"
            " gr.ai_possible_mixed_facility_record AS rank_ai_mixed,"
            " plr.needs_attribution_review AS rank_pilot_review"
        )
    else:
        service_expr = "NULL AS service_code"
        ai_exprs = [f"NULL AS {c}" for c in _AI_COLS]
        pilot_exprs = [f"NULL AS {alias}" for alias, _ in _PILOT_COLS]
        enrich_joins = ""
        rank_joins = ""
        rank_cols = (
            "NULL AS rank_ai_status, NULL AS rank_ai_assertion,"
            " NULL AS rank_ai_mixed, NULL AS rank_pilot_review"
        )

    # Deterministic ordering expressions (static SQL text — only documented
    # view/table names and rank CASE logic; no user input ever enters).
    conflict_b = (
        f"size(array_intersect(coalesce(b.review_reason_codes, array(''))"
        f", {_LOCATION_CONFLICT_SQL})) > 0"
    )
    unresolved_expr = (
        "CASE WHEN (b.rank_ai_status = 'validated_extraction' AND b.rank_ai_assertion IN "
        "('ambiguous','negated','planned_or_historical_only','not_mentioned'))"
        " OR b.rank_ai_mixed OR b.rank_pilot_review"
        f" OR {conflict_b} THEN 1 ELSE 0 END"
    )
    category_expr = (
        "CASE"
        " WHEN b.rank_ai_status = 'validated_extraction' AND b.rank_ai_assertion = 'explicit_service_claim' THEN 0"
        " WHEN b.rank_ai_status = 'validated_extraction' AND b.rank_ai_assertion = 'procedure_list_claim' THEN 1"
        " WHEN b.rank_ai_status = 'validated_extraction' AND b.rank_ai_assertion = 'specialty_only' THEN 5"
        " WHEN b.rank_ai_status = 'validated_extraction' AND b.rank_ai_assertion = 'equipment_only' THEN 6"
        " WHEN b.src_rank = 0 THEN 2"
        " WHEN b.src_rank = 1 THEN 3"
        " WHEN b.src_rank = 2 THEN 4"
        " WHEN b.src_rank = 3 THEN 5"
        " ELSE 6 END"
    )
    if sort == "name":
        order_expr = "s.facility_name NULLS LAST, s.record_id ASC"
    elif sort == "distance":
        order_expr = "s.distance_km ASC, s.facility_name NULLS LAST, s.record_id ASC"
    else:
        order_expr = "s.unresolved ASC, s.category_rank ASC, s.facility_name NULLS LAST, s.record_id ASC"
    if origin is not None:
        # Distance-mode filters: usable recorded coordinates, inside the
        # India screen rectangle, no known location conflict, within the
        # user-chosen radius. Every exclusion is counted and reported.
        distance_expr = f"{_haversine_sql('b.latitude', 'b.longitude')} AS distance_km"
        conflict_s = (
            f"size(array_intersect(coalesce(s.review_reason_codes, array(''))"
            f", {_LOCATION_CONFLICT_SQL})) > 0"
        )
        distance_where = (
            " WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL"
            " AND NOT isnan(s.latitude) AND NOT isnan(s.longitude)"
            " AND s.latitude BETWEEN -90 AND 90 AND s.longitude BETWEEN -180 AND 180"
            " AND NOT (s.latitude = 0 AND s.longitude = 0)"
            f" AND s.latitude BETWEEN {INDIA_RECT[0]} AND {INDIA_RECT[1]}"
            f" AND s.longitude BETWEEN {INDIA_RECT[2]} AND {INDIA_RECT[3]}"
            f" AND NOT {conflict_s}"
            f" AND {_haversine_sql('s.latitude', 's.longitude')} <= :radius_km"
        )
    else:
        distance_expr = "NULL AS distance_km"
        distance_where = ""

    data_sql = f"""
WITH m AS ({matches_sql}),
elem AS (
  SELECT record_id, source_field, element_position, evidence_text,
         array_sort(array_distinct(collect_list(matched_phrase))) AS matched_terms
  FROM m
  GROUP BY record_id, source_field, element_position, evidence_text
),
agg AS (
  SELECT record_id,
         sort_array(collect_list(struct(source_field, element_position, evidence_text, matched_terms))) AS evidence_all,
         COUNT(*) AS evidence_count,
         array_sort(array_distinct(flatten(collect_list(matched_terms)))) AS matched_terms_all,
         MIN(CASE source_field
             WHEN 'capability' THEN 0
             WHEN 'procedure' THEN 1
             WHEN 'description' THEN 2
             WHEN 'specialties' THEN 3
             WHEN 'equipment' THEN 4
             ELSE 5 END) AS src_rank
  FROM elem
  GROUP BY record_id
),
base AS (
  SELECT a.record_id, a.evidence_all, a.evidence_count, a.matched_terms_all, a.src_rank,
         f.name_clean AS facility_name, f.city_clean AS city, f.state_clean AS state,
         f.pincode_clean AS pincode, c.coordinate_status, c.latitude, c.longitude,
         rq.review_priority, rq.review_reason_codes,
         {rank_cols}
  FROM agg a
  JOIN {FACILITIES_TABLE} f ON f._source_row_id = a.record_id
  LEFT JOIN {COORDS_VIEW} c ON c._source_row_id = a.record_id
  LEFT JOIN {REVIEW_QUEUE_TABLE} rq ON rq._source_row_id = a.record_id{rank_joins}{where}
),
scored AS (
  SELECT b.*,
    {unresolved_expr} AS unresolved,
    {category_expr} AS category_rank,
    {distance_expr}
  FROM base b
),
page AS (
  SELECT * FROM (
    SELECT s.*, row_number() OVER (ORDER BY {order_expr}) AS rn
    FROM scored s{distance_where}
  ) t WHERE rn > {offset} AND rn <= {offset + limit}
)
SELECT
  p.record_id, p.facility_name, p.city, p.state, p.pincode, p.coordinate_status,
  p.latitude, p.longitude,
  p.review_priority, p.review_reason_codes,
  p.evidence_count,
  CASE WHEN p.evidence_count > {EVIDENCE_DISPLAY_CAP} THEN slice(p.evidence_all, 1, {EVIDENCE_DISPLAY_CAP}) ELSE p.evidence_all END AS evidence,
  (p.evidence_count > {EVIDENCE_DISPLAY_CAP}) AS evidence_truncated,
  p.matched_terms_all,
  p.src_rank, p.category_rank, p.unresolved, p.distance_km,
  ct.phone_numbers AS contact_phones, ct.websites AS contact_websites,
  'unknown' AS current_availability_status,
  'unverified' AS verification_status,
  'pending' AS human_review_status,
  :row_note AS notice,
  {service_expr},
  {', '.join(ai_exprs)},
  {', '.join(pilot_exprs)}
FROM page p
LEFT JOIN {CONTACTS_VIEW} ct ON ct._source_row_id = p.record_id{enrich_joins}
ORDER BY p.rn ASC
"""
    # Count + data run on ONE connection (halves connect/auth overhead).
    # Timings are logged without query text, phrases, or coordinates.
    t_pair0 = time.perf_counter()
    count_rows, rows = run_query_pair(
        count_sql, params, data_sql, {**params, "row_note": FREE_TEXT_ROW_NOTE}
    )
    t_pair = time.perf_counter()
    count_row = count_rows[0] if count_rows else {}
    total = int(count_row.get("total_count") or 0)
    phrases_matched = _complex(count_row.get("phrases_matched")) or []
    aliases_used = [p for p in phrases_matched if p != base]

    t_shape0 = time.perf_counter()
    results = [_shape_row(r) for r in rows]
    t_shape = time.perf_counter()
    log.warning(
        "SEARCH timing: pair_ms=%.0f shape_ms=%.0f results=%d page=%d",
        (t_pair - t_pair0) * 1000.0, (t_shape - t_shape0) * 1000.0, len(results), page,
    )
    # Label specialist-mention evidence: a named specialist (e.g. a cardiac
    # surgeon) is not proof that surgery is performed onsite.
    specialist_used = False
    for res in results:
        for it in res["sql_evidence"]["items"]:
            if _is_specialist_mention(it.get("matched_terms", ())):
                specialist_used = True
                it["mention_kind"] = "specialist_mention"
                it["mention_note"] = (
                    "Specialist mention — names a specialist; not proof that "
                    "surgery is performed onsite."
                )

    limitations = list(FREE_TEXT_LIMITATIONS)
    if len(variants) >= MAX_MATCH_VARIANTS:
        limitations.append(
            "Alias expansion was capped; some equivalent spellings may not have been searched."
        )
    if not dialysis:
        limitations.append(
            "This search does not map to the dialysis service family, so no AI "
            "interpretation or pilot website evidence is attached."
        )
    if specialist_used:
        limitations.append(
            "Some matched evidence is a specialist mention (e.g., 'cardiac "
            "surgeon') — a named specialist is not proof that surgery is "
            "performed onsite."
        )
    if origin is not None:
        limitations.append(
            "Distance search uses RECORDED coordinates, not verified geolocation; "
            "distance is straight-line, not travel time."
        )

    # "Why is this first" — the actual criteria that placed the first result
    # first, for page 1 only. Never a best-hospital claim.
    first_explanation = None
    if page == 1 and results:
        o = results[0]["ordering"]
        if sort == "distance":
            first_explanation = (
                f"Closest recorded location to your search point "
                f"(about {o['distance_km']} km straight-line). Recorded location — "
                "not independently verified. Ties broken by facility name, "
                "then record ID. Not a medical ranking."
            )
        else:
            first_explanation = (
                f"First among {total} matching records under 'Evidence relevance': "
                f"{o['category_label'].lower()} ({o['category_detail']}); "
                + ("no unresolved issues flagged. " if not o["unresolved"]
                   else "unresolved-issue records are always placed after ordinary matches. ")
                + "Ties broken by facility name, then record ID. Provisional "
                "shortlist order — not a best-hospital ranking."
            )

    ordering_block: Dict[str, Any] = {
        "sort": sort,
        "rules": ORDERING_RULES[sort],
        "first_result_explanation": first_explanation,
    }
    if origin is not None:
        ordering_block["distance_scope"] = {
            "radius_km": radius,
            "user_accuracy_m": (int(oacc) if oacc else None),
            "distance_note": (
                "Straight-line distance to recorded coordinates — not travel "
                "time; recorded locations are not independently verified. "
                "Reported device accuracy is shown to avoid misleading precision."
            ),
            "excluded": {
                "no_or_invalid_coordinates": int(count_row.get("excluded_no_coords") or 0),
                "outside_india_rectangle": int(count_row.get("excluded_outside_india") or 0),
                "known_location_conflict": int(count_row.get("excluded_location_conflict") or 0),
                "outside_radius": int(count_row.get("excluded_outside_radius") or 0),
            },
        }

    response: Dict[str, Any] = {
        "total_count": total,
        "page": page,
        "page_size": page_size,
        "note": SERVICE_NOTE,
        "query": {
            "original": original,
            "interpreted": base,
            "aliases_used": aliases_used,
            "aliases_considered": [v for v in variants if v != base],
            "match_method": FREE_TEXT_MATCH_METHOD,
            "service_family": "dialysis" if dialysis else None,
            "limitations": limitations,
            "reviewed_equivalence": equivalence_note,
        },
        "ordering": ordering_block,
        "results": results,
    }
    if total == 0:
        response["no_results_note"] = (
            "No matching evidence found in this dataset. That does not mean "
            "no facilities offer it — only that no dataset text matched this phrase."
        )
        if origin is not None:
            response["no_results_note"] += (
                " For distance searches, records without usable recorded "
                "coordinates, outside the India screen rectangle, or with known "
                "location conflicts are excluded (counts above)."
            )
    return response
