"""Virtue Foundation Facility Finder — FastAPI application.

Serves the no-build frontend (frontend/) and the facility API from the same
origin. All database access runs through the app's managed identity via
DBSQL, with named (:name) parameters only.
"""
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from server.facilities import (
    SERVICES,
    SERVICE_CANONICAL,
    SERVICE_NOTE,
    SORT_OPTIONS,
    search_facilities,
    search_facilities_text,
)

FRONTEND_DIR = Path(__file__).parent / "frontend"

app = FastAPI(title="Virtue Foundation Facility Finder")


@app.get("/api/health")
def health() -> dict:
    """Liveness probe."""
    return {"status": "ok"}


@app.get("/api/facilities/services")
def facilities_services() -> dict:
    """List supported service codes and the dataset-wide caveat."""
    return {"services": SERVICES, "note": SERVICE_NOTE}


@app.get("/api/facilities/search")
def facilities_search(
    q: Optional[str] = Query(None),
    service: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pin: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    sort: str = Query("evidence"),
) -> dict:
    """Search facility records with ONE token-boundary matching engine.

    q: a free-text care phrase (e.g. "cataract surgery"). service: a legacy
    service code, translated to its canonical care phrase and run through
    the SAME engine — service= callers get record sets identical to typing
    the phrase. This deliberately drops the legacy index's substring false
    positives (e.g. "mri" inside "Amritsar"). Supplying both or neither is
    rejected. One result per record.

    sort: "evidence" (default; documented deterministic shortlist policy
    computed BEFORE pagination — never a medical ranking), "name", or
    "distance" (which requires a device origin and is only available via
    POST — precise coordinates never appear in URL query strings). Raises
    on invalid input or query failure — never returns mock data.
    """
    q = _resolve_query_or_service(q, service)
    return search_facilities_text(
        q=q, city=city, pin=pin, page=page, page_size=page_size, sort=sort
    )


class OriginBody(BaseModel):
    """Device location for a distance search. Travels ONLY in the request
    body (never a URL query string), is used solely for the requested
    distance search, and is never logged or persisted."""

    lat: float
    lng: float
    accuracy_m: Optional[float] = None


class SearchBody(BaseModel):
    q: Optional[str] = None
    service: Optional[str] = None
    city: Optional[str] = None
    pin: Optional[str] = None
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)
    sort: str = "evidence"
    origin: Optional[OriginBody] = None
    radius_km: Optional[float] = Field(None, ge=0.5, le=500)


@app.post("/api/facilities/search")
def facilities_search_post(body: SearchBody) -> dict:
    """Same engine and same filters as GET /api/facilities/search.

    The POST body additionally supports an optional device origin for
    distance search (sort=distance or an explicit radius filter): precise
    coordinates are accepted only here, used solely for this search, never
    logged, never persisted, and never combined silently with city/PIN
    (that combination is rejected with 400).
    """
    q = _resolve_query_or_service(body.q, body.service)
    origin = body.origin.model_dump() if body.origin is not None else None
    return search_facilities_text(
        q=q,
        city=body.city,
        pin=body.pin,
        page=body.page,
        page_size=body.page_size,
        sort=body.sort,
        origin=origin,
        radius_km=body.radius_km,
    )


def _resolve_query_or_service(q: Optional[str], service: Optional[str]) -> str:
    """Shared validation: exactly one of q (free-text care phrase) or
    service (legacy code) must be supplied; service is translated to its
    canonical care phrase so both paths use the SAME engine."""
    if q is not None and service is not None:
        raise HTTPException(
            status_code=400,
            detail="Provide either q (free-text care phrase) or service, not both.",
        )
    if service is not None:
        if service not in SERVICE_CANONICAL:
            raise HTTPException(
                status_code=400,
                detail=(f"Unknown service code: {service!r}. Valid codes: "
                        + ", ".join(sorted(SERVICE_CANONICAL)) + "."),
            )
        q = SERVICE_CANONICAL[service]
    if q is None:
        raise HTTPException(
            status_code=400,
            detail=("Provide q (a care phrase such as 'cataract surgery') or one "
                    "of the service codes."),
        )
    return q


# Static frontend mounted last so /api/* routes keep precedence.
app.mount(
    "/",
    StaticFiles(directory=str(FRONTEND_DIR), html=True),
    name="frontend",
)


# ---------------------------------------------------------------------------
# Startup self-test (diagnostic). Runs once at app start, IN THE DEPLOYED
# RUNTIME, exercising the real DBSQL connector under this app's managed
# identity, and logs a summary to the app logs. Non-fatal by design:
# failures are logged, the app never crashes, and there is no mock fallback.
# ---------------------------------------------------------------------------
def _selftest() -> None:
    import logging
    import os
    import threading

    # uvicorn.error is the logger whose output is verifiably shipped to app logs
    log = logging.getLogger("uvicorn.error")
    done = threading.Event()

    def _run() -> None:
        try:
            from server.facilities import SERVICES, search_facilities_text
            from server.sql import run_query

            env_keys = sorted(k for k in os.environ if k.startswith("DATABRICKS"))
            log.warning(
                "SELFTEST begin; databricks env keys present (names only): %s",
                env_keys,
            )
            one = run_query("SELECT 1 AS ok", {})[0]["ok"]
            log.warning("SELFTEST connector ok (select1=%s); services=%s", one, len(SERVICES))
            jaipur = search_facilities_text("dialysis", city="Jaipur", page=1, page_size=100)
            with_ret = [
                r for r in jaipur["results"]
                if (r.get("jaipur_pilot_supplement") or {}).get("retrieved_evidence_quote")
            ]
            first = jaipur["results"][0] if jaipur["results"] else None
            log.warning(
                "SELFTEST managed-identity DBSQL OK: jaipur_dialysis_total=%s; "
                "retrieved_evidence_rows=%s; retrieved_evidence_example=%s; "
                "sample_facility=%s; sample_sql_evidence_items=%s; sample_ai_status=%s",
                jaipur["total_count"],
                len(with_ret),
                (with_ret[0]["facility_name"] if with_ret else "none"),
                (first["facility_name"] if first else "none"),
                (len(first["sql_evidence"]["items"]) if first else 0),
                (first["ai_enrichment_status"] if first else "none"),
            )
            ft = search_facilities_text("physiotherapy", page=1, page_size=5)
            log.warning(
                "SELFTEST free-text q ok: interpreted=%s; aliases_used=%s; "
                "total=%s; first=%s",
                ft["query"]["interpreted"],
                ft["query"]["aliases_used"],
                ft["total_count"],
                (ft["results"][0]["facility_name"] if ft["results"] else "none"),
            )
            csec = search_facilities_text("c section", page=1, page_size=5)
            log.warning(
                "SELFTEST c-section q ok: interpreted=%s; total=%s; first=%s",
                csec["query"]["interpreted"],
                csec["total_count"],
                (csec["results"][0]["facility_name"] if csec["results"] else "none"),
            )
            mri = search_facilities_text("mri", page=1, page_size=5)
            log.warning(
                "SELFTEST mri q ok: interpreted=%s; aliases_used=%s; total=%s; first=%s",
                mri["query"]["interpreted"],
                mri["query"]["aliases_used"],
                mri["total_count"],
                (mri["results"][0]["facility_name"] if mri["results"] else "none"),
            )
            ces = search_facilities_text("cesarean delivery", page=1, page_size=5)
            log.warning(
                "SELFTEST cesarean-delivery equivalence ok: interpreted=%s; "
                "total=%s; matches_c_section_button=%s",
                ces["query"]["interpreted"],
                ces["total_count"],
                ces["total_count"] == csec["total_count"],
            )
            ev = search_facilities_text(
                "dialysis", city="Jaipur", page=1, page_size=5, sort="evidence"
            )
            r0 = ev["results"][0] if ev["results"] else None
            log.warning(
                "SELFTEST evidence ordering ok: sort=%s; first=%s; "
                "category_label=%s; unresolved=%s; explanation=%s",
                ev["ordering"]["sort"],
                (r0["facility_name"] if r0 else "none"),
                (r0["ordering"]["category_label"] if r0 else "none"),
                (r0["ordering"]["unresolved"] if r0 else None),
                ((ev["ordering"]["first_result_explanation"] or "none")[:180]),
            )
            dist = search_facilities_text(
                "dialysis", page=1, page_size=10, sort="distance",
                origin={"lat": 26.9124, "lng": 75.7873, "accuracy_m": 1500.0},
                radius_km=50.0,
            )
            dists = [r["ordering"]["distance_km"] for r in dist["results"]]
            log.warning(
                "SELFTEST distance ok: total=%s; excluded=%s; ascending=%s; "
                "first_dists=%s; first=%s",
                dist["total_count"],
                dist["ordering"].get("distance_scope", {}).get("excluded"),
                dists == sorted(dists),
                dists[:5],
                (dist["results"][0]["facility_name"] if dist["results"] else "none"),
            )
            if dist["results"]:
                cont = dist["results"][0].get("contacts") or {}
                log.warning(
                    "SELFTEST contacts ok: phones=%s; websites=%s; note=%s",
                    cont.get("phones"), cont.get("websites"), cont.get("note"),
                )
        except Exception as e:  # diagnostic only — never crash the app
            log.warning(
                "SELFTEST managed-identity DBSQL FAILED: %s: %s",
                type(e).__name__,
                str(e)[:400],
            )
        finally:
            done.set()

    def _watchdog() -> None:
        if not done.wait(240):
            log.warning(
                "SELFTEST TIMEOUT: no completion within 240s — hung before the "
                "warehouse was reached (token or connection stage)"
            )

    threading.Thread(target=_run, daemon=True, name="startup-selftest").start()
    threading.Thread(target=_watchdog, daemon=True, name="selftest-watchdog").start()


_selftest()

