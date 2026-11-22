/* Virtue Foundation — “Find care options” frontend logic (no build step).
   Same-origin /api calls only; no tokens in browser code. Device location,
   when explicitly shared, is sent only in the POST request body — never in
   a URL — and never logged or persisted. */

// ---------------------------------------------------------------- Constants
const SERVICES = [
  { code: "dialysis", label: "Dialysis", q: "dialysis" },
  { code: "MRI", label: "MRI", q: "mri" },
  { code: "NICU", label: "NICU", q: "nicu" },
  { code: "cardiac_surgery", label: "Cardiac surgery", q: "cardiac surgery" },
  { code: "cesarean_delivery", label: "C-section", q: "c section" },
  { code: "burn_care", label: "Burn care", q: "burn care" },
];

// Friendly names for dataset source fields (raw/snake_case never shown).
const SOURCE_LABELS = {
  capability: "services list",
  procedure: "procedures list",
  description: "facility description",
  specialties: "specialties list",
  equipment: "equipment list",
};

const HOW_STEPS = [
  "Started with 10,088 source rows.",
  "Preserved raw information and cleaned formats/missing values.",
  "Flagged 11 redundant exact copies, leaving 10,077 representative records — not necessarily distinct hospitals.",
  "Built a normalized search index over every searchable text element — 579,335 elements across 9,997 records. Typed phrases and the quick-search buttons both use this single token-boundary matching engine.",
  "Added AI interpretation for dialysis records that passed format and quotation checks; a small number remain text-only pending review.",
  "Applied your care phrase and location filters (city/PIN text, or an explicitly shared device location with a chosen radius).",
  "Ordered the results by the chosen sort — “Evidence relevance” is a documented provisional shortlist policy computed before pagination, not a medical ranking.",
];

const SCOPE_LABELS = {
  unspecified: "Patient scope: unspecified",
  pediatric: "Patient scope: pediatric",
  mixed_explicit: "Patient scope: mixed (explicit)",
};

// ---------------------------------------------------------------- State
const state = {
  q: "",
  city: "",
  pin: "",
  page: 1,
  pageSize: 20,
  sort: "evidence",   // evidence | name | distance
  origin: null,       // {lat, lng, accuracy_m} — POST body only, never a URL
  radius: 25,        // km around the origin
  view: "list",      // list | map
  data: null,
  lastError: null,
  authRequired: false,
  compare: [],        // record_ids, max 3
  compareRecords: {}, // record_id -> record snapshot; tray names + removal survive page changes
};

// ---------------------------------------------------------------- DOM helpers
const $ = (id) => document.getElementById(id);

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") node.className = v;
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
    else if (v !== null && v !== undefined && v !== false) node.setAttribute(k, v === true ? "" : String(v));
  }
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    node.append(c.nodeType ? c : document.createTextNode(String(c)));
  }
  return node;
}

function show(id, on) { $(id).hidden = !on; }

function hideAllStates() {
  ["state-loading", "state-auth", "state-error", "state-empty"].forEach((id) => show(id, false));
}

// ---------------------------------------------------------------- API (same-origin)
async function apiSearch() {
  // A device origin travels ONLY in the POST body — never in a URL — so it
  // cannot appear in URL/query strings or their logs.
  let res;
  if (state.origin) {
    res = await fetch("/api/facilities/search", {
      method: "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({
        q: state.q || undefined,
        city: state.city || undefined,
        pin: state.pin || undefined,
        page: state.page,
        page_size: state.pageSize,
        sort: state.sort,
        origin: state.origin,
        radius_km: state.radius,
      }),
    });
  } else {
    const p = new URLSearchParams();
    if (state.q) p.set("q", state.q);
    if (state.city) p.set("city", state.city);
    if (state.pin) p.set("pin", state.pin);
    p.set("page", String(state.page));
    p.set("page_size", String(state.pageSize));
    p.set("sort", state.sort);
    res = await fetch(`/api/facilities/search?${p.toString()}`, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
  }
  if (res.status === 401) return { authRequired: true };
  let body = null;
  try { body = await res.json(); } catch (e) { body = null; }
  if (!res.ok) return { error: { status: res.status, detail: (body && body.detail) ? String(body.detail) : "" } };
  return { data: body };
}

// ---------------------------------------------------------------- Device location
// Explicit consent via the "Use my location" button only. The position is
// kept in memory, sent only with the search POST body, never logged, never
// persisted, and never combined with city/PIN (the backend rejects that
// combination with 400).
function showLocationStatus(msg, isError) {
  const el = $("loc-status");
  el.textContent = msg;
  el.classList.toggle("loc-status-error", !!isError);
  el.hidden = !msg;
}

function enableDistanceSort(on) {
  for (const o of $("sort-input").options) {
    if (o.value === "distance") o.disabled = !on;
  }
}

function requestLocation() {
  if (!("geolocation" in navigator)) {
    showLocationStatus("Location is not available in this browser — city and PIN search still work.", true);
    return;
  }
  showLocationStatus("Waiting for location permission…");
  $("loc-btn").setAttribute("disabled", "");
  navigator.geolocation.getCurrentPosition(
    (pos) => {
      $("loc-btn").removeAttribute("disabled");
      const acc = pos.coords.accuracy != null ? pos.coords.accuracy : null;
      state.origin = { lat: pos.coords.latitude, lng: pos.coords.longitude, accuracy_m: acc };
      // Device location and city/PIN are mutually exclusive filters.
      state.city = "";
      state.pin = "";
      $("city-input").value = "";
      $("pin-input").value = "";
      $("city-input").setAttribute("disabled", "");
      $("pin-input").setAttribute("disabled", "");
      $("loc-clear").hidden = false;
      enableDistanceSort(true);
      let msg = "Device location set. It is sent only with your next search — never saved or logged. City/PIN filters are off while it is active.";
      if (acc != null && acc > 5000) {
        msg += ` Reported accuracy is low (±${Math.round(acc / 1000)} km) — distances may be misleading.`;
      }
      showLocationStatus(msg);
    },
    (err) => {
      $("loc-btn").removeAttribute("disabled");
      const msgs = {
        1: "Location was denied — you can still search by city or PIN.",
        2: "Location is unavailable right now — you can still search by city or PIN.",
        3: "Location request timed out — try again, or use city or PIN.",
      };
      showLocationStatus(msgs[err.code] || "Location could not be obtained — city and PIN search still work.", true);
    },
    { enableHighAccuracy: false, timeout: 15000, maximumAge: 300000 }
  );
}

function clearLocation() {
  state.origin = null;
  $("city-input").removeAttribute("disabled");
  $("pin-input").removeAttribute("disabled");
  $("loc-clear").hidden = true;
  showLocationStatus("");
  enableDistanceSort(false);
  if (state.sort === "distance") {
    state.sort = "evidence";
    $("sort-input").value = "evidence";
  }
}
// ---------------------------------------------------------------- Interactive map
// Leaflet 1.9.4 (BSD-2-Clause open source) is self-hosted at /leaflet.js
// and /leaflet.css — no CDN, no build step, no API key. The basemap is
// OpenStreetMap raster tiles (open data, ODbL licence). Markers use
// RECORDED facility coordinates — never verified geolocations — and the
// map is never used for directions. If basemap tiles cannot load (a
// network or security policy), an explicit notice appears — never a
// silent schematic fallback.
let mapInstance = null;         // L.Map, created once on first use
let mapOriginLayer = null;      // L.Circle for the search radius
let mapNoticeShown = false;     // tile-error notice, once per session
const mapMarkers = new Map();   // record_id -> L.circleMarker

function hasUsableCoords(r) {
  const lat = r.location && r.location.latitude;
  const lng = r.location && r.location.longitude;
  return Number.isFinite(lat) && Number.isFinite(lng) && !(lat === 0 && lng === 0);
}

function renderMap() {
  const canvas = $("map-canvas");
  if (typeof L === "undefined") {
    canvas.innerHTML = "";
    canvas.append(h("p", { class: "muted", role: "note" }, "The map library could not load in this environment. The list view, evidence, and distance search still work."));
    return;
  }
  if (!mapInstance) {
    mapInstance = L.map(canvas, { attributionControl: true });
    mapInstance.setView([22.9, 79.9], 4);
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "Basemap © OpenStreetMap contributors (ODbL) — tiles: tile.openstreetmap.org",
    }).on("tileerror", showMapTileNotice).addTo(mapInstance);
  }
  mapInstance.invalidateSize();
  updateMapMarkers();
}

function updateMapMarkers() {
  if (!mapInstance) return;
  for (const m of mapMarkers.values()) mapInstance.removeLayer(m);
  mapMarkers.clear();
  if (mapOriginLayer) { mapInstance.removeLayer(mapOriginLayer); mapOriginLayer = null; }
  const rows = (state.data && state.data.results) || [];
  const pts = rows.filter(hasUsableCoords);
  const bounds = [];
  pts.forEach((r, i) => {
    const sel = state.compare.includes(r.record_id);
    const marker = L.circleMarker([r.location.latitude, r.location.longitude], {
      radius: sel ? 12 : 10,
      color: sel ? "#1d4ed8" : "#0f766e",
      weight: 2,
      fillColor: sel ? "#1d4ed8" : "#0d9488",
      fillOpacity: 0.92,
      className: `facility-marker${sel ? " marker-selected" : ""}`,
    });
    marker.bindPopup(mapPopupHtml(r, i + 1));
    marker.on("click", () => highlightFromMap(r.record_id));
    marker.addTo(mapInstance);
    mapMarkers.set(r.record_id, marker);
    bounds.push([r.location.latitude, r.location.longitude]);
  });
  if (state.origin) {
    mapOriginLayer = L.circle([state.origin.lat, state.origin.lng], {
      radius: (state.radius || 25) * 1000,
      color: "#2563eb", weight: 1.5, fillOpacity: 0.06,
      dashArray: "6 6", interactive: false,
    }).addTo(mapInstance);
    bounds.push([state.origin.lat, state.origin.lng]);
  }
  if (bounds.length > 1) mapInstance.fitBounds(bounds, { padding: [28, 28], maxZoom: 14 });
  else if (bounds.length === 1) mapInstance.setView(bounds[0], 12);
  if (!pts.length) setMapNote("No usable recorded locations on this page — markers are unavailable for these records. Recorded locations are never independently verified.");
  else if (!mapNoticeShown) setMapNote(null);
}

function setMapNote(text) {
  const note = $("map-note");
  if (!note) return;
  if (text) { note.hidden = false; note.textContent = text; }
  else note.hidden = true;
}

function showMapTileNotice() {
  if (mapNoticeShown) return;
  mapNoticeShown = true;
  setMapNote("Basemap tiles could not load — likely blocked by a network or security policy in this environment. Markers still use the records’ real recorded coordinates; use the list view and the distance search meanwhile. (OpenStreetMap tiles, no API key.)");
}

function escapeHtml(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function mapPopupHtml(r, n) {
  const loc = [r.location.city, r.location.state].filter(Boolean).join(", ");
  const dist = r.ordering && r.ordering.distance_km != null
    ? `<br>≈ ${escapeHtml(r.ordering.distance_km)} km away — straight-line, recorded location` : "";
  return `<strong>${escapeHtml(r.facility_name)}</strong><br>${escapeHtml(loc || "No recorded location text")}${dist}<br>Evidence: ${escapeHtml(evidenceCategory(r).label)} — recorded, not verified.<br><span class="map-popup-hint">Pin ${n} on this page — not a ranking.</span>`;
}

function highlightFromMap(id) {
  const card = document.querySelector(`#results-list [data-record-id="${CSS.escape(id)}"]`);
  if (card) {
    card.classList.add("card-highlight");
    card.scrollIntoView({ behavior: "smooth", block: "center" });
    setTimeout(() => card.classList.remove("card-highlight"), 2600);
  }
}

function showMapForRecord(r) {
  if (!hasUsableCoords(r)) return;
  setView("map");
  const marker = mapMarkers.get(r.record_id);
  if (marker && mapInstance) {
    mapInstance.panTo(marker.getLatLng());
    marker.openPopup();
  }
}

function setView(v) {
  state.view = v;
  const isMap = v === "map";
  $("view-list").classList.toggle("active", !isMap);
  $("view-list").setAttribute("aria-pressed", String(!isMap));
  $("view-map").classList.toggle("active", isMap);
  $("view-map").setAttribute("aria-pressed", String(isMap));
  $("map-panel").hidden = !isMap;
  if (isMap) renderMap();
}

// ---------------------------------------------------------------- Search flow
async function runSearch(resetPage = false) {
  if (resetPage) state.page = 1;
  hideAllStates();
  $("results-list").innerHTML = "";
  show("pagination", false);
  show("results-count", false);
  show("first-explanation", false);
  show("results-reminder", false);
  show("view-toggle", false);
  $("map-panel").hidden = true;
  show("state-loading", true);
  try {
    const out = await apiSearch();
    if (out.authRequired) {
      state.authRequired = true;
      show("state-loading", false);
      show("state-auth", true);
      return;
    }
    if (out.error) {
      state.lastError = out.error;
      show("state-loading", false);
      renderErrorState(out.error);
      return;
    }
    state.data = out.data;
    show("state-loading", false);
    renderResults();
  } catch (e) {
    show("state-loading", false);
    renderErrorState({ status: 0, detail: "Network error — the facility service could not be reached." });
  }
}

function renderErrorState(err) {
  const detail = $("error-detail");
  detail.textContent =
    err.status === 400
      ? `The service rejected the request: ${err.detail}`
      : err.status
        ? `The facility service returned HTTP ${err.status}. Your filters are saved — try again shortly.`
        : err.detail || "The facility service is unavailable.";
  show("state-error", true);
}

// ---------------------------------------------------------------- Render: results
function renderSearchingFor() {
  const el = $("searching-for");
  const q = state.data && state.data.query;
  if (q) {
    const aliases = (q.aliases_used || []).length ? ` — also matched: ${q.aliases_used.join(", ")}` : "";
    const eq = q.reviewed_equivalence ? ` (${q.reviewed_equivalence})` : "";
    el.textContent = `Searching for “${q.interpreted}”${aliases}${eq}`;
    el.hidden = false;
  } else {
    el.hidden = true;
  }
}

function renderOrderingPanel() {
  const rulesEl = $("ordering-rules");
  rulesEl.innerHTML = "";
  const o = state.data && state.data.ordering;
  rulesEl.append(h("p", { class: "muted" },
    "This order is a provisional shortlist policy — never a clinically validated ranking or a “best hospital” claim."));
  if (o && o.rules) {
    rulesEl.append(h("ul", { class: "subtle-list" }, ...o.rules.map((s) => h("li", {}, s))));
  } else {
    rulesEl.append(h("p", { class: "muted" }, "Run a search to see the exact ordering used for those results."));
  }
}

function renderResults() {
  const { total_count: total, results, ordering } = state.data;
  renderSearchingFor();
  renderOrderingPanel();
  const countEl = $("results-count");
  const explainEl = $("first-explanation");
  if (total === 0) {
    countEl.hidden = true;
    explainEl.hidden = true;
    show("results-reminder", false);
    show("view-toggle", false);
    show("state-empty", true);
    show("pagination", false);
    return;
  }
  show("state-empty", false);
  show("results-reminder", true);
  countEl.hidden = false;
  countEl.textContent = `${total.toLocaleString()} matching source record${total === 1 ? "" : "s"}${state.origin ? ` within ${state.radius} km of your device location` : ""} — records, not necessarily distinct facilities`;
  // “Why is this first” — page 1 only, and never as a ranking claim.
  if (ordering && ordering.first_result_explanation && state.data.page === 1) {
    explainEl.hidden = false;
    explainEl.textContent = ordering.first_result_explanation;
  } else {
    explainEl.hidden = true;
  }
  show("view-toggle", true);
  setView(state.view); // re-renders the map if it is the active view
  const list = $("results-list");
  list.innerHTML = "";
  for (const r of results) list.append(renderCard(r));
  if (results.length === 0) {
    list.append(h("li", { class: "state-block" }, h("p", { class: "muted" }, "No records on this page — use the previous page.")));
  }
  renderPagination();
}

// Plain-language evidence category derived from the backend's ordering label
// and detail. Never turned into a verified-care claim.
function evidenceCategory(r) {
  const o = r.ordering || {};
  const label = o.category_label || "Service listed in source text";
  const cls = label === "Conflicting or unclear information" || label === "Interpretation awaiting review"
    ? "badge-ambiguous"
    : label === "Procedure listed" ? "badge-procedure"
      : label === "Specialty mention only" ? "badge-specialty"
        : label === "Equipment mention only" ? "badge-equipment"
          : "badge-explicit";
  return { label, detail: o.category_detail || "", cls };
}

function evidenceExplanation(r) {
  const o = r.ordering || {};
  const first = r.sql_evidence.items && r.sql_evidence.items[0];
  const src = first && first.source_field ? SOURCE_LABELS[first.source_field] || first.source_field : "source text";
  const terms = (r.sql_evidence.matched_terms || []).slice(0, 3).join(", ");
  let why = `Matched ${terms ? `“${terms}”` : "the care phrase"} in the facility’s ${src}.`;
  if (o.category_detail) why += ` Category — ${o.category_label}: ${o.category_detail}.`;
  if (r.jaipur_pilot_supplement && r.jaipur_pilot_supplement.retrieved_evidence_quote) {
    why += " A website statement is also available (see details).";
  }
  if (r.sql_evidence.items && r.sql_evidence.items.some((it) => it.mention_kind === "specialist_mention")) {
    why += " Includes a specialist mention — a named specialist is not proof that the procedure is performed onsite.";
  }
  return why;
}

function scopeChips(r) {
  const chips = [];
  const ai = r.ai_interpretation;
  if (ai) {
    const scope = SCOPE_LABELS[ai.patient_scope] || `Patient scope: ${ai.patient_scope}`;
    const c = h("span", { class: "chip-scope" }, scope);
    if (ai.patient_scope === "unspecified") c.append(h("span", { class: "hint" }, " — does not mean adult eligibility"));
    if (ai.patient_scope === "pediatric") c.append(h("span", { class: "hint" }, " — pediatric record"));
    chips.push(c);
    for (const t of ai.treatment_types || []) {
      const isCrrt = String(t).toLowerCase() === "crrt";
      chips.push(h("span", { class: `chip-modality${isCrrt ? " chip-modality-crrt" : ""}` }, `${t}${isCrrt ? " — distinct from routine dialysis" : ""}`));
    }
  }
  return chips;
}

function renderCard(r) {
  const loc = r.location;
  const locText = [loc.city, loc.state, loc.pincode ? `PIN ${loc.pincode}` : null].filter(Boolean).join(" · ");
  const card = h("li", { class: "card", "data-record-id": r.record_id });
  const ev = evidenceCategory(r);
  const badges = [h("span", { class: `badge ${ev.cls}`, title: ev.detail || undefined }, ev.label)];
  if (r.jaipur_pilot_supplement && r.jaipur_pilot_supplement.retrieved_evidence_quote) {
    badges.push(h("span", { class: "badge badge-neutral", title: "A retrieved website statement exists — historical, not a verification of current care." }, "Website statement available"));
  }
  if (r.ordering && r.ordering.distance_km != null) {
    badges.push(h("span", { class: "badge badge-neutral" }, `≈ ${r.ordering.distance_km} km — straight-line, recorded location`));
  }
  card.append(
    h("div", { class: "card-head" },
      h("div", {},
        h("h3", { class: "facility-name" }, r.facility_name),
        h("p", { class: "facility-location" }, locText || "No recorded location text"),
      ),
      h("p", { class: "badge-row" }, ...badges),
    )
  );
  // Prominent warnings first — location, branch, negation, historical,
  // pediatric, and modality notes come structured from the backend.
  const o = r.ordering || {};
  if ((o.unresolved_reasons || []).length) {
    card.append(h("ul", { class: "warning warning-list", role: "note" },
      ...o.unresolved_reasons.map((t) => h("li", {}, `⚠ ${t}`))));
  }
  const chips = scopeChips(r);
  if (chips.length) card.append(h("p", { class: "badge-row" }, ...chips));
  // One exact supporting excerpt + a short plain-language explanation.
  const first = r.sql_evidence.items && r.sql_evidence.items[0];
  if (first) {
    const src = first.source_field ? SOURCE_LABELS[first.source_field] || first.source_field : "source text";
    card.append(h("div", { class: "excerpt" },
      h("p", { class: "excerpt-text" }, `“${first.evidence_text || "(no text returned)"}”`),
      h("p", { class: "excerpt-src muted" }, `From the facility’s ${src}`),
    ));
  }
  card.append(h("p", { class: "why" }, evidenceExplanation(r)));
  card.append(
    h("div", { class: "card-actions" },
      h("button", { class: "btn btn-primary btn-small", type: "button", onclick: () => openDrawer(r) }, "Details and evidence"),
      hasUsableCoords(r) ? h("button", { class: "btn btn-ghost btn-small", type: "button", onclick: () => showMapForRecord(r) }, "Show on map") : null,
      compareButton(r),
    )
  );
  return card;
}

function compareButton(r) {
  const selected = state.compare.includes(r.record_id);
  const full = state.compare.length >= 3;
  const btn = h("button", {
    class: `btn ${selected ? "btn-primary" : "btn-ghost"} btn-small`, type: "button",
    "aria-pressed": selected ? "true" : "false",
    onclick: () => toggleCompare(r),
  }, selected ? "In comparison" : "Compare");
  if (!selected && full) btn.setAttribute("disabled", "");
  return btn;
}

// ---------------------------------------------------------------- Pagination
function renderPagination() {
  const { total_count: total, page, page_size: pageSize } = state.data;
  const nav = $("pagination");
  nav.innerHTML = "";
  if (total <= pageSize) { show("pagination", false); return; }
  const pages = Math.ceil(total / pageSize);
  const start = (page - 1) * pageSize + 1;
  const end = Math.min(total, page * pageSize);
  const prev = h("button", { class: "page-btn", type: "button", onclick: () => gotoPage(page - 1) }, "‹ Prev");
  if (page <= 1) prev.setAttribute("disabled", "");
  nav.append(prev);
  const win = 2;
  for (let p = 1; p <= pages; p++) {
    if (p === 1 || p === pages || Math.abs(p - page) <= win) {
      const b = h("button", { class: "page-btn", type: "button", onclick: () => gotoPage(p) }, String(p));
      if (p === page) b.setAttribute("aria-current", "page");
      nav.append(b);
    } else if (Math.abs(p - page) === win + 1) {
      nav.append(h("span", { class: "page-note" }, "…"));
    }
  }
  const next = h("button", { class: "page-btn", type: "button", onclick: () => gotoPage(page + 1) }, "Next ›");
  if (page >= pages) next.setAttribute("disabled", "");
  nav.append(next);
  nav.append(h("span", { class: "page-note" }, `Showing ${start}–${end} of ${total.toLocaleString()} matching source records`));
  show("pagination", true);
}

function gotoPage(p) {
  const pages = Math.max(1, Math.ceil(state.data.total_count / state.data.page_size));
  state.page = Math.min(Math.max(1, p), pages);
  runSearch();
  $("results").scrollIntoView({ behavior: "smooth" });
}

// ---------------------------------------------------------------- Evidence drawer
function quoteBlock(cls, src, text) {
  return h("div", { class: `quote-block ${cls}` }, h("span", { class: "quote-src" }, src), h("span", { class: "quote-text" }, text));
}

function openDrawer(r) {
  const body = $("drawer-body");
  body.innerHTML = "";
  $("drawer-title").textContent = r.facility_name;
  const o = r.ordering || {};
  const ai = r.ai_interpretation;
  const p = r.jaipur_pilot_supplement;
  const friendlyField = (f) => (f ? SOURCE_LABELS[f] || f : "source text");

  // 1. Why this matched
  const why = h("section", { class: "drawer-section" }, h("h4", {}, "Why this matched"));
  why.append(
    h("p", {}, h("strong", {}, o.category_label || "Service listed in source text"),
      o.category_detail ? ` — ${o.category_detail}` : ""),
    h("p", { class: "muted" }, evidenceExplanation(r)),
  );
  const first = r.sql_evidence.items && r.sql_evidence.items[0];
  if (first) {
    const terms = first.matched_terms && first.matched_terms.length ? ` (matched: ${first.matched_terms.join(", ")})` : "";
    why.append(quoteBlock("", `From the facility’s ${friendlyField(first.source_field)}${terms}`, first.evidence_text || "(no text returned)"));
  }
  if (o.distance_km != null) {
    why.append(h("p", { class: "muted" }, `About ${o.distance_km} km straight-line from your search point — recorded location, not verified geolocation; not travel time.`));
  }
  body.append(why);

  // 2. What the sources say
  const srcs = h("section", { class: "drawer-section" }, h("h4", {}, "What the sources say"));
  const dsItems = r.sql_evidence.items || [];
  if (dsItems.length) {
    srcs.append(h("p", { class: "muted" }, "Exact quotations from the dataset, with the part of the record they came from:"));
    for (const it of dsItems) {
      srcs.append(quoteBlock("", `Facility’s ${friendlyField(it.source_field)}${it.mention_kind === "specialist_mention" ? " — specialist mention (not proof of onsite surgery)" : ""}`, it.evidence_text || "(no text)"));
    }
    if (r.sql_evidence.count > dsItems.length) {
      srcs.append(h("p", { class: "muted" }, `+ ${r.sql_evidence.count - dsItems.length} more matching text element${r.sql_evidence.count - dsItems.length === 1 ? "" : "s"} not shown — mention count is never used to rank records.`));
    }
  } else {
    srcs.append(h("p", { class: "muted" }, "No dataset quotations were returned for this record."));
  }
  if (ai && (ai.evidence_quotations || []).length) {
    srcs.append(h("p", { class: "muted" }, "AI-extracted verbatim quotations (validated extraction — not verified care):"));
    for (const q of ai.evidence_quotations) {
      srcs.append(quoteBlock("", `AI quotation — ${friendlyField(q.source_field)}`, q.exact_quote || "(no text)"));
    }
  }
  if (r.ai_enrichment_status === "extraction_needs_review") {
    srcs.append(h("div", { class: "warning", role: "note" }, "⚠ Interpretation awaiting review — the dataset quotations above are the evidence; the AI interpretation for this record is withheld pending review."));
  }
  if (p && p.retrieved_evidence_quote) {
    srcs.append(h("p", { class: "muted" }, "Website statement (retrieved pilot evidence — historical, not a verification of current care):"));
    srcs.append(quoteBlock("quote-web", `${p.retrieved_evidence_url || "(no URL)"} — retrieved ${p.retrieved_evidence_retrieved_at || "unknown date"} (retrieval date, not a service-verification date)`, p.retrieved_evidence_quote));
    if (p.retrieved_temporal_context) srcs.append(h("p", { class: "muted" }, `Statement context: ${p.retrieved_temporal_context.replace(/_/g, " ")} — not current-availability evidence.`));
  }
  if (p && (p.unattributed_source_url_candidates || []).length) {
    srcs.append(h("p", { class: "muted" }, "Unattributed candidate links (not quoted, not verified): ", p.unattributed_source_url_candidates.join(" · ")));
  }
  body.append(srcs);

  // 3. What still needs checking
  const check = h("section", { class: "drawer-section" }, h("h4", {}, "What still needs checking"));
  const items = (o.unresolved_reasons || []).slice();
  if (ai) {
    if (ai.patient_scope === "pediatric") items.push("Record describes pediatric care — adult eligibility is not established.");
    if (ai.patient_scope === "unspecified") items.push("Patient scope is unspecified — that does not mean adults are eligible.");
    if (ai.is_crrt) items.push("CRRT is listed — CRRT is not interchangeable with routine dialysis.");
    if (ai.possible_mixed_facility_record) items.push("Possible mixed-facility/branch record — evidence may refer to a different facility or branch.");
  }
  if (p && p.retrieved_unresolved_issues) items.push(`Known conflict: ${p.retrieved_unresolved_issues}`);
  items.push("Recorded location is not independently verified — no directions are offered.");
  check.append(h("ul", { class: "subtle-list" }, ...items.map((t) => h("li", {}, t))));
  check.append(h("p", {}, h("strong", {}, "Current availability: contact facility to confirm.")));
  const c = r.contacts;
  if (c && ((c.phones || []).length || (c.websites || []).length)) {
    const cl = h("ul", { class: "subtle-list" });
    for (const ph of c.phones || []) cl.append(h("li", {}, `Phone (unverified): ${ph}`));
    for (const u of c.websites || []) cl.append(h("li", {}, `Website (unverified, attribution not confirmed): ${u}`));
    check.append(h("p", { class: "muted" }, "Contacts — copied from the source data and syntax-checked only; they may belong to a different branch:"), cl);
    if (c.branch_warning) check.append(h("div", { class: "warning", role: "note" }, `⚠ ${c.branch_warning}`));
  } else {
    check.append(h("p", { class: "muted" }, "No valid phone numbers or websites in the source data for this record — the tool does not invent contacts."));
  }
  body.append(check);

  // 4. How we ordered this result
  const ord = h("section", { class: "drawer-section" }, h("h4", {}, "How we ordered this result"));
  const ds = state.data && state.data.ordering;
  if (ds && ds.rules) ord.append(h("ul", { class: "subtle-list" }, ...ds.rules.map((s) => h("li", {}, s))));
  ord.append(h("p", { class: "muted" }, "This order is a provisional shortlist policy — not a clinically validated ranking or a “best hospital” claim. Mention counts, repeated pages, and confidence scores are never used."));
  body.append(ord);

  // 5. Recorded location (small print)
  const locSec = h("section", { class: "drawer-section" }, h("h4", {}, "Recorded location"));
  const loc = r.location;
  locSec.append(h("ul", { class: "subtle-list" },
    h("li", {}, `Recorded location text: ${[loc.city, loc.state, loc.pincode ? `PIN ${loc.pincode}` : null].filter(Boolean).join(" · ") || "none recorded"}`),
    h("li", {}, `Coordinate status: ${loc.coordinate_status || "unknown"} — recorded location, not independently verified; no directions are offered.`),
  ));
  if (p) {
    if (p.retrieved_branch_address_quote) {
      locSec.append(quoteBlock("quote-web", `Retrieved address quote — ${p.retrieved_branch_address_url || "(no URL)"}`, p.retrieved_branch_address_quote));
      if (p.retrieved_branch_assessment) locSec.append(h("p", { class: "muted" }, `Branch assessment: ${p.retrieved_branch_assessment}`));
    }
    if (p.needs_attribution_review) locSec.append(h("div", { class: "warning", role: "note" }, "⚠ This record is flagged for attribution review — identity is not settled."));
  }
  body.append(locSec);

  openDialog("drawer");
}

// ---------------------------------------------------------------- Comparison
function toggleCompare(r) {
  const i = state.compare.indexOf(r.record_id);
  if (i >= 0) {
    state.compare.splice(i, 1);
    delete state.compareRecords[r.record_id];
  } else {
    if (state.compare.length >= 3) return;
    state.compare.push(r.record_id);
    state.compareRecords[r.record_id] = r;
  }
  refreshCompareUI();
  if (mapInstance) updateMapMarkers();
  // Re-render cards to update buttons
  if (state.data) {
    const list = $("results-list");
    list.innerHTML = "";
    for (const row of state.data.results) list.append(renderCard(row));
  }
}

function findRecord(id) {
  // Selection happens from visible cards, but the user may paginate away
  // before opening the comparison — snapshots keep names and removal
  // working for records no longer on the current page.
  const onPage = (state.data && state.data.results.find((r) => r.record_id === id)) || null;
  return onPage || state.compareRecords[id] || null;
}

function refreshCompareUI() {
  const tray = $("compare-tray");
  if (!state.compare.length) { show("compare-tray", false); return; }
  show("compare-tray", true);
  const items = $("compare-tray-items");
  items.innerHTML = "";
  for (const id of state.compare) {
    const r = findRecord(id);
    items.append(h("span", { class: "tray-item" }, (r ? r.facility_name : id).slice(0, 40), h("button", { type: "button", "aria-label": "Remove from comparison", onclick: () => { const rec = findRecord(id); if (rec) toggleCompare(rec); } }, "✕")));
  }
  const openBtn = $("compare-open");
  if (state.compare.length >= 2) openBtn.removeAttribute("disabled");
  else openBtn.setAttribute("disabled", "");
}

function openCompare() {
  if (state.compare.length < 2) return;
  const records = state.compare.map((id) => findRecord(id)).filter(Boolean);
  const body = $("compare-body");
  body.innerHTML = "";
  body.append(h("p", { class: "muted" }, "Up to three records side by side. Current availability is unknown for every record; ordering is a documented shortlist policy, not a ranking."));
  const table = h("table", { class: "compare-table" });
  const head = h("thead", {}, h("tr", {}, h("th", { scope: "col" }, "Aspect"), ...records.map((r) => h("th", { scope: "col" }, r.facility_name))));
  const rowsSpec = [
    ["Evidence category", (r) => {
      const o = r.ordering || {};
      return `${o.category_label || "Service listed in source text"}${o.category_detail ? ` — ${o.category_detail}` : ""}`;
    }],
    ["Recorded location", (r) => [r.location.city, r.location.state, r.location.pincode ? `PIN ${r.location.pincode}` : null].filter(Boolean).join(" · ") || "none"],
    ["Distance from search point", (r) => r.ordering && r.ordering.distance_km != null ? `≈ ${r.ordering.distance_km} km straight-line (recorded location)` : "Not applicable (no device location)"],
    ["Exact supporting quote", (r) => { const it = (r.sql_evidence.items || [])[0]; return it && it.evidence_text ? `“${it.evidence_text}” (from the facility’s ${it.source_field ? SOURCE_LABELS[it.source_field] || it.source_field : "source text"})` : "None returned"; }],
    ["Patient scope", (r) => r.ai_interpretation ? `${r.ai_interpretation.patient_scope}${r.ai_interpretation.patient_scope === "unspecified" ? " (not adult eligibility)" : ""}` : "No AI interpretation"],
    ["Modality", (r) => {
      const t = (r.ai_interpretation && r.ai_interpretation.treatment_types) || [];
      return t.length ? t.join(", ") + (r.ai_interpretation && r.ai_interpretation.is_crrt ? " (CRRT is distinct from routine dialysis)" : "") : "Not extracted";
    }],
    ["Website statement", (r) => {
      const p = r.jaipur_pilot_supplement;
      if (!p || !p.retrieved_evidence_quote) return "None";
      return `${p.retrieved_evidence_url || "(URL missing)"} — retrieved ${p.retrieved_evidence_retrieved_at || "unknown date"}; ${p.retrieved_temporal_context || "context unknown"} (retrieval is not verification)`;
    }],
    ["Needs checking", (r) => {
      const items = ((r.ordering && r.ordering.unresolved_reasons) || []).slice();
      if (r.ai_interpretation && r.ai_interpretation.patient_scope === "pediatric") items.push("Pediatric record — adult eligibility not established");
      items.push("Current availability: contact facility to confirm");
      return items;
    }],
    ["Contacts (unverified)", (r) => {
      const c = r.contacts;
      if (!c) return "None in source data";
      const parts = [];
      if ((c.phones || []).length) parts.push(c.phones.join(", "));
      if ((c.websites || []).length) parts.push(c.websites.join(", "));
      return parts.length ? parts.join(" · ") : "None valid in source data";
    }],
    ["Current availability", () => "Contact facility to confirm — unknown for every record"],
  ];
  const tbody = h("tbody");
  for (const [label, fn] of rowsSpec) {
    const tr = h("tr", {}, h("th", { scope: "row" }, label));
    for (const r of records) {
      const v = fn(r);
      tr.append(h("td", {}, Array.isArray(v) ? h("ul", { class: "subtle-list" }, ...v.map((t) => h("li", {}, t))) : String(v)));
    }
    tbody.append(tr);
  }
  table.append(head, tbody);
  body.append(h("div", { class: "compare-table-wrap" }, table));
  body.append(h("h4", { style: "margin-top:18px" }, "What to confirm next"));
  const checks = [
    ["Correct branch", "Multiple records may describe one facility or its branches.", (r) => !!(r.ai_interpretation && r.ai_interpretation.possible_mixed_facility_record) || !!(r.jaipur_pilot_supplement && r.jaipur_pilot_supplement.needs_attribution_review)],
    ["Service provision", "A mention — even an explicit claim — is not verified current provision.", (r) => !(r.ai_interpretation && r.ai_interpretation.assertion === "explicit_service_claim")],
    ["Patient eligibility", "Scope may be pediatric, mixed, or unspecified; unspecified is not adult eligibility.", (r) => !(r.ai_interpretation && r.ai_interpretation.patient_scope === "mixed_explicit")],
    ["Appointment / capacity", "No capacity data exists in the dataset.", () => true],
    ["Contact details", "Contacts from the source data are unverified and may belong to another branch — confirm before outreach.", () => true],
  ];
  const list = h("ul", { class: "checklist" });
  for (const [title, why, flagged] of checks) {
    const anyFlagged = records.some((r) => flagged(r));
    list.append(h("li", {}, h("strong", {}, title + (anyFlagged ? " — check first" : "")), h("span", { class: "check-why" }, why)));
  }
  body.append(list);
  openDialog("compare-modal");
}

// ---------------------------------------------------------------- How this search works
function renderHowPanel() {
  const body = $("how-body");
  body.innerHTML = "";
  body.append(h("p", { class: "muted" }, "How a search is built — from source rows to the results you see:"));
  body.append(h("ol", { class: "how-steps" }, ...HOW_STEPS.map((s) => h("li", {}, s))));
  const o = state.data && state.data.ordering;
  body.append(h("h4", {}, "Ordering of the current results"));
  if (o && o.rules) {
    body.append(h("ul", { class: "subtle-list" }, ...o.rules.map((s) => h("li", {}, s))));
  } else {
    body.append(h("p", { class: "muted" }, "Run a search to see its exact ordering."));
  }
  const current = state.data ? state.data.total_count : null;
  body.append(
    h("div", { class: "how-globals" },
      h("p", {}, h("strong", {}, "What is never done: "),
        "No confidence percentages, care-gap scores, or weighted “best hospital” rankings. No availability inference from recent retrieval or 24/7 text. Historical statements never count as current provision."),
      h("p", {}, "“AI validated” means the extraction’s structure and verbatim quotations passed checks — not that treatment delivery was verified."),
      h("p", {}, "Repeated mentions within a record and repeated pages of a website do not count as independent corroboration."),
      h("p", {}, current != null ? `This search currently matches ${current.toLocaleString()} source records. Free-text search overall covers 579,335 text elements across 9,997 records.` : "Free-text search covers 579,335 text elements across 9,997 records; run a search to see its record count."),
    )
  );
}

// ---------------------------------------------------------------- Dialogs
let lastFocus = null;
function openDialog(id) {
  lastFocus = document.activeElement;
  show(id, true);
  const dlg = $(id);
  dlg.querySelector(".dialog-close").focus();
}
function closeDialog(id) {
  show(id, false);
  if (lastFocus && lastFocus.focus) lastFocus.focus();
}
document.addEventListener("click", (e) => {
  const t = e.target.closest("[data-close]");
  if (t) closeDialog(t.getAttribute("data-close"));
});
document.addEventListener("keydown", (e) => {
  if (e.key !== "Escape") return;
  for (const id of ["drawer", "compare-modal", "how-modal"]) if (!$(id).hidden) closeDialog(id);
});

// ---------------------------------------------------------------- Services selector
function renderServices() {
  const wrap = $("service-chips");
  wrap.innerHTML = "";
  for (const s of SERVICES) {
    const chip = h("button", {
      class: `chip${state.q === s.q ? " active" : ""}`, type: "button",
      role: "radio", "aria-checked": state.q === s.q ? "true" : "false",
      onclick: () => { state.q = s.q; const ci = $("care-input"); if (ci) ci.value = s.q; renderServices(); },
    }, s.label);
    wrap.append(chip);
  }
}

// ---------------------------------------------------------------- Init
function init() {
  renderServices();
  renderHowPanel();

  $("search-form").addEventListener("submit", (e) => {
    e.preventDefault();
    state.city = $("city-input").value.trim();
    state.pin = $("pin-input").value.trim();
    state.sort = $("sort-input").value;
    state.radius = Number($("radius-input").value) || 25;
    const care = ($("care-input").value || "").trim();
    state.q = care;
    if (!state.q) {
      $("error-detail").textContent = "Type a care phrase (e.g., “cataract surgery”) or pick a quick-search suggestion first.";
      show("state-error", true);
      return;
    }
    if (state.origin && (state.city || state.pin)) {
      // Defensive: the backend rejects this combination; avoid the round trip.
      $("error-detail").textContent = "Device location and city/PIN filters cannot be combined — clear the location or the city/PIN filters.";
      show("state-error", true);
      return;
    }
    renderServices();
    state.compare = [];
    state.compareRecords = {};
    refreshCompareUI();
    runSearch(true);
  });
  $("reset-btn").addEventListener("click", () => {
    state.city = ""; state.pin = ""; state.page = 1; state.q = ""; state.sort = "evidence"; state.view = "list";
    clearLocation();
    setTimeout(() => {
      $("city-input").value = ""; $("pin-input").value = ""; $("care-input").value = "";
      $("sort-input").value = "evidence"; $("radius-input").value = "25";
      setView("list");
      renderServices();
    }, 0);
  });
  $("care-input").addEventListener("input", () => {
    if (state.q) { state.q = ""; renderServices(); }
  });
  $("loc-btn").addEventListener("click", requestLocation);
  $("loc-clear").addEventListener("click", clearLocation);
  $("sort-input").addEventListener("change", () => {
    state.sort = $("sort-input").value;
    if (state.data) runSearch(true);
  });
  $("radius-input").addEventListener("change", () => {
    state.radius = Number($("radius-input").value) || 25;
    if (state.data && state.origin) runSearch(true);
  });
  $("view-list").addEventListener("click", () => setView("list"));
  $("view-map").addEventListener("click", () => setView("map"));
  $("how-open").addEventListener("click", () => { renderHowPanel(); openDialog("how-modal"); });
  $("auth-retry").addEventListener("click", () => runSearch());
  $("error-retry").addEventListener("click", () => runSearch());
  $("compare-open").addEventListener("click", openCompare);
  $("compare-clear").addEventListener("click", () => { state.compare = []; state.compareRecords = {}; refreshCompareUI(); if (mapInstance) updateMapMarkers(); if (state.data) { const l = $("results-list"); l.innerHTML = ""; for (const r of state.data.results) l.append(renderCard(r)); } });
}

init();
