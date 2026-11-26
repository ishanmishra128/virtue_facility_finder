// Frontend self-tests — simulated browser checks against the served app.
// Uses only the public UI (iframe + DOM), never internals or secrets.
"use strict";

(function () {
  const list = document.getElementById("selftest-results");
  const frame = document.getElementById("app-frame");
  const results = [];
  let pass = 0, fail = 0;

  function record(name, ok, detail) {
    results.push({ name, ok, detail: detail || "" });
    if (ok) pass += 1; else fail += 1;
    const li = document.createElement("li");
    li.className = "card";
    const badge = document.createElement("span");
    badge.className = "badge " + (ok ? "badge-explicit" : "badge-sql-only");
    badge.textContent = ok ? "PASS" : "FAIL";
    const p = document.createElement("strong");
    p.textContent = name;
    li.append(badge, " ", p);
    if (detail) {
      const d = document.createElement("p");
      d.className = "muted";
      d.textContent = detail;
      li.append(d);
    }
    list.append(li);
  }

  function doc() { return frame.contentDocument; }
  function id(d, name) { return d.getElementById(name); }
  function text(el) { return (el && el.textContent) ? el.textContent.trim() : ""; }
  function visible(el) { return !!el && !el.hidden && el.style.display !== "none"; }

  function waitFor(condFn, timeoutMs, pollMs) {
    return new Promise(function (resolve) {
      const t0 = Date.now();
      (function tick() {
        let v = null;
        try { v = condFn(); } catch (e) { v = null; }
        if (v) return resolve(v);
        if (Date.now() - t0 > timeoutMs) return resolve(null);
        setTimeout(tick, pollMs || 200);
      })();
    });
  }

  async function run() {
    // 1. App frame loaded with expected controls
    const d = await waitFor(function () {
      const dd = doc();
      return dd && id(dd, "care-input") && id(dd, "service-chips").children.length >= 6 ? dd : null;
    }, 15000, 250);
    record("App page loads with search form and 6 quick-search chips", !!d,
      d ? "" : "Timed out waiting for the app frame. Is this page opened while signed in?");
    if (!d) return;

    const chips = id(d, "service-chips").children.length;
    record("Quick-search chips rendered", chips === 6, chips + " chips found");

    // 2. Live search via the public form
    const input = id(d, "care-input");
    input.value = "dialysis";
    id(d, "search-form").dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    const gotResults = await waitFor(function () {
      const dd = doc();
      return id(dd, "results-list").children.length > 0;
    }, 30000, 400);
    const errVisible = visible(id(d, "state-error")) || visible(id(d, "state-auth"));
    record("Live search returns result cards", !!gotResults,
      gotResults ? id(d, "results-list").children.length + " cards on page 1" :
        (errVisible ? "App showed an error/auth state instead of results" : "No cards within 30 s"));
    if (!gotResults) return;

    // 3. Evidence, reminder, explanation present
    const firstCard = id(d, "results-list").children[0];
    const cardText = text(firstCard);
    record("First card shows an evidence badge", /listed|specialty|equipment|equipment|procedures|review/i.test(cardText), cardText.slice(0, 90));
    record("First card shows a quoted excerpt or warning content", cardText.length > 120, "");
    record("Availability reminder visible above results", visible(id(d, "results-reminder")), "");
    record("First-result ordering explanation shown", visible(id(d, "first-explanation")) && text(id(d, "first-explanation")).length > 10, text(id(d, "first-explanation")).slice(0, 90));
    record("Result count shown", /matching source records/.test(text(id(d, "results-count"))), text(id(d, "results-count")));

    // 3b. Comparison flow (regression: Compare side-by-side must become ENABLED)
    function compareBtnOf(card) {
      return Array.from(card.querySelectorAll("button")).find(function (b) { return /compare|in comparison/i.test(b.textContent.trim()); });
    }
    const cards0 = doc().querySelectorAll("#results-list .card");
    if (cards0.length >= 2 && compareBtnOf(cards0[0]) && compareBtnOf(cards0[1])) {
      compareBtnOf(cards0[0]).click();
      // Cards re-render after each toggle — re-query the second card's button.
      const second = doc().querySelectorAll("#results-list .card")[1];
      if (second && compareBtnOf(second)) compareBtnOf(second).click();
      const trayVisible = visible(id(doc(), "compare-tray"));
      const openBtn = id(doc(), "compare-open");
      const openEnabled = trayVisible && openBtn && !openBtn.disabled;
      record("Selecting two facilities shows tray + ENABLED compare button", openEnabled,
        trayVisible ? "tray visible" : "tray hidden");
      if (openEnabled) {
        openBtn.click();
        const modal = visible(id(doc(), "compare-modal"));
        const rows = doc().querySelectorAll("#compare-modal table tr").length;
        record("Comparison table opens side by side", !!modal && rows > 5, modal ? rows + " table rows" : "modal did not open");
        const checklist = doc().querySelector("#compare-modal .checklist");
        record("Capacity row uses live-info wording (no false dataset claim)",
          !!checklist && /no live appointment or treatment-capacity information is available/i.test(checklist.textContent),
          checklist ? checklist.textContent.slice(0, 120) : "checklist not found");
        const hint = doc().querySelector("#compare-modal .compare-scroll-hint");
        record("Wide-table scroll affordance hint shown",
          !!hint && /scroll sideways/i.test(hint.textContent), "");
        const closeBtn = doc().querySelector("#compare-modal [data-close]");
        if (closeBtn) closeBtn.click();
        const removeBtn = doc().querySelector("#compare-tray-items button");
        if (removeBtn) removeBtn.click();
        const left = doc().querySelectorAll("#compare-tray-items .tray-item").length;
        record("Facility removable from comparison", left === 1, left + " items left");
      }
    } else {
      record("Comparison flow testable (two cards with compare buttons)", false, cards0.length + " cards found");
    }

    // 4. Interactive Leaflet map with OpenStreetMap basemap
    const mapBtn = id(d, "view-map");
    if (mapBtn) mapBtn.click();
    const markers = await waitFor(function () {
      const dd = doc();
      const panel = id(dd, "map-panel");
      if (!visible(panel)) return null;
      return panel.querySelectorAll(".facility-marker").length;
    }, 6000, 250);
    record("Interactive map renders with facility markers", (markers || 0) > 0,
      (markers || 0) + " markers at recorded coordinates");
    const tiles = await waitFor(function () {
      const dd = doc();
      const n = dd.querySelectorAll(".leaflet-tile-loaded").length;
      return n >= 2 ? n : null;
    }, 20000, 500);
    record("OpenStreetMap basemap tiles load (network/CSP check)", !!tiles,
      tiles ? tiles + " tiles loaded" : "Tiles did not load — likely blocked by a network or security policy in this environment");
    const note = id(doc(), "map-note");
    record("No hidden basemap failure (tile-error notice absent)", !(note && visible(note)),
      note && note.textContent ? note.textContent.slice(0, 90) : "");

    // 5. Evidence drawer
    const detailsBtn = firstCard.querySelector("button");
    const drawerBtn = Array.from(firstCard.querySelectorAll("button"))
      .find(function (b) { return /details/i.test(b.textContent); });
    if (drawerBtn) drawerBtn.click();
    const drawerBody = await waitFor(function () {
      const dd = doc();
      const body = id(dd, "drawer-body");
      return visible(id(dd, "drawer")) && body && body.textContent.length > 40 ? body : null;
    }, 5000, 200);
    const drawerText = drawerBody ? drawerBody.textContent : "";
    record("Evidence drawer opens with availability statement",
      /contact facility to confirm/i.test(drawerText), drawerText.slice(0, 80));
    record("Drawer includes unverified contact labeling or its absence notice",
      /unverified|does not invent contacts/i.test(drawerText), "");
    record("No '(no text)' placeholder quotes in drawer", !/\(no text\)/.test(drawerText),
      /\(no text\)/.test(drawerText) ? "placeholder quote still rendered" : "");
    record("Raw snake_case enums are not shown (temporal/modality)",
      !/(undated_statement|dated_historical_statement|unspecified_dialysis|peritoneal_dialysis)/.test(drawerText), "");

    // Summary
    const summary = document.createElement("p");
    summary.className = "results-reminder";
    summary.textContent = "Self-tests finished: " + pass + " passed, " + fail + " failed.";
    list.before(summary);
  }

  run().catch(function (e) {
    record("Self-test runner error", false, String(e && e.message ? e.message : e));
  });
})();
