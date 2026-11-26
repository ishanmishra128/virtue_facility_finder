# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 10 — Link pilot source evidence (saved pages only; no refetch of successful pages, no LLM)
# MAGIC
# MAGIC Reads `vf.silver.pilot_source_pages_v1` (all fetch rows preserved) and the three corresponding pilot records from
# MAGIC `vf.gold.dialysis_jaipur_pilot_v1` (exact-name match; stop on missing/ambiguous). Identical page content
# MAGIC (`content_hash`) within a facility is deduplicated for evidence counting — WWW redirects and repeated site
# MAGIC navigation are not independent corroboration. (1) Complete service and identity/address passages are displayed
# MAGIC with record_id, requested/final URL, retrieval timestamp, content hash, and truncation flag. (2) Hyperlinks are
# MAGIC extracted from the SAVED HTML (labels/paths mentioning dialysis/haemodialysis/hemodialysis/nephrology), relative
# MAGIC URLs resolved against final_url; after displaying candidates, at most ONE new service page per facility is
# MAGIC fetched — only on the facility's candidate domains and only if not already fetched — with the notebook-09
# MAGIC safeguards (timeouts, 512 KB bound, one retry, validated public HTTP(S) redirects, polite delays, blocks never
# MAGIC bypassed). New fetches are saved separately error-if-exists (`vf.silver.pilot_source_pages_followup_v1`); an HTTP
# MAGIC 200 is never treated as substantive evidence. (3) `vf.silver.pilot_service_evidence_v1` holds one row per distinct
# MAGIC service passage with exact quote, adjacent context, fetch metadata, identity/address support text, and
# MAGIC DETERMINISTIC proposed labels only (`proposed_support_type`: explicit_dialysis_mention / nephrology_only /
# MAGIC needs_review; `temporal_context`: dated_historical_statement / undated_statement / needs_review — dating uses
# MAGIC years present in the text, never the retrieval date; the 2023 Vaishali statement stays explicitly historical),
# MAGIC with `branch_match_status='pending_review'` and `human_review_status='pending'`. Every quotation is validated
# MAGIC verbatim against the saved extracted text; nephrology is never treated as dialysis and missing mentions are not
# MAGIC proof of absence; verification/current-availability statuses and the Gold view are not changed.

# COMMAND ----------

# DBTITLE 1,Saved-page passages with full metadata
# Cell 1 — Load SAVED pages (no refetching, no LLM) and the 3 pilot records; display complete service and
# identity/address passages with full retrieval metadata. Identical page content (content_hash) within a
# facility is deduplicated for evidence counting; all original fetch rows remain preserved in the table.
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

GOLD_VIEW = "vf.gold.dialysis_jaipur_pilot_v1"
PAGES_TABLE = "vf.silver.pilot_source_pages_v1"
TARGETS = ["Vaishali Hospital & Surgical Research Center Jaipur",
           "CKS Hospital",
           "Monilek Hospital and Research Center"]
FACILITY_PINS = {"CKS Hospital": "302013",
                 "Monilek Hospital and Research Center": "302004",
                 "Vaishali Hospital & Surgical Research Center Jaipur": "302021"}

gv = spark.table(GOLD_VIEW)
match_counts = {t: gv.where(F.col("facility_name") == t).count() for t in TARGETS}
for t, c in match_counts.items():
    print(f"exact match count {c}: {t}")
missing = [t for t, c in match_counts.items() if c == 0]
ambiguous = [t for t, c in match_counts.items() if c > 1]
if missing or ambiguous:
    raise ValueError(f"Stopping — missing: {missing}, ambiguous: {ambiguous}. No fuzzy matching.")
df_pilot = gv.where(F.col("facility_name").isin(TARGETS))
pilot_ids = {r["facility_name"]: r["record_id"] for r in df_pilot.select("facility_name", "record_id").collect()}
print(f"Pilot records: {len(pilot_ids)} (exact match each)")

pages_all = spark.table(PAGES_TABLE)  # all fetch rows preserved (no refetch)
n_fetch = pages_all.count()
pages = pages_all.where(F.col("fetch_status") == "success")

# Deduplicate identical page content within each facility for EVIDENCE COUNTING (content_hash):
# WWW redirects or repeated site navigation are not independent corroboration.
seen, unique_pages = set(), []
for r in pages.select("record_id", "facility_name", "requested_url", "final_url", "retrieved_at_utc",
                      "content_hash", "truncation_flag", "extracted_text").collect():
    key = (r["record_id"], r["content_hash"])
    if key not in seen:
        seen.add(key)
        unique_pages.append(r)
print(f"Fetch rows preserved: {n_fetch}; unique page contents: {len(unique_pages)} (duplicates excluded from evidence counting)")

SERVICE_KW = re.compile(r"dialysis|haemodialysis|hemodialysis|nephrology", re.IGNORECASE)

def ident_re(facility):
    parts = ["jaipur", FACILITY_PINS[facility], facility.split()[0]]
    return re.compile("|".join(re.escape(p) for p in parts if p), re.IGNORECASE)

PASSAGE_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType(), True),
    T.StructField("facility_name", T.StringType(), True),
    T.StructField("requested_url", T.StringType(), True),
    T.StructField("final_url", T.StringType(), True),
    T.StructField("retrieved_at_utc", T.StringType(), True),
    T.StructField("content_hash", T.StringType(), True),
    T.StructField("truncation_flag", T.BooleanType(), True),
    T.StructField("paragraph_index", T.IntegerType(), True),
    T.StructField("previous_paragraph", T.StringType(), True),
    T.StructField("passage", T.StringType(), True),
    T.StructField("next_paragraph", T.StringType(), True),
])

service_rows, identity_rows = [], []
for r in unique_pages:
    ident = ident_re(r["facility_name"])
    paras = r["extracted_text"].split("\n") if r["extracted_text"] else []
    for i, p in enumerate(paras):
        base = {"record_id": r["record_id"], "facility_name": r["facility_name"],
                "requested_url": r["requested_url"], "final_url": r["final_url"],
                "retrieved_at_utc": r["retrieved_at_utc"], "content_hash": r["content_hash"],
                "truncation_flag": r["truncation_flag"], "paragraph_index": i}
        if SERVICE_KW.search(p):
            service_rows.append({**base, "previous_paragraph": paras[i - 1] if i > 0 else None,
                                 "passage": p, "next_paragraph": paras[i + 1] if i < len(paras) - 1 else None})
        if ident.search(p):
            identity_rows.append({**base, "previous_paragraph": None, "passage": p, "next_paragraph": None})

print(f"\nComplete service passages (deduplicated pages): {len(service_rows)}")
display(spark.createDataFrame(service_rows, PASSAGE_SCHEMA)
        .orderBy(F.asc("facility_name"), F.asc("requested_url"), F.asc("paragraph_index")))

print(f"\nComplete identity/address passages (deduplicated pages): {len(identity_rows)}")
display(spark.createDataFrame(identity_rows, PASSAGE_SCHEMA)
        .select("record_id", "facility_name", "requested_url", "final_url", "retrieved_at_utc",
                "content_hash", "truncation_flag", "paragraph_index", "passage")
        .orderBy(F.asc("facility_name"), F.asc("requested_url"), F.asc("paragraph_index")))

# COMMAND ----------

# DBTITLE 1,Extract keyword links; fetch ≤1 new page per facility
# Cell 2 — Extract keyword hyperlinks from SAVED HTML (no invented paths), resolve relative URLs against
# final_url, display candidate links, then fetch at most ONE new service page per facility (same candidate
# domain, not already fetched) with the notebook-09 safeguards. New fetches saved error-if-exists separately.
import hashlib, ipaddress, re, socket, time
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
from pyspark.sql import types as T

FOLLOWUP_TABLE = "vf.silver.pilot_source_pages_followup_v1"
CANDIDATE_DOMAINS = {  # candidate first-party domains, NOT confirmed official sites
    "CKS Hospital": ["ckshospitals.com", "www.ckshospitals.com"],
    "Monilek Hospital and Research Center": ["monilekhospital.com", "www.monilekhospital.com"],
    "Vaishali Hospital & Surgical Research Center Jaipur": ["vaishalihospital.com", "www.vaishalihospital.com"],
}
MAX_BYTES = 512 * 1024
CONNECT_TIMEOUT, READ_TIMEOUT = 8, 15
POLITE_DELAY_SECONDS = 1.5
MAX_RETRIES = 1
MAX_REDIRECTS = 5
TRANSIENT_STATUS = {502, 503, 504}
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DatabricksPilotSourceVerification/1.0; read-only page check)"}
LINK_KW = re.compile(r"dialysis|haemodialysis|hemodialysis|nephrology", re.IGNORECASE)
DIALYSIS_KW = re.compile(r"dialysis|haemodialysis|hemodialysis", re.IGNORECASE)

class _LinkExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links, self._href, self._buf = [], None, []
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href = dict(attrs).get("href")
            self._buf = []
    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            label = re.sub(r"\s+", " ", "".join(self._buf)).strip()
            self.links.append((self._href, label))
            self._href = None
    def handle_data(self, data):
        if self._href is not None:
            self._buf.append(data)

# Extract links from deduplicated saved pages (raw_html comes from the saved table; no refetch).
html_by_hash = {r["content_hash"]: r["raw_html"] for r in
                pages.where(F.col("raw_html").isNotNull()).select("content_hash", "raw_html").collect()}
fetched_urls = set()
for r in pages_all.select("requested_url", "final_url").collect():
    fetched_urls.update(u for u in (r["requested_url"], r["final_url"]) if u)

link_rows, seen_links = [], set()
for r in unique_pages:
    raw = html_by_hash.get(r["content_hash"])
    if not raw:
        continue
    p = _LinkExtractor()
    p.feed(raw)
    p.close()
    for href, label in p.links:
        if not href:
            continue
        absu = urljoin(r["final_url"], href)  # resolve relative URLs against final_url
        u = urlparse(absu)
        if u.scheme not in ("http", "https") or not u.hostname:
            continue
        if not (LINK_KW.search(label or "") or LINK_KW.search(u.path or "")):
            continue
        key = (r["record_id"], absu)
        if key in seen_links:
            continue
        seen_links.add(key)
        link_rows.append({"record_id": r["record_id"], "facility_name": r["facility_name"],
                          "source_page": r["final_url"], "link_label": label,
                          "resolved_url": absu, "hostname": u.hostname,
                          "dialysis_terms": bool(DIALYSIS_KW.search(label or "") or DIALYSIS_KW.search(u.path or "")),
                          "already_fetched": absu in fetched_urls})

LINK_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType(), True),
    T.StructField("facility_name", T.StringType(), True),
    T.StructField("source_page", T.StringType(), True),
    T.StructField("link_label", T.StringType(), True),
    T.StructField("resolved_url", T.StringType(), True),
    T.StructField("hostname", T.StringType(), True),
    T.StructField("dialysis_terms", T.BooleanType(), True),
    T.StructField("already_fetched", T.BooleanType(), True),
])
print(f"Candidate hyperlinks from saved HTML (label/path mentions dialysis/haemodialysis/hemodialysis/nephrology): {len(link_rows)}")
display(spark.createDataFrame(link_rows, LINK_SCHEMA).orderBy(F.asc("facility_name"), F.asc("resolved_url")))

# Selection: at most ONE new service page per facility — same candidate domain, not already fetched,
# dialysis-term links before nephrology-only; deterministic URL tie-break.
followup_exists = spark.catalog.tableExists(FOLLOWUP_TABLE)
if followup_exists:
    print(f"{FOLLOWUP_TABLE} already exists — not fetching again; cell 3 reads the saved follow-up pages.")
chosen = []
if not followup_exists:
    for facility, domains in CANDIDATE_DOMAINS.items():
        cands = [l for l in link_rows
                 if l["facility_name"] == facility
                 and l["hostname"] in domains
                 and not l["already_fetched"]]
        cands = sorted(cands, key=lambda l: (0 if l["dialysis_terms"] else 1, l["resolved_url"]))
        if cands:
            chosen.append(cands[0])
            print(f"  {facility}: will fetch {cands[0]['resolved_url']} (label: {cands[0]['link_label']!r})")
        else:
            print(f"  {facility}: no eligible new service page on candidate domains — nothing fetched")

# --- network safeguards identical to notebook 09 ---
def is_public_http(url):
    try:
        u = urlparse(url)
    except Exception as e:
        return False, f"unparseable_url: {e}"
    if u.scheme not in ("http", "https") or not u.hostname:
        return False, f"non_public_http_s_destination: scheme={u.scheme or 'none'}"
    try:
        infos = socket.getaddrinfo(u.hostname, None)
    except Exception as e:
        return False, f"dns_resolution_failed: {type(e).__name__}: {e}"
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except Exception:
            continue
        if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
                or ip.is_multicast or ip.is_unspecified):
            return False, f"private_or_local_address: {ip}"
    return True, None

BLOCK_TAGS = {"p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "br", "tr", "td", "th",
              "section", "article", "blockquote", "pre", "ul", "ol", "table", "header", "footer", "nav"}

class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.in_title, self.title, self.skip = [], False, "", 0
    def handle_starttag(self, tag, attrs):
        if tag == "title":
            self.in_title = True
        if tag in ("script", "style", "noscript", "template"):
            self.skip += 1
        if tag in BLOCK_TAGS:
            self.parts.append("\n")
    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
        if tag in ("script", "style", "noscript", "template") and self.skip:
            self.skip -= 1
        if tag in BLOCK_TAGS:
            self.parts.append("\n")
    def handle_data(self, data):
        if self.in_title:
            self.title += data.strip() + " "
        elif not self.skip:
            self.parts.append(data)

def extract_text_and_title(html_text):
    p = _TextExtractor()
    try:
        p.feed(html_text)
        p.close()
    except Exception as e:
        return None, None, f"stdlib_html_parser_failed: {type(e).__name__}"
    paras = [re.sub(r"\s+", " ", seg).strip() for seg in "".join(p.parts).split("\n")]
    paras = [seg for seg in paras if seg]
    title = re.sub(r"\s+", " ", p.title).strip() or None
    return title, ("\n".join(paras) if paras else None), "stdlib_html_parser_block_text"

def fetch_page(url):
    ok, reason = is_public_http(url)
    if not ok:
        return {"kind": "rejected", "error": f"requested destination rejected: {reason}", "final_url": None}
    current, redirects, attempt = url, 0, 0
    while True:
        try:
            with requests.get(current, headers=HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                              allow_redirects=False, stream=True) as r:
                if 300 <= r.status_code < 400 and r.headers.get("location"):
                    if redirects >= MAX_REDIRECTS:
                        return {"kind": "error", "error": f"too_many_redirects (> {MAX_REDIRECTS})", "final_url": current}
                    nxt = urljoin(current, r.headers["location"])
                    if (urlparse(nxt).hostname or "") != (urlparse(current).hostname or ""):
                        print(f"  [cross-domain redirect] {current} -> {nxt}")
                    ok, reason = is_public_http(nxt)
                    if not ok:
                        return {"kind": "rejected", "error": f"redirect destination rejected: {reason}", "final_url": nxt}
                    redirects += 1
                    current = nxt
                    continue
                if r.status_code in TRANSIENT_STATUS and attempt < MAX_RETRIES:
                    attempt += 1
                    time.sleep(POLITE_DELAY_SECONDS)
                    continue
                chunks, total = [], 0
                for chunk in r.iter_content(chunk_size=65536):
                    if total + len(chunk) > MAX_BYTES:
                        chunks.append(chunk[: MAX_BYTES - total])
                        total = MAX_BYTES
                        break
                    chunks.append(chunk)
                    total += len(chunk)
                return {"kind": "response", "status": r.status_code,
                        "content_type": (r.headers.get("Content-Type") or "").split(";")[0].strip().lower() or None,
                        "final_url": current, "body": b"".join(chunks), "bytes": total}
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES:
                attempt += 1
                time.sleep(POLITE_DELAY_SECONDS)
                continue
            return {"kind": "error", "error": f"{type(e).__name__}: {e}", "final_url": current}

PAGES_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType(), True),
    T.StructField("facility_name", T.StringType(), True),
    T.StructField("requested_url", T.StringType(), True),
    T.StructField("final_url", T.StringType(), True),
    T.StructField("retrieved_at_utc", T.StringType(), True),
    T.StructField("http_status", T.IntegerType(), True),
    T.StructField("content_type", T.StringType(), True),
    T.StructField("fetch_status", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
    T.StructField("page_title", T.StringType(), True),
    T.StructField("raw_html", T.StringType(), True),
    T.StructField("extracted_text", T.StringType(), True),
    T.StructField("content_hash", T.StringType(), True),
    T.StructField("extraction_method", T.StringType(), True),
    T.StructField("truncation_flag", T.BooleanType(), True),
])

if not followup_exists and chosen:
    frows, fetched = [], 0
    print("\nFetching follow-up pages (driver-side; blocks never bypassed; HTTP 200 is NOT substantive evidence):")
    for sel in chosen:
        if fetched:
            time.sleep(POLITE_DELAY_SECONDS)
        fetched += 1
        url = sel["resolved_url"]
        out = fetch_page(url)
        row = {"record_id": sel["record_id"], "facility_name": sel["facility_name"],
               "requested_url": url, "final_url": out.get("final_url"),
               "retrieved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "http_status": None, "content_type": None, "fetch_status": None, "error_message": None,
               "page_title": None, "raw_html": None, "extracted_text": None, "content_hash": None,
               "extraction_method": None, "truncation_flag": False}
        if out["kind"] == "rejected":
            row["fetch_status"], row["error_message"] = "redirect_rejected", out["error"]
        elif out["kind"] == "error":
            row["fetch_status"], row["error_message"] = "fetch_error", out["error"]
        else:
            row["http_status"], row["content_type"] = out["status"], out["content_type"]
            row["content_hash"] = hashlib.sha256(out["body"]).hexdigest()
            row["raw_html"] = out["body"].decode("utf-8", errors="replace")
            row["truncation_flag"] = out["bytes"] >= MAX_BYTES
            ct = out["content_type"] or ""
            if out["status"] in (401, 403, 429):
                row["fetch_status"], row["error_message"] = "blocked", f"blocked_response_http_{out['status']}"
            elif out["status"] >= 400:
                row["fetch_status"], row["error_message"] = "http_error", f"http_status_{out['status']}"
            elif ct and not (ct.startswith("text/html") or "xhtml" in ct):
                row["fetch_status"], row["error_message"] = "unsupported_content", f"unsupported_content_type: {ct}"
            else:
                title, text, method = extract_text_and_title(row["raw_html"])
                row["page_title"], row["extraction_method"] = title, method
                if method.startswith("stdlib_html_parser_failed"):
                    row["fetch_status"], row["error_message"] = "unsupported_content", method
                elif text is None:
                    row["fetch_status"], row["error_message"] = "empty_page", "no_readable_text_extracted"
                else:
                    row["extracted_text"], row["fetch_status"] = text, "success"
        print(f"  {row['fetch_status']:<18} http={row['http_status']} {url} -> {row['final_url']}")
        frows.append(row)
    df_followup = spark.createDataFrame(frows, PAGES_SCHEMA)
    df_followup.write.mode("error").saveAsTable(FOLLOWUP_TABLE)  # error-if-exists
    print(f"\nWrote {FOLLOWUP_TABLE}: {len(frows)} rows (saved separately from pilot_source_pages_v1).")
elif not followup_exists:
    print("\nNo follow-up fetches — no eligible new service pages on candidate domains.")
else:
    df_followup = spark.table(FOLLOWUP_TABLE)
    print(f"\nLoaded {df_followup.count()} saved follow-up rows from {FOLLOWUP_TABLE} (no refetch).")

# COMMAND ----------

# DBTITLE 1,Build & write evidence table with validation
# Cell 3 — Build the reviewable evidence table vf.silver.pilot_service_evidence_v1: one row per distinct
# service passage (deduplicated page content), DETERMINISTIC proposed labels only, full context retained,
# every quotation validated against the saved extracted text BEFORE the error-if-exists write.
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVIDENCE_TABLE = "vf.silver.pilot_service_evidence_v1"
try:
    _ev_exists = spark.catalog.tableExists(EVIDENCE_TABLE)
except Exception:
    _ev_exists = False
if _ev_exists:
    raise ValueError(f"{EVIDENCE_TABLE} already exists — stopping (error-if-exists). Cell 4 reads the saved table.")

# Passage sources: the saved pilot pages plus any follow-up pages (if that table exists).
page_source_rows = (pages_all.where(F.col("fetch_status") == "success")
    .select("record_id", "facility_name", "requested_url", "final_url", "retrieved_at_utc",
            "content_hash", "truncation_flag", "extracted_text").collect())
if spark.catalog.tableExists(FOLLOWUP_TABLE):
    page_source_rows += (spark.table(FOLLOWUP_TABLE).where(F.col("fetch_status") == "success")
        .select("record_id", "facility_name", "requested_url", "final_url", "retrieved_at_utc",
                "content_hash", "truncation_flag", "extracted_text").collect())

_seen, ev_pages = set(), []
for r in page_source_rows:  # deduplicate identical page content within each facility
    key = (r["record_id"], r["content_hash"])
    if key not in _seen:
        _seen.add(key)
        ev_pages.append(r)

DIALYSIS_KW = re.compile(r"dialysis|haemodialysis|hemodialysis", re.IGNORECASE)
NEPHRO_KW = re.compile(r"nephrology|nephrologist", re.IGNORECASE)
FUTURE_KW = re.compile(r"coming soon|planned|launch(?:ing|ed)?|upcoming|will (?:offer|start|provide|open|commence)|set to (?:open|launch)", re.IGNORECASE)
YEAR_KW = re.compile(r"\b(19[89][0-9]|20[0-2][0-9])\b")  # years present IN THE TEXT — never the retrieval date

def proposed_support_type(p):
    """Deterministic text rules only. Nephrology alone is NOT treated as dialysis."""
    if FUTURE_KW.search(p):
        return "needs_review"
    if DIALYSIS_KW.search(p):
        return "explicit_dialysis_mention"
    if NEPHRO_KW.search(p):
        return "nephrology_only"
    return "needs_review"

def temporal_context(p):
    """Deterministic: hedging/future markers => needs_review; explicit year in text => dated historical
    statement (the 2023 Vaishali statement stays explicitly historical); otherwise undated."""
    if FUTURE_KW.search(p):
        return "needs_review"
    if YEAR_KW.search(p):
        return "dated_historical_statement"
    return "undated_statement"

def identity_support_text(r):
    """Identity/address text from the SAME page: PIN-mentioning paragraph first, then facility-name, then Jaipur."""
    paras = r["extracted_text"].split("\n") if r["extracted_text"] else []
    pin, name, city = [], [], []
    pin_code = FACILITY_PINS.get(r["facility_name"], "")
    first_word = r["facility_name"].split()[0]
    for p in paras:
        if pin_code and pin_code in p:
            pin.append(p)
        elif first_word.lower() in p.lower():
            name.append(p)
        elif "jaipur" in p.lower():
            city.append(p)
    picks = (pin[:1] + name[:1]) or city[:1]
    return " | ".join(picks[:2]) or None

ev_rows = []
for r in ev_pages:
    support = identity_support_text(r)
    paras = r["extracted_text"].split("\n") if r["extracted_text"] else []
    for i, p in enumerate(paras):
        if SERVICE_KW.search(p):
            adjacent = "\n".join(x for x in [
                f"previous: {paras[i - 1]}" if i > 0 else None,
                f"next: {paras[i + 1]}" if i < len(paras) - 1 else None] if x)
            ev_rows.append({
                "record_id": r["record_id"],
                "facility_name": r["facility_name"],
                "source_location": f"retrieved_page_extracted_text:paragraph_{i}",
                "exact_quote": p,
                "adjacent_context": adjacent or None,
                "requested_url": r["requested_url"],
                "final_url": r["final_url"],
                "retrieved_at": r["retrieved_at_utc"],
                "content_hash": r["content_hash"],
                "truncation_flag": r["truncation_flag"],
                "identity_support_text": support,
                "proposed_support_type": proposed_support_type(p),
                "temporal_context": temporal_context(p),
                "branch_match_status": "pending_review",
                "human_review_status": "pending",
            })

# Validate EVERY quotation exists verbatim in the corresponding saved extracted text (same record + hash).
text_by_key = {(r["record_id"], r["content_hash"]): r["extracted_text"] for r in page_source_rows}
n_ok = sum(1 for ev in ev_rows
           if (text_by_key.get((ev["record_id"], ev["content_hash"])) or "").find(ev["exact_quote"]) >= 0)
print(f"Quote validation before write: {n_ok}/{len(ev_rows)} quotations found verbatim in saved extracted text")
if n_ok != len(ev_rows):
    raise ValueError(f"Quote validation failed for {len(ev_rows) - n_ok} rows — evidence table NOT written.")

EVIDENCE_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType(), True),
    T.StructField("facility_name", T.StringType(), True),
    T.StructField("source_location", T.StringType(), True),
    T.StructField("exact_quote", T.StringType(), True),
    T.StructField("adjacent_context", T.StringType(), True),
    T.StructField("requested_url", T.StringType(), True),
    T.StructField("final_url", T.StringType(), True),
    T.StructField("retrieved_at", T.StringType(), True),
    T.StructField("content_hash", T.StringType(), True),
    T.StructField("truncation_flag", T.BooleanType(), True),
    T.StructField("identity_support_text", T.StringType(), True),
    T.StructField("proposed_support_type", T.StringType(), True),
    T.StructField("temporal_context", T.StringType(), True),
    T.StructField("branch_match_status", T.StringType(), True),
    T.StructField("human_review_status", T.StringType(), True),
])
df_evidence = spark.createDataFrame(ev_rows, EVIDENCE_SCHEMA)
df_evidence.write.mode("error").saveAsTable(EVIDENCE_TABLE)  # error-if-exists
print(f"\nWrote {EVIDENCE_TABLE}: {len(ev_rows)} rows (one per distinct service passage; deduplicated pages).")
for f in TARGETS:
    n = sum(1 for ev in ev_rows if ev["facility_name"] == f)
    print(f"  {f}: {n} passages")
print("Proposed labels are deterministic text rules only; every row is branch_match_status='pending_review', human_review_status='pending'.")
print("No verification/current-availability status has been changed; the Gold view is untouched.")

# COMMAND ----------

# DBTITLE 1,Concise evidence display & validation checks
# Cell 4 — Concise evidence table + quote-validation checks, re-run from the SAVED tables (independent re-check).
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVIDENCE_TABLE = "vf.silver.pilot_service_evidence_v1"
FOLLOWUP_TABLE = "vf.silver.pilot_source_pages_followup_v1"
PAGES_TABLE = "vf.silver.pilot_source_pages_v1"
GOLD_VIEW = "vf.gold.dialysis_jaipur_pilot_v1"
TARGETS = ["Vaishali Hospital & Surgical Research Center Jaipur", "CKS Hospital", "Monilek Hospital and Research Center"]

ev = spark.table(EVIDENCE_TABLE)
print(f"Evidence rows in {EVIDENCE_TABLE}: {ev.count()}")
print("\nFixed review statuses (no verification claim made anywhere):")
display(ev.groupBy("branch_match_status", "human_review_status").count())

# Independent re-validation: quotes from the SAVED evidence table vs. saved page text (incl. follow-up pages).
page_text = {(r["record_id"], r["content_hash"]): r["extracted_text"] for r in
             spark.table(PAGES_TABLE).where(F.col("fetch_status") == "success")
             .select("record_id", "content_hash", "extracted_text").collect()}
if spark.catalog.tableExists(FOLLOWUP_TABLE):
    for r in (spark.table(FOLLOWUP_TABLE).where(F.col("fetch_status") == "success")
              .select("record_id", "content_hash", "extracted_text").collect()):
        page_text[(r["record_id"], r["content_hash"])] = r["extracted_text"]

val_rows = []
for evr in ev.select("record_id", "facility_name", "content_hash", "exact_quote").collect():
    text = page_text.get((evr["record_id"], evr["content_hash"]))
    val_rows.append({"record_id": evr["record_id"], "facility_name": evr["facility_name"],
                     "content_hash": evr["content_hash"], "quote_head": evr["exact_quote"][:80],
                     "quote_exists_in_saved_text": bool(text) and evr["exact_quote"] in text})
n_ok = sum(1 for v in val_rows if v["quote_exists_in_saved_text"])
print(f"\nQuote validation re-run from saved tables: {n_ok}/{len(val_rows)} PASS")
VAL_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType(), True),
    T.StructField("facility_name", T.StringType(), True),
    T.StructField("content_hash", T.StringType(), True),
    T.StructField("quote_head", T.StringType(), True),
    T.StructField("quote_exists_in_saved_text", T.BooleanType(), True),
])
display(spark.createDataFrame(val_rows, VAL_SCHEMA)
        .orderBy(F.asc("facility_name"), F.asc("content_hash")))
if n_ok != len(val_rows):
    raise ValueError(f"Quote re-validation failed for {len(val_rows) - n_ok} rows.")

print("\nEvidence by proposed_support_type and temporal_context (deterministic labels; nephrology_only ≠ dialysis):")
display(ev.groupBy("facility_name", "proposed_support_type", "temporal_context").count()
        .orderBy(F.asc("facility_name"), F.asc("proposed_support_type"), F.asc("temporal_context")))

print("\nConcise evidence table (full context, identity support, hashes, and raw fetch rows remain in the saved table):")
display(ev.select("record_id", "facility_name", "exact_quote", "proposed_support_type", "temporal_context",
                  "requested_url", "truncation_flag", "branch_match_status", "human_review_status")
        .orderBy(F.asc("facility_name"), F.asc("requested_url"), F.asc("record_id")))

bad = (spark.table(GOLD_VIEW)
       .where(F.col("facility_name").isin(TARGETS))
       .where((F.col("verification_status") != "unverified") | (F.col("current_availability_status") != "unknown"))
       .count())
print(f"\nGold statuses unchanged: {bad} of the 3 pilot records deviate from unverified/unknown (expected 0; nothing updated).")