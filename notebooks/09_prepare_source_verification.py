# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 09 — Prepare source verification (part 1, cells 1-2: read-only; no writes, no fetching)
# MAGIC
# MAGIC Reads `vf.gold.dialysis_jaipur_pilot_v1`, inspects actual facility names first, and selects the three pilot records by
# MAGIC EXACT name match — stopping on missing or ambiguous matches rather than fuzzy matching. Candidate URLs
# MAGIC (`unattributed_source_url_candidates`) are exploded to one row per record/URL, exact URLs deduplicated within each
# MAGIC record, and hostnames parsed safely (`parse_url`, NULL when not parseable). Retained per row: record_id, facility_name,
# MAGIC assertion, evidence_quotations, and attribution warnings (facility_attribution, possible_mixed_facility_record,
# MAGIC needs_attribution_review, attribution_evidence). No pages are fetched; no LLM is called; no domain is classified as
# MAGIC official; no URL is attached to a quotation; no tables are written.

# COMMAND ----------

# DBTITLE 1,Select pilot records & explode URLs
# Cell 1 — Inspect actual names, select the 3 exact pilot records, explode candidate URLs (read-only).
# No page fetching, no LLM calls, no domain classification, no URL-to-quotation attachment, no writes.
from pyspark.sql import functions as F

GOLD_VIEW = "vf.gold.dialysis_jaipur_pilot_v1"
gv = spark.table(GOLD_VIEW)

# Inspect actual facility names first.
names = [r["facility_name"] for r in gv.select("facility_name").orderBy(F.asc("facility_name")).collect()]
print(f"All facility names in {GOLD_VIEW} ({len(names)}):")
for n in names:
    print(" -", n)

TARGETS = ["Vaishali Hospital & Surgical Research Center Jaipur",
           "CKS Hospital",
           "Monilek Hospital and Research Center"]
match_counts = {t: gv.where(F.col("facility_name") == t).count() for t in TARGETS}
for t, c in match_counts.items():
    print(f"exact match count {c}: {t}")
missing = [t for t, c in match_counts.items() if c == 0]
ambiguous = [t for t, c in match_counts.items() if c > 1]
if missing or ambiguous:
    raise ValueError(f"Stopping — missing: {missing}, ambiguous: {ambiguous}. No fuzzy matching applied.")

df_pilot = gv.where(F.col("facility_name").isin(TARGETS))
n_sel, n_names = df_pilot.count(), df_pilot.select("facility_name").distinct().count()
print(f"Selected records: {n_sel} rows, {n_names} distinct facilities (expected 3/3)")
if (n_sel, n_names) != (3, 3):
    raise ValueError(f"Unexpected selection: {n_sel} rows, {n_names} distinct facilities — stopping.")

# One row per record/URL: explode_outer retains records with no candidates as a null-URL row;
# array_distinct deduplicates exact URLs within each record (no case or path normalization).
df_urls = (df_pilot
    .withColumn("url", F.explode_outer(F.array_distinct("unattributed_source_url_candidates")))
    .withColumn("hostname", F.expr("parse_url(url, 'HOST')"))  # safe: NULL when not parseable
    .select("record_id", "facility_name", "assertion",
            "evidence_quotations", "attribution_evidence", "facility_attribution",
            "possible_mixed_facility_record", "needs_attribution_review",
            "url", "hostname"))
n_rows = df_urls.count()
n_urls = df_urls.where(F.col("url").isNotNull()).count()
no_url = [r["facility_name"] for r in df_urls.where(F.col("url").isNull()).select("facility_name").distinct().collect()]
print(f"Exploded rows: {n_rows} total, {n_urls} non-null candidate URLs; facilities with no candidate URLs: {no_url if no_url else 'none'}")

# COMMAND ----------

# DBTITLE 1,Display URL candidates & attribution concerns
# Cell 2 — Display: (1) candidate URL counts per facility and hostname, (2) all candidate URLs
# grouped by facility with hostname, (3) evidence quotations and attribution concerns (read-only).
from pyspark.sql import functions as F

print("1) Candidate URL counts per facility and hostname:")
display(df_urls.where(F.col("url").isNotNull())
        .groupBy("facility_name", "hostname").count()
        .orderBy(F.asc("facility_name"), F.asc("hostname")))

print("2) All candidate URLs by facility (exact URLs, deduplicated within each record):")
display(df_urls.where(F.col("url").isNotNull())
        .select("facility_name", "url", "hostname")
        .orderBy(F.asc("facility_name"), F.asc("url")))

print("3) Evidence quotations and attribution concerns for the three pilot records:")
display(df_pilot.select(
    "facility_name", "assertion", "evidence_quotations",
    "attribution_evidence", "facility_attribution",
    "possible_mixed_facility_record", "needs_attribution_review",
).orderBy(F.asc("facility_name")))

# COMMAND ----------

# DBTITLE 1,Source retrieval overview
# MAGIC %md
# MAGIC # 09 — Source retrieval (part 2, cells 3-5: driver-side fetch; no LLM calls)
# MAGIC
# MAGIC Bounded retrieval executed entirely in Databricks on the driver (no Spark UDFs), reusing the ACTUAL candidate URLs
# MAGIC from the gold view — never invented paths. Per facility, only the candidate first-party domains are eligible
# MAGIC (ckshospitals.com / monilekhospital.com / vaishalihospital.com and their www variants — candidate domains, NOT
# MAGIC confirmed official sites), up to 4 URLs each, prioritized by path: dialysis/nephrology, then services, then
# MAGIC contact/about, then homepage, with deterministic URL-string tie-breaks. At most 12 pages are fetched with timeouts,
# MAGIC a 512 KB response bound, at most one retry on transient errors, polite delays between requests, and manual
# MAGIC redirects validated before following (public HTTP(S) only; private/local addresses rejected; cross-domain redirects
# MAGIC logged). Blocked responses are recorded, never bypassed; rate limits are honored. If network access is unavailable
# MAGIC the step stops with the actual error — model knowledge is never substituted and retrieval success is never faked.
# MAGIC One row per attempted facility/URL (record_id, facility_name, requested_url, final_url, retrieved_at_utc,
# MAGIC http_status, content_type, fetch_status, error_message, page_title, raw_html, extracted_text, content_hash,
# MAGIC extraction_method, truncation_flag) is written to `vf.silver.pilot_source_pages_v1` (error-if-exists; fetch once —
# MAGIC cells 4-5 read only the saved table). No verified status is assigned, no domain is claimed official, absence of a
# MAGIC keyword match is not evidence of absence of dialysis, and the Gold view is not updated.

# COMMAND ----------

# DBTITLE 1,Select & fetch candidate URLs, write pilot_source_pages
# Cell 3 — Select up to 4 candidate first-party URLs per facility and fetch them (driver-side; max 12 pages).
# No LLM calls. Domains below are CANDIDATE first-party domains from source records, NOT confirmed official sites.
# Fetch happens once: if the table already exists, stop and inspect the saved results instead.
import hashlib, ipaddress, re, socket, time
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
from pyspark.sql import functions as F

TARGET_TABLE = "vf.silver.pilot_source_pages_v1"
try:
    _exists = spark.catalog.tableExists(TARGET_TABLE)
except Exception:
    _exists = False
if _exists:
    raise ValueError(f"{TARGET_TABLE} already exists — not fetching again. Cells 4-5 read the saved results.")

MAX_PAGES = 12            # hard cap on pages fetched
MAX_BYTES = 512 * 1024    # bounded response size
CONNECT_TIMEOUT, READ_TIMEOUT = 8, 15
POLITE_DELAY_SECONDS = 1.5  # honor rate limits: pause between requests
MAX_RETRIES = 1           # at most one retry for transient errors
MAX_REDIRECTS = 5
TRANSIENT_STATUS = {502, 503, 504}
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DatabricksPilotSourceVerification/1.0; read-only page check)"}

CANDIDATE_DOMAINS = {  # per facility — candidate first-party domains, NOT confirmed official sites
    "CKS Hospital": ["ckshospitals.com", "www.ckshospitals.com"],
    "Monilek Hospital and Research Center": ["monilekhospital.com", "www.monilekhospital.com"],
    "Vaishali Hospital & Surgical Research Center Jaipur": ["vaishalihospital.com", "www.vaishalihospital.com"],
}

def path_tier(path):
    """Priority: dialysis/nephrology paths, then services, then contact/about, then homepage, then other."""
    p = (path or "").lower()
    if re.search(r"dialysis|nephro", p):
        return 0
    if "service" in p:
        return 1
    if re.search(r"contact|about", p):
        return 2
    if p in ("", "/"):
        return 3
    return 4

# Select from the ACTUAL exploded candidates (df_urls, cell 1). No invented paths.
selected = []
print("URL selection (from vf.gold candidate URLs; deterministic tie-break by URL string):")
for facility, domains in CANDIDATE_DOMAINS.items():
    cand = (df_urls
            .where((F.col("facility_name") == facility) & F.col("hostname").isin(domains))
            .select("record_id", "facility_name", "url")
            .withColumn("path", F.expr("coalesce(parse_url(url, 'PATH'), '')"))
            .collect())
    ordered = sorted(cand, key=lambda r: (path_tier(r["path"]), r["url"]))
    chosen = ordered[:4]
    print(f"  {facility}: {len(cand)} candidate URLs on candidate domains -> selected {len(chosen)}")
    for r in chosen:
        print(f"    [tier {path_tier(r['path'])}] {r['url']}")
    selected.extend(chosen)
if len(selected) > MAX_PAGES:
    selected = selected[:MAX_PAGES]
    print(f"Trimmed selection to the {MAX_PAGES}-page cap")
print(f"Total selected: {len(selected)} URLs (fetch cap {MAX_PAGES} pages)")

def is_public_http(url):
    """Only public HTTP(S) destinations are allowed; private/local/other-scheme addresses are rejected."""
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
    """Readable text with paragraph boundaries preserved: block tags become line breaks."""
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
    """Driver-side fetch: manual redirects (each destination validated BEFORE following), at most one
    retry on transient errors, bounded body. Blocked responses are recorded, never bypassed."""
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

rows, fetched = [], 0
print("\nFetching (driver-side; polite delay between requests; blocks are never bypassed):")
for sel in selected:
    if fetched >= MAX_PAGES:
        print(f"  Stopping: {MAX_PAGES}-page cap reached.")
        break
    if fetched:
        time.sleep(POLITE_DELAY_SECONDS)
    fetched += 1
    url = sel["url"]
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
    rows.append(row)

got_response = any(r["http_status"] is not None for r in rows)
if rows and not got_response:
    first_err = next((r["error_message"] for r in rows if r["error_message"]), "unknown")
    raise RuntimeError("Network access appears unavailable — no HTTP response from any of the "
                       f"{len(rows)} attempted URLs; stopping with the actual error and writing nothing. "
                       f"First error: {first_err}")

from pyspark.sql import types as T
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
df_pages = spark.createDataFrame(rows, PAGES_SCHEMA)
df_pages.write.mode("error").saveAsTable(TARGET_TABLE)  # error-if-exists
n_saved = spark.table(TARGET_TABLE).count()
print(f"\nWrote {TARGET_TABLE}: {n_saved} rows (one per attempted facility/URL). A successful HTTP response does not prove useful evidence.")

# COMMAND ----------

# DBTITLE 1,Fetch-status counts & selected URLs
# Cell 4 — Inspection reads ONLY the saved table (no refetch): fetch-status counts + selected URLs.
from pyspark.sql import functions as F

pages = spark.table("vf.silver.pilot_source_pages_v1")

print("Fetch-status counts (vf.silver.pilot_source_pages_v1):")
display(pages.groupBy("fetch_status").count().orderBy(F.asc("fetch_status")))

print("Selected URLs and outcomes (requested -> final; candidate links, NOT confirmed official sites):")
display(pages.select("facility_name", "requested_url", "final_url", "http_status", "content_type",
                     "fetch_status", "truncation_flag")
        .orderBy(F.asc("facility_name"), F.asc("requested_url")))

# COMMAND ----------

# DBTITLE 1,Keyword & identity paragraphs from saved pages
# Cell 5 — Paragraph inspection from the SAVED table (read-only): service keywords, identity/address
# candidates, and full-text availability. No verified status is assigned; no domain is claimed official;
# absence of a keyword match is NOT evidence of absence of dialysis; the Gold view is not updated.
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

pages = spark.table("vf.silver.pilot_source_pages_v1")
FACILITY_PINS = {"CKS Hospital": "302013",
                 "Monilek Hospital and Research Center": "302004",
                 "Vaishali Hospital & Surgical Research Center Jaipur": "302021"}
SERVICE_KW = re.compile(r"dialysis|haemodialysis|hemodialysis|nephrology", re.IGNORECASE)
PARA_SCHEMA = T.StructType([
    T.StructField("facility_name", T.StringType(), True),
    T.StructField("requested_url", T.StringType(), True),
    T.StructField("paragraph_index", T.IntegerType(), True),
    T.StructField("previous_paragraph", T.StringType(), True),
    T.StructField("paragraph", T.StringType(), True),
    T.StructField("next_paragraph", T.StringType(), True),
])

kw_rows, id_rows = [], []
for r in (pages.where(F.col("extracted_text").isNotNull())
          .select("facility_name", "requested_url", "extracted_text").collect()):
    paras = r["extracted_text"].split("\n")
    ident_parts = ["jaipur", FACILITY_PINS.get(r["facility_name"]), r["facility_name"].split()[0]]
    ident = re.compile("|".join(re.escape(p) for p in ident_parts if p), re.IGNORECASE)
    for i, p in enumerate(paras):
        if SERVICE_KW.search(p):
            kw_rows.append({"facility_name": r["facility_name"], "requested_url": r["requested_url"],
                            "paragraph_index": i,
                            "previous_paragraph": paras[i - 1] if i > 0 else None,
                            "paragraph": p,
                            "next_paragraph": paras[i + 1] if i < len(paras) - 1 else None})
        if ident.search(p):
            id_rows.append({"facility_name": r["facility_name"], "requested_url": r["requested_url"],
                            "paragraph_index": i, "previous_paragraph": None,
                            "paragraph": p, "next_paragraph": None})

print(f"Service-keyword paragraphs (dialysis/haemodialysis/hemodialysis/nephrology, with adjacent context): {len(kw_rows)}")
display(spark.createDataFrame(kw_rows, PARA_SCHEMA).orderBy(F.asc("facility_name"), F.asc("requested_url"), F.asc("paragraph_index")))
if not kw_rows:
    print("  No keyword matches in the retrieved text — this is NOT evidence that dialysis is absent.")

print(f"\nCandidate identity/address paragraphs (hospital name, Jaipur, or its PIN): {len(id_rows)}")
display(spark.createDataFrame(id_rows, PARA_SCHEMA)
        .select("facility_name", "requested_url", "paragraph_index", "paragraph")
        .orderBy(F.asc("facility_name"), F.asc("requested_url"), F.asc("paragraph_index")))

print("\nFull extracted text remains available in vf.silver.pilot_source_pages_v1 (raw_html also retained in the table):")
display(pages.select("facility_name", "requested_url", "fetch_status", "page_title", "extracted_text")
        .orderBy(F.asc("facility_name"), F.asc("requested_url")))