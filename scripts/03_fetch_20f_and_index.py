import os, re, json, time, hashlib, requests
import pandas as pd
from datetime import datetime

"""
Usage:
    python scripts/03_fetch_20f_and_index.py
"""

# Config
DATA_DIR = "data/intermediate"
OUTPUT_DIR = "companies"
LOGS_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

HEADERS = {"User-Agent": "First-name Last-name email"}  # Enter your info

# Detect latest cik_map file
def get_latest_cik_map():
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("cik_map_") and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("No cik_map_*.csv found in data/intermediate")

    dated_files = []
    for f in files:
        m = re.search(r"cik_map_(\d{8})\.csv", f)
        if m:
            run_date = datetime.strptime(m.group(1), "%Y%m%d")
            dated_files.append((run_date, f))
    if not dated_files:
        raise ValueError("No valid cik_map_*.csv with YYYYMMDD in filename")

    latest_date, latest_file = max(dated_files, key=lambda x: x[0])
    return latest_date.strftime("%Y%m%d"), os.path.join(DATA_DIR, latest_file)

RUN_DATE, INPUT_FILE = get_latest_cik_map()

# Use a single persistent index file
INDEX_FILE = os.path.join(DATA_DIR, "annual_reports_index.json")
ERRORS_FILE = os.path.join(LOGS_DIR, f"errors_{RUN_DATE}.jsonl")
SUPPERS_FILE = os.path.join(LOGS_DIR, f"superseded_{RUN_DATE}.jsonl")
RUN_SUMMARY_FILE = os.path.join(LOGS_DIR, "run_summary.json")

print(f"[INFO] Using cik_map for run_date={RUN_DATE}: {INPUT_FILE}")

# Helpers
def sha256_bytes(b: bytes):
    return hashlib.sha256(b).hexdigest()

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get_submissions(cik10):
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def filter_filings(submissions, forms=("20-F",)):
    rec = submissions["filings"]["recent"]
    out = []
    for form, accession, date, period, doc in zip(
        rec["form"], rec["accessionNumber"], rec["filingDate"],
        rec["reportDate"], rec["primaryDocument"]
    ):
        if form in forms:
            out.append({
                "form": form,
                "accession": accession,
                "filing_date": date,
                "report_period": period,
                "primary_doc": doc,
            })
    return out

def build_filing_url(cik10, accession, doc):
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik10)}/{accession.replace('-', '')}/{doc}"

def is_html(doc_name):
    return doc_name.lower().endswith((".htm", ".html"))

# Load existing index (persistent)
if os.path.exists(INDEX_FILE):
    with open(INDEX_FILE) as f:
        annual_index = json.load(f)
else:
    annual_index = []

# Dedup trackers
seen_keys = {(row["cik10"], row["accession"]) for row in annual_index}
hashes_by_ticker = {}
for row in annual_index:
    if "sha256" in row:
        hashes_by_ticker.setdefault(row["ticker"], set()).add(row["sha256"])

run_summary = {}

# Main loop
df = pd.read_csv(INPUT_FILE, dtype=str).fillna("")

for _, row in df.iterrows():
    ticker, cik10 = row["ticker"], row["cik10"]
    if cik10 == "PENDING":
        continue

    start_time = time.time()
    submissions = get_submissions(cik10)
    filings = filter_filings(submissions, forms=("20-F",))

    # Keep latest per year & log superseded
    filings_by_year = {}
    for f in filings:
        year = f["filing_date"][:4]
        if year not in filings_by_year:
            filings_by_year[year] = f
        else:
            old = filings_by_year[year]
            if f["filing_date"] > old["filing_date"]:
                filings_by_year[year] = f
                with open(SUPPERS_FILE, "a") as sf:
                    sf.write(json.dumps({
                        "ticker": ticker,
                        "year": year,
                        "superseded_accession": old["accession"],
                        "kept_accession": f["accession"]
                    }) + "\n")
            else:
                with open(SUPPERS_FILE, "a") as sf:
                    sf.write(json.dumps({
                        "ticker": ticker,
                        "year": year,
                        "superseded_accession": f["accession"],
                        "kept_accession": old["accession"]
                    }) + "\n")

    folder = os.path.join(OUTPUT_DIR, ticker)
    os.makedirs(folder, exist_ok=True)

    successes, failures = 0, 0
    total_filings = len(filings_by_year)

    for year, f in filings_by_year.items():
        key = (cik10, f["accession"])
        if key in seen_keys:
            print(f"[SKIP] {ticker} {year} accession {f['accession']} already indexed.")
            continue

        if not is_html(f["primary_doc"]):
            print(f"[SKIP] {ticker} {year} accession {f['accession']} skipped (not HTML).")
            with open(ERRORS_FILE, "a") as ef:
                ef.write(json.dumps({
                    "ticker": ticker,
                    "year": year,
                    "accession": f["accession"],
                    "note": "primary doc not HTML",
                    "primary_doc": f["primary_doc"]
                }) + "\n")
            failures += 1
            continue

        url = build_filing_url(cik10, f["accession"], f["primary_doc"])
        try:
            r = requests.get(url, headers=HEADERS)
            r.raise_for_status()
            content = r.content
            sha = sha256_bytes(content)

            # Deduplication by file hash per ticker
            if sha in hashes_by_ticker.get(ticker, set()):
                print(f"[SKIP] {ticker} {year} accession {f['accession']} duplicate content (same file hash).")
                continue

            # Use accession for stable filename (no UUID)
            accession_str = f['accession'].replace("-", "")
            filename = f"{year}_{ticker}_{f['form']}_{accession_str}.html"
            filepath = os.path.join(folder, filename)

            with open(filepath, "wb") as out:
                out.write(content)

            entry = {
                "ticker": ticker,
                "cik10": cik10,
                "year": year,
                **f,
                "filing_url": url,
                "localPath": filepath,
                "sha256": sha,
                "bytes": len(content),
            }
            annual_index.append(entry)
            seen_keys.add(key)
            hashes_by_ticker.setdefault(ticker, set()).add(sha)

            print(f"[{ticker}] {year} saved {filename}")
            successes += 1

        except Exception as e:
            print(f"[{ticker}] ERROR {year}: {e}")
            with open(ERRORS_FILE, "a") as ef:
                ef.write(json.dumps({
                    "ticker": ticker,
                    "year": year,
                    "accession": f["accession"],
                    "error": str(e)
                }) + "\n")
            failures += 1

    runtime = round(time.time() - start_time, 2)
    run_summary[ticker] = {
        "total_filings": total_filings,
        "successes": successes,
        "failures": failures,
        "runtime_sec": runtime,
        "last_run": datetime.now().isoformat()
    }

# Post-run consistency check
for entry in annual_index:
    path = entry["localPath"]
    if not os.path.exists(path):
        print(f"[WARN] Missing file on disk for index entry: {entry['ticker']} {entry['accession']}")
    elif "sha256" not in entry or not entry["sha256"]:
        entry["sha256"] = sha256_file(path)
        print(f"[INFO] Added missing sha256 for {entry['ticker']} {entry['accession']}")

# Save outputs
with open(INDEX_FILE, "w") as f:
    json.dump(annual_index, f, indent=2)

with open(RUN_SUMMARY_FILE, "w") as f:
    json.dump(run_summary, f, indent=2)

print(f"[INFO] Completed. Index → {INDEX_FILE}, Run summary → {RUN_SUMMARY_FILE}")